package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages the es_outbox table which tracks vertices that need Elasticsearch sync.
 *
 * Table is partitioned by status: PRIMARY KEY ((status), vertex_id).
 * This makes "SELECT ... WHERE status = 'PENDING'" a direct partition scan —
 * no ALLOW FILTERING, scales to millions of rows.
 *
 * Status transitions (PENDING → FAILED) require DELETE from old partition +
 * INSERT into new partition (Cassandra can't change partition key in-place).
 * Completion (PENDING → done) is just a DELETE from the PENDING partition.
 *
 * gc_grace_seconds is set to 1 hour (not default 10 days) since this is a
 * transient queue table with LOGGED batches — no anti-entropy window needed.
 * Tombstones from completed entries are compacted within the hour.
 *
 * All rows are written with a 7-day TTL for auto-cleanup of stuck entries.
 */
public class ESOutboxRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ESOutboxRepository.class);

    static final String STATUS_PENDING = "PENDING";
    static final String STATUS_DONE    = "DONE";
    static final String STATUS_FAILED  = "FAILED";

    static final String ACTION_INDEX  = "index";
    static final String ACTION_DELETE = "delete";

    private static final int TTL_SECONDS = 7 * 24 * 3600; // 7 days
    static final int MAX_ATTEMPTS = 10;

    private final CqlSession session;
    private final PreparedStatement insertPendingStmt;
    private final PreparedStatement deletePendingStmt;
    private final PreparedStatement insertFailedStmt;
    private final PreparedStatement updateAttemptStmt;
    private final PreparedStatement selectPendingStmt;

    public ESOutboxRepository(CqlSession session) {
        this.session = session;

        // INSERT into PENDING partition with 7-day TTL
        this.insertPendingStmt = session.prepare(
            "INSERT INTO es_outbox (status, vertex_id, es_action, properties_json, attempt_count, created_at, last_attempted_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL " + TTL_SECONDS
        );

        // DELETE from PENDING partition (targeted PK delete)
        this.deletePendingStmt = session.prepare(
            "DELETE FROM es_outbox WHERE status = ? AND vertex_id = ?"
        );

        // INSERT into FAILED partition with 7-day TTL (for observability)
        this.insertFailedStmt = session.prepare(
            "INSERT INTO es_outbox (status, vertex_id, es_action, properties_json, attempt_count, created_at, last_attempted_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL " + TTL_SECONDS
        );

        // UPDATE within PENDING partition (no status change, keeps same partition)
        this.updateAttemptStmt = session.prepare(
            "UPDATE es_outbox USING TTL " + TTL_SECONDS +
            " SET attempt_count = ?, last_attempted_at = ? WHERE status = ? AND vertex_id = ?"
        );

        // Direct partition scan — no ALLOW FILTERING needed!
        this.selectPendingStmt = session.prepare(
            "SELECT vertex_id, es_action, status, properties_json, attempt_count, created_at, last_attempted_at " +
            "FROM es_outbox WHERE status = ?"
        );
    }

    /**
     * Returns a bound INSERT statement for an outbox entry without executing it.
     * Used by CassandraGraph.commit() to include outbox writes in the same batch.
     */
    public BoundStatement bindInsert(String vertexId, String action, String propertiesJson) {
        Instant now = Instant.now();
        return insertPendingStmt.bind(STATUS_PENDING, vertexId, action, propertiesJson, 0, now, now);
    }

    public void insert(String vertexId, String action, String propertiesJson) {
        session.execute(bindInsert(vertexId, action, propertiesJson));
    }

    /**
     * Commit-time cleanup: DELETE from PENDING partition by PK.
     * This is a targeted single-partition delete — fast and bounded.
     */
    public void markDone(String vertexId) {
        session.execute(deletePendingStmt.bind(STATUS_PENDING, vertexId));
    }

    /**
     * Batch cleanup for successfully synced entries: DELETE from PENDING partition.
     * All deletes target the same partition (status='PENDING'), so we use UNLOGGED
     * batch (single-partition batch is efficient in Cassandra — no coordinator log).
     *
     * With the partitioned schema, there's no need for a short-TTL DONE partition.
     * The old DONE trick was to avoid tombstones on ALLOW FILTERING scans — but now
     * PENDING is a direct partition scan, and tombstones are compacted within 1 hour
     * (gc_grace_seconds=3600 on the table).
     */
    public void batchMarkDone(List<String> vertexIds) {
        if (vertexIds.isEmpty()) return;
        int chunkSize = 100;
        for (int i = 0; i < vertexIds.size(); i += chunkSize) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
            int end = Math.min(i + chunkSize, vertexIds.size());
            for (int j = i; j < end; j++) {
                batch.addStatement(deletePendingStmt.bind(STATUS_PENDING, vertexIds.get(j)));
            }
            session.execute(batch.build());
        }
    }

    /**
     * Move entry from PENDING to FAILED partition. Uses LOGGED batch since it
     * spans two partitions (DELETE from PENDING + INSERT into FAILED).
     */
    public void markFailed(String vertexId, int attemptCount) {
        Instant now = Instant.now();
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(deletePendingStmt.bind(STATUS_PENDING, vertexId));
        batch.addStatement(insertFailedStmt.bind(STATUS_FAILED, vertexId, "", "", attemptCount, now, now));
        session.execute(batch.build());
    }

    /**
     * Increment attempt count within PENDING partition. No status change, so this
     * is a simple UPDATE within the same partition — no partition move needed.
     */
    public void incrementAttempt(String vertexId, int newAttemptCount) {
        session.execute(updateAttemptStmt.bind(newAttemptCount, Instant.now(), STATUS_PENDING, vertexId));
    }

    /**
     * Direct partition scan of the PENDING partition — O(PENDING rows), not O(total rows).
     * Under normal operation the PENDING partition is near-empty (inline sync handles 99%+).
     * During ES outages the partition grows but scans remain efficient (single-partition read).
     */
    public List<OutboxEntry> getPendingEntries(int limit) {
        ResultSet rs = session.execute(
            selectPendingStmt.bind(STATUS_PENDING).setPageSize(limit)
        );

        List<OutboxEntry> entries = new ArrayList<>();
        int count = 0;
        for (Row row : rs) {
            if (count >= limit) break;
            entries.add(rowToEntry(row));
            count++;
        }
        return entries;
    }

    /**
     * Returns all vertex_ids in the FAILED partition. Used by ESReconciliationJob
     * to deterministically drain entries that the outbox processor gave up on.
     */
    public List<String> getFailedVertexIds(int limit) {
        ResultSet rs = session.execute(
            selectPendingStmt.bind(STATUS_FAILED).setPageSize(limit)
        );

        List<String> ids = new ArrayList<>();
        int count = 0;
        for (Row row : rs) {
            if (count >= limit) break;
            ids.add(row.getString("vertex_id"));
            count++;
        }
        return ids;
    }

    /**
     * Delete a single entry from the FAILED partition after successful reindex.
     */
    public void deleteFailedEntry(String vertexId) {
        session.execute(deletePendingStmt.bind(STATUS_FAILED, vertexId));
    }

    private OutboxEntry rowToEntry(Row row) {
        return new OutboxEntry(
            row.getString("vertex_id"),
            row.getString("es_action"),
            row.getString("status"),
            row.getString("properties_json"),
            row.getInt("attempt_count"),
            row.getInstant("created_at"),
            row.getInstant("last_attempted_at")
        );
    }

    public static class OutboxEntry {
        public final String vertexId;
        public final String action;
        public final String status;
        public final String propertiesJson;
        public final int attemptCount;
        public final Instant createdAt;
        public final Instant lastAttemptedAt;

        public OutboxEntry(String vertexId, String action, String status, String propertiesJson,
                           int attemptCount, Instant createdAt, Instant lastAttemptedAt) {
            this.vertexId        = vertexId;
            this.action          = action;
            this.status          = status;
            this.propertiesJson  = propertiesJson;
            this.attemptCount    = attemptCount;
            this.createdAt       = createdAt;
            this.lastAttemptedAt = lastAttemptedAt;
        }
    }
}
