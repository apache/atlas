package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Tracks migration progress in a Cassandra table for resume support.
 * Each token range completion is recorded so that on restart, completed ranges are skipped.
 */
public class MigrationStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationStateStore.class);

    private final CqlSession session;
    private final String keyspace;

    private PreparedStatement insertStmt;
    private PreparedStatement selectCompletedStmt;
    private PreparedStatement selectPhaseStmt;

    public MigrationStateStore(CqlSession session, String keyspace) {
        this.session  = session;
        this.keyspace = keyspace;
    }

    public void init() {
        session.execute(
            "CREATE TABLE IF NOT EXISTS " + keyspace + ".migration_state (" +
            "  phase text," +
            "  token_range_start bigint," +
            "  token_range_end bigint," +
            "  status text," +
            "  vertices_processed bigint," +
            "  edges_processed bigint," +
            "  last_updated timestamp," +
            "  PRIMARY KEY ((phase), token_range_start)" +
            ")");

        insertStmt = session.prepare(
            "INSERT INTO " + keyspace + ".migration_state " +
            "(phase, token_range_start, token_range_end, status, vertices_processed, edges_processed, last_updated) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        selectCompletedStmt = session.prepare(
            "SELECT token_range_start FROM " + keyspace + ".migration_state " +
            "WHERE phase = ? AND status = 'COMPLETED' ALLOW FILTERING");

        selectPhaseStmt = session.prepare(
            "SELECT token_range_start, status, vertices_processed, edges_processed " +
            "FROM " + keyspace + ".migration_state WHERE phase = ?");
    }

    public void markRangeCompleted(String phase, long rangeStart, long rangeEnd,
                                    long verticesProcessed, long edgesProcessed) {
        session.execute(insertStmt.bind(
            phase, rangeStart, rangeEnd, "COMPLETED",
            verticesProcessed, edgesProcessed, Instant.now()));
    }

    public void markRangeStarted(String phase, long rangeStart, long rangeEnd) {
        session.execute(insertStmt.bind(
            phase, rangeStart, rangeEnd, "IN_PROGRESS",
            0L, 0L, Instant.now()));
    }

    public void markRangeFailed(String phase, long rangeStart, long rangeEnd) {
        session.execute(insertStmt.bind(
            phase, rangeStart, rangeEnd, "FAILED",
            0L, 0L, Instant.now()));
    }

    /**
     * Returns the set of token range start values that have been completed.
     * Used on resume to skip already-migrated ranges.
     */
    public Set<Long> getCompletedRanges(String phase) {
        Set<Long> completed = new HashSet<>();
        ResultSet rs = session.execute(selectCompletedStmt.bind(phase));
        for (Row row : rs) {
            completed.add(row.getLong("token_range_start"));
        }
        LOG.info("Phase '{}': found {} completed token ranges from previous run", phase, completed.size());
        return completed;
    }

    /**
     * Returns summary counts for a phase (for reporting).
     */
    public long[] getPhaseSummary(String phase) {
        long totalVertices = 0, totalEdges = 0, completedRanges = 0;
        ResultSet rs = session.execute(selectPhaseStmt.bind(phase));
        for (Row row : rs) {
            if ("COMPLETED".equals(row.getString("status"))) {
                totalVertices += row.getLong("vertices_processed");
                totalEdges    += row.getLong("edges_processed");
                completedRanges++;
            }
        }
        return new long[]{completedRanges, totalVertices, totalEdges};
    }

    /** Clear state for a fresh migration */
    public void clearState(String phase) {
        session.execute("DELETE FROM " + keyspace + ".migration_state WHERE phase = '" + phase + "'");
        LOG.info("Cleared migration state for phase '{}'", phase);
    }
}
