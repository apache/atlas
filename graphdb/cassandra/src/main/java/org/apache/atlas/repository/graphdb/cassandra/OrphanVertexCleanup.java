package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Background job that detects and repairs orphan vertices — vertices in the {@code vertices}
 * table that have a {@code __guid} but no corresponding entry in the {@code vertex_index}
 * table for {@code __guid_idx}.
 *
 * Uses a progressive cursor scan: each cycle processes a bounded number of rows (default 10,000)
 * starting from the last saved token position. Progress is persisted in {@code repair_progress}
 * so that crash or restart resumes from the checkpoint — no work is lost.
 *
 * Lease-guarded via {@link JobLeaseManager} so only one pod runs this at a time.
 */
public class OrphanVertexCleanup implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanVertexCleanup.class);

    private static final String JOB_NAME = "orphan-vertex-cleanup";
    private static final int LEASE_TTL_SECONDS = 30 * 60; // 30 minutes
    private static final int DEFAULT_ROWS_PER_CYCLE = 10_000;
    private static final int SCAN_PAGE_SIZE = 500;

    private final CqlSession session;
    private final IndexRepository indexRepository;
    private final JobLeaseManager leaseManager;
    private final int rowsPerCycle;

    public OrphanVertexCleanup(CqlSession session, IndexRepository indexRepository, JobLeaseManager leaseManager) {
        this(session, indexRepository, leaseManager, DEFAULT_ROWS_PER_CYCLE);
    }

    public OrphanVertexCleanup(CqlSession session, IndexRepository indexRepository,
                                JobLeaseManager leaseManager, int rowsPerCycle) {
        this.session = session;
        this.indexRepository = indexRepository;
        this.leaseManager = leaseManager;
        this.rowsPerCycle = rowsPerCycle;
    }

    @Override
    public void run() {
        if (!leaseManager.tryAcquire(JOB_NAME, LEASE_TTL_SECONDS)) {
            LOG.info("OrphanVertexCleanup: another pod holds the lease, skipping");
            return;
        }

        try {
            runProgressiveScan();
        } finally {
            leaseManager.release(JOB_NAME);
        }
    }

    private void runProgressiveScan() {
        LOG.info("OrphanVertexCleanup: starting progressive scan (limit={})", rowsPerCycle);
        long startTime = System.currentTimeMillis();

        try {
            // Read cursor from repair_progress
            long lastToken = loadLastToken();
            boolean isFirstRun = (lastToken == Long.MIN_VALUE);

            int totalScanned = 0;
            int orphansFound = 0;
            int orphansRepaired = 0;
            long latestToken = lastToken;

            // Scan vertices starting from cursor token
            SimpleStatement stmt = SimpleStatement.builder(
                    "SELECT vertex_id, properties, token(vertex_id) AS tkn FROM vertices " +
                    "WHERE token(vertex_id) > ? LIMIT ?")
                .addPositionalValues(lastToken, rowsPerCycle)
                .setPageSize(SCAN_PAGE_SIZE)
                .build();

            ResultSet rs = session.execute(stmt);

            for (Row row : rs) {
                totalScanned++;
                String vertexId = row.getString("vertex_id");
                String propsJson = row.getString("properties");
                long token = row.getLong("tkn");
                latestToken = token;

                if (propsJson == null || propsJson.isEmpty()) continue;

                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
                    if (props == null) continue;

                    Object guidObj = props.get("__guid");
                    if (guidObj == null) continue;

                    // Skip soft-deleted vertices — repairing their index would resurrect them
                    Object stateObj = props.get("__state");
                    if (stateObj != null && "DELETED".equals(String.valueOf(stateObj))) continue;

                    String guid = String.valueOf(guidObj);

                    // Check if __guid_idx entry exists
                    String indexedVertexId = indexRepository.lookupVertex("__guid_idx", guid);
                    if (indexedVertexId == null) {
                        orphansFound++;
                        LOG.warn("OrphanVertexCleanup: vertex '{}' has __guid='{}' but no __guid_idx entry, repairing",
                                vertexId, guid);

                        // Repair: create the missing index entry
                        indexRepository.addIndex("__guid_idx", guid, vertexId);

                        // Also repair qn_type_idx if qualifiedName and __typeName exist
                        Object qn = props.get("qualifiedName");
                        if (qn == null) qn = props.get("Referenceable.qualifiedName");
                        Object typeName = props.get("__typeName");
                        if (qn != null && typeName != null) {
                            indexRepository.addIndex("qn_type_idx", qn + ":" + typeName, vertexId);
                        }

                        orphansRepaired++;
                    }
                } catch (Exception e) {
                    LOG.warn("OrphanVertexCleanup: failed to process vertex '{}': {}", vertexId, e.getMessage());
                }

                if (totalScanned % 5000 == 0) {
                    LOG.info("OrphanVertexCleanup: scanned {} vertices, {} orphans found so far",
                            totalScanned, orphansFound);
                }
            }

            // Determine if the scan has wrapped around (no more rows)
            boolean cycleComplete = totalScanned < rowsPerCycle;

            // Save progress
            if (cycleComplete) {
                // Full cycle done — reset cursor for next cycle
                saveProgress(Long.MIN_VALUE, totalScanned, true);
                LOG.info("OrphanVertexCleanup: full cycle complete, cursor reset");
            } else {
                saveProgress(latestToken, totalScanned, false);
            }

            long elapsed = System.currentTimeMillis() - startTime;
            LOG.info("OrphanVertexCleanup: completed in {}ms — scanned {} vertices, {} orphans found, {} repaired, cycleComplete={}",
                    elapsed, totalScanned, orphansFound, orphansRepaired, cycleComplete);

        } catch (Exception e) {
            LOG.error("OrphanVertexCleanup: failed", e);
        }
    }

    private long loadLastToken() {
        try {
            ResultSet rs = session.execute(
                SimpleStatement.newInstance("SELECT last_token, cycle_complete FROM repair_progress WHERE job_name = ?", JOB_NAME));
            Row row = rs.one();
            if (row == null) return Long.MIN_VALUE;

            boolean cycleComplete = row.isNull("cycle_complete") || row.getBoolean("cycle_complete");
            if (cycleComplete) return Long.MIN_VALUE;

            return row.isNull("last_token") ? Long.MIN_VALUE : row.getLong("last_token");
        } catch (Exception e) {
            LOG.warn("OrphanVertexCleanup: failed to load progress, starting from beginning: {}", e.getMessage());
            return Long.MIN_VALUE;
        }
    }

    private void saveProgress(long lastToken, int rowsProcessed, boolean cycleComplete) {
        try {
            session.execute(SimpleStatement.newInstance(
                "INSERT INTO repair_progress (job_name, last_token, last_run_at, rows_processed, cycle_complete) " +
                "VALUES (?, ?, ?, ?, ?)",
                JOB_NAME, lastToken, Instant.now(), (long) rowsProcessed, cycleComplete));
        } catch (Exception e) {
            LOG.warn("OrphanVertexCleanup: failed to save progress: {}", e.getMessage());
        }
    }
}
