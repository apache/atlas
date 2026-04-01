package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Background job that detects and removes orphan edges — edges in the {@code edges_by_id}
 * table where one or both endpoint vertices no longer exist in the {@code vertices} table.
 *
 * Uses a progressive cursor scan: each cycle processes a bounded number of rows (default 10,000)
 * starting from the last saved token position. Progress is persisted in {@code repair_progress}
 * so that crash or restart resumes from the checkpoint — no work is lost.
 *
 * Lease-guarded via {@link JobLeaseManager} so only one pod runs this at a time.
 */
public class OrphanEdgeCleanup implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanEdgeCleanup.class);

    private static final String JOB_NAME = "orphan-edge-cleanup";
    private static final int LEASE_TTL_SECONDS = 30 * 60; // 30 minutes
    private static final int DEFAULT_ROWS_PER_CYCLE = 10_000;
    private static final int SCAN_PAGE_SIZE = 500;
    private static final int DELETE_BATCH_SIZE = 50;

    private final CqlSession session;
    private final VertexRepository vertexRepository;
    private final EdgeRepository edgeRepository;
    private final CassandraGraph graph;
    private final JobLeaseManager leaseManager;
    private final int rowsPerCycle;

    public OrphanEdgeCleanup(CqlSession session, VertexRepository vertexRepository,
                              EdgeRepository edgeRepository, CassandraGraph graph,
                              JobLeaseManager leaseManager) {
        this(session, vertexRepository, edgeRepository, graph, leaseManager, DEFAULT_ROWS_PER_CYCLE);
    }

    public OrphanEdgeCleanup(CqlSession session, VertexRepository vertexRepository,
                              EdgeRepository edgeRepository, CassandraGraph graph,
                              JobLeaseManager leaseManager, int rowsPerCycle) {
        this.session = session;
        this.vertexRepository = vertexRepository;
        this.edgeRepository = edgeRepository;
        this.graph = graph;
        this.leaseManager = leaseManager;
        this.rowsPerCycle = rowsPerCycle;
    }

    @Override
    public void run() {
        if (!leaseManager.tryAcquire(JOB_NAME, LEASE_TTL_SECONDS)) {
            LOG.info("OrphanEdgeCleanup: another pod holds the lease, skipping");
            return;
        }

        try {
            runProgressiveScan();
        } finally {
            leaseManager.release(JOB_NAME);
        }
    }

    private void runProgressiveScan() {
        LOG.info("OrphanEdgeCleanup: starting progressive scan (limit={})", rowsPerCycle);
        long startTime = System.currentTimeMillis();

        try {
            long lastToken = loadLastToken();

            int totalScanned = 0;
            int orphansFound = 0;
            int orphansDeleted = 0;
            long latestToken = lastToken;

            SimpleStatement stmt = SimpleStatement.builder(
                    "SELECT edge_id, out_vertex_id, in_vertex_id, edge_label, token(edge_id) AS tkn FROM edges_by_id " +
                    "WHERE token(edge_id) > ? LIMIT ?")
                .addPositionalValues(lastToken, rowsPerCycle)
                .setPageSize(SCAN_PAGE_SIZE)
                .build();

            ResultSet rs = session.execute(stmt);

            List<OrphanEdge> orphanBatch = new ArrayList<>(DELETE_BATCH_SIZE);

            for (Row row : rs) {
                totalScanned++;
                String edgeId = row.getString("edge_id");
                String outVertexId = row.getString("out_vertex_id");
                String inVertexId = row.getString("in_vertex_id");
                String edgeLabel = row.getString("edge_label");
                long token = row.getLong("tkn");
                latestToken = token;

                if (edgeId == null || outVertexId == null || inVertexId == null) continue;

                // Check if both endpoint vertices exist (LOCAL_QUORUM to avoid
                // false negatives from eventual consistency on recently-written vertices)
                boolean outExists = vertexRepository.vertexExistsQuorum(outVertexId);
                boolean inExists = vertexRepository.vertexExistsQuorum(inVertexId);

                if (!outExists || !inExists) {
                    orphansFound++;
                    orphanBatch.add(new OrphanEdge(edgeId, outVertexId, inVertexId, edgeLabel));
                    LOG.warn("OrphanEdgeCleanup: edge '{}' ({} -> {}, label='{}') has missing endpoint(s): out={}, in={}",
                            edgeId, outVertexId, inVertexId, edgeLabel, outExists, inExists);

                    if (orphanBatch.size() >= DELETE_BATCH_SIZE) {
                        orphansDeleted += deleteOrphanEdges(orphanBatch);
                        orphanBatch.clear();
                    }
                }

                if (totalScanned % 5000 == 0) {
                    LOG.info("OrphanEdgeCleanup: scanned {} edges, {} orphans found so far",
                            totalScanned, orphansFound);
                }
            }

            // Process remaining batch
            if (!orphanBatch.isEmpty()) {
                orphansDeleted += deleteOrphanEdges(orphanBatch);
            }

            boolean cycleComplete = totalScanned < rowsPerCycle;

            if (cycleComplete) {
                saveProgress(Long.MIN_VALUE, totalScanned, true);
                LOG.info("OrphanEdgeCleanup: full cycle complete, cursor reset");
            } else {
                saveProgress(latestToken, totalScanned, false);
            }

            long elapsed = System.currentTimeMillis() - startTime;
            LOG.info("OrphanEdgeCleanup: completed in {}ms — scanned {} edges, {} orphans found, {} deleted, cycleComplete={}",
                    elapsed, totalScanned, orphansFound, orphansDeleted, cycleComplete);

        } catch (Exception e) {
            LOG.error("OrphanEdgeCleanup: failed", e);
        }
    }

    private int deleteOrphanEdges(List<OrphanEdge> orphans) {
        int deleted = 0;
        for (OrphanEdge orphan : orphans) {
            try {
                CassandraEdge edge = new CassandraEdge(
                        orphan.edgeId, orphan.outVertexId, orphan.inVertexId, orphan.label, graph);
                edgeRepository.deleteEdge(edge);
                deleted++;
            } catch (Exception e) {
                LOG.warn("OrphanEdgeCleanup: failed to delete edge '{}': {}", orphan.edgeId, e.getMessage());
            }
        }
        return deleted;
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
            LOG.warn("OrphanEdgeCleanup: failed to load progress, starting from beginning: {}", e.getMessage());
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
            LOG.warn("OrphanEdgeCleanup: failed to save progress: {}", e.getMessage());
        }
    }

    private static class OrphanEdge {
        final String edgeId;
        final String outVertexId;
        final String inVertexId;
        final String label;

        OrphanEdge(String edgeId, String outVertexId, String inVertexId, String label) {
            this.edgeId = edgeId;
            this.outVertexId = outVertexId;
            this.inVertexId = inVertexId;
            this.label = label;
        }
    }
}
