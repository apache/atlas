package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Background reconciliation job that repairs Cassandra↔ES consistency in two phases:
 *
 * <b>Phase 1 — Deterministic:</b> Drains the FAILED partition of the es_outbox table.
 * These are vertices that the ESOutboxProcessor gave up on after 10 attempts (e.g.,
 * ES mapping errors, field limit exceeded). The job reads each vertex_id from Cassandra,
 * extracts the GUID, and reindexes to ES. On success, the FAILED entry is deleted.
 * This phase is bounded (only processes known failures) and self-healing — once the
 * root cause is fixed (e.g., ES field limit raised), the next cycle clears the backlog.
 *
 * <b>Phase 2 — Probabilistic:</b> Random sampling of ~1000 vertices across the Cassandra
 * ring to catch edge cases where the outbox entry itself was lost (e.g., Cassandra
 * write of outbox row failed after vertex was committed).
 *
 * Runs every 6 hours, lease-guarded so only one pod executes at a time.
 */
public class ESReconciliationJob implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ESReconciliationJob.class);

    private static final String LEASE_NAME = "es-reconciliation";
    private static final int LEASE_TTL_SECONDS = 2 * 3600; // 2 hours

    private static final int SAMPLE_PROBES = 5;       // number of random token probes
    private static final int ROWS_PER_PROBE = 200;    // rows per probe → ~1000 total
    private static final int MGET_BATCH_SIZE = 100;
    private static final int REINDEX_BATCH_SIZE = 50;
    private static final double CRITICAL_MISS_RATE = 0.05; // 5%

    private static final int FAILED_DRAIN_BATCH_SIZE = 50; // reindex batch size for failed entries

    private final CqlSession session;
    private final CassandraGraph graph;
    private final JobLeaseManager leaseManager;
    private final ESOutboxRepository outboxRepository;

    public ESReconciliationJob(CqlSession session, CassandraGraph graph,
                               JobLeaseManager leaseManager, ESOutboxRepository outboxRepository) {
        this.session = session;
        this.graph = graph;
        this.leaseManager = leaseManager;
        this.outboxRepository = outboxRepository;
    }

    @Override
    public void run() {
        if (!leaseManager.tryAcquire(LEASE_NAME, LEASE_TTL_SECONDS)) {
            LOG.info("ESReconciliationJob: another pod holds the lease, skipping");
            return;
        }

        try {
            runAudit();
        } finally {
            leaseManager.release(LEASE_NAME);
        }
    }

    private void runAudit() {
        long startTime = System.currentTimeMillis();

        try {
            RestClient esClient = AtlasElasticsearchDatabase.getLowLevelClient();
            if (esClient == null) {
                LOG.warn("ESReconciliationJob: ES client not available, skipping");
                return;
            }

            // ---- Phase 1: Drain FAILED outbox entries (deterministic repair) ----
            int failedDrained = drainFailedOutboxEntries();

            // ---- Phase 2: Random sample audit (probabilistic repair) ----
            LOG.info("ESReconciliationJob: starting sample-based ES audit ({} probes x {} rows)",
                    SAMPLE_PROBES, ROWS_PER_PROBE);

            int totalSampled = 0;
            int totalMissing = 0;
            int totalReindexed = 0;

            // Sample vertices by picking random token positions across the Cassandra ring.
            // Uses the vertices table (simple PK) instead of vertex_index (composite PK)
            // to avoid ALLOW FILTERING restrictions on token-range scans.
            for (int probe = 0; probe < SAMPLE_PROBES; probe++) {
                long randomToken = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, Long.MAX_VALUE);

                SimpleStatement stmt = SimpleStatement.builder(
                        "SELECT vertex_id, properties FROM vertices " +
                        "WHERE token(vertex_id) >= ? LIMIT ?")
                    .addPositionalValues(randomToken, ROWS_PER_PROBE)
                    .build();

                ResultSet rs = session.execute(stmt);

                List<String> guidBatch = new ArrayList<>(MGET_BATCH_SIZE);
                Map<String, String> guidToVertexId = new LinkedHashMap<>();

                for (Row row : rs) {
                    String vertexId = row.getString("vertex_id");
                    String propsJson = row.getString("properties");
                    if (vertexId == null || propsJson == null) continue;

                    // Extract __guid from properties, skip soft-deleted vertices
                    String guid = extractGuid(propsJson);
                    if (guid == null) continue;
                    if (isDeleted(propsJson)) continue;

                    guidBatch.add(guid);
                    guidToVertexId.put(guid, vertexId);
                    totalSampled++;

                    if (guidBatch.size() >= MGET_BATCH_SIZE) {
                        Set<String> missing = findMissingInES(esClient, guidBatch, guidToVertexId);
                        totalMissing += missing.size();
                        if (!missing.isEmpty()) {
                            totalReindexed += reindexMissing(missing);
                        }
                        guidBatch.clear();
                        guidToVertexId.clear();
                    }
                }

                // Process remaining batch for this probe
                if (!guidBatch.isEmpty()) {
                    Set<String> missing = findMissingInES(esClient, guidBatch, guidToVertexId);
                    totalMissing += missing.size();
                    if (!missing.isEmpty()) {
                        totalReindexed += reindexMissing(missing);
                    }
                }
            }

            double missRate = totalSampled > 0 ? (double) totalMissing / totalSampled : 0;
            long elapsed = System.currentTimeMillis() - startTime;

            if (missRate > CRITICAL_MISS_RATE) {
                LOG.error("ESReconciliationJob: CRITICAL — miss rate {}/{} ({}) exceeds {}% threshold. " +
                          "This indicates a systemic ES sync problem.",
                        totalMissing, totalSampled, String.format("%.1f%%", missRate * 100),
                        (int)(CRITICAL_MISS_RATE * 100));
            }

            LOG.info("ESReconciliationJob: completed in {}ms — " +
                            "phase1: {} failed outbox entries drained, " +
                            "phase2: sampled {} GUIDs, {} missing from ES, {} reindexed (miss rate: {})",
                    elapsed, failedDrained, totalSampled, totalMissing, totalReindexed,
                    String.format("%.2f%%", missRate * 100));

        } catch (Exception e) {
            LOG.error("ESReconciliationJob: failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private String extractGuid(String propsJson) {
        try {
            Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
            if (props == null) return null;
            Object guid = props.get("__guid");
            return guid != null ? String.valueOf(guid) : null;
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean isDeleted(String propsJson) {
        try {
            if (!propsJson.contains("DELETED")) return false;
            Map<String, Object> props = AtlasType.fromJson(propsJson, Map.class);
            if (props == null) return false;
            Object state = props.get("__state");
            return state != null && "DELETED".equals(String.valueOf(state));
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> findMissingInES(RestClient esClient, List<String> guids,
                                         Map<String, String> guidToVertexId) {
        Set<String> missing = new LinkedHashSet<>();
        try {
            String indexName = Constants.VERTEX_INDEX_NAME;

            StringBuilder mgetBody = new StringBuilder("{\"docs\":[");
            boolean first = true;
            for (String guid : guids) {
                String vertexId = guidToVertexId.get(guid);
                if (vertexId == null) continue;
                if (!first) mgetBody.append(",");
                mgetBody.append("{\"_index\":\"").append(indexName)
                        .append("\",\"_id\":\"").append(vertexId)
                        .append("\",\"_source\":false}");
                first = false;
            }
            mgetBody.append("]}");

            Request mgetReq = new Request("POST", "/_mget");
            mgetReq.setJsonEntity(mgetBody.toString());
            Response resp = esClient.performRequest(mgetReq);
            String respBody = EntityUtils.toString(resp.getEntity());

            Map<String, Object> mgetResp = AtlasType.fromJson(respBody, Map.class);
            List<Map<String, Object>> docs = (List<Map<String, Object>>) mgetResp.get("docs");
            if (docs == null) return missing;

            Map<String, String> vertexIdToGuid = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : guidToVertexId.entrySet()) {
                vertexIdToGuid.put(entry.getValue(), entry.getKey());
            }

            for (Map<String, Object> doc : docs) {
                Boolean found = (Boolean) doc.get("found");
                if (found == null || !found) {
                    String vertexId = String.valueOf(doc.get("_id"));
                    String guid = vertexIdToGuid.get(vertexId);
                    if (guid != null) {
                        missing.add(guid);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("ESReconciliationJob: _mget check failed for batch of {} GUIDs: {}",
                    guids.size(), e.getMessage());
        }
        return missing;
    }

    /**
     * Phase 1: Read all FAILED outbox vertex_ids, look up each vertex in Cassandra,
     * extract the GUID, reindex to ES, and delete the FAILED entry on success.
     *
     * This is deterministic — processes exactly the known failures, not a random sample.
     * Bounded by the FAILED partition size (typically small after the root cause is fixed).
     */
    private int drainFailedOutboxEntries() {
        List<String> failedVertexIds = outboxRepository.getFailedVertexIds(5000);
        if (failedVertexIds.isEmpty()) {
            LOG.info("ESReconciliationJob phase 1: no FAILED outbox entries");
            return 0;
        }

        LOG.info("ESReconciliationJob phase 1: draining {} FAILED outbox entries", failedVertexIds.size());

        int reindexed = 0;
        int notFound = 0;

        // Batch the GUIDs for reindexVertices
        List<String> guidBatch = new ArrayList<>(FAILED_DRAIN_BATCH_SIZE);
        // Track vertex_id → guid so we can delete FAILED entries on success
        Map<String, String> vertexIdToGuid = new LinkedHashMap<>();

        for (String vertexId : failedVertexIds) {
            // Read vertex from Cassandra, extract GUID
            CassandraVertex vertex = graph.getVertexRepository().getVertex(vertexId, graph);
            if (vertex == null) {
                // Vertex was deleted — clean up the stale FAILED entry
                outboxRepository.deleteFailedEntry(vertexId);
                notFound++;
                continue;
            }

            String guid = vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
            if (guid == null) {
                // Non-entity vertex (e.g., system vertex) — clean up
                outboxRepository.deleteFailedEntry(vertexId);
                notFound++;
                continue;
            }

            guidBatch.add(guid);
            vertexIdToGuid.put(vertexId, guid);

            if (guidBatch.size() >= FAILED_DRAIN_BATCH_SIZE) {
                int count = graph.reindexVertices(guidBatch);
                reindexed += count;
                // Delete FAILED entries for successfully reindexed vertices
                if (count == guidBatch.size()) {
                    for (String vid : vertexIdToGuid.keySet()) {
                        outboxRepository.deleteFailedEntry(vid);
                    }
                }
                guidBatch.clear();
                vertexIdToGuid.clear();
            }
        }

        // Process remaining batch
        if (!guidBatch.isEmpty()) {
            int count = graph.reindexVertices(guidBatch);
            reindexed += count;
            if (count == guidBatch.size()) {
                for (String vid : vertexIdToGuid.keySet()) {
                    outboxRepository.deleteFailedEntry(vid);
                }
            }
        }

        LOG.info("ESReconciliationJob phase 1: drained {} FAILED entries — {} reindexed, {} not found/cleaned up",
                failedVertexIds.size(), reindexed, notFound);
        return reindexed;
    }

    private int reindexMissing(Set<String> guids) {
        try {
            List<String> batch = new ArrayList<>(REINDEX_BATCH_SIZE);
            int totalReindexed = 0;

            for (String guid : guids) {
                batch.add(guid);
                if (batch.size() >= REINDEX_BATCH_SIZE) {
                    totalReindexed += graph.reindexVertices(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                totalReindexed += graph.reindexVertices(batch);
            }

            LOG.info("ESReconciliationJob: reindexed {} of {} missing GUIDs", totalReindexed, guids.size());
            return totalReindexed;
        } catch (Exception e) {
            LOG.error("ESReconciliationJob: reindex failed for {} GUIDs: {}", guids.size(), e.getMessage());
            return 0;
        }
    }
}
