/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.notification.task.AtlasDistributedTaskNotificationSender;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasType;
import org.janusgraph.util.encoding.LongEncoding;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;

@Component
public class BulkPurgeService {
    private static final Logger LOG = LoggerFactory.getLogger(BulkPurgeService.class);

    private static final String REDIS_KEY_PREFIX = "bulk_purge:";
    private static final String REDIS_LOCK_PREFIX = "bulk_purge_lock:";
    private static final String PURGE_MODE_CONNECTION = "CONNECTION";
    private static final String PURGE_MODE_QN_PREFIX = "QUALIFIED_NAME_PREFIX";
    private static final int MIN_QN_PREFIX_LENGTH = 10;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AtlasGraph graph;
    private final RedisService redisService;
    private final Set<EntityAuditRepository> auditRepositories;
    private final AtlasDistributedTaskNotificationSender taskNotificationSender;

    private final ExecutorService coordinatorExecutor;

    // Lazily initialized ES client (cached for reuse across calls)
    private volatile RestClient esClient;

    // Active purge tracking for cancel support
    private final ConcurrentHashMap<String, PurgeContext> activePurges = new ConcurrentHashMap<>();

    @Inject
    public BulkPurgeService(AtlasGraph graph,
                            RedisService redisService,
                            Set<EntityAuditRepository> auditRepositories,
                            AtlasDistributedTaskNotificationSender taskNotificationSender) {
        this.graph = graph;
        this.redisService = redisService;
        this.auditRepositories = auditRepositories;
        this.taskNotificationSender = taskNotificationSender;

        this.coordinatorExecutor = Executors.newFixedThreadPool(2,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-coordinator-%d")
                        .build());
    }

    private synchronized RestClient getEsClient() {
        if (esClient == null) {
            esClient = AtlasElasticsearchDatabase.getLowLevelClient();
        }
        return esClient;
    }

    @VisibleForTesting
    void setEsClient(RestClient client) {
        this.esClient = client;
    }

    /**
     * Purge all assets belonging to a connection.
     * The Connection entity itself is NOT deleted unless {@code deleteConnection} is true.
     *
     * @param connectionQN     qualifiedName of the connection
     * @param submittedBy      user who submitted the request
     * @param deleteConnection if true, delete the Connection entity after all child assets are purged
     */
    public String bulkPurgeByConnection(String connectionQN, String submittedBy, boolean deleteConnection) throws AtlasBaseException {
        if (connectionQN == null || connectionQN.isEmpty()) {
            throw new AtlasBaseException("connectionQualifiedName is required");
        }

        // Verify connection exists in graph
        AtlasVertex connVertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(graph,
                CONNECTION_ENTITY_TYPE, QUALIFIED_NAME, connectionQN);
        if (connVertex == null) {
            throw new AtlasBaseException("Connection not found: " + connectionQN);
        }

        // ES query: prefix on __qualifiedNameHierarchy matching child assets only.
        // Append "/" so the prefix matches children but NOT the Connection entity itself:
        //   Connection QN "default/snowflake/123" → hierarchy value "default/snowflake/123" (no trailing /)
        //   Child QN      "default/snowflake/123/db" → hierarchy includes "default/snowflake/123/db"
        // Prefix "default/snowflake/123/" matches children but not the connection.
        // This also prevents prefix collisions between connections (e.g. "123" vs "1234").
        String esQuery = buildPrefixQuery(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, connectionQN + "/");
        return submitPurge(connectionQN, PURGE_MODE_CONNECTION, esQuery, submittedBy, deleteConnection);
    }

    /**
     * Purge all assets matching a qualifiedName prefix.
     */
    public String bulkPurgeByQualifiedName(String qualifiedNamePrefix, String submittedBy) throws AtlasBaseException {
        if (qualifiedNamePrefix == null || qualifiedNamePrefix.length() < MIN_QN_PREFIX_LENGTH) {
            throw new AtlasBaseException("qualifiedName prefix must be at least " + MIN_QN_PREFIX_LENGTH + " characters");
        }

        // ES query: prefix on __qualifiedNameHierarchy (always indexed by JanusGraph)
        String esQuery = buildPrefixQuery(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, qualifiedNamePrefix);
        return submitPurge(qualifiedNamePrefix, PURGE_MODE_QN_PREFIX, esQuery, submittedBy, false);
    }

    /**
     * Get the current status of a purge request.
     */
    public Map<String, Object> getStatus(String requestId) {
        // Search active purges first
        for (PurgeContext ctx : activePurges.values()) {
            if (ctx.requestId.equals(requestId)) {
                return ctx.toStatusMap();
            }
        }

        // Check Redis for completed/failed purges
        // Scan all known keys (in production, store requestId -> purgeKey mapping)
        String redisValue = redisService.getValue("bulk_purge_request:" + requestId);
        if (redisValue != null) {
            try {
                return MAPPER.readValue(redisValue, Map.class);
            } catch (Exception e) {
                LOG.warn("Failed to parse purge status from Redis for requestId={}", requestId, e);
            }
        }

        return null;
    }

    /**
     * Cancel an in-progress purge.
     */
    public boolean cancelPurge(String requestId) {
        for (PurgeContext ctx : activePurges.values()) {
            if (ctx.requestId.equals(requestId)) {
                LOG.info("BulkPurge: Cancel requested for requestId={}, purgeKey={}", requestId, ctx.purgeKey);
                ctx.cancelRequested = true;
                return true;
            }
        }
        return false;
    }

    // ======================== PRIVATE METHODS ========================

    private String submitPurge(String purgeKey, String purgeMode, String esQuery, String submittedBy, boolean deleteConnection) throws AtlasBaseException {
        String redisKey = REDIS_KEY_PREFIX + purgeKey;
        String lockKey = REDIS_LOCK_PREFIX + purgeKey;

        // Check for existing active purge
        String existingStatus = redisService.getValue(redisKey);
        if (existingStatus != null) {
            try {
                JsonNode statusNode = MAPPER.readTree(existingStatus);
                String status = statusNode.has("status") ? statusNode.get("status").asText() : "";
                if ("RUNNING".equals(status)) {
                    long lastHeartbeat = statusNode.has("lastHeartbeat") ? statusNode.get("lastHeartbeat").asLong() : 0;
                    long fiveMinutesAgo = System.currentTimeMillis() - (5 * 60 * 1000);
                    if (lastHeartbeat > fiveMinutesAgo) {
                        throw new AtlasBaseException("Bulk purge already in progress for: " + purgeKey +
                                " (requestId: " + statusNode.get("requestId").asText() + ")");
                    }
                    LOG.warn("BulkPurge: Found stale RUNNING purge for {} (last heartbeat: {}). Allowing re-submit.", purgeKey, lastHeartbeat);
                }
            } catch (AtlasBaseException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Failed to parse existing purge status for {}", purgeKey, e);
            }
        }

        String requestId = UUID.randomUUID().toString();

        // Write initial status
        PurgeContext ctx = new PurgeContext(requestId, purgeKey, purgeMode, submittedBy, esQuery, deleteConnection);
        ctx.status = "PENDING";
        writeRedisStatus(ctx);

        // Store requestId -> purgeKey mapping for status lookup
        int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
        redisService.putValue("bulk_purge_request:" + requestId, ctx.toJson(), redisTtl);

        activePurges.put(purgeKey, ctx);

        // Submit to coordinator executor
        coordinatorExecutor.submit(() -> executePurge(ctx, lockKey, redisKey));

        LOG.info("BulkPurge: Submitted requestId={}, purgeKey={}, purgeMode={}, submittedBy={}",
                requestId, purgeKey, purgeMode, submittedBy);

        return requestId;
    }

    private void executePurge(PurgeContext ctx, String lockKey, String redisKey) {
        ScheduledExecutorService heartbeatExecutor = null;

        try {
            // Acquire distributed lock
            boolean lockAcquired = redisService.acquireDistributedLock(lockKey);
            if (!lockAcquired) {
                LOG.warn("BulkPurge: Could not acquire lock for {}", ctx.purgeKey);
                ctx.status = "FAILED";
                ctx.error = "Could not acquire distributed lock";
                writeRedisStatus(ctx);
                return;
            }

            ctx.status = "RUNNING";
            ctx.lastHeartbeat = System.currentTimeMillis();
            writeRedisStatus(ctx);

            // Start heartbeat
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("bulk-purge-heartbeat-" + ctx.purgeKey).build());
            int heartbeatInterval = AtlasConfiguration.BULK_PURGE_HEARTBEAT_INTERVAL_MS.getInt();
            heartbeatExecutor.scheduleAtFixedRate(() -> {
                try {
                    ctx.lastHeartbeat = System.currentTimeMillis();
                    writeRedisStatus(ctx);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Heartbeat failed for {}", ctx.purgeKey, e);
                }
            }, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);

            // Phase 1+2: Stream from ES and delete in parallel
            long startTime = System.currentTimeMillis();
            streamAndDelete(ctx);

            if (ctx.cancelRequested) {
                ctx.status = "CANCELLED";
                LOG.info("BulkPurge: Cancelled for purgeKey={}, deleted={}, failed={}",
                        ctx.purgeKey, ctx.totalDeleted.get(), ctx.totalFailed.get());
            } else {
                // Phase 3: Post-purge reconciliation
                esCleanup(ctx);
                repairExternalLineage(ctx);

                // Phase 4: Delete the Connection entity itself (if requested)
                if (ctx.deleteConnection && PURGE_MODE_CONNECTION.equals(ctx.purgeMode)) {
                    deleteConnectionVertex(ctx);
                }

                writeSummaryAuditEvent(ctx, startTime);

                ctx.status = "COMPLETED";
                long duration = System.currentTimeMillis() - startTime;
                LOG.info("BulkPurge: Completed for purgeKey={}, deleted={}, failed={}, duration={}ms",
                        ctx.purgeKey, ctx.totalDeleted.get(), ctx.totalFailed.get(), duration);
            }

            writeRedisStatus(ctx);

        } catch (Exception e) {
            LOG.error("BulkPurge: Failed for purgeKey={}", ctx.purgeKey, e);
            ctx.status = "FAILED";
            ctx.error = e.getMessage();
            writeRedisStatus(ctx);
        } finally {
            if (heartbeatExecutor != null) {
                heartbeatExecutor.shutdownNow();
            }
            activePurges.remove(ctx.purgeKey);
            try {
                redisService.releaseDistributedLock(lockKey);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to release lock for {}", ctx.purgeKey, e);
            }
        }
    }

    /**
     * Phase 1+2: Stream ES scroll results and delete vertices in parallel.
     */
    private void streamAndDelete(PurgeContext ctx) throws Exception {
        int batchSize = AtlasConfiguration.BULK_PURGE_BATCH_SIZE.getInt();
        int esPageSize = AtlasConfiguration.BULK_PURGE_ES_PAGE_SIZE.getInt();
        int scrollTimeoutMin = AtlasConfiguration.BULK_PURGE_SCROLL_TIMEOUT_MINUTES.getInt();

        // Determine worker count
        int configuredWorkerCount = AtlasConfiguration.BULK_PURGE_WORKER_COUNT.getInt();

        // First, get total count via ES
        long totalEntities = getEntityCount(ctx.esQuery);
        ctx.totalDiscovered = totalEntities;
        LOG.info("BulkPurge: Discovered {} entities for purgeKey={}", totalEntities, ctx.purgeKey);

        if (totalEntities == 0) {
            LOG.info("BulkPurge: No entities to delete for purgeKey={}", ctx.purgeKey);
            return;
        }

        // Auto-scale worker count
        int workerCount = getWorkerCount(totalEntities, configuredWorkerCount);
        ctx.workerCount = workerCount;
        writeRedisStatus(ctx);

        // Create batch queue with backpressure
        BlockingQueue<BatchWork> batchQueue = new LinkedBlockingQueue<>(workerCount * 2);

        // Create worker pool
        ExecutorService workerPool = Executors.newFixedThreadPool(workerCount,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-worker-%d-" + ctx.purgeKey)
                        .build());

        // Start workers
        List<Future<?>> workerFutures = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            workerFutures.add(workerPool.submit(() -> workerLoop(ctx, batchQueue)));
        }

        // Stream ES scroll into queue (coordinator role)
        streamESScrollIntoBatchQueue(ctx, batchQueue, batchSize, esPageSize, scrollTimeoutMin);

        // Send poison pills to stop workers
        for (int i = 0; i < workerCount; i++) {
            batchQueue.put(BatchWork.POISON_PILL);
        }

        // Wait for all workers to finish
        for (Future<?> f : workerFutures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                LOG.error("BulkPurge: Worker failed for purgeKey={}", ctx.purgeKey, e.getCause());
            }
        }

        workerPool.shutdown();
        workerPool.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Worker loop: pulls batches from queue until poison pill received.
     */
    private void workerLoop(PurgeContext ctx, BlockingQueue<BatchWork> queue) {
        while (!ctx.cancelRequested) {
            try {
                BatchWork work = queue.poll(5, TimeUnit.SECONDS);
                if (work == null) continue;
                if (work == BatchWork.POISON_PILL) break;

                processBatch(ctx, work);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("BulkPurge: Worker error for purgeKey={}", ctx.purgeKey, e);
            }
        }
    }

    /**
     * Process a single batch: delete vertices, commit, update progress.
     */
    private void processBatch(PurgeContext ctx, BatchWork work) {
        int batchDeleted = 0;
        int batchFailed = 0;

        // Use injected graph — JanusGraph manages thread-local transactions internally
        AtlasGraph workerGraph = this.graph;

        for (String vertexId : work.vertexIds) {
            if (ctx.cancelRequested) break;

            try {
                AtlasVertex vertex = workerGraph.getVertex(vertexId);
                if (vertex == null) {
                    continue; // Already deleted (idempotent)
                }

                // Only collect lineage for vertices that actually have lineage.
                // Since ALL same-connection entities are deleted, only vertices with
                // __hasLineage=true can have external lineage partners to repair.
                Boolean vertexHasLineage = vertex.getProperty(HAS_LINEAGE, Boolean.class);
                if (Boolean.TRUE.equals(vertexHasLineage)) {
                    collectExternalLineageVertices(ctx, vertex);
                }

                // Remove all edges
                Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH);
                for (AtlasEdge edge : edges) {
                    try {
                        workerGraph.removeEdge(edge);
                    } catch (Exception e) {
                        // Edge may already be removed by another vertex deletion
                        LOG.debug("BulkPurge: Could not remove edge {} for vertex {}", edge.getId(), vertexId);
                    }
                }

                workerGraph.removeVertex(vertex);
                batchDeleted++;
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to remove vertex {}", vertexId, e);
                batchFailed++;
            }
        }

        // Commit with retry
        boolean committed = commitWithRetry(workerGraph, work.batchIndex);
        if (committed) {
            ctx.totalDeleted.addAndGet(batchDeleted);
            ctx.totalFailed.addAndGet(batchFailed);
        } else {
            // Whole batch failed
            ctx.totalFailed.addAndGet(work.vertexIds.size());
            try {
                workerGraph.rollback();
            } catch (Exception e) {
                LOG.warn("BulkPurge: Rollback failed for batch {}", work.batchIndex, e);
            }
        }

        int batches = ctx.completedBatches.incrementAndGet();
        ctx.lastProcessedBatchIndex = Math.max(ctx.lastProcessedBatchIndex, work.batchIndex);

        // Periodic progress update
        if (batches % 10 == 0) {
            writeRedisStatus(ctx);
            LOG.info("BulkPurge: Progress purgeKey={}, batches={}, deleted={}, failed={}",
                    ctx.purgeKey, batches, ctx.totalDeleted.get(), ctx.totalFailed.get());
        }
    }

    private boolean commitWithRetry(AtlasGraph workerGraph, int batchIndex) {
        int maxRetries = AtlasConfiguration.BULK_PURGE_COMMIT_MAX_RETRIES.getInt();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                workerGraph.commit();
                return true;
            } catch (Exception e) {
                LOG.warn("BulkPurge: Commit failed for batch {} (attempt {}/{})", batchIndex, attempt, maxRetries, e);
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep((long) Math.pow(2, attempt - 1) * 500); // 500ms, 1s, 2s
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }
        return false;
    }

    private void collectExternalLineageVertices(PurgeContext ctx, AtlasVertex vertex) {
        try {
            String[] lineageLabels = {PROCESS_INPUTS, PROCESS_OUTPUTS};
            Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH, lineageLabels);

            for (AtlasEdge edge : edges) {
                try {
                    AtlasVertex inVertex = edge.getInVertex();
                    AtlasVertex outVertex = edge.getOutVertex();
                    AtlasVertex other = (inVertex.getId().equals(vertex.getId())) ? outVertex : inVertex;

                    String otherConnQN = other.getProperty(CONNECTION_QUALIFIED_NAME, String.class);
                    if (otherConnQN != null && !otherConnQN.equals(ctx.purgeKey)) {
                        ctx.externalLineageVertexIds.add(other.getId().toString());
                    }
                } catch (Exception e) {
                    LOG.debug("BulkPurge: Could not process lineage edge for vertex {}", vertex.getId(), e);
                }
            }
        } catch (Exception e) {
            LOG.debug("BulkPurge: Could not collect lineage vertices for {}", vertex.getId(), e);
        }
    }

    // ======================== ES OPERATIONS ========================

    private long getEntityCount(String esQuery) throws Exception {
        RestClient esClient = getEsClient();
        String endpoint = "/" + VERTEX_INDEX_NAME + "/_count";

        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(esQuery, ContentType.APPLICATION_JSON));

        Response response = esClient.performRequest(request);
        String responseBody = readResponseBody(response);
        JsonNode root = MAPPER.readTree(responseBody);
        return root.get("count").asLong();
    }

    /**
     * Stream ES scroll results into the batch queue.
     */
    private void streamESScrollIntoBatchQueue(PurgeContext ctx,
                                              BlockingQueue<BatchWork> batchQueue,
                                              int batchSize,
                                              int esPageSize,
                                              int scrollTimeoutMin) throws Exception {
        RestClient esClient = getEsClient();
        String scrollTimeout = scrollTimeoutMin + "m";

        // Build scroll request — only fetch _id field (vertex ID)
        String scrollQuery = buildScrollQuery(ctx.esQuery, esPageSize);

        // Initial search with scroll
        String endpoint = "/" + VERTEX_INDEX_NAME + "/_search?scroll=" + scrollTimeout;
        Request searchRequest = new Request("POST", endpoint);
        searchRequest.setEntity(new NStringEntity(scrollQuery, ContentType.APPLICATION_JSON));

        Response response = esClient.performRequest(searchRequest);
        String responseBody = readResponseBody(response);
        JsonNode root = MAPPER.readTree(responseBody);

        String scrollId = root.get("_scroll_id").asText();
        JsonNode hits = root.get("hits").get("hits");

        List<String> currentBatch = new ArrayList<>(batchSize);
        int batchIndex = 0;

        try {
            while (hits != null && hits.size() > 0 && !ctx.cancelRequested) {
                for (JsonNode hit : hits) {
                    if (ctx.cancelRequested) break;

                    // ES _id is base-36 encoded; decode to the JanusGraph long vertex ID
                    String esDocId = hit.get("_id").asText();
                    String vertexId = String.valueOf(LongEncoding.decode(esDocId));
                    currentBatch.add(vertexId);

                    if (currentBatch.size() >= batchSize) {
                        batchQueue.put(new BatchWork(new ArrayList<>(currentBatch), batchIndex++));
                        currentBatch.clear();
                    }
                }

                if (ctx.cancelRequested) break;

                // Scroll next page
                Request scrollRequest = new Request("POST", "/_search/scroll");
                String scrollBody = MAPPER.writeValueAsString(
                        MAPPER.createObjectNode()
                                .put("scroll", scrollTimeout)
                                .put("scroll_id", scrollId));
                scrollRequest.setEntity(new NStringEntity(scrollBody, ContentType.APPLICATION_JSON));

                response = esClient.performRequest(scrollRequest);
                responseBody = readResponseBody(response);
                root = MAPPER.readTree(responseBody);
                scrollId = root.get("_scroll_id").asText();
                hits = root.get("hits").get("hits");
            }

            // Queue remaining batch
            if (!currentBatch.isEmpty() && !ctx.cancelRequested) {
                batchQueue.put(new BatchWork(new ArrayList<>(currentBatch), batchIndex));
            }

        } finally {
            // Clear scroll
            clearScroll(esClient, scrollId);
        }
    }

    private void clearScroll(RestClient esClient, String scrollId) {
        try {
            if (scrollId != null) {
                Request clearRequest = new Request("DELETE", "/_search/scroll");
                String body = MAPPER.writeValueAsString(
                        MAPPER.createObjectNode().put("scroll_id", scrollId));
                clearRequest.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));
                esClient.performRequest(clearRequest);
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to clear scroll", e);
        }
    }

    // ======================== POST-PURGE PHASE ========================

    /**
     * Phase 3a: ES delete_by_query as safety-net cleanup.
     */
    private void esCleanup(PurgeContext ctx) {
        try {
            RestClient esClient = getEsClient();
            String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&slices=auto";

            Request request = new Request("POST", endpoint);
            request.setEntity(new NStringEntity(ctx.esQuery, ContentType.APPLICATION_JSON));

            Response response = esClient.performRequest(request);
            String responseBody = readResponseBody(response);
            LOG.info("BulkPurge: ES cleanup completed for purgeKey={}, response={}", ctx.purgeKey, responseBody);
        } catch (Exception e) {
            LOG.error("BulkPurge: ES cleanup failed for purgeKey={}. Manual cleanup may be needed.", ctx.purgeKey, e);
        }
    }

    /**
     * Phase 4: Delete the Connection vertex itself after all child assets have been purged.
     */
    private void deleteConnectionVertex(PurgeContext ctx) {
        try {
            AtlasVertex connVertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(graph,
                    CONNECTION_ENTITY_TYPE, QUALIFIED_NAME, ctx.purgeKey);
            if (connVertex == null) {
                LOG.warn("BulkPurge: Connection vertex not found for deletion: {}", ctx.purgeKey);
                return;
            }

            // Remove all edges
            Iterable<AtlasEdge> edges = connVertex.getEdges(AtlasEdgeDirection.BOTH);
            for (AtlasEdge edge : edges) {
                try {
                    graph.removeEdge(edge);
                } catch (Exception e) {
                    LOG.debug("BulkPurge: Could not remove edge {} from connection vertex", edge.getId(), e);
                }
            }

            graph.removeVertex(connVertex);
            graph.commit();

            ctx.connectionDeleted = true;
            ctx.totalDeleted.incrementAndGet();

            LOG.info("BulkPurge: Connection entity deleted for purgeKey={}", ctx.purgeKey);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to delete Connection entity for purgeKey={}", ctx.purgeKey, e);
            ctx.error = "Child assets purged but Connection deletion failed: " + e.getMessage();
            try {
                graph.rollback();
            } catch (Exception re) {
                LOG.warn("BulkPurge: Rollback failed after connection deletion failure", re);
            }
        }
    }

    /**
     * Phase 3b: Repair __hasLineage on external vertices whose lineage partners were purged.
     *
     * For each external vertex, checks whether it still has any active lineage edges.
     * If none remain, sets {@code __hasLineage = false} directly on the graph vertex.
     * This mirrors the synchronous path in {@code DeleteHandlerV1.resetHasLineageOnInputOutputDelete}.
     */
    private void repairExternalLineage(PurgeContext ctx) {
        if (ctx.externalLineageVertexIds.isEmpty()) {
            LOG.info("BulkPurge: No external lineage vertices to repair for purgeKey={}", ctx.purgeKey);
            return;
        }

        LOG.info("BulkPurge: Repairing lineage on {} external vertices for purgeKey={}",
                ctx.externalLineageVertexIds.size(), ctx.purgeKey);

        int repaired = 0;
        int skipped = 0;

        String[] lineageLabels = {PROCESS_INPUTS, PROCESS_OUTPUTS};

        for (String vertexId : ctx.externalLineageVertexIds) {
            boolean repairSuccess = false;
            for (int attempt = 1; attempt <= 3 && !repairSuccess; attempt++) {
                try {
                    AtlasVertex vertex = graph.getVertex(vertexId);
                    if (vertex == null) {
                        skipped++;
                        repairSuccess = true;
                        break;
                    }

                    boolean hasActiveLineage = false;
                    Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH, lineageLabels);
                    for (AtlasEdge edge : edges) {
                        String edgeState = edge.getProperty(STATE_PROPERTY_KEY, String.class);
                        if (ACTIVE_STATE_VALUE.equals(edgeState)) {
                            hasActiveLineage = true;
                            break;  // O(1) for vertices that still have other lineage
                        }
                    }

                    if (!hasActiveLineage) {
                        AtlasGraphUtilsV2.setEncodedProperty(vertex, HAS_LINEAGE, false);
                        graph.commit();
                        repaired++;
                    }
                    repairSuccess = true;
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Lineage repair attempt {}/3 failed for vertex {}",
                            attempt, vertexId, e);
                    try {
                        graph.rollback();
                    } catch (Exception re) { /* ignore */ }
                    if (attempt < 3) {
                        try {
                            Thread.sleep(attempt * 500L);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            if (!repairSuccess) {
                LOG.error("BulkPurge: Failed to repair lineage for vertex {} after 3 attempts", vertexId);
                skipped++;
            }
        }

        LOG.info("BulkPurge: Lineage repair complete for purgeKey={}: repaired={}, skipped={}",
                ctx.purgeKey, repaired, skipped);
    }

    /**
     * Phase 3c: Write a summary audit event.
     */
    private void writeSummaryAuditEvent(PurgeContext ctx, long startTime) {
        try {
            long duration = System.currentTimeMillis() - startTime;

            ObjectNode detailsNode = MAPPER.createObjectNode();
            detailsNode.put("purgeKey", ctx.purgeKey);
            detailsNode.put("purgeMode", ctx.purgeMode);
            detailsNode.put("totalDeleted", ctx.totalDeleted.get());
            detailsNode.put("totalFailed", ctx.totalFailed.get());
            detailsNode.put("totalDiscovered", ctx.totalDiscovered);
            detailsNode.put("durationMs", duration);
            detailsNode.put("requestId", ctx.requestId);
            detailsNode.put("workerCount", ctx.workerCount);

            // ESBasedAuditRepository expects:
            //   1. details string prefixed with the audit action prefix ("Purged: ")
            //   2. a non-null AtlasEntity on the event (skips if null)
            String auditPrefix = "Purged: ";
            String details = auditPrefix + detailsNode.toString();

            // Create a minimal AtlasEntity so ES audit doesn't skip this event
            AtlasEntity summaryEntity = new AtlasEntity("__internal");
            summaryEntity.setGuid(ctx.requestId);
            summaryEntity.setAttribute("qualifiedName", "bulk-purge:" + ctx.purgeKey);
            summaryEntity.setUpdateTime(new java.util.Date());

            String entityId = ctx.purgeKey;
            EntityAuditEventV2 event = new EntityAuditEventV2(
                    entityId,
                    System.currentTimeMillis(),
                    ctx.submittedBy,
                    EntityAuditActionV2.ENTITY_PURGE,
                    details,
                    summaryEntity);
            // Must use AtlasType.toJson() — ESBasedAuditRepository template injects this
            // raw into a JSON document (slot {5} is unquoted in the template).
            event.setEntityQualifiedName(AtlasType.toJson("bulk-purge:" + ctx.purgeKey));

            for (EntityAuditRepository auditRepository : auditRepositories) {
                auditRepository.putEventsV2(event);
            }
            LOG.info("BulkPurge: Audit event written for purgeKey={}", ctx.purgeKey);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to write audit event for purgeKey={}", ctx.purgeKey, e);
        }
    }

    // ======================== ES QUERY BUILDERS ========================

    private String buildTermQuery(String field, String value) {
        try {
            ObjectNode query = MAPPER.createObjectNode();
            ObjectNode queryBody = MAPPER.createObjectNode();
            ObjectNode term = MAPPER.createObjectNode();
            term.put(field, value);
            queryBody.set("term", term);
            query.set("query", queryBody);
            return MAPPER.writeValueAsString(query);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build term query", e);
        }
    }

    private String buildPrefixQuery(String field, String value) {
        try {
            ObjectNode query = MAPPER.createObjectNode();
            ObjectNode queryBody = MAPPER.createObjectNode();
            ObjectNode prefix = MAPPER.createObjectNode();
            prefix.put(field, value);
            queryBody.set("prefix", prefix);
            query.set("query", queryBody);
            return MAPPER.writeValueAsString(query);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build prefix query", e);
        }
    }

    private String buildScrollQuery(String esQuery, int pageSize) {
        try {
            JsonNode queryNode = MAPPER.readTree(esQuery);
            ObjectNode scrollQuery = MAPPER.createObjectNode();
            scrollQuery.set("query", queryNode.get("query"));
            scrollQuery.put("size", pageSize);
            scrollQuery.put("_source", false); // Only need _id, which is always returned
            scrollQuery.put("track_total_hits", true);
            return MAPPER.writeValueAsString(scrollQuery);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build scroll query", e);
        }
    }

    // ======================== HELPER METHODS ========================

    private int getWorkerCount(long totalEntities, int configuredMax) {
        if (totalEntities < 10_000) return 1;
        if (totalEntities < 100_000) return 2;
        if (totalEntities < 1_000_000) return Math.min(configuredMax, 4);
        return Math.min(configuredMax, 8);
    }

    private void writeRedisStatus(PurgeContext ctx) {
        try {
            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            String redisKey = REDIS_KEY_PREFIX + ctx.purgeKey;
            String json = ctx.toJson();
            redisService.putValue(redisKey, json, redisTtl);
            // Also update the requestId mapping
            redisService.putValue("bulk_purge_request:" + ctx.requestId, json, redisTtl);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to write Redis status for {}", ctx.purgeKey, e);
        }
    }

    private String readResponseBody(Response response) throws Exception {
        try (InputStream is = response.getEntity().getContent()) {
            return new String(is.readAllBytes());
        }
    }

    /**
     * Holds state for an active purge operation.
     */
    static class PurgeContext {
        final String requestId;
        final String purgeKey;
        final String purgeMode;
        final String submittedBy;
        final String esQuery;
        final long submittedAt;
        final boolean deleteConnection;

        volatile String status;
        volatile String error;
        volatile long lastHeartbeat;
        volatile long totalDiscovered;
        volatile int workerCount;
        volatile int lastProcessedBatchIndex;
        volatile boolean cancelRequested;
        volatile boolean connectionDeleted;

        final AtomicInteger totalDeleted = new AtomicInteger(0);
        final AtomicInteger totalFailed = new AtomicInteger(0);
        final AtomicInteger completedBatches = new AtomicInteger(0);
        final Set<String> externalLineageVertexIds = ConcurrentHashMap.newKeySet();

        PurgeContext(String requestId, String purgeKey, String purgeMode, String submittedBy, String esQuery, boolean deleteConnection) {
            this.requestId = requestId;
            this.purgeKey = purgeKey;
            this.purgeMode = purgeMode;
            this.submittedBy = submittedBy;
            this.esQuery = esQuery;
            this.submittedAt = System.currentTimeMillis();
            this.deleteConnection = deleteConnection;
        }

        String toJson() {
            try {
                return MAPPER.writeValueAsString(toStatusMap());
            } catch (Exception e) {
                return "{}";
            }
        }

        Map<String, Object> toStatusMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("requestId", requestId);
            map.put("purgeKey", purgeKey);
            map.put("purgeMode", purgeMode);
            map.put("status", status);
            map.put("submittedBy", submittedBy);
            map.put("submittedAt", submittedAt);
            map.put("totalDiscovered", totalDiscovered);
            map.put("deletedCount", totalDeleted.get());
            map.put("failedCount", totalFailed.get());
            map.put("completedBatches", completedBatches.get());
            map.put("lastProcessedBatchIndex", lastProcessedBatchIndex);
            map.put("lastHeartbeat", lastHeartbeat);
            map.put("workerCount", workerCount);
            map.put("batchSize", AtlasConfiguration.BULK_PURGE_BATCH_SIZE.getInt());
            map.put("deleteConnection", deleteConnection);
            map.put("connectionDeleted", connectionDeleted);
            if (error != null) {
                map.put("error", error);
            }
            return map;
        }
    }

    /**
     * Represents a batch of vertex IDs to delete.
     */
    static class BatchWork {
        static final BatchWork POISON_PILL = new BatchWork(Collections.emptyList(), -1);

        final List<String> vertexIds;
        final int batchIndex;

        BatchWork(List<String> vertexIds, int batchIndex) {
            this.vertexIds = vertexIds;
            this.batchIndex = batchIndex;
        }
    }
}
