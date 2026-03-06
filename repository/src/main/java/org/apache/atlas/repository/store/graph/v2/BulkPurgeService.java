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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.notification.task.AtlasDistributedTaskNotificationSender;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graph.IAtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.model.Tag;
import org.apache.atlas.repository.store.graph.v2.tags.PaginatedTagResult;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.tasks.TaskManagement;
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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_REFRESH_PROPAGATION;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_ENTITY_GUID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_CLASSIFICATION_NAME;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;

@Component
public class BulkPurgeService {
    private static final Logger LOG = LoggerFactory.getLogger(BulkPurgeService.class);

    private static final String REDIS_KEY_PREFIX = "bulk_purge:";
    private static final String REDIS_LOCK_PREFIX = "bulk_purge_lock:";
    private static final String REDIS_ACTIVE_PURGE_KEYS = "bulk_purge_active_keys";
    private static final String REDIS_CANCEL_PREFIX = "bulk_purge_cancel:";
    private static final String PURGE_MODE_CONNECTION = "CONNECTION";
    private static final String PURGE_MODE_QN_PREFIX = "QUALIFIED_NAME_PREFIX";
    private static final int MIN_QN_PREFIX_LENGTH = 10;
    private static final long BATCH_WARN_THRESHOLD_MS = 30_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String HOSTNAME = getHostname();

    // Internal defaults (not externally configurable — change here if needed)
    private static final int    COORDINATOR_POOL_SIZE           = 4;
    private static final int    ES_PAGE_SIZE                    = 5000;
    private static final int    COMMIT_MAX_RETRIES              = 3;
    private static final long   COMMIT_TIMEOUT_MS               = 120_000;
    private static final long   GET_VERTICES_TIMEOUT_MS         = 60_000;
    private static final int    HEARTBEAT_INTERVAL_MS           = 30_000;
    private static final int    SCROLL_TIMEOUT_MINUTES          = 30;
    private static final long   STALL_THRESHOLD_MS              = 900_000; // 15 minutes
    private static final int    INCREMENTAL_ES_CLEANUP_INTERVAL = 50;
    private static final long   ORPHAN_CHECK_INTERVAL_MS        = 300_000;
    private static final int    ORPHAN_MAX_RESUBMITS            = 3;

    private final AtlasGraph graph;
    private final IAtlasGraphProvider graphProvider;
    private final RedisService redisService;
    private final Set<EntityAuditRepository> auditRepositories;
    private final AtlasDistributedTaskNotificationSender taskNotificationSender;
    private final TaskManagement taskManagement;

    private final ThreadPoolExecutor coordinatorExecutor;

    // Dedicated executor for commit/getVertices timeout enforcement
    private final ExecutorService timeoutExecutor;

    // Lazily initialized ES client (cached for reuse across calls)
    private volatile RestClient esClient;
    private volatile Boolean tagV2Override; // @VisibleForTesting: override DynamicConfigStore in tests
    private volatile TagDAO tagDAOOverride;  // @VisibleForTesting: override TagDAOCassandraImpl in tests

    // Active purge tracking for cancel support
    private final ConcurrentHashMap<String, PurgeContext> activePurges = new ConcurrentHashMap<>();

    // Orphan checker background thread
    private ScheduledExecutorService orphanCheckerExecutor;

    @Inject
    public BulkPurgeService(AtlasGraph graph,
                            IAtlasGraphProvider graphProvider,
                            RedisService redisService,
                            Set<EntityAuditRepository> auditRepositories,
                            AtlasDistributedTaskNotificationSender taskNotificationSender,
                            TaskManagement taskManagement) {
        this.graph = graph;
        this.graphProvider = graphProvider;
        this.redisService = redisService;
        this.auditRepositories = auditRepositories;
        this.taskNotificationSender = taskNotificationSender;
        this.taskManagement = taskManagement;

        this.coordinatorExecutor = new ThreadPoolExecutor(
                COORDINATOR_POOL_SIZE, COORDINATOR_POOL_SIZE, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(COORDINATOR_POOL_SIZE),  // bounded queue = pool size
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-coordinator-%d")
                        .build());

        this.timeoutExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-timeout-%d")
                        .build());
    }

    @PostConstruct
    private void startOrphanChecker() {
        if (!AtlasConfiguration.BULK_PURGE_ORPHAN_CHECK_ENABLED.getBoolean()) {
            LOG.info("BulkPurge: Orphan checker is disabled");
            return;
        }

        orphanCheckerExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("bulk-purge-orphan-checker").build());

        orphanCheckerExecutor.scheduleAtFixedRate(
                this::checkAndRecoverOrphanedPurges,
                ORPHAN_CHECK_INTERVAL_MS, ORPHAN_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);

        LOG.info("BulkPurge: Orphan checker started with interval={}ms", ORPHAN_CHECK_INTERVAL_MS);
    }

    @PreDestroy
    private void shutdown() {
        LOG.info("BulkPurge: Shutting down — marking {} active purges as FAILED", activePurges.size());

        // Mark all in-memory active purges as FAILED in Redis before shutdown
        for (Map.Entry<String, PurgeContext> entry : activePurges.entrySet()) {
            try {
                PurgeContext ctx = entry.getValue();
                ctx.status = "FAILED";
                ctx.error = "Pod shutdown";
                writeRedisStatus(ctx);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to mark purge {} as FAILED during shutdown", entry.getKey(), e);
            }
        }

        if (orphanCheckerExecutor != null) {
            orphanCheckerExecutor.shutdownNow();
        }
        coordinatorExecutor.shutdownNow();
        timeoutExecutor.shutdownNow();
        try {
            coordinatorExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    @VisibleForTesting
    void setTagV2Override(Boolean tagV2Enabled) {
        this.tagV2Override = tagV2Enabled;
    }

    @VisibleForTesting
    void setTagDAOOverride(TagDAO tagDAO) {
        this.tagDAOOverride = tagDAO;
    }

    private TagDAO getTagDAO() {
        return tagDAOOverride != null ? tagDAOOverride : TagDAOCassandraImpl.getInstance();
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
        return bulkPurgeByConnection(connectionQN, submittedBy, deleteConnection, 0);
    }

    /**
     * Purge all assets belonging to a connection.
     * The Connection entity itself is NOT deleted unless {@code deleteConnection} is true.
     *
     * @param connectionQN       qualifiedName of the connection
     * @param submittedBy        user who submitted the request
     * @param deleteConnection   if true, delete the Connection entity after all child assets are purged
     * @param workerCountOverride if > 0, use this many workers (capped at configuredMax) instead of auto-scaling
     */
    public String bulkPurgeByConnection(String connectionQN, String submittedBy, boolean deleteConnection, int workerCountOverride) throws AtlasBaseException {
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
        return submitPurge(connectionQN, PURGE_MODE_CONNECTION, esQuery, submittedBy, deleteConnection, workerCountOverride);
    }

    /**
     * Purge all assets matching a qualifiedName prefix.
     */
    public String bulkPurgeByQualifiedName(String qualifiedNamePrefix, String submittedBy) throws AtlasBaseException {
        return bulkPurgeByQualifiedName(qualifiedNamePrefix, submittedBy, 0);
    }

    /**
     * Purge all assets matching a qualifiedName prefix.
     *
     * @param qualifiedNamePrefix the qualifiedName prefix to match
     * @param submittedBy         user who submitted the request
     * @param workerCountOverride if > 0, use this many workers (capped at configuredMax) instead of auto-scaling
     */
    public String bulkPurgeByQualifiedName(String qualifiedNamePrefix, String submittedBy, int workerCountOverride) throws AtlasBaseException {
        if (qualifiedNamePrefix == null || qualifiedNamePrefix.length() < MIN_QN_PREFIX_LENGTH) {
            throw new AtlasBaseException("qualifiedName prefix must be at least " + MIN_QN_PREFIX_LENGTH + " characters");
        }

        // ES query: prefix on __qualifiedNameHierarchy (always indexed by JanusGraph)
        String esQuery = buildPrefixQuery(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, qualifiedNamePrefix);
        return submitPurge(qualifiedNamePrefix, PURGE_MODE_QN_PREFIX, esQuery, submittedBy, false, workerCountOverride);
    }

    /**
     * Get the current status of a purge request.
     */
    public Map<String, Object> getStatus(String requestId) {
        // Search active purges first
        for (PurgeContext ctx : activePurges.values()) {
            if (ctx.requestId.equals(requestId)) {
                Map<String, Object> status = ctx.toStatusMap();
                status.put("source", "in-memory");
                status.put("respondingPod", HOSTNAME);
                return status;
            }
        }

        // Check Redis for completed/failed purges
        String redisValue = redisService.getValue("bulk_purge_request:" + requestId);
        if (redisValue != null) {
            try {
                Map<String, Object> status = MAPPER.readValue(redisValue, Map.class);
                status.put("source", "redis");
                status.put("respondingPod", HOSTNAME);
                return status;
            } catch (Exception e) {
                LOG.warn("Failed to parse purge status from Redis for requestId={}", requestId, e);
            }
        }

        return null;
    }

    /**
     * Cancel an in-progress purge. Interrupts worker threads if they are blocked.
     */
    /**
     * Cancel a purge by requestId. Writes cancel signal to Redis first (durable,
     * works cross-pod), then attempts local in-memory cancel for instant effect
     * if this pod happens to be running the purge.
     */
    public boolean cancelPurge(String requestId) {
        // Look up purgeKey from Redis — this is the durable source of truth
        String redisValue = redisService.getValue("bulk_purge_request:" + requestId);
        if (redisValue == null) {
            return false;
        }

        try {
            JsonNode status = MAPPER.readTree(redisValue);
            String purgeKey = status.has("purgeKey") ? status.get("purgeKey").asText() : null;
            if (purgeKey == null) {
                return false;
            }

            // 1. Redis first — ensures durability even if this pod crashes
            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            redisService.putValue(REDIS_CANCEL_PREFIX + purgeKey, "CANCEL_REQUESTED", redisTtl);
            LOG.info("BulkPurge: Cancel signal written to Redis for requestId={}, purgeKey={}", requestId, purgeKey);

            // 2. Local cancel — instant effect if this pod is running the purge
            for (PurgeContext ctx : activePurges.values()) {
                if (ctx.requestId.equals(requestId)) {
                    ctx.cancelRequested = true;
                    interruptWorkers(ctx);
                    LOG.info("BulkPurge: Local cancel applied for requestId={}, purgeKey={}", requestId, purgeKey);
                    break;
                }
            }

            return true;
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to cancel purge for requestId={}", requestId, e);
            return false;
        }
    }

    /**
     * Get system-level information about the bulk purge coordinator.
     */
    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("activeCoordinators", coordinatorExecutor.getActiveCount());
        status.put("queuedCoordinators", coordinatorExecutor.getQueue().size());
        status.put("maxCoordinators", coordinatorExecutor.getMaximumPoolSize());
        status.put("activePurges", activePurges.size());
        status.put("respondingPod", HOSTNAME);

        List<Map<String, Object>> purgeList = new ArrayList<>();
        for (PurgeContext ctx : activePurges.values()) {
            Map<String, Object> purgeInfo = new LinkedHashMap<>();
            purgeInfo.put("requestId", ctx.requestId);
            purgeInfo.put("purgeKey", ctx.purgeKey);
            purgeInfo.put("status", ctx.status);
            purgeInfo.put("currentPhase", ctx.currentPhase);
            purgeInfo.put("deletedCount", ctx.totalDeleted.get());
            purgeInfo.put("completedBatches", ctx.completedBatches.get());
            purgeList.add(purgeInfo);
        }
        status.put("purges", purgeList);

        return status;
    }

    // ======================== PRIVATE METHODS ========================

    private String submitPurge(String purgeKey, String purgeMode, String esQuery, String submittedBy, boolean deleteConnection, int workerCountOverride) throws AtlasBaseException {
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
        PurgeContext ctx = new PurgeContext(requestId, purgeKey, purgeMode, submittedBy, esQuery, deleteConnection, workerCountOverride);
        ctx.status = "PENDING";
        writeRedisStatus(ctx);

        // Store requestId -> purgeKey mapping for status lookup
        int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
        redisService.putValue("bulk_purge_request:" + requestId, ctx.toJson(), redisTtl);

        activePurges.put(purgeKey, ctx);
        addToActivePurgeKeys(purgeKey);

        // Submit to coordinator executor (bounded queue — rejects if at capacity)
        try {
            coordinatorExecutor.submit(() -> executePurge(ctx, lockKey, redisKey));
        } catch (RejectedExecutionException e) {
            activePurges.remove(purgeKey);
            removeFromActivePurgeKeys(purgeKey);
            ctx.status = "FAILED";
            ctx.error = "System at capacity — all coordinator slots are busy. Retry later.";
            writeRedisStatus(ctx);
            throw new AtlasBaseException("Bulk purge system at capacity (" +
                    coordinatorExecutor.getMaximumPoolSize() + " concurrent purges). Please retry later.");
        }

        LOG.info("BulkPurge: Submitted requestId={}, purgeKey={}, purgeMode={}, submittedBy={}",
                requestId, purgeKey, purgeMode, submittedBy);

        return requestId;
    }

    private void executePurge(PurgeContext ctx, String lockKey, String redisKey) {
        ScheduledExecutorService heartbeatExecutor = null;

        try {
            // Acquire distributed lock (retry for orphan recovery to handle watchdog race)
            boolean lockAcquired = false;
            int lockRetries = ctx.orphanRecovery ? 3 : 1;
            for (int attempt = 1; attempt <= lockRetries; attempt++) {
                lockAcquired = redisService.acquireDistributedLock(lockKey);
                if (lockAcquired) break;
                if (attempt < lockRetries) {
                    LOG.info("BulkPurge: Lock not available for {} (attempt {}/{}), retrying in 2s...",
                            ctx.purgeKey, attempt, lockRetries);
                    Thread.sleep(2000);
                }
            }
            if (!lockAcquired) {
                LOG.warn("BulkPurge: Could not acquire lock for {}", ctx.purgeKey);
                ctx.status = "FAILED";
                ctx.error = "Could not acquire distributed lock";
                writeRedisStatus(ctx);
                return;
            }

            ctx.status = "RUNNING";
            if (tagV2Override != null) {
                ctx.isTagV2 = tagV2Override;
            } else {
                try {
                    ctx.isTagV2 = DynamicConfigStore.isTagV2Enabled();
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Could not determine Tag V2 status, defaulting to false", e);
                    ctx.isTagV2 = false;
                }
            }
            ctx.lastHeartbeat = System.currentTimeMillis();
            writeRedisStatus(ctx);

            // Start heartbeat with stall detection (P7) and force-cancel check (P3)
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("bulk-purge-heartbeat-" + ctx.purgeKey).build());
            heartbeatExecutor.scheduleAtFixedRate(() -> {
                try {
                    ctx.lastHeartbeat = System.currentTimeMillis();

                    // P7: Progress-based stall detection
                    long lastProgress = ctx.lastProgressTimestamp.get();
                    if (lastProgress > 0 && (System.currentTimeMillis() - lastProgress) > STALL_THRESHOLD_MS) {
                        LOG.error("BulkPurge: STALLED — no progress for {}ms, purgeKey={}, completedBatches={}",
                                System.currentTimeMillis() - lastProgress, ctx.purgeKey, ctx.completedBatches.get());
                        ctx.stalled = true;
                    }

                    // P3: Check Redis for force-cancel signal from another pod
                    String cancelSignal = redisService.getValue(REDIS_CANCEL_PREFIX + ctx.purgeKey);
                    if ("CANCEL_REQUESTED".equals(cancelSignal)) {
                        LOG.info("BulkPurge: Force-cancel signal detected via Redis for purgeKey={}", ctx.purgeKey);
                        ctx.cancelRequested = true;
                        interruptWorkers(ctx);
                        redisService.removeValue(REDIS_CANCEL_PREFIX + ctx.purgeKey);
                    }

                    writeRedisStatus(ctx);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Heartbeat failed for {}", ctx.purgeKey, e);
                }
            }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

            long startTime = System.currentTimeMillis();
            ctx.currentPhase = "VERTEX_DELETION";
            writeRedisStatus(ctx);
            streamAndDelete(ctx);

            if (ctx.cancelRequested) {
                ctx.status = "CANCELLED";
                LOG.info("BulkPurge: Cancelled for purgeKey={}, deleted={}, failed={}",
                        ctx.purgeKey, ctx.totalDeleted.get(), ctx.totalFailed.get());
            } else {
                long phaseStart = System.currentTimeMillis();

                ctx.currentPhase = "ES_CLEANUP";
                writeRedisStatus(ctx);

                // Retry ES cleanup up to 3 times if entities remain after delete_by_query.
                // The first attempt may fail due to transient ES issues (circuit breaker,
                // concurrent load, shard relocation) — the exception is caught inside esCleanup().
                int esCleanupMaxRetries = 3;
                long remainingAfterEsCleanup = -1;
                for (int esAttempt = 1; esAttempt <= esCleanupMaxRetries; esAttempt++) {
                    esCleanup(ctx);
                    LOG.info("BulkPurge: ES cleanup attempt {}/{} completed in {}ms for purgeKey={}",
                            esAttempt, esCleanupMaxRetries, System.currentTimeMillis() - phaseStart, ctx.purgeKey);

                    try {
                        remainingAfterEsCleanup = getEntityCount(ctx.esQuery);
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: ES verification count failed on attempt {} for purgeKey={}",
                                esAttempt, ctx.purgeKey, e);
                        remainingAfterEsCleanup = -1;
                    }

                    if (remainingAfterEsCleanup <= 0) {
                        break;
                    }

                    if (esAttempt < esCleanupMaxRetries) {
                        LOG.warn("BulkPurge: {} entities remain after ES cleanup attempt {} for purgeKey={}, retrying...",
                                remainingAfterEsCleanup, esAttempt, ctx.purgeKey);
                        try { Thread.sleep(2000L * esAttempt); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }

                ctx.currentPhase = "ES_VERIFICATION";
                writeRedisStatus(ctx);
                ctx.remainingAfterCleanup = Math.max(0, remainingAfterEsCleanup);
                if (remainingAfterEsCleanup > 0) {
                    LOG.error("BulkPurge: {} entities remain after all ES cleanup attempts for purgeKey={}",
                            remainingAfterEsCleanup, ctx.purgeKey);
                    ctx.error = remainingAfterEsCleanup + " entities remain after cleanup";
                } else {
                    LOG.info("BulkPurge: Verification passed — 0 entities remaining for purgeKey={}", ctx.purgeKey);
                }

                phaseStart = System.currentTimeMillis();
                ctx.currentPhase = "LINEAGE_REPAIR";
                writeRedisStatus(ctx);
                repairExternalLineage(ctx);
                LOG.info("BulkPurge: Phase LINEAGE_REPAIR completed in {}ms for purgeKey={}",
                        System.currentTimeMillis() - phaseStart, ctx.purgeKey);

                phaseStart = System.currentTimeMillis();
                ctx.currentPhase = "TAG_CLEANUP_SOURCES";
                writeRedisStatus(ctx);
                cleanPropagatedTagsFromDeletedSources(ctx);
                LOG.info("BulkPurge: Phase TAG_CLEANUP_SOURCES completed in {}ms for purgeKey={}",
                        System.currentTimeMillis() - phaseStart, ctx.purgeKey);

                phaseStart = System.currentTimeMillis();
                ctx.currentPhase = "TAG_CLEANUP_EXTERNAL";
                writeRedisStatus(ctx);
                repairExternalPropagatedClassifications(ctx);
                LOG.info("BulkPurge: Phase TAG_CLEANUP_EXTERNAL completed in {}ms for purgeKey={}",
                        System.currentTimeMillis() - phaseStart, ctx.purgeKey);

                phaseStart = System.currentTimeMillis();
                ctx.currentPhase = "RELAY_PROPAGATION_REFRESH";
                writeRedisStatus(ctx);
                triggerRelayPropagationRefresh(ctx);
                LOG.info("BulkPurge: Phase RELAY_PROPAGATION_REFRESH completed in {}ms for purgeKey={}",
                        System.currentTimeMillis() - phaseStart, ctx.purgeKey);

                if (ctx.deleteConnection && PURGE_MODE_CONNECTION.equals(ctx.purgeMode)) {
                    ctx.currentPhase = "DELETE_CONNECTION";
                    writeRedisStatus(ctx);
                    deleteConnectionVertex(ctx);
                }

                ctx.currentPhase = "AUDIT";
                writeSummaryAuditEvent(ctx, startTime);

                ctx.currentPhase = null;
                ctx.status = "COMPLETED";
                long duration = System.currentTimeMillis() - startTime;
                LOG.info("BulkPurge: Completed for purgeKey={}, deleted={}, failed={}, remaining={}, duration={}ms",
                        ctx.purgeKey, ctx.totalDeleted.get(), ctx.totalFailed.get(), ctx.remainingAfterCleanup, duration);
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
            if ("COMPLETED".equals(ctx.status) || "CANCELLED".equals(ctx.status) || "FAILED".equals(ctx.status)) {
                removeFromActivePurgeKeys(ctx.purgeKey);
            }
            // Clean up any stale cancel signal so a re-triggered purge for the
            // same purgeKey does not get immediately cancelled.
            try {
                redisService.removeValue(REDIS_CANCEL_PREFIX + ctx.purgeKey);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to remove cancel signal for {}", ctx.purgeKey, e);
            }
            try {
                redisService.releaseDistributedLock(lockKey);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to release lock for {}", ctx.purgeKey, e);
            }
        }
    }

    /**
     * Phase 1+2: Stream ES scroll results and delete vertices in parallel.
     * Uses a bulk-loading graph (storage.batch-loading=true) to disable the
     * ConsistentKeyLocker and eliminate lock contention between workers.
     */
    private void streamAndDelete(PurgeContext ctx) throws Exception {
        int batchSize = AtlasConfiguration.BULK_PURGE_BATCH_SIZE.getInt();

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

        // Determine worker count: use override if provided, otherwise auto-scale
        int workerCount;
        if (ctx.workerCountOverride > 0) {
            workerCount = Math.min(ctx.workerCountOverride, configuredWorkerCount);
            LOG.info("BulkPurge: Using worker count override={} (capped to {}) for purgeKey={}",
                    ctx.workerCountOverride, workerCount, ctx.purgeKey);
        } else {
            workerCount = getWorkerCount(totalEntities, configuredWorkerCount);
        }
        ctx.workerCount = workerCount;
        writeRedisStatus(ctx);

        // Create batch queue with backpressure
        BlockingQueue<BatchWork> batchQueue = new LinkedBlockingQueue<>(workerCount * 2);

        // Create worker pool — stored in PurgeContext for cancel/interrupt support (P2)
        ExecutorService workerPool = Executors.newFixedThreadPool(workerCount,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-worker-%d-" + ctx.purgeKey)
                        .build());
        ctx.workerPool = workerPool;

        // Each worker gets its own bulk-loading graph to avoid ConcurrentModificationException.
        // JanusGraph's internal commit uses non-thread-safe HashMaps, so sharing one graph
        // across workers causes corruption when multiple workers commit concurrently.
        List<AtlasGraph> workerGraphs = new ArrayList<>(workerCount);

        // Start workers — each creates its own graph instance
        List<Future<?>> workerFutures = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            AtlasGraph workerGraph = getBulkLoadingGraph();
            workerGraphs.add(workerGraph);
            LOG.info("BulkPurge: Created bulk-loading graph for worker {} (purgeKey={})", i, ctx.purgeKey);
            workerFutures.add(workerPool.submit(() -> workerLoop(ctx, batchQueue, workerGraph)));
        }
        ctx.workerFutures = workerFutures;

        try {
            // Stream ES scroll into queue (coordinator role)
            streamESScrollIntoBatchQueue(ctx, batchQueue, batchSize);
        } finally {
            // Only clear the queue on cancel/interrupt — in the normal path, workers
            // must drain remaining batches before seeing their poison pills.
            // Without this guard, batchQueue.clear() discards unconsumed batches
            // whose entities never get deleted from graph (counter under-reports too).
            if (ctx.cancelRequested || Thread.currentThread().isInterrupted()) {
                batchQueue.clear();
            }

            for (int i = 0; i < workerCount; i++) {
                // Use offer with timeout instead of put to avoid blocking forever
                // if workers already exited (cancel path) and queue is full.
                if (!batchQueue.offer(BatchWork.POISON_PILL, 30, TimeUnit.SECONDS)) {
                    LOG.warn("BulkPurge: Timed out enqueuing poison pill for worker {} (purgeKey={}). " +
                            "Queue may be full with unconsumed batches.", i, ctx.purgeKey);
                    // Force clear and retry — workers are likely dead
                    batchQueue.clear();
                    batchQueue.put(BatchWork.POISON_PILL);
                }
            }

            for (Future<?> f : workerFutures) {
                try {
                    f.get(2, TimeUnit.MINUTES);
                } catch (CancellationException e) {
                    // Expected when interruptWorkers() cancelled this future
                } catch (TimeoutException e) {
                    LOG.warn("BulkPurge: Worker timed out during shutdown for purgeKey={}", ctx.purgeKey);
                } catch (ExecutionException e) {
                    LOG.error("BulkPurge: Worker failed for purgeKey={}", ctx.purgeKey, e.getCause());
                }
            }

            workerPool.shutdown();
            workerPool.awaitTermination(1, TimeUnit.MINUTES);

            // Shutdown all worker graphs
            for (int i = 0; i < workerGraphs.size(); i++) {
                try {
                    workerGraphs.get(i).shutdown();
                    LOG.info("BulkPurge: Bulk-loading graph shut down for worker {} (purgeKey={})", i, ctx.purgeKey);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to shut down bulk-loading graph for worker {} (purgeKey={})", i, ctx.purgeKey, e);
                }
            }
        }
    }

    @VisibleForTesting
    AtlasGraph getBulkLoadingGraph() throws Exception {
        return graphProvider.getBulkLoading();
    }

    /**
     * Worker loop: pulls batches from queue until poison pill received.
     * Checks both cancelRequested flag and Thread.interrupted() for responsive cancellation (P2).
     */
    private void workerLoop(PurgeContext ctx, BlockingQueue<BatchWork> queue, AtlasGraph workerGraph) {
        while (!ctx.cancelRequested && !Thread.currentThread().isInterrupted()) {
            try {
                BatchWork work = queue.poll(5, TimeUnit.SECONDS);
                if (work == null) continue;
                if (work == BatchWork.POISON_PILL) break;

                processBatch(ctx, work, workerGraph);
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
     * Uses batch vertex retrieval (single round-trip) and relies on
     * JanusGraph's AbstractVertex.remove() to handle edge cleanup internally.
     */
    private void processBatch(PurgeContext ctx, BatchWork work, AtlasGraph workerGraph) {
        long batchStartTime = System.nanoTime();
        int batchDeleted = 0;
        int batchFailed = 0;

        // P0: Timing for getVertices
        long t0 = System.nanoTime();

        // P1: getVertices with timeout
        String[] ids = work.vertexIds.toArray(new String[0]);
        Set<AtlasVertex> vertexSet;
        Future<Set<AtlasVertex>> getVerticesFuture = null;
        try {
            getVerticesFuture = timeoutExecutor.submit(() -> workerGraph.getVertices(ids));
            vertexSet = getVerticesFuture.get(GET_VERTICES_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOG.error("BulkPurge: getVertices TIMED OUT for batch {} (purgeKey={})", work.batchIndex, ctx.purgeKey);
            if (getVerticesFuture != null) getVerticesFuture.cancel(true);
            ctx.totalFailed.addAndGet(work.vertexIds.size());
            ctx.completedBatches.incrementAndGet();
            return;
        } catch (InterruptedException e) {
            if (getVerticesFuture != null) getVerticesFuture.cancel(true);
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            LOG.error("BulkPurge: getVertices FAILED for batch {} (purgeKey={})", work.batchIndex, ctx.purgeKey, e.getCause());
            ctx.totalFailed.addAndGet(work.vertexIds.size());
            ctx.completedBatches.incrementAndGet();
            return;
        }

        long getVerticesMs = (System.nanoTime() - t0) / 1_000_000;

        Map<String, AtlasVertex> vertexMap = new HashMap<>(vertexSet.size());
        for (AtlasVertex v : vertexSet) {
            vertexMap.put(v.getId().toString(), v);
        }

        // P0: Timing for removeVertex loop
        long t1 = System.nanoTime();
        long collectLineageNs = 0;

        for (String vertexId : work.vertexIds) {
            if (ctx.cancelRequested || Thread.currentThread().isInterrupted()) break;

            try {
                AtlasVertex vertex = vertexMap.get(vertexId);
                if (vertex == null) {
                    continue; // Already deleted (idempotent)
                }

                Boolean vertexHasLineage = vertex.getProperty(HAS_LINEAGE, Boolean.class);
                if (Boolean.TRUE.equals(vertexHasLineage)) {
                    long lineageT0 = System.nanoTime();
                    collectExternalLineageVertices(ctx, vertex);
                    collectLineageNs += (System.nanoTime() - lineageT0);
                }

                if (ctx.isTagV2) {
                    List<String> traitNames = vertex.getMultiValuedProperty(TRAIT_NAMES_PROPERTY_KEY, String.class);
                    if (traitNames != null && !traitNames.isEmpty()) {
                        ctx.entitiesWithDirectTags.add(vertexId);
                    }
                }

                workerGraph.removeVertex(vertex);
                batchDeleted++;
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to remove vertex {}", vertexId, e);
                batchFailed++;
            }
        }

        long removeVertexMs = (System.nanoTime() - t1) / 1_000_000;

        // P0: Timing for commit
        long t2 = System.nanoTime();
        boolean committed = commitWithRetry(workerGraph, work.batchIndex);
        long commitMs = (System.nanoTime() - t2) / 1_000_000;

        if (committed) {
            ctx.totalDeleted.addAndGet(batchDeleted);
            ctx.totalFailed.addAndGet(batchFailed);
        } else {
            ctx.totalFailed.addAndGet(work.vertexIds.size());
            try {
                workerGraph.rollback();
            } catch (Exception e) {
                LOG.warn("BulkPurge: Rollback failed for batch {}", work.batchIndex, e);
            }
        }

        int batches = ctx.completedBatches.incrementAndGet();
        ctx.lastProcessedBatchIndex.accumulateAndGet(work.batchIndex, Math::max);

        // P7: Update progress timestamp for stall detection
        ctx.lastProgressTimestamp.set(System.currentTimeMillis());

        long totalBatchMs = (System.nanoTime() - batchStartTime) / 1_000_000;

        // P0: Log timing — WARN if batch exceeds threshold
        if (totalBatchMs > BATCH_WARN_THRESHOLD_MS) {
            LOG.warn("BulkPurge: SLOW batch {} for purgeKey={}: total={}ms, getVertices={}ms, removeVertex={}ms, " +
                            "collectLineage={}ms, commit={}ms, vertices={}",
                    work.batchIndex, ctx.purgeKey, totalBatchMs, getVerticesMs, removeVertexMs,
                    collectLineageNs / 1_000_000, commitMs, work.vertexIds.size());
        } else if (batches % 10 == 0) {
            LOG.info("BulkPurge: Progress purgeKey={}, batches={}, deleted={}, failed={}, " +
                            "lastBatch: total={}ms, getVertices={}ms, removeVertex={}ms, commit={}ms",
                    ctx.purgeKey, batches, ctx.totalDeleted.get(), ctx.totalFailed.get(),
                    totalBatchMs, getVerticesMs, removeVertexMs, commitMs);
        }

        // Periodic Redis status update
        if (batches % 10 == 0) {
            writeRedisStatus(ctx);
        }

        // P6: Incremental ES cleanup every N committed batches
        if (INCREMENTAL_ES_CLEANUP_INTERVAL > 0 && batches % INCREMENTAL_ES_CLEANUP_INTERVAL == 0 && committed) {
            triggerIncrementalEsCleanup(ctx);
        }
    }

    private boolean commitWithRetry(AtlasGraph workerGraph, int batchIndex) {
        for (int attempt = 1; attempt <= COMMIT_MAX_RETRIES; attempt++) {
            Future<?> commitFuture = null;
            try {
                // P1: Commit with timeout to prevent indefinite blocking
                commitFuture = timeoutExecutor.submit(() -> workerGraph.commit());
                commitFuture.get(COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return true;
            } catch (TimeoutException e) {
                LOG.error("BulkPurge: Commit TIMED OUT for batch {} (attempt {}/{}, timeout={}ms)",
                        batchIndex, attempt, COMMIT_MAX_RETRIES, COMMIT_TIMEOUT_MS);
                // Cancel the still-running commit to prevent race with rollback
                if (commitFuture != null) commitFuture.cancel(true);
                // Don't retry on timeout — Cassandra is likely under pressure
                return false;
            } catch (InterruptedException e) {
                if (commitFuture != null) commitFuture.cancel(true);
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                LOG.warn("BulkPurge: Commit failed for batch {} (attempt {}/{})", batchIndex, attempt, COMMIT_MAX_RETRIES, e.getCause());
                if (attempt < COMMIT_MAX_RETRIES) {
                    try {
                        Thread.sleep((long) Math.pow(2, attempt - 1) * 500); // 500ms, 1s, 2s
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Commit failed for batch {} (attempt {}/{})", batchIndex, attempt, COMMIT_MAX_RETRIES, e);
                if (attempt < COMMIT_MAX_RETRIES) {
                    try {
                        Thread.sleep((long) Math.pow(2, attempt - 1) * 500);
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

                    if (isExternalVertex(ctx, other)) {
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

    /**
     * Determine if a vertex is external to the purge scope.
     *
     * In CONNECTION mode: external means the vertex belongs to a different connection.
     * In QN_PREFIX mode: external means the vertex's qualifiedName does NOT start with
     * the purge prefix — it could be in the same connection but outside the prefix scope.
     */
    private boolean isExternalVertex(PurgeContext ctx, AtlasVertex other) {
        if (PURGE_MODE_CONNECTION.equals(ctx.purgeMode)) {
            String otherConnQN = other.getProperty(CONNECTION_QUALIFIED_NAME, String.class);
            return otherConnQN != null && !otherConnQN.equals(ctx.purgeKey);
        } else {
            // QN_PREFIX mode: check if the vertex's QN hierarchy falls outside the purge prefix
            String otherQNH = other.getProperty(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, String.class);
            if (otherQNH == null) {
                // Can't determine — treat as external to be safe
                return true;
            }
            // The ES query uses purgeKey as prefix. For CONNECTION mode the prefix has a trailing "/",
            // but for QN_PREFIX mode it may not. Use startsWith on the raw purgeKey.
            return !otherQNH.startsWith(ctx.purgeKey);
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
                                              int batchSize) throws Exception {
        RestClient esClient = getEsClient();
        String scrollTimeout = SCROLL_TIMEOUT_MINUTES + "m";

        // Build scroll request — only fetch _id field (vertex ID)
        String scrollQuery = buildScrollQuery(ctx.esQuery, ES_PAGE_SIZE);

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
     * Uses wait_for_completion=false for large purges to avoid blocking on a
     * slow single-node ES. Falls back to synchronous mode if async submission fails.
     */
    private void esCleanup(PurgeContext ctx) {
        try {
            RestClient esClient = getEsClient();

            if (ctx.totalDiscovered > 10_000) {
                // Large purge: run async to avoid blocking the coordinator thread
                esCleanupAsync(esClient, ctx);
            } else {
                // Small purge: synchronous is fine
                esCleanupSync(esClient, ctx);
            }
        } catch (Exception e) {
            LOG.error("BulkPurge: ES cleanup failed for purgeKey={}. Manual cleanup may be needed.", ctx.purgeKey, e);
        }
    }

    private void esCleanupSync(RestClient esClient, PurgeContext ctx) throws Exception {
        String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&refresh=true";

        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(ctx.esQuery, ContentType.APPLICATION_JSON));

        Response response = esClient.performRequest(request);
        String responseBody = readResponseBody(response);
        LOG.info("BulkPurge: ES cleanup completed (sync) for purgeKey={}, response={}", ctx.purgeKey, responseBody);
    }

    private void esCleanupAsync(RestClient esClient, PurgeContext ctx) throws Exception {
        // Submit as async task — ES returns immediately with a task ID
        String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&wait_for_completion=false";

        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(ctx.esQuery, ContentType.APPLICATION_JSON));

        Response response = esClient.performRequest(request);
        String responseBody = readResponseBody(response);
        JsonNode root = MAPPER.readTree(responseBody);

        String taskId = root.has("task") ? root.get("task").asText() : null;
        if (taskId == null) {
            LOG.warn("BulkPurge: ES delete_by_query did not return task ID, response={}", responseBody);
            return;
        }

        LOG.info("BulkPurge: ES cleanup submitted as async task={} for purgeKey={}", taskId, ctx.purgeKey);

        // Poll task status until complete (with timeout)
        long maxWaitMs = 5 * 60 * 1000; // 5 minutes max
        long deadline = System.currentTimeMillis() + maxWaitMs;
        int pollIntervalMs = 2000;

        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(pollIntervalMs);

            Request taskRequest = new Request("GET", "/_tasks/" + taskId);
            Response taskResponse = esClient.performRequest(taskRequest);
            String taskBody = readResponseBody(taskResponse);
            JsonNode taskNode = MAPPER.readTree(taskBody);

            boolean completed = taskNode.has("completed") && taskNode.get("completed").asBoolean();
            if (completed) {
                JsonNode taskResponseNode = taskNode.path("response");
                long deleted = taskResponseNode.path("deleted").asLong();
                long failures = taskResponseNode.path("failures").size();
                LOG.info("BulkPurge: ES cleanup completed (async) for purgeKey={}, deleted={}, failures={}",
                        ctx.purgeKey, deleted, failures);

                // Refresh index so subsequent _count verification sees the deletions
                refreshEsIndex(esClient);
                return;
            }

            // Log progress
            JsonNode taskStatus = taskNode.path("task").path("status");
            long esDeleted = taskStatus.path("deleted").asLong();
            long esTotal = taskStatus.path("total").asLong();
            LOG.debug("BulkPurge: ES cleanup in progress for purgeKey={}, deleted={}/{}",
                    ctx.purgeKey, esDeleted, esTotal);
        }

        LOG.warn("BulkPurge: ES cleanup async task {} did not complete within {}ms for purgeKey={}. " +
                "Task continues in background.", taskId, maxWaitMs, ctx.purgeKey);
    }

    private void refreshEsIndex(RestClient esClient) {
        try {
            Request refreshRequest = new Request("POST", "/" + VERTEX_INDEX_NAME + "/_refresh");
            esClient.performRequest(refreshRequest);
        } catch (Exception e) {
            LOG.warn("BulkPurge: ES index refresh failed (verification count may be stale)", e);
        }
    }

    /**
     * P6: Fire an async ES _delete_by_query to clean up already-committed batches incrementally.
     * Non-blocking, idempotent (conflicts=proceed), runs in background.
     */
    private void triggerIncrementalEsCleanup(PurgeContext ctx) {
        try {
            RestClient esClient = getEsClient();
            String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&wait_for_completion=false";

            Request request = new Request("POST", endpoint);
            request.setEntity(new NStringEntity(ctx.esQuery, ContentType.APPLICATION_JSON));

            Response response = esClient.performRequest(request);
            String responseBody = readResponseBody(response);
            LOG.info("BulkPurge: Incremental ES cleanup submitted for purgeKey={}, batches={}, response={}",
                    ctx.purgeKey, ctx.completedBatches.get(), responseBody);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Incremental ES cleanup failed for purgeKey={}", ctx.purgeKey, e);
        }
    }

    /**
     * P2: Interrupt worker threads to unblock them from stuck operations.
     */
    private void interruptWorkers(PurgeContext ctx) {
        ExecutorService pool = ctx.workerPool;
        List<Future<?>> futures = ctx.workerFutures;

        if (pool != null) {
            pool.shutdownNow();
        }
        if (futures != null) {
            for (Future<?> f : futures) {
                f.cancel(true);
            }
        }
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
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

            // Capture the GUID before deleting the graph vertex — needed for ES cleanup
            String connGuid = connVertex.getProperty(GUID_PROPERTY_KEY, String.class);

            // removeVertex() handles edge removal internally via JanusGraph's
            // AbstractVertex.remove() — no need for explicit edge iteration
            graph.removeVertex(connVertex);
            graph.commit();

            ctx.connectionDeleted = true;
            ctx.totalDeleted.incrementAndGet();

            LOG.info("BulkPurge: Connection entity deleted for purgeKey={}", ctx.purgeKey);

            // Delete the Connection entity's ES document.
            // The main esCleanup uses prefix "connectionQN/" which only matches children,
            // not the Connection entity itself (its __qualifiedNameHierarchy has no trailing "/").
            deleteConnectionFromES(ctx, connGuid);
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

    private void deleteConnectionFromES(PurgeContext ctx, String connGuid) {
        try {
            RestClient esClient = getEsClient();
            String query = buildTermQuery(GUID_PROPERTY_KEY, connGuid);
            String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&refresh=true";

            Request request = new Request("POST", endpoint);
            request.setEntity(new NStringEntity(query, ContentType.APPLICATION_JSON));

            Response response = esClient.performRequest(request);
            String responseBody = readResponseBody(response);
            LOG.info("BulkPurge: Connection ES document deleted for purgeKey={}, guid={}, response={}",
                    ctx.purgeKey, connGuid, responseBody);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to delete Connection ES document for purgeKey={}, guid={}. " +
                    "Document will be orphaned in ES.", ctx.purgeKey, connGuid, e);
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
     * Phase 3c-2: Clean stale propagated classifications on direct lineage neighbor vertices.
     *
     * For V1 (graph-based tags): Uses classification edges to find stale propagated tags.
     * For V2 (Cassandra-based tags): Uses TagDAO to find stale propagated tags in Cassandra.
     *
     * This acts as a safety net for Phase 3c-1 (which handles cleanup from the source side).
     * Phase 3c-1 handles transitive propagation via Cassandra; this method handles direct
     * neighbors and catches any entities missed by Phase 3c-1.
     */
    private void repairExternalPropagatedClassifications(PurgeContext ctx) {
        if (ctx.externalLineageVertexIds.isEmpty()) {
            LOG.info("BulkPurge: No external vertices to check for stale propagated classifications for purgeKey={}", ctx.purgeKey);
            return;
        }

        LOG.info("BulkPurge: Checking {} external vertices for stale propagated classifications for purgeKey={}",
                ctx.externalLineageVertexIds.size(), ctx.purgeKey);

        TagDAO tagDAO = ctx.isTagV2 ? getTagDAO() : null;
        int cleaned = 0;
        int errors = 0;

        for (String vertexId : ctx.externalLineageVertexIds) {
            try {
                AtlasVertex vertex = graph.getVertex(vertexId);
                if (vertex == null) {
                    continue;
                }

                if (ctx.isTagV2) {
                    cleaned += repairPropagatedClassificationsV2(vertex, vertexId, tagDAO);
                } else {
                    cleaned += repairPropagatedClassificationsV1(vertex, vertexId);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to clean propagated classifications for vertex {}", vertexId, e);
                errors++;
                try {
                    graph.rollback();
                } catch (Exception re) { /* ignore */ }
            }
        }

        LOG.info("BulkPurge: Propagated classification cleanup complete for purgeKey={}: cleaned={}, errors={}",
                ctx.purgeKey, cleaned, errors);
    }

    /**
     * V1 path: Clean stale propagated classification edges on an external vertex.
     * Uses graph classification edges to find propagated tags where the source entity no longer exists.
     */
    private int repairPropagatedClassificationsV1(AtlasVertex vertex, String vertexId) {
        List<String> staleClassificationNames = new ArrayList<>();

        Iterable<AtlasEdge> classEdges = vertex.getEdges(AtlasEdgeDirection.OUT, CLASSIFICATION_LABEL);
        List<AtlasEdge> edgesToRemove = new ArrayList<>();

        for (AtlasEdge edge : classEdges) {
            Boolean isPropagated = edge.getProperty(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class);
            if (!Boolean.TRUE.equals(isPropagated)) {
                continue;
            }

            AtlasVertex classificationVertex = edge.getInVertex();
            String sourceEntityGuid = classificationVertex.getProperty(CLASSIFICATION_ENTITY_GUID, String.class);

            if (sourceEntityGuid != null) {
                AtlasVertex sourceEntity = AtlasGraphUtilsV2.findByGuid(graph, sourceEntityGuid);
                if (sourceEntity == null) {
                    String classificationName = edge.getProperty(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class);
                    if (classificationName != null) {
                        staleClassificationNames.add(classificationName);
                    }
                    edgesToRemove.add(edge);
                }
            }
        }

        if (!edgesToRemove.isEmpty()) {
            for (AtlasEdge edge : edgesToRemove) {
                graph.removeEdge(edge);
            }

            for (String staleName : staleClassificationNames) {
                removePropagatedTraitFromVertex(vertex, staleName);
            }

            graph.commit();
            LOG.debug("BulkPurge: Cleaned {} stale propagated classifications (V1) from vertex {}",
                    staleClassificationNames.size(), vertexId);
        }

        return edgesToRemove.isEmpty() ? 0 : 1;
    }

    /**
     * V2 path: Clean stale propagated tags on an external vertex using Cassandra TagDAO.
     * Finds propagated tags where the source entity no longer exists in the graph.
     */
    private int repairPropagatedClassificationsV2(AtlasVertex vertex, String vertexId, TagDAO tagDAO) throws Exception {
        List<Tag> allTags = tagDAO.getAllTagsByVertexId(vertexId);
        if (allTags == null || allTags.isEmpty()) {
            return 0;
        }

        List<Tag> stalePropagatedTags = new ArrayList<>();

        for (Tag tag : allTags) {
            if (!tag.isPropagated()) {
                continue;
            }

            // Check if source entity still exists in the graph
            String sourceVertexId = tag.getSourceVertexId();
            if (sourceVertexId != null) {
                AtlasVertex sourceVertex = graph.getVertex(sourceVertexId);
                if (sourceVertex == null) {
                    stalePropagatedTags.add(tag);
                }
            }
        }

        if (!stalePropagatedTags.isEmpty()) {
            tagDAO.deleteTags(stalePropagatedTags);

            for (Tag tag : stalePropagatedTags) {
                removePropagatedTraitFromVertex(vertex, tag.getTagTypeName());
            }
            graph.commit();

            LOG.debug("BulkPurge: Cleaned {} stale propagated tags (V2) from vertex {}",
                    stalePropagatedTags.size(), vertexId);
        }

        return stalePropagatedTags.size();
    }

    /**
     * Phase 3c-3: Trigger CLASSIFICATION_REFRESH_PROPAGATION tasks for the relay propagation case.
     *
     * Scenario: E(external) → A(purged) → B(external). E has direct tag "PII" that propagated
     * through A to B. After A is deleted, B retains a stale propagated tag from source E.
     * Neither cleanPropagatedTagsFromDeletedSources (source=A, but B's tag has source=E) nor
     * repairPropagatedClassificationsV2 (checks if source exists — E exists) catches this.
     *
     * Fix: For each external vertex, collect propagated tags whose source is still alive.
     * Create CLASSIFICATION_REFRESH_PROPAGATION tasks for each unique (sourceGuid, tagTypeName).
     * The task handler runs full BFS from source, detects broken lineage, and cleans stale tags.
     */
    private void triggerRelayPropagationRefresh(PurgeContext ctx) {
        if (ctx.externalLineageVertexIds.isEmpty()) {
            return;
        }

        TagDAO tagDAO = ctx.isTagV2 ? getTagDAO() : null;

        Set<String> refreshKeys = new HashSet<>();
        List<String[]> tasksToCreate = new ArrayList<>();

        for (String extVertexId : ctx.externalLineageVertexIds) {
            try {
                if (ctx.isTagV2) {
                    collectRelaySourcesV2(extVertexId, tagDAO, refreshKeys, tasksToCreate);
                } else {
                    collectRelaySourcesV1(extVertexId, refreshKeys, tasksToCreate);
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to collect relay sources for vertex {}", extVertexId, e);
            }
        }

        int created = 0;
        for (String[] pair : tasksToCreate) {
            try {
                String entityGuid = pair[0];
                String tagTypeName = pair[1];
                Map<String, Object> taskParams = new HashMap<>();
                taskParams.put(PARAM_ENTITY_GUID, entityGuid);
                taskParams.put(PARAM_CLASSIFICATION_NAME, tagTypeName);

                taskManagement.createTaskV2(
                        CLASSIFICATION_REFRESH_PROPAGATION,
                        ctx.submittedBy,
                        taskParams,
                        tagTypeName,
                        entityGuid
                );
                created++;
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to create refresh task for [{}, {}]", pair[0], pair[1], e);
            }
        }

        if (created > 0) {
            LOG.info("BulkPurge: Created {} CLASSIFICATION_REFRESH_PROPAGATION tasks for relay cleanup, purgeKey={}",
                    created, ctx.purgeKey);
        }
    }

    /**
     * V2: Collect (sourceGuid, tagTypeName) pairs from propagated tags on an external vertex
     * where the source entity still exists (relay case).
     */
    private void collectRelaySourcesV2(String extVertexId, TagDAO tagDAO,
                                        Set<String> refreshKeys, List<String[]> tasksToCreate) throws Exception {
        List<Tag> allTags = tagDAO.getAllTagsByVertexId(extVertexId);
        if (allTags == null || allTags.isEmpty()) {
            return;
        }

        for (Tag tag : allTags) {
            if (!tag.isPropagated()) {
                continue;
            }

            String sourceVertexId = tag.getSourceVertexId();
            if (sourceVertexId == null) {
                continue;
            }

            AtlasVertex sourceVertex = graph.getVertex(sourceVertexId);
            if (sourceVertex == null) {
                continue; // Source deleted — already handled by repairPropagatedClassificationsV2
            }

            String sourceGuid = sourceVertex.getProperty(GUID_PROPERTY_KEY, String.class);
            if (sourceGuid == null) {
                continue;
            }

            String key = sourceGuid + "|" + tag.getTagTypeName();
            if (refreshKeys.add(key)) {
                tasksToCreate.add(new String[]{sourceGuid, tag.getTagTypeName()});
            }
        }
    }

    /**
     * V1: Collect (sourceGuid, tagTypeName) pairs from propagated classification edges on an external vertex
     * where the source entity still exists (relay case).
     */
    private void collectRelaySourcesV1(String extVertexId,
                                        Set<String> refreshKeys, List<String[]> tasksToCreate) {
        AtlasVertex vertex = graph.getVertex(extVertexId);
        if (vertex == null) {
            return;
        }

        Iterable<AtlasEdge> classEdges = vertex.getEdges(AtlasEdgeDirection.OUT, CLASSIFICATION_LABEL);
        for (AtlasEdge edge : classEdges) {
            Boolean isPropagated = edge.getProperty(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class);
            if (!Boolean.TRUE.equals(isPropagated)) {
                continue;
            }

            AtlasVertex classificationVertex = edge.getInVertex();
            String sourceEntityGuid = classificationVertex.getProperty(CLASSIFICATION_ENTITY_GUID, String.class);
            if (sourceEntityGuid == null) {
                continue;
            }

            AtlasVertex sourceEntity = AtlasGraphUtilsV2.findByGuid(graph, sourceEntityGuid);
            if (sourceEntity == null) {
                continue; // Source deleted — already handled by repairPropagatedClassificationsV1
            }

            String classificationName = edge.getProperty(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class);
            if (classificationName == null) {
                continue;
            }

            String key = sourceEntityGuid + "|" + classificationName;
            if (refreshKeys.add(key)) {
                tasksToCreate.add(new String[]{sourceEntityGuid, classificationName});
            }
        }
    }

    /**
     * Phase 3c-1 (V2 only): Clean propagated tags from deleted source entities using Cassandra.
     *
     * When entity A (in purged connection) has direct tag "PII" that propagated via lineage
     * to entities B, C, D (possibly in other connections and at any hop distance), this method:
     * 1. Queries Cassandra's propagated_tags_by_source to find ALL downstream entities
     * 2. Deletes the propagated tag entries from Cassandra
     * 3. Updates graph properties (__propagatedTraitNames, __propagatedClassificationNames) on external vertices
     * 4. Cleans zombie Cassandra entries for the deleted entity itself
     *
     * This uses Cassandra's propagated_tags_by_source index which tracks ALL propagated entities
     * per (source, tagType) — no BFS traversal needed, handles any hop distance.
     */
    private void cleanPropagatedTagsFromDeletedSources(PurgeContext ctx) {
        if (!ctx.isTagV2 || ctx.entitiesWithDirectTags.isEmpty()) {
            return;
        }

        TagDAO tagDAO = getTagDAO();
        int totalCleaned = 0;
        int totalErrors = 0;

        LOG.info("BulkPurge: Cleaning propagated tags from {} deleted source entities for purgeKey={}",
                ctx.entitiesWithDirectTags.size(), ctx.purgeKey);

        for (String deletedVertexId : ctx.entitiesWithDirectTags) {
            try {
                List<Tag> allTags = tagDAO.getAllTagsByVertexId(deletedVertexId);
                if (allTags == null || allTags.isEmpty()) {
                    continue;
                }

                // Process each DIRECT tag that allows propagation removal on delete
                for (Tag tag : allTags) {
                    if (tag.isPropagated()) {
                        continue;
                    }
                    if (!tag.getRemovePropagationsOnEntityDelete()) {
                        continue;
                    }

                    // Page through ALL entities that received this tag (any hop distance)
                    String pagingState = null;
                    List<Tag> propagatedTagsToDelete = new ArrayList<>();
                    List<AtlasVertex> verticesToUpdate = new ArrayList<>();

                    while (true) {
                        PaginatedTagResult result = tagDAO.getPropagationsForAttachmentBatchWithPagination(
                                deletedVertexId, tag.getTagTypeName(), pagingState, 10000);

                        for (Tag propagatedTag : result.getTags()) {
                            propagatedTagsToDelete.add(propagatedTag);

                            // Update graph properties on the receiving vertex (if it still exists in graph)
                            AtlasVertex extVertex = graph.getVertex(propagatedTag.getVertexId());
                            if (extVertex != null) {
                                verticesToUpdate.add(extVertex);
                            }
                        }

                        if (Boolean.TRUE.equals(result.isDone()) || !result.hasMorePages()) {
                            break;
                        }
                        pagingState = result.getPagingState();
                    }

                    // Delete propagated tags from Cassandra (soft-delete tags_by_id + hard-delete propagated_tags_by_source)
                    if (!propagatedTagsToDelete.isEmpty()) {
                        tagDAO.deleteTags(propagatedTagsToDelete);
                    }

                    // Update graph properties on external vertices (batched commits)
                    int batchCount = 0;
                    for (AtlasVertex extVertex : verticesToUpdate) {
                        try {
                            removePropagatedTraitFromVertex(extVertex, tag.getTagTypeName());
                            batchCount++;
                            if (batchCount % 50 == 0) {
                                try {
                                    graph.commit();
                                } catch (Exception commitEx) {
                                    LOG.warn("BulkPurge: Commit failed during propagated tag cleanup for tag {}, attempting rollback", tag.getTagTypeName(), commitEx);
                                    try { graph.rollback(); } catch (Exception rbEx) { LOG.warn("BulkPurge: Rollback also failed", rbEx); }
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn("BulkPurge: Failed to update graph properties on vertex for tag {}", tag.getTagTypeName(), e);
                        }
                    }
                    if (batchCount % 50 != 0 && batchCount > 0) {
                        try {
                            graph.commit();
                        } catch (Exception commitEx) {
                            LOG.warn("BulkPurge: Final commit failed during propagated tag cleanup for tag {}, attempting rollback", tag.getTagTypeName(), commitEx);
                            try { graph.rollback(); } catch (Exception rbEx) { LOG.warn("BulkPurge: Rollback also failed", rbEx); }
                        }
                    }

                    totalCleaned += propagatedTagsToDelete.size();
                }

                // Clean zombie Cassandra entries for the deleted entity itself
                tagDAO.deleteTags(allTags);

            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to clean propagated tags for deleted source {}", deletedVertexId, e);
                totalErrors++;
            }
        }

        LOG.info("BulkPurge: Propagated tag cleanup from deleted sources complete for purgeKey={}: cleaned={}, errors={}",
                ctx.purgeKey, totalCleaned, totalErrors);
    }

    /**
     * Remove a single propagated tag name from a vertex's graph properties.
     * Updates __propagatedTraitNames (multi-valued) and __propagatedClassificationNames (pipe-delimited).
     */
    private void removePropagatedTraitFromVertex(AtlasVertex vertex, String tagTypeName) {
        List<String> currentTraits = vertex.getMultiValuedProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, String.class);
        if (currentTraits == null || currentTraits.isEmpty()) {
            return;
        }

        List<String> updatedTraits = new ArrayList<>(currentTraits);
        if (!updatedTraits.remove(tagTypeName)) {
            return; // tag not present on this vertex
        }

        vertex.removeProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        for (String trait : updatedTraits) {
            vertex.addListProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, trait);
        }

        if (updatedTraits.isEmpty()) {
            vertex.removeProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        } else {
            StringBuilder sb = new StringBuilder();
            for (String trait : updatedTraits) {
                if (sb.length() > 0) sb.append("|");
                sb.append(trait);
            }
            AtlasGraphUtilsV2.setEncodedProperty(vertex, PROPAGATED_CLASSIFICATION_NAMES_KEY, sb.toString());
        }

        AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());

        // Clear stale classificationText — will be rebuilt on next read/reindex
        vertex.removeProperty(CLASSIFICATION_TEXT_KEY);
    }

    /**
     * Write a summary audit event.
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

    @VisibleForTesting
    int getWorkerCount(long totalEntities, int configuredMax) {
        if (totalEntities < 1_000)   return 1;
        if (totalEntities < 10_000)  return 2;
        if (totalEntities < 50_000)  return 4;
        if (totalEntities < 500_000) return Math.min(configuredMax, 8);
        return configuredMax;
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

    // ======================== ACTIVE PURGE KEY REGISTRY ========================

    private void addToActivePurgeKeys(String purgeKey) {
        try {
            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            String existing = redisService.getValue(REDIS_ACTIVE_PURGE_KEYS);
            Set<String> keys = parseActivePurgeKeys(existing);
            keys.add(purgeKey);
            redisService.putValue(REDIS_ACTIVE_PURGE_KEYS, MAPPER.writeValueAsString(keys), redisTtl);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to add purge key to active registry: {}", purgeKey, e);
        }
    }

    private void removeFromActivePurgeKeys(String purgeKey) {
        try {
            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            String existing = redisService.getValue(REDIS_ACTIVE_PURGE_KEYS);
            Set<String> keys = parseActivePurgeKeys(existing);
            keys.remove(purgeKey);
            if (keys.isEmpty()) {
                redisService.removeValue(REDIS_ACTIVE_PURGE_KEYS);
            } else {
                redisService.putValue(REDIS_ACTIVE_PURGE_KEYS, MAPPER.writeValueAsString(keys), redisTtl);
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to remove purge key from active registry: {}", purgeKey, e);
        }
    }

    private Set<String> getActivePurgeKeys() {
        String existing = redisService.getValue(REDIS_ACTIVE_PURGE_KEYS);
        return parseActivePurgeKeys(existing);
    }

    private Set<String> parseActivePurgeKeys(String json) {
        if (json == null || json.isEmpty()) {
            return new LinkedHashSet<>();
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            Set<String> keys = new LinkedHashSet<>();
            if (node.isArray()) {
                for (JsonNode item : node) {
                    keys.add(item.asText());
                }
            }
            return keys;
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to parse active purge keys from Redis", e);
            return new LinkedHashSet<>();
        }
    }

    // ======================== ORPHAN CHECKER ========================

    @VisibleForTesting
    void checkAndRecoverOrphanedPurges() {
        boolean lockAcquired = false;
        try {
            lockAcquired = redisService.acquireDistributedLock("bulk_purge_orphan_checker_lock");
        } catch (Exception e) {
            LOG.debug("BulkPurge: Orphan checker could not acquire lock", e);
            return;
        }
        if (!lockAcquired) return;

        try {
            Set<String> activePurgeKeys = getActivePurgeKeys();
            if (activePurgeKeys.isEmpty()) return;

            for (String purgeKey : activePurgeKeys) {
                try {
                    String redisValue = redisService.getValue(REDIS_KEY_PREFIX + purgeKey);
                    if (redisValue == null) {
                        removeFromActivePurgeKeys(purgeKey);
                        continue;
                    }

                    JsonNode status = MAPPER.readTree(redisValue);
                    String purgeStatus = status.has("status") ? status.get("status").asText() : "";
                    long lastHeartbeat = status.has("lastHeartbeat") ? status.get("lastHeartbeat").asLong() : 0;
                    long fiveMinutesAgo = System.currentTimeMillis() - (5 * 60 * 1000);

                    // P7: Also detect stalled purges (heartbeat alive but no progress)
                    long lastProgressTs = status.has("lastProgressTimestamp") ? status.get("lastProgressTimestamp").asLong() : 0;
                    boolean isStalled = "RUNNING".equals(purgeStatus) && lastProgressTs > 0
                            && (System.currentTimeMillis() - lastProgressTs) > STALL_THRESHOLD_MS;

                    if ("RUNNING".equals(purgeStatus) && (lastHeartbeat < fiveMinutesAgo || isStalled)) {
                        if (isStalled) {
                            LOG.warn("BulkPurge: Orphan checker detected STALLED purge for {} " +
                                    "(heartbeat alive but no progress for {}ms)", purgeKey,
                                    System.currentTimeMillis() - lastProgressTs);
                        }

                        // Orphaned — check if work remains
                        String esQuery = status.has("esQuery") ? status.get("esQuery").asText() : null;
                        if (esQuery == null) {
                            removeFromActivePurgeKeys(purgeKey);
                            continue;
                        }

                        long remaining = getEntityCount(esQuery);

                        if (remaining == 0) {
                            markOrphanedPurgeCompleted(purgeKey, status);
                        } else {
                            int resubmitCount = status.has("resubmitCount") ? status.get("resubmitCount").asInt() : 0;
                            int maxResubmits = ORPHAN_MAX_RESUBMITS;

                            if (resubmitCount < maxResubmits) {
                                LOG.info("BulkPurge: Auto-recovering orphaned purge for {} (remaining={}, attempt={})",
                                        purgeKey, remaining, resubmitCount + 1);
                                resubmitOrphanedPurge(purgeKey, status, resubmitCount + 1);
                            } else {
                                // P8: Mark as FAILED and clean up after max resubmits
                                LOG.error("BulkPurge: Orphaned purge for {} exceeded max resubmits ({}). Marking as FAILED.",
                                        purgeKey, maxResubmits);
                                markOrphanedPurgeFailed(purgeKey, status,
                                        "Exceeded max resubmits (" + maxResubmits + ") — manual intervention required");
                            }
                        }
                    } else if ("FAILED".equals(purgeStatus)) {
                        // Only retry FAILED purges that were orphan-recovery resubmits (i.e., failed
                        // due to stale lock or transient issue during recovery, not legitimately failed).
                        boolean isOrphanRecovery = status.has("orphanRecovery") && status.get("orphanRecovery").asBoolean();
                        int resubmitCount = status.has("resubmitCount") ? status.get("resubmitCount").asInt() : 0;
                        int maxResubmits = ORPHAN_MAX_RESUBMITS;
                        String esQuery = status.has("esQuery") ? status.get("esQuery").asText() : null;

                        if (isOrphanRecovery && esQuery != null && resubmitCount < maxResubmits) {
                            long remaining = getEntityCount(esQuery);
                            if (remaining > 0) {
                                LOG.info("BulkPurge: Retrying FAILED orphan-recovery for {} (remaining={}, attempt={})",
                                        purgeKey, remaining, resubmitCount + 1);
                                resubmitOrphanedPurge(purgeKey, status, resubmitCount + 1);
                            } else {
                                markOrphanedPurgeCompleted(purgeKey, status);
                            }
                        } else {
                            removeFromActivePurgeKeys(purgeKey);
                        }
                    } else if ("COMPLETED".equals(purgeStatus) || "CANCELLED".equals(purgeStatus)) {
                        removeFromActivePurgeKeys(purgeKey);
                    }
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Orphan checker error for purgeKey={}", purgeKey, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Orphan checker failed", e);
        } finally {
            try {
                redisService.releaseDistributedLock("bulk_purge_orphan_checker_lock");
            } catch (Exception e) {
                LOG.debug("BulkPurge: Failed to release orphan checker lock", e);
            }
        }
    }

    private void markOrphanedPurgeCompleted(String purgeKey, JsonNode status) {
        try {
            ObjectNode updated = (ObjectNode) status;
            updated.put("status", "COMPLETED");
            updated.put("orphanRecovery", true);

            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            redisService.putValue(REDIS_KEY_PREFIX + purgeKey, MAPPER.writeValueAsString(updated), redisTtl);

            String requestId = status.has("requestId") ? status.get("requestId").asText() : null;
            if (requestId != null) {
                redisService.putValue("bulk_purge_request:" + requestId, MAPPER.writeValueAsString(updated), redisTtl);
            }

            removeFromActivePurgeKeys(purgeKey);
            LOG.info("BulkPurge: Marked orphaned purge as COMPLETED for purgeKey={}", purgeKey);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to mark orphaned purge as completed for {}", purgeKey, e);
        }
    }

    /**
     * P8: Mark an orphaned purge as FAILED when max resubmits have been exceeded.
     */
    private void markOrphanedPurgeFailed(String purgeKey, JsonNode status, String errorMessage) {
        try {
            ObjectNode updated = (ObjectNode) status;
            updated.put("status", "FAILED");
            updated.put("error", errorMessage);
            updated.put("orphanRecovery", true);

            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            redisService.putValue(REDIS_KEY_PREFIX + purgeKey, MAPPER.writeValueAsString(updated), redisTtl);

            String requestId = status.has("requestId") ? status.get("requestId").asText() : null;
            if (requestId != null) {
                redisService.putValue("bulk_purge_request:" + requestId, MAPPER.writeValueAsString(updated), redisTtl);
            }

            removeFromActivePurgeKeys(purgeKey);
            LOG.info("BulkPurge: Marked orphaned purge as FAILED for purgeKey={}: {}", purgeKey, errorMessage);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to mark orphaned purge as FAILED for {}", purgeKey, e);
        }
    }

    private void resubmitOrphanedPurge(String purgeKey, JsonNode status, int resubmitCount) {
        try {
            // Force-release the stale lock from the dead process before resubmitting.
            // The old lock may still be held in Redis (Redisson watchdog timeout is 10 min)
            // even though the process that held it is dead.
            String lockKey = REDIS_LOCK_PREFIX + purgeKey;
            try {
                redisService.forceReleaseDistributedLock(lockKey);
                LOG.info("BulkPurge: Force-released stale lock for orphaned purge {}", purgeKey);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to force-release stale lock for {}, resubmit may fail", purgeKey, e);
            }

            // Clean stale cancel signal so the resubmitted purge is not immediately killed
            try {
                redisService.removeValue(REDIS_CANCEL_PREFIX + purgeKey);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to clean cancel signal for {} before resubmit", purgeKey, e);
            }

            String purgeMode = status.has("purgeMode") ? status.get("purgeMode").asText() : PURGE_MODE_CONNECTION;
            String esQuery = status.get("esQuery").asText();
            String submittedBy = status.has("submittedBy") ? status.get("submittedBy").asText() : "orphan-checker";
            boolean deleteConnection = status.has("deleteConnection") && status.get("deleteConnection").asBoolean();
            String originalRequestId = status.has("requestId") ? status.get("requestId").asText() : null;

            String requestId = UUID.randomUUID().toString();

            PurgeContext ctx = new PurgeContext(requestId, purgeKey, purgeMode, submittedBy, esQuery, deleteConnection, 0);
            ctx.status = "PENDING";
            ctx.resubmitCount = resubmitCount;
            ctx.orphanRecovery = true;
            writeRedisStatus(ctx);

            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            redisService.putValue("bulk_purge_request:" + requestId, ctx.toJson(), redisTtl);

            activePurges.put(purgeKey, ctx);
            addToActivePurgeKeys(purgeKey);

            String redisKey = REDIS_KEY_PREFIX + purgeKey;
            try {
                coordinatorExecutor.submit(() -> executePurge(ctx, lockKey, redisKey));
            } catch (java.util.concurrent.RejectedExecutionException ree) {
                LOG.error("BulkPurge: Coordinator pool rejected resubmission for {} — pool is full", purgeKey, ree);
                activePurges.remove(purgeKey);
                removeFromActivePurgeKeys(purgeKey);
                ctx.status = "FAILED";
                ctx.error = "Resubmission rejected: coordinator pool at capacity";
                writeRedisStatus(ctx);
                return;
            }

            // Update the ORIGINAL requestId to RESUBMITTED only after successful submission
            if (originalRequestId != null && !originalRequestId.equals(requestId)) {
                try {
                    Map<String, Object> originalUpdate = new LinkedHashMap<>();
                    originalUpdate.put("status", "RESUBMITTED");
                    originalUpdate.put("newRequestId", requestId);
                    originalUpdate.put("resubmitCount", resubmitCount);
                    originalUpdate.put("purgeKey", purgeKey);
                    redisService.putValue("bulk_purge_request:" + originalRequestId,
                            MAPPER.writeValueAsString(originalUpdate), redisTtl);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to update original requestId {} during resubmit", originalRequestId, e);
                }
            }

            LOG.info("BulkPurge: Resubmitted orphaned purge for {} as requestId={} (attempt={}, originalRequestId={})",
                    purgeKey, requestId, resubmitCount, originalRequestId);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to resubmit orphaned purge for {}", purgeKey, e);
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
        final int workerCountOverride;

        volatile String status;
        volatile String error;
        volatile long lastHeartbeat;
        volatile long totalDiscovered;
        volatile long remainingAfterCleanup = -1;
        volatile int workerCount;
        volatile boolean cancelRequested;
        volatile boolean connectionDeleted;
        volatile int resubmitCount;
        volatile String currentPhase;
        volatile boolean stalled;
        volatile boolean orphanRecovery;

        final AtomicInteger totalDeleted = new AtomicInteger(0);
        final AtomicInteger totalFailed = new AtomicInteger(0);
        final AtomicInteger completedBatches = new AtomicInteger(0);
        final AtomicInteger lastProcessedBatchIndex = new AtomicInteger(0);
        final AtomicLong lastProgressTimestamp = new AtomicLong(0);
        final Set<String> externalLineageVertexIds = ConcurrentHashMap.newKeySet();
        final Set<String> entitiesWithDirectTags = ConcurrentHashMap.newKeySet();
        volatile boolean isTagV2;

        // P2: Stored for cancel/interrupt support
        volatile ExecutorService workerPool;
        volatile List<Future<?>> workerFutures;

        PurgeContext(String requestId, String purgeKey, String purgeMode, String submittedBy, String esQuery, boolean deleteConnection, int workerCountOverride) {
            this.requestId = requestId;
            this.purgeKey = purgeKey;
            this.purgeMode = purgeMode;
            this.submittedBy = submittedBy;
            this.esQuery = esQuery;
            this.submittedAt = System.currentTimeMillis();
            this.deleteConnection = deleteConnection;
            this.workerCountOverride = workerCountOverride;
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
            map.put("esQuery", esQuery);
            map.put("totalDiscovered", totalDiscovered);
            map.put("deletedCount", totalDeleted.get());
            map.put("failedCount", totalFailed.get());
            map.put("completedBatches", completedBatches.get());
            map.put("lastProcessedBatchIndex", lastProcessedBatchIndex.get());
            map.put("lastHeartbeat", lastHeartbeat);
            long progressTs = lastProgressTimestamp.get();
            if (progressTs > 0) {
                map.put("lastProgressTimestamp", progressTs);
            }
            map.put("workerCount", workerCount);
            map.put("batchSize", AtlasConfiguration.BULK_PURGE_BATCH_SIZE.getInt());
            map.put("deleteConnection", deleteConnection);
            map.put("connectionDeleted", connectionDeleted);
            if (stalled) {
                map.put("stalled", true);
            }
            if (currentPhase != null) {
                map.put("currentPhase", currentPhase);
            }
            if (remainingAfterCleanup >= 0) {
                map.put("remainingAfterCleanup", remainingAfterCleanup);
            }
            if (resubmitCount > 0) {
                map.put("resubmitCount", resubmitCount);
            }
            if (orphanRecovery) {
                map.put("orphanRecovery", true);
            }
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
