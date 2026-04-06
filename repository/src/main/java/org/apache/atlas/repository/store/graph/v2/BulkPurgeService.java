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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.ApplicationProperties;
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
import org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel;
import org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.*;
import org.apache.atlas.repository.store.graph.v2.purge.PurgeBatchCleanupService;
import org.apache.atlas.repository.store.graph.v2.purge.PurgeESOperations;
import org.apache.atlas.repository.store.graph.v2.purge.PurgeOrphanRecovery;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.atlas.discovery.EntityDiscoveryService;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.*;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;

/**
 * Orchestrator for bulk entity purge operations. Streams entity IDs from ES,
 * deletes vertices in parallel worker threads, and delegates cleanup + reconciliation
 * to extracted service classes.
 *
 * <p>Delegates to:
 * <ul>
 *   <li>{@link PurgeESOperations} — ES scroll, reconciliation, query builders</li>
 *   <li>{@link PurgeBatchCleanupService} — per-batch inline cleanup (16 steps)</li>
 *   <li>{@link PurgeOrphanRecovery} — orphan checker + Redis status management</li>
 * </ul>
 */
@Component
public class BulkPurgeService {
    private static final Logger LOG = LoggerFactory.getLogger(BulkPurgeService.class);

    private static final int    MIN_QN_PREFIX_LENGTH    = 10;
    private static final long   BATCH_WARN_THRESHOLD_MS = 30_000;
    private static final String HOSTNAME                = getHostname();

    // Internal defaults
    private static final int  COORDINATOR_POOL_SIZE     = 4;
    private static final int  COMMIT_MAX_RETRIES        = 3;
    private static final int  HEARTBEAT_INTERVAL_MS     = 30_000;
    private static final long STALL_THRESHOLD_MS        = 900_000; // 15 minutes
    private static final long ORPHAN_CHECK_INTERVAL_MS  = 300_000;

    // Configurable via ApplicationProperties
    private static final long DEFAULT_COMMIT_TIMEOUT_MS       = 120_000;
    private static final long DEFAULT_GET_VERTICES_TIMEOUT_MS = 60_000;
    private static final long DEFAULT_ES_SETTLE_WAIT_MS       = 5_000;

    private final AtlasGraph graph;
    private final IAtlasGraphProvider graphProvider;
    private final RedisService redisService;
    private final Set<EntityAuditRepository> auditRepositories;
    private final AtlasDistributedTaskNotificationSender taskNotificationSender;
    private final TaskManagement taskManagement;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityDiscoveryService discovery;

    // Delegate services
    private final PurgeESOperations esOps;
    private final PurgeBatchCleanupService cleanupService;
    private final PurgeOrphanRecovery orphanRecovery;

    private final ThreadPoolExecutor coordinatorExecutor;
    private final ExecutorService timeoutExecutor;

    private volatile TagDAO tagDAOOverride;

    private final long commitTimeoutMs;
    private final long getVerticesTimeoutMs;

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
                            TaskManagement taskManagement,
                            AtlasTypeRegistry typeRegistry,
                            EntityDiscoveryService discovery) {
        this.graph = graph;
        this.graphProvider = graphProvider;
        this.redisService = redisService;
        this.auditRepositories = auditRepositories;
        this.taskNotificationSender = taskNotificationSender;
        this.taskManagement = taskManagement;
        this.typeRegistry = typeRegistry;
        this.discovery = discovery;

        // Read configurable timeouts
        long commitTimeout = DEFAULT_COMMIT_TIMEOUT_MS;
        long getVerticesTimeout = DEFAULT_GET_VERTICES_TIMEOUT_MS;
        long esSettle = DEFAULT_ES_SETTLE_WAIT_MS;
        try {
            commitTimeout = ApplicationProperties.get().getLong("atlas.bulk.purge.commit.timeout.ms", DEFAULT_COMMIT_TIMEOUT_MS);
            getVerticesTimeout = ApplicationProperties.get().getLong("atlas.bulk.purge.get.vertices.timeout.ms", DEFAULT_GET_VERTICES_TIMEOUT_MS);
            esSettle = ApplicationProperties.get().getLong("atlas.bulk.purge.es.settle.wait.ms", DEFAULT_ES_SETTLE_WAIT_MS);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Could not read timeout config from ApplicationProperties, using defaults", e);
        }
        this.commitTimeoutMs = commitTimeout;
        this.getVerticesTimeoutMs = getVerticesTimeout;

        // Create delegate services
        this.esOps = new PurgeESOperations(graph, esSettle);
        this.cleanupService = new PurgeBatchCleanupService(graph, typeRegistry, taskManagement,
                discovery, esOps, this::getTagDAO);

        this.coordinatorExecutor = new ThreadPoolExecutor(
                COORDINATOR_POOL_SIZE, COORDINATOR_POOL_SIZE, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(COORDINATOR_POOL_SIZE),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-coordinator-%d")
                        .build());

        this.orphanRecovery = new PurgeOrphanRecovery(redisService, esOps, activePurges,
                (ctx, lockKey, redisKey) -> coordinatorExecutor.submit(() -> executePurge(ctx, lockKey, redisKey)));

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
                orphanRecovery::checkAndRecoverOrphanedPurges,
                ORPHAN_CHECK_INTERVAL_MS, ORPHAN_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);

        LOG.info("BulkPurge: Orphan checker started with interval={}ms", ORPHAN_CHECK_INTERVAL_MS);
    }

    @PreDestroy
    private void shutdown() {
        LOG.info("BulkPurge: Shutting down -- marking {} active purges as FAILED", activePurges.size());

        for (Map.Entry<String, PurgeContext> entry : activePurges.entrySet()) {
            try {
                PurgeContext ctx = entry.getValue();
                ctx.status = "FAILED";
                ctx.error = "Pod shutdown";
                orphanRecovery.writeRedisStatus(ctx);
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

    @VisibleForTesting
    void setEsClient(org.elasticsearch.client.RestClient client) {
        esOps.setEsClient(client);
    }

    @VisibleForTesting
    void setTagDAOOverride(TagDAO tagDAO) {
        this.tagDAOOverride = tagDAO;
    }

    @VisibleForTesting
    void checkAndRecoverOrphanedPurges() {
        orphanRecovery.checkAndRecoverOrphanedPurges();
    }

    @VisibleForTesting
    PurgeBatchCleanupService getCleanupService() {
        return cleanupService;
    }

    private TagDAO getTagDAO() {
        return tagDAOOverride != null ? tagDAOOverride : TagDAOCassandraImpl.getInstance();
    }

    // ======================== PUBLIC API ========================

    public String bulkPurgeByConnection(String connectionQN, String submittedBy, boolean deleteConnection) throws AtlasBaseException {
        return bulkPurgeByConnection(connectionQN, submittedBy, deleteConnection, 0);
    }

    public String bulkPurgeByConnection(String connectionQN, String submittedBy, boolean deleteConnection, int workerCountOverride) throws AtlasBaseException {
        if (connectionQN == null || connectionQN.isEmpty()) {
            throw new AtlasBaseException("connectionQualifiedName is required");
        }

        AtlasVertex connVertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(graph,
                CONNECTION_ENTITY_TYPE, QUALIFIED_NAME, connectionQN);
        if (connVertex == null) {
            throw new AtlasBaseException("Connection not found: " + connectionQN);
        }

        String esQuery = esOps.buildPrefixQuery(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, connectionQN + "/");
        return submitPurge(connectionQN, PURGE_MODE_CONNECTION, esQuery, submittedBy, deleteConnection, workerCountOverride);
    }

    public String bulkPurgeByQualifiedName(String qualifiedNamePrefix, String submittedBy) throws AtlasBaseException {
        return bulkPurgeByQualifiedName(qualifiedNamePrefix, submittedBy, 0);
    }

    public String bulkPurgeByQualifiedName(String qualifiedNamePrefix, String submittedBy, int workerCountOverride) throws AtlasBaseException {
        if (qualifiedNamePrefix == null || qualifiedNamePrefix.length() < MIN_QN_PREFIX_LENGTH) {
            throw new AtlasBaseException("qualifiedName prefix must be at least " + MIN_QN_PREFIX_LENGTH + " characters");
        }

        String esQuery = esOps.buildPrefixQuery(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, qualifiedNamePrefix);
        return submitPurge(qualifiedNamePrefix, PURGE_MODE_QN_PREFIX, esQuery, submittedBy, false, workerCountOverride);
    }

    public Map<String, Object> getStatus(String requestId) {
        for (PurgeContext ctx : activePurges.values()) {
            if (ctx.requestId.equals(requestId)) {
                Map<String, Object> status = ctx.toStatusMap();
                status.put("source", "in-memory");
                status.put("respondingPod", HOSTNAME);
                return status;
            }
        }

        String redisValue = redisService.getValue("bulk_purge_request:" + requestId);
        if (redisValue != null) {
            try {
                Map<String, Object> status = BulkPurgeModel.MAPPER.readValue(redisValue, Map.class);
                status.put("source", "redis");
                status.put("respondingPod", HOSTNAME);
                return status;
            } catch (Exception e) {
                LOG.warn("Failed to parse purge status from Redis for requestId={}", requestId, e);
            }
        }

        return null;
    }

    public boolean cancelPurge(String requestId) {
        for (PurgeContext ctx : activePurges.values()) {
            if (ctx.requestId.equals(requestId)) {
                ctx.cancelRequested = true;
                orphanRecovery.interruptWorkers(ctx);
                try {
                    int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
                    redisService.putValue(REDIS_CANCEL_PREFIX + ctx.purgeKey, "CANCEL_REQUESTED", redisTtl);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Failed to write Redis cancel signal for requestId={}", requestId, e);
                }
                LOG.info("BulkPurge: Local cancel applied for requestId={}, purgeKey={}", requestId, ctx.purgeKey);
                return true;
            }
        }

        String redisValue = redisService.getValue("bulk_purge_request:" + requestId);
        if (redisValue == null) {
            return false;
        }

        try {
            JsonNode status = BulkPurgeModel.MAPPER.readTree(redisValue);
            String purgeKey = status.has("purgeKey") ? status.get("purgeKey").asText() : null;
            if (purgeKey == null) {
                return false;
            }

            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            redisService.putValue(REDIS_CANCEL_PREFIX + purgeKey, "CANCEL_REQUESTED", redisTtl);
            LOG.info("BulkPurge: Cancel signal written to Redis for requestId={}, purgeKey={}", requestId, purgeKey);
            return true;
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to cancel purge for requestId={}", requestId, e);
            return false;
        }
    }

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

    // ======================== SUBMIT & EXECUTE ========================

    private String submitPurge(String purgeKey, String purgeMode, String esQuery, String submittedBy, boolean deleteConnection, int workerCountOverride) throws AtlasBaseException {
        String redisKey = REDIS_KEY_PREFIX + purgeKey;
        String lockKey = REDIS_LOCK_PREFIX + purgeKey;

        String existingStatus = redisService.getValue(redisKey);
        if (existingStatus != null) {
            try {
                JsonNode statusNode = BulkPurgeModel.MAPPER.readTree(existingStatus);
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

        PurgeContext ctx = new PurgeContext(requestId, purgeKey, purgeMode, submittedBy, esQuery, deleteConnection, workerCountOverride);
        ctx.status = "PENDING";
        orphanRecovery.writeRedisStatus(ctx);

        int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
        redisService.putValue("bulk_purge_request:" + requestId, ctx.toJson(), redisTtl);

        activePurges.put(purgeKey, ctx);
        orphanRecovery.addToActivePurgeKeys(purgeKey);

        try {
            coordinatorExecutor.submit(() -> executePurge(ctx, lockKey, redisKey));
        } catch (RejectedExecutionException e) {
            activePurges.remove(purgeKey);
            orphanRecovery.removeFromActivePurgeKeys(purgeKey);
            ctx.status = "FAILED";
            ctx.error = "System at capacity -- all coordinator slots are busy. Retry later.";
            orphanRecovery.writeRedisStatus(ctx);
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
                orphanRecovery.writeRedisStatus(ctx);
                return;
            }

            ctx.status = "RUNNING";
            ctx.lastHeartbeat = System.currentTimeMillis();
            orphanRecovery.writeRedisStatus(ctx);

            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("bulk-purge-heartbeat-" + ctx.purgeKey).build());
            heartbeatExecutor.scheduleAtFixedRate(() -> {
                try {
                    ctx.lastHeartbeat = System.currentTimeMillis();

                    long lastProgress = ctx.lastProgressTimestamp.get();
                    if (lastProgress > 0 && (System.currentTimeMillis() - lastProgress) > STALL_THRESHOLD_MS) {
                        LOG.error("BulkPurge: STALLED -- no progress for {}ms, purgeKey={}, completedBatches={}",
                                System.currentTimeMillis() - lastProgress, ctx.purgeKey, ctx.completedBatches.get());
                        ctx.stalled = true;
                    }

                    String cancelSignal = redisService.getValue(REDIS_CANCEL_PREFIX + ctx.purgeKey);
                    if ("CANCEL_REQUESTED".equals(cancelSignal)) {
                        LOG.info("BulkPurge: Force-cancel signal detected via Redis for purgeKey={}", ctx.purgeKey);
                        ctx.cancelRequested = true;
                        orphanRecovery.interruptWorkers(ctx);
                        redisService.removeValue(REDIS_CANCEL_PREFIX + ctx.purgeKey);
                    }

                    orphanRecovery.writeRedisStatus(ctx);
                } catch (Exception e) {
                    LOG.warn("BulkPurge: Heartbeat failed for {}", ctx.purgeKey, e);
                }
            }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

            long startTime = System.currentTimeMillis();
            ctx.currentPhase = "STREAMING_DELETE_AND_CLEANUP";
            orphanRecovery.writeRedisStatus(ctx);
            streamAndDelete(ctx);

            if (ctx.cancelRequested) {
                ctx.status = "CANCELLED";
                LOG.info("BulkPurge: Cancelled for purgeKey={}, deleted={}, failed={}",
                        ctx.purgeKey, ctx.totalDeleted.get(), ctx.totalFailed.get());
            } else {
                long phaseStart = System.currentTimeMillis();

                ctx.currentPhase = "ES_RECONCILIATION";
                orphanRecovery.writeRedisStatus(ctx);

                int esReconcileMaxRetries = 3;
                long remainingAfterEsCleanup = -1;
                for (int esAttempt = 1; esAttempt <= esReconcileMaxRetries; esAttempt++) {
                    esOps.reconcileESCleanup(ctx);
                    LOG.info("BulkPurge: ES reconciliation attempt {}/{} completed in {}ms for purgeKey={}",
                            esAttempt, esReconcileMaxRetries, System.currentTimeMillis() - phaseStart, ctx.purgeKey);

                    try {
                        remainingAfterEsCleanup = esOps.getEntityCount(ctx.esQuery);
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: ES verification count failed on attempt {} for purgeKey={}",
                                esAttempt, ctx.purgeKey, e);
                        remainingAfterEsCleanup = -1;
                    }

                    if (remainingAfterEsCleanup <= 0) break;

                    if (esAttempt < esReconcileMaxRetries) {
                        LOG.warn("BulkPurge: {} entities remain after ES reconciliation attempt {} for purgeKey={}, retrying...",
                                remainingAfterEsCleanup, esAttempt, ctx.purgeKey);
                        try { Thread.sleep(2000L * esAttempt); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }

                // Secondary QN-based ES reconciliation
                if (remainingAfterEsCleanup <= 0 && PURGE_MODE_CONNECTION.equals(ctx.purgeMode)) {
                    try {
                        String secondaryQuery = esOps.buildPrefixQuery(QUALIFIED_NAME, ctx.purgeKey + "/");
                        long secondaryCount = esOps.getEntityCount(secondaryQuery);
                        if (secondaryCount > 0) {
                            LOG.info("BulkPurge: Secondary QN query found {} remaining docs for purgeKey={}, reconciling...",
                                    secondaryCount, ctx.purgeKey);
                            PurgeContext secondaryCtx = new PurgeContext(ctx.requestId, ctx.purgeKey, ctx.purgeMode,
                                    ctx.submittedBy, secondaryQuery, false, 0);
                            esOps.reconcileESCleanup(secondaryCtx);
                            remainingAfterEsCleanup = secondaryCount;
                        }
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: Secondary QN reconciliation failed for purgeKey={}", ctx.purgeKey, e);
                    }
                }

                ctx.currentPhase = "ES_VERIFICATION";
                orphanRecovery.writeRedisStatus(ctx);
                ctx.remainingAfterCleanup = Math.max(0, remainingAfterEsCleanup);
                if (remainingAfterEsCleanup > 0) {
                    LOG.warn("BulkPurge: {} entities remain after all reconciliation attempts for purgeKey={}. " +
                            "These may require manual cleanup.", remainingAfterEsCleanup, ctx.purgeKey);
                    ctx.error = remainingAfterEsCleanup + " entities remain after reconciliation";
                } else {
                    LOG.info("BulkPurge: Verification passed -- 0 entities remaining for purgeKey={}", ctx.purgeKey);
                }

                if (ctx.deleteConnection && PURGE_MODE_CONNECTION.equals(ctx.purgeMode)) {
                    ctx.currentPhase = "DELETE_CONNECTION";
                    orphanRecovery.writeRedisStatus(ctx);
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

            orphanRecovery.writeRedisStatus(ctx);

        } catch (Exception e) {
            LOG.error("BulkPurge: Failed for purgeKey={}", ctx.purgeKey, e);
            ctx.status = "FAILED";
            ctx.error = e.getMessage();
            orphanRecovery.writeRedisStatus(ctx);
        } finally {
            if (heartbeatExecutor != null) {
                heartbeatExecutor.shutdownNow();
            }
            activePurges.remove(ctx.purgeKey);
            if ("COMPLETED".equals(ctx.status) || "CANCELLED".equals(ctx.status) || "FAILED".equals(ctx.status)) {
                orphanRecovery.removeFromActivePurgeKeys(ctx.purgeKey);
            }
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

    // ======================== STREAM & DELETE ========================

    private void streamAndDelete(PurgeContext ctx) throws Exception {
        int batchSize = AtlasConfiguration.BULK_PURGE_BATCH_SIZE.getInt();
        int configuredWorkerCount = AtlasConfiguration.BULK_PURGE_WORKER_COUNT.getInt();

        long totalEntities = esOps.getEntityCount(ctx.esQuery);
        ctx.totalDiscovered = totalEntities;
        LOG.info("BulkPurge: Discovered {} entities for purgeKey={}", totalEntities, ctx.purgeKey);

        if (totalEntities == 0) {
            LOG.info("BulkPurge: No entities to delete for purgeKey={}", ctx.purgeKey);
            return;
        }

        int workerCount;
        if (ctx.workerCountOverride > 0) {
            workerCount = Math.min(ctx.workerCountOverride, configuredWorkerCount);
            LOG.info("BulkPurge: Using worker count override={} (capped to {}) for purgeKey={}",
                    ctx.workerCountOverride, workerCount, ctx.purgeKey);
        } else {
            workerCount = getWorkerCount(totalEntities, configuredWorkerCount);
        }
        ctx.workerCount = workerCount;
        orphanRecovery.writeRedisStatus(ctx);

        BlockingQueue<BatchWork> batchQueue = new LinkedBlockingQueue<>(workerCount * 2);

        ExecutorService workerPool = Executors.newFixedThreadPool(workerCount,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("bulk-purge-worker-%d-" + ctx.purgeKey)
                        .build());
        ctx.workerPool = workerPool;

        List<AtlasGraph> workerGraphs = new ArrayList<>(workerCount);

        List<Future<?>> workerFutures = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            AtlasGraph workerGraph = getBulkLoadingGraph();
            workerGraphs.add(workerGraph);
            LOG.info("BulkPurge: Created bulk-loading graph for worker {} (purgeKey={})", i, ctx.purgeKey);
            workerFutures.add(workerPool.submit(() -> workerLoop(ctx, batchQueue, workerGraph)));
        }
        ctx.workerFutures = workerFutures;

        try {
            esOps.streamESScrollIntoBatchQueue(ctx, batchQueue, batchSize);
        } finally {
            if (ctx.cancelRequested || Thread.currentThread().isInterrupted()) {
                batchQueue.clear();
            }

            for (int i = 0; i < workerCount; i++) {
                if (!batchQueue.offer(BatchWork.POISON_PILL, 30, TimeUnit.SECONDS)) {
                    LOG.warn("BulkPurge: Timed out enqueuing poison pill for worker {} (purgeKey={}). " +
                            "Queue may be full with unconsumed batches.", i, ctx.purgeKey);
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

    // ======================== WORKER ========================

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

    private void processBatch(PurgeContext ctx, BatchWork work, AtlasGraph workerGraph) {
        long batchStartTime = System.nanoTime();
        int batchDeleted = 0;
        int batchFailed = 0;

        long t0 = System.nanoTime();

        String[] ids = work.vertexIds.toArray(new String[0]);
        Set<AtlasVertex> vertexSet;
        Future<Set<AtlasVertex>> getVerticesFuture = null;
        try {
            getVerticesFuture = timeoutExecutor.submit(() -> workerGraph.getVertices(ids));
            vertexSet = getVerticesFuture.get(getVerticesTimeoutMs, TimeUnit.MILLISECONDS);
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

        BatchCleanupMetadata batchMeta = new BatchCleanupMetadata();

        long t1 = System.nanoTime();
        long collectLineageNs = 0;

        for (String vertexId : work.vertexIds) {
            if (ctx.cancelRequested || Thread.currentThread().isInterrupted()) break;

            try {
                AtlasVertex vertex = vertexMap.get(vertexId);
                if (vertex == null) continue;

                String typeName = vertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
                if (typeName == null) {
                    typeName = ""; // guard against null — type-specific branches will safely skip
                }

                String guid = vertex.getProperty(GUID_PROPERTY_KEY, String.class);

                Boolean vertexHasLineage = vertex.getProperty(HAS_LINEAGE, Boolean.class);
                if (Boolean.TRUE.equals(vertexHasLineage)) {
                    long lineageT0 = System.nanoTime();
                    collectExternalLineageVertices(ctx, vertex, batchMeta.externalLineageVerts);
                    collectLineageNs += (System.nanoTime() - lineageT0);
                }

                if (guid != null) {
                    batchMeta.deletedGuids.add(guid);
                }

                // Tag cleanup metadata
                List<String> traitNames = vertex.getMultiValuedProperty(TRAIT_NAMES_PROPERTY_KEY, String.class);
                boolean hasDirectTags = traitNames != null && !traitNames.isEmpty();
                if (hasDirectTags) {
                    batchMeta.directTagVerts.add(vertexId);
                }

                if (!hasDirectTags) {
                    List<String> propagatedTraitNames = vertex.getMultiValuedProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, String.class);
                    if (propagatedTraitNames != null && !propagatedTraitNames.isEmpty()) {
                        batchMeta.propagatedOnlyTagVerts.add(vertexId);
                    }
                }

                // Type-specific pre-scan
                if (DATASET_ENTITY_TYPE.equals(typeName) && guid != null) {
                    batchMeta.datasetGuids.add(guid);
                }
                if (ATLAS_GLOSSARY_TERM_ENTITY_TYPE.equals(typeName)) {
                    String qn = vertex.getProperty(QUALIFIED_NAME, String.class);
                    String name = vertex.getProperty(NAME, String.class);
                    if (qn != null) {
                        batchMeta.termInfo.add(new String[]{guid, qn, name != null ? name : ""});
                    }
                }
                if (ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE.equals(typeName)) {
                    collectCategoryCleanupInfo(vertex, batchMeta);
                }

                // Access control entity types
                if (PERSONA_ENTITY_TYPE.equals(typeName) && guid != null) {
                    String roleId = vertex.getProperty("roleId", String.class);
                    String qn = vertex.getProperty(QUALIFIED_NAME, String.class);
                    batchMeta.personaCleanup.add(new PersonaCleanupInfo(guid, roleId, qn));
                }
                if (PURPOSE_ENTITY_TYPE.equals(typeName) && guid != null) {
                    String qn = vertex.getProperty(QUALIFIED_NAME, String.class);
                    batchMeta.purposeCleanup.add(new PurposeCleanupInfo(guid, qn));
                }
                if (DATA_PRODUCT_ENTITY_TYPE.equals(typeName) && guid != null) {
                    batchMeta.dataProductGuids.add(guid);
                }
                if (QUERY_COLLECTION_ENTITY_TYPE.equals(typeName) && guid != null) {
                    batchMeta.collectionCleanup.add(new CollectionCleanupInfo(guid));
                }
                if (CONNECTION_ENTITY_TYPE.equals(typeName) && guid != null) {
                    batchMeta.connectionCleanup.add(new ConnectionCleanupInfo(guid));
                }
                if (STAKEHOLDER_TITLE_ENTITY_TYPE.equals(typeName) && guid != null) {
                    batchMeta.stakeholderTitleCleanup.add(new StakeholderTitleCleanupInfo(guid, vertexId));
                }

                // Remove edges then vertex
                List<AtlasVertex> structVertices = null;
                List<AtlasVertex> ownedResourceVertices = null;

                Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH);
                for (AtlasEdge edge : edges) {
                    try {
                        AtlasVertex other = edge.getOutVertex().getId().equals(vertex.getId())
                                ? edge.getInVertex() : edge.getOutVertex();
                        if (isExternalVertex(ctx, other)) {
                            batchMeta.externalVerts.add(other.getId().toString());
                        } else if (isStructVertex(other)) {
                            if (structVertices == null) structVertices = new ArrayList<>(4);
                            structVertices.add(other);
                        } else if (isOwnedResource(other)) {
                            if (ownedResourceVertices == null) ownedResourceVertices = new ArrayList<>(4);
                            ownedResourceVertices.add(other);
                        }
                    } catch (Exception e) {
                        LOG.debug("BulkPurge: Could not check status for edge endpoint", e);
                    }
                    workerGraph.removeEdge(edge);
                }

                if (structVertices != null) {
                    for (AtlasVertex sv : structVertices) {
                        try {
                            Iterable<AtlasEdge> structEdges = sv.getEdges(AtlasEdgeDirection.BOTH);
                            for (AtlasEdge sEdge : structEdges) {
                                workerGraph.removeEdge(sEdge);
                            }
                            workerGraph.removeVertex(sv);
                        } catch (Exception e) {
                            LOG.debug("BulkPurge: Failed to remove struct vertex {}", sv.getId(), e);
                        }
                    }
                }

                // Delete ReadMe/Link vertices that are owned resources of the deleted entity
                if (ownedResourceVertices != null) {
                    for (AtlasVertex rv : ownedResourceVertices) {
                        try {
                            Iterable<AtlasEdge> rvEdges = rv.getEdges(AtlasEdgeDirection.BOTH);
                            for (AtlasEdge rvEdge : rvEdges) {
                                workerGraph.removeEdge(rvEdge);
                            }
                            workerGraph.removeVertex(rv);
                            batchDeleted++;
                        } catch (Exception e) {
                            LOG.debug("BulkPurge: Failed to remove owned resource vertex {}", rv.getId(), e);
                        }
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

        long t2 = System.nanoTime();
        boolean committed = commitWithRetry(workerGraph, work.batchIndex);
        long commitMs = (System.nanoTime() - t2) / 1_000_000;

        if (committed) {
            ctx.totalDeleted.addAndGet(batchDeleted);
            ctx.totalFailed.addAndGet(batchFailed);

            long t3 = System.nanoTime();
            cleanupService.performBatchCleanup(ctx, batchMeta);
            long cleanupMs = (System.nanoTime() - t3) / 1_000_000;

            if (cleanupMs > 5000) {
                LOG.info("BulkPurge: Batch {} inline cleanup took {}ms for purgeKey={}", work.batchIndex, cleanupMs, ctx.purgeKey);
            }
        } else {
            ctx.totalFailed.addAndGet(work.vertexIds.size());
            try { workerGraph.rollback(); } catch (Exception e) {
                LOG.warn("BulkPurge: Rollback failed for batch {}", work.batchIndex, e);
            }
        }

        int batches = ctx.completedBatches.incrementAndGet();
        ctx.lastProcessedBatchIndex.accumulateAndGet(work.batchIndex, Math::max);
        ctx.lastProgressTimestamp.set(System.currentTimeMillis());

        long totalBatchMs = (System.nanoTime() - batchStartTime) / 1_000_000;

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

        if (batches % 10 == 0) {
            orphanRecovery.writeRedisStatus(ctx);
        }
    }

    private boolean commitWithRetry(AtlasGraph workerGraph, int batchIndex) {
        for (int attempt = 1; attempt <= COMMIT_MAX_RETRIES; attempt++) {
            Future<?> commitFuture = null;
            try {
                commitFuture = timeoutExecutor.submit(() -> workerGraph.commit());
                commitFuture.get(commitTimeoutMs, TimeUnit.MILLISECONDS);
                return true;
            } catch (TimeoutException e) {
                LOG.error("BulkPurge: Commit TIMED OUT for batch {} (attempt {}/{}, timeout={}ms)",
                        batchIndex, attempt, COMMIT_MAX_RETRIES, commitTimeoutMs);
                if (commitFuture != null) commitFuture.cancel(true);
                return false;
            } catch (InterruptedException e) {
                if (commitFuture != null) commitFuture.cancel(true);
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                LOG.warn("BulkPurge: Commit failed for batch {} (attempt {}/{})", batchIndex, attempt, COMMIT_MAX_RETRIES, e.getCause());
                if (attempt < COMMIT_MAX_RETRIES) {
                    try { Thread.sleep((long) Math.pow(2, attempt - 1) * 500); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            } catch (Exception e) {
                LOG.warn("BulkPurge: Commit failed for batch {} (attempt {}/{})", batchIndex, attempt, COMMIT_MAX_RETRIES, e);
                if (attempt < COMMIT_MAX_RETRIES) {
                    try { Thread.sleep((long) Math.pow(2, attempt - 1) * 500); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }
        return false;
    }

    // ======================== BATCH HELPERS ========================

    private void collectExternalLineageVertices(PurgeContext ctx, AtlasVertex vertex, Set<String> targetSet) {
        try {
            String[] lineageLabels = {PROCESS_INPUTS, PROCESS_OUTPUTS};
            Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH, lineageLabels);

            for (AtlasEdge edge : edges) {
                try {
                    AtlasVertex inVertex = edge.getInVertex();
                    AtlasVertex outVertex = edge.getOutVertex();
                    AtlasVertex other = (inVertex.getId().equals(vertex.getId())) ? outVertex : inVertex;

                    if (isExternalVertex(ctx, other)) {
                        targetSet.add(other.getId().toString());
                    }
                } catch (Exception e) {
                    LOG.debug("BulkPurge: Could not process lineage edge for vertex {}", vertex.getId(), e);
                }
            }
        } catch (Exception e) {
            LOG.debug("BulkPurge: Could not collect lineage vertices for {}", vertex.getId(), e);
        }
    }

    private void collectCategoryCleanupInfo(AtlasVertex categoryVertex, BatchCleanupMetadata batchMeta) {
        try {
            String catQN = categoryVertex.getProperty(QUALIFIED_NAME, String.class);
            List<String> childVertexIds = new ArrayList<>();
            List<String> termVertexIds = new ArrayList<>();

            Iterator<AtlasEdge> parentEdges = categoryVertex.getEdges(
                    AtlasEdgeDirection.OUT, CATEGORY_PARENT_EDGE_LABEL).iterator();
            while (parentEdges.hasNext()) {
                AtlasEdge edge = parentEdges.next();
                if (ACTIVE_STATE_VALUE.equals(edge.getProperty(STATE_PROPERTY_KEY, String.class))) {
                    childVertexIds.add(edge.getInVertex().getId().toString());
                }
            }

            Iterator<AtlasEdge> termEdges = categoryVertex.getEdges(
                    AtlasEdgeDirection.OUT, CATEGORY_TERMS_EDGE_LABEL).iterator();
            while (termEdges.hasNext()) {
                termVertexIds.add(termEdges.next().getInVertex().getId().toString());
            }

            if (!childVertexIds.isEmpty() || !termVertexIds.isEmpty()) {
                batchMeta.categoryCleanup.add(new CategoryCleanupInfo(childVertexIds, catQN, termVertexIds));
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to collect category cleanup info for vertex {}", categoryVertex.getId(), e);
        }
    }

    private boolean isExternalVertex(PurgeContext ctx, AtlasVertex other) {
        if (PURGE_MODE_CONNECTION.equals(ctx.purgeMode)) {
            String otherConnQN = other.getProperty(CONNECTION_QUALIFIED_NAME, String.class);
            return otherConnQN != null && !otherConnQN.equals(ctx.purgeKey);
        } else {
            String otherQNH = other.getProperty(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, String.class);
            if (otherQNH == null) return true;
            return !otherQNH.startsWith(ctx.purgeKey);
        }
    }

    private boolean isStructVertex(AtlasVertex vertex) {
        if (vertex.getProperty(GUID_PROPERTY_KEY, String.class) != null) return false;
        String typeName = vertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
        if (typeName == null) return false;
        try {
            AtlasType type = typeRegistry.getType(typeName);
            return type.getTypeCategory() == TypeCategory.STRUCT;
        } catch (AtlasBaseException e) {
            return false;
        }
    }

    /**
     * Returns true if the vertex is a ReadMe or Link entity — resource types
     * that are logically owned by their parent asset but not discovered by the
     * ES scroll (their QN is independent of the connection hierarchy).
     * These must be deleted alongside the parent to avoid orphans.
     */
    private boolean isOwnedResource(AtlasVertex vertex) {
        String typeName = vertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
        return README_ENTITY_TYPE.equals(typeName) || LINK_ENTITY_TYPE.equals(typeName);
    }

    // ======================== CONNECTION DELETION ========================

    private void deleteConnectionVertex(PurgeContext ctx) {
        try {
            AtlasVertex connVertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(graph,
                    CONNECTION_ENTITY_TYPE, QUALIFIED_NAME, ctx.purgeKey);
            if (connVertex == null) {
                LOG.warn("BulkPurge: Connection vertex not found for deletion: {}", ctx.purgeKey);
                return;
            }

            String connGuid = connVertex.getProperty(GUID_PROPERTY_KEY, String.class);
            cleanConnectionPoliciesAndRole(ctx, connGuid);

            graph.removeVertex(connVertex);
            graph.commit();

            ctx.connectionDeleted = true;
            ctx.totalDeleted.incrementAndGet();
            LOG.info("BulkPurge: Connection entity deleted for purgeKey={}", ctx.purgeKey);

            esOps.deleteConnectionFromES(ctx, connGuid);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to delete Connection entity for purgeKey={}", ctx.purgeKey, e);
            ctx.error = "Child assets purged but Connection deletion failed: " + e.getMessage();
            try { graph.rollback(); } catch (Exception re) {
                LOG.warn("BulkPurge: Rollback failed after connection deletion failure", re);
            }
        }
    }

    private void cleanConnectionPoliciesAndRole(PurgeContext ctx, String connGuid) {
        if (connGuid == null) return;

        String roleName = String.format("connection_admins_%s", connGuid);

        try {
            List<String> policyVertexIds = esOps.findConnectionPolicyVertexIds(connGuid, roleName);

            if (!policyVertexIds.isEmpty()) {
                LOG.info("BulkPurge: Deleting {} bootstrap policies for connection {}", policyVertexIds.size(), connGuid);

                List<String> policyEsIds = new ArrayList<>();
                for (String vertexId : policyVertexIds) {
                    try {
                        AtlasVertex policyVertex = graph.getVertex(vertexId);
                        if (policyVertex != null) {
                            policyEsIds.add(org.janusgraph.util.encoding.LongEncoding.encode(Long.parseLong(vertexId)));
                            Iterable<AtlasEdge> edges = policyVertex.getEdges(AtlasEdgeDirection.BOTH);
                            for (AtlasEdge edge : edges) {
                                graph.removeEdge(edge);
                            }
                            graph.removeVertex(policyVertex);
                            graph.commit();
                        }
                    } catch (Exception e) {
                        LOG.warn("BulkPurge: Failed to delete policy vertex {} for connection {}", vertexId, connGuid, e);
                        try { graph.rollback(); } catch (Exception rbEx) { LOG.warn("BulkPurge: Rollback failed", rbEx); }
                    }
                }

                if (!policyEsIds.isEmpty()) {
                    esOps.deleteESDocsByIds(esOps.getEsClient(), policyEsIds);
                }
            } else {
                LOG.info("BulkPurge: No bootstrap policies found for connection {}", connGuid);
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to find/delete policies for connection {}", connGuid, e);
        }

        try {
            new KeycloakStore().removeRoleByName(roleName);
            LOG.info("BulkPurge: Keycloak role {} removed for connection {}", roleName, connGuid);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to remove Keycloak role {} for connection {}", roleName, connGuid, e);
        }
    }

    // ======================== AUDIT ========================

    private void writeSummaryAuditEvent(PurgeContext ctx, long startTime) {
        try {
            long duration = System.currentTimeMillis() - startTime;

            com.fasterxml.jackson.databind.node.ObjectNode detailsNode = BulkPurgeModel.MAPPER.createObjectNode();
            detailsNode.put("purgeKey", ctx.purgeKey);
            detailsNode.put("purgeMode", ctx.purgeMode);
            detailsNode.put("totalDeleted", ctx.totalDeleted.get());
            detailsNode.put("totalFailed", ctx.totalFailed.get());
            detailsNode.put("totalDiscovered", ctx.totalDiscovered);
            detailsNode.put("durationMs", duration);
            detailsNode.put("requestId", ctx.requestId);
            detailsNode.put("workerCount", ctx.workerCount);

            String auditPrefix = "Purged: ";
            String details = auditPrefix + detailsNode.toString();

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
            event.setEntityQualifiedName(AtlasType.toJson("bulk-purge:" + ctx.purgeKey));

            for (EntityAuditRepository auditRepository : auditRepositories) {
                auditRepository.putEventsV2(event);
            }
            LOG.info("BulkPurge: Audit event written for purgeKey={}", ctx.purgeKey);
        } catch (Exception e) {
            LOG.error("BulkPurge: Failed to write audit event for purgeKey={}", ctx.purgeKey, e);
        }
    }

    // ======================== HELPERS ========================

    @VisibleForTesting
    int getWorkerCount(long totalEntities, int configuredMax) {
        if (totalEntities < 1_000)   return 1;
        if (totalEntities < 10_000)  return 2;
        if (totalEntities < 50_000)  return 4;
        if (totalEntities < 500_000) return Math.min(configuredMax, 8);
        return configuredMax;
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
