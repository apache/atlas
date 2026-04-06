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
package org.apache.atlas.repository.store.graph.v2.purge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.PurgeContext;
import org.apache.atlas.service.redis.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.*;

/**
 * Orphan checker background thread + Redis status management for the BulkPurge subsystem.
 * Detects purges that were abandoned (pod crash, stall) and auto-recovers them.
 */
public class PurgeOrphanRecovery {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeOrphanRecovery.class);

    private static final long STALL_THRESHOLD_MS    = 900_000; // 15 minutes
    private static final int  ORPHAN_MAX_RESUBMITS  = 3;

    private final RedisService redisService;
    private final PurgeESOperations esOps;
    private final ConcurrentHashMap<String, PurgeContext> activePurges;
    private final PurgeExecutionCallback executionCallback;

    /**
     * Callback interface for submitting resubmitted purges back to the coordinator.
     */
    @FunctionalInterface
    public interface PurgeExecutionCallback {
        /**
         * Submit a PurgeContext for execution on the coordinator.
         * @throws java.util.concurrent.RejectedExecutionException if the coordinator pool is full
         */
        void execute(PurgeContext ctx, String lockKey, String redisKey);
    }

    public PurgeOrphanRecovery(RedisService redisService,
                               PurgeESOperations esOps,
                               ConcurrentHashMap<String, PurgeContext> activePurges,
                               PurgeExecutionCallback executionCallback) {
        this.redisService      = redisService;
        this.esOps             = esOps;
        this.activePurges      = activePurges;
        this.executionCallback = executionCallback;
    }

    // ======================== REDIS STATUS ========================

    public void writeRedisStatus(PurgeContext ctx) {
        try {
            int redisTtl = AtlasConfiguration.BULK_PURGE_REDIS_TTL_SECONDS.getInt();
            String redisKey = REDIS_KEY_PREFIX + ctx.purgeKey;
            String json = ctx.toJson();
            redisService.putValue(redisKey, json, redisTtl);
            redisService.putValue("bulk_purge_request:" + ctx.requestId, json, redisTtl);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to write Redis status for {}", ctx.purgeKey, e);
        }
    }

    /**
     * P2: Interrupt worker threads to unblock them from stuck operations.
     */
    public void interruptWorkers(PurgeContext ctx) {
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

    // ======================== ACTIVE PURGE KEY REGISTRY ========================

    public void addToActivePurgeKeys(String purgeKey) {
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

    public void removeFromActivePurgeKeys(String purgeKey) {
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

    Set<String> getActivePurgeKeys() {
        String existing = redisService.getValue(REDIS_ACTIVE_PURGE_KEYS);
        return parseActivePurgeKeys(existing);
    }

    Set<String> parseActivePurgeKeys(String json) {
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
    public void checkAndRecoverOrphanedPurges() {
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

                    long lastProgressTs = status.has("lastProgressTimestamp") ? status.get("lastProgressTimestamp").asLong() : 0;
                    boolean isStalled = "RUNNING".equals(purgeStatus) && lastProgressTs > 0
                            && (System.currentTimeMillis() - lastProgressTs) > STALL_THRESHOLD_MS;

                    if ("RUNNING".equals(purgeStatus) && (lastHeartbeat < fiveMinutesAgo || isStalled)) {
                        if (isStalled) {
                            LOG.warn("BulkPurge: Orphan checker detected STALLED purge for {} " +
                                    "(heartbeat alive but no progress for {}ms)", purgeKey,
                                    System.currentTimeMillis() - lastProgressTs);
                        }

                        String esQuery = status.has("esQuery") ? status.get("esQuery").asText() : null;
                        if (esQuery == null) {
                            removeFromActivePurgeKeys(purgeKey);
                            continue;
                        }

                        long remaining = esOps.getEntityCount(esQuery);

                        if (remaining == 0) {
                            markOrphanedPurgeCompleted(purgeKey, status);
                        } else {
                            int resubmitCount = status.has("resubmitCount") ? status.get("resubmitCount").asInt() : 0;
                            if (resubmitCount < ORPHAN_MAX_RESUBMITS) {
                                LOG.info("BulkPurge: Auto-recovering orphaned purge for {} (remaining={}, attempt={})",
                                        purgeKey, remaining, resubmitCount + 1);
                                resubmitOrphanedPurge(purgeKey, status, resubmitCount + 1);
                            } else {
                                LOG.error("BulkPurge: Orphaned purge for {} exceeded max resubmits ({}). Marking as FAILED.",
                                        purgeKey, ORPHAN_MAX_RESUBMITS);
                                markOrphanedPurgeFailed(purgeKey, status,
                                        "Exceeded max resubmits (" + ORPHAN_MAX_RESUBMITS + ") -- manual intervention required");
                            }
                        }
                    } else if ("FAILED".equals(purgeStatus)) {
                        boolean isOrphanRecovery = status.has("orphanRecovery") && status.get("orphanRecovery").asBoolean();
                        int resubmitCount = status.has("resubmitCount") ? status.get("resubmitCount").asInt() : 0;
                        String esQuery = status.has("esQuery") ? status.get("esQuery").asText() : null;

                        if (isOrphanRecovery && esQuery != null && resubmitCount < ORPHAN_MAX_RESUBMITS) {
                            long remaining = esOps.getEntityCount(esQuery);
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
            // Force-release the stale lock from the dead process
            String lockKey = REDIS_LOCK_PREFIX + purgeKey;
            try {
                redisService.forceReleaseDistributedLock(lockKey);
                LOG.info("BulkPurge: Force-released stale lock for orphaned purge {}", purgeKey);
            } catch (Exception e) {
                LOG.warn("BulkPurge: Failed to force-release stale lock for {}, resubmit may fail", purgeKey, e);
            }

            // Clean stale cancel signal
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
                executionCallback.execute(ctx, lockKey, redisKey);
            } catch (java.util.concurrent.RejectedExecutionException ree) {
                LOG.error("BulkPurge: Coordinator pool rejected resubmission for {} -- pool is full", purgeKey, ree);
                activePurges.remove(purgeKey);
                removeFromActivePurgeKeys(purgeKey);
                ctx.status = "FAILED";
                ctx.error = "Resubmission rejected: coordinator pool at capacity";
                writeRedisStatus(ctx);
                return;
            }

            // Update the ORIGINAL requestId to RESUBMITTED
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
}
