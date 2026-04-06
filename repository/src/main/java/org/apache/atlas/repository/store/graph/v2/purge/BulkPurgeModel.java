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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasConfiguration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data classes and shared constants for the BulkPurge subsystem.
 * All inner classes are DTOs with no behavior beyond serialization helpers.
 */
public final class BulkPurgeModel {

    // Shared constants used across the purge subsystem
    public static final String REDIS_KEY_PREFIX        = "bulk_purge:";
    public static final String REDIS_LOCK_PREFIX       = "bulk_purge_lock:";
    public static final String REDIS_ACTIVE_PURGE_KEYS = "bulk_purge_active_keys";
    public static final String REDIS_CANCEL_PREFIX     = "bulk_purge_cancel:";
    public static final String PURGE_MODE_CONNECTION   = "CONNECTION";
    public static final String PURGE_MODE_QN_PREFIX    = "QUALIFIED_NAME_PREFIX";

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private BulkPurgeModel() {} // Utility class — no instances

    // ======================== PURGE CONTEXT ========================

    /**
     * Holds state for an active purge operation.
     */
    public static class PurgeContext {
        public final String requestId;
        public final String purgeKey;
        public final String purgeMode;
        public final String submittedBy;
        public final String esQuery;
        public final long   submittedAt;
        public final boolean deleteConnection;
        public final int    workerCountOverride;

        public volatile String  status;
        public volatile String  error;
        public volatile long    lastHeartbeat;
        public volatile long    totalDiscovered;
        public volatile long    remainingAfterCleanup = -1;
        public volatile int     workerCount;
        public volatile boolean cancelRequested;
        public volatile boolean connectionDeleted;
        public volatile int     resubmitCount;
        public volatile String  currentPhase;
        public volatile boolean stalled;
        public volatile boolean orphanRecovery;

        public final AtomicInteger totalDeleted            = new AtomicInteger(0);
        public final AtomicInteger totalFailed             = new AtomicInteger(0);
        public final AtomicInteger completedBatches        = new AtomicInteger(0);
        public final AtomicInteger lastProcessedBatchIndex = new AtomicInteger(0);
        public final AtomicLong    lastProgressTimestamp    = new AtomicLong(0);

        // P2: Stored for cancel/interrupt support
        public volatile ExecutorService workerPool;
        public volatile List<Future<?>> workerFutures;

        public PurgeContext(String requestId, String purgeKey, String purgeMode,
                            String submittedBy, String esQuery,
                            boolean deleteConnection, int workerCountOverride) {
            this.requestId          = requestId;
            this.purgeKey           = purgeKey;
            this.purgeMode          = purgeMode;
            this.submittedBy        = submittedBy;
            this.esQuery            = esQuery;
            this.submittedAt        = System.currentTimeMillis();
            this.deleteConnection   = deleteConnection;
            this.workerCountOverride = workerCountOverride;
        }

        public String toJson() {
            try {
                return MAPPER.writeValueAsString(toStatusMap());
            } catch (Exception e) {
                return "{}";
            }
        }

        public Map<String, Object> toStatusMap() {
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

    // ======================== BATCH WORK ========================

    /**
     * Represents a batch of vertex IDs to delete.
     */
    public static class BatchWork {
        public static final BatchWork POISON_PILL = new BatchWork(Collections.emptyList(), -1);

        public final List<String> vertexIds;
        public final int batchIndex;

        public BatchWork(List<String> vertexIds, int batchIndex) {
            this.vertexIds = vertexIds;
            this.batchIndex = batchIndex;
        }
    }

    // ======================== BATCH CLEANUP METADATA ========================

    /**
     * Per-batch metadata for inline cleanup. Discarded after each batch completes.
     * O(batch_size) memory -- no accumulation across batches.
     */
    public static class BatchCleanupMetadata {
        public final Set<String>                        externalLineageVerts      = new HashSet<>();
        public final Set<String>                        externalVerts             = new HashSet<>();
        public final Set<String>                        directTagVerts            = new HashSet<>();
        public final Set<String>                        propagatedOnlyTagVerts    = new HashSet<>();
        public final Set<String>                        deletedGuids              = new HashSet<>();
        public final Set<String>                        datasetGuids              = new HashSet<>();
        public final List<String[]>                     termInfo                  = new ArrayList<>(); // [guid, qn, name]
        public final List<CategoryCleanupInfo>          categoryCleanup           = new ArrayList<>();

        // Access control cleanup metadata
        public final List<PersonaCleanupInfo>           personaCleanup            = new ArrayList<>();
        public final List<PurposeCleanupInfo>           purposeCleanup            = new ArrayList<>();
        public final List<String>                       dataProductGuids          = new ArrayList<>();
        public final List<CollectionCleanupInfo>        collectionCleanup         = new ArrayList<>();
        public final List<ConnectionCleanupInfo>        connectionCleanup         = new ArrayList<>();
        public final List<StakeholderTitleCleanupInfo>  stakeholderTitleCleanup   = new ArrayList<>();
    }

    // ======================== CLEANUP INFO DTOs ========================

    /**
     * Cleanup info for a single deleted AtlasGlossaryCategory.
     */
    public static class CategoryCleanupInfo {
        public final List<String> childVertexIds;
        public final String       categoryQN;
        public final List<String> termVertexIds;

        public CategoryCleanupInfo(List<String> childVertexIds, String categoryQN, List<String> termVertexIds) {
            this.childVertexIds = childVertexIds;
            this.categoryQN    = categoryQN;
            this.termVertexIds = termVertexIds;
        }
    }

    public static class PersonaCleanupInfo {
        public final String guid;
        public final String roleId;
        public final String qualifiedName;

        public PersonaCleanupInfo(String guid, String roleId, String qualifiedName) {
            this.guid          = guid;
            this.roleId        = roleId;
            this.qualifiedName = qualifiedName;
        }
    }

    public static class PurposeCleanupInfo {
        public final String guid;
        public final String qualifiedName;

        public PurposeCleanupInfo(String guid, String qualifiedName) {
            this.guid          = guid;
            this.qualifiedName = qualifiedName;
        }
    }

    public static class CollectionCleanupInfo {
        public final String guid;

        public CollectionCleanupInfo(String guid) {
            this.guid = guid;
        }
    }

    public static class ConnectionCleanupInfo {
        public final String guid;

        public ConnectionCleanupInfo(String guid) {
            this.guid = guid;
        }
    }

    public static class StakeholderTitleCleanupInfo {
        public final String guid;
        public final String vertexId;

        public StakeholderTitleCleanupInfo(String guid, String vertexId) {
            this.guid     = guid;
            this.vertexId = vertexId;
        }
    }
}
