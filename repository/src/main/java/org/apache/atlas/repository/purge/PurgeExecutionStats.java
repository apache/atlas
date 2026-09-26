/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.purge;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Mutable run-context for purge execution accounting. Counters are updated sequentially
 * during orchestration aggregation and reconciliation.
 * <p>
 * This class is strictly single-thread-owned. Mutations must only happen on the main
 * orchestrator thread after worker shutdown.
 * <p>
 * Request-scope balance: {@code requestedCount = purgedCount + failedCount + skippedRequestedCount}.
 * Expanded-scope balance uses {@link #getExpandedEntityCount()} and deduplicated outcome counters.
 */
public final class PurgeExecutionStats {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeExecutionStats.class);

    private final Set<String> originallyRequestedGuids;
    private final Set<String> producedDeletionCandidates;
    private final Set<String> accountedPurgedGuids   = new HashSet<>();
    private final Set<String> accountedOutcomeGuids  = new HashSet<>();

    private long validGuidCount;
    private long batchCount;
    private long reconciledUnprocessedCount;
    private long purgedCount;
    private long purgedDependenciesCount;
    private long failedCount;
    private long failedDependenciesCount;
    private long skippedRequestedCount;
    private long skippedDependenciesCount;
    private boolean executionFailed;

    public PurgeExecutionStats(Set<String> originallyRequestedGuids, long validGuidCount) {
        this.originallyRequestedGuids    = originallyRequestedGuids;
        this.validGuidCount              = validGuidCount;
        this.producedDeletionCandidates  = new LinkedHashSet<>();
    }

    public Set<String> getOriginallyRequestedGuids() {
        return originallyRequestedGuids;
    }

    public Set<String> getProducedDeletionCandidates() {
        return producedDeletionCandidates;
    }

    public long getValidGuidCount() {
        return validGuidCount;
    }

    public long getExpandedEntityCount() {
        return producedDeletionCandidates.size();
    }

    public long getSkippedCount() {
        return skippedRequestedCount + skippedDependenciesCount;
    }

    public long getReconciledUnprocessedCount() {
        return reconciledUnprocessedCount;
    }

    public long getPurgedCount() {
        return purgedCount;
    }

    public long getPurgedDependenciesCount() {
        return purgedDependenciesCount;
    }

    public long getFailedCount() {
        return failedCount;
    }

    public long getFailedDependenciesCount() {
        return failedDependenciesCount;
    }

    public long getSkippedRequestedCount() {
        return skippedRequestedCount;
    }

    public long getSkippedDependenciesCount() {
        return skippedDependenciesCount;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public boolean isExecutionFailed() {
        return executionFailed;
    }

    public void markExecutionFailed() {
        executionFailed = true;
    }

    public void recordFailures(List<FailedEntity> failures) {
        if (failures == null) {
            return;
        }

        for (FailedEntity failedEntity : failures) {
            recordFailure(failedEntity);
        }
    }

    public void recordBatchOutcome(EntityMutationResponse batchResponse, Set<String> batchGuids) {
        if (batchGuids == null || batchGuids.isEmpty()) {
            return;
        }

        List<AtlasEntityHeader> purgedEntities = batchResponse != null ? batchResponse.getPurgedEntities() : null;
        List<FailedEntity>      failedEntities = batchResponse != null ? batchResponse.getFailedEntities() : null;
        recordBatchOutcome(purgedEntities, failedEntities, batchGuids);
    }

    public void recordBatchOutcome(List<AtlasEntityHeader> purgedEntities, List<FailedEntity> failedEntities,
                                   Set<String> batchGuids) {
        if (batchGuids == null || batchGuids.isEmpty()) {
            return;
        }

        batchCount++;

        applyOutcomes(purgedEntities, failedEntities);

        validateBatchInvariant(batchGuids, purgedEntities, failedEntities);
    }

    public void recordBatchThrowable(Set<String> batchGuids, List<FailedEntity> batchFailures) {
        if (batchGuids == null || batchGuids.isEmpty()) {
            return;
        }

        batchCount++;

        applyOutcomes(null, batchFailures);

        validateBatchInvariant(batchGuids, null, batchFailures);
    }

    public void recordReconciledUnprocessed(long count) {
        reconciledUnprocessedCount += count;
    }

    void recordPurged(String guid) {
        if (guid == null || !accountedPurgedGuids.add(guid)) {
            return;
        }

        if (accountedOutcomeGuids.remove(guid)) {
            decrementOutcome(guid);
        }

        accountedOutcomeGuids.add(guid);

        if (originallyRequestedGuids.contains(guid)) {
            purgedCount++;
        } else {
            purgedDependenciesCount++;
        }
    }

    public void recordFailure(FailedEntity failedEntity) {
        if (failedEntity == null) {
            return;
        }

        String guid      = failedEntity.getGuid();
        String errorCode = failedEntity.getErrorCode();

        if (guid == null || accountedPurgedGuids.contains(guid) || !accountedOutcomeGuids.add(guid)) {
            return;
        }

        boolean requested = originallyRequestedGuids.contains(guid);

        incrementFailureCounters(errorCode, requested);

        if (PurgeUtils.isExecutionFailureCode(errorCode)) {
            executionFailed = true;
        }
    }

    private void incrementFailureCounters(String errorCode, boolean requested) {
        if (PurgeUtils.isSkippablePurgeFailureCode(errorCode)) {
            if (requested) {
                skippedRequestedCount++;
            } else {
                skippedDependenciesCount++;
            }
        } else if (requested) {
            failedCount++;
        } else {
            failedDependenciesCount++;
        }
    }

    /**
     * Rebuilds stats by scanning a completed response. Used by tests and legacy callers that
     * do not run through the worker pipeline.
     */
    public static PurgeExecutionStats fromResponse(EntityMutationResponse response,
                                                   Set<String> originallyRequestedGuids,
                                                   long validGuidCount) {
        PurgeExecutionStats stats = new PurgeExecutionStats(originallyRequestedGuids, validGuidCount);
        stats.applyOutcomes(
                response != null ? response.getPurgedEntities() : null,
                response != null ? response.getFailedEntities() : null);

        return stats;
    }

    private void applyOutcomes(List<AtlasEntityHeader> purgedEntities, List<FailedEntity> failedEntities) {
        if (purgedEntities != null) {
            for (AtlasEntityHeader header : purgedEntities) {
                recordPurged(header.getGuid());
            }
        }

        if (failedEntities != null) {
            for (FailedEntity failedEntity : failedEntities) {
                recordFailure(failedEntity);
            }
        }
    }

    public void validateSummaryBalances(PurgeSummary summary) {
        long requestedBalance = summary.getPurgedCount() + summary.getFailedCount() + summary.getSkippedRequestedCount();
        if (summary.getRequestedCount() != requestedBalance) {
            LOG.warn("Purge summary request-scope balance mismatch: requestedCount={}, purged+failed+skippedRequested={}",
                    summary.getRequestedCount(), requestedBalance);
        }

        // unprocessedCount is a breakdown of reconciled shutdown failures already included in failedCount.
        long expandedBalance = summary.getPurgedCount() + summary.getPurgedDependenciesCount()
                + summary.getFailedCount() + summary.getFailedDependenciesCount()
                + summary.getSkippedRequestedCount() + summary.getSkippedDependenciesCount();
        if (summary.getExpandedEntityCount() > 0 && summary.getExpandedEntityCount() != expandedBalance) {
            LOG.warn("Purge summary expanded-scope balance mismatch: expandedEntityCount={}, outcomeTotal={}",
                    summary.getExpandedEntityCount(), expandedBalance);
        }
    }

    private void decrementOutcome(String guid) {
        boolean requested = originallyRequestedGuids.contains(guid);

        if (requested) {
            if (purgedCount > 0) {
                purgedCount--;
                return;
            }
            if (failedCount > 0) {
                failedCount--;
                return;
            }
            if (skippedRequestedCount > 0) {
                skippedRequestedCount--;
            }
        } else {
            if (purgedDependenciesCount > 0) {
                purgedDependenciesCount--;
                return;
            }
            if (failedDependenciesCount > 0) {
                failedDependenciesCount--;
                return;
            }
            if (skippedDependenciesCount > 0) {
                skippedDependenciesCount--;
            }
        }
    }

    private static void validateBatchInvariant(Set<String> batchGuids, List<AtlasEntityHeader> purgedEntities,
                                               List<FailedEntity> failedEntities) {
        int batchInputCount = batchGuids.size();
        int batchPurged     = purgedEntities != null ? purgedEntities.size() : 0;
        int batchFailed     = 0;
        int batchSkipped    = 0;

        if (failedEntities != null) {
            for (FailedEntity failedEntity : failedEntities) {
                if (PurgeUtils.isSkippablePurgeFailureCode(failedEntity.getErrorCode())) {
                    batchSkipped++;
                } else {
                    batchFailed++;
                }
            }
        }

        int batchAccounted = batchPurged + batchFailed + batchSkipped;
        if (batchAccounted != batchInputCount) {
            LOG.warn("Purge batch accounting mismatch: batchSize={}, purged={}, failed={}, skipped={}",
                    batchInputCount, batchPurged, batchFailed, batchSkipped);
        }
    }
}
