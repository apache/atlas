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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;

/**
 * Shared purge policy helpers for failure classification and summary attachment.
 */
public final class PurgeUtils {
    private PurgeUtils() {
    }

    /**
     * Returns true for skippable purge failure codes (not-found, not-deleted).
     */
    public static boolean isSkippablePurgeFailureCode(String errorCode) {
        return classifyPurgeFailureCode(errorCode) == PurgeFailureCategory.SKIPPABLE;
    }

    /**
     * Classifies a purge failure error code for stats, audit, and batch accounting.
     */
    public static PurgeFailureCategory classifyPurgeFailureCode(String errorCode) {
        if (AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode().equals(errorCode)
                || AtlasErrorCode.NOT_IN_DELETED_STATE.getErrorCode().equals(errorCode)) {
            return PurgeFailureCategory.SKIPPABLE;
        }
        if (AtlasErrorCode.INVALID_GUID.getErrorCode().equals(errorCode)
                || AtlasErrorCode.TYPE_NAME_NOT_FOUND.getErrorCode().equals(errorCode)) {
            return PurgeFailureCategory.PRE_VALIDATION_FAILURE;
        }
        return PurgeFailureCategory.EXECUTION_FAILURE;
    }

    /**
     * Returns true when the error code represents an execution-time failure (not pre-validation or skippable).
     */
    public static boolean isExecutionFailureCode(String errorCode) {
        return classifyPurgeFailureCode(errorCode) == PurgeFailureCategory.EXECUTION_FAILURE;
    }

    /**
     * Builds a {@link PurgeSummary} from incremental {@link PurgeExecutionStats} counters (O(1)).
     */
    public static PurgeSummary buildPurgeSummary(PurgeExecutionStats stats) {
        PurgeSummary summary = new PurgeSummary(
                stats.getOriginallyRequestedGuids().size(),
                stats.getPurgedCount(),
                stats.getPurgedDependenciesCount(),
                stats.getFailedCount(),
                stats.getFailedDependenciesCount(),
                stats.getSkippedCount());
        summary.setValidGuidCount(stats.getValidGuidCount());
        summary.setExpandedEntityCount(stats.getExpandedEntityCount());
        summary.setBatchCount(stats.getBatchCount());
        summary.setSkippedRequestedCount(stats.getSkippedRequestedCount());
        summary.setSkippedDependenciesCount(stats.getSkippedDependenciesCount());
        summary.setUnprocessedCount(stats.getReconciledUnprocessedCount());
        summary.setExecutionFailed(stats.isExecutionFailed());
        return summary;
    }

    /**
     * Default purge failure classification for a single GUID.
     */
    public static FailedEntity classifyPurgeFailure(String guid, Throwable error, String defaultMessage) {
        if (error instanceof AtlasBaseException) {
            AtlasBaseException atlasError = (AtlasBaseException) error;
            return new FailedEntity(guid, atlasError.getAtlasErrorCode().getErrorCode(),
                    error.getMessage());
        }

        String message = defaultMessage != null ? defaultMessage : (error != null ? error.getMessage() : null);
        return new FailedEntity(guid, AtlasErrorCode.INTERNAL_ERROR.getErrorCode(), message);
    }

    /**
     * Builds {@code summary} from incremental {@link PurgeExecutionStats} counters (O(1)).
     */
    public static void attachPurgeSummary(EntityMutationResponse response, PurgeExecutionStats stats) {
        if (response == null || stats == null) {
            return;
        }

        PurgeSummary summary = buildPurgeSummary(stats);
        stats.validateSummaryBalances(summary);
        response.setSummary(summary);
    }
}
