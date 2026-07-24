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
package org.apache.atlas.services;

import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditRowKind;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.purge.PurgeExecutionStats;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Purge audit write path: batch/summary graph writes and purgefailure.log emission.
 */
public final class PurgeAuditWriter {
    private static final Logger LOG               = LoggerFactory.getLogger(PurgeAuditWriter.class);
    private static final Logger PURGE_FAILURE_LOG = LoggerFactory.getLogger("PURGE_FAILURE");

    private PurgeAuditWriter() {
    }

    public static void logFailures(String runId, AuditOperation operation, List<FailedEntity> failedEntities) {
        if (failedEntities == null) {
            return;
        }

        for (FailedEntity failedEntity : failedEntities) {
            PURGE_FAILURE_LOG.error("[PURGE_FAILURE] runId={} op={} guid={} code={} msg={}",
                    runId, operation, failedEntity.getGuid(), failedEntity.getErrorCode(), failedEntity.getErrorMessage());
        }
    }

    public static void writeBatch(AtlasAuditService auditService, AuditOperation operation, String runId,
                                  Set<String> batchInputGuids, EntityMutationResponse batchResponse) {
        if (batchResponse == null || CollectionUtils.isEmpty(batchInputGuids)) {
            return;
        }

        logFailures(runId, operation, batchResponse.getFailedEntities());

        if (auditService == null || operation == null) {
            return;
        }

        String params = PurgeUtils.buildGuidParams(batchInputGuids);

        List<AtlasEntityHeader> purgedEntities = batchResponse.getPurgedEntities();
        String result;
        long   resultCount;

        if (CollectionUtils.isEmpty(purgedEntities)) {
            result      = "";
            resultCount = 0;
        } else {
            result = PurgeUtils.buildGuidParams(purgedEntities.stream()
                    .map(AtlasEntityHeader::getGuid)
                    .collect(Collectors.toList()));
            resultCount = purgedEntities.size();
        }

        try {
            auditService.add(operation, params, result, resultCount, runId, AuditRowKind.BATCH);
        } catch (Exception e) {
            LOG.warn("Failed to write purge batch audit entry", e);
        }
    }

    public static void finishRun(AtlasAuditService auditService, AuditOperation operation, String runId,
                                 Set<String> originallyRequestedGuids, PurgeExecutionStats stats) {
        if (stats == null || CollectionUtils.isEmpty(originallyRequestedGuids)) {
            return;
        }

        writeSummary(auditService, operation, runId, originallyRequestedGuids, stats);
    }

    private static void writeSummary(AtlasAuditService auditService, AuditOperation operation, String runId,
                                     Set<String> originallyRequestedGuids, PurgeExecutionStats stats) {
        if (auditService == null || operation == null) {
            return;
        }

        PurgeSummary summary = PurgeUtils.buildPurgeSummary(stats, runId);
        String params      = PurgeUtils.buildGuidParams(originallyRequestedGuids);
        String result      = AtlasJson.toJson(summary);
        long   resultCount = summary.getPurgedCount();

        try {
            auditService.add(operation, params, result, resultCount, runId, AuditRowKind.SUMMARY);
        } catch (Exception e) {
            LOG.warn("Failed to write purge summary audit entry", e);
        }
    }
}
