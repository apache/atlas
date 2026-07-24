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
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditRowKind;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.ogm.AtlasAuditEntryDTO;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared purge policy helpers, summary attachment, and purge audit read helpers.
 */
public final class PurgeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeUtils.class);

    enum FailureCategory {
        SKIPPABLE,
        PRE_VALIDATION_FAILURE,
        EXECUTION_FAILURE
    }

    private PurgeUtils() {
    }

    public static boolean isSkippablePurgeFailureCode(String errorCode) {
        return classifyPurgeFailureCode(errorCode) == FailureCategory.SKIPPABLE;
    }

    public static boolean isExecutionFailureCode(String errorCode) {
        return classifyPurgeFailureCode(errorCode) == FailureCategory.EXECUTION_FAILURE;
    }

    static FailureCategory classifyPurgeFailureCode(String errorCode) {
        if (AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode().equals(errorCode)
                || AtlasErrorCode.NOT_IN_DELETED_STATE.getErrorCode().equals(errorCode)) {
            return FailureCategory.SKIPPABLE;
        }
        if (AtlasErrorCode.INVALID_GUID.getErrorCode().equals(errorCode)
                || AtlasErrorCode.TYPE_NAME_NOT_FOUND.getErrorCode().equals(errorCode)) {
            return FailureCategory.PRE_VALIDATION_FAILURE;
        }
        return FailureCategory.EXECUTION_FAILURE;
    }

    public static PurgeSummary buildPurgeSummary(PurgeExecutionStats stats, String runId) {
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
        summary.setRunId(runId);
        return summary;
    }

    public static void preScanGuids(Set<String> guids, Set<String> validGuids, List<FailedEntity> failedEntities,
                                    AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        if (guids == null) {
            return;
        }

        for (String guid : guids) {
            FailedEntity failure = validatePurgePreconditions(guid, graph, typeRegistry);
            if (failure != null) {
                failedEntities.add(failure);
            } else {
                validGuids.add(guid);
            }
        }
    }

    public static FailedEntity validatePurgePreconditions(String guid, AtlasGraph graph,
                                                          AtlasTypeRegistry typeRegistry) {
        if (!isValidUuid(guid)) {
            LOG.debug("Purge request ignored for invalid GUID format: {}", guid);
            return createPreScanFailure(guid, AtlasErrorCode.INVALID_GUID, guid);
        }

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (vertex == null) {
            LOG.debug("Purge request ignored for non-existent entity: guid={}", guid);
            return createPreScanFailure(guid, AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        if (AtlasGraphUtilsV2.getState(vertex) != Status.DELETED) {
            LOG.debug("Purge request ignored for entity not in DELETED state: guid={}", guid);
            return createPreScanFailure(guid, AtlasErrorCode.NOT_IN_DELETED_STATE, guid);
        }

        String typeName = AtlasGraphUtilsV2.getTypeName(vertex);
        if (StringUtils.isBlank(typeName)) {
            LOG.debug("Purge request ignored for entity with missing type name: guid={}", guid);
            return createPreScanFailure(guid, AtlasErrorCode.TYPE_NAME_NOT_FOUND, "null");
        }
        if (!typeRegistry.isRegisteredType(typeName)) {
            LOG.debug("Purge request ignored for entity with unregistered type: guid={}, typeName={}", guid, typeName);
            return createPreScanFailure(guid, AtlasErrorCode.TYPE_NAME_NOT_FOUND, typeName);
        }

        return null;
    }

    public static FailedEntity createPreScanFailure(String guid, AtlasErrorCode errorCode, String... messageArgs) {
        return new FailedEntity(guid,
                errorCode.getErrorCode(),
                errorCode.getFormattedErrorMessage(messageArgs));
    }

    public static boolean isValidUuid(String guid) {
        if (guid == null) {
            return false;
        }
        try {
            java.util.UUID.fromString(guid);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static FailedEntity classifyPurgeFailure(String guid, Throwable error, String defaultMessage) {
        if (error instanceof AtlasBaseException) {
            AtlasBaseException atlasError = (AtlasBaseException) error;
            return new FailedEntity(guid, atlasError.getAtlasErrorCode().getErrorCode(),
                    error.getMessage());
        }

        String message = defaultMessage != null ? defaultMessage : (error != null ? error.getMessage() : null);
        return new FailedEntity(guid, AtlasErrorCode.INTERNAL_ERROR.getErrorCode(), message);
    }

    public static void attachPurgeSummary(EntityMutationResponse response, PurgeExecutionStats stats, String runId) {
        if (response == null || stats == null) {
            return;
        }

        PurgeSummary summary = buildPurgeSummary(stats, runId);
        stats.validateSummaryBalances(summary);
        response.setSummary(summary);
    }

    public static String buildGuidParams(Collection<String> guids) {
        return guids.stream()
                .filter(Objects::nonNull)
                .sorted()
                .collect(Collectors.joining(","));
    }

    public static boolean isPurgeSummaryAudit(AtlasAuditEntry entry) {
        return entry != null && AuditRowKind.SUMMARY == entry.getAuditRowKind();
    }

    public static boolean isPurgeBatchAudit(AtlasAuditEntry entry) {
        return entry != null && AuditRowKind.BATCH == entry.getAuditRowKind();
    }

    public static PurgeSummary parsePurgeSummary(AtlasAuditEntry entry) {
        if (entry == null || StringUtils.isBlank(entry.getResult())) {
            return null;
        }

        String result = entry.getResult().trim();
        if (!result.startsWith("{")) {
            return null;
        }

        try {
            PurgeSummary summary = AtlasJson.fromJson(result, PurgeSummary.class);
            return hasPurgeSummary(summary) ? summary : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean hasPurgeSummary(PurgeSummary summary) {
        if (summary == null) {
            return false;
        }

        return StringUtils.isNotBlank(summary.getRunId())
                || summary.getRequestedCount() > 0
                || summary.getExpandedEntityCount() > 0
                || summary.getBatchCount() > 0
                || summary.getValidGuidCount() > 0;
    }

    public static String resolveRunId(AtlasAuditEntry auditEntry) {
        if (auditEntry == null || StringUtils.isBlank(auditEntry.getRunId())) {
            return null;
        }

        return auditEntry.getRunId();
    }

    private static boolean isLegacyPurgeAudit(AtlasAuditEntry entry) {
        return entry != null
                && entry.getAuditRowKind() == null
                && (entry.getOperation() == AuditOperation.PURGE || entry.getOperation() == AuditOperation.AUTO_PURGE);
    }

    public static boolean isCorrelatedPurgeAudit(AtlasAuditEntry entry) {
        return isPurgeSummaryAudit(entry) || isPurgeBatchAudit(entry) || isLegacyPurgeAudit(entry);
    }

    public static List<AtlasAuditEntry> excludeBatchRowsFromResults(List<AtlasAuditEntry> entries) {
        List<AtlasAuditEntry> ret = new ArrayList<>();

        if (entries == null) {
            return ret;
        }

        for (AtlasAuditEntry entry : entries) {
            if (!isPurgeBatchAudit(entry)) {
                ret.add(entry);
            }
        }

        return ret;
    }

    public static void excludeBatchRowsFromAuditSearch(AuditSearchParameters auditSearchParameters) {
        if (auditSearchParameters == null) {
            return;
        }

        SearchParameters.FilterCriteria excludeBatch = new SearchParameters.FilterCriteria();
        excludeBatch.setAttributeName(AtlasAuditEntryDTO.ATTRIBUTE_AUDIT_ROW_KIND);
        excludeBatch.setOperator(SearchParameters.Operator.NEQ);
        excludeBatch.setAttributeValue(AuditRowKind.BATCH.name());

        SearchParameters.FilterCriteria originalFilters = auditSearchParameters.getAuditFilters();
        SearchParameters.FilterCriteria newFilters      = new SearchParameters.FilterCriteria();
        newFilters.setCondition(SearchParameters.FilterCriteria.Condition.AND);
        newFilters.setCriterion(new ArrayList<>());

        if (originalFilters != null) {
            newFilters.getCriterion().add(originalFilters);
        }

        newFilters.getCriterion().add(excludeBatch);
        auditSearchParameters.setAuditFilters(newFilters);
    }

    public static List<String> paginateStringList(List<String> values, int limit, int offset) {
        if (values == null || values.isEmpty()) {
            return new ArrayList<>();
        }

        int from = Math.min(Math.max(offset, 0), values.size());
        int to   = Math.min(from + Math.max(limit, 0), values.size());
        return new ArrayList<>(values.subList(from, to));
    }

    public static boolean hasRunIdFilter(SearchParameters.FilterCriteria auditFilters) {
        if (auditFilters == null) {
            return false;
        }

        if (AtlasAuditEntryDTO.ATTRIBUTE_RUN_ID.equals(auditFilters.getAttributeName())
                && SearchParameters.Operator.EQ.equals(auditFilters.getOperator())
                && StringUtils.isNotEmpty(auditFilters.getAttributeValue())) {
            return true;
        }

        if (auditFilters.getCriterion() != null) {
            for (SearchParameters.FilterCriteria each : auditFilters.getCriterion()) {
                if (hasRunIdFilter(each)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static List<String> collectPurgedGuidsFromBatchEntries(List<AtlasAuditEntry> batchRows) {
        List<String> orderedEntityGuids = new ArrayList<>();
        Set<String>  seen               = new LinkedHashSet<>();

        if (batchRows == null) {
            return orderedEntityGuids;
        }

        for (AtlasAuditEntry batchRow : batchRows) {
            appendPurgedGuidsFromBatchResult(batchRow.getResult(), orderedEntityGuids, seen);
        }

        return orderedEntityGuids;
    }

    private static void appendPurgedGuidsFromBatchResult(String result, List<String> orderedEntityGuids, Set<String> seen) {
        if (StringUtils.isBlank(result)) {
            return;
        }

        for (String guid : result.split(",")) {
            guid = guid.trim();
            if (StringUtils.isNotBlank(guid) && seen.add(guid)) {
                orderedEntityGuids.add(guid);
            }
        }
    }
}
