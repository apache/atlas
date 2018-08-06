/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.impexp;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.clusterinfo.AtlasCluster;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.ExportImportAuditEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

@Component
public class AuditsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AuditsWriter.class);
    private static final String CLUSTER_NAME_DEFAULT = "default";

    private ClusterService clusterService;
    private ExportImportAuditService auditService;

    private ExportAudits auditForExport = new ExportAudits();
    private ImportAudits auditForImport = new ImportAudits();

    @Inject
    public AuditsWriter(ClusterService clusterService, ExportImportAuditService auditService) {
        this.clusterService = clusterService;
        this.auditService = auditService;
    }

    public void write(String userName, AtlasExportResult result, long startTime, long endTime, List<String> entityCreationOrder) throws AtlasBaseException {
        auditForExport.add(userName, result, startTime, endTime, entityCreationOrder);
    }

    public void write(String userName, AtlasImportResult result, long startTime, long endTime, List<String> entityCreationOrder) throws AtlasBaseException {
        auditForImport.add(userName, result, startTime, endTime, entityCreationOrder);
    }

    private boolean isReplicationOptionSet(Map<String, ? extends Object> options, String replicatedKey) {
        return options.containsKey(replicatedKey);
    }

    private void updateReplicationAttribute(boolean isReplicationSet, String clusterName,
                                            List<String> exportedGuids,
                                            String attrNameReplicated) throws AtlasBaseException {
        if (!isReplicationSet) {
            return;
        }

        AtlasCluster cluster = saveCluster(clusterName);
        clusterService.updateEntityWithCluster(cluster, exportedGuids, attrNameReplicated);
    }

    private String getClusterNameFromOptions(Map options, String key) {
        return options.containsKey(key)
                ? (String) options.get(key)
                : "";
    }

    private void addAuditEntry(String userName, String sourceCluster, String targetCluster, String operation,
                               String result, long startTime, long endTime, boolean hasData) throws AtlasBaseException {
        if(!hasData) return;

        ExportImportAuditEntry entry = new ExportImportAuditEntry();

        entry.setUserName(userName);
        entry.setSourceClusterName(sourceCluster);
        entry.setTargetClusterName(targetCluster);
        entry.setOperation(operation);
        entry.setResultSummary(result);
        entry.setStartTime(startTime);
        entry.setEndTime(endTime);

        auditService.save(entry);
        LOG.info("addAuditEntry: user: {}, source: {}, target: {}, operation: {}", entry.getUserName(),
                            entry.getSourceClusterName(), entry.getTargetClusterName(), entry.getOperation());
    }

    private AtlasCluster saveCluster(String clusterName) throws AtlasBaseException {
        AtlasCluster cluster = new AtlasCluster(clusterName, clusterName);
        return clusterService.save(cluster);
    }

    public static String getCurrentClusterName() {
        try {
            return ApplicationProperties.get().getString(AtlasConstants.CLUSTER_NAME_KEY, CLUSTER_NAME_DEFAULT);
        } catch (AtlasException e) {
            LOG.error("getCurrentClusterName", e);
        }

        return "";
    }

    private class ExportAudits {
        private AtlasExportRequest request;
        private AtlasCluster cluster;
        private String targetClusterName;
        private String optionKeyReplicatedTo;
        private boolean replicationOptionState;

        public void add(String userName, AtlasExportResult result, long startTime, long endTime, List<String> entitityGuids) throws AtlasBaseException {
            optionKeyReplicatedTo = AtlasExportRequest.OPTION_KEY_REPLICATED_TO;
            request = result.getRequest();
            cluster = saveCluster(getCurrentClusterName());
            replicationOptionState = isReplicationOptionSet(request.getOptions(), optionKeyReplicatedTo);
            targetClusterName = getClusterNameFromOptions(request.getOptions(), optionKeyReplicatedTo);

            addAuditEntry(userName,
                    cluster.getName(), targetClusterName,
                    ExportImportAuditEntry.OPERATION_EXPORT,
                    AtlasType.toJson(result), startTime, endTime, !entitityGuids.isEmpty());

            updateReplicationAttributeForExport(entitityGuids, request);
        }

        private void updateReplicationAttributeForExport(List<String> entityGuids, AtlasExportRequest request) throws AtlasBaseException {
            if(!replicationOptionState) return;

            updateReplicationAttribute(replicationOptionState, targetClusterName, entityGuids, Constants.ATTR_NAME_REPLICATED_TO_CLUSTER);
        }
    }

    private class ImportAudits {
        private AtlasImportRequest request;
        private boolean replicationOptionState;
        private AtlasCluster cluster;
        private String optionKeyReplicatedFrom;
        private AtlasImportResult result;

        public void add(String userName, AtlasImportResult result, long startTime, long endTime, List<String> entitityGuids) throws AtlasBaseException {
            this.result = result;
            request = result.getRequest();
            optionKeyReplicatedFrom = AtlasImportRequest.OPTION_KEY_REPLICATED_FROM;
            replicationOptionState = isReplicationOptionSet(request.getOptions(), optionKeyReplicatedFrom);
            cluster = saveCluster(getClusterNameFromOptionsState());

            String sourceCluster = getClusterNameFromOptions(request.getOptions(), optionKeyReplicatedFrom);
            addAuditEntry(userName,
                    sourceCluster, cluster.getName(),
                    ExportImportAuditEntry.OPERATION_EXPORT, AtlasType.toJson(result), startTime, endTime, !entitityGuids.isEmpty());

            updateReplicationAttributeForImport(entitityGuids);
        }

        private void updateReplicationAttributeForImport(List<String> entityGuids) throws AtlasBaseException {
            if(!replicationOptionState) return;

            String targetClusterName = cluster.getName();
            updateReplicationAttribute(replicationOptionState, targetClusterName, entityGuids, Constants.ATTR_NAME_REPLICATED_FROM_CLUSTER);
        }

        private String getClusterNameFromOptionsState() {
            return replicationOptionState
                    ? getClusterNameFromOptions(request.getOptions(), optionKeyReplicatedFrom)
                    : getCurrentClusterName();
        }
    }
}
