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
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.ExportImportAuditEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

@Component
public class AuditsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AuditsWriter.class);
    private static final String CLUSTER_NAME_DEFAULT = "default";
    private static final String DC_SERVER_NAME_SEPARATOR = "$";

    private AtlasServerService atlasServerService;
    private ExportImportAuditService auditService;

    private ExportAudits auditForExport = new ExportAudits();
    private ImportAudits auditForImport = new ImportAudits();

    @Inject
    public AuditsWriter(AtlasServerService atlasServerService, ExportImportAuditService auditService) {
        this.atlasServerService = atlasServerService;
        this.auditService = auditService;
    }

    public void write(String userName, AtlasExportResult result,
                      long startTime, long endTime,
                      List<String> entityCreationOrder) throws AtlasBaseException {
        auditForExport.add(userName, result, startTime, endTime, entityCreationOrder);
    }

    public void write(String userName, AtlasImportResult result,
                      long startTime, long endTime,
                      List<String> entityCreationOrder) throws AtlasBaseException {
        auditForImport.add(userName, result, startTime, endTime, entityCreationOrder);
    }

    private void updateReplicationAttribute(boolean isReplicationSet,
                                            String serverName, String serverFullName,
                                            List<String> exportedGuids,
                                            String attrNameReplicated,
                                            long lastModifiedTimestamp) throws AtlasBaseException {
        if (!isReplicationSet || CollectionUtils.isEmpty(exportedGuids)) {
            return;
        }

        AtlasServer server = saveServer(serverName, serverFullName, exportedGuids.get(0), lastModifiedTimestamp);
        atlasServerService.updateEntitiesWithServer(server, exportedGuids, attrNameReplicated);
    }

    private AtlasServer saveServer(String clusterName, String serverFullName,
                                   String entityGuid,
                                   long lastModifiedTimestamp) throws AtlasBaseException {

        AtlasServer server = atlasServerService.getCreateAtlasServer(clusterName, serverFullName);
        server.setAdditionalInfoRepl(entityGuid, lastModifiedTimestamp);
        if (LOG.isDebugEnabled()) {
            LOG.debug("saveServer: {}", server);
        }

        return atlasServerService.save(server);
    }

    public static String getCurrentClusterName() {
        try {
            return ApplicationProperties.get().getString(AtlasConstants.CLUSTER_NAME_KEY, CLUSTER_NAME_DEFAULT);
        } catch (AtlasException e) {
            LOG.error("getCurrentClusterName", e);
        }

        return StringUtils.EMPTY;
    }

    static String getServerNameFromFullName(String fullName) {
        if (StringUtils.isEmpty(fullName) || !fullName.contains(DC_SERVER_NAME_SEPARATOR)) {
            return fullName;
        }

        String[] splits = StringUtils.split(fullName, DC_SERVER_NAME_SEPARATOR);
        if (splits == null || splits.length < 1) {
            return "";
        } else if (splits.length >= 2) {
            return splits[1];
        } else {
            return splits[0];
        }
    }

    private void saveCurrentServer() throws AtlasBaseException {
        atlasServerService.getCreateAtlasServer(getCurrentClusterName(), getCurrentClusterName());
    }

    private class ExportAudits {
        private AtlasExportRequest request;
        private String targetServerName;
        private boolean replicationOptionState;
        private String targetServerFullName;

        public void add(String userName, AtlasExportResult result,
                        long startTime, long endTime,
                        List<String> entityGuids) throws AtlasBaseException {
            request = result.getRequest();
            replicationOptionState = request.isReplicationOptionSet();

            saveCurrentServer();

            targetServerFullName = request.getOptionKeyReplicatedTo();
            targetServerName = getServerNameFromFullName(targetServerFullName);
            auditService.add(userName, getCurrentClusterName(), targetServerName,
                    ExportImportAuditEntry.OPERATION_EXPORT,
                    AtlasType.toJson(result), startTime, endTime, !entityGuids.isEmpty());

            if (result.getOperationStatus() == AtlasExportResult.OperationStatus.FAIL) {
                return;
            }

            updateReplicationAttribute(replicationOptionState, targetServerName, targetServerFullName,
                    entityGuids, Constants.ATTR_NAME_REPLICATED_TO, result.getChangeMarker());
        }
    }

    private class ImportAudits {
        private AtlasImportRequest request;
        private boolean replicationOptionState;
        private String sourceServerName;
        private String optionKeyReplicatedFrom;
        private String sourceServerFullName;

        public void add(String userName, AtlasImportResult result,
                        long startTime, long endTime,
                        List<String> entityGuids) throws AtlasBaseException {
            optionKeyReplicatedFrom = AtlasImportRequest.OPTION_KEY_REPLICATED_FROM;
            request = result.getRequest();
            replicationOptionState = request.isReplicationOptionSet();

            saveCurrentServer();

            sourceServerFullName = request.getOptionKeyReplicatedFrom();
            sourceServerName = getServerNameFromFullName(sourceServerFullName);
            auditService.add(userName,
                    sourceServerName, getCurrentClusterName(),
                    ExportImportAuditEntry.OPERATION_IMPORT,
                    AtlasType.toJson(result), startTime, endTime, !entityGuids.isEmpty());

            if(result.getOperationStatus() == AtlasImportResult.OperationStatus.FAIL) {
                return;
            }

            updateReplicationAttribute(replicationOptionState, sourceServerName, sourceServerFullName, entityGuids,
                    Constants.ATTR_NAME_REPLICATED_FROM, result.getExportResult().getChangeMarker());
        }
    }
}
