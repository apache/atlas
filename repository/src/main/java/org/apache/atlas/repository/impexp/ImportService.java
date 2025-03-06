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
package org.apache.atlas.repository.impexp;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.entitytransform.BaseEntityHandler;
import org.apache.atlas.entitytransform.TransformerContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.store.graph.BulkImporter;
import org.apache.atlas.repository.store.graph.v2.AsyncImportTaskExecutor;
import org.apache.atlas.repository.store.graph.v2.EntityImportStream;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasStringUtil;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.FAILED;
import static org.apache.atlas.model.impexp.AtlasImportRequest.TRANSFORMERS_KEY;
import static org.apache.atlas.model.impexp.AtlasImportRequest.TRANSFORMS_KEY;
import static org.apache.atlas.model.impexp.AtlasImportResult.OperationStatus.SUCCESS;

@Component
public class ImportService implements AsyncImporter {
    private static final Logger LOG = LoggerFactory.getLogger(ImportService.class);

    private static final String ATLAS_TYPE_HIVE_TABLE = "hive_table";
    private static final String OPERATION_STATUS      = "operationStatus";

    private final AtlasTypeDefStore       typeDefStore;
    private final AtlasTypeRegistry       typeRegistry;
    private final BulkImporter            bulkImporter;
    private final AuditsWriter            auditsWriter;
    private final ImportTransformsShaper  importTransformsShaper;
    private final AsyncImportTaskExecutor asyncImportTaskExecutor;
    private final AsyncImportService      asyncImportService;
    private final AtlasAuditService       auditService;

    private final TableReplicationRequestProcessor tableReplicationRequestProcessor;

    private long startTimestamp;
    private long endTimestamp;

    @Inject
    public ImportService(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry, BulkImporter bulkImporter,
            AuditsWriter auditsWriter, ImportTransformsShaper importTransformsShaper,
            TableReplicationRequestProcessor tableReplicationRequestProcessor, AsyncImportTaskExecutor asyncImportTaskExecutor,
            AsyncImportService asyncImportService, AtlasAuditService auditService) {
        this.typeDefStore                     = typeDefStore;
        this.typeRegistry                     = typeRegistry;
        this.bulkImporter                     = bulkImporter;
        this.auditsWriter                     = auditsWriter;
        this.importTransformsShaper           = importTransformsShaper;
        this.tableReplicationRequestProcessor = tableReplicationRequestProcessor;
        this.asyncImportTaskExecutor = asyncImportTaskExecutor;
        this.asyncImportService = asyncImportService;
        this.auditService  = auditService;
    }

    public AtlasImportResult run(InputStream inputStream, String userName, String hostName, String requestingIP) throws AtlasBaseException {
        return run(inputStream, null, userName, hostName, requestingIP);
    }

    public AtlasImportResult run(InputStream inputStream, AtlasImportRequest request, String userName, String hostName, String requestingIP) throws AtlasBaseException {
        if (request == null) {
            request = new AtlasImportRequest();
        }

        EntityImportStream source = createZipSource(request, inputStream, AtlasConfiguration.IMPORT_TEMP_DIRECTORY.getString());

        return run(source, request, userName, hostName, requestingIP);
    }

    public AtlasImportResult run(AtlasImportRequest request, String userName, String hostName, String requestingIP) throws AtlasBaseException {
        String fileName = request.getFileName();

        if (StringUtils.isBlank(fileName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "FILENAME parameter not found");
        }

        AtlasImportResult result = null;

        try {
            LOG.info("==> import(user={}, from={}, fileName={})", userName, requestingIP, fileName);

            File file = new File(fileName);

            result = run(new FileInputStream(file), request, userName, hostName, requestingIP);
        } catch (AtlasBaseException excp) {
            LOG.error("import(user={}, from={}, fileName={}): failed", userName, requestingIP, excp);

            throw excp;
        } catch (FileNotFoundException excp) {
            LOG.error("import(user={}, from={}, fileName={}): file not found", userName, requestingIP, excp);

            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, fileName + ": file not found");
        } catch (Exception excp) {
            LOG.error("import(user={}, from={}, fileName={}): failed", userName, requestingIP, excp);

            throw new AtlasBaseException(excp);
        } finally {
            LOG.info("<== import(user={}, from={}, fileName={}): status={}", userName, requestingIP, fileName,
                    (result == null ? AtlasImportResult.OperationStatus.FAIL : result.getOperationStatus()));
        }

        return result;
    }

    public AtlasAsyncImportRequest run(AtlasImportRequest request, InputStream inputStream, String userName, String hostName, String requestingIP) throws AtlasBaseException {
        if (request == null) {
            request = new AtlasImportRequest();
        }
        try {
            LOG.info("==> asyncImport(user={}, from={}, request={})", userName, requestingIP, request);
            EntityImportStream source = createZipSource(inputStream, AtlasConfiguration.IMPORT_TEMP_DIRECTORY.getString());

            String transforms = MapUtils.isNotEmpty(request.getOptions()) ? request.getOptions().get(TRANSFORMS_KEY) : null;
            setImportTransform(source, transforms);

            String transformers = MapUtils.isNotEmpty(request.getOptions()) ? request.getOptions().get(TRANSFORMERS_KEY) : null;
            setEntityTransformerHandlers(source, transformers);

            AtlasImportResult result = new AtlasImportResult(request, userName, requestingIP, hostName, System.currentTimeMillis());
            result.setExportResult(source.getExportResult());
            return asyncImportTaskExecutor.run(result, source);
        } finally {
            LOG.info("<== asyncImport(user={}, from={}, request={})", userName, requestingIP, request);
        }
    }

    public List<Map<String, Object>> getAllAsyncImports() throws AtlasBaseException {
        return asyncImportService.getAllImports();
    }

    public AtlasAsyncImportRequest getAsyncImportStatus(String importId) throws AtlasBaseException {
        return asyncImportService.getImportStatusById(importId);
    }

    public void abortAsyncImport(String importId) throws AtlasBaseException {
        asyncImportTaskExecutor.abortImport(importId);
    }

    @VisibleForTesting
    AtlasImportResult run(EntityImportStream source, AtlasImportRequest request, String userName, String hostName, String requestingIP) throws AtlasBaseException {
        AtlasImportResult result = new AtlasImportResult(request, userName, requestingIP, hostName, System.currentTimeMillis());

        try {
            LOG.info("==> import(user={}, from={}, request={})", userName, requestingIP, request);

            RequestContext.get().setImportInProgress(true);

            String transforms = AtlasStringUtil.getOption(request.getOptions(), TRANSFORMS_KEY);

            setImportTransform(source, transforms);

            String transformers = AtlasStringUtil.getOption(request.getOptions(), TRANSFORMERS_KEY);

            setEntityTransformerHandlers(source, transformers);

            startTimestamp = System.currentTimeMillis();

            processTypes(source.getTypesDef(), result);
            setStartPosition(request, source);

            processEntities(userName, source, result);

            processReplicationDeletion(source.getExportResult().getRequest(), request);
        } catch (AtlasBaseException excp) {
            LOG.error("import(user={}, from={}): failed", userName, requestingIP, excp);

            throw excp;
        } catch (Exception excp) {
            LOG.error("import(user={}, from={}): failed", userName, requestingIP, excp);

            throw new AtlasBaseException(excp);
        } finally {
            RequestContext.get().setImportInProgress(false);

            if (source != null) {
                source.close();
            }

            LOG.info("<== import(user={}, from={}): status={}", userName, requestingIP, result.getOperationStatus());
        }

        return result;
    }

    @Override
    public void onImportTypeDef(AtlasTypesDef typesDef, String importId) throws AtlasBaseException {
        RequestContext.get().setImportInProgress(true);
        LOG.info("==> onImportTypeDef(typesDef={}, importId={})", typesDef, importId);
        AtlasAsyncImportRequest importRequest = asyncImportService.fetchImportRequestByImportId(importId);
        if (importRequest == null) {
            throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
        }
        AtlasImportResult result = importRequest.getAtlasImportResult();
        try {
            processTypes(typesDef, result);
        } catch (AtlasBaseException abe) {
            importRequest.setStatus(FAILED);
            throw abe;
        } finally {
            RequestContext.get().setImportInProgress(false);
            importRequest.setAtlasImportResult(result);
            asyncImportService.updateImportRequest(importRequest);
            LOG.info("<== onImportTypeDef(typesDef={}, importResult={})", typesDef, importRequest.getAtlasImportResult());
        }
    }

    @Override
    public Boolean onImportEntity(AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo, String importId, int position) throws AtlasBaseException {
        LOG.info("==> onImportEntity(atlasEntityWithExtInfo={}, importId={}, position={})", atlasEntityWithExtInfo, importId, position);
        AtlasAsyncImportRequest importRequest = asyncImportService.fetchImportRequestByImportId(importId);
        if (importRequest == null) {
            throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
        }
        AtlasImportResult result = importRequest.getAtlasImportResult();
        float importProgress = importRequest.getImportDetails().getImportProgress();
        int importedEntitiesCounter = importRequest.getImportDetails().getImportedEntitiesCount();
        int failedEntitiesCounter = importRequest.getImportDetails().getFailedEntitiesCount();
        Set<String> processedEntities = new HashSet<>(result.getProcessedEntities());
        List<String> failedEntities = importRequest.getImportDetails().getFailedEntities();
        RequestContext.get().setImportInProgress(true);
        EntityMutationResponse entityMutationResponse = null;
        try {
            startTimestamp = importRequest.getStartedProcessingAt();
            TypesUtil.Pair<EntityMutationResponse, Float> resp = this.bulkImporter.asyncImport(atlasEntityWithExtInfo, entityMutationResponse,
                    result, processedEntities, failedEntities, position, importRequest.getImportDetails().getTotalEntitiesCount(), importProgress);
            endTimestamp = System.currentTimeMillis();

            importedEntitiesCounter += 1;
            importRequest.getImportDetails().setImportedEntitiesCount(importedEntitiesCounter);
            result.setProcessedEntities(new ArrayList<>(processedEntities));
            importRequest.getImportDetails().setImportProgress(resp.right);
        } catch (AtlasBaseException abe) {
            failedEntitiesCounter += 1;
            importRequest.getImportDetails().setFailedEntitiesCount(failedEntitiesCounter);
            failedEntities.add(atlasEntityWithExtInfo.getEntity().getGuid());
            importRequest.getImportDetails().setFailedEntities(failedEntities);
            importRequest.getImportDetails().addFailure(atlasEntityWithExtInfo.getEntity().getGuid(), abe.getMessage());
        } finally {
            RequestContext.get().setImportInProgress(false);
            result.incrementMeticsCounter("duration", getDuration(this.endTimestamp, this.startTimestamp));
            importRequest.setAtlasImportResult(result);
            importRequest.setCompletedAt(System.currentTimeMillis());
            asyncImportService.updateImportRequest(importRequest);

            LOG.info("<== onImportEntity(atlasEntityWithExtInfo={}, importId={}, position={})", atlasEntityWithExtInfo, importId, position);
        }
        return importRequest.getImportDetails().getPublishedEntityCount() <=
                importRequest.getImportDetails().getImportedEntitiesCount() + importRequest.getImportDetails().getFailedEntitiesCount();
    }

    @Override
    public void onImportComplete(String importId) throws AtlasBaseException {
        LOG.info("==> onImportComplete(importId={})", importId);
        try {
            AtlasAsyncImportRequest importRequest = asyncImportService.fetchImportRequestByImportId(importId);
            if (importRequest == null) {
                throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
            }
            if (importRequest.getImportDetails().getTotalEntitiesCount() == importRequest.getImportDetails().getImportedEntitiesCount()) {
                importRequest.setStatus(AtlasAsyncImportRequest.ImportStatus.SUCCESSFUL);
                importRequest.getAtlasImportResult().setOperationStatus(SUCCESS);
            } else if (importRequest.getImportDetails().getImportedEntitiesCount() > 0) {
                importRequest.setStatus(AtlasAsyncImportRequest.ImportStatus.PARTIAL_SUCCESS);
                importRequest.getAtlasImportResult().setOperationStatus(AtlasImportResult.OperationStatus.PARTIAL_SUCCESS);
            } else {
                importRequest.setStatus(FAILED);
            }
            asyncImportService.updateImportRequest(importRequest);
            AtlasImportResult result = importRequest.getAtlasImportResult();
            processReplicationDeletion(result.getExportResult().getRequest(), result.getRequest());
            auditsWriter.write(result.getUserName(), result, startTimestamp, endTimestamp, importRequest.getImportDetails().getCreationOrder());
            addToImportOperationAudits(result);
        } finally {
            asyncImportTaskExecutor.onCompleteImportRequest(importId);
            LOG.info("<== onImportComplete(importId={})", importId);
        }
    }

    @VisibleForTesting
    void setImportTransform(EntityImportStream source, String transforms) throws AtlasBaseException {
        ImportTransforms importTransform = ImportTransforms.fromJson(transforms);

        if (importTransform == null) {
            return;
        }

        importTransformsShaper.shape(importTransform, source.getExportResult().getRequest());

        source.setImportTransform(importTransform);

        if (LOG.isDebugEnabled()) {
            debugLog("   => transforms: {}", AtlasType.toJson(importTransform));
        }
    }

    @VisibleForTesting
    void setEntityTransformerHandlers(EntityImportStream source, String transformersJson) throws AtlasBaseException {
        if (StringUtils.isEmpty(transformersJson)) {
            return;
        }

        TransformerContext      context        = new TransformerContext(typeRegistry, typeDefStore, source.getExportResult().getRequest());
        List<BaseEntityHandler> entityHandlers = BaseEntityHandler.fromJson(transformersJson, context);

        if (CollectionUtils.isEmpty(entityHandlers)) {
            return;
        }

        source.setEntityHandlers(entityHandlers);
    }

    @VisibleForTesting
    boolean checkHiveTableIncrementalSkipLineage(AtlasImportRequest importRequest, AtlasExportRequest exportRequest) {
        if (exportRequest == null || CollectionUtils.isEmpty(exportRequest.getItemsToExport())) {
            return false;
        }

        for (AtlasObjectId itemToExport : exportRequest.getItemsToExport()) {
            if (!itemToExport.getTypeName().equalsIgnoreCase(ATLAS_TYPE_HIVE_TABLE)) {
                return false;
            }
        }

        return importRequest.isReplicationOptionSet() && exportRequest.isReplicationOptionSet() &&
                exportRequest.getFetchTypeOptionValue().equalsIgnoreCase(AtlasExportRequest.FETCH_TYPE_INCREMENTAL) &&
                exportRequest.getSkipLineageOptionValue();
    }

    private void debugLog(String s, Object... params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(s, params);
        }
    }

    private void setStartPosition(AtlasImportRequest request, EntityImportStream source) throws AtlasBaseException {
        if (request.getStartGuid() != null) {
            source.setPositionUsingEntityGuid(request.getStartGuid());
        } else if (request.getStartPosition() != null) {
            source.setPosition(Integer.parseInt(request.getStartPosition()));
        }
    }

    @VisibleForTesting
    void processTypes(AtlasTypesDef typeDefinitionMap, AtlasImportResult result) throws AtlasBaseException {
        if (result.getRequest().getUpdateTypeDefs() != null && !result.getRequest().getUpdateTypeDefs().equals("true")) {
            return;
        }

        ImportTypeDefProcessor importTypeDefProcessor = new ImportTypeDefProcessor(this.typeDefStore, this.typeRegistry);
        importTypeDefProcessor.processTypes(typeDefinitionMap, result);
    }

    private void processEntities(String userName, EntityImportStream importSource, AtlasImportResult result) throws AtlasBaseException {
        result.setExportResult(importSource.getExportResult());

        this.bulkImporter.bulkImport(importSource, result);

        endTimestamp = System.currentTimeMillis();

        result.incrementMeticsCounter("duration", getDuration(this.endTimestamp, this.startTimestamp));
        result.setOperationStatus(AtlasImportResult.OperationStatus.SUCCESS);

        if (isMigrationMode(result.getRequest())) {
            return;
        }

        auditsWriter.write(userName, result, startTimestamp, endTimestamp, importSource.getCreationOrder());
    }

    private EntityImportStream createZipSource(InputStream inputStream, String configuredTemporaryDirectory) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(configuredTemporaryDirectory)) {
                return new ZipSource(inputStream);
            }
            return new ZipSourceWithBackingDirectory(inputStream, configuredTemporaryDirectory);
        } catch (IOException ex) {
            throw new AtlasBaseException(ex);
        }
    }

    @VisibleForTesting
    void processReplicationDeletion(AtlasExportRequest exportRequest, AtlasImportRequest importRequest) throws AtlasBaseException {
        if (checkHiveTableIncrementalSkipLineage(importRequest, exportRequest)) {
            tableReplicationRequestProcessor.process(exportRequest, importRequest);
        }
    }

    private int getDuration(long endTime, long startTime) {
        return (int) (endTime - startTime);
    }

    private EntityImportStream createZipSource(AtlasImportRequest request, InputStream inputStream, String configuredTemporaryDirectory) throws AtlasBaseException {
        try {
            if (isMigrationMode(request) || AtlasStringUtil.optionEquals(request.getOptions(), AtlasImportRequest.OPTION_KEY_FORMAT, AtlasImportRequest.OPTION_KEY_FORMAT_ZIP_DIRECT)) {
                LOG.info("ZipSource Format: ZipDirect: Size: {}", AtlasStringUtil.getOption(request.getOptions(), "size"));

                return getZipDirectEntityImportStream(request, inputStream);
            }

            if (StringUtils.isEmpty(configuredTemporaryDirectory)) {
                return new ZipSource(inputStream);
            }

            return new ZipSourceWithBackingDirectory(inputStream, configuredTemporaryDirectory);
        } catch (IOException ex) {
            throw new AtlasBaseException(ex);
        }
    }

    private EntityImportStream getZipDirectEntityImportStream(AtlasImportRequest request, InputStream inputStream) throws IOException, AtlasBaseException {
        ZipSourceDirect zipSourceDirect = new ZipSourceDirect(inputStream, request.getSizeOption());

        LOG.info("Using ZipSourceDirect: Size: {} entities", zipSourceDirect.size());

        return zipSourceDirect;
    }

    private boolean isMigrationMode(AtlasImportRequest request) {
        return AtlasStringUtil.hasOption(request.getOptions(), AtlasImportRequest.OPTION_KEY_MIGRATION);
    }

    @VisibleForTesting
    void addToImportOperationAudits(AtlasImportResult result) throws AtlasBaseException {
        Map<String, Object> optionMap = new HashMap<>();
        optionMap.put(OPERATION_STATUS, result.getOperationStatus().name());
        String params = AtlasJson.toJson(optionMap);
        if (result.getExportResult().getRequest() == null) {
            int resultCount = result.getProcessedEntities().size();
            auditService.add(AtlasAuditEntry.AuditOperation.IMPORT, params, AtlasJson.toJson(result.getMetrics()), resultCount);
        } else {
            List<AtlasObjectId> objectIds = result.getExportResult().getRequest().getItemsToExport();
            auditImportExportOperations(objectIds, AtlasAuditEntry.AuditOperation.IMPORT, params);
        }
    }

    private void auditImportExportOperations(List<AtlasObjectId> objectIds, AtlasAuditEntry.AuditOperation auditOperation, String params) throws AtlasBaseException {
        Map<String, Long> entityCountByType = objectIds.stream().collect(Collectors.groupingBy(AtlasObjectId::getTypeName, Collectors.counting()));
        int resultCount = objectIds.size();

        auditService.add(auditOperation, params, AtlasJson.toJson(entityCountByType), resultCount);
    }
}
