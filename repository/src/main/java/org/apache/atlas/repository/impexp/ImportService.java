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
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.BulkImporter;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

@Component
public class ImportService {
    private static final Logger LOG = LoggerFactory.getLogger(ImportService.class);

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasTypeRegistry typeRegistry;
    private final BulkImporter bulkImporter;
    private AuditHelper auditHelper;

    private long startTimestamp;
    private long endTimestamp;

    @Inject
    public ImportService(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry, BulkImporter bulkImporter, AuditHelper auditHelper) {
        this.typeDefStore = typeDefStore;
        this.typeRegistry = typeRegistry;
        this.bulkImporter = bulkImporter;
        this.auditHelper = auditHelper;
    }

    public AtlasImportResult run(ZipSource source, String userName,
                                 String hostName, String requestingIP) throws AtlasBaseException {
        return run(source, null, userName, hostName, requestingIP);
    }


    public AtlasImportResult run(ZipSource source, AtlasImportRequest request, String userName,
                                 String hostName, String requestingIP) throws AtlasBaseException {
        if (request == null) {
            request = new AtlasImportRequest();
        }

        AtlasImportResult result = new AtlasImportResult(request, userName, requestingIP, hostName, System.currentTimeMillis());

        try {
            LOG.info("==> import(user={}, from={})", userName, requestingIP);

            String transforms = MapUtils.isNotEmpty(request.getOptions()) ? request.getOptions().get(AtlasImportRequest.TRANSFORMS_KEY) : null;

            setImportTransform(source, transforms);
            startTimestamp = System.currentTimeMillis();
            processTypes(source.getTypesDef(), result);
            setStartPosition(request, source);
            processEntities(userName, source, result);

            result.setOperationStatus(AtlasImportResult.OperationStatus.SUCCESS);
        } catch (AtlasBaseException excp) {
            LOG.error("import(user={}, from={}): failed", userName, requestingIP, excp);

            throw excp;
        } catch (Exception excp) {
            LOG.error("import(user={}, from={}): failed", userName, requestingIP, excp);

            throw new AtlasBaseException(excp);
        } finally {
            source.close();
            LOG.info("<== import(user={}, from={}): status={}", userName, requestingIP, result.getOperationStatus());
        }

        return result;
    }

    @VisibleForTesting
    void setImportTransform(ZipSource source, String transforms) throws AtlasBaseException {
        ImportTransforms importTransform = ImportTransforms.fromJson(transforms);
        if (importTransform == null) {
            return;
        }

        updateTransformsWithSubTypes(importTransform);
        source.setImportTransform(importTransform);
    }

    private void updateTransformsWithSubTypes(ImportTransforms importTransforms) throws AtlasBaseException {
        String[] transformTypes = importTransforms.getTypes().toArray(new String[importTransforms.getTypes().size()]);
        for (int i = 0; i < transformTypes.length; i++) {
            String typeName = transformTypes[i];
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
            if (entityType == null) {
                continue;
            }

            importTransforms.addParentTransformsToSubTypes(typeName, entityType.getAllSubTypes());
        }
    }

    private void setStartPosition(AtlasImportRequest request, ZipSource source) throws AtlasBaseException {
        if (request.getStartGuid() != null) {
            source.setPositionUsingEntityGuid(request.getStartGuid());
        } else if (request.getStartPosition() != null) {
            source.setPosition(Integer.parseInt(request.getStartPosition()));
        }
    }

    public AtlasImportResult run(AtlasImportRequest request, String userName, String hostName, String requestingIP)
            throws AtlasBaseException {
        String fileName = request.getFileName();

        if (StringUtils.isBlank(fileName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "FILENAME parameter not found");
        }

        AtlasImportResult result = null;

        try {
            LOG.info("==> import(user={}, from={}, fileName={})", userName, requestingIP, fileName);

            String transforms = MapUtils.isNotEmpty(request.getOptions()) ? request.getOptions().get(AtlasImportRequest.TRANSFORMS_KEY) : null;
            File file = new File(fileName);
            ZipSource source = new ZipSource(new ByteArrayInputStream(FileUtils.readFileToByteArray(file)), ImportTransforms.fromJson(transforms));
            result = run(source, request, userName, hostName, requestingIP);
        } catch (AtlasBaseException excp) {
            LOG.error("import(user={}, from={}, fileName={}): failed", userName, requestingIP, excp);

            throw excp;
        } catch (FileNotFoundException excp) {
            LOG.error("import(user={}, from={}, fileName={}): file not found", userName, requestingIP, excp);

            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, fileName + ": file not found");
        } catch (IOException excp) {
            LOG.error("import(user={}, from={}, fileName={}): cannot read file", userName, requestingIP, excp);

            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, fileName + ": cannot read file");
        } catch (Exception excp) {
            LOG.error("import(user={}, from={}, fileName={}): failed", userName, requestingIP, excp);

            throw new AtlasBaseException(excp);
        } finally {
            LOG.info("<== import(user={}, from={}, fileName={}): status={}", userName, requestingIP, fileName,
                    (result == null ? AtlasImportResult.OperationStatus.FAIL : result.getOperationStatus()));
        }

        return result;
    }

    private void processTypes(AtlasTypesDef typeDefinitionMap, AtlasImportResult result) throws AtlasBaseException {
        if (result.getRequest().getUpdateTypeDefs() != null && !result.getRequest().getUpdateTypeDefs().equals("true")) {
            return;
        }

        ImportTypeDefProcessor importTypeDefProcessor = new ImportTypeDefProcessor(this.typeDefStore, this.typeRegistry);
        importTypeDefProcessor.processTypes(typeDefinitionMap, result);
    }

    private void processEntities(String userName, ZipSource importSource, AtlasImportResult result) throws AtlasBaseException {
        this.bulkImporter.bulkImport(importSource, result);

        endTimestamp = System.currentTimeMillis();
        result.incrementMeticsCounter("duration", getDuration(this.endTimestamp, this.startTimestamp));
        auditHelper.audit(userName, result, startTimestamp, endTimestamp, !importSource.getCreationOrder().isEmpty());
    }

    private int getDuration(long endTime, long startTime) {
        return (int) (endTime - startTime);
    }
}
