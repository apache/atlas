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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;


public class ImportService {
    private static final Logger LOG = LoggerFactory.getLogger(ImportService.class);

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasEntityStore  entityStore;
    private final AtlasTypeRegistry typeRegistry;

    private long startTimestamp;
    private long endTimestamp;


    public ImportService(final AtlasTypeDefStore typeDefStore, final AtlasEntityStore entityStore, AtlasTypeRegistry typeRegistry) {
        this.typeDefStore = typeDefStore;
        this.entityStore  = entityStore;
        this.typeRegistry = typeRegistry;
    }

    public AtlasImportResult run(ZipSource source, AtlasImportRequest request, String userName,
                                 String hostName, String requestingIP) throws AtlasBaseException {
        AtlasImportResult result = new AtlasImportResult(request, userName, requestingIP, hostName, System.currentTimeMillis());

        try {
            LOG.info("==> import(user={}, from={})", userName, requestingIP);

            startTimestamp = System.currentTimeMillis();
            processTypes(source.getTypesDef(), result);
            processEntities(source, result);

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

    public AtlasImportResult run(AtlasImportRequest request, String userName, String hostName, String requestingIP)
                                                                                            throws AtlasBaseException {
        String fileName = (String)request.getOptions().get("FILENAME");

        if (StringUtils.isBlank(fileName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "FILENAME parameter not found");
        }

        AtlasImportResult result = null;

        try {
            LOG.info("==> import(user={}, from={}, fileName={})", userName, requestingIP, fileName);

            File      file   = new File(fileName);
            ZipSource source = new ZipSource(new ByteArrayInputStream(FileUtils.readFileToByteArray(file)));

            result = run(source, request, userName, hostName, requestingIP);
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

    private void processTypes(AtlasTypesDef typeDefinitionMap, AtlasImportResult result) throws AtlasBaseException {
        setGuidToEmpty(typeDefinitionMap);

        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typeDefinitionMap, this.typeRegistry);

        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);

            updateMetricsForTypesDef(typesToCreate, result);
        }
    }

    private void updateMetricsForTypesDef(AtlasTypesDef typeDefinitionMap, AtlasImportResult result) {
        result.incrementMeticsCounter("typedef:classification", typeDefinitionMap.getClassificationDefs().size());
        result.incrementMeticsCounter("typedef:enum", typeDefinitionMap.getEnumDefs().size());
        result.incrementMeticsCounter("typedef:entitydef", typeDefinitionMap.getEntityDefs().size());
        result.incrementMeticsCounter("typedef:struct", typeDefinitionMap.getStructDefs().size());
    }

    private void setGuidToEmpty(AtlasTypesDef typesDef) {
        for (AtlasEntityDef def: typesDef.getEntityDefs()) {
            def.setGuid(null);
        }

        for (AtlasClassificationDef def: typesDef.getClassificationDefs()) {
            def.setGuid(null);
        }

        for (AtlasEnumDef def: typesDef.getEnumDefs()) {
            def.setGuid(null);
        }

        for (AtlasStructDef def: typesDef.getStructDefs()) {
            def.setGuid(null);
        }
    }

    private void processEntities(ZipSource importSource, AtlasImportResult result) throws AtlasBaseException {
        this.entityStore.bulkImport(importSource, result);

        endTimestamp = System.currentTimeMillis();
        result.incrementMeticsCounter("duration", (int) (this.endTimestamp - this.startTimestamp));
    }
}
