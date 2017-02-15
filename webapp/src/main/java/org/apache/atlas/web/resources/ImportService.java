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
package org.apache.atlas.web.resources;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.commons.io.FileUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;


public class ImportService {
    private static final Logger LOG = LoggerFactory.getLogger(ImportService.class);

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasEntityStore  entityStore;


    public ImportService(final AtlasTypeDefStore typeDefStore, final AtlasEntityStore entityStore) {
        this.typeDefStore = typeDefStore;
        this.entityStore  = entityStore;
    }

    public AtlasImportResult run(ZipSource source, AtlasImportRequest request, String userName,
                                 String hostName, String requestingIP) throws AtlasBaseException {
        AtlasImportResult result = new AtlasImportResult(request, userName, requestingIP, hostName, System.currentTimeMillis());

        try {
            LOG.info("==> import(user={}, from={})", userName, requestingIP);

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
            try {
                source.close();
            } catch (IOException e) {
                // ignore
            }

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
        setGuidToEmpty(typeDefinitionMap.getEntityDefs());
        typeDefStore.updateTypesDef(typeDefinitionMap);

        result.incrementMeticsCounter("Enum(s)", typeDefinitionMap.getEnumDefs().size());
        result.incrementMeticsCounter("Struct(s)", typeDefinitionMap.getStructDefs().size());
        result.incrementMeticsCounter("Classification(s)", typeDefinitionMap.getClassificationDefs().size());
        result.incrementMeticsCounter("Entity definition(s)", typeDefinitionMap.getEntityDefs().size());
    }

    private void setGuidToEmpty(List<AtlasEntityDef> entityDefList) {
        for (AtlasEntityDef edf: entityDefList) {
            edf.setGuid("");
        }
    }

    private void processEntities(ZipSource importSource, AtlasImportResult result) throws AtlasBaseException {
        this.entityStore.createOrUpdate(importSource, false);
        result.incrementMeticsCounter("Entities", importSource.getCreationOrder().size());
    }
}
