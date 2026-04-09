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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.impexp.AsyncImportStatus;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_ID;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_STATUS;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.ASYNC_IMPORT_TYPE_NAME;

@Service
public class AsyncImportService {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncImportService.class);

    private final DataAccess dataAccess;

    @Inject
    public AsyncImportService(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    public AtlasAsyncImportRequest fetchImportRequestByImportId(String importId) {
        try {
            AtlasAsyncImportRequest request = new AtlasAsyncImportRequest();

            request.setImportId(importId);

            return dataAccess.load(request);
        } catch (Exception e) {
            LOG.error("Error fetching request with importId: {}", importId, e);

            return null;
        }
    }

    public void saveImportRequest(AtlasAsyncImportRequest importRequest) throws AtlasBaseException {
        try {
            dataAccess.save(importRequest);

            LOG.debug("Save request ID: {} request: {}", importRequest.getImportId(), importRequest);
        } catch (AtlasBaseException e) {
            LOG.error("Failed to save import: {} with request: {}", importRequest.getImportId(), importRequest, e);

            throw e;
        }
    }

    public void updateImportRequest(AtlasAsyncImportRequest importRequest) {
        try {
            saveImportRequest(importRequest);
        } catch (AtlasBaseException abe) {
            LOG.error("Failed to update import: {} with request: {}", importRequest.getImportId(), importRequest, abe);
        }
    }

    public List<String> fetchInProgressImportIds() {
        return AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, ImportStatus.PROCESSING),
                PROPERTY_KEY_ASYNC_IMPORT_ID);
    }

    public List<String> fetchQueuedImportRequests() {
        return AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, ImportStatus.WAITING),
                PROPERTY_KEY_ASYNC_IMPORT_ID);
    }

    public void deleteRequests() {
        try {
            dataAccess.delete(AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING));
        } catch (Exception e) {
            LOG.error("Error deleting import requests", e);
        }
    }

    public AtlasAsyncImportRequest abortImport(String importId) throws AtlasBaseException {
        AtlasAsyncImportRequest importRequestToKill = fetchImportRequestByImportId(importId);

        try {
            if (importRequestToKill == null) {
                throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
            }

            if (importRequestToKill.getStatus().equals(ImportStatus.STAGING) || importRequestToKill.getStatus().equals(ImportStatus.WAITING)) {
                importRequestToKill.setStatus(ImportStatus.ABORTED);

                saveImportRequest(importRequestToKill);

                LOG.info("Successfully aborted import request: {}", importId);
            } else {
                LOG.error("Cannot abort import request {}: request is in status: {}", importId, importRequestToKill.getStatus());

                throw new AtlasBaseException(AtlasErrorCode.IMPORT_ABORT_NOT_ALLOWED, importId, importRequestToKill.getStatus().getStatus());
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to abort import request: {}", importId, e);

            throw e;
        }

        return importRequestToKill;
    }

    @GraphTransaction
    public PList<AsyncImportStatus> getAsyncImportsStatus(int offset, int limit) throws AtlasBaseException {
        LOG.debug("==> AsyncImportService.getAllImports()");

        List<String> allImportGuids = AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING);

        List<AsyncImportStatus> requestedPage;

        if (CollectionUtils.isNotEmpty(allImportGuids)) {
            List<String> paginatedGuids = allImportGuids.stream().skip(offset).limit(limit).collect(Collectors.toList());

            List<AtlasAsyncImportRequest>     importsToLoad = paginatedGuids.stream().map(AtlasAsyncImportRequest::new).collect(Collectors.toList());
            Iterable<AtlasAsyncImportRequest> loadedImports = dataAccess.load(importsToLoad);

            requestedPage = StreamSupport.stream(loadedImports.spliterator(), false).map(AtlasAsyncImportRequest::toImportMinInfo).collect(Collectors.toList());
        } else {
            requestedPage = Collections.emptyList();
        }

        LOG.debug("<== AsyncImportService.getAllImports() : {}", requestedPage);

        return new PList<>(requestedPage, offset, limit, allImportGuids.size(), SortType.NONE, null);
    }

    @GraphTransaction
    public AtlasAsyncImportRequest getAsyncImportRequest(String importId) throws AtlasBaseException {
        LOG.debug("==> AsyncImportService.getImportStatusById(importId={})", importId);

        try {
            AtlasAsyncImportRequest importRequest = fetchImportRequestByImportId(importId);

            if (importRequest == null) {
                throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
            }

            return importRequest;
        } finally {
            LOG.debug("<== AsyncImportService.getImportStatusById(importId={})", importId);
        }
    }
}
