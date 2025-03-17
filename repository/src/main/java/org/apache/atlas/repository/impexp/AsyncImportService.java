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
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ASYNC_IMPORT_TYPE_NAME;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.ABORTED;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.PROCESSING;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.WAITING;

@Service
public class AsyncImportService {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncImportService.class);

    private final DataAccess dataAccess;

    @Inject
    public AsyncImportService(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    public synchronized AtlasAsyncImportRequest fetchImportRequestByImportId(String importId) {
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

    public synchronized List<String> fetchInProgressImportIds() {
        List<String> guids = AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING);

        List<String> inProgressImportIds = new ArrayList<>();

        List<AtlasAsyncImportRequest> importsToLoad = guids.stream()
                .map(AtlasAsyncImportRequest::new)
                .collect(Collectors.toList());

        Iterable<AtlasAsyncImportRequest> allRequests = null;
        try {
            allRequests = dataAccess.load(importsToLoad);
            if (allRequests != null) {
                for (AtlasAsyncImportRequest request : allRequests) {
                    if (request != null) {
                        if (ObjectUtils.equals(request.getStatus(), PROCESSING)) {
                            inProgressImportIds.add(request.getImportId());
                        }
                    }
                }
            }
        } catch (AtlasBaseException e) {
            LOG.error("Could not get in progress import requests.", e);
        }

        return inProgressImportIds;
    }

    public synchronized List<String> fetchQueuedImportRequests() {
        List<String> guids = AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING);

        List<String> queuedImportIds = new ArrayList<>();

        List<AtlasAsyncImportRequest> importsToLoad = guids.stream()
                .map(AtlasAsyncImportRequest::new)
                .collect(Collectors.toList());

        Iterable<AtlasAsyncImportRequest> allRequests = null;
        try {
            allRequests = dataAccess.load(importsToLoad);
        } catch (AtlasBaseException e) {
            LOG.error("Could not get all import request to be queued.", e);
        }

        if (allRequests != null) {
            for (AtlasAsyncImportRequest request : allRequests) {
                if (request != null) {
                    if (ObjectUtils.equals(request.getStatus(), WAITING)) {
                        queuedImportIds.add(request.getImportId());
                    }
                }
            }
        }

        return queuedImportIds;
    }

    public void deleteRequests() {
        try {
            dataAccess.delete(AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING));
        } catch (Exception e) {
            LOG.error("Error deleting import requests");
        }
    }

    public AtlasAsyncImportRequest abortImport(String importId) throws AtlasBaseException {
        AtlasAsyncImportRequest importRequestToKill = fetchImportRequestByImportId(importId);
        try {
            if (importRequestToKill == null) {
                throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
            }
            if (importRequestToKill.getStatus().equals(WAITING)) {
                importRequestToKill.setStatus(ABORTED);
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
    public List<Map<String, Object>> getAllImports() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AsyncImportService.getAllImports()");
        }

        List<Map<String, Object>> importRequests;

        List<String> guids = AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING);

        if (CollectionUtils.isNotEmpty(guids)) {
            List<AtlasAsyncImportRequest> importsToLoad = guids.stream()
                    .map(AtlasAsyncImportRequest::new)
                    .collect(Collectors.toList());

            Iterable<AtlasAsyncImportRequest> loadedImports = dataAccess.load(importsToLoad);

            importRequests = StreamSupport.stream(loadedImports.spliterator(), false)
                    .map(AtlasAsyncImportRequest::getImportMinInfo)
                    .collect(Collectors.toList());
        } else {
            importRequests = Collections.emptyList();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AsyncImportService.getAllImports() : {}", importRequests);
        }

        return importRequests;
    }

    @GraphTransaction
    public AtlasAsyncImportRequest getImportStatusById(String importId) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AsyncImportService.getImportStatusById(importId={})", importId);
        }

        AtlasAsyncImportRequest atlasAsyncImportRequest = new AtlasAsyncImportRequest();
        atlasAsyncImportRequest.setImportId(importId);
        AtlasAsyncImportRequest importRequest = dataAccess.load(atlasAsyncImportRequest);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AsyncImportService.getImportStatusById(importId={})", importId);
        }

        return importRequest;
    }
}
