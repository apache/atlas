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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.impexp.AsyncImportStatus;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.tasks.GraphClaimable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import static org.apache.atlas.model.impexp.AtlasImportResult.OperationStatus.FAIL;
import static org.apache.atlas.model.impexp.AtlasImportResult.OperationStatus.PARTIAL_SUCCESS;
import static org.apache.atlas.model.impexp.AtlasImportResult.OperationStatus.SUCCESS;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_ID;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_STATUS;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.ASYNC_IMPORT_TYPE_NAME;

@Service
public class AsyncImportService implements GraphClaimable<AtlasAsyncImportRequest> {
    private static final Logger LOG                                              = LoggerFactory.getLogger(AsyncImportService.class);
    private static final int    MAX_ATTEMPTS                                     = 3;
    private static final String EXCEPTION_CLASS_NAME_PERMANENT_LOCKING_EXCEPTION = "PermanentLockingException";

    private final DataAccess                                          dataAccess;
    private final ImportCacheManager<String, AtlasAsyncImportRequest> importCache;
    private final long                                                processingStaleThresholdMs;
    private final String                                              nodeId;

    @Inject
    public AsyncImportService(DataAccess dataAccess) {
        this(dataAccess, AtlasConfiguration.ASYNC_IMPORT_CLAIM_STALE_THRESHOLD_MS.getLong());
    }

    AsyncImportService(DataAccess dataAccess, long processingStaleThresholdMs) {
        this.dataAccess  = dataAccess;
        this.importCache = new ImportCacheManager<>();
        this.processingStaleThresholdMs = processingStaleThresholdMs;
        this.nodeId = buildNodeId();
    }

    public void populateCache(AtlasAsyncImportRequest importRequest) {
        if (importRequest != null && StringUtils.isNotEmpty(importRequest.getGuid()) && importRequest.getGuid().charAt(0) != '-') {
            importCache.put(importRequest.getImportId(), importRequest);
        }
    }

    public AtlasAsyncImportRequest fetchImportRequestByImportId(String importId) {
        try {
            AtlasAsyncImportRequest cachedRequest = importCache.get(importId);

            if (cachedRequest != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache hit for importId: {}", importId);
                }
                return cachedRequest;
            }
            AtlasAsyncImportRequest request = new AtlasAsyncImportRequest();

            request.setImportId(importId);

            request = dataAccess.load(request);

            populateCache(request);

            return request;
        } catch (Exception e) {
            LOG.error("Error fetching request with importId: {}", importId, e);

            return null;
        }
    }

    public void saveImport(String importId) {
        try {
            AtlasAsyncImportRequest importRequest = importCache.get(importId);
            if (importRequest != null) {
                saveImportRequest(importRequest);
                importCache.invalidate(importId);
            }
        } catch (Throwable e) {
            LOG.error("Error saving import request from cache for importId: {}", importId, e);
        }
    }

    public void saveImportRequest(AtlasAsyncImportRequest importRequest) throws AtlasBaseException {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                dataAccess.saveNoLoad(importRequest);
                LOG.debug("Save request ID: {} request: {}", importRequest.getImportId(), importRequest);
                return;
            } catch (Throwable e) {
                List<Throwable> throwableList = ExceptionUtils.getThrowableList(e);

                if (!throwableList.isEmpty()
                        && containsException(throwableList, EXCEPTION_CLASS_NAME_PERMANENT_LOCKING_EXCEPTION)
                        && (attempt < MAX_ATTEMPTS - 1)) {
                    LOG.error("Caught {} , Retrying the transaction, attempt count is:{}",
                            EXCEPTION_CLASS_NAME_PERMANENT_LOCKING_EXCEPTION, attempt);
                    continue;
                }

                LOG.error("Failed to save import: {} with request: {}", importRequest.getImportId(), importRequest, e);
                if (e instanceof AtlasBaseException) {
                    throw (AtlasBaseException) e;
                }

                throw new AtlasBaseException(AtlasErrorCode.IMPORT_FAILED, e);
            }
        }
        throw new AtlasBaseException(AtlasErrorCode.IMPORT_FAILED, "Failed to save import request after retries");
    }

    public void updateImportRequest(AtlasAsyncImportRequest importRequest) {
        try {
            saveImportRequest(importRequest);
        } catch (AtlasBaseException abe) {
            LOG.error("Failed to update import: {} with request: {}", importRequest.getImportId(), importRequest, abe);
        }
    }

    /**
     * Returns a fresh view of the import request, resolving a stuck PROCESSING request to a
     * terminal status when all published entities have already been processed.
     *
     * <p>Entity progress is often only in the local cache until {@code onImportComplete} persists
     * it, so the cache is consulted before invalidating. If the cache is incomplete, a fresh
     * JanusGraph read is used (required for active-active correctness).
     */
    public AtlasAsyncImportRequest resolveRequestStatus(String importId) throws AtlasBaseException {
        AtlasAsyncImportRequest cached = importCache.get(importId);

        if (cached != null
                && cached.getStatus() == ImportStatus.PROCESSING
                && isProcessingComplete(cached)) {
            return finalizeCompletedProcessingRequest(cached);
        }

        importCache.invalidate(importId);

        AtlasAsyncImportRequest importRequest = fetchImportRequestByImportId(importId);
        if (importRequest == null
                || importRequest.getStatus() != ImportStatus.PROCESSING
                || !isProcessingComplete(importRequest)) {
            return importRequest;
        }

        return finalizeCompletedProcessingRequest(importRequest);
    }

    public List<String> fetchInProgressImportIds() {
        return AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, ImportStatus.PROCESSING),
                PROPERTY_KEY_ASYNC_IMPORT_ID);
    }

    private boolean containsException(final List<Throwable> exceptions, final String exceptionName) {
        return exceptions.stream().anyMatch(o -> o.getClass().getSimpleName().equals(exceptionName));
    }

    private AtlasAsyncImportRequest finalizeCompletedProcessingRequest(AtlasAsyncImportRequest importRequest) throws AtlasBaseException {
        ImportStatus resolvedStatus = resolveCompletedStatus(importRequest);
        importRequest.setStatus(resolvedStatus);
        importRequest.setCompletedTime(System.currentTimeMillis());

        AtlasImportResult importResult = importRequest.getImportResult();
        if (importResult != null) {
            importResult.setOperationStatus(resolveOperationStatus(resolvedStatus));
            importRequest.setImportResult(importResult);
        }

        saveImportRequest(importRequest);
        populateCache(importRequest);

        LOG.info("Resolved completed PROCESSING request importId={} to status={}",
                importRequest.getImportId(), resolvedStatus);

        return importRequest;
    }

    /**
     * Matches {@link org.apache.atlas.repository.impexp.ImportService#onImportEntity} completion:
     * processing is done when every published entity has been imported or failed.
     */
    private boolean isProcessingComplete(AtlasAsyncImportRequest importRequest) {
        AtlasAsyncImportRequest.ImportDetails details = importRequest.getImportDetails();

        if (details == null || details.getPublishedEntityCount() <= 0) {
            return false;
        }

        int processedEntities = details.getImportedEntitiesCount() + details.getFailedEntitiesCount();
        return processedEntities >= details.getPublishedEntityCount();
    }

    private ImportStatus resolveCompletedStatus(AtlasAsyncImportRequest importRequest) {
        AtlasAsyncImportRequest.ImportDetails details = importRequest.getImportDetails();
        if (details.getTotalEntitiesCount() == details.getImportedEntitiesCount()) {
            return ImportStatus.SUCCESSFUL;
        } else if (details.getImportedEntitiesCount() > 0) {
            return ImportStatus.PARTIAL_SUCCESS;
        }

        return ImportStatus.FAILED;
    }

    private AtlasImportResult.OperationStatus resolveOperationStatus(ImportStatus status) {
        if (status == ImportStatus.SUCCESSFUL) {
            return SUCCESS;
        } else if (status == ImportStatus.PARTIAL_SUCCESS) {
            return PARTIAL_SUCCESS;
        }

        return FAIL;
    }

    public List<String> fetchQueuedImportRequests() {
        return AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, ImportStatus.WAITING),
                PROPERTY_KEY_ASYNC_IMPORT_ID);
    }

    /**
     * Implements {@link GraphClaimable#tryClaim()}: claims the next WAITING import.
     * Delegates to {@link #claimNextWaitingImport()}.
     */
    @Override
    public AtlasAsyncImportRequest tryClaim() throws AtlasBaseException {
        return claimNextWaitingImport();
    }

    @Override
    @GraphTransaction
    public void recoverStaleClaims() throws AtlasBaseException {
        for (String importId : fetchInProgressImportIds()) {
            AtlasAsyncImportRequest processingImport = loadFresh(importId);

            if (processingImport == null || !ImportStatus.PROCESSING.equals(processingImport.getStatus())) {
                continue;
            }

            if (!isStaleProcessingImport(processingImport, System.currentTimeMillis())) {
                continue;
            }

            reclaimStaleProcessingImport(processingImport);
        }
    }

    /**
     * Atomically claims the next WAITING import for processing on this node.
     *
     * <p>The entire check-then-act is wrapped in a single {@link GraphTransaction}: JanusGraph's
     * row-level HBase locking ensures that when two nodes race, only one can commit the
     * WAITING → PROCESSING transition.  The loser gets a locking conflict, the
     * {@link org.apache.atlas.GraphTransactionInterceptor} retries the transaction, and on
     * retry the loser finds the import already PROCESSING → returns {@code null}.
     *
     * @return the claimed {@link AtlasAsyncImportRequest} (already persisted as PROCESSING),
     *         or {@code null} if nothing is claimable (another import is running or no WAITING imports exist).
     */
    @GraphTransaction
    public AtlasAsyncImportRequest claimNextWaitingImport() throws AtlasBaseException {
        if (hasAnyActiveProcessingImport()) {
            LOG.debug("claimNextWaitingImport(): node={} an import is already PROCESSING globally, skipping", nodeId);
            return null;
        }

        List<String> waitingIds = fetchQueuedImportRequests();
        if (waitingIds.isEmpty()) {
            LOG.debug("claimNextWaitingImport(): node={} no imports in WAITING state", nodeId);
            return null;
        }

        String importId = waitingIds.get(0);

        // Status check: read fresh from JanusGraph — NOT from the per-JVM importCache.
        // The cache is node-local; in active-active mode another node may have already
        // transitioned this import to PROCESSING while our cache still shows WAITING.
        // Only the status field needs a live read; all other fields (parameters, topic name,
        // importId) are written once at creation and are safe to serve from cache after claiming.
        ImportStatus liveStatus = fetchStatusFromGraph(importId);
        if (liveStatus == null || !ImportStatus.WAITING.equals(liveStatus)) {
            LOG.debug("claimNextWaitingImport(): node={} import {} is no longer WAITING (concurrent claim), liveStatus={}",
                    nodeId, importId, liveStatus);
            return null;
        }

        // Status confirmed WAITING in JanusGraph — now load the full object.
        // Use the cache for the remaining fields (avoids a second graph read for metadata
        // that cannot have changed since creation).
        AtlasAsyncImportRequest importRequest = fetchImportRequestByImportId(importId);
        if (importRequest == null) {
            LOG.debug("claimNextWaitingImport(): node={} import {} not found", nodeId, importId);
            return null;
        }

        importRequest.setStatus(ImportStatus.PROCESSING);
        importRequest.setProcessingStartTime(System.currentTimeMillis());
        saveImportRequest(importRequest);

        LOG.info("claimNextWaitingImport(): node={} successfully claimed import {}", nodeId, importId);
        return importRequest;
    }

    boolean hasAnyActiveProcessingImport() throws AtlasBaseException {
        for (String importId : fetchInProgressImportIds()) {
            AtlasAsyncImportRequest processingImport = loadFresh(importId);

            if (processingImport == null || !ImportStatus.PROCESSING.equals(processingImport.getStatus())) {
                continue;
            }

            return true;
        }

        return false;
    }

    boolean isStaleProcessingImport(AtlasAsyncImportRequest importRequest, long now) {
        long processingStartTime = importRequest.getProcessingStartTime();

        if (processingStartTime <= 0L) {
            return true;
        }

        return now - processingStartTime >= processingStaleThresholdMs;
    }

    private void reclaimStaleProcessingImport(AtlasAsyncImportRequest importRequest) throws AtlasBaseException {
        String importId = importRequest.getImportId();

        LOG.warn("claimNextWaitingImport(): node={} recovering stale PROCESSING import {} back to WAITING", nodeId, importId);

        importRequest.setStatus(ImportStatus.WAITING);
        importRequest.setProcessingStartTime(0L);
        saveImportRequest(importRequest);
    }

    /**
     * Loads the full import request directly from JanusGraph, bypassing the
     * per-JVM {@link #importCache}.  Used in the status-query path where any
     * mutable field (status, processingStartTime, errorMessage, progress) may
     * have been updated by another node and the cache would return stale data.
     *
     * @return the live {@link AtlasAsyncImportRequest}, or {@code null} if not found
     */
    AtlasAsyncImportRequest loadFresh(String importId) {
        try {
            AtlasAsyncImportRequest request = new AtlasAsyncImportRequest();
            request.setImportId(importId);
            return dataAccess.load(request);
        } catch (Exception e) {
            LOG.error("loadFresh(): failed to load import {} from JanusGraph", importId, e);
            return null;
        }
    }

    /**
     * Reads only the {@code status} property of an import request directly from
     * JanusGraph, bypassing the per-JVM {@link #importCache}.
     *
     * <p>Used exclusively in the CAS claim path where a stale cached status would
     * give a false positive on the WAITING check.  All other metadata fields (topic
     * name, parameters, importId) are written once at creation and are safe to read
     * from the cache after the status is confirmed live.
     *
     * @return the live {@link ImportStatus}, or {@code null} if the import is not found
     */
    ImportStatus fetchStatusFromGraph(String importId) {
        List<String> values = AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                ASYNC_IMPORT_TYPE_NAME,
                Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_ID, importId),
                PROPERTY_KEY_ASYNC_IMPORT_STATUS);
        if (values == null || values.isEmpty()) {
            return null;
        }
        try {
            return ImportStatus.valueOf(values.get(0));
        } catch (IllegalArgumentException e) {
            LOG.warn("fetchStatusFromGraph(): unrecognised status '{}' for import {}", values.get(0), importId);
            return null;
        }
    }

    private String buildNodeId() {
        String runMode  = AtlasRunMode.current().name();
        String hostName = System.getenv("HOSTNAME");
        String jvmId    = ManagementFactory.getRuntimeMXBean().getName();

        if (StringUtils.isBlank(hostName)) {
            hostName = "unknown-host";
        }

        return runMode + "@" + hostName + "#" + jvmId;
    }

    public void deleteRequests() {
        try {
            dataAccess.delete(AtlasGraphUtilsV2.findEntityGUIDsByType(ASYNC_IMPORT_TYPE_NAME, SortOrder.ASCENDING));

            importCache.clear();
        } catch (Exception e) {
            LOG.error("Error deleting import requests", e);
        }
    }

    public void deleteRequest(AtlasAsyncImportRequest importRequest) {
        try {
            if (importRequest != null) {
                dataAccess.delete(importRequest.getGuid());
                importCache.invalidate(importRequest.getImportId());
            }
        } catch (Exception e) {
            LOG.warn("Error deleting import request with importId: {}", importRequest.getImportId(), e);
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
            // Bypass the per-JVM cache entirely — load directly from JanusGraph.
            // In active-active mode, any field that changes during processing
            // (status, processingStartTime, errorMessage, progress counters) is updated
            // by whichever node owns the import.  A cache-first read on any other node
            // returns stale values for ALL of these fields, not just status.
            // Client status queries require correctness over performance, so we always
            // go to the authoritative store here.
            AtlasAsyncImportRequest importRequest = loadFresh(importId);

            if (importRequest == null) {
                throw new AtlasBaseException(AtlasErrorCode.IMPORT_NOT_FOUND, importId);
            }

            return importRequest;
        } finally {
            LOG.debug("<== AsyncImportService.getImportStatusById(importId={})", importId);
        }
    }
}
