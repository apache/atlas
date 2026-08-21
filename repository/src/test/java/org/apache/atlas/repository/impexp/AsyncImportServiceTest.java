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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.impexp.AsyncImportStatus;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.PROCESSING;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.SUCCESSFUL;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.WAITING;
import static org.apache.atlas.model.impexp.AtlasImportResult.OperationStatus.SUCCESS;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_ID;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_STATUS;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.ASYNC_IMPORT_TYPE_NAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class AsyncImportServiceTest {
    private DataAccess         dataAccess;
    private AtlasGraph         graph;
    private AsyncImportService asyncImportService;

    @Mock
    private AtlasGraphUtilsV2 atlasGraphUtilsV2;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);

        dataAccess         = mock(DataAccess.class);
        graph              = mock(AtlasGraph.class, RETURNS_DEEP_STUBS);
        asyncImportService = new AsyncImportService(dataAccess, graph);
    }

    @Test
    public void testFetchImportRequestByImportId() throws Exception {
        String                  importId    = "import123";
        AtlasAsyncImportRequest mockRequest = new AtlasAsyncImportRequest();

        mockRequest.setImportId(importId);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(mockRequest);

        AtlasAsyncImportRequest result = asyncImportService.fetchImportRequestByImportId(importId);

        assertNotNull(result);
        assertEquals(result.getImportId(), importId);
        verify(dataAccess, times(1)).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testFetchImportRequestByImportIdError() throws AtlasBaseException {
        String importId = "import123";

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenThrow(new RuntimeException("Test Exception"));

        AtlasAsyncImportRequest result = asyncImportService.fetchImportRequestByImportId(importId);

        assertNull(result);
        verify(dataAccess, times(1)).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testSaveImportRequest() throws AtlasBaseException {
        AtlasAsyncImportRequest importRequest = new AtlasAsyncImportRequest();

        importRequest.setImportId("import123");

        asyncImportService.saveImportRequest(importRequest);

        verify(dataAccess, times(1)).saveNoLoad(importRequest);
    }

    @Test
    public void testUpdateImportRequest() throws AtlasBaseException {
        AtlasAsyncImportRequest importRequest = new AtlasAsyncImportRequest();

        importRequest.setImportId("import123");

        doThrow(new AtlasBaseException("Save failed")).when(dataAccess).save(importRequest);

        asyncImportService.updateImportRequest(importRequest);

        verify(dataAccess, times(1)).saveNoLoad(importRequest);
    }

    @Test
    public void testFetchInProgressImportIds() throws AtlasBaseException {
        AtlasAsyncImportRequest request1 = new AtlasAsyncImportRequest();
        AtlasAsyncImportRequest request2 = new AtlasAsyncImportRequest();

        request1.setImportId("guid1");
        request1.setStatus(PROCESSING);

        request2.setImportId("guid2");
        request2.setStatus(SUCCESSFUL);

        try (MockedStatic<AtlasGraphUtilsV2> mockedStatic = mockStatic(AtlasGraphUtilsV2.class)) {
            mockedStatic.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                    Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, PROCESSING),
                    PROPERTY_KEY_ASYNC_IMPORT_ID)).thenReturn(Collections.singletonList("guid1"));

            mockedStatic.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                    Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, SUCCESSFUL),
                    PROPERTY_KEY_ASYNC_IMPORT_ID)).thenReturn(Collections.singletonList("guid2"));

            List<String> result = asyncImportService.fetchInProgressImportIds();

            assertEquals(result.size(), 1);
            assertTrue(result.contains("guid1"));

            mockedStatic.verify(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(anyString(), any(Map.class), anyString()));
        }
    }

    @Test
    public void testFetchQueuedImportRequests() throws AtlasBaseException {
        AtlasAsyncImportRequest request1 = new AtlasAsyncImportRequest();
        AtlasAsyncImportRequest request2 = new AtlasAsyncImportRequest();

        request1.setImportId("guid1");
        request1.setStatus(WAITING);

        request2.setImportId("guid2");
        request2.setStatus(PROCESSING);

        try (MockedStatic<AtlasGraphUtilsV2> mockStatic = mockStatic(AtlasGraphUtilsV2.class)) {
            mockStatic.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                    Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, WAITING),
                    PROPERTY_KEY_ASYNC_IMPORT_ID)).thenReturn(Collections.singletonList("guid1"));

            mockStatic.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(ASYNC_IMPORT_TYPE_NAME,
                    Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, PROCESSING),
                    PROPERTY_KEY_ASYNC_IMPORT_ID)).thenReturn(Collections.singletonList("guid2"));

            List<String> result = asyncImportService.fetchQueuedImportRequests();

            assertEquals(result.size(), 1);
            assertTrue(result.contains("guid1"));
        }
    }

    @Test
    public void testDeleteRequests() throws AtlasBaseException {
        List<String> guids = Arrays.asList("guid1", "guid2");

        try (MockedStatic<AtlasGraphUtilsV2> mockStatic = mockStatic(AtlasGraphUtilsV2.class)) {
            mockStatic.when(() -> AtlasGraphUtilsV2.findEntityGUIDsByType(anyString(), any())).thenReturn(guids);

            asyncImportService.deleteRequests();

            verify(dataAccess, times(1)).delete(guids);
        }
    }

    @Test
    public void testGetAsyncImportsStatus() throws AtlasBaseException {
        List<String>            guids            = Arrays.asList("guid1", "guid2");
        AtlasAsyncImportRequest request1         = spy(new AtlasAsyncImportRequest());
        AtlasImportResult       mockImportResult = mock(AtlasImportResult.class);

        request1.setImportId("guid1");
        request1.setStatus(AtlasAsyncImportRequest.ImportStatus.PROCESSING);
        request1.setReceivedTime(System.currentTimeMillis());

        doReturn("admin").when(mockImportResult).getUserName();
        request1.setImportResult(mockImportResult);

        int offset = 0;
        int limit = 10;

        try (MockedStatic<AtlasGraphUtilsV2> mockStatic = mockStatic(AtlasGraphUtilsV2.class)) {
            mockStatic.when(() -> AtlasGraphUtilsV2.findEntityGUIDsByType(anyString(), any())).thenReturn(guids);
            when(dataAccess.load(anyList())).thenReturn(Collections.singletonList(request1));

            PList<AsyncImportStatus> result = asyncImportService.getAsyncImportsStatus(offset, limit);

            assertEquals(result.getList().size(), 1);
            assertEquals(result.getList().get(0).getImportId(), "guid1");
            assertEquals(result.getList().get(0).getImportRequestUser(), "admin");

            verify(dataAccess, times(1)).load(anyList());
        }
    }

    @Test
    public void testGetImportStatusById() throws AtlasBaseException {
        String                  importId = "import123";
        AtlasAsyncImportRequest request  = new AtlasAsyncImportRequest();

        request.setImportId(importId);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(request);

        AtlasAsyncImportRequest result = asyncImportService.getAsyncImportRequest(importId);

        assertNotNull(result);
        assertEquals(result.getImportId(), importId);
        verify(dataAccess, times(1)).load(any(AtlasAsyncImportRequest.class));
    }

    // =====================================================================
// Tests for GraphClaimable integration and cache-bypass behaviour
// Added for active-active HA: verify tryClaim, fetchStatusFromGraph,
// loadFresh, and cache-bypass in claim and status paths.
// =====================================================================

    // ----- tryClaim -----

    @Test
    public void testTryClaim_delegatesToClaimNextWaitingImport_whenNothingWaiting() throws Exception {
        try (MockedStatic<AtlasGraphUtilsV2> mock = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            // No PROCESSING, no WAITING
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                    ASYNC_IMPORT_TYPE_NAME, Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, PROCESSING),
                    PROPERTY_KEY_ASYNC_IMPORT_ID)).thenReturn(Collections.emptyList());
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                    ASYNC_IMPORT_TYPE_NAME, Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, WAITING),
                    PROPERTY_KEY_ASYNC_IMPORT_ID)).thenReturn(Collections.emptyList());

            AtlasAsyncImportRequest result = asyncImportService.tryClaim();

            org.testng.Assert.assertNull(result, "tryClaim must return null when nothing is WAITING");
        }
    }

    @Test
    public void testClaimNextWaitingImport_concurrentCall_onlyOneNodeClaims() throws Exception {
        AtomicBoolean globallyClaimed = new AtomicBoolean(false);

        AsyncImportService service = new AsyncImportService(dataAccess, graph, 60_000L) {
            @Override
            boolean hasAnyActiveProcessingImport() {
                return false;
            }

            @Override
            public List<String> fetchQueuedImportRequests() {
                return Collections.singletonList("imp-race");
            }

            @Override
            AtlasAsyncImportRequest.ImportStatus fetchStatusFromGraph(String importId) {
                return globallyClaimed.compareAndSet(false, true) ? WAITING : PROCESSING;
            }

            @Override
            public AtlasAsyncImportRequest fetchImportRequestByImportId(String importId) {
                AtlasAsyncImportRequest request = new AtlasAsyncImportRequest();
                request.setImportId(importId);
                request.setStatus(WAITING);
                return request;
            }

            @Override
            public void saveImportRequest(AtlasAsyncImportRequest importRequest) {
                // no-op for deterministic unit test
            }
        };

        CountDownLatch startGate = new CountDownLatch(1);
        AtomicReference<AtlasAsyncImportRequest> result1 = new AtomicReference<>();
        AtomicReference<AtlasAsyncImportRequest> result2 = new AtomicReference<>();
        AtomicReference<Exception> error1 = new AtomicReference<>();
        AtomicReference<Exception> error2 = new AtomicReference<>();

        Thread t1 = new Thread(() -> {
            try {
                startGate.await(5, TimeUnit.SECONDS);
                result1.set(service.claimNextWaitingImport());
            } catch (Exception e) {
                error1.set(e);
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                startGate.await(5, TimeUnit.SECONDS);
                result2.set(service.claimNextWaitingImport());
            } catch (Exception e) {
                error2.set(e);
            }
        });

        t1.start();
        t2.start();
        startGate.countDown();
        t1.join(TimeUnit.SECONDS.toMillis(5));
        t2.join(TimeUnit.SECONDS.toMillis(5));

        assertNull(error1.get(), "Thread 1 should not fail");
        assertNull(error2.get(), "Thread 2 should not fail");

        int nonNullClaims = 0;
        if (result1.get() != null) {
            nonNullClaims++;
        }
        if (result2.get() != null) {
            nonNullClaims++;
        }

        assertEquals(nonNullClaims, 1,
                "Exactly one concurrent claimNextWaitingImport() call should claim the WAITING request");
    }

    @Test
    public void testClaimNextWaitingImport_returnsNull_whenAnyGlobalProcessingExists() throws Exception {
        AsyncImportService service = spy(asyncImportService);

        doReturn(Collections.singletonList("processing-import")).when(service).fetchInProgressImportIds();

        AtlasAsyncImportRequest processing = new AtlasAsyncImportRequest();
        processing.setImportId("processing-import");
        processing.setStatus(PROCESSING);
        doReturn(processing).when(service).loadFresh("processing-import");

        AtlasAsyncImportRequest result = service.claimNextWaitingImport();

        assertNull(result, "Claim should return null when any import is globally PROCESSING");
        verify(service, never()).fetchQueuedImportRequests();
    }

    @Test
    public void testClaimNextWaitingImport_returnsNull_whenConcurrentNodeAlreadyClaimedLiveStatus() throws Exception {
        String importId = "imp-concurrent-race";
        AtlasAsyncImportRequest cachedWaiting = new AtlasAsyncImportRequest();
        cachedWaiting.setImportId(importId);
        cachedWaiting.setStatus(WAITING);
        asyncImportService.populateCache(cachedWaiting);

        try (MockedStatic<AtlasGraphUtilsV2> mock = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            // Global pre-check: no active PROCESSING imports.
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, PROCESSING),
                            PROPERTY_KEY_ASYNC_IMPORT_ID))
                    .thenReturn(Collections.emptyList());

            // One WAITING id is discoverable from queue scan.
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, WAITING),
                            PROPERTY_KEY_ASYNC_IMPORT_ID))
                    .thenReturn(Collections.singletonList(importId));

            // Live graph status check sees that another node already moved it to PROCESSING.
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_ID, importId),
                            PROPERTY_KEY_ASYNC_IMPORT_STATUS))
                    .thenReturn(Collections.singletonList(PROCESSING.name()));

            AtlasAsyncImportRequest claimed = asyncImportService.claimNextWaitingImport();

            assertNull(claimed, "claim must abort when live status is no longer WAITING");
            verify(dataAccess, never()).saveNoLoad(any(AtlasAsyncImportRequest.class));
        }
    }

    @Test
    public void testClaimNextWaitingImport_claimsWaitingImport_whenLiveStatusStillWaiting() throws Exception {
        String importId = "imp-claim-success";
        AtlasAsyncImportRequest liveWaiting = new AtlasAsyncImportRequest();
        liveWaiting.setImportId(importId);
        liveWaiting.setStatus(WAITING);
        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(liveWaiting);

        try (MockedStatic<AtlasGraphUtilsV2> mock = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            // No active processing imports.
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, PROCESSING),
                            PROPERTY_KEY_ASYNC_IMPORT_ID))
                    .thenReturn(Collections.emptyList());

            // One queued waiting import.
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_STATUS, WAITING),
                            PROPERTY_KEY_ASYNC_IMPORT_ID))
                    .thenReturn(Collections.singletonList(importId));

            // Live status still WAITING -> this node can claim.
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_ID, importId),
                            PROPERTY_KEY_ASYNC_IMPORT_STATUS))
                    .thenReturn(Collections.singletonList(WAITING.name()));

            AtlasAsyncImportRequest claimed = asyncImportService.claimNextWaitingImport();

            assertNotNull(claimed);
            assertEquals(claimed.getImportId(), importId);
            assertEquals(claimed.getStatus(), PROCESSING);
            assertTrue(claimed.getProcessingStartTime() > 0L,
                    "claimed import should have processingStartTime set");
            verify(dataAccess, times(1)).saveNoLoad(liveWaiting);
        }
    }

    /**
     * Imports run one at a time across the cluster, and only a claim's own holder can renew it.  So a
     * claim left behind after an import finishes does not just linger - it locks every other node out
     * of starting an import until the lease lapses.
     */
    @Test
    public void finishedImportHandsTheClaimBack() throws Exception {
        AsyncImportService service  = spy(new AsyncImportService(dataAccess, graph, 60_000L));
        AtlasAsyncImportRequest finished = new AtlasAsyncImportRequest();

        finished.setImportId("imp-finished");
        finished.setStatus(SUCCESSFUL);

        service.saveImportRequest(finished);

        verify(service, times(1)).releaseImportClaim();
    }

    @Test
    public void importStillRunningKeepsItsClaim() throws Exception {
        AsyncImportService service = spy(new AsyncImportService(dataAccess, graph, 60_000L));
        AtlasAsyncImportRequest running = new AtlasAsyncImportRequest();

        running.setImportId("imp-running");
        running.setStatus(PROCESSING);

        service.saveImportRequest(running);

        verify(service, never()).releaseImportClaim();
    }

    /**
     * Failing to start after taking the claim must not leave the claim held, or the queue stalls for
     * the whole lease with nothing running.
     */
    @Test
    public void claimIsHandedBackWhenStartingTheImportFails() throws Exception {
        AsyncImportService service = spy(new AsyncImportService(dataAccess, graph, 60_000L));

        doReturn(false).when(service).hasAnyActiveProcessingImport();
        doReturn(Collections.singletonList("imp-explodes")).when(service).fetchQueuedImportRequests();
        doThrow(new IllegalStateException("graph unavailable")).when(service).fetchStatusFromGraph(anyString());

        try {
            service.claimNextWaitingImport();

            org.testng.Assert.fail("The failure must reach the caller");
        } catch (IllegalStateException expected) {
            assertEquals(expected.getMessage(), "graph unavailable");
        }

        verify(service, times(1)).releaseImportClaim();
    }

    // ----- fetchStatusFromGraph -----

    @Test
    public void testFetchStatusFromGraph_returnsLiveStatus() {
        String importId = "imp-fetch-status";

        try (MockedStatic<AtlasGraphUtilsV2> mock = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_ID, importId),
                            PROPERTY_KEY_ASYNC_IMPORT_STATUS))
                    .thenReturn(Collections.singletonList(PROCESSING.name()));

            AtlasAsyncImportRequest.ImportStatus status = asyncImportService.fetchStatusFromGraph(importId);

            org.testng.Assert.assertEquals(status, PROCESSING);
        }
    }

    @Test
    public void testFetchStatusFromGraph_returnsNull_whenNotFound() {
        String importId = "imp-not-found";

        try (MockedStatic<AtlasGraphUtilsV2> mock = Mockito.mockStatic(AtlasGraphUtilsV2.class)) {
            mock.when(() -> AtlasGraphUtilsV2.findEntityPropertyValuesByTypeAndAttributes(
                            ASYNC_IMPORT_TYPE_NAME,
                            Collections.singletonMap(PROPERTY_KEY_ASYNC_IMPORT_ID, importId),
                            PROPERTY_KEY_ASYNC_IMPORT_STATUS))
                    .thenReturn(Collections.emptyList());

            AtlasAsyncImportRequest.ImportStatus status = asyncImportService.fetchStatusFromGraph(importId);

            org.testng.Assert.assertNull(status);
        }
    }

    // ----- loadFresh -----

    @Test
    public void testLoadFresh_loadsDirectlyFromGraph_bypassingCache() throws Exception {
        String importId = "imp-loadfresh";

        // Pre-populate cache with stale WAITING status
        AtlasAsyncImportRequest stale = new AtlasAsyncImportRequest();
        stale.setImportId(importId);
        stale.setStatus(WAITING);
        asyncImportService.populateCache(stale);

        // Graph has PROCESSING (updated by another node)
        AtlasAsyncImportRequest live = new AtlasAsyncImportRequest();
        live.setImportId(importId);
        live.setStatus(PROCESSING);
        Mockito.when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(live);

        AtlasAsyncImportRequest result = asyncImportService.loadFresh(importId);

        org.testng.Assert.assertNotNull(result);
        org.testng.Assert.assertEquals(result.getStatus(), PROCESSING,
                "loadFresh must return live graph value, not stale cache");
        Mockito.verify(dataAccess, Mockito.times(1)).load(any(AtlasAsyncImportRequest.class));
    }

    // ----- getAsyncImportRequest: always bypasses cache -----

    @Test
    public void testGetAsyncImportRequest_alwaysLoadsFromGraph() throws Exception {
        String importId = "imp-status-fresh";

        // Cache has stale WAITING
        AtlasAsyncImportRequest stale = new AtlasAsyncImportRequest();
        stale.setImportId(importId);
        stale.setStatus(WAITING);
        asyncImportService.populateCache(stale);

        // JanusGraph has PROCESSING
        AtlasAsyncImportRequest live = new AtlasAsyncImportRequest();
        live.setImportId(importId);
        live.setStatus(PROCESSING);
        Mockito.when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(live);

        AtlasAsyncImportRequest result = asyncImportService.getAsyncImportRequest(importId);

        org.testng.Assert.assertEquals(result.getStatus(), PROCESSING,
                "Status endpoint must return live JanusGraph value, not stale cached value");
        Mockito.verify(dataAccess, Mockito.times(1)).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testHasAnyActiveProcessingImport_reclaimsStaleProcessingImport() throws AtlasBaseException {
        AsyncImportService service = spy(new AsyncImportService(dataAccess, graph, 1000L));
        String staleImportId = "stale-processing";

        AtlasAsyncImportRequest stale = new AtlasAsyncImportRequest();
        stale.setImportId(staleImportId);
        stale.setStatus(PROCESSING);
        stale.setProcessingStartTime(System.currentTimeMillis() - 5000L);

        doReturn(Collections.singletonList(staleImportId)).when(service).fetchInProgressImportIds();
        doReturn(stale).when(service).loadFresh(staleImportId);

        service.recoverStaleClaims();
        boolean hasActiveProcessing = service.hasAnyActiveProcessingImport();

        assertFalse(hasActiveProcessing, "stale PROCESSING import should be reclaimed and not block claims");
        assertEquals(stale.getStatus(), WAITING, "stale import should be moved back to WAITING");
        assertEquals(stale.getProcessingStartTime(), 0L, "reclaimed import should reset processing start time");
        verify(service, times(1)).saveImportRequest(stale);
    }

    @Test
    public void testHasAnyActiveProcessingImport_keepsFreshProcessingImportActive() throws AtlasBaseException {
        AsyncImportService service = spy(new AsyncImportService(dataAccess, graph, 60000L));
        String activeImportId = "active-processing";

        AtlasAsyncImportRequest active = new AtlasAsyncImportRequest();
        active.setImportId(activeImportId);
        active.setStatus(PROCESSING);
        active.setProcessingStartTime(System.currentTimeMillis());

        doReturn(Collections.singletonList(activeImportId)).when(service).fetchInProgressImportIds();
        doReturn(active).when(service).loadFresh(activeImportId);

        boolean hasActiveProcessing = service.hasAnyActiveProcessingImport();

        assertTrue(hasActiveProcessing, "fresh PROCESSING import should continue to block new claims");
        verify(service, times(0)).saveImportRequest(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testSaveImportHandlesRuntimeException() throws AtlasBaseException {
        AtlasAsyncImportRequest importRequest = new AtlasAsyncImportRequest();
        importRequest.setImportId("import-runtime");
        importRequest.setGuid("guid-runtime");

        asyncImportService.populateCache(importRequest);

        doThrow(new RuntimeException(new PermanentLockingException("lock conflict"))).when(dataAccess).saveNoLoad(importRequest);

        asyncImportService.saveImport("import-runtime");

        verify(dataAccess, times(3)).saveNoLoad(importRequest);
    }

    @Test
    public void testSaveImportRequestRetriesAndSucceeds() throws AtlasBaseException {
        AtlasAsyncImportRequest importRequest = new AtlasAsyncImportRequest();
        importRequest.setImportId("import123");

        doThrow(new RuntimeException(new PermanentLockingException("lock conflict")))
                .doNothing()
                .when(dataAccess).saveNoLoad(importRequest);

        asyncImportService.saveImportRequest(importRequest);

        verify(dataAccess, times(2)).saveNoLoad(importRequest);
    }

    @Test
    public void testSaveImportRequestNoRetryForNonTransactionFailure() throws AtlasBaseException {
        AtlasAsyncImportRequest importRequest = new AtlasAsyncImportRequest();
        importRequest.setImportId("import123");

        doThrow(new RuntimeException("validation failure")).when(dataAccess).saveNoLoad(importRequest);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> asyncImportService.saveImportRequest(importRequest));
        assertEquals(exception.getAtlasErrorCode(), org.apache.atlas.AtlasErrorCode.IMPORT_FAILED);
        verify(dataAccess, times(1)).saveNoLoad(importRequest);
    }

    @Test
    public void testResolveRequestStatusResolvesCompletedProcessingRequest() throws AtlasBaseException {
        String importId = "import-complete";

        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest(new AtlasImportResult());
        request.setImportId(importId);
        request.setStatus(PROCESSING);
        request.getImportDetails().setTotalEntitiesCount(5);
        request.getImportDetails().setPublishedEntityCount(5);
        request.getImportDetails().setImportedEntitiesCount(5);
        request.getImportDetails().setFailedEntitiesCount(0);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(request);

        AtlasAsyncImportRequest resolved = asyncImportService.resolveRequestStatus(importId);

        assertEquals(resolved.getStatus(), SUCCESSFUL);
        assertEquals(resolved.getImportResult().getOperationStatus(), SUCCESS);
        verify(dataAccess, times(1)).saveNoLoad(request);
    }

    @Test
    public void testResolveRequestStatusResolvesFromCacheWhenProgressNotYetPersisted() throws AtlasBaseException {
        String importId = "import-cache-complete";

        AtlasAsyncImportRequest cached = new AtlasAsyncImportRequest(new AtlasImportResult());
        cached.setImportId(importId);
        cached.setGuid("guid-cache-complete");
        cached.setStatus(PROCESSING);
        cached.getImportDetails().setTotalEntitiesCount(5);
        cached.getImportDetails().setPublishedEntityCount(5);
        cached.getImportDetails().setImportedEntitiesCount(5);
        cached.getImportDetails().setFailedEntitiesCount(0);

        asyncImportService.populateCache(cached);

        AtlasAsyncImportRequest staleGraph = new AtlasAsyncImportRequest(new AtlasImportResult());
        staleGraph.setImportId(importId);
        staleGraph.setStatus(PROCESSING);
        staleGraph.getImportDetails().setTotalEntitiesCount(5);
        staleGraph.getImportDetails().setPublishedEntityCount(5);
        staleGraph.getImportDetails().setImportedEntitiesCount(0);
        staleGraph.getImportDetails().setFailedEntitiesCount(0);
        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(staleGraph);

        AtlasAsyncImportRequest resolved = asyncImportService.resolveRequestStatus(importId);

        assertEquals(resolved.getStatus(), SUCCESSFUL);
        verify(dataAccess, times(1)).saveNoLoad(cached);
        verify(dataAccess, never()).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testResolveRequestStatusDoesNotResolveIncompleteProcessingRequest() throws AtlasBaseException {
        String importId = "import-incomplete";

        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest(new AtlasImportResult());
        request.setImportId(importId);
        request.setStatus(PROCESSING);
        request.getImportDetails().setTotalEntitiesCount(5);
        request.getImportDetails().setPublishedEntityCount(5);
        request.getImportDetails().setImportedEntitiesCount(2);
        request.getImportDetails().setFailedEntitiesCount(1);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(request);

        AtlasAsyncImportRequest resolved = asyncImportService.resolveRequestStatus(importId);

        assertEquals(resolved.getStatus(), PROCESSING);
        verify(dataAccess, times(0)).saveNoLoad(request);
    }

    @AfterMethod
    public void tearDown() {
        Mockito.reset(dataAccess);
    }
}
