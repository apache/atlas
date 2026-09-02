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
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
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

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.PROCESSING;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.SUCCESSFUL;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.WAITING;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_ID;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_ASYNC_IMPORT_STATUS;
import static org.apache.atlas.repository.ogm.impexp.AtlasAsyncImportRequestDTO.ASYNC_IMPORT_TYPE_NAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class AsyncImportServiceTest {
    private DataAccess         dataAccess;
    private AsyncImportService asyncImportService;

    @Mock
    private AtlasGraphUtilsV2 atlasGraphUtilsV2;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);

        dataAccess         = mock(DataAccess.class);
        asyncImportService = new AsyncImportService(dataAccess);

        // keep retry-backed reads from sleeping through the configured graph storage delay
        asyncImportService.retryDelayMs = 1;
        asyncImportService.maxAttempts  = 3;
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
    public void testFetchImportRequestWithRetrySucceedsAfterTransientFailure() throws AtlasBaseException {
        String                  importId    = "import-transient";
        AtlasAsyncImportRequest mockRequest = new AtlasAsyncImportRequest();

        mockRequest.setImportId(importId);

        doThrow(new RuntimeException("backend unavailable"))
                .doThrow(new RuntimeException("backend unavailable"))
                .doReturn(mockRequest)
                .when(dataAccess).load(any(AtlasAsyncImportRequest.class));

        AtlasAsyncImportRequest result = asyncImportService.fetchImportRequestByImportIdWithRetry(importId);

        assertNotNull(result);
        assertEquals(result.getImportId(), importId);
        verify(dataAccess, times(3)).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testFetchImportRequestWithRetryGivesUpAfterMaxAttempts() throws AtlasBaseException {
        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenThrow(new RuntimeException("backend unavailable"));

        assertNull(asyncImportService.fetchImportRequestByImportIdWithRetry("import-unreadable"));

        verify(dataAccess, times(3)).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testFetchImportRequestWithRetryDoesNotRetryMissingRequest() throws AtlasBaseException {
        when(dataAccess.load(any(AtlasAsyncImportRequest.class)))
                .thenThrow(new AtlasBaseException(org.apache.atlas.AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, "type", "attrs"));

        assertNull(asyncImportService.fetchImportRequestByImportIdWithRetry("import-missing"));

        // a request that does not exist is a definitive answer, retrying it only delays the caller
        verify(dataAccess, times(1)).load(any(AtlasAsyncImportRequest.class));
    }

    @Test
    public void testFetchImportRequestWithRetryServesCachedRequestWithoutLoading() throws AtlasBaseException {
        AtlasAsyncImportRequest cached = new AtlasAsyncImportRequest(new AtlasImportResult());

        cached.setImportId("import-cached");
        cached.setGuid("guid-cached");
        cached.setStatus(PROCESSING);

        asyncImportService.populateCache(cached);

        assertSame(asyncImportService.fetchImportRequestByImportIdWithRetry("import-cached"), cached);

        verify(dataAccess, times(0)).load(any(AtlasAsyncImportRequest.class));
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

    @Test
    public void testResolveAbandonedRequestResolvesIncompleteProcessingRequest() throws AtlasBaseException {
        String importId = "import-abandoned";

        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest(new AtlasImportResult());
        request.setImportId(importId);
        request.setStatus(PROCESSING);
        request.getImportDetails().setTotalEntitiesCount(5);
        request.getImportDetails().setImportedEntitiesCount(2);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(request);

        // the topic is drained, so the request must be resolved even though its counters are short
        AtlasAsyncImportRequest resolved = asyncImportService.resolveAbandonedRequest(importId);

        assertEquals(resolved.getStatus(), AtlasAsyncImportRequest.ImportStatus.PARTIAL_SUCCESS);
        assertTrue(resolved.getCompletedTime() > 0);
        verify(dataAccess, times(1)).saveNoLoad(request);
    }

    @Test
    public void testResolveAbandonedRequestResolvesRequestThatImportedNothing() throws AtlasBaseException {
        String importId = "import-abandoned-empty";

        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest(new AtlasImportResult());
        request.setImportId(importId);
        request.setStatus(PROCESSING);
        request.getImportDetails().setTotalEntitiesCount(5);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(request);

        AtlasAsyncImportRequest resolved = asyncImportService.resolveAbandonedRequest(importId);

        assertEquals(resolved.getStatus(), AtlasAsyncImportRequest.ImportStatus.FAILED);
        verify(dataAccess, times(1)).saveNoLoad(request);
    }

    @Test
    public void testResolveAbandonedRequestLeavesNonProcessingRequestUntouched() throws AtlasBaseException {
        String importId = "import-waiting";

        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest(new AtlasImportResult());
        request.setImportId(importId);
        request.setStatus(WAITING);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(request);

        AtlasAsyncImportRequest resolved = asyncImportService.resolveAbandonedRequest(importId);

        assertEquals(resolved.getStatus(), WAITING);
        verify(dataAccess, times(0)).saveNoLoad(request);
    }

    @Test
    public void testResolveRequestStatusKeepsInFlightProgressCached() throws AtlasBaseException {
        String importId = "import-in-flight";

        AtlasAsyncImportRequest inFlight = new AtlasAsyncImportRequest(new AtlasImportResult());
        inFlight.setImportId(importId);
        inFlight.setGuid("guid-in-flight");
        inFlight.setStatus(PROCESSING);
        inFlight.getImportDetails().setTotalEntitiesCount(5);
        inFlight.getImportDetails().setImportedEntitiesCount(4);

        asyncImportService.populateCache(inFlight);

        // the persisted copy is stale: progress is not written to the graph until the import completes
        AtlasAsyncImportRequest persisted = new AtlasAsyncImportRequest(new AtlasImportResult());
        persisted.setImportId(importId);
        persisted.setStatus(PROCESSING);
        persisted.getImportDetails().setTotalEntitiesCount(5);

        when(dataAccess.load(any(AtlasAsyncImportRequest.class))).thenReturn(persisted);

        AtlasAsyncImportRequest resolved = asyncImportService.resolveRequestStatus(importId);

        assertSame(resolved, inFlight);
        assertEquals(resolved.getImportDetails().getImportedEntitiesCount(), 4);
        verify(dataAccess, times(0)).load(any(AtlasAsyncImportRequest.class));

        // the entry must still be cached, otherwise the running import loses its progress
        assertSame(asyncImportService.fetchImportRequestByImportId(importId), inFlight);
    }

    @Test
    public void testCacheDoesNotEvictInFlightRequestUnderSizePressure() throws InterruptedException {
        AtlasAsyncImportRequest inFlight = new AtlasAsyncImportRequest(new AtlasImportResult());
        inFlight.setImportId("import-processing");
        inFlight.setGuid("guid-processing");
        inFlight.setStatus(PROCESSING);
        inFlight.getImportDetails().setImportedEntitiesCount(7);

        asyncImportService.populateCache(inFlight);

        // eviction targets the oldest entry, so make the in-flight request the unambiguous candidate
        Thread.sleep(5);

        for (int i = 0; i < 25; i++) {
            AtlasAsyncImportRequest terminal = new AtlasAsyncImportRequest(new AtlasImportResult());
            terminal.setImportId("import-done-" + i);
            terminal.setGuid("guid-done-" + i);
            terminal.setStatus(SUCCESSFUL);

            asyncImportService.populateCache(terminal);
        }

        assertSame(asyncImportService.fetchImportRequestByImportId("import-processing"), inFlight);
    }

    @Test
    public void testIsTerminalPinsOnlyInFlightRequests() {
        AtlasAsyncImportRequest request = new AtlasAsyncImportRequest(new AtlasImportResult());

        for (AtlasAsyncImportRequest.ImportStatus status : AtlasAsyncImportRequest.ImportStatus.values()) {
            request.setStatus(status);

            boolean inFlight = status == AtlasAsyncImportRequest.ImportStatus.STAGING
                    || status == WAITING
                    || status == PROCESSING;

            assertEquals(AsyncImportService.isTerminal(request), !inFlight, "unexpected pinning for status " + status);
        }
    }

    @AfterMethod
    public void tearDown() {
        Mockito.reset(dataAccess);
    }
}
