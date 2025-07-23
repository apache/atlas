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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.repository.impexp.AsyncImportService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.ABORTED;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.FAILED;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus.WAITING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ImportTaskListenerImplTest {
    private static final String VALID_IMPORT_ID   = "valid-id";
    private static final String INVALID_IMPORT_ID = "invalid-id";

    @Mock
    private AsyncImportService asyncImportService;

    @Mock
    private NotificationHookConsumer notificationHookConsumer;

    @Mock
    private BlockingDeque<String> requestQueue;

    @InjectMocks
    private ImportTaskListenerImpl importTaskListener;

    private AtlasAsyncImportRequest importRequest;

    @BeforeTest
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);

        importRequest = mock(AtlasAsyncImportRequest.class);

        when(importRequest.getImportId()).thenReturn("import123");
        when(importRequest.getTopicName()).thenReturn("topic1");

        requestQueue       = mock(BlockingDeque.class);
        asyncImportService = mock(AsyncImportService.class);

        when(asyncImportService.fetchImportRequestByImportId("import123")).thenReturn(importRequest);

        notificationHookConsumer = mock(NotificationHookConsumer.class);
        importTaskListener       = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);
    }

    @BeforeMethod
    public void resetMocks() throws AtlasException {
        MockitoAnnotations.openMocks(this);

        when(importRequest.getImportId()).thenReturn("import123");
        when(importRequest.getTopicName()).thenReturn("topic1");
        when(asyncImportService.fetchImportRequestByImportId(any(String.class))).thenReturn(importRequest);

        importTaskListener = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);
    }

    @AfterMethod
    public void teardown() {
        Mockito.reset(asyncImportService, notificationHookConsumer, requestQueue, importRequest);
    }

    @Test
    public void testOnReceiveImportRequestAddsRequestToQueue() throws InterruptedException, AtlasBaseException {
        importTaskListener.onReceiveImportRequest(importRequest);

        Thread.sleep(500);

        verify(requestQueue, times(1)).put("import123");
        verify(asyncImportService, times(1)).updateImportRequest(importRequest);
    }

    @Test
    @Ignore
    public void testOnReceiveImportRequestTriggersStartNextImport() throws Exception {
        doNothing().when(requestQueue).put("import123");
        when(requestQueue.poll(10, TimeUnit.SECONDS)).thenReturn("import123");

        importTaskListener.onReceiveImportRequest(importRequest);

        Thread.sleep(500);

        verify(asyncImportService, atLeastOnce()).fetchImportRequestByImportId("import123");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testOnReceiveImportRequestHandlesQueueException() throws InterruptedException, AtlasBaseException {
        doThrow(new InterruptedException()).when(requestQueue).put(any(String.class));

        try {
            importTaskListener.onReceiveImportRequest(importRequest);
        } finally {
            verify(requestQueue, times(1)).put("import123");
            verify(asyncImportService, times(1)).updateImportRequest(importRequest);
        }
    }

    @Test
    public void testOnCompleteImportRequest() {
        importTaskListener.onCompleteImportRequest("import123");

        verify(notificationHookConsumer, times(1))
                .closeImportConsumer("import123", "ATLAS_IMPORT_import123");
    }

    @Test
    public void testPopulateRequestQueueFillsQueueWithRequests() throws InterruptedException {
        List<String> imports = new ArrayList<>();

        imports.add("import1");
        imports.add("import2");
        imports.add("import3");

        when(asyncImportService.fetchQueuedImportRequests()).thenReturn(imports);

        importTaskListener.populateRequestQueue();

        verify(requestQueue, times(1)).offer("import1", 5, TimeUnit.SECONDS);
        verify(requestQueue, times(1)).offer("import2", 5, TimeUnit.SECONDS);
        verify(requestQueue, times(1)).offer("import3", 5, TimeUnit.SECONDS);
        verify(asyncImportService, times(1)).fetchQueuedImportRequests();
    }

    @Test
    public void testPopulateRequestQueueHandlesInterruptedException() throws InterruptedException {
        List<String> imports = new ArrayList<>();

        imports.add("import1");

        when(asyncImportService.fetchQueuedImportRequests()).thenReturn(imports);

        try {
            doThrow(new InterruptedException()).when(requestQueue)
                    .offer(any(String.class), eq(5L), eq(TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            // ignored
        }

        importTaskListener.populateRequestQueue();

        verify(requestQueue, times(1)).offer("import1", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testStopImport_GracefulShutdown() throws Exception {
        ExecutorService mockExecutorService = mock(ExecutorService.class);

        when(mockExecutorService.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);

        Field executorServiceField = ImportTaskListenerImpl.class.getDeclaredField("executorService");

        executorServiceField.setAccessible(true);
        executorServiceField.set(importTaskListener, mockExecutorService);

        importTaskListener.stop();

        verify(mockExecutorService, times(1)).shutdown();
        verify(mockExecutorService, times(1)).awaitTermination(30, TimeUnit.SECONDS);
        verify(mockExecutorService, never()).shutdownNow();
    }

    @Test
    public void testStopImport_ForcedShutdown() throws Exception {
        ExecutorService mockExecutorService = mock(ExecutorService.class);

        when(mockExecutorService.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(false);
        when(mockExecutorService.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(false);

        Field executorServiceField = ImportTaskListenerImpl.class.getDeclaredField("executorService");

        executorServiceField.setAccessible(true);
        executorServiceField.set(importTaskListener, mockExecutorService);

        importTaskListener.stop();

        verify(mockExecutorService, times(1)).shutdown();
        verify(mockExecutorService, times(1)).awaitTermination(30, TimeUnit.SECONDS);
        verify(mockExecutorService, times(1)).shutdownNow();
    }

    @Test
    public void testInstanceIsActive() {
        importTaskListener.instanceIsActive();

        verify(asyncImportService, atLeast(0)).fetchQueuedImportRequests();
        verify(asyncImportService, atLeast(0)).fetchInProgressImportIds();
    }

    @Test
    public void testInstanceIsPassive() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        ExecutorService mockExecutorService = mock(ExecutorService.class);

        when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        Field executorServiceField = ImportTaskListenerImpl.class.getDeclaredField("executorService");

        executorServiceField.setAccessible(true);
        executorServiceField.set(importTaskListener, mockExecutorService);

        importTaskListener.instanceIsPassive();

        verify(mockExecutorService, times(1)).shutdown();

        Field semaphoreField = ImportTaskListenerImpl.class.getDeclaredField("asyncImportSemaphore");

        semaphoreField.setAccessible(true);

        Semaphore semaphore = (Semaphore) semaphoreField.get(importTaskListener);

        assertEquals(semaphore.availablePermits(), 1);
    }

    @Test
    public void testGetHandlerOrder() {
        int order = importTaskListener.getHandlerOrder();

        assertEquals(order, 8);
    }

    @Test
    public void testStartAsyncImportIfAvailable_WithInvalidStatus() throws Exception {
        when(importRequest.getStatus()).thenReturn(FAILED);
        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123").thenReturn(null);

        importTaskListener.onReceiveImportRequest(importRequest);

        verify(notificationHookConsumer, never()).startAsyncImportConsumer(any(), anyString(), anyString());
    }

    @Test
    public void testStartImportConsumer_Successful() throws Exception {
        Mockito.doReturn("import123").when(importRequest).getImportId();
        when(importRequest.getStatus()).thenReturn(WAITING);
        when(importRequest.getTopicName()).thenReturn("topic1");

        ExecutorService realExecutor  = java.util.concurrent.Executors.newSingleThreadExecutor();
        Field           executorField = ImportTaskListenerImpl.class.getDeclaredField("executorService");

        executorField.setAccessible(true);
        executorField.set(importTaskListener, realExecutor);
        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123");

        importTaskListener.onReceiveImportRequest(importRequest);

        Thread.sleep(500);

        verify(notificationHookConsumer, atLeastOnce())
                .startAsyncImportConsumer(NotificationInterface.NotificationType.ASYNC_IMPORT, "import123", "topic1");

        realExecutor.shutdownNow();
    }

    @Test
    public void testStartImportConsumer_Failure() throws Exception {
        when(importRequest.getStatus()).thenReturn(WAITING);
        when(importRequest.getTopicName()).thenReturn("topic1");

        doThrow(new RuntimeException("Consumer failed"))
                .when(notificationHookConsumer)
                .startAsyncImportConsumer(NotificationInterface.NotificationType.ASYNC_IMPORT, "import123", "topic1");

        doAnswer(invocation -> {
            Object newStatus = invocation.getArgument(0);
            when(importRequest.getStatus()).thenReturn((AtlasAsyncImportRequest.ImportStatus) newStatus);
            return null;
        }).when(importRequest).setStatus(any());

        ExecutorService realExecutor  = java.util.concurrent.Executors.newSingleThreadExecutor();
        Field           executorField = ImportTaskListenerImpl.class.getDeclaredField("executorService");

        executorField.setAccessible(true);
        executorField.set(importTaskListener, realExecutor);

        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123");

        importTaskListener.onReceiveImportRequest(importRequest);

        Thread.sleep(500);

        verify(notificationHookConsumer, times(1))
                .closeImportConsumer("import123", "ATLAS_IMPORT_import123");

        realExecutor.shutdownNow();
    }

    @Test(dataProvider = "importQueueScenarios")
    public void testGetImportIdFromQueue(String[] pollResults, AtlasAsyncImportRequest[] fetchResults, String expectedImportId, int expectedPollCount) throws InterruptedException {
        //configure mock queue behaviour
        if (pollResults.length > 0) {
            when(requestQueue.poll(anyLong(), any())).thenReturn(pollResults[0], java.util.Arrays.copyOfRange(pollResults, 1, pollResults.length));
        }

        // Configure fetch service behavior
        for (AtlasAsyncImportRequest fetchResult : fetchResults) {
            when(asyncImportService.fetchImportRequestByImportId(fetchResult.getImportId())).thenReturn(fetchResult);
        }

        // Execute the method
        AtlasAsyncImportRequest result = importTaskListener.getNextImportFromQueue();

        // Validate results
        if (expectedImportId == null) {
            assertNull(result, "Expected result to be null.");
        } else {
            assertNotNull(result, "Expected a valid import request.");
            assertEquals(result.getImportId(), expectedImportId);
        }

        // Verify that poll was called expected times
        verify(requestQueue, atLeast(expectedPollCount)).poll(anyLong(), any());
    }

    @DataProvider(name = "importQueueScenarios")
    public Object[][] provideImportQueueScenarios() {
        AtlasAsyncImportRequest validRequest   = new AtlasAsyncImportRequest();
        AtlasAsyncImportRequest invalidRequest = new AtlasAsyncImportRequest();

        validRequest.setImportId(VALID_IMPORT_ID);
        validRequest.setStatus(WAITING);

        invalidRequest.setImportId(INVALID_IMPORT_ID);
        invalidRequest.setStatus(ABORTED);

        return new Object[][] {
                {new String[] {VALID_IMPORT_ID}, new AtlasAsyncImportRequest[] {validRequest}, VALID_IMPORT_ID, 1},
                {new String[] {null, null, null, null, null}, new AtlasAsyncImportRequest[] {}, null, 5},
                {new String[] {INVALID_IMPORT_ID, VALID_IMPORT_ID}, new AtlasAsyncImportRequest[] {invalidRequest, validRequest}, VALID_IMPORT_ID, 2},
                {new String[] {INVALID_IMPORT_ID, INVALID_IMPORT_ID, VALID_IMPORT_ID}, new AtlasAsyncImportRequest[] {invalidRequest, invalidRequest, validRequest}, VALID_IMPORT_ID, 3},
                {new String[] {null, null, VALID_IMPORT_ID}, new AtlasAsyncImportRequest[] {validRequest}, VALID_IMPORT_ID, 3}
        };
    }

    @Test
    public void testStartAsyncImportIfAvailable_SemaphoreUnavailable() throws AtlasException {
        Semaphore              mockSemaphore = mock(Semaphore.class);
        ExecutorService        mockExecutor  = mock(ExecutorService.class);
        ImportTaskListenerImpl sut           = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);

        setExecutorServiceAndSemaphore(sut, mockExecutor, mockSemaphore);

        when(mockSemaphore.tryAcquire()).thenReturn(false);

        sut.startAsyncImportIfAvailable(VALID_IMPORT_ID);

        verify(mockSemaphore, times(1)).tryAcquire(); // Ensures semaphore was checked
        verify(asyncImportService, never()).fetchImportRequestByImportId(anyString());
        verify(mockExecutor, never()).submit(any(Runnable.class));
        verify(mockSemaphore, never()).release();
    }

    @Test
    public void testStartAsyncImportIfAvailable_ValidImportIdProvided() throws AtlasException {
        Semaphore              asyncImportSemaphore = mock(Semaphore.class);
        ExecutorService        executorService      = mock(ExecutorService.class);
        ImportTaskListenerImpl sut                  = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);

        setExecutorServiceAndSemaphore(sut, executorService, asyncImportSemaphore);

        AtlasAsyncImportRequest validRequest = new AtlasAsyncImportRequest();

        validRequest.setImportId(VALID_IMPORT_ID);
        validRequest.setStatus(WAITING);

        when(asyncImportSemaphore.tryAcquire()).thenReturn(true);
        when(asyncImportService.fetchImportRequestByImportId(VALID_IMPORT_ID)).thenReturn(validRequest);

        sut.startAsyncImportIfAvailable(VALID_IMPORT_ID);

        verify(asyncImportSemaphore, times(1)).tryAcquire();
        verify(executorService, times(1)).submit(any(Runnable.class));
        verify(asyncImportSemaphore, never()).release(); // Should not release since task is submitted
    }

    @Test
    public void testStartAsyncImportIfAvailable_InvalidImportIdProvided() throws AtlasException {
        Semaphore              asyncImportSemaphore = mock(Semaphore.class);
        ExecutorService        executorService      = mock(ExecutorService.class);
        ImportTaskListenerImpl sut                  = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);

        setExecutorServiceAndSemaphore(sut, executorService, asyncImportSemaphore);

        AtlasAsyncImportRequest invalidRequest = new AtlasAsyncImportRequest();

        invalidRequest.setImportId(INVALID_IMPORT_ID);
        invalidRequest.setStatus(ABORTED);

        when(asyncImportSemaphore.tryAcquire()).thenReturn(true);
        when(asyncImportService.fetchImportRequestByImportId(INVALID_IMPORT_ID)).thenReturn(invalidRequest);

        sut.startAsyncImportIfAvailable(INVALID_IMPORT_ID);

        verify(asyncImportSemaphore, times(1)).tryAcquire();
        verify(asyncImportSemaphore, times(1)).release(); // Ensures semaphore is released on failure
        verify(executorService, never()).submit(any(Runnable.class));
    }

    @Test
    public void testStartAsyncImportIfAvailable_NullImportId_ValidRequestFromQueue() throws AtlasException, InterruptedException {
        Semaphore              asyncImportSemaphore = mock(Semaphore.class);
        ExecutorService        executorService      = mock(ExecutorService.class);
        ImportTaskListenerImpl sut                  = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);

        setExecutorServiceAndSemaphore(sut, executorService, asyncImportSemaphore);

        AtlasAsyncImportRequest validRequest = new AtlasAsyncImportRequest();

        validRequest.setImportId(VALID_IMPORT_ID);
        validRequest.setStatus(WAITING);

        when(asyncImportSemaphore.tryAcquire()).thenReturn(true);
        when(requestQueue.poll(anyLong(), any())).thenReturn(VALID_IMPORT_ID);
        when(asyncImportService.fetchImportRequestByImportId(VALID_IMPORT_ID)).thenReturn(validRequest);

        sut.startAsyncImportIfAvailable(null);

        verify(asyncImportSemaphore, times(1)).tryAcquire();
        verify(executorService, times(1)).submit(any(Runnable.class));
        verify(asyncImportSemaphore, never()).release();
    }

    @Test
    public void testStartAsyncImportIfAvailable_NullImportId_InvalidRequestFromQueue() throws AtlasException, InterruptedException {
        Semaphore              asyncImportSemaphore = mock(Semaphore.class);
        ExecutorService        executorService      = mock(ExecutorService.class);
        ImportTaskListenerImpl sut                  = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);

        setExecutorServiceAndSemaphore(sut, executorService, asyncImportSemaphore);

        AtlasAsyncImportRequest invalidRequest = new AtlasAsyncImportRequest();

        invalidRequest.setImportId(INVALID_IMPORT_ID);
        invalidRequest.setStatus(ABORTED);

        when(requestQueue.poll(anyLong(), any())).thenReturn(INVALID_IMPORT_ID).thenReturn(null);
        when(asyncImportService.fetchImportRequestByImportId(INVALID_IMPORT_ID)).thenReturn(invalidRequest);

        when(asyncImportSemaphore.tryAcquire()).thenReturn(true);

        sut.startAsyncImportIfAvailable(null);

        verify(asyncImportSemaphore, times(1)).tryAcquire();
        verify(executorService, never()).submit(any(Runnable.class));
        verify(asyncImportSemaphore, times(1)).release();
    }

    @Test
    public void testStartAsyncImportIfAvailable_ExceptionDuringExecution() throws AtlasException {
        Semaphore              asyncImportSemaphore = mock(Semaphore.class);
        ExecutorService        executorService      = mock(ExecutorService.class);
        ImportTaskListenerImpl sut                  = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer, requestQueue);

        setExecutorServiceAndSemaphore(sut, executorService, asyncImportSemaphore);

        when(asyncImportSemaphore.tryAcquire()).thenReturn(true);
        when(asyncImportService.fetchImportRequestByImportId(VALID_IMPORT_ID)).thenThrow(new RuntimeException("Unexpected Error"));

        try {
            sut.startAsyncImportIfAvailable(VALID_IMPORT_ID);
        } catch (Exception e) {
            fail("Exception should not propagate, but it did.");
        }

        verify(asyncImportSemaphore, times(1)).release();
    }

    @Test
    public void testStartInternalIsNonBlocking() throws InterruptedException {
        // Setup synchronization latches
        CountDownLatch populateDoneLatch = new CountDownLatch(1);
        CountDownLatch startNextStartedLatch = new CountDownLatch(1);
        CountDownLatch blockStartNextLatch = new CountDownLatch(1);
        CountDownLatch methodReturnedLatch = new CountDownLatch(1);

        AtomicBoolean populateCompleted = new AtomicBoolean(false);

        ImportTaskListenerImpl importTaskListenerSpy = Mockito.spy(importTaskListener);

        // Mock populateRequestQueue()
        doAnswer(invocation -> {
            populateCompleted.set(true);
            populateDoneLatch.countDown();
            return null;
        }).when(importTaskListenerSpy).populateRequestQueue();

        // Mock startNextImportInQueue()
        doAnswer(invocation -> {
            assertTrue(populateCompleted.get(), "populateRequestQueue must finish before startNextImportInQueue");
            startNextStartedLatch.countDown();
            blockStartNextLatch.await();  // block until test releases it
            return null;
        }).when(importTaskListenerSpy).startNextImportInQueue();

        // Run startInternal() in a separate thread to track non-blocking behavior
        new Thread(() -> {
            importTaskListenerSpy.startInternal();
            methodReturnedLatch.countDown();  // signal that method returned
        }, "test-startInternal-thread").start();

        // Wait for populateRequestQueue() to be called
        assertTrue(populateDoneLatch.await(1, TimeUnit.SECONDS), "populateRequestQueue didn't complete");

        // Wait for startNextImportInQueue() to start (which confirms async call happened)
        assertTrue(startNextStartedLatch.await(1, TimeUnit.SECONDS), "startNextImportInQueue didn't start");

        // Ensure startInternal() already returned
        assertTrue(methodReturnedLatch.await(1, TimeUnit.SECONDS), "startInternal() should return promptly");

        // Unblock async method so thread can exit
        blockStartNextLatch.countDown();
    }

    @Test
    public void testImportNotProcessedWhenPassive() throws Exception {
        Mockito.doReturn("import123").when(importRequest).getImportId();
        when(importRequest.getStatus()).thenReturn(WAITING);
        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123");
        importTaskListener.instanceIsPassive();
        importTaskListener.onReceiveImportRequest(importRequest);
        Thread.sleep(200);
        verify(notificationHookConsumer, never()).startAsyncImportConsumer(any(), anyString(), anyString());
    }

    @Test
    public void testExecutorNotRecreatedWhenPassive() throws Exception {
        Mockito.doReturn("import123").when(importRequest).getImportId();
        when(importRequest.getStatus()).thenReturn(WAITING);
        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123");
        when(importRequest.getStatus()).thenReturn(WAITING);
        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123");
        importTaskListener.instanceIsPassive();
        Field executorField = ImportTaskListenerImpl.class.getDeclaredField("executorService");
        executorField.setAccessible(true);
        ExecutorService exec = (ExecutorService) executorField.get(importTaskListener);
        if (exec != null) {
            exec.shutdownNow();
        }
        importTaskListener.onReceiveImportRequest(importRequest);
        Thread.sleep(200);
        ExecutorService execAfter = (ExecutorService) executorField.get(importTaskListener);
        // Should remain null when passive
        assertTrue(execAfter == null);
    }

    @Test
    public void testExecutorRecreatedWhenActive() throws Exception {
        when(importRequest.getStatus()).thenReturn(WAITING);
        when(requestQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn("import123");
        importTaskListener.instanceIsActive();
        Field executorField = ImportTaskListenerImpl.class.getDeclaredField("executorService");
        executorField.setAccessible(true);
        ExecutorService exec = (ExecutorService) executorField.get(importTaskListener);
        if (exec != null) {
            exec.shutdownNow();
        }
        importTaskListener.onReceiveImportRequest(importRequest);
        Thread.sleep(200);
        ExecutorService execAfter = (ExecutorService) executorField.get(importTaskListener);
        assertNotNull(execAfter);
        assertTrue(!execAfter.isShutdown() && !execAfter.isTerminated());
    }

    @Test
    public void ensureExecutorAliveCreatesSingleInstanceUnderConcurrency() throws Exception {
        // Ensure active mode and a clean executor state
        importTaskListener.instanceIsActive();

        Field execField = ImportTaskListenerImpl.class.getDeclaredField("executorService");
        execField.setAccessible(true);
        execField.set(importTaskListener, null);

        int threads = 64;
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService callers = java.util.concurrent.Executors.newFixedThreadPool(threads);

        List<Future<ExecutorService>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(callers.submit(() -> {
                start.await();
                return importTaskListener.ensureExecutorAlive();
            }));
        }

        ExecutorService first = null;
        for (Future<ExecutorService> f : futures) {
            ExecutorService es = f.get(10, TimeUnit.SECONDS);
            assertNotNull(es, "Executor should be created");
            if (first == null) {
                first = es;
            }
            else {
                assertSame(first, es, "All callers must see the same instance");
            }
        }

        callers.shutdownNow();
        first.shutdownNow();
    }

    @Test
    public void ensureExecutorAliveRecreatesOnceIfShutdownUnderConcurrency() throws Exception {
        // Ensure active mode
        importTaskListener.instanceIsActive();

        // First creation
        ExecutorService first = importTaskListener.ensureExecutorAlive();
        assertNotNull(first);

        // Force recreate path: mark current as shutdown and ensure the field holds that value
        first.shutdown();

        Field execField = ImportTaskListenerImpl.class.getDeclaredField("executorService");
        execField.setAccessible(true);
        execField.set(importTaskListener, first);

        int threads = 64;
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService callers = java.util.concurrent.Executors.newFixedThreadPool(threads);

        List<Future<ExecutorService>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(callers.submit(() -> {
                start.await();
                return importTaskListener.ensureExecutorAlive();
            }));
        }

        ExecutorService second = null;
        for (Future<ExecutorService> f : futures) {
            ExecutorService es = f.get(10, TimeUnit.SECONDS);
            assertNotNull(es);
            if (second == null) {
                second = es;
            }
            else {
                assertSame(second, es, "All callers must see the same new instance");
            }
        }

        assertNotSame(first, second, "Executor must be replaced after shutdown");
        callers.shutdownNow();
        second.shutdownNow();
    }

    @Test
    public void ensureExecutorAliveReturnsNullWhenPassiveEvenUnderConcurrency() throws Exception {
        // Put into passive mode (ensureExecutorAlive should early-return null)
        importTaskListener.instanceIsPassive();

        Field execField = ImportTaskListenerImpl.class.getDeclaredField("executorService");
        execField.setAccessible(true);
        execField.set(importTaskListener, null);

        int threads = 32;
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService callers = java.util.concurrent.Executors.newFixedThreadPool(threads);

        List<Future<ExecutorService>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(callers.submit(() -> {
                start.await();
                return importTaskListener.ensureExecutorAlive();
            }));
        }

        for (Future<ExecutorService> f : futures) {
            assertNull(f.get(5, TimeUnit.SECONDS), "No executor should be created in passive mode");
        }

        // Field should remain null
        assertNull(execField.get(importTaskListener));
        callers.shutdownNow();
    }

    private void setExecutorServiceAndSemaphore(ImportTaskListenerImpl importTaskListener, ExecutorService mockExecutor, Semaphore mockSemaphore) {
        try {
            Field executorField = ImportTaskListenerImpl.class.getDeclaredField("executorService");

            executorField.setAccessible(true);
            executorField.set(importTaskListener, mockExecutor);

            Field semaphoreField = ImportTaskListenerImpl.class.getDeclaredField("asyncImportSemaphore");

            semaphoreField.setAccessible(true);
            semaphoreField.set(importTaskListener, mockSemaphore);
        } catch (Exception e) {
            fail("Failed to set mocks for testing: " + e.getMessage());
        }
    }
}
