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
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.apache.atlas.repository.impexp.AsyncImportService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class ImportTaskListenerImplTest {
    private static final String IMPORT_ID = "import123";
    private static final String TOPIC     = "ATLAS_IMPORT_import123";

    @Mock private AsyncImportService       asyncImportService;
    @Mock private NotificationHookConsumer notificationHookConsumer;
    @Mock private AtlasAsyncImportRequest  importRequest;

    private ImportTaskListenerImpl importTaskListener;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        when(importRequest.getImportId()).thenReturn(IMPORT_ID);
        when(importRequest.getTopicName()).thenReturn(TOPIC);

        // Default: no import available to claim — keeps the background scheduler quiet
        when(asyncImportService.tryClaim()).thenReturn(null);

        importTaskListener = new ImportTaskListenerImpl(asyncImportService, notificationHookConsumer);
    }

    @AfterMethod
    public void tearDown() throws AtlasException {
        importTaskListener.stop();
    }

    // -------------------------------------------------------------------------
    // onReceiveImportRequest
    // -------------------------------------------------------------------------

    @Test
    public void testOnReceiveImportRequest_SetsWaitingAndTriggersClaim() throws Exception {
        importTaskListener.onReceiveImportRequest(importRequest);

        Thread.sleep(300);

        verify(importRequest, times(1)).setStatus(ImportStatus.WAITING);
        verify(asyncImportService, times(1)).updateImportRequest(importRequest);
        // async claim attempt fires — recovery + claim called at least once (GraphClaimable contract)
        verify(asyncImportService, atLeastOnce()).recoverStaleClaims();
        verify(asyncImportService, atLeastOnce()).tryClaim();
    }

    @Test
    public void testOnReceiveImportRequest_DoesNotThrowWhenClaimFails() throws Exception {
        when(asyncImportService.tryClaim()).thenThrow(new AtlasBaseException("JanusGraph error"));

        importTaskListener.onReceiveImportRequest(importRequest);
        Thread.sleep(300);

        // updateImportRequest must still complete even if claim throws
        verify(asyncImportService, times(1)).updateImportRequest(importRequest);
    }

    // -------------------------------------------------------------------------
    // onCompleteImportRequest
    // -------------------------------------------------------------------------

    @Test
    public void testOnCompleteImportRequest_ClosesConsumerAndReleasesSemaphore() throws Exception {
        // acquire semaphore to simulate a running import
        Semaphore sem = getSemaphore();
        sem.acquire();

        importTaskListener.onCompleteImportRequest(IMPORT_ID);
        Thread.sleep(300);

        verify(notificationHookConsumer, times(1)).closeImportConsumer(IMPORT_ID, TOPIC);
        assertEquals(sem.availablePermits(), 1, "Semaphore must be released after completion");
    }

    @Test
    public void testOnCompleteImportRequest_TriggersNextClaim() throws Exception {
        importTaskListener.onCompleteImportRequest(IMPORT_ID);
        Thread.sleep(300);

        verify(asyncImportService, atLeastOnce()).recoverStaleClaims();
        verify(asyncImportService, atLeastOnce()).tryClaim();
    }

    // -------------------------------------------------------------------------
    // tryClaimAndStartImport
    // -------------------------------------------------------------------------

    @Test
    public void testTryClaimAndStartImport_SemaphoreUnavailable_SkipsClaim() throws Exception {
        getSemaphore().acquire(); // simulate node already running an import

        importTaskListener.tryClaimAndStartImport();

        verify(asyncImportService, never()).recoverStaleClaims();
        verify(asyncImportService, never()).tryClaim();
        assertEquals(getSemaphore().availablePermits(), 0, "Semaphore must stay acquired");
    }

    @Test
    public void testTryClaimAndStartImport_NoImportAvailable_ReleasesSemaphore() throws Exception {
        when(asyncImportService.tryClaim()).thenReturn(null);

        importTaskListener.tryClaimAndStartImport();

        verify(asyncImportService, times(1)).recoverStaleClaims();
        verify(asyncImportService, times(1)).tryClaim();
        assertEquals(getSemaphore().availablePermits(), 1, "Semaphore must be released when nothing to claim");
    }

    @Test
    public void testTryClaimAndStartImport_ClaimSucceeds_SubmitsToExecutor() throws Exception {
        AtlasAsyncImportRequest claimed = new AtlasAsyncImportRequest();
        claimed.setImportId(IMPORT_ID);
        claimed.setStatus(ImportStatus.PROCESSING);
        // getTopicName() is computed as ASYNC_IMPORT_TOPIC_PREFIX + importId — no setter needed
        when(asyncImportService.tryClaim()).thenReturn(claimed);

        ExecutorService mockExecutor = mock(ExecutorService.class);
        setExecutorService(mockExecutor);

        importTaskListener.tryClaimAndStartImport();

        verify(asyncImportService, times(1)).recoverStaleClaims();
        verify(asyncImportService, times(1)).tryClaim();
        verify(mockExecutor, times(1)).submit(any(Runnable.class));
        assertEquals(getSemaphore().availablePermits(), 0, "Semaphore must be held while import runs");
    }

    @Test
    public void testTryClaimAndStartImport_ClaimThrows_ReleasesSemaphore() throws Exception {
        when(asyncImportService.tryClaim()).thenThrow(new AtlasBaseException("DB error"));

        importTaskListener.tryClaimAndStartImport(); // must not propagate

        assertEquals(getSemaphore().availablePermits(), 1, "Semaphore must be released on exception");
    }

    // -------------------------------------------------------------------------
    // startImportConsumer (tested indirectly via tryClaimAndStartImport)
    // -------------------------------------------------------------------------

    @Test
    public void testStartImportConsumer_StartsKafkaConsumer() throws Exception {
        AtlasAsyncImportRequest claimed = new AtlasAsyncImportRequest();
        claimed.setImportId(IMPORT_ID);
        claimed.setStatus(ImportStatus.PROCESSING);
        when(asyncImportService.tryClaim()).thenReturn(claimed);

        // Use real single-thread executor so the submitted task actually runs
        ExecutorService realExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        setExecutorService(realExecutor);

        importTaskListener.tryClaimAndStartImport();

        Thread.sleep(500);

        verify(notificationHookConsumer, times(1))
                .startAsyncImportConsumer(NotificationInterface.NotificationType.ASYNC_IMPORT, IMPORT_ID, TOPIC);
    }

    @Test
    public void testStartImportConsumer_ConsumerThrows_MarksFailedAndCompletesImport() throws Exception {
        AtlasAsyncImportRequest claimed = new AtlasAsyncImportRequest();
        claimed.setImportId(IMPORT_ID);
        claimed.setStatus(ImportStatus.PROCESSING);
        // Only the first claim succeeds; post-complete claim attempts find nothing.
        when(asyncImportService.tryClaim()).thenReturn(claimed).thenReturn(null);

        doThrow(new RuntimeException("Kafka error"))
                .when(notificationHookConsumer)
                .startAsyncImportConsumer(any(), anyString(), anyString());

        ExecutorService realExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        setExecutorService(realExecutor);

        importTaskListener.tryClaimAndStartImport();

        Thread.sleep(500);

        assertEquals(claimed.getStatus(), ImportStatus.FAILED);
        verify(asyncImportService, atLeastOnce()).updateImportRequest(claimed);
        // onCompleteImportRequest fires → closeImportConsumer is called
        verify(notificationHookConsumer, atLeastOnce()).closeImportConsumer(IMPORT_ID, TOPIC);
        assertEquals(getSemaphore().availablePermits(), 1, "Semaphore must be released after consumer start failure");
    }

    @Test
    public void testStartImportConsumer_PersistFailedStillCompletesImport() throws Exception {
        AtlasAsyncImportRequest claimed = new AtlasAsyncImportRequest();
        claimed.setImportId(IMPORT_ID);
        claimed.setStatus(ImportStatus.PROCESSING);
        when(asyncImportService.tryClaim()).thenReturn(claimed).thenReturn(null);

        doThrow(new RuntimeException("Kafka error"))
                .when(notificationHookConsumer)
                .startAsyncImportConsumer(any(), anyString(), anyString());
        doThrow(new RuntimeException("graph unavailable"))
                .when(asyncImportService)
                .updateImportRequest(any(AtlasAsyncImportRequest.class));

        ExecutorService realExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        setExecutorService(realExecutor);

        importTaskListener.tryClaimAndStartImport();

        Thread.sleep(500);

        assertEquals(claimed.getStatus(), ImportStatus.FAILED);
        verify(notificationHookConsumer, atLeastOnce()).closeImportConsumer(IMPORT_ID, TOPIC);
        assertEquals(getSemaphore().availablePermits(), 1,
                "Semaphore must be released even when persisting FAILED status throws");
    }

    @Test
    public void testStartImportConsumer_FailedPersistDoesNotDeadlockQueue_NextImportCanBeClaimed() throws Exception {
        AtlasAsyncImportRequest failedImport = new AtlasAsyncImportRequest();
        failedImport.setImportId(IMPORT_ID);
        failedImport.setStatus(ImportStatus.PROCESSING);

        AtlasAsyncImportRequest nextImport = new AtlasAsyncImportRequest();
        nextImport.setImportId("import456");
        nextImport.setStatus(ImportStatus.PROCESSING);

        // First claim fails while starting consumer; second claim should still be possible.
        when(asyncImportService.tryClaim()).thenReturn(failedImport).thenReturn(null).thenReturn(nextImport);

        doThrow(new RuntimeException("Kafka start failure"))
                .when(notificationHookConsumer)
                .startAsyncImportConsumer(any(), anyString(), anyString());
        doThrow(new RuntimeException("graph commit failure"))
                .when(asyncImportService)
                .updateImportRequest(any(AtlasAsyncImportRequest.class));

        ExecutorService realExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        setExecutorService(realExecutor);

        importTaskListener.tryClaimAndStartImport();
        Thread.sleep(500);

        // First import failure path must release permit and invoke completion.
        assertEquals(getSemaphore().availablePermits(), 1,
                "Semaphore must be released even when FAILED-state persistence throws");
        verify(notificationHookConsumer, atLeastOnce()).closeImportConsumer(IMPORT_ID, TOPIC);

        // Allow second import-start attempt to succeed, proving queue is not deadlocked.
        org.mockito.Mockito.reset(notificationHookConsumer);
        org.mockito.Mockito.doNothing()
                .when(notificationHookConsumer)
                .startAsyncImportConsumer(any(), anyString(), anyString());

        importTaskListener.tryClaimAndStartImport();
        Thread.sleep(500);

        verify(notificationHookConsumer, times(1))
                .startAsyncImportConsumer(NotificationInterface.NotificationType.ASYNC_IMPORT,
                        "import456", "ATLAS_IMPORT_import456");
    }

    // -------------------------------------------------------------------------
    // instanceIsActive
    // -------------------------------------------------------------------------

    @Test
    public void testInstanceIsActive_StartsScheduler() throws Exception {
        importTaskListener.instanceIsActive();

        ScheduledExecutorService scheduler = getScheduler();

        assertNotNull(scheduler, "Scheduler must be running after instanceIsActive");
        assertTrue(!scheduler.isShutdown(), "Scheduler must not be shut down");
    }

    @Test
    public void testInstanceIsActive_IsIdempotent() throws Exception {
        importTaskListener.instanceIsActive();
        ScheduledExecutorService first = getScheduler();

        importTaskListener.instanceIsActive(); // second call — must be no-op
        ScheduledExecutorService second = getScheduler();

        assertSame(first, second, "Scheduler must not be recreated on duplicate instanceIsActive");
    }

    // -------------------------------------------------------------------------
    // stop (Service lifecycle)
    // -------------------------------------------------------------------------

    @Test
    public void testStop_GracefulExecutorShutdown() throws Exception {
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);
        setExecutorService(mockExecutor);

        importTaskListener.stop();

        verify(mockExecutor, times(1)).shutdown();
        verify(mockExecutor, times(1)).awaitTermination(30, TimeUnit.SECONDS);
        verify(mockExecutor, never()).shutdownNow();
    }

    @Test
    public void testStop_ForcedShutdownWhenGracefulTimesOut() throws Exception {
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(false);
        when(mockExecutor.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(false);
        setExecutorService(mockExecutor);

        importTaskListener.stop();

        verify(mockExecutor, times(1)).shutdown();
        verify(mockExecutor, times(1)).shutdownNow();
    }

    // -------------------------------------------------------------------------
    // getHandlerOrder
    // -------------------------------------------------------------------------

    @Test
    public void testGetHandlerOrder() {
        assertEquals(importTaskListener.getHandlerOrder(), 8);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Semaphore getSemaphore() {
        return importTaskListener.getSemaphore();
    }

    private ScheduledExecutorService getScheduler() throws Exception {
        Field f = ImportTaskListenerImpl.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        return (ScheduledExecutorService) f.get(importTaskListener);
    }

    private void setExecutorService(ExecutorService executor) {
        importTaskListener.setExecutorService(executor);
    }
}
