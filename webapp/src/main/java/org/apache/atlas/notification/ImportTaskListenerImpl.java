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

package org.apache.atlas.notification;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.apache.atlas.repository.impexp.AsyncImportService;
import org.apache.atlas.repository.store.graph.v2.asyncimport.ImportTaskListener;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX;

/**
 * Listens for async import requests and coordinates processing across all active nodes.
 *
 * <p><b>Active-active HA model:</b> every active node (leader or follower) participates in
 * import scheduling.  The in-memory queue and local-semaphore-only approach from the
 * single-node design has been replaced with JanusGraph-backed coordination:
 *
 * <ul>
 *   <li>Import request state (STAGING → WAITING → PROCESSING → COMPLETE) is persisted in
 *       JanusGraph/HBase and is therefore visible to all nodes.</li>
 *   <li>{@link AsyncImportService#claimNextWaitingImport()} performs an atomic
 *       check-then-set inside a single {@code @GraphTransaction}: only one node can commit
 *       the WAITING → PROCESSING transition; the other gets a JanusGraph locking conflict
 *       and backs off on retry.</li>
 *   <li>A per-node {@link Semaphore}{@code (1)} prevents the same node from submitting two
 *       claim attempts concurrently.</li>
 *   <li>A periodic background scheduler (every {@value #IMPORT_POLL_INTERVAL_SECONDS}s) on
 *       each node ensures WAITING imports are picked up even when the REST call arrived on a
 *       different, busy node.</li>
 * </ul>
 *
 * <p><b>Correctness for incremental imports:</b> because {@code claimNextWaitingImport()}
 * returns {@code null} whenever any import is globally PROCESSING, at most one import runs
 * cluster-wide at any time, preserving the ordering required by incremental export/import
 * chains (import-N may depend on import-N-1 being fully committed).
 */
@Component
@Order(8)
@DependsOn(value = "notificationHookConsumer")
public class ImportTaskListenerImpl implements Service, ActiveStateChangeHandler, ImportTaskListener {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTaskListenerImpl.class);

    private static final String THREADNAME_PREFIX            = ImportTaskListener.class.getSimpleName();
    private static final int    ASYNC_IMPORT_PERMITS         = 1;
    private static final long   IMPORT_POLL_INTERVAL_SECONDS = 5L;

    private volatile ExecutorService          executorService;
    private volatile ScheduledExecutorService scheduler;
    private final    AsyncImportService       asyncImportService;
    private final    NotificationHookConsumer notificationHookConsumer;
    private final    Semaphore                asyncImportSemaphore;
    private final    Configuration            applicationProperties;
    private final    AtomicBoolean            started = new AtomicBoolean(false);

    @Inject
    public ImportTaskListenerImpl(AsyncImportService asyncImportService,
                                  NotificationHookConsumer notificationHookConsumer) throws AtlasException {
        this.asyncImportService       = asyncImportService;
        this.notificationHookConsumer = notificationHookConsumer;
        this.asyncImportSemaphore     = new Semaphore(ASYNC_IMPORT_PERMITS);
        this.applicationProperties    = ApplicationProperties.get();
    }

    // -------------------------------------------------------------------------
    // Service lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws AtlasException {
        // activation is handled exclusively by instanceIsActive()
    }

    @Override
    public void stop() throws AtlasException {
        try {
            stopScheduler();
            stopImport();
        } finally {
            releaseAsyncImportSemaphore();
        }
    }

    @PreDestroy
    public void stopImport() {
        LOG.info("ImportTaskListenerImpl: shutting down import executor...");

        if (executorService == null) {
            return;
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("ImportTaskListenerImpl: executor did not stop in 30s, waiting 10s more...");
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.warn("ImportTaskListenerImpl: forcing executor shutdown");
                    executorService.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }

        executorService = null;

        LOG.info("ImportTaskListenerImpl: import executor stopped");
    }

    // -------------------------------------------------------------------------
    // ActiveStateChangeHandler
    // -------------------------------------------------------------------------

    @Override
    public void instanceIsActive() {
        // Import scheduling only runs on nodes that serve the REST API and process imports:
        // MONOLITHIC and METADATA_SERVER.
        //
        // NOTIFICATION_PROCESSOR — hook consumer only, no import API, no scheduler needed.
        // INITIALIZER            — one-shot init that exits immediately; starting a polling
        //                          scheduler here causes it to fire against a closing graph
        //                          during JVM shutdown, flooding logs with errors.
        if (!AtlasRunMode.current().runsMetadataServer()) {
            LOG.info("ImportTaskListenerImpl.instanceIsActive(): RUN_MODE={} — skipping import scheduler",
                    AtlasRunMode.current());
            return;
        }

        LOG.info("ImportTaskListenerImpl.instanceIsActive(): starting import scheduler (RUN_MODE={})",
                AtlasRunMode.current());
        startInternal();
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.IMPORT_TASK_LISTENER.getOrder();
    }

    // -------------------------------------------------------------------------
    // ImportTaskListener
    // -------------------------------------------------------------------------

    /**
     * Called by {@link org.apache.atlas.repository.store.graph.v2.AsyncImportTaskExecutor}
     * after publishing all entities to the per-import Kafka topic.
     * Sets the request to WAITING in JanusGraph, then immediately attempts to claim it.
     */
    @Override
    public void onReceiveImportRequest(AtlasAsyncImportRequest importRequest) throws AtlasBaseException {
        LOG.info("==> onReceiveImportRequest(importId={})", importRequest.getImportId());

        importRequest.setStatus(ImportStatus.WAITING);
        asyncImportService.updateImportRequest(importRequest);

        // Trigger an immediate claim attempt asynchronously so the REST call returns quickly.
        CompletableFuture.runAsync(this::tryClaimAndStartImport)
                .exceptionally(ex -> {
                    LOG.error("onReceiveImportRequest: error triggering claim for import {}", importRequest.getImportId(), ex);
                    return null;
                });

        LOG.info("<== onReceiveImportRequest(importId={})", importRequest.getImportId());
    }

    /**
     * Called when the Kafka consumer finishes processing an import (success or failure).
     * Releases the per-node semaphore and immediately tries to claim the next WAITING import.
     */
    @Override
    public void onCompleteImportRequest(String importId) {
        LOG.info("==> onCompleteImportRequest(importId={})", importId);

        try {
            notificationHookConsumer.closeImportConsumer(importId, ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId);
        } finally {
            releaseAsyncImportSemaphore();

            CompletableFuture.runAsync(this::tryClaimAndStartImport)
                    .exceptionally(ex -> {
                        LOG.error("onCompleteImportRequest: error triggering next claim after import {}", importId, ex);
                        return null;
                    });

            LOG.info("<== onCompleteImportRequest(importId={})", importId);
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void startInternal() {
        if (!started.compareAndSet(false, true)) {
            LOG.info("ImportTaskListenerImpl.startInternal(): already started, skipping");
            return;
        }

        startScheduler();

        // Immediately attempt to pick up any WAITING imports left from before this node started.
        CompletableFuture.runAsync(this::tryClaimAndStartImport)
                .exceptionally(ex -> {
                    LOG.error("startInternal: error during initial claim attempt", ex);
                    return null;
                });
    }

    /**
     * Attempts to claim and start the next available import on this node.
     *
     * <p>Uses the per-node {@link #asyncImportSemaphore} as a first gate (avoids hitting
     * JanusGraph when this node already has an import running), then delegates stale-claim
     * recovery + global exclusive claim via {@link AsyncImportService#recoverStaleClaims()}
     * and {@link AsyncImportService#tryClaim()} — the
     * {@link org.apache.atlas.tasks.GraphClaimable} contract that atomically transitions
     * the next WAITING import to PROCESSING inside a single {@code @GraphTransaction}.
     */
    @VisibleForTesting
    void tryClaimAndStartImport() {
        // Final guard: abort if called on a non-metadata-server node (e.g. INITIALIZER
        // during JVM shutdown when the scheduler fires against a closing graph).
        if (!AtlasRunMode.current().runsMetadataServer()) {
            return;
        }

        if (!asyncImportSemaphore.tryAcquire()) {
            LOG.info("tryClaimAndStartImport(): an import is already running on this node, skipping");
            return;
        }

        AtlasAsyncImportRequest claimed = null;
        try {
            asyncImportService.recoverStaleClaims();
            claimed = asyncImportService.tryClaim();
        } catch (IllegalStateException e) {
            // Graph is closed — this happens during JVM shutdown (INITIALIZER mode exits
            // via System.exit(0) while the poller is still scheduled).  Silently stop.
            asyncImportSemaphore.release();
            stopScheduler();
            return;
        } catch (Exception e) {
            LOG.error("tryClaimAndStartImport(): failed to claim next import from JanusGraph", e);
        }

        if (claimed == null) {
            asyncImportSemaphore.release();
            return;
        }

        ExecutorService exec = ensureExecutorAlive();
        if (exec != null) {
            final AtlasAsyncImportRequest toProcess = claimed;
            exec.submit(() -> startImportConsumer(toProcess));
        } else {
            LOG.warn("tryClaimAndStartImport(): no executor available, releasing semaphore");
            asyncImportSemaphore.release();
        }
    }

    private void startScheduler() {
        // Hard guard: never create the scheduler on nodes that don't serve imports.
        // This catches any call path that bypasses the instanceIsActive() check.
        if (!AtlasRunMode.current().runsMetadataServer()) {
            LOG.info("startScheduler(): RUN_MODE={} — not starting import poller",
                    AtlasRunMode.current());
            return;
        }

        if (scheduler != null && !scheduler.isShutdown()) {
            LOG.debug("startScheduler(): already running");
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(THREADNAME_PREFIX + "-poller-%d")
                        .setDaemon(true)   // daemon so JVM shutdown isn't blocked
                        .setUncaughtExceptionHandler((t, ex) ->
                                LOG.error("Uncaught exception in import poller thread {}", t.getName(), ex))
                        .build());
        scheduler.scheduleWithFixedDelay(
                this::tryClaimAndStartImport,
                IMPORT_POLL_INTERVAL_SECONDS,
                IMPORT_POLL_INTERVAL_SECONDS,
                TimeUnit.SECONDS);

        LOG.info("startScheduler(): import polling scheduler started (interval={}s)", IMPORT_POLL_INTERVAL_SECONDS);
    }

    private void stopScheduler() {
        if (scheduler == null || scheduler.isShutdown()) {
            return;
        }

        scheduler.shutdownNow();
        scheduler = null;

        LOG.info("stopScheduler(): import polling scheduler stopped");
    }

    @VisibleForTesting
    ExecutorService ensureExecutorAlive() {
        if (executorService == null || executorService.isShutdown() || executorService.isTerminated()) {
            synchronized (this) {
                if (executorService == null || executorService.isShutdown() || executorService.isTerminated()) {
                    executorService = Executors.newSingleThreadExecutor(
                            new ThreadFactoryBuilder()
                                    .setNameFormat(THREADNAME_PREFIX + "-worker-%d")
                                    .setUncaughtExceptionHandler((t, ex) ->
                                            LOG.error("Uncaught exception in import worker thread {}", t.getName(), ex))
                                    .build());
                    LOG.info("ensureExecutorAlive(): import worker executor (re)created");
                }
            }
        }
        return executorService;
    }

    private void startImportConsumer(AtlasAsyncImportRequest importRequest) {
        LOG.info("==> startImportConsumer(importId={})", importRequest.getImportId());

        try {
            // Status already set to PROCESSING by claimNextWaitingImport().
            notificationHookConsumer.startAsyncImportConsumer(
                    NotificationInterface.NotificationType.ASYNC_IMPORT,
                    importRequest.getImportId(),
                    importRequest.getTopicName());
        } catch (Exception e) {
            importRequest.setStatus(ImportStatus.FAILED);

            LOG.error("startImportConsumer(): failed to start consumer for import {}, marking FAILED",
                    importRequest.getImportId(), e);
        } finally {
            // Persist-failure must not leave the per-node semaphore held or block the queue.
            if (ImportStatus.FAILED.equals(importRequest.getStatus())) {
                try {
                    asyncImportService.updateImportRequest(importRequest);
                } catch (Throwable t) {
                    LOG.error("startImportConsumer(): failed to persist FAILED state for importId={}",
                            importRequest.getImportId(), t);
                } finally {
                    onCompleteImportRequest(importRequest.getImportId());
                }
            }

            LOG.info("<== startImportConsumer(importId={})", importRequest.getImportId());
        }
    }

    private void releaseAsyncImportSemaphore() {
        if (asyncImportSemaphore.availablePermits() == 0) {
            asyncImportSemaphore.release();
            LOG.debug("releaseAsyncImportSemaphore(): released");
        } else {
            LOG.debug("releaseAsyncImportSemaphore(): no permit held, nothing to release");
        }
    }

    @VisibleForTesting
    Semaphore getSemaphore() {
        return asyncImportSemaphore;
    }

    @VisibleForTesting
    void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
}
