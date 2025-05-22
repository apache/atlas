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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.apache.atlas.repository.impexp.AsyncImportService;
import org.apache.atlas.repository.store.graph.v2.asyncimport.ImportTaskListener;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX;
import static org.apache.atlas.AtlasErrorCode.IMPORT_QUEUEING_FAILED;

@Component
@Order(8)
@DependsOn(value = "notificationHookConsumer")
public class ImportTaskListenerImpl implements Service, ActiveStateChangeHandler, ImportTaskListener {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTaskListenerImpl.class);

    private static final String THREADNAME_PREFIX    = ImportTaskListener.class.getSimpleName();
    private static final int    ASYNC_IMPORT_PERMITS = 1; // Only one asynchronous import task is permitted

    private final BlockingQueue<String>    requestQueue;    // Blocking queue for requests
    private final ExecutorService          executorService; // Single-thread executor for sequential processing
    private final AsyncImportService       asyncImportService;
    private final NotificationHookConsumer notificationHookConsumer;
    private final Semaphore                asyncImportSemaphore;
    private final Configuration            applicationProperties;

    @Inject
    public ImportTaskListenerImpl(AsyncImportService asyncImportService, NotificationHookConsumer notificationHookConsumer) throws AtlasException {
        this(asyncImportService, notificationHookConsumer, new LinkedBlockingQueue<>());
    }

    public ImportTaskListenerImpl(AsyncImportService asyncImportService, NotificationHookConsumer notificationHookConsumer, BlockingQueue<String> requestQueue) throws AtlasException {
        this.asyncImportService       = asyncImportService;
        this.notificationHookConsumer = notificationHookConsumer;
        this.requestQueue             = requestQueue;
        this.asyncImportSemaphore     = new Semaphore(ASYNC_IMPORT_PERMITS);
        this.applicationProperties    = ApplicationProperties.get();
        this.executorService          = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(THREADNAME_PREFIX + " thread-%d")
                .setUncaughtExceptionHandler((thread, throwable) -> LOG.error("Uncaught exception in thread {}: {}", thread.getName(), throwable.getMessage(), throwable)).build());
    }

    @Override
    public void start() throws AtlasException {
        if (HAConfiguration.isHAEnabled(applicationProperties)) {
            LOG.info("HA is enabled, not starting import consumers inline.");

            return;
        }

        startInternal();
    }

    private void startInternal() {
        CompletableFuture<Void> populateTask = CompletableFuture.runAsync(this::populateRequestQueue)
                .exceptionally(ex -> {
                    LOG.error("Failed to populate request queue", ex);
                    return null;
                });

        CompletableFuture<Void> resumeTask = CompletableFuture.runAsync(this::resumeInProgressImports)
                .exceptionally(ex -> {
                    LOG.error("Failed to resume in-progress imports", ex);
                    return null;
                });

        // Wait for both tasks to complete before proceeding
        CompletableFuture.allOf(populateTask, resumeTask)
                .thenRun(this::startNextImportInQueue)
                .exceptionally(ex -> {
                    LOG.error("Failed to start next import in queue", ex);
                    return null;
                }).join();
    }

    @Override
    public void onReceiveImportRequest(AtlasAsyncImportRequest importRequest) throws AtlasBaseException {
        try {
            LOG.info("==> onReceiveImportRequest(atlasAsyncImportRequest={})", importRequest);

            importRequest.setStatus(ImportStatus.WAITING);

            asyncImportService.updateImportRequest(importRequest);
            requestQueue.put(importRequest.getImportId());

            startNextImportInQueue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            LOG.warn("Failed to add import request: {} to the queue", importRequest.getImportId());

            throw new AtlasBaseException(IMPORT_QUEUEING_FAILED, e, importRequest.getImportId());
        } finally {
            LOG.info("<== onReceiveImportRequest(atlasAsyncImportRequest={})", importRequest);
        }
    }

    @Override
    public void onCompleteImportRequest(String importId) {
        LOG.info("==> onCompleteImportRequest(importId={})", importId);

        try {
            notificationHookConsumer.closeImportConsumer(importId, ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId);
        } finally {
            releaseAsyncImportSemaphore();
            startNextImportInQueue();

            LOG.info("<== onCompleteImportRequest(importId={})", importId);
        }
    }

    private void startNextImportInQueue() {
        LOG.info("==> startNextImportInQueue()");

        startAsyncImportIfAvailable(null);

        LOG.info("<== startNextImportInQueue()");
    }

    @VisibleForTesting
    void startAsyncImportIfAvailable(String importId) {
        LOG.info("==> startAsyncImportIfAvailable()");

        try {
            if (!asyncImportSemaphore.tryAcquire()) {
                LOG.info("An async import is in progress, import request is queued");

                return;
            }

            AtlasAsyncImportRequest nextImport = (importId != null) ? asyncImportService.fetchImportRequestByImportId(importId) : getNextImportFromQueue();

            if (isNotValidImportRequest(nextImport)) {
                releaseAsyncImportSemaphore();
                return;
            }

            executorService.submit(() -> startImportConsumer(nextImport));
        } catch (Exception e) {
            LOG.error("Error while starting the next import, releasing the lock if held", e);

            releaseAsyncImportSemaphore();
        } finally {
            LOG.info("<== startAsyncImportIfAvailable()");
        }
    }

    private void startImportConsumer(AtlasAsyncImportRequest importRequest) {
        try {
            LOG.info("==> startImportConsumer(atlasAsyncImportRequest={})", importRequest);

            notificationHookConsumer.startAsyncImportConsumer(NotificationInterface.NotificationType.ASYNC_IMPORT, importRequest.getImportId(), importRequest.getTopicName());

            importRequest.setStatus(ImportStatus.PROCESSING);
            importRequest.setProcessingStartTime(System.currentTimeMillis());
        } catch (Exception e) {
            LOG.error("Failed to start consumer for import: {}, marking import as failed", importRequest, e);

            importRequest.setStatus(ImportStatus.FAILED);
        } finally {
            asyncImportService.updateImportRequest(importRequest);

            if (ObjectUtils.equals(importRequest.getStatus(), ImportStatus.FAILED)) {
                onCompleteImportRequest(importRequest.getImportId());
            }

            LOG.info("<== startImportConsumer(atlasAsyncImportRequest={})", importRequest);
        }
    }

    @VisibleForTesting
    AtlasAsyncImportRequest getNextImportFromQueue() {
        LOG.info("==> getNextImportFromQueue()");

        final int               maxRetries = 5;
        int                     retryCount = 0;
        AtlasAsyncImportRequest nextImport = null;

        while (retryCount < maxRetries) {
            try {
                String importId = requestQueue.poll(10, TimeUnit.SECONDS);

                if (importId == null) {
                    retryCount++;

                    LOG.warn("Still waiting for import request... (attempt {} of {})", retryCount, maxRetries);

                    continue;
                }

                // Reset retry count because we got a valid importId (even if it's invalid later)
                retryCount = 0;

                AtlasAsyncImportRequest importRequest = asyncImportService.fetchImportRequestByImportId(importId);

                if (isNotValidImportRequest(importRequest)) {
                    LOG.info("Import request {}, is not in a valid status to start import, hence skipping..", importRequest);

                    continue;
                }

                LOG.info("<== getImportIdFromQueue(nextImportId={})", importRequest.getImportId());

                return importRequest;
            } catch (InterruptedException e) {
                LOG.error("Thread interrupted while waiting for importId from the queue", e);
                // Restore the interrupt flag
                Thread.currentThread().interrupt();
                return null;
            }
        }

        LOG.error("Exceeded max retry attempts. Exiting...");

        return null;
    }

    @VisibleForTesting
    boolean isNotValidImportRequest(AtlasAsyncImportRequest importRequest) {
        return importRequest == null ||
                (!ImportStatus.WAITING.equals(importRequest.getStatus()) && !ImportStatus.PROCESSING.equals(importRequest.getStatus()));
    }

    private void releaseAsyncImportSemaphore() {
        LOG.info("==> releaseAsyncImportSemaphore()");

        if (asyncImportSemaphore.availablePermits() == 0) {
            asyncImportSemaphore.release();

            LOG.info("<== releaseAsyncImportSemaphore()");
        } else {
            LOG.info("<== releaseAsyncImportSemaphore(); no lock held");
        }
    }

    void populateRequestQueue() {
        LOG.info("==> populateRequestQueue()");

        List<String> importRequests = asyncImportService.fetchQueuedImportRequests();

        try {
            if (!importRequests.isEmpty()) {
                for (String request : importRequests) {
                    try {
                        if (!requestQueue.offer(request, 5, TimeUnit.SECONDS)) { // Wait up to 5 sec
                            LOG.warn("populateRequestQueue(): Request {} could not be added to the queue", request);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        LOG.error("populateRequestQueue(): Failed to add requests to queue");

                        break; // Exit loop on interruption
                    }
                }

                LOG.info("populateRequestQueue(): Added {} requests to queue", importRequests.size());
            } else {
                LOG.warn("populateRequestQueue(): No queued requests found.");
            }
        } finally {
            LOG.info("<== populateRequestQueue()");
        }
    }

    private void resumeInProgressImports() {
        LOG.info("==> resumeInProgressImports()");

        try {
            String importId = asyncImportService.fetchInProgressImportIds().stream().findFirst().orElse(null);

            if (importId == null) {
                LOG.warn("No imports found to resume");

                return;
            }

            LOG.info("Resuming import id={}", importId);

            startAsyncImportIfAvailable(importId);
        } finally {
            LOG.info("<== resumeInProgressImports()");
        }
    }

    @PreDestroy
    public void stopImport() {
        LOG.info("Shutting down import processor...");

        executorService.shutdown(); // Initiate an orderly shutdown

        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Executor service did not terminate gracefully within the timeout. Waiting longer...");

                // Retry shutdown before forcing it
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.warn("Forcing shutdown...");

                    executorService.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            LOG.error("Shutdown interrupted. Forcing shutdown...");

            executorService.shutdownNow();
        }

        LOG.info("Import processor stopped.");
    }

    @Override
    public void stop() throws AtlasException {
        try {
            stopImport();
        } finally {
            releaseAsyncImportSemaphore();
        }
    }

    @Override
    public void instanceIsActive() {
        LOG.info("Reacting to active state: initializing Kafka consumers");

        startInternal();
    }

    @Override
    public void instanceIsPassive() {
        try {
            stopImport();
        } finally {
            releaseAsyncImportSemaphore();
        }
    }

    @Override
    public int getHandlerOrder() {
        return ActiveStateChangeHandler.HandlerOrder.IMPORT_TASK_LISTENER.getOrder();
    }
}
