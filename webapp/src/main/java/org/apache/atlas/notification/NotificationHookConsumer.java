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
import kafka.utils.ShutdownableThread;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityPartialUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.filters.AuditFilter;
import org.apache.atlas.web.filters.AuditFilter.AuditLog;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer of notifications from hooks e.g., hive hook etc.
 */
@Component
@Order(4)
@DependsOn(value = {"atlasTypeDefStoreInitializer", "atlasTypeDefGraphStoreV2"})
public class NotificationHookConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG        = LoggerFactory.getLogger(NotificationHookConsumer.class);
    private static final Logger PERF_LOG   = AtlasPerfTracer.getPerfLogger(NotificationHookConsumer.class);
    private static final Logger FAILED_LOG = LoggerFactory.getLogger("FAILED");

    private static final int    SC_OK          = 200;
    private static final int    SC_BAD_REQUEST = 400;
    private static final String THREADNAME_PREFIX = NotificationHookConsumer.class.getSimpleName();

    public static final String CONSUMER_THREADS_PROPERTY         = "atlas.notification.hook.numthreads";
    public static final String CONSUMER_RETRIES_PROPERTY         = "atlas.notification.hook.maxretries";
    public static final String CONSUMER_FAILEDCACHESIZE_PROPERTY = "atlas.notification.hook.failedcachesize";
    public static final String CONSUMER_RETRY_INTERVAL           = "atlas.notification.consumer.retry.interval";
    public static final String CONSUMER_MIN_RETRY_INTERVAL       = "atlas.notification.consumer.min.retry.interval";
    public static final String CONSUMER_MAX_RETRY_INTERVAL       = "atlas.notification.consumer.max.retry.interval";

    public static final int SERVER_READY_WAIT_TIME_MS = 1000;

    private final AtlasEntityStore       atlasEntityStore;
    private final ServiceState           serviceState;
    private final AtlasInstanceConverter instanceConverter;
    private final AtlasTypeRegistry      typeRegistry;
    private final int                    maxRetries;
    private final int                    failedMsgCacheSize;
    private final int                    minWaitDuration;
    private final int                    maxWaitDuration;

    private NotificationInterface notificationInterface;
    private ExecutorService       executors;
    private Configuration         applicationProperties;

    @VisibleForTesting
    final int consumerRetryInterval;

    @VisibleForTesting
    List<HookConsumer> consumers;

    @Inject
    public NotificationHookConsumer(NotificationInterface notificationInterface, AtlasEntityStore atlasEntityStore,
                                    ServiceState serviceState, AtlasInstanceConverter instanceConverter,
                                    AtlasTypeRegistry typeRegistry) throws AtlasException {
        this.notificationInterface = notificationInterface;
        this.atlasEntityStore      = atlasEntityStore;
        this.serviceState          = serviceState;
        this.instanceConverter     = instanceConverter;
        this.typeRegistry          = typeRegistry;
        this.applicationProperties = ApplicationProperties.get();

        maxRetries            = applicationProperties.getInt(CONSUMER_RETRIES_PROPERTY, 3);
        failedMsgCacheSize    = applicationProperties.getInt(CONSUMER_FAILEDCACHESIZE_PROPERTY, 20);
        consumerRetryInterval = applicationProperties.getInt(CONSUMER_RETRY_INTERVAL, 500);
        minWaitDuration       = applicationProperties.getInt(CONSUMER_MIN_RETRY_INTERVAL, consumerRetryInterval); // 500 ms  by default
        maxWaitDuration       = applicationProperties.getInt(CONSUMER_MAX_RETRY_INTERVAL, minWaitDuration * 60);  //  30 sec by default
    }

    @Override
    public void start() throws AtlasException {
        startInternal(applicationProperties, null);
    }

    void startInternal(Configuration configuration, ExecutorService executorService) {
        if (consumers == null) {
            consumers = new ArrayList<>();
        }
        if (executorService != null) {
            executors = executorService;
        }
        if (!HAConfiguration.isHAEnabled(configuration)) {
            LOG.info("HA is disabled, starting consumers inline.");

            startConsumers(executorService);
        }
    }

    private void startConsumers(ExecutorService executorService) {
        int                                          numThreads            = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 1);
        List<NotificationConsumer<HookNotification>> notificationConsumers = notificationInterface.createConsumers(NotificationType.HOOK, numThreads);

        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(notificationConsumers.size(), new ThreadFactoryBuilder().setNameFormat(THREADNAME_PREFIX + " thread-%d").build());
        }

        executors = executorService;

        for (final NotificationConsumer<HookNotification> consumer : notificationConsumers) {
            HookConsumer hookConsumer = new HookConsumer(consumer);

            consumers.add(hookConsumer);
            executors.submit(hookConsumer);
        }
    }

    @Override
    public void stop() {
        //Allow for completion of outstanding work
        try {
            stopConsumerThreads();
            if (executors != null) {
                executors.shutdown();

                if (!executors.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }

                executors = null;
            }

            notificationInterface.close();
        } catch (InterruptedException e) {
            LOG.error("Failure in shutting down consumers");
        }
    }

    private void stopConsumerThreads() {
        LOG.info("==> stopConsumerThreads()");

        if (consumers != null) {
            for (HookConsumer consumer : consumers) {
                consumer.shutdown();
            }

            consumers.clear();
        }

        LOG.info("<== stopConsumerThreads()");
    }

    /**
     * Start Kafka consumer threads that read from Kafka topic when server is activated.
     * <p>
     * Since the consumers create / update entities to the shared backend store, only the active instance
     * should perform this activity. Hence, these threads are started only on server activation.
     */
    @Override
    public void instanceIsActive() {
        LOG.info("Reacting to active state: initializing Kafka consumers");

        startConsumers(executors);
    }

    /**
     * Stop Kafka consumer threads that read from Kafka topic when server is de-activated.
     * <p>
     * Since the consumers create / update entities to the shared backend store, only the active instance
     * should perform this activity. Hence, these threads are stopped only on server deactivation.
     */
    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive state: shutting down Kafka consumers.");

        stop();
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.NOTIFICATION_HOOK_CONSUMER.getOrder();
    }

    static class Timer {
        public void sleep(int interval) throws InterruptedException {
            Thread.sleep(interval);
        }
    }

    static class AdaptiveWaiter {
        private final long increment;
        private final long maxDuration;
        private final long minDuration;
        private final long resetInterval;
        private       long lastWaitAt;

        @VisibleForTesting
        long waitDuration;

        public AdaptiveWaiter(long minDuration, long maxDuration, long increment) {
            this.minDuration   = minDuration;
            this.maxDuration   = maxDuration;
            this.increment     = increment;
            this.waitDuration  = minDuration;
            this.lastWaitAt    = 0;
            this.resetInterval = maxDuration * 2;
        }

        public void pause(Exception ex) {
            setWaitDurations();

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} in NotificationHookConsumer. Waiting for {} ms for recovery.", ex.getClass().getName(), waitDuration, ex);
                }

                Thread.sleep(waitDuration);
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} in NotificationHookConsumer. Waiting for recovery interrupted.", ex.getClass().getName(), e);
                }
            }
        }

        private void setWaitDurations() {
            long timeSinceLastWait = (lastWaitAt == 0) ? 0 : System.currentTimeMillis() - lastWaitAt;

            lastWaitAt = System.currentTimeMillis();

            if (timeSinceLastWait > resetInterval) {
                waitDuration = minDuration;
            } else {
                waitDuration += increment;
                if (waitDuration > maxDuration) {
                    waitDuration = maxDuration;
                }
            }
        }
    }

    @VisibleForTesting
    class HookConsumer extends ShutdownableThread {
        private final NotificationConsumer<HookNotification> consumer;
        private final AtomicBoolean                          shouldRun      = new AtomicBoolean(false);
        private final List<HookNotification>                 failedMessages = new ArrayList<>();
        private final AdaptiveWaiter                         adaptiveWaiter = new AdaptiveWaiter(minWaitDuration, maxWaitDuration, minWaitDuration);

        @VisibleForTesting
        final FailedCommitOffsetRecorder failedCommitOffsetRecorder;

        public HookConsumer(NotificationConsumer<HookNotification> consumer) {
            super("atlas-hook-consumer-thread", false);

            this.consumer = consumer;
            failedCommitOffsetRecorder = new FailedCommitOffsetRecorder();
        }

        @Override
        public void doWork() {
            LOG.info("==> HookConsumer doWork()");

            shouldRun.set(true);

            if (!serverAvailable(new NotificationHookConsumer.Timer())) {
                return;
            }

            try {
                while (shouldRun.get()) {
                    try {
                        List<AtlasKafkaMessage<HookNotification>> messages = consumer.receive();

                        for (AtlasKafkaMessage<HookNotification> msg : messages) {
                            handleMessage(msg);
                        }
                    } catch (IllegalStateException ex) {
                        adaptiveWaiter.pause(ex);
                    } catch (Exception e) {
                        if (shouldRun.get()) {
                            LOG.warn("Exception in NotificationHookConsumer", e);

                            adaptiveWaiter.pause(e);
                        } else {
                            break;
                        }
                    }
                }
            } finally {
                if (consumer != null) {
                    LOG.info("closing NotificationConsumer");

                    consumer.close();
                }

                LOG.info("<== HookConsumer doWork()");
            }
        }

        @VisibleForTesting
        void handleMessage(AtlasKafkaMessage<HookNotification> kafkaMsg) throws AtlasServiceException, AtlasException {
            AtlasPerfTracer  perf        = null;
            HookNotification message     = kafkaMsg.getMessage();
            String           messageUser = message.getUser();
            long             startTime   = System.currentTimeMillis();
            boolean          isFailedMsg = false;
            AuditLog         auditLog = null;

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, message.getType().name());
            }

            try {
                if(failedCommitOffsetRecorder.isMessageReplayed(kafkaMsg.getOffset())) {
                    commit(kafkaMsg);
                    return;
                }

                // Used for intermediate conversions during create and update
                for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("handleMessage({}): attempt {}", message.getType().name(), numRetries);
                    }

                    try {
                        RequestContext requestContext = RequestContext.get();

                        requestContext.setUser(messageUser, null);

                        switch (message.getType()) {
                            case ENTITY_CREATE: {
                                final EntityCreateRequest      createRequest = (EntityCreateRequest) message;
                                final AtlasEntitiesWithExtInfo entities      = instanceConverter.toAtlasEntities(createRequest.getEntities());

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClient.API_V1.CREATE_ENTITY.getMethod(),
                                                            AtlasClient.API_V1.CREATE_ENTITY.getNormalizedPath());
                                }

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), false);
                            }
                            break;

                            case ENTITY_PARTIAL_UPDATE: {
                                final EntityPartialUpdateRequest partialUpdateRequest = (EntityPartialUpdateRequest) message;
                                final Referenceable              referenceable        = partialUpdateRequest.getEntity();
                                final AtlasEntitiesWithExtInfo   entities             = instanceConverter.toAtlasEntity(referenceable);

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                                            String.format(AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), partialUpdateRequest.getTypeName()));
                                }

                                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(partialUpdateRequest.getTypeName());
                                String          guid       = AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, Collections.singletonMap(partialUpdateRequest.getAttribute(), (Object)partialUpdateRequest.getAttributeValue()));

                                // There should only be one root entity
                                entities.getEntities().get(0).setGuid(guid);

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), true);
                            }
                            break;

                            case ENTITY_DELETE: {
                                final EntityDeleteRequest deleteRequest = (EntityDeleteRequest) message;

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                                            String.format(AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), deleteRequest.getTypeName()));
                                }

                                try {
                                    AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(deleteRequest.getTypeName());

                                    atlasEntityStore.deleteByUniqueAttributes(type, Collections.singletonMap(deleteRequest.getAttribute(), (Object) deleteRequest.getAttributeValue()));
                                } catch (ClassCastException cle) {
                                    LOG.error("Failed to delete entity {}", deleteRequest);
                                }
                            }
                            break;

                            case ENTITY_FULL_UPDATE: {
                                final EntityUpdateRequest      updateRequest = (EntityUpdateRequest) message;
                                final AtlasEntitiesWithExtInfo entities      = instanceConverter.toAtlasEntities(updateRequest.getEntities());

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(),
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                                }

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), false);
                            }
                            break;

                            case ENTITY_CREATE_V2: {
                                final EntityCreateRequestV2 createRequestV2 = (EntityCreateRequestV2) message;
                                final AtlasEntitiesWithExtInfo entities        = createRequestV2.getEntities();

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClientV2.API_V2.CREATE_ENTITY.getMethod(),
                                                            AtlasClientV2.API_V2.CREATE_ENTITY.getNormalizedPath());
                                }

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), false);
                            }
                            break;

                            case ENTITY_PARTIAL_UPDATE_V2: {
                                final EntityPartialUpdateRequestV2 partialUpdateRequest = (EntityPartialUpdateRequestV2) message;
                                final AtlasObjectId                entityId             = partialUpdateRequest.getEntityId();
                                final AtlasEntityWithExtInfo       entity               = partialUpdateRequest.getEntity();

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(),
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                                }

                                atlasEntityStore.updateEntity(entityId, entity, true);
                            }
                            break;

                            case ENTITY_FULL_UPDATE_V2: {
                                final EntityUpdateRequestV2    updateRequest = (EntityUpdateRequestV2) message;
                                final AtlasEntitiesWithExtInfo entities      = updateRequest.getEntities();

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(),
                                                            AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                                }

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), false);
                            }
                            break;

                            case ENTITY_DELETE_V2: {
                                final EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) message;
                                final List<AtlasObjectId>   entities      = deleteRequest.getEntities();

                                try {
                                    for (AtlasObjectId entity : entities) {
                                        if (auditLog == null) {
                                            auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                                    AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                                                    String.format(AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), entity.getTypeName()));
                                        }

                                        AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());

                                        atlasEntityStore.deleteByUniqueAttributes(type, entity.getUniqueAttributes());
                                    }
                                } catch (ClassCastException cle) {
                                    LOG.error("Failed to do delete entities {}", entities);
                                }
                            }
                            break;

                            default:
                                throw new IllegalStateException("Unknown notification type: " + message.getType().name());
                        }

                        break;
                    } catch (Throwable e) {
                        LOG.warn("Error handling message", e);
                        try {
                            LOG.info("Sleeping for {} ms before retry", consumerRetryInterval);

                            Thread.sleep(consumerRetryInterval);
                        } catch (InterruptedException ie) {
                            LOG.error("Notification consumer thread sleep interrupted");
                        }

                        if (numRetries == (maxRetries - 1)) {
                            LOG.warn("Max retries exceeded for message {}", message, e);

                            isFailedMsg = true;

                            failedMessages.add(message);

                            if (failedMessages.size() >= failedMsgCacheSize) {
                                recordFailedMessages();
                            }
                            return;
                        }
                    } finally {
                        RequestContext.clear();
                    }
                }

                commit(kafkaMsg);
            } finally {
                AtlasPerfTracer.log(perf);

                if (auditLog != null) {
                    auditLog.setHttpStatus(isFailedMsg ? SC_BAD_REQUEST : SC_OK);
                    auditLog.setTimeTaken(System.currentTimeMillis() - startTime);

                    AuditFilter.audit(auditLog);
                }
            }
        }

        private void recordFailedMessages() {
            //logging failed messages
            for (HookNotification message : failedMessages) {
                FAILED_LOG.error("[DROPPED_NOTIFICATION] {}", AbstractNotification.getMessageJson(message));
            }

            failedMessages.clear();
        }

        private void commit(AtlasKafkaMessage<HookNotification> kafkaMessage) {
            boolean commitSucceessStatus = false;
            try {
                recordFailedMessages();

                TopicPartition partition = new TopicPartition("ATLAS_HOOK", kafkaMessage.getPartition());

                consumer.commit(partition, kafkaMessage.getOffset() + 1);
                commitSucceessStatus = true;
            } finally {
                failedCommitOffsetRecorder.recordIfFailed(commitSucceessStatus, kafkaMessage.getOffset());
            }
        }

        boolean serverAvailable(Timer timer) {
            try {
                while (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                    try {
                        LOG.info("Atlas Server is not ready. Waiting for {} milliseconds to retry...", SERVER_READY_WAIT_TIME_MS);

                        timer.sleep(SERVER_READY_WAIT_TIME_MS);
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting for Atlas Server to become ready, " + "exiting consumer thread.", e);

                        return false;
                    }
                }
            } catch (Throwable e) {
                LOG.info("Handled AtlasServiceException while waiting for Atlas Server to become ready, exiting consumer thread.", e);

                return false;
            }

            LOG.info("Atlas Server is ready, can start reading Kafka events.");

            return true;
        }

        @Override
        public void shutdown() {
            LOG.info("==> HookConsumer shutdown()");

            // handle the case where thread was not started at all
            // and shutdown called
            if (shouldRun.get() == false) {
                return;
            }

            super.initiateShutdown();

            shouldRun.set(false);

            if (consumer != null) {
                consumer.wakeup();
            }

            super.awaitShutdown();

            LOG.info("<== HookConsumer shutdown()");
        }
    }

    static class FailedCommitOffsetRecorder {
        private Long currentOffset;

        public void recordIfFailed(boolean commitStatus, long offset) {
            if(commitStatus) {
                currentOffset = null;
            } else {
                currentOffset = offset;
            }
        }

        public boolean isMessageReplayed(long offset) {
            return currentOffset != null && currentOffset == offset;
        }

        public Long getCurrentOffset() {
            return currentOffset;
        }
    }
}
