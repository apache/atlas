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
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.notification.hook.HookNotification.EntityCreateRequest;
import org.apache.atlas.notification.hook.HookNotification.EntityDeleteRequest;
import org.apache.atlas.notification.hook.HookNotification.EntityPartialUpdateRequest;
import org.apache.atlas.notification.hook.HookNotification.EntityUpdateRequest;
import org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.filters.AuditFilter;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.DateTimeHelper;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE;
import static org.apache.atlas.AtlasClientV2.API_V2.UPDATE_ENTITY;
import static org.apache.atlas.AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE;

/**
 * Consumer of notifications from hooks e.g., hive hook etc.
 */
@Component
@Order(4)
public class NotificationHookConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationHookConsumer.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger(NotificationHookConsumer.class);
    private static final String LOCALHOST = "localhost";
    private static Logger FAILED_LOG = LoggerFactory.getLogger("FAILED");

    private static final String THREADNAME_PREFIX = NotificationHookConsumer.class.getSimpleName();

    public static final String CONSUMER_THREADS_PROPERTY = "atlas.notification.hook.numthreads";
    public static final String CONSUMER_RETRIES_PROPERTY = "atlas.notification.hook.maxretries";
    public static final String CONSUMER_FAILEDCACHESIZE_PROPERTY = "atlas.notification.hook.failedcachesize";
    public static final String CONSUMER_RETRY_INTERVAL = "atlas.notification.consumer.retry.interval";
    public static final String CONSUMER_MIN_RETRY_INTERVAL = "atlas.notification.consumer.min.retry.interval";
    public static final String CONSUMER_MAX_RETRY_INTERVAL = "atlas.notification.consumer.max.retry.interval";


    public static final int SERVER_READY_WAIT_TIME_MS = 1000;
    private final AtlasEntityStore atlasEntityStore;
    private final ServiceState serviceState;
    private final AtlasInstanceConverter instanceConverter;
    private final AtlasTypeRegistry typeRegistry;
    private final int maxRetries;
    private final int failedMsgCacheSize;

    @VisibleForTesting
    final int consumerRetryInterval;
    private final int minWaitDuration;
    private final int maxWaitDuration;

    private NotificationInterface notificationInterface;
    private ExecutorService executors;
    private Configuration applicationProperties;

    @VisibleForTesting
    List<HookConsumer> consumers;

    @Inject
    public NotificationHookConsumer(NotificationInterface notificationInterface, AtlasEntityStore atlasEntityStore,
                                    ServiceState serviceState, AtlasInstanceConverter instanceConverter,
                                    AtlasTypeRegistry typeRegistry) throws AtlasException {
        this.notificationInterface = notificationInterface;
        this.atlasEntityStore = atlasEntityStore;
        this.serviceState = serviceState;
        this.instanceConverter = instanceConverter;
        this.typeRegistry = typeRegistry;

        this.applicationProperties = ApplicationProperties.get();

        maxRetries = applicationProperties.getInt(CONSUMER_RETRIES_PROPERTY, 3);
        failedMsgCacheSize = applicationProperties.getInt(CONSUMER_FAILEDCACHESIZE_PROPERTY, 20);
        consumerRetryInterval = applicationProperties.getInt(CONSUMER_RETRY_INTERVAL, 500);
        minWaitDuration = applicationProperties.getInt(CONSUMER_MIN_RETRY_INTERVAL, consumerRetryInterval); // 500 ms  by default
        maxWaitDuration = applicationProperties.getInt(CONSUMER_MAX_RETRY_INTERVAL, minWaitDuration * 60);  //  30 sec by default
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
        int numThreads = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 1);
        List<NotificationConsumer<HookNotificationMessage>> notificationConsumers =
                notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, numThreads);
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(notificationConsumers.size(),
                    new ThreadFactoryBuilder().setNameFormat(THREADNAME_PREFIX + " thread-%d").build());
        }
        executors = executorService;
        for (final NotificationConsumer<HookNotificationMessage> consumer : notificationConsumers) {
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

        private long lastWaitAt;
        @VisibleForTesting
        long waitDuration;

        public AdaptiveWaiter(long minDuration, long maxDuration, long increment) {
            this.minDuration = minDuration;
            this.maxDuration = maxDuration;
            this.increment = increment;

            this.waitDuration = minDuration;
            this.lastWaitAt = 0;
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
        private final NotificationConsumer<HookNotificationMessage> consumer;
        private final AtomicBoolean shouldRun = new AtomicBoolean(false);
        private List<HookNotificationMessage> failedMessages = new ArrayList<>();

        private final AdaptiveWaiter adaptiveWaiter = new AdaptiveWaiter(minWaitDuration, maxWaitDuration, minWaitDuration);

        public HookConsumer(NotificationConsumer<HookNotificationMessage> consumer) {
            super("atlas-hook-consumer-thread", false);
            this.consumer = consumer;
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
                        List<AtlasKafkaMessage<HookNotificationMessage>> messages = consumer.receive();
                        for (AtlasKafkaMessage<HookNotificationMessage> msg : messages) {
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
        void handleMessage(AtlasKafkaMessage<HookNotificationMessage> kafkaMsg) throws AtlasServiceException, AtlasException {
            AtlasPerfTracer perf = null;

            HookNotificationMessage message = kafkaMsg.getMessage();
            String messageUser = message.getUser();

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, message.getType().name());
            }

            try {
                // Used for intermediate conversions during create and update
                AtlasEntity.AtlasEntitiesWithExtInfo entities;
                for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("handleMessage({}): attempt {}", message.getType().name(), numRetries);
                    }
                    try {
                        RequestContext requestContext = RequestContext.createContext();
                        requestContext.setUser(messageUser);

                        switch (message.getType()) {
                            case ENTITY_CREATE:
                                EntityCreateRequest createRequest = (EntityCreateRequest) message;

                                if (numRetries == 0) { // audit only on the first attempt
                                    AtlasBaseClient.API api = AtlasClient.API_V1.CREATE_ENTITY;
                                    audit(messageUser, api.getMethod(), api.getNormalizedPath());
                                }

                                entities = instanceConverter.toAtlasEntities(createRequest.getEntities());

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), false);
                                break;

                            case ENTITY_PARTIAL_UPDATE:
                                final EntityPartialUpdateRequest partialUpdateRequest = (EntityPartialUpdateRequest) message;

                                if (numRetries == 0) { // audit only on the first attempt
                                    AtlasBaseClient.API api = UPDATE_ENTITY_BY_ATTRIBUTE;
                                    audit(messageUser, api.getMethod(),
                                          String.format(api.getNormalizedPath(), partialUpdateRequest.getTypeName()));
                                }

                                Referenceable referenceable = partialUpdateRequest.getEntity();
                                entities = instanceConverter.toAtlasEntity(referenceable);

                                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(partialUpdateRequest.getTypeName());
                                String guid = AtlasGraphUtilsV1.getGuidByUniqueAttributes(entityType, new HashMap<String, Object>() {
                                    {
                                        put(partialUpdateRequest.getAttribute(), partialUpdateRequest.getAttributeValue());
                                    }
                                });

                                // There should only be one root entity
                                entities.getEntities().get(0).setGuid(guid);

                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), true);
                                break;

                            case ENTITY_DELETE:
                                final EntityDeleteRequest deleteRequest = (EntityDeleteRequest) message;

                                if (numRetries == 0) { // audit only on the first attempt
                                    AtlasBaseClient.API api = DELETE_ENTITY_BY_ATTRIBUTE;
                                    audit(messageUser, api.getMethod(),
                                          String.format(api.getNormalizedPath(), deleteRequest.getTypeName()));
                                }

                                try {
                                    AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(deleteRequest.getTypeName());
                                    atlasEntityStore.deleteByUniqueAttributes(type,
                                            new HashMap<String, Object>() {{
                                                put(deleteRequest.getAttribute(), deleteRequest.getAttributeValue());
                                            }});
                                } catch (ClassCastException cle) {
                                    LOG.error("Failed to do a partial update on Entity");
                                }
                                break;

                            case ENTITY_FULL_UPDATE:
                                EntityUpdateRequest updateRequest = (EntityUpdateRequest) message;

                                if (numRetries == 0) { // audit only on the first attempt
                                    AtlasBaseClient.API api = UPDATE_ENTITY;
                                    audit(messageUser, api.getMethod(), api.getNormalizedPath());
                                }

                                entities = instanceConverter.toAtlasEntities(updateRequest.getEntities());
                                atlasEntityStore.createOrUpdate(new AtlasEntityStream(entities), false);
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
                            failedMessages.add(message);
                            if (failedMessages.size() >= failedMsgCacheSize) {
                                recordFailedMessages();
                            }
                            return;
                        }
                    } finally {
                        RequestContext.clear();
                        RequestContextV1.clear();
                    }
                }
                commit(kafkaMsg);
            } finally {
                AtlasPerfTracer.log(perf);
            }
        }

        private void recordFailedMessages() {
            //logging failed messages
            for (HookNotificationMessage message : failedMessages) {
                FAILED_LOG.error("[DROPPED_NOTIFICATION] {}", AbstractNotification.getMessageJson(message));
            }
            failedMessages.clear();
        }

        private void commit(AtlasKafkaMessage<HookNotificationMessage> kafkaMessage) {
            recordFailedMessages();
            TopicPartition partition = new TopicPartition("ATLAS_HOOK", kafkaMessage.getPartition());
            consumer.commit(partition, kafkaMessage.getOffset() + 1);
        }

        boolean serverAvailable(Timer timer) {
            try {
                while (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                    try {
                        LOG.info("Atlas Server is not ready. Waiting for {} milliseconds to retry...",
                                SERVER_READY_WAIT_TIME_MS);
                        timer.sleep(SERVER_READY_WAIT_TIME_MS);
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting for Atlas Server to become ready, "
                                + "exiting consumer thread.", e);
                        return false;
                    }
                }
            } catch (Throwable e) {
                LOG.info(
                        "Handled AtlasServiceException while waiting for Atlas Server to become ready, "
                                + "exiting consumer thread.", e);
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

    private void audit(String messageUser, String method, String path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> audit({},{}, {})", messageUser, method, path);
        }

        AuditFilter.audit(messageUser, THREADNAME_PREFIX, method, LOCALHOST, path, LOCALHOST,
                DateTimeHelper.formatDateUTC(new Date()));
    }
}