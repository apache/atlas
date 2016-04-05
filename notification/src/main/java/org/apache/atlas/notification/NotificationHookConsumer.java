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
package org.apache.atlas.notification;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import kafka.consumer.ConsumerTimeoutException;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer of notifications from hooks e.g., hive hook etc.
 */
@Singleton
public class NotificationHookConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationHookConsumer.class);

    public static final String CONSUMER_THREADS_PROPERTY = "atlas.notification.hook.numthreads";
    public static final String ATLAS_ENDPOINT_PROPERTY = "atlas.rest.address";
    public static final int SERVER_READY_WAIT_TIME_MS = 1000;

    private NotificationInterface notificationInterface;
    private ExecutorService executors;
    private String atlasEndpoint;
    private Configuration applicationProperties;
    private List<HookConsumer> consumers;

    @Inject
    public NotificationHookConsumer(NotificationInterface notificationInterface) {
        this.notificationInterface = notificationInterface;
    }

    @Override
    public void start() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        startInternal(configuration, null);
    }

    void startInternal(Configuration configuration,
                       ExecutorService executorService) {
        this.applicationProperties = configuration;
        this.atlasEndpoint = applicationProperties.getString(ATLAS_ENDPOINT_PROPERTY, "http://localhost:21000");
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
        List<NotificationConsumer<HookNotification.HookNotificationMessage>> notificationConsumers =
                notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, numThreads);
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(notificationConsumers.size());
        }
        executors = executorService;
        for (final NotificationConsumer<HookNotification.HookNotificationMessage> consumer : notificationConsumers) {
            HookConsumer hookConsumer = new HookConsumer(consumer);
            consumers.add(hookConsumer);
            executors.submit(hookConsumer);
        }
    }

    @Override
    public void stop() {
        //Allow for completion of outstanding work
        notificationInterface.close();
        try {
            if (executors != null) {
                stopConsumerThreads();
                executors.shutdownNow();
                if (!executors.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
                executors = null;
            }
        } catch (InterruptedException e) {
            LOG.error("Failure in shutting down consumers");
        }
    }

    private void stopConsumerThreads() {
        if (consumers != null) {
            for (HookConsumer consumer : consumers) {
                consumer.stop();
            }
            consumers.clear();
        }
    }

    /**
     * Start Kafka consumer threads that read from Kafka topic when server is activated.
     *
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
     *
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

    class HookConsumer implements Runnable {
        private final NotificationConsumer<HookNotification.HookNotificationMessage> consumer;
        private final AtomicBoolean shouldRun = new AtomicBoolean(false);

        public HookConsumer(NotificationConsumer<HookNotification.HookNotificationMessage> consumer) {
            this.consumer = consumer;
        }

        private boolean hasNext() {
            try {
                return consumer.hasNext();
            } catch (ConsumerTimeoutException e) {
                return false;
            }
        }

        @Override
        public void run() {
            shouldRun.set(true);

            if (!serverAvailable(new NotificationHookConsumer.Timer())) {
                return;
            }

            while (shouldRun.get()) {
                try {
                    if (hasNext()) {
                        HookNotification.HookNotificationMessage message = consumer.next();
                        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(message.getUser());
                        AtlasClient atlasClient = getAtlasClient(ugi);

                        try {
                            switch (message.getType()) {
                            case ENTITY_CREATE:
                                HookNotification.EntityCreateRequest createRequest =
                                        (HookNotification.EntityCreateRequest) message;
                                atlasClient.createEntity(createRequest.getEntities());
                                break;

                            case ENTITY_PARTIAL_UPDATE:
                                HookNotification.EntityPartialUpdateRequest partialUpdateRequest =
                                        (HookNotification.EntityPartialUpdateRequest) message;
                                atlasClient.updateEntity(partialUpdateRequest.getTypeName(),
                                        partialUpdateRequest.getAttribute(),
                                        partialUpdateRequest.getAttributeValue(), partialUpdateRequest.getEntity());
                                break;

                            case ENTITY_DELETE:
                                HookNotification.EntityDeleteRequest deleteRequest =
                                    (HookNotification.EntityDeleteRequest) message;
                                atlasClient.deleteEntity(deleteRequest.getTypeName(),
                                    deleteRequest.getAttribute(),
                                    deleteRequest.getAttributeValue());
                                break;

                            case ENTITY_FULL_UPDATE:
                                HookNotification.EntityUpdateRequest updateRequest =
                                        (HookNotification.EntityUpdateRequest) message;
                                atlasClient.updateEntities(updateRequest.getEntities());
                                break;

                            default:
                                throw new IllegalStateException("Unhandled exception!");
                            }
                        } catch (Exception e) {
                            //todo handle failures
                            LOG.warn("Error handling message {}", message, e);
                        }
                    }
                } catch (Throwable t) {
                    LOG.warn("Failure in NotificationHookConsumer", t);
                }
            }
        }

        protected AtlasClient getAtlasClient(UserGroupInformation ugi) {
            return new AtlasClient(atlasEndpoint, ugi, ugi.getShortUserName());
        }

        boolean serverAvailable(Timer timer) {
            try {
                AtlasClient atlasClient = getAtlasClient(UserGroupInformation.getCurrentUser());
                while (!atlasClient.isServerReady()) {
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

        public void stop() {
            shouldRun.set(false);
        }
    }
}
