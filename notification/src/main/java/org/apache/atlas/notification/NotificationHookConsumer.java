/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Consumer of notifications from hooks e.g., hive hook etc
 */
@Singleton
public class NotificationHookConsumer implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationHookConsumer.class);

    public static final String CONSUMER_THREADS_PROPERTY = "atlas.notification.hook.numthreads";
    public static final String ATLAS_ENDPOINT_PROPERTY = "atlas.rest.address";
    public static final int SERVER_READY_WAIT_TIME_MS = 1000;

    @Inject
    private NotificationInterface notificationInterface;
    private ExecutorService executors;
    private AtlasClient atlasClient;

    @Override
    public void start() throws AtlasException {
        Configuration applicationProperties = ApplicationProperties.get();

        String atlasEndpoint = applicationProperties.getString(ATLAS_ENDPOINT_PROPERTY, "http://localhost:21000");
        atlasClient = new AtlasClient(atlasEndpoint);
        int numThreads = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 1);
        List<NotificationConsumer<JSONArray>> consumers =
                notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, numThreads);
        executors = Executors.newFixedThreadPool(consumers.size());

        for (final NotificationConsumer<JSONArray> consumer : consumers) {
            executors.submit(new HookConsumer(consumer));
        }
    }

    @Override
    public void stop() {
        //Allow for completion of outstanding work
        notificationInterface.close();
        try {
            if (executors != null && !executors.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            LOG.error("Failure in shutting down consumers");
        }
    }

    static class Timer {
        public void sleep(int interval) throws InterruptedException {
            Thread.sleep(interval);
        }
    }

    class HookConsumer implements Runnable {
        private final NotificationConsumer<JSONArray> consumer;
        private final AtlasClient client;

        public HookConsumer(NotificationConsumer<JSONArray> consumer) {
            this(atlasClient, consumer);
        }

        public HookConsumer(AtlasClient client, NotificationConsumer<JSONArray> consumer) {
            this.client = client;
            this.consumer = consumer;
        }

        @Override
        public void run() {

            if (!serverAvailable(new NotificationHookConsumer.Timer())) {
                return;
            }

            while(consumer.hasNext()) {
                JSONArray entityJson = consumer.next();
                LOG.info("Processing message {}", entityJson);
                try {
                    JSONArray guids = atlasClient.createEntity(entityJson);
                    LOG.info("Create entities with guid {}", guids);
                } catch (Exception e) {
                    //todo handle failures
                    LOG.warn("Error handling message {}", entityJson, e);
                }
            }
        }

        boolean serverAvailable(Timer timer) {
            try {
                while (!client.isServerReady()) {
                    try {
                        LOG.info("Atlas Server is not ready. Waiting for {} milliseconds to retry...",
                                SERVER_READY_WAIT_TIME_MS);
                        timer.sleep(SERVER_READY_WAIT_TIME_MS);
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting for Atlas Server to become ready, " +
                                "exiting consumer thread.", e);
                        return false;
                    }
                }
            } catch (AtlasServiceException e) {
                LOG.info(
                        "Handled AtlasServiceException while waiting for Atlas Server to become ready, " +
                                "exiting consumer thread.", e);
                return false;
            }
            LOG.info("Atlas Server is ready, can start reading Kafka events.");
            return true;
        }
    }
}
