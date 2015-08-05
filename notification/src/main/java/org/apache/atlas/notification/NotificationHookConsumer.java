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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NotificationHookConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationHookConsumer.class);

    public static final String CONSUMER_THREADS_PROPERTY = "atlas.notification.hook.numthreads";
    public static final String ATLAS_ENDPOINT_PROPERTY = "atlas.rest.address";

    @Inject
    private static NotificationInterface notificationInterface;

    private static ExecutorService executors;
    private static AtlasClient atlasClient;

    public static void start() throws AtlasException {
        Configuration applicationProperties = ApplicationProperties.get();
        notificationInterface.initialize(applicationProperties);

        String atlasEndpoint = applicationProperties.getString(ATLAS_ENDPOINT_PROPERTY, "http://localhost:21000");
        atlasClient = new AtlasClient(atlasEndpoint);
        int numThreads = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 2);
        List<NotificationConsumer> consumers =
                notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, numThreads);
        executors = Executors.newFixedThreadPool(consumers.size());

        for (final NotificationConsumer consumer : consumers) {
            executors.submit(new HookConsumer(consumer));
        }
    }

    public static void stop() {
        notificationInterface.shutdown();
        executors.shutdown();
    }

    static class HookConsumer implements Runnable {
        private final NotificationConsumer consumer;

        public HookConsumer(NotificationConsumer consumerInterface) {
            this.consumer = consumerInterface;
        }

        @Override
        public void run() {
            while(consumer.hasNext()) {
                String entityJson = consumer.next();
                LOG.debug("Processing message {}", entityJson);
                try {
                    atlasClient.createEntity(entityJson);
                } catch (AtlasServiceException e) {
                    //todo handle failures
                    LOG.warn("Error handling message {}", entityJson);
                }
            }
        }
    }
}
