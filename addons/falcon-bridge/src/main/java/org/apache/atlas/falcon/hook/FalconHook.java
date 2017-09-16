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

package org.apache.atlas.falcon.hook;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.falcon.bridge.FalconBridge;
import org.apache.atlas.falcon.event.FalconEvent;
import org.apache.atlas.falcon.publisher.FalconEventPublisher;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Falcon hook sends lineage information to the Atlas Service.
 */
public class FalconHook extends AtlasHook implements FalconEventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(FalconHook.class);

    public static final String CONF_PREFIX = "atlas.hook.falcon.";
    private static final String MIN_THREADS = CONF_PREFIX + "minThreads";
    private static final String MAX_THREADS = CONF_PREFIX + "maxThreads";
    private static final String KEEP_ALIVE_TIME = CONF_PREFIX + "keepAliveTime";
    public static final String QUEUE_SIZE = CONF_PREFIX + "queueSize";
    public static final String CONF_SYNC = CONF_PREFIX + "synchronous";

    public static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static ExecutorService executor;

    private static final int minThreadsDefault = 5;
    private static final int maxThreadsDefault = 5;
    private static final long keepAliveTimeDefault = 10;
    private static final int queueSizeDefault = 10000;

    private static boolean sync;

    private static ConfigurationStore STORE;

    private enum Operation {
        ADD,
        UPDATE
    }

    static {
        try {
            // initialize the async facility to process hook calls. We don't
            // want to do this inline since it adds plenty of overhead for the query.
            int minThreads = atlasProperties.getInt(MIN_THREADS, minThreadsDefault);
            int maxThreads = atlasProperties.getInt(MAX_THREADS, maxThreadsDefault);
            long keepAliveTime = atlasProperties.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);
            int queueSize = atlasProperties.getInt(QUEUE_SIZE, queueSizeDefault);
            sync = atlasProperties.getBoolean(CONF_SYNC, false);

            executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(queueSize),
                    new ThreadFactoryBuilder().setNameFormat("Atlas Logger %d").build());

            ShutdownHookManager.get().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        LOG.info("==> Shutdown of Atlas Falcon Hook");

                        executor.shutdown();
                        executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                        executor = null;
                    } catch (InterruptedException ie) {
                        LOG.info("Interrupt received in shutdown.");
                    } finally {
                        LOG.info("<== Shutdown of Atlas Falcon Hook");
                    }
                    // shutdown client
                }
            }, AtlasConstants.ATLAS_SHUTDOWN_HOOK_PRIORITY);

            STORE = ConfigurationStore.get();

            notificationInterface = NotificationProvider.get();

        } catch (Exception e) {
            LOG.error("Caught exception initializing the falcon hook.", e);
        }


        LOG.info("Created Atlas Hook for Falcon");
    }

    @Override
    public void publish(final Data data) {
        final FalconEvent event = data.getEvent();
        try {
            if (sync) {
                fireAndForget(event);
            } else {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            fireAndForget(event);
                        } catch (Throwable e) {
                            LOG.info("Atlas hook failed", e);
                        }
                    }
                });
            }
        } catch (Throwable t) {
            LOG.warn("Error in processing data {}", data, t);
        }
    }

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }

    private void fireAndForget(FalconEvent event) throws FalconException, URISyntaxException {
        LOG.info("Entered Atlas hook for Falcon hook operation {}", event.getOperation());
        List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();

        Operation op = getOperation(event.getOperation());
        String user = getUser(event.getUser());
        LOG.info("fireAndForget user:{}", user);
        switch (op) {
        case ADD:
            messages.add(new HookNotification.EntityCreateRequest(user, createEntities(event, user)));
            break;

        }
        notifyEntities(messages);
    }

    private List<Referenceable> createEntities(FalconEvent event, String user) throws FalconException, URISyntaxException {
        List<Referenceable> entities = new ArrayList<>();

        switch (event.getOperation()) {
        case ADD_CLUSTER:
            entities.add(FalconBridge
                    .createClusterEntity((org.apache.falcon.entity.v0.cluster.Cluster) event.getEntity()));
            break;

        case ADD_PROCESS:
            entities.addAll(FalconBridge.createProcessEntity((Process) event.getEntity(), STORE));
            break;

        case ADD_FEED:
            entities.addAll(FalconBridge.createFeedCreationEntity((Feed) event.getEntity(), STORE));
            break;

        case UPDATE_CLUSTER:
        case UPDATE_FEED:
        case UPDATE_PROCESS:
        default:
            LOG.info("Falcon operation {} is not valid or supported", event.getOperation());
        }

        return entities;
    }

    private static Operation getOperation(final FalconEvent.OPERATION op) throws FalconException {
        switch (op) {
        case ADD_CLUSTER:
        case ADD_FEED:
        case ADD_PROCESS:
            return Operation.ADD;

        case UPDATE_CLUSTER:
        case UPDATE_FEED:
        case UPDATE_PROCESS:
            return Operation.UPDATE;

        default:
            throw new FalconException("Falcon operation " + op + " is not valid or supported");
        }
    }
}

