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
package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ICuratorFactory;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.ApplicationProperties.DEFAULT_INDEX_RECOVERY;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_NAME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_START_TIME;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;

@Component
@Order(8)
public class IndexRecoveryService implements Service, ActiveStateChangeHandler {
    private static final Logger LOG                                       = LoggerFactory.getLogger(IndexRecoveryService.class);
    private static final String DATE_FORMAT                               = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String INDEX_HEALTH_MONITOR_THREAD_NAME          = "index-health-monitor";
    private static final String SOLR_STATUS_CHECK_RETRY_INTERVAL          = "atlas.graph.index.status.check.frequency";
    private static final String SOLR_INDEX_RECOVERY_CONFIGURED_START_TIME = "atlas.graph.index.recovery.start.time";
    private static final long   SOLR_STATUS_RETRY_DEFAULT_MS              = 30000; // 30 secs default
    private static final String INDEX_RECOVERY_LOCK = "/index-recovery-lock";

    private final Thread                 indexHealthMonitor;
    private final RecoveryInfoManagement recoveryInfoManagement;
    private       Configuration          configuration;
    private       boolean                isIndexRecoveryEnabled;
    private       RecoveryThread         recoveryThread;

    @Inject
    public IndexRecoveryService(Configuration config, AtlasGraph graph, ICuratorFactory curatorFactory) {
        this.configuration               = config;
        this.isIndexRecoveryEnabled      = config.getBoolean(ApplicationProperties.INDEX_RECOVERY_CONF, DEFAULT_INDEX_RECOVERY);
        long recoveryStartTimeFromConfig = getRecoveryStartTimeFromConfig(config);
        long healthCheckFrequencyMillis  = config.getLong(SOLR_STATUS_CHECK_RETRY_INTERVAL, SOLR_STATUS_RETRY_DEFAULT_MS);
        this.recoveryInfoManagement      = new RecoveryInfoManagement(graph);

        final String zkRoot = HAConfiguration.getZookeeperProperties(configuration).getZkRoot();
        final boolean isActiveActiveHAEnabled = HAConfiguration.isActiveActiveHAEnabled(configuration);
        this.recoveryThread = new RecoveryThread(recoveryInfoManagement, graph, recoveryStartTimeFromConfig, healthCheckFrequencyMillis, curatorFactory, zkRoot, isActiveActiveHAEnabled);
        this.indexHealthMonitor = new Thread(recoveryThread, INDEX_HEALTH_MONITOR_THREAD_NAME);
    }

    private long getRecoveryStartTimeFromConfig(Configuration config) {
        long ret = 0L;

        try {
            String time = config.getString(SOLR_INDEX_RECOVERY_CONFIGURED_START_TIME);

            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            ret = dateFormat.parse(time).toInstant().toEpochMilli();
        } catch (Exception e) {
            LOG.debug("Error fetching: {}", SOLR_INDEX_RECOVERY_CONFIGURED_START_TIME);
        }

        return ret;
    }

    @Override
    public void start() throws AtlasException {
        if (configuration == null || !HAConfiguration.isHAEnabled(configuration)) {
            LOG.info("==> IndexRecoveryService.start()");

            startTxLogMonitoring();

            LOG.info("<== IndexRecoveryService.start()");
        }
    }

    @Override
    public void stop() throws AtlasException {
        try {
            recoveryThread.shutdown();

            indexHealthMonitor.join();
        } catch (InterruptedException e) {
            LOG.error("indexHealthMonitor: Interrupted", e);
        }
    }

    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("==> IndexRecoveryService.instanceIsActive()");

        startTxLogMonitoring();

        LOG.info("<== IndexRecoveryService.instanceIsActive()");
    }

    @Override
    public void instanceIsPassive() throws AtlasException {
        LOG.info("IndexRecoveryService.instanceIsPassive(): Shutting down!.");
        recoveryThread.shutdown();
    }

    @Override
    public int getHandlerOrder() {
        return ActiveStateChangeHandler.HandlerOrder.INDEX_RECOVERY.getOrder();
    }

    private void startTxLogMonitoring() {
        if (!isIndexRecoveryEnabled) {
            LOG.warn("IndexRecoveryService: Recovery should be enabled.");

            return;
        }
        if (indexHealthMonitor.isAlive()) {
            recoveryThread.shouldRun.set(true);
            LOG.info("IndexRecoveryService: Resuming existing thread.");
        } else {
            LOG.info("IndexRecoveryService: Starting new thread.");
            indexHealthMonitor.start();
        }
    }

    private static class RecoveryThread implements Runnable {
        private final AtlasGraph             graph;
        private final RecoveryInfoManagement recoveryInfoManagement;
        private       long                   indexStatusCheckRetryMillis;
        private       Object                 txRecoveryObject;
        private final ICuratorFactory curatorFactory;
        private final String zkRoot;
        private final boolean isActiveActiveHAEnabled;

        private final AtomicBoolean          shouldRun = new AtomicBoolean(false);

        private RecoveryThread(RecoveryInfoManagement recoveryInfoManagement, AtlasGraph graph, long startTimeFromConfig, long healthCheckFrequencyMillis,
                               ICuratorFactory curatorFactory, String zkRoot, boolean isActiveActiveHAEnabled) {
            this.graph                       = graph;
            this.recoveryInfoManagement      = recoveryInfoManagement;
            this.indexStatusCheckRetryMillis = healthCheckFrequencyMillis;
            this.curatorFactory = curatorFactory;
            this.zkRoot = zkRoot;
            this.isActiveActiveHAEnabled = isActiveActiveHAEnabled;

            if (startTimeFromConfig > 0) {
                this.recoveryInfoManagement.updateStartTime(startTimeFromConfig);
            }
        }

        public void run() {
            shouldRun.set(true);

            LOG.info("Index Health Monitor: Starting...");

            InterProcessMutex lock = null;
            while (true) {
                if (shouldRun.get()) {
                    try {
                        lock = acquireDistributedLock();

                        boolean indexHealthy = isIndexHealthy();

                        if (this.txRecoveryObject == null && indexHealthy) {
                            startMonitoring();
                        }

                        if (this.txRecoveryObject != null && !indexHealthy) {
                            stopMonitoring();
                        }
                    } catch (Exception e) {
                        LOG.error("Error: Index recovery monitoring!", e);
                    }
                    finally {
                        releaseLock(lock);
                    }
                }
            }
        }

        public void shutdown() {
            try {
                LOG.info("Index Health Monitor: Shutdown: Starting...");

                // handle the case where thread was not started at all
                // and shutdown called
                if (shouldRun.get() == false) {
                    return;
                }

                shouldRun.set(false);
            } finally {
                LOG.info("Index Health Monitor: Shutdown: Done!");
            }
        }

        private boolean isIndexHealthy() throws AtlasException, InterruptedException {
            Thread.sleep(indexStatusCheckRetryMillis);

            return this.graph.getGraphIndexClient().isHealthy();
        }

        private void startMonitoring() {
            Long startTime = null;

            try {
                startTime        = recoveryInfoManagement.getStartTime();
                Instant newStartTime = Instant.now();
                txRecoveryObject = this.graph.getManagementSystem().startIndexRecovery(startTime);
                recoveryInfoManagement.updateStartTime(newStartTime.toEpochMilli());

                printIndexRecoveryStats();
            } catch (Exception e) {
                LOG.error("Index Recovery: Start: Error!", e);
            } finally {
                LOG.info("Index Recovery: Started! Recovery time: {}", Instant.ofEpochMilli(startTime));
            }
        }

        private void stopMonitoring() {
            Instant newStartTime = Instant.now().minusMillis(indexStatusCheckRetryMillis);

            try {
                this.graph.getManagementSystem().stopIndexRecovery(txRecoveryObject);

                printIndexRecoveryStats();
            } catch (Exception e) {
                LOG.info("Index Recovery: Stopped! Error!", e);
            } finally {
                this.txRecoveryObject = null;

                LOG.info("Index Recovery: Stopped! Recovery time: {}", newStartTime);
            }
        }

        private void printIndexRecoveryStats() {
            this.graph.getManagementSystem().printIndexRecoveryStats(txRecoveryObject);
        }

        private InterProcessMutex acquireDistributedLock() throws Exception {
            if (!isActiveActiveHAEnabled)
                return null;

            final InterProcessMutex indexRecoveryLock = curatorFactory.lockInstance(zkRoot, INDEX_RECOVERY_LOCK);

            LOG.info("Attempting to acquire a lock on Index recovery");
            indexRecoveryLock.acquire();
            LOG.info("Acquired a lock on Index recovery");
            return indexRecoveryLock;
        }

        private void releaseLock(InterProcessMutex indexRecoveryLock) {
            if (indexRecoveryLock == null)
                return;

            try {
                if (indexRecoveryLock.isOwnedByCurrentThread()) {
                    LOG.info("About to release index recovery lock");
                    indexRecoveryLock.release();
                    LOG.info("successfully released index recovery lock");
                }
            } catch (Exception e) {
                LOG.error("Error while releasing a lock of index recovery " + e.getMessage(), e);
                //Do not throw exception as it will terminate continuous while loop
            }
        }
    }

    @VisibleForTesting
    static class RecoveryInfoManagement {
        private static final String INDEX_RECOVERY_TYPE_NAME = "__solrIndexRecoveryInfo";

        private final AtlasGraph graph;

        public RecoveryInfoManagement(AtlasGraph graph) {
            this.graph = graph;
        }

        public void updateStartTime(Long startTime) {
            try {
                Long        prevStartTime = null;
                AtlasVertex vertex        = findVertex();

                if (vertex == null) {
                    vertex = graph.addVertex();
                } else {
                    prevStartTime = getStartTime(vertex);
                }

                setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_NAME, INDEX_RECOVERY_TYPE_NAME);
                setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_START_TIME, startTime);
                setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, prevStartTime);

            } catch (Exception ex) {
                LOG.error("Error: Updating: {}!", ex);
            } finally {
                graph.commit();
            }
        }

        public Long getStartTime() {
            AtlasVertex vertex = findVertex();

            return getStartTime(vertex);
        }

        private Long getStartTime(AtlasVertex vertex) {
            if (vertex == null) {
                LOG.warn("Vertex passed is NULL: Returned is 0");

                return 0L;
            }

            Long startTime = 0L;

            try {
                startTime = vertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
            } catch (Exception e) {
                LOG.error("Error retrieving startTime", e);
            }

            return startTime;
        }

        private AtlasVertex findVertex() {
            AtlasGraphQuery       query   = graph.query().has(PROPERTY_KEY_INDEX_RECOVERY_NAME, INDEX_RECOVERY_TYPE_NAME);
            Iterator<AtlasVertex> results = query.vertices().iterator();

            return results.hasNext() ? results.next() : null;
        }
    }
}