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
package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.ApplicationProperties.DEFAULT_INDEX_RECOVERY;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_NAME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_START_TIME;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;

@Component
@Order(8)
public class IndexRecoveryService implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IndexRecoveryService.class);

    private static final String DATE_FORMAT                               = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String INDEX_HEALTH_MONITOR_THREAD_NAME          = "index-health-monitor";
    private static final String SOLR_STATUS_CHECK_RETRY_INTERVAL          = "atlas.graph.index.status.check.frequency";
    private static final String SOLR_INDEX_RECOVERY_CONFIGURED_START_TIME = "atlas.index.recovery.start.time";
    private static final long   SOLR_STATUS_RETRY_DEFAULT_MS              = 30000; // 30 secs default

    public final  RecoveryInfoManagement recoveryInfoManagement;
    public        RecoveryThread         recoveryThread;

    private final Thread        indexHealthMonitor;
    private final Configuration configuration;
    private final boolean       isIndexRecoveryEnabled;

    @Inject
    public IndexRecoveryService(Configuration config, AtlasGraph graph) {
        this.configuration          = config;
        this.isIndexRecoveryEnabled = config.getBoolean(ApplicationProperties.INDEX_RECOVERY_CONF, DEFAULT_INDEX_RECOVERY);

        long recoveryStartTimeFromConfig = getRecoveryStartTimeFromConfig(config);
        long healthCheckFrequencyMillis  = config.getLong(SOLR_STATUS_CHECK_RETRY_INTERVAL, SOLR_STATUS_RETRY_DEFAULT_MS);

        this.recoveryInfoManagement = new RecoveryInfoManagement(graph);
        this.recoveryThread         = new RecoveryThread(recoveryInfoManagement, graph, recoveryStartTimeFromConfig, healthCheckFrequencyMillis);
        this.indexHealthMonitor     = new Thread(recoveryThread, INDEX_HEALTH_MONITOR_THREAD_NAME);
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
        LOG.info("==> IndexRecoveryService.instanceIsPassive()");

        stop();

        LOG.info("<== IndexRecoveryService.instanceIsPassive()");
    }

    @Override
    public int getHandlerOrder() {
        return ActiveStateChangeHandler.HandlerOrder.INDEX_RECOVERY.getOrder();
    }

    private long getRecoveryStartTimeFromConfig(Configuration config) {
        long ret = 0L;

        try {
            String time = config.getString(SOLR_INDEX_RECOVERY_CONFIGURED_START_TIME);
            if (StringUtils.isNotBlank(time)) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
                dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

                ret = dateFormat.parse(time).toInstant().toEpochMilli();
            }
        } catch (Exception e) {
            LOG.error("Error fetching: {}", SOLR_INDEX_RECOVERY_CONFIGURED_START_TIME, e);
        }

        return ret;
    }

    private void startTxLogMonitoring() {
        if (!isIndexRecoveryEnabled) {
            LOG.warn("IndexRecoveryService: Recovery should be enabled.");

            return;
        }

        try {
            if (indexHealthMonitor.getState() == Thread.State.NEW) {
                indexHealthMonitor.start();
            }
        } catch (Exception ex) {
            LOG.error("Error while starting Index Health Monitor", ex);
        }
    }

    public static class RecoveryThread implements Runnable {
        private final AtlasGraph             graph;
        private final RecoveryInfoManagement recoveryInfoManagement;
        private final AtomicBoolean          shouldRun = new AtomicBoolean(false);
        private final long                   indexStatusCheckRetryMillis;
        private       Object                 txRecoveryObject;

        private RecoveryThread(RecoveryInfoManagement recoveryInfoManagement, AtlasGraph graph, long startTimeFromConfig, long healthCheckFrequencyMillis) {
            this.graph                       = graph;
            this.recoveryInfoManagement      = recoveryInfoManagement;
            this.indexStatusCheckRetryMillis = healthCheckFrequencyMillis;

            if (startTimeFromConfig > 0) {
                this.recoveryInfoManagement.updateStartTime(startTimeFromConfig);
            }
        }

        public void run() {
            shouldRun.set(true);

            LOG.info("Index Health Monitor: Starting...");

            while (shouldRun.get()) {
                try {
                    boolean isIdxHealthy = waitAndCheckIfIndexBackendHealthy();

                    if (this.txRecoveryObject == null && isIdxHealthy) {
                        startMonitoring();
                    }

                    if (this.txRecoveryObject != null && !isIdxHealthy) {
                        stopMonitoring();
                    }
                } catch (Exception e) {
                    LOG.error("Error: Index recovery monitoring!", e);
                }
            }
        }

        public void shutdown() {
            try {
                LOG.info("Index Health Monitor: Shutdown: Starting...");

                // handle the case where thread was not started at all
                // and shutdown called
                if (!shouldRun.get()) {
                    return;
                }

                shouldRun.set(false);
            } finally {
                LOG.info("Index Health Monitor: Shutdown: Done!");
            }
        }

        public boolean isIndexBackendHealthy() throws AtlasException {
            return this.graph.getGraphIndexClient().isHealthy();
        }

        public void startMonitoringByUserRequest(Long startTime) {
            startMonitoring(startTime);
        }

        public void stopMonitoringByUserRequest() {
            stopIndexRecovery();

            LOG.info("Index Recovery: Stopped!");
        }

        private boolean waitAndCheckIfIndexBackendHealthy() throws AtlasException, InterruptedException {
            Thread.sleep(indexStatusCheckRetryMillis);

            return isIndexBackendHealthy();
        }

        private void startMonitoring() {
            startMonitoring(recoveryInfoManagement.getStartTime());
        }

        private void startMonitoring(Long startTime) {
            if (startTime == null || startTime == 0L) {
                LOG.error("Index Recovery requested without start time");

                return;
            }

            try {
                txRecoveryObject = this.graph.getManagementSystem().startIndexRecovery(startTime);

                printIndexRecoveryStats();

                LOG.info("Index Recovery: Started! Recovery time: {}", Instant.ofEpochMilli(startTime));
            } catch (Exception e) {
                LOG.error("Index Recovery with recovery time: {} failed", Instant.ofEpochMilli(startTime), e);
            }
        }

        private void stopMonitoring() {
            stopIndexRecoveryAndUpdateStartTime();
        }

        private void stopIndexRecoveryAndUpdateStartTime() {
            Instant newStartTime = Instant.now().minusMillis(2 * indexStatusCheckRetryMillis);

            stopIndexRecovery();

            recoveryInfoManagement.updateStartTime(newStartTime.toEpochMilli());

            LOG.info("Index Recovery: Stopped! Recovery time: {}", newStartTime);
        }

        private void stopIndexRecovery() {
            try {
                this.graph.getManagementSystem().stopIndexRecovery(txRecoveryObject);

                printIndexRecoveryStats();
            } catch (Exception e) {
                LOG.info("Index Recovery: Stopped! Error!", e);
            } finally {
                this.txRecoveryObject = null;
            }
        }

        private void printIndexRecoveryStats() {
            this.graph.getManagementSystem().printIndexRecoveryStats(txRecoveryObject);
        }
    }

    @VisibleForTesting
    public static class RecoveryInfoManagement {
        private static final String INDEX_RECOVERY_TYPE_NAME = "__solrIndexRecoveryInfo";

        private final AtlasGraph graph;

        public RecoveryInfoManagement(AtlasGraph graph) {
            this.graph = graph;
        }

        public void updateStartTime(long time) {
            updateIndexRecoveryTime(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, time);
        }

        public void updateCustomStartTime(long time) {
            updateIndexRecoveryTime(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, time);
        }

        public void updateIndexRecoveryTime(String timePropertyKey, long time) {
            Map<String, String> indexRecoveryData = new HashMap<>();

            indexRecoveryData.put(timePropertyKey, String.valueOf(time));

            updateIndexRecoveryData(indexRecoveryData);
        }

        public void updateIndexRecoveryData(Map<String, String> indexRecoveryData) {
            try {
                Long        startTime          = NumberUtils.createLong(indexRecoveryData.get(PROPERTY_KEY_INDEX_RECOVERY_START_TIME));
                Long        prevStartTime      = NumberUtils.createLong(indexRecoveryData.get(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME));
                Long        customStartTime    = NumberUtils.createLong(indexRecoveryData.get(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME));
                boolean     isStartTimeUpdated = startTime != null;
                AtlasVertex vertex             = findVertex();

                if (vertex == null) {
                    vertex = graph.addVertex();

                    setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_NAME, INDEX_RECOVERY_TYPE_NAME);
                } else {
                    prevStartTime = isStartTimeUpdated ? getStartTime(vertex) : prevStartTime;
                }

                if (startTime != null) {
                    setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_START_TIME, startTime);
                }

                if (prevStartTime != null) {
                    setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, prevStartTime);
                }

                if (customStartTime != null) {
                    setEncodedProperty(vertex, PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, customStartTime);
                }
            } catch (Exception ex) {
                LOG.error("Error updating index recovery data", ex);
            } finally {
                graph.commit();
            }
        }

        public Long getStartTime() {
            AtlasVertex vertex = findVertex();

            return getStartTime(vertex);
        }

        public AtlasVertex findVertex() {
            AtlasGraphQuery       query   = graph.query().has(PROPERTY_KEY_INDEX_RECOVERY_NAME, INDEX_RECOVERY_TYPE_NAME);
            Iterator<AtlasVertex> results = query.vertices().iterator();

            return results.hasNext() ? results.next() : null;
        }

        private Long getStartTime(AtlasVertex vertex) {
            Long defaultStartTime = getStartTimeByTxLogTTL();

            if (vertex == null) {
                LOG.warn("Vertex passed is NULL: Returned is startTime by TTL {}", Instant.ofEpochMilli(defaultStartTime));

                return defaultStartTime;
            }

            Long startTime = 0L;

            try {
                startTime = vertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
            } catch (Exception e) {
                LOG.error("Error retrieving startTime", e);
            }

            return startTime == null || startTime == 0L ? defaultStartTime : startTime;
        }

        private Long getStartTimeByTxLogTTL() {
            long ttl = AtlasConfiguration.SOLR_INDEX_TX_LOG_TTL_CONF.getLong();
            return Instant.now().minus(ttl, ChronoUnit.HOURS).toEpochMilli();
        }
    }
}
