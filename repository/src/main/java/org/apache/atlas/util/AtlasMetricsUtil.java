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
package org.apache.atlas.util;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.metrics.AtlasNotificationMetrics;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_SERVER_START_TS;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_SERVER_ACTIVE_TS;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_SERVER_UP_SINCE;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_START_OFFSET;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_CURRENT_OFFSET;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_SOLR_STATUS;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_HBASE_STATUS;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_LAST_MESSAGE_PROCESSED_TIME_TS;
import static org.apache.atlas.model.metrics.AtlasNotificationMetrics.STAT_AVG_MESSAGE_PROCESSING_TIME;

@Component
public class AtlasMetricsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasMetricsUtil.class);

    private static final SimpleDateFormat simpleDateFormat   = new SimpleDateFormat("d MMM, yyyy : hh:mm aaa z");

    private static final long TWO_DAYS_MS           = 1000 * 60 * 60 * 24 * 2;
    private static final long DAY_MS                = 1000 * 60 * 60 * 24;
    private static final long TWO_HOURS_MS          = 1000 * 60 * 60 * 2;
    private static final long HOUR_MS               = 1000 * 60 * 60;
    private static final long MIN_MS                = 1000 * 60;
    private static final long SEC_MS                = 1000;
    private static final int  METRIC_WINDOW_SIZE    = 48;

    private final AtlasGraph graph;
    private final String STATUS_CONNECTED     = "connected";
    private final String STATUS_NOT_CONNECTED = "not-connected";
    private final AtlasNotificationMetrics atlasNotificationMetrics;

    private long countMsgProcessed        = 0;
    private long totalMsgProcessingTimeMs = 0;

    @Inject
    public AtlasMetricsUtil(AtlasGraph graph) {
        this.graph = graph;
        this.atlasNotificationMetrics = new AtlasNotificationMetrics();
    }

    public Map<String, Object> getAtlasMetrics() {
        Map<String, Object> statisticsMap = new HashMap<>();
        statisticsMap.putAll(atlasNotificationMetrics.getData());

        statisticsMap.put(STAT_HBASE_STATUS, getHBaseStatus());
        statisticsMap.put(STAT_SOLR_STATUS , getSolrStatus());
        statisticsMap.put(STAT_SERVER_UP_SINCE, getUpSinceTime());
        formatStatistics(statisticsMap);

        return statisticsMap;
    }

    public void setKafkaOffsets(long value){
        if (Long.parseLong(getStat(STAT_START_OFFSET).toString()) == -1) {
            addStat(STAT_START_OFFSET, value);
        }
        addStat(STAT_CURRENT_OFFSET, ++value);
    }

    public void setAvgMsgProcessingTime(long value) {
        countMsgProcessed++;
        totalMsgProcessingTimeMs += value;
        value = totalMsgProcessingTimeMs / countMsgProcessed;

        addStat(STAT_AVG_MESSAGE_PROCESSING_TIME, value);
    }

    public void setLastMsgProcessedTime() {
        addStat(STAT_LAST_MESSAGE_PROCESSED_TIME_TS, System.currentTimeMillis());
    }

    public void setServerStartTime() {
        addStat(STAT_SERVER_START_TS, System.currentTimeMillis());
    }

    public void setServerActiveTime() {
        addStat(STAT_SERVER_ACTIVE_TS, System.currentTimeMillis());
    }


    private void addStat(String key, Object value) {
        Map<String, Object> data = atlasNotificationMetrics.getData();
        if (data == null) {
            data = new HashMap<>();
        }
        data.put(key, value);
        atlasNotificationMetrics.setData(data);
    }

    private Object getStat(String key) {
        Map<String, Object> data = atlasNotificationMetrics.getData();
        Object ret = data.get(key);
        if (ret == null) {
            return -1;
        }
        return ret;
    }

    private void formatStatistics(Map<String, Object> statisticsMap) {
        for (Map.Entry<String, Object> stat : statisticsMap.entrySet()) {
            switch (stat.getKey()) {
                case STAT_SERVER_UP_SINCE:
                    statisticsMap.put(stat.getKey(), millisToTimeDiff(Long.parseLong(stat.getValue().toString())));
                    break;

                case STAT_LAST_MESSAGE_PROCESSED_TIME_TS:
                    statisticsMap.put(stat.getKey(), millisToTimeStamp(Long.parseLong(stat.getValue().toString())));
                    break;

                case STAT_SERVER_START_TS:
                case STAT_SERVER_ACTIVE_TS:
                    statisticsMap.put(stat.getKey(), millisToTimeStamp(Long.parseLong(stat.getValue().toString())));
                    break;

                case STAT_AVG_MESSAGE_PROCESSING_TIME:
                    statisticsMap.put(stat.getKey(), stat.getValue() + " milliseconds");
                    break;

                case STAT_HBASE_STATUS:
                case STAT_SOLR_STATUS:
                    String curState = ((boolean) stat.getValue()) ? STATUS_CONNECTED : STATUS_NOT_CONNECTED;
                    statisticsMap.put(stat.getKey(), curState);
                    break;

                default:
                    statisticsMap.put(stat.getKey(), stat.getValue());
            }
        }
    }

    private boolean getHBaseStatus(){

        String query = "g.V().next()";
        try {
            runWithTimeout(new Runnable() {
                @Override
                public void run() {
                    try {
                        graph.executeGremlinScript(query, false);
                    } catch (AtlasBaseException e) {
                        LOG.error(e.getMessage());
                    }
                }
            }, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean getSolrStatus(){
        String query = AtlasGraphUtilsV2.getIndexSearchPrefix() + "\"" + "__type.name\"" + " : (*)";
        try {
            runWithTimeout(new Runnable() {
                @Override
                public void run() {
                        graph.indexQuery(Constants.VERTEX_INDEX, query).vertexTotals();
                }
            }, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }
        return true;
    }

    private void runWithTimeout(final Runnable runnable, long timeout, TimeUnit timeUnit) throws Exception {
        runWithTimeout(new Callable<Object>() {
            @Override
            public Object call() {
                runnable.run();
                return null;
            }
        }, timeout, timeUnit);
    }

    private <T> T runWithTimeout(Callable<T> callable, long timeout, TimeUnit timeUnit) throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<T> future = executor.submit(callable);
        executor.shutdown();
        try {
            return future.get(timeout, timeUnit);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof Exception) {
                throw (Exception) t;
            } else {
                throw new IllegalStateException(t);
            }
        }
    }

    private long getUpSinceTime() {
        long upTS = Long.parseLong(getStat(STAT_SERVER_START_TS).toString());
        return System.currentTimeMillis() - upTS;
    }

    private String millisToTimeDiff(long msDiff) {
        StringBuilder sb = new StringBuilder();

        long diffSeconds = msDiff / SEC_MS % 60;
        long diffMinutes = msDiff / MIN_MS % 60;
        long diffHours = msDiff / HOUR_MS % 24;
        long diffDays = msDiff / DAY_MS;

        if (diffDays > 0) sb.append(diffDays).append(" day ");
        if (diffHours > 0) sb.append(diffHours).append(" hour ");
        if (diffMinutes > 0) sb.append(diffMinutes).append(" min ");
        if (diffSeconds > 0) sb.append(diffSeconds).append(" sec");

        return sb.toString();
    }

    private String millisToTimeStamp(long ms) {
        return simpleDateFormat.format(ms);
    }

    public void onNotificationProcessingComplete(HookNotification.HookNotificationType type, boolean isFailedMsg, long timestamp) {
        if (!isFailedMsg) {
            // type related notification will be ignored.
            switch (type) {
                case ENTITY_CREATE:
                case ENTITY_CREATE_V2: {
                    updateMetricWindow(AtlasNotificationMetrics.MetricsCounterType.ENTITY_CREATED, timestamp);
                }
                break;

                case ENTITY_FULL_UPDATE:
                case ENTITY_FULL_UPDATE_V2:
                case ENTITY_PARTIAL_UPDATE:
                case ENTITY_PARTIAL_UPDATE_V2: {
                    updateMetricWindow(AtlasNotificationMetrics.MetricsCounterType.ENTITY_UPDATED, timestamp);
                }
                break;

                case ENTITY_DELETE:
                case ENTITY_DELETE_V2: {
                    updateMetricWindow(AtlasNotificationMetrics.MetricsCounterType.ENTITY_DELETE, timestamp);
                }
                break;
            }
        } else {
            updateMetricWindow(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_FAILED, timestamp);
        }
    }

    private void updateMetricWindow(AtlasNotificationMetrics.MetricsCounterType counterType, long timestamp) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updateMetricWindow with type : ({}) at ({}) =========>", counterType.name(), millisToTimeDiff(timestamp));
        }

        long timestampToHours = timestamp / HOUR_MS;
        int curIndex = (int) timestampToHours % METRIC_WINDOW_SIZE;

        if (atlasNotificationMetrics.getTimestamp(curIndex) != timestampToHours) {
            atlasNotificationMetrics.setTimestamp(curIndex, timestampToHours);
            atlasNotificationMetrics.clearMetricWindow(curIndex);
            atlasNotificationMetrics.resetMetricWindow(curIndex);
            atlasNotificationMetrics.updateMetricWindow(curIndex, counterType, 1);
        } else {
            int existingVal = atlasNotificationMetrics.getMetricWindowTypeVal(curIndex, counterType);
            atlasNotificationMetrics.updateMetricWindow(curIndex, counterType, existingVal + 1);
        }
        atlasNotificationMetrics.increaseTotalCount();
    }

    public Map<Object, Map<Object, Object>> getMetrics(long timestamp) {

        LOG.info(" ========> getMetricWindow at ({}) ", millisToTimeDiff(timestamp));

        Map<Object, Map<Object, Object>> resMap = new HashMap<>();
        initSubMap(resMap);

        long diff;
        int notificationCountForLastHour  = 0, notificationCountForThisHour = 0,
            notificationCountForYesterday = 0, notificationCountForToday    = 0;

        for (int i = 0; i < METRIC_WINDOW_SIZE; i++) {
            long windowTimestamp = atlasNotificationMetrics.getTimestamp(i);
            if (windowTimestamp == 0) continue;
            try {
                diff = timestamp - windowTimestamp * HOUR_MS;
                if (diff > DAY_MS && diff < TWO_DAYS_MS) {
                    for (Map.Entry<AtlasNotificationMetrics.MetricsCounterType, Integer> entry : atlasNotificationMetrics.getMetricWindowMapByIndex(i).entrySet()) {
                        int existingVal = (int) resMap.get(AtlasNotificationMetrics.MetricsTimeType.YESTERDAY).get(entry.getKey());
                        resMap.get(AtlasNotificationMetrics.MetricsTimeType.YESTERDAY).put(entry.getKey(), entry.getValue() + existingVal);
                        if (!entry.getKey().equals(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_FAILED)) {
                            notificationCountForYesterday = notificationCountForYesterday + entry.getValue();
                        }
                    }
                } else if (diff <= DAY_MS) {
                    for (Map.Entry<AtlasNotificationMetrics.MetricsCounterType, Integer> entry : atlasNotificationMetrics.getMetricWindowMapByIndex(i).entrySet()) {
                        int existingVal = (int) resMap.get(AtlasNotificationMetrics.MetricsTimeType.TODAY).get(entry.getKey());
                        resMap.get(AtlasNotificationMetrics.MetricsTimeType.TODAY).put(entry.getKey(), entry.getValue() + existingVal);
                        if (!entry.getKey().equals(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_FAILED)) {
                            notificationCountForToday = notificationCountForToday + entry.getValue();
                        }
                    }
                }

                if (diff <= TWO_HOURS_MS && diff > HOUR_MS) {
                    for (Map.Entry<AtlasNotificationMetrics.MetricsCounterType, Integer> entry : atlasNotificationMetrics.getMetricWindowMapByIndex(i).entrySet()) {
                        int existingVal = (int) resMap.get(AtlasNotificationMetrics.MetricsTimeType.PAST_HOUR).get(entry.getKey());
                        resMap.get(AtlasNotificationMetrics.MetricsTimeType.PAST_HOUR).put(entry.getKey(), entry.getValue() + existingVal);
                        if (!entry.getKey().equals(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_FAILED)) {
                            notificationCountForLastHour = notificationCountForLastHour + entry.getValue();
                        }
                    }
                } else if (diff <= HOUR_MS) {
                    for (Map.Entry<AtlasNotificationMetrics.MetricsCounterType, Integer> entry : atlasNotificationMetrics.getMetricWindowMapByIndex(i).entrySet()) {
                        int existingVal = (int) resMap.get(AtlasNotificationMetrics.MetricsTimeType.THIS_HOUR).get(entry.getKey());
                        resMap.get(AtlasNotificationMetrics.MetricsTimeType.THIS_HOUR).put(entry.getKey(), entry.getValue() + existingVal);
                        if (!entry.getKey().equals(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_FAILED)) {
                            notificationCountForThisHour = notificationCountForThisHour + entry.getValue();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.info("Error when getting NotificationMetrics, ", e);
            }
        }

        for (AtlasNotificationMetrics.MetricsTimeType type : AtlasNotificationMetrics.MetricsTimeType.values()) {
            switch (type) {
                case TODAY:
                    resMap.get(AtlasNotificationMetrics.MetricsTimeType.TODAY).put(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_PROCESSED, notificationCountForToday);
                break;

                case PAST_HOUR:
                    resMap.get(AtlasNotificationMetrics.MetricsTimeType.PAST_HOUR).put(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_PROCESSED, notificationCountForLastHour);
                break;

                case THIS_HOUR:
                    resMap.get(AtlasNotificationMetrics.MetricsTimeType.THIS_HOUR).put(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_PROCESSED, notificationCountForThisHour);
                break;

                case YESTERDAY:
                    resMap.get(AtlasNotificationMetrics.MetricsTimeType.YESTERDAY).put(AtlasNotificationMetrics.MetricsCounterType.NOTIFICATION_PROCESSED, notificationCountForYesterday);
                break;
            }
            resMap.put(AtlasNotificationMetrics.TOTAL_MSG_COUNT, Collections.singletonMap(AtlasNotificationMetrics.TOTAL_MSG_COUNT, atlasNotificationMetrics.getTotalSinceLastStart()));

        }

        LOG.info(" getMetricWindow <======== ");

        return resMap;
    }

    private void initSubMap(Map<Object, Map<Object, Object>> map) {
        for (AtlasNotificationMetrics.MetricsTimeType timeType : AtlasNotificationMetrics.MetricsTimeType.values()) {
            map.put(timeType, new HashMap<>());
            for (AtlasNotificationMetrics.MetricsCounterType metricType : AtlasNotificationMetrics.MetricsCounterType.values()) {
                map.get(timeType).put(metricType, 0);
            }
        }
    }
}
