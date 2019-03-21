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
package org.apache.atlas.model.metrics;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Atlas statistics
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasNotificationMetrics {

    public static final String STAT_SERVER_START_TS                = "serverStartTimeStamp";
    public static final String STAT_SERVER_ACTIVE_TS               = "serverActiveTimeStamp";
    public static final String STAT_SERVER_UP_SINCE                = "serverUpTime";
    public static final String STAT_START_OFFSET                   = "KafkaTopic:ATLAS_HOOK:startOffset";
    public static final String STAT_CURRENT_OFFSET                 = "KafkaTopic:ATLAS_HOOK:currentOffset";
    public static final String STAT_SOLR_STATUS                    = "solrConnectionStatus";
    public static final String STAT_HBASE_STATUS                   = "HBaseConnectionStatus";
    public static final String STAT_LAST_MESSAGE_PROCESSED_TIME_TS = "lastMessageProcessedTimeStamp";
    public static final String STAT_AVG_MESSAGE_PROCESSING_TIME    = "avgMessageProcessingTime";
    public static final String TOTAL_MSG_COUNT                     = "TOTAL_MSG_COUNT";

    private static final int  METRIC_WINDOW_SIZE                   = 48;

    private List<Map<MetricsCounterType, Integer>> metricWindow;
    private Map<String, Object> data = new HashMap<>();
    private long[] timestamp;

    private int totalSinceLastStart = 0;

    public enum MetricsCounterType {
        NOTIFICATION_PROCESSED,
        NOTIFICATION_FAILED,
        ENTITY_CREATED,
        ENTITY_UPDATED,
        ENTITY_DELETE,
    }

    public enum MetricsTimeType {
        THIS_HOUR,
        PAST_HOUR,
        TODAY,
        YESTERDAY,
    }

    public AtlasNotificationMetrics() {
        initTotalCount();
        initMetricWindow();
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "AtlasNotificationMetrics{" + "data=" + data + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasNotificationMetrics other = (AtlasNotificationMetrics) o;
        return Objects.equals(this.data, other.data);
    }

    private void initMetricWindow() {
        metricWindow = new ArrayList<>(METRIC_WINDOW_SIZE);
        timestamp = new long[METRIC_WINDOW_SIZE];
        initSubMetricWindow(metricWindow);
    }

    private void initSubMetricWindow(List<Map<MetricsCounterType, Integer>> metricWindow) {
        for (int i = 0; i < METRIC_WINDOW_SIZE; i++) {
            metricWindow.add(i, new HashMap<>());

            for (MetricsCounterType metricType : MetricsCounterType.values()) {
                metricWindow.get(i).put(metricType, 0);
            }
        }
    }

    public void resetMetricWindow(int index) {
        for (AtlasNotificationMetrics.MetricsCounterType metricType : AtlasNotificationMetrics.MetricsCounterType.values()) {
            metricWindow.get(index).put(metricType, 0);
        }
    }

    public void clearMetricWindow(int index) {
        metricWindow.get(index).clear();
    }

    public void updateMetricWindow(int index, MetricsCounterType type, int val ) {
        metricWindow.get(index).put(type, val);
    }

    public int getMetricWindowTypeVal(int index, MetricsCounterType type) {
        return metricWindow.get(index).get(type);
    }

    public Map<MetricsCounterType, Integer> getMetricWindowMapByIndex(int index) {
        return metricWindow.get(index);
    }

    public void setTimestamp(int index, long time) {
        timestamp[index] = time;
    }

    public long getTimestamp(int index) {
        return timestamp[index];
    }

    public int getTotalSinceLastStart() {
        return totalSinceLastStart;
    }

    public void increaseTotalCount() {
        totalSinceLastStart++;
    }

    private void initTotalCount() {
        totalSinceLastStart = 0;
    }
}
