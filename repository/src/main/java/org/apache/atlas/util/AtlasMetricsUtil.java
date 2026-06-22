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

import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.util.AtlasMetricsCounter.StatsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_AVG_PROCESSING_TIME_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_GLOBAL_ENTITY_TYPE_COUNTS_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_GLOBAL_FAILED_ENTITY_TYPE_COUNTS_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_INPUT_TOPIC_STATS;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_MESSAGES_FAILED_BY_TOPIC;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_MESSAGES_PRODUCED_BY_TOPIC;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_AVG_TIME_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_AVG_TIME_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_AVG_TIME_PREV_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_AVG_TIME_PREV_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_AVG_TIME_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_COUNT_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_COUNT_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_COUNT_PREV_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_COUNT_PREV_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_COUNT_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_CREATES_COUNT_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_CREATES_COUNT_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_CREATES_COUNT_PREV_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_CREATES_COUNT_PREV_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_CREATES_COUNT_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_DELETES_COUNT_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_DELETES_COUNT_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_DELETES_COUNT_PREV_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_DELETES_COUNT_PREV_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_DELETES_COUNT_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_FAILED_COUNT_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_FAILED_COUNT_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_FAILED_COUNT_PREV_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_FAILED_COUNT_PREV_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_FAILED_COUNT_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_LAST_MESSAGE_PROCESSED_TIME;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_START_TIME_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_START_TIME_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_TOPIC_DETAILS;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_UPDATES_COUNT_CURR_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_UPDATES_COUNT_CURR_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_UPDATES_COUNT_PREV_DAY;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_UPDATES_COUNT_PREV_HOUR;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_NOTIFY_UPDATES_COUNT_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_OUTPUT_TOPIC_STATS;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_SERVER_ACTIVE_TIMESTAMP;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_SERVER_START_TIMESTAMP;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_SERVER_STATUS_BACKEND_STORE;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_SERVER_STATUS_INDEX_STORE;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_SERVER_UP_TIME;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_TOTAL_MESSAGES_CONSUMED_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_TOTAL_MESSAGES_FAILED_TOTAL;
import static org.apache.atlas.model.metrics.AtlasMetrics.STAT_TOTAL_MESSAGES_PROCESSED_TOTAL;
import static org.apache.atlas.repository.Constants.TYPE_NAME_INTERNAL;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.apache.atlas.util.AtlasMetricsCounter.Period.ALL;
import static org.apache.atlas.util.AtlasMetricsCounter.Period.CURR_DAY;
import static org.apache.atlas.util.AtlasMetricsCounter.Period.CURR_HOUR;
import static org.apache.atlas.util.AtlasMetricsCounter.Period.PREV_DAY;
import static org.apache.atlas.util.AtlasMetricsCounter.Period.PREV_HOUR;

@Component
public class AtlasMetricsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasMetricsUtil.class);

    private static final long   SEC_MS               = 1000;
    private static final long   MIN_MS               = 60 * SEC_MS;
    private static final long   HOUR_MS              = 60 * MIN_MS;
    private static final long   DAY_MS               = 24 * HOUR_MS;
    private static final String STATUS_CONNECTED     = "connected";
    private static final String STATUS_NOT_CONNECTED = "not-connected";

    private final AtlasGraph              graph;
    private final Map<String, TopicStats> topicStats        = new HashMap<>();
    private final AtlasMetricsCounter     messagesProcessed = new AtlasMetricsCounter("messagesProcessed");
    private final AtlasMetricsCounter     messagesFailed    = new AtlasMetricsCounter("messagesFailed");
    private final AtlasMetricsCounter     entityCreates     = new AtlasMetricsCounter("entityCreates");
    private final AtlasMetricsCounter     entityUpdates     = new AtlasMetricsCounter("entityUpdates");
    private final AtlasMetricsCounter     entityDeletes     = new AtlasMetricsCounter("entityDeletes");

    private final AtlasMetricsCounter npMessagesConsumed  = new AtlasMetricsCounter("notificationProcessor.messagesConsumed");
    private final AtlasMetricsCounter npMessagesProcessed = new AtlasMetricsCounter("notificationProcessor.messagesProcessed");
    private final AtlasMetricsCounter npMessagesFailed    = new AtlasMetricsCounter("notificationProcessor.messagesFailed");
    private final AtlasMetricsCounter npMessagesRouted    = new AtlasMetricsCounter("notificationProcessor.messagesRouted");

    private final AtlasMetricsCounter npEntitiesRouted     = new AtlasMetricsCounter("notificationProcessor.entitiesRouted");
    private final AtlasMetricsCounter npEntitiesFailed     = new AtlasMetricsCounter("notificationProcessor.entitiesFailed");
    private final AtlasMetricsCounter npPublishFailed      = new AtlasMetricsCounter("notificationProcessor.publishFailed");

    // Stats grouped by input topic (consumer2 input)
    private final Map<String, TopicStats> npInputTopicStats  = new ConcurrentHashMap<>();
    // Stats grouped by output/routed topic (consumer2 output)
    private final Map<String, TopicStats> npOutputTopicStats = new ConcurrentHashMap<>();

    private final Map<String, Long> globalEntityTypeCountsNP        = new ConcurrentHashMap<>();
    private final Map<String, Long> globalFailedEntityTypeCountsNP  = new ConcurrentHashMap<>();
    private final Map<String, Long> totalMessagesPublishedByTopicNP = new ConcurrentHashMap<>();
    private final Map<String, Long> totalFailedByTopicNP            = new ConcurrentHashMap<>();
    private       long              serverStartTime;
    private       long              serverActiveTime;

    @Inject
    public AtlasMetricsUtil(AtlasGraph graph) {
        this.graph = graph;
    }

    // visible only for testing
    public void init(Clock clock) {
        messagesProcessed.init(clock);
        messagesFailed.init(clock);
        entityCreates.init(clock);
        entityUpdates.init(clock);
        entityDeletes.init(clock);
    }

    public void onServerStart() {
        serverStartTime = System.currentTimeMillis();
    }

    public void onServerActivation() {
        serverActiveTime = System.currentTimeMillis();
    }

    public void onNotificationProcessingComplete(String topicName, int partition, long msgOffset, NotificationStat stats) {
        messagesProcessed.incrWithMeasure(stats.timeTakenMs);
        entityCreates.incrBy(stats.entityCreates);
        entityUpdates.incrBy(stats.entityUpdates);
        entityDeletes.incrBy(stats.entityDeletes);

        if (stats.isFailedMsg) {
            messagesFailed.incr();
        }

        TopicStats topicStat = topicStats.get(topicName);

        if (topicStat == null) {
            topicStat = new TopicStats(topicName);

            topicStats.put(topicName, topicStat);
        }

        TopicPartitionStat partitionStat = topicStat.get(partition);

        if (partitionStat == null) {
            partitionStat = new TopicPartitionStat(topicName, partition, msgOffset, msgOffset);

            topicStat.set(partition, partitionStat);
        }

        partitionStat.setCurrentOffset(msgOffset + 1);

        if (stats.isFailedMsg) {
            partitionStat.incrFailedMessageCount();
        }

        partitionStat.incrProcessedMessageCount(stats.timeTakenMs);
        partitionStat.setLastMessageProcessedTime(messagesProcessed.getLastIncrTime().toEpochMilli());
    }

    public void onNotificationProcessorComplete(String inputTopic, int partition, long msgOffset, NotificationProcessorStats stats) {
        // 1) Input topic stats
        TopicStats inStats = npInputTopicStats.computeIfAbsent(inputTopic, TopicStats::new);

        TopicPartitionStat inPart = inStats.getPartitionStats().computeIfAbsent(
                partition, p -> new TopicPartitionStat(inputTopic, p, msgOffset, msgOffset));

        inPart.updateOnSuccess(msgOffset, stats.getProcessingTimeMs());
        if (stats.isFailed()) {
            inPart.updateOnFailure(msgOffset);
        }

        stats.getEntityTypeCounts().forEach(inStats::incrEntityType);
        stats.getRoutedTopicCounts().forEach(inStats::incrRouted);
        stats.getFailedTopicCounts().forEach(inStats::incrFailedRouting);

        // failed entity types (map + counter totals)
        long failedEntitiesTotal = 0;
        for (Map.Entry<String, Long> e : stats.getFailedEntityTypeCounts().entrySet()) {
            String type = e.getKey();
            long count  = e.getValue() == null ? 0 : e.getValue();

            globalFailedEntityTypeCountsNP.merge(type, count, Long::sum);
            inStats.incrFailedRouting("entityType:" + type, count);

            failedEntitiesTotal += count;
        }
        if (failedEntitiesTotal > 0) {
            npEntitiesFailed.incrBy(failedEntitiesTotal);
        }

        // 2) Output topic stats (all routed topics)
        Set<String> allRoutedTopics = new HashSet<>();
        allRoutedTopics.addAll(stats.getRoutedTopicCounts().keySet());
        allRoutedTopics.addAll(stats.getFailedTopicCounts().keySet());

        for (String routedTopic : allRoutedTopics) {
            TopicStats outStats = npOutputTopicStats.computeIfAbsent(routedTopic, TopicStats::new);

            TopicPartitionStat outPart = outStats.getPartitionStats().computeIfAbsent(
                    partition, p -> new TopicPartitionStat(routedTopic, p, msgOffset, msgOffset));

            outPart.updateOnSuccess(msgOffset, 0); // no processing time for output topics
            outStats.incrReceivedFrom(inputTopic, 1);
        }

        // 3) Global totals + counters
        npMessagesConsumed.incrWithMeasure(stats.getProcessingTimeMs());
        npMessagesProcessed.incrWithMeasure(stats.getProcessingTimeMs());

        if (stats.isFailed()) {
            npMessagesFailed.incr();
        }

        // entity-type totals (map + counter totals)
        long routedEntitiesTotal = 0;
        for (Map.Entry<String, Long> e : stats.getEntityTypeCounts().entrySet()) {
            String type = e.getKey();
            long cnt    = e.getValue() == null ? 0 : e.getValue();

            globalEntityTypeCountsNP.merge(type, cnt, Long::sum);
            routedEntitiesTotal += cnt;
        }
        if (routedEntitiesTotal > 0) {
            npEntitiesRouted.incrBy(routedEntitiesTotal);
        }

        // publish success totals
        for (Map.Entry<String, Long> e : stats.getRoutedTopicCounts().entrySet()) {
            String outTopic = e.getKey();
            long cnt        = e.getValue() == null ? 0 : e.getValue();

            totalMessagesPublishedByTopicNP.merge(outTopic, cnt, Long::sum);
            npMessagesRouted.incrBy(cnt);
        }

        // publish failure totals (map + counter totals)
        long publishFailedTotal = 0;
        for (Map.Entry<String, Long> e : stats.getFailedTopicCounts().entrySet()) {
            String outTopic = e.getKey();
            long cnt        = e.getValue() == null ? 0 : e.getValue();

            totalFailedByTopicNP.merge(outTopic, cnt, Long::sum);
            publishFailedTotal += cnt;
        }
        if (publishFailedTotal > 0) {
            npPublishFailed.incrBy(publishFailedTotal);
        }
    }

    public Map<String, Object> getStats() {
        Map<String, Object> ret = new HashMap<>();

        StatsReport messagesProcessed   = this.messagesProcessed.report();
        StatsReport messagesFailed      = this.messagesFailed.report();
        StatsReport entityCreates       = this.entityCreates.report();
        StatsReport entityUpdates       = this.entityUpdates.report();
        StatsReport entityDeletes       = this.entityDeletes.report();
        StatsReport npMessagesProcessed = this.npMessagesProcessed.report();
        StatsReport npMessagesConsumed  = this.npMessagesConsumed.report();
        StatsReport npMessagesFailed    = this.npMessagesFailed.report();

        ret.put(STAT_SERVER_START_TIMESTAMP, serverStartTime);
        ret.put(STAT_SERVER_ACTIVE_TIMESTAMP, serverActiveTime);
        ret.put(STAT_SERVER_UP_TIME, millisToTimeDiff(System.currentTimeMillis() - serverStartTime));
        ret.put(STAT_SERVER_STATUS_BACKEND_STORE, getBackendStoreStatus() ? STATUS_CONNECTED : STATUS_NOT_CONNECTED);
        ret.put(STAT_SERVER_STATUS_INDEX_STORE, getIndexStoreStatus() ? STATUS_CONNECTED : STATUS_NOT_CONNECTED);

        Map<String, Map<String, Long>> topicDetails = new HashMap<>();

        for (TopicStats tStat : topicStats.values()) {
            for (TopicPartitionStat tpStat : tStat.partitionStats.values()) {
                Map<String, Long> tpDetails = new HashMap<>();

                tpDetails.put("offsetStart", tpStat.getStartOffset());
                tpDetails.put("offsetCurrent", tpStat.getCurrentOffset());
                tpDetails.put("failedMessageCount", tpStat.getFailedMessageCount());
                tpDetails.put("lastMessageProcessedTime", tpStat.getLastMessageProcessedTime());
                tpDetails.put("processedMessageCount", tpStat.getProcessedMessageCount());
                tpDetails.put("avgProcessingTime", tpStat.getAvgProcessingTime());

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting failedMessageCount : {} and lastMessageProcessedTime : {} for topic {}-{}", tpStat.getFailedMessageCount(), tpStat.getLastMessageProcessedTime(), tpStat.getTopicName(), tpStat.getPartition());
                }

                topicDetails.put(tpStat.getTopicName() + "-" + tpStat.getPartition(), tpDetails);
            }
        }

        ret.put(STAT_NOTIFY_TOPIC_DETAILS, topicDetails);
        ret.put(STAT_NOTIFY_LAST_MESSAGE_PROCESSED_TIME, this.messagesProcessed.getLastIncrTime().toEpochMilli());

        ret.put(STAT_NOTIFY_COUNT_TOTAL, messagesProcessed.getCount(ALL));
        ret.put(STAT_NOTIFY_AVG_TIME_TOTAL, messagesProcessed.getMeasureAvg(ALL));
        ret.put(STAT_NOTIFY_FAILED_COUNT_TOTAL, messagesFailed.getCount(ALL));
        ret.put(STAT_NOTIFY_CREATES_COUNT_TOTAL, entityCreates.getCount(ALL));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_TOTAL, entityUpdates.getCount(ALL));
        ret.put(STAT_NOTIFY_DELETES_COUNT_TOTAL, entityDeletes.getCount(ALL));

        ret.put(STAT_NOTIFY_START_TIME_CURR_DAY, messagesProcessed.getDayStartTimeMs());
        ret.put(STAT_NOTIFY_COUNT_CURR_DAY, messagesProcessed.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_AVG_TIME_CURR_DAY, messagesProcessed.getMeasureAvg(CURR_DAY));
        ret.put(STAT_NOTIFY_FAILED_COUNT_CURR_DAY, messagesFailed.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_CREATES_COUNT_CURR_DAY, entityCreates.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_CURR_DAY, entityUpdates.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_DELETES_COUNT_CURR_DAY, entityDeletes.getCount(CURR_DAY));

        ret.put(STAT_NOTIFY_START_TIME_CURR_HOUR, messagesProcessed.getHourStartTimeMs());
        ret.put(STAT_NOTIFY_COUNT_CURR_HOUR, messagesProcessed.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_AVG_TIME_CURR_HOUR, messagesProcessed.getMeasureAvg(CURR_HOUR));
        ret.put(STAT_NOTIFY_FAILED_COUNT_CURR_HOUR, messagesFailed.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_CREATES_COUNT_CURR_HOUR, entityCreates.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_CURR_HOUR, entityUpdates.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_DELETES_COUNT_CURR_HOUR, entityDeletes.getCount(CURR_HOUR));

        ret.put(STAT_NOTIFY_COUNT_PREV_HOUR, messagesProcessed.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_AVG_TIME_PREV_HOUR, messagesProcessed.getMeasureAvg(PREV_HOUR));
        ret.put(STAT_NOTIFY_FAILED_COUNT_PREV_HOUR, messagesFailed.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_CREATES_COUNT_PREV_HOUR, entityCreates.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_PREV_HOUR, entityUpdates.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_DELETES_COUNT_PREV_HOUR, entityDeletes.getCount(PREV_HOUR));

        ret.put(STAT_NOTIFY_COUNT_PREV_DAY, messagesProcessed.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_AVG_TIME_PREV_DAY, messagesProcessed.getMeasureAvg(PREV_DAY));
        ret.put(STAT_NOTIFY_FAILED_COUNT_PREV_DAY, messagesFailed.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_CREATES_COUNT_PREV_DAY, entityCreates.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_PREV_DAY, entityUpdates.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_DELETES_COUNT_PREV_DAY, entityDeletes.getCount(PREV_DAY));

        // Notification Processor stats
        ret.put(STAT_TOTAL_MESSAGES_CONSUMED_TOTAL, npMessagesConsumed.getCount(ALL));
        ret.put(STAT_TOTAL_MESSAGES_PROCESSED_TOTAL, npMessagesProcessed.getCount(ALL));
        ret.put(STAT_TOTAL_MESSAGES_FAILED_TOTAL, npMessagesFailed.getCount(ALL));
        ret.put(STAT_AVG_PROCESSING_TIME_TOTAL, npMessagesProcessed.getMeasureAvg(ALL));

        ret.put(STAT_GLOBAL_ENTITY_TYPE_COUNTS_TOTAL, new HashMap<>(globalEntityTypeCountsNP));
        ret.put(STAT_GLOBAL_FAILED_ENTITY_TYPE_COUNTS_TOTAL, new HashMap<>(globalFailedEntityTypeCountsNP));
        ret.put(STAT_MESSAGES_PRODUCED_BY_TOPIC, new HashMap<>(totalMessagesPublishedByTopicNP));
        ret.put(STAT_MESSAGES_FAILED_BY_TOPIC, new HashMap<>(totalFailedByTopicNP));
        ret.put(STAT_INPUT_TOPIC_STATS, new HashMap<>(npInputTopicStats));
        ret.put(STAT_OUTPUT_TOPIC_STATS, new HashMap<>(npOutputTopicStats));

        return ret;
    }

    public boolean isBackendStoreActive() {
        return getBackendStoreStatus();
    }

    public boolean isIndexStoreActive() {
        return getIndexStoreStatus();
    }

    private boolean getBackendStoreStatus() {
        try {
            runWithTimeout(() -> {
                graph.query().has(TYPE_NAME_PROPERTY_KEY, TYPE_NAME_INTERNAL).vertices(1);

                graphCommit();
            }, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(e.getMessage());

            graphRollback();

            return false;
        }

        return true;
    }

    private boolean getIndexStoreStatus() {
        final String query = AtlasGraphUtilsV2.getIndexSearchPrefix() + "\"" + Constants.TYPE_NAME_PROPERTY_KEY + "\":(" + TYPE_NAME_INTERNAL + ")";

        try {
            runWithTimeout(() -> {
                graph.indexQuery(Constants.VERTEX_INDEX, query).vertices(0, 1);

                graphCommit();
            }, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(e.getMessage());

            graphRollback();

            return false;
        }

        return true;
    }

    private void runWithTimeout(final Runnable runnable, long timeout, TimeUnit timeUnit) throws Exception {
        runWithTimeout(() -> {
            runnable.run();

            return null;
        }, timeout, timeUnit);
    }

    private <T> T runWithTimeout(Callable<T> callable, long timeout, TimeUnit timeUnit) throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<T>       future   = executor.submit(callable);

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

    private void graphCommit() {
        try {
            graph.commit();
        } catch (Exception ex) {
            LOG.warn("Graph transaction commit failed; attempting to rollback graph transaction.", ex);

            graphRollback();
        }
    }

    private void graphRollback() {
        try {
            graph.rollback();
        } catch (Exception ex) {
            LOG.warn("Graph transaction rollback failed", ex);
        }
    }

    private String millisToTimeDiff(long msDiff) {
        StringBuilder sb = new StringBuilder();

        long diffSeconds = msDiff / SEC_MS % 60;
        long diffMinutes = msDiff / MIN_MS % 60;
        long diffHours   = msDiff / HOUR_MS % 24;
        long diffDays    = msDiff / DAY_MS;

        if (diffDays > 0) {
            sb.append(diffDays).append(" day ");
        }

        if (diffHours > 0) {
            sb.append(diffHours).append(" hour ");
        }

        if (diffMinutes > 0) {
            sb.append(diffMinutes).append(" min ");
        }

        if (diffSeconds > 0) {
            sb.append(diffSeconds).append(" sec");
        }

        return sb.toString();
    }

    public static class NotificationStat {
        public boolean isFailedMsg;
        public long    timeTakenMs;
        public int     entityCreates;
        public int     entityUpdates;
        public int     entityDeletes;

        public NotificationStat() {}

        public NotificationStat(boolean isFailedMsg, long timeTakenMs) {
            this.isFailedMsg = isFailedMsg;
            this.timeTakenMs = timeTakenMs;
        }

        public void updateStats(EntityMutationResponse response) {
            entityCreates += getSize(response.getCreatedEntities());
            entityUpdates += getSize(response.getUpdatedEntities());
            entityUpdates += getSize(response.getPartialUpdatedEntities());
            entityDeletes += getSize(response.getDeletedEntities());
        }

        private int getSize(Collection<?> collection) {
            return collection != null ? collection.size() : 0;
        }
    }

    public static class NotificationProcessorStats {
        private boolean failed;
        private long    processingTimeMs;
        private long    offset;
        private int     partition;

        private final Map<String, Long> entityTypeCounts        = new HashMap<>();
        private final Map<String, Long> routedTopicCounts       = new HashMap<>();
        private final Map<String, Long> failedTopicCounts       = new HashMap<>();
        private final Map<String, Long> failedEntityTypeCounts  = new HashMap<>();

        public boolean isFailed() {
            return failed;
        }

        public void setFailed(boolean failed) {
            this.failed = failed;
        }

        public long getProcessingTimeMs() {
            return processingTimeMs;
        }

        public void setProcessingTimeMs(long processingTimeMs) {
            this.processingTimeMs = processingTimeMs;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public Map<String, Long> getEntityTypeCounts() {
            return entityTypeCounts;
        }

        public Map<String, Long> getRoutedTopicCounts() {
            return routedTopicCounts;
        }

        public Map<String, Long> getFailedTopicCounts() {
            return failedTopicCounts;
        }

        public Map<String, Long> getFailedEntityTypeCounts() {
            return failedEntityTypeCounts;
        }

        public void incrEntityType(String type) {
            entityTypeCounts.merge(type, 1L, Long::sum);
        }

        public void incrRoutedTopic(String topic) {
            routedTopicCounts.merge(topic, 1L, Long::sum);
        }

        public void incrFailedTopic(String topic) {
            failedTopicCounts.merge(topic, 1L, Long::sum);
        }

        public void incrFailedEntityType(String type) {
            failedEntityTypeCounts.merge(type, 1L, Long::sum);
        }
    }

    public static class TopicStats {
        private final String                              topicName;
        private final Map<Integer, TopicPartitionStat>     partitionStats = new HashMap<>();

        // processor-side maps
        private final Map<String, Long> entityTypeCounts             = new HashMap<>();
        private final Map<String, Long> routedMessagesPerOutputTopic = new HashMap<>();
        private final Map<String, Long> failedRoutingPerOutputTopic  = new HashMap<>();
        private final Map<String, Long> messagesFromInputTopic       = new HashMap<>();

        public TopicStats(String topicName) {
            this.topicName = topicName;
        }

        public String getTopicName() {
            return topicName;
        }

        public Map<Integer, TopicPartitionStat> getPartitionStats() {
            return partitionStats;
        }

        public TopicPartitionStat get(Integer partition) {
            return partitionStats.get(partition);
        }

        public void set(Integer partition, TopicPartitionStat partitionStat) {
            partitionStats.put(partition, partitionStat);
        }

        public void incrEntityType(String type, long cnt) {
            entityTypeCounts.merge(type, cnt, Long::sum);
        }

        public void incrRouted(String outputTopic, long cnt) {
            routedMessagesPerOutputTopic.merge(outputTopic, cnt, Long::sum);
        }

        public void incrFailedRouting(String outputTopic, long cnt) {
            failedRoutingPerOutputTopic.merge(outputTopic, cnt, Long::sum);
        }

        public void incrReceivedFrom(String inputTopic, long cnt) {
            messagesFromInputTopic.merge(inputTopic, cnt, Long::sum);
        }
    }

    // ---------------------------
    // MERGED TopicPartitionStat (consumer + processor)
    // NOTE: existing field names kept to preserve old getStats() logic
    // ---------------------------
    public static class TopicPartitionStat {
        private final String     topicName;
        private final int        partition;
        private final long       startOffset;
        private       long       currentOffset;
        private       long       lastMessageProcessedTime;
        private final AtomicLong failedMessageCount    = new AtomicLong();
        private final AtomicLong processedMessageCount = new AtomicLong();
        // processor additions
        private long lastFailedTime;
        private final AtomicLong totalProcessingTimeMs = new AtomicLong();

        public TopicPartitionStat(String topicName, int partition, long startOffset, long currentOffset) {
            this.topicName     = topicName;
            this.partition     = partition;
            this.startOffset   = startOffset;
            this.currentOffset = currentOffset;
        }

        public String getTopicName() {
            return topicName;
        }

        public int getPartition() {
            return partition;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getCurrentOffset() {
            return currentOffset;
        }

        public void setCurrentOffset(long currentOffset) {
            this.currentOffset = currentOffset;
        }

        public long getLastMessageProcessedTime() {
            return lastMessageProcessedTime;
        }

        public void setLastMessageProcessedTime(long lastMessageProcessedTime) {
            this.lastMessageProcessedTime = lastMessageProcessedTime;
        }

        public long getFailedMessageCount() {
            return failedMessageCount.get();
        }

        public void incrFailedMessageCount() {
            this.failedMessageCount.incrementAndGet();
        }

        public long getProcessedMessageCount() {
            return processedMessageCount.get();
        }

        public void incrProcessedMessageCount(long timeTakenMs) {
            this.processedMessageCount.incrementAndGet();
            this.totalProcessingTimeMs.addAndGet(timeTakenMs);
        }

        public long getAvgProcessingTime() {
            long processedMessageCount = this.processedMessageCount.get();

            return processedMessageCount == 0 ? 0 : (totalProcessingTimeMs.get() / processedMessageCount);
        }

        // processor wrappers
        public void updateOnSuccess(long offset, long processingTimeMs) {
            this.currentOffset = offset;
            this.processedMessageCount.incrementAndGet();
            this.totalProcessingTimeMs.addAndGet(processingTimeMs);
            this.lastMessageProcessedTime = System.currentTimeMillis();
        }

        public void updateOnFailure(long offset) {
            this.currentOffset = offset;
            this.failedMessageCount.incrementAndGet();
            this.lastFailedTime = System.currentTimeMillis();
        }
    }
}
