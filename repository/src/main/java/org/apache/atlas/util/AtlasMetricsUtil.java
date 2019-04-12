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
import org.apache.atlas.util.AtlasMetricsCounter.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.atlas.model.metrics.AtlasMetrics.*;
import static org.apache.atlas.repository.Constants.TYPE_NAME_INTERNAL;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.apache.atlas.util.AtlasMetricsCounter.Period.*;

@Component
public class AtlasMetricsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasMetricsUtil.class);

    private static final long   SEC_MS               = 1000;
    private static final long   MIN_MS               =   60 * SEC_MS;
    private static final long   HOUR_MS              =   60 * MIN_MS;
    private static final long   DAY_MS               =   24 * HOUR_MS;
    private static final String STATUS_CONNECTED     = "connected";
    private static final String STATUS_NOT_CONNECTED = "not-connected";

    private final AtlasGraph          graph;
    private       long                serverStartTime   = 0;
    private       long                serverActiveTime  = 0;
    private       long                msgOffsetStart    = -1;
    private       long                msgOffsetCurrent  = 0;
    private final AtlasMetricsCounter messagesProcessed = new AtlasMetricsCounter("messagesProcessed");
    private final AtlasMetricsCounter messagesFailed    = new AtlasMetricsCounter("messagesFailed");
    private final AtlasMetricsCounter entityCreates     = new AtlasMetricsCounter("entityCreates");
    private final AtlasMetricsCounter entityUpdates     = new AtlasMetricsCounter("entityUpdates");
    private final AtlasMetricsCounter entityDeletes     = new AtlasMetricsCounter("entityDeletes");

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

    public void onNotificationProcessingComplete(long msgOffset, NotificationStat stats) {
        messagesProcessed.incrWithMeasure(stats.timeTakenMs);
        entityCreates.incrBy(stats.entityCreates);
        entityUpdates.incrBy(stats.entityUpdates);
        entityDeletes.incrBy(stats.entityDeletes);

        if (stats.isFailedMsg) {
            messagesFailed.incr();
        }

        if (msgOffsetStart == -1) {
            msgOffsetStart = msgOffset;
        }

        msgOffsetCurrent = ++msgOffset;
    }

    public Map<String, Object> getStats() {
        Map<String, Object> ret = new HashMap<>();

        Stats messagesProcessed = this.messagesProcessed.report();
        Stats messagesFailed    = this.messagesFailed.report();
        Stats entityCreates     = this.entityCreates.report();
        Stats entityUpdates     = this.entityUpdates.report();
        Stats entityDeletes     = this.entityDeletes.report();

        ret.put(STAT_SERVER_START_TIMESTAMP, serverStartTime);
        ret.put(STAT_SERVER_ACTIVE_TIMESTAMP, serverActiveTime);
        ret.put(STAT_SERVER_UP_TIME, millisToTimeDiff(System.currentTimeMillis() - serverStartTime));
        ret.put(STAT_SERVER_STATUS_BACKEND_STORE, getBackendStoreStatus() ? STATUS_CONNECTED : STATUS_NOT_CONNECTED);
        ret.put(STAT_SERVER_STATUS_INDEX_STORE, getIndexStoreStatus() ? STATUS_CONNECTED : STATUS_NOT_CONNECTED);

        ret.put(STAT_NOTIFY_START_OFFSET, msgOffsetStart);
        ret.put(STAT_NOTIFY_CURRENT_OFFSET, msgOffsetCurrent);
        ret.put(STAT_NOTIFY_LAST_MESSAGE_PROCESSED_TIME, this.messagesProcessed.getLastIncrTime().toEpochMilli());

        ret.put(STAT_NOTIFY_COUNT_TOTAL,         messagesProcessed.getCount(ALL));
        ret.put(STAT_NOTIFY_AVG_TIME_TOTAL,      messagesProcessed.getMeasureAvg(ALL));
        ret.put(STAT_NOTIFY_FAILED_COUNT_TOTAL,  messagesFailed.getCount(ALL));
        ret.put(STAT_NOTIFY_CREATES_COUNT_TOTAL, entityCreates.getCount(ALL));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_TOTAL, entityUpdates.getCount(ALL));
        ret.put(STAT_NOTIFY_DELETES_COUNT_TOTAL, entityDeletes.getCount(ALL));

        ret.put(STAT_NOTIFY_START_TIME_CURR_DAY,    messagesProcessed.getDayStartTimeMs());
        ret.put(STAT_NOTIFY_COUNT_CURR_DAY,         messagesProcessed.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_AVG_TIME_CURR_DAY,      messagesProcessed.getMeasureAvg(CURR_DAY));
        ret.put(STAT_NOTIFY_FAILED_COUNT_CURR_DAY,  messagesFailed.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_CREATES_COUNT_CURR_DAY, entityCreates.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_CURR_DAY, entityUpdates.getCount(CURR_DAY));
        ret.put(STAT_NOTIFY_DELETES_COUNT_CURR_DAY, entityDeletes.getCount(CURR_DAY));

        ret.put(STAT_NOTIFY_START_TIME_CURR_HOUR,    messagesProcessed.getHourStartTimeMs());
        ret.put(STAT_NOTIFY_COUNT_CURR_HOUR,         messagesProcessed.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_AVG_TIME_CURR_HOUR,      messagesProcessed.getMeasureAvg(CURR_HOUR));
        ret.put(STAT_NOTIFY_FAILED_COUNT_CURR_HOUR,  messagesFailed.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_CREATES_COUNT_CURR_HOUR, entityCreates.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_CURR_HOUR, entityUpdates.getCount(CURR_HOUR));
        ret.put(STAT_NOTIFY_DELETES_COUNT_CURR_HOUR, entityDeletes.getCount(CURR_HOUR));

        ret.put(STAT_NOTIFY_COUNT_PREV_HOUR,         messagesProcessed.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_AVG_TIME_PREV_HOUR,      messagesProcessed.getMeasureAvg(PREV_HOUR));
        ret.put(STAT_NOTIFY_FAILED_COUNT_PREV_HOUR,  messagesFailed.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_CREATES_COUNT_PREV_HOUR, entityCreates.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_PREV_HOUR, entityUpdates.getCount(PREV_HOUR));
        ret.put(STAT_NOTIFY_DELETES_COUNT_PREV_HOUR, entityDeletes.getCount(PREV_HOUR));

        ret.put(STAT_NOTIFY_COUNT_PREV_DAY,         messagesProcessed.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_AVG_TIME_PREV_DAY,      messagesProcessed.getMeasureAvg(PREV_DAY));
        ret.put(STAT_NOTIFY_FAILED_COUNT_PREV_DAY,  messagesFailed.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_CREATES_COUNT_PREV_DAY, entityCreates.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_UPDATES_COUNT_PREV_DAY, entityUpdates.getCount(PREV_DAY));
        ret.put(STAT_NOTIFY_DELETES_COUNT_PREV_DAY, entityDeletes.getCount(PREV_DAY));

        return ret;
    }

    private boolean getBackendStoreStatus(){
        try {
            runWithTimeout(new Runnable() {
                @Override
                public void run() {
                    graph.query().has(TYPE_NAME_PROPERTY_KEY, TYPE_NAME_INTERNAL).vertices(1);
                }
            }, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean getIndexStoreStatus(){
        final String query = AtlasGraphUtilsV2.getIndexSearchPrefix() + "\"" + Constants.TYPE_NAME_PROPERTY_KEY + "\":(" + TYPE_NAME_INTERNAL + ")";

        try {
            runWithTimeout(new Runnable() {
                @Override
                public void run() {
                    graph.indexQuery(Constants.VERTEX_INDEX, query).vertices(0, 1);
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

    private String millisToTimeDiff(long msDiff) {
        StringBuilder sb = new StringBuilder();

        long diffSeconds = msDiff / SEC_MS % 60;
        long diffMinutes = msDiff / MIN_MS % 60;
        long diffHours   = msDiff / HOUR_MS % 24;
        long diffDays    = msDiff / DAY_MS;

        if (diffDays > 0) sb.append(diffDays).append(" day ");
        if (diffHours > 0) sb.append(diffHours).append(" hour ");
        if (diffMinutes > 0) sb.append(diffMinutes).append(" min ");
        if (diffSeconds > 0) sb.append(diffSeconds).append(" sec");

        return sb.toString();
    }

    public static class NotificationStat {
        public boolean isFailedMsg   = false;
        public long    timeTakenMs   = 0;
        public int     entityCreates = 0;
        public int     entityUpdates = 0;
        public int     entityDeletes = 0;

        public NotificationStat() { }

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

        private int getSize(Collection collection) {
            return collection != null ? collection.size() : 0;
        }
    }
}
