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
import org.apache.atlas.model.AtlasStatistics;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;
import java.util.concurrent.*;

import static org.apache.atlas.model.AtlasStatistics.STAT_SERVER_START_TS;
import static org.apache.atlas.model.AtlasStatistics.STAT_SERVER_ACTIVE_TS;
import static org.apache.atlas.model.AtlasStatistics.STAT_SERVER_UP_SINCE;
import static org.apache.atlas.model.AtlasStatistics.STAT_START_OFFSET;
import static org.apache.atlas.model.AtlasStatistics.STAT_CURRENT_OFFSET;
import static org.apache.atlas.model.AtlasStatistics.STAT_SOLR_STATUS;
import static org.apache.atlas.model.AtlasStatistics.STAT_HBASE_STATUS;
import static org.apache.atlas.model.AtlasStatistics.STAT_LAST_MESSAGE_PROCESSED_TIME_TS;
import static org.apache.atlas.model.AtlasStatistics.STAT_AVG_MESSAGE_PROCESSING_TIME;
import static org.apache.atlas.model.AtlasStatistics.STAT_MESSAGES_CONSUMED;

@Component
public class StatisticsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticsUtil.class);

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("d MMM, yyyy : hh:mm aaa z");

    private static final long DAY = 1000 * 60 * 60 * 24;
    private static final long HOUR = 1000 * 60 * 60;
    private static final long MIN = 1000 * 60;
    private static final long SEC = 1000;

    private final AtlasGraph graph;
    private final String STATUS_CONNECTED = "connected";
    private final String STATUS_NOT_CONNECTED = "not-connected";
    private final AtlasStatistics atlasStatistics;

    private long countMsgProcessed        = 0;
    private long totalMsgProcessingTimeMs = 0;
    private Locale locale                 = new Locale("en", "US");
    private NumberFormat numberFormat;

    @Inject
    public StatisticsUtil(AtlasGraph graph) {
        this.graph = graph;
        this.atlasStatistics = new AtlasStatistics();
        numberFormat = NumberFormat.getInstance(locale);
    }

    public Map<String, Object> getAtlasStatistics() {
        Map<String, Object> statisticsMap = new HashMap<>();
        statisticsMap.putAll(atlasStatistics.getData());

        statisticsMap.put(STAT_HBASE_STATUS, getHBaseStatus());
        statisticsMap.put(STAT_SOLR_STATUS, getSolrStatus());
        statisticsMap.put(STAT_SERVER_UP_SINCE, getUpSinceTime());
        if(countMsgProcessed > 0) {
            statisticsMap.put(STAT_MESSAGES_CONSUMED, countMsgProcessed);
        }
        formatStatistics(statisticsMap);

        return statisticsMap;
    }

    public void setKafkaOffsets(long value) {
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
        Map<String, Object> data = atlasStatistics.getData();
        if (data == null) {
            data = new HashMap<>();
        }
        data.put(key, value);
        atlasStatistics.setData(data);
    }

    private Object getStat(String key) {
        Map<String, Object> data = atlasStatistics.getData();
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
                    statisticsMap.put(stat.getKey(), formatNumber(Long.parseLong(stat.getValue().toString())) + " milliseconds");
                    break;

                case STAT_HBASE_STATUS:
                case STAT_SOLR_STATUS:
                    String curState = ((boolean) stat.getValue()) ? STATUS_CONNECTED : STATUS_NOT_CONNECTED;
                    statisticsMap.put(stat.getKey(), curState);
                    break;

                case STAT_MESSAGES_CONSUMED:
                case STAT_START_OFFSET:
                case STAT_CURRENT_OFFSET:
                    statisticsMap.put(stat.getKey(), formatNumber(Long.parseLong(stat.getValue().toString())));
                    break;

                default:
                    statisticsMap.put(stat.getKey(), stat.getValue());
            }
        }
    }

    private boolean getHBaseStatus() {

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

    private boolean getSolrStatus() {
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

        long diffSeconds = msDiff / SEC % 60;
        long diffMinutes = msDiff / MIN % 60;
        long diffHours = msDiff / HOUR % 24;
        long diffDays = msDiff / DAY;

        if (diffDays > 0) sb.append(diffDays).append(" day ");
        if (diffHours > 0) sb.append(diffHours).append(" hour ");
        if (diffMinutes > 0) sb.append(diffMinutes).append(" min ");
        if (diffSeconds > 0) sb.append(diffSeconds).append(" sec");

        return sb.toString();
    }

    private String millisToTimeStamp(long ms) {
        return simpleDateFormat.format(ms);
    }

    private String formatNumber(long value) {
        return numberFormat.format(value);
    }

}
