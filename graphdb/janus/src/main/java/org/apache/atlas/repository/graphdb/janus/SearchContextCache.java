package org.apache.atlas.repository.graphdb.janus;
import org.apache.atlas.RequestContext;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
@Component
public class SearchContextCache {
    private static final Logger LOG = LoggerFactory.getLogger(SearchContextCache.class);
    private static RedisService redisService = null;

    public static final String INVALID_SEQUENCE = "invalid_sequence";


    public SearchContextCache(@Qualifier("redisServiceImpl") RedisService redisService) {
        SearchContextCache.redisService = redisService;
    }


    public static void put(String key, Integer sequence, String esAsyncId) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("putInCache");
       try {
           // Build the string in format `sequence/esAsyncId` and store it in redis
           String val = sequence + "/" + esAsyncId;
           redisService.putValue(key, val, 30);
       } finally {
           RequestContext.get().endMetricRecord(metric);
       }
    }
    public static String get(String key) {
        try {
            return redisService.getValue(key);
        } catch (Exception e) {
            LOG.error("Error while fetching value from Redis", e);
            return null;
        }

    }

    public static String getESAsyncSearchIdFromContextCache(String key, Integer sequence){
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getESAsyncSearchIdFromContextCache");
        try {
            //Get the context cache for the given key
            String contextCache = get(key);
            if(contextCache == null || sequence == null){
                return null;
            }
            // Split the context cache to get the sequence and ESAsyncId
            String[] contextCacheSplit = contextCache.split("/");
            if(contextCacheSplit.length != 2){
                return null;
            }
            int seq = Integer.parseInt(contextCacheSplit[0]);
            if(sequence > seq){
                return contextCacheSplit[1];
            } else if (sequence < seq) {
                return INVALID_SEQUENCE;
            }
            return null;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }

    }
    public static void remove(String key) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("removeFromCache");
        try {
            redisService.removeValue(key);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }

    }
}

