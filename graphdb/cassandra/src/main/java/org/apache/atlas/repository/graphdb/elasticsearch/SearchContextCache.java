package org.apache.atlas.repository.graphdb.elasticsearch;

import org.apache.atlas.RequestContext;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * SearchContextCache for Cassandra graph backend.
 * Note: @Component removed to avoid Spring bean conflict with janus module's version
 * during the migration period when both modules are on the classpath.
 */
public class SearchContextCache {
    private static final Logger LOG = LoggerFactory.getLogger(SearchContextCache.class);
    private static RedisService redisService = null;

    public static final String INVALID_SEQUENCE = "invalid_sequence";

    @Inject
    public SearchContextCache(RedisService redisService) {
        SearchContextCache.redisService = redisService;
    }


    public static void put(String key, Integer sequence, String esAsyncId) {
        if (redisService == null) {
            LOG.debug("SearchContextCache.put: redisService not available, skipping");
            return;
        }
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
        if (redisService == null) {
            return null;
        }
        try {
            return redisService.getValue(key);
        } catch (Exception e) {
            LOG.error("Error while fetching value from Redis", e);
            return null;
        }

    }

    public static String getESAsyncSearchIdFromContextCache(String key, Integer sequence){
        if (redisService == null) {
            return null;
        }
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
        if (redisService == null) {
            return;
        }
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("removeFromCache");
        try {
            redisService.removeValue(key);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }

    }
}
