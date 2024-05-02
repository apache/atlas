package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class FeatureFlagStore {
    private static RedisService redisService = null;
    public FeatureFlagStore(@Qualifier("redisServiceImpl") RedisService redisService) {
        FeatureFlagStore.redisService = redisService;
    }

    public static boolean evaluate(String key,String expectedValue) {
        String updatedKey = createFeatureFlagNamespace(key);
        if (updatedKey == null) {
            return false;
        }
        String value = redisService.getValue(updatedKey);
        return StringUtils.equals(value, expectedValue);
    }

    private static String createFeatureFlagNamespace(String value) {
        if(StringUtils.isEmpty(value)) {
            return null;
        }
        return "ff:"+ value;
    }
}
