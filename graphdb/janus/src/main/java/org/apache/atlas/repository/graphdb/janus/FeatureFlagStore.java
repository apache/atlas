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

    public static boolean evaluate(String key, String expectedValue) {
        boolean ret = false;
        try{
            if (StringUtils.isEmpty(key) || StringUtils.isEmpty(expectedValue))
                return ret;
            String value = redisService.getValue(addFeatureFlagNamespace(key));
            ret = StringUtils.equals(value, expectedValue);
        } catch (Exception e) {
            return ret;
        }
        return ret;
    }

    public static void setFlag(String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value))
            return;

        redisService.putValue(addFeatureFlagNamespace(key), value);
    }

    public static void deleteFlag(String key) {
        if (StringUtils.isEmpty(key))
            return;

        redisService.removeValue(addFeatureFlagNamespace(key));
    }

    private static String addFeatureFlagNamespace(String key) {
        return "ff:"+key;
    }
}
