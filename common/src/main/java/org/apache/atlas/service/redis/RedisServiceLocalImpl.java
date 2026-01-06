package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component("redisServiceImpl")
@Profile("local")  // Use this simple Redis implementation in tests (no conditional check needed)
public class RedisServiceLocalImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceLocalImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        LOG.info("==> RedisServiceLocalImpl.init() - Starting local Redis service initialization.");
        redisClient = Redisson.create(getLocalConfig());
        redisCacheClient = Redisson.create(getLocalConfig());
        LOG.info("Local redis client created successfully.");
    }

    @Override
    public String getValue(String key) {
        return super.getValue(key);
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        return super.putValue(key, value, timeout);
    }

    @Override
    public void removeValue(String key) {
        super.removeValue(key);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}

