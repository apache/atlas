package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RedisServiceImpl extends AbstractRedisService{

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        try {
            redisClient = Redisson.create(getProdConfig());
            redisCacheClient = Redisson.create(getCacheImplConfig());
        } catch (Exception e) {
            LOG.error("Error creating Sentinel redis client.", e);
            throw new AtlasException("Error creating Sentinel redis client.", e);
        }
        if (redisClient== null || redisCacheClient == null) {
            throw new AtlasException("Failed to create Sentinel redis client.");
        }

        LOG.debug("Sentinel redis client created successfully.");
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
