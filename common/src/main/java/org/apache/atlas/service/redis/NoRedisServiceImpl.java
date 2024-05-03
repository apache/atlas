package org.apache.atlas.service.redis;

import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl", isDefault = true)
public class NoRedisServiceImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(NoRedisServiceImpl.class);

    @PostConstruct
    public void init() {
        LOG.info("Enabled no redis implementation.");
    }

    @Override
    public boolean acquireDistributedLock(String key) {
        //do nothing
        return true;
    }

    @Override
    public void releaseDistributedLock(String key) {
        //do nothing
    }

    @Override
    public String getValue(String key) {
        return null;
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        return null;
    }

    @Override
    public void removeValue(String key) {

    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
