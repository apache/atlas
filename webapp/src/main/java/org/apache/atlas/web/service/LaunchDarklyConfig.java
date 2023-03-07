package org.apache.atlas.web.service;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LaunchDarklyConfig {
    private static final Logger LOG = LoggerFactory.getLogger(LaunchDarklyConfig.class);
    private String sdkKey;
    private LDContext ldContext;
    private LDClient client;

    public LaunchDarklyConfig(String sdkKey) {
        this.sdkKey = sdkKey;
        initClient();
    }

    private void initClient() {
        if(StringUtils.isNotEmpty(sdkKey)) {
            try {
                client = new LDClient(sdkKey);
            } catch (Exception e) {
                LOG.error("Error while initializing LaunchDarkly client", e);
            }
        }
    }

    public void initContext(String context, String name, String key, String value) {
        if (client == null) {
            return;
        }

        if(StringUtils.isNotEmpty(context) && StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
            try {
                ldContext = LDContext.builder(context)
                        .name(name)
                        .set(key, value)
                        .build();
            } catch (Exception e) {
                LOG.error("Error while initializing LaunchDarkly context", e);
            }
        }
    }

    public boolean evaluate(String featureKey) {
        boolean ret;
        try{
            ret = client != null ? client.boolVariation(featureKey, ldContext, false) : false;
        } catch (Exception e) {
           return false;
        }
        return ret;
    }

    public LDClient getClient() {
        return client;
    }

}
