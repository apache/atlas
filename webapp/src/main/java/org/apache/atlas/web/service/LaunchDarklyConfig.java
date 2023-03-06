package org.apache.atlas.web.service;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
public class LaunchDarklyConfig {
    private String sdkKey;
    private LDContext ldContext;
    private LDClient client;

    @Inject
    public LaunchDarklyConfig() {
        sdkKey              = System.getenv("USER_LAUNCH_DARKLY_SDK_KEY");
        initClient();
    }

    private void initClient() {
        if(StringUtils.isNotEmpty(sdkKey)) {
            client      = new LDClient(sdkKey);
        }
    }

    public void initContext(String context, String name, String key, String value) {
       ldContext =  LDContext.builder(context)
                .name(name)
                .set(key, value)
                .build();
    }

    public boolean evaluate(String featureKey) {
        return client !=null ? client.boolVariation(featureKey, ldContext, false): false;
    }

    public LDClient getClient() {
        return client;
    }

    public LDContext getLdContext() {
        return ldContext;
    }
}
