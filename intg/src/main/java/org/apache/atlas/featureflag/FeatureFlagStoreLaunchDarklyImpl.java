package org.apache.atlas.featureflag;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import org.springframework.stereotype.Service;

@Service
public class FeatureFlagStoreLaunchDarklyImpl implements FeatureFlagStore {

    private final LDClient client;

    public FeatureFlagStoreLaunchDarklyImpl(LDClient ldClient) {
        this.client = ldClient;
    }

    @Override
    public boolean evaluate(String featureFlagKey, String featureFlagValue) {
        boolean ret;
        try {
            LDContext ldContext = initContext(featureFlagKey, featureFlagValue);
            ret = client.boolVariation(featureFlagKey, ldContext, false);
        } catch (Exception e) {
            return false;
        }
        return ret;
    }

    private static LDContext initContext(String key, String value) {
        LDContext ldContext = LDContext.builder(AtlasFeatureFlagConfig.UNQ_CONTEXT_KEY)
                .name(AtlasFeatureFlagConfig.CONTEXT_NAME)
                .set(key, value)
                .build();
        return ldContext;
    }

}