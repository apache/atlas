package org.apache.atlas.featureflag;

public interface FeatureFlagStore {
    boolean evaluate(String featureFlagKey, String featureFlagValue);
}