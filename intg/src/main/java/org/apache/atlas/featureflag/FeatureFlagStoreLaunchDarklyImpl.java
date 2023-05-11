/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.featureflag;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

@Component
public class FeatureFlagStoreLaunchDarklyImpl implements FeatureFlagStore {

    private final LDClient client;

    @Inject
    public FeatureFlagStoreLaunchDarklyImpl(AtlasFeatureFlagClient client) {
        this.client = client.getClient();
    }

    @Override
    public boolean evaluate(FeatureFlag flag, String key, boolean value) {
        boolean ret;
        try {
            ret = client.boolVariation(flag.getKey(), getContext(key, value), flag.getDefaultValue());
        } catch (Exception e) {
            return false;
        }

        return ret;
    }

    @Override
    public boolean evaluate(FeatureFlag flag, String key, String value) {
        boolean ret;
        try {
            ret = client.boolVariation(flag.getKey(), getContext(key, value), flag.getDefaultValue());
        } catch (Exception e) {
            return false;
        }

        return ret;
    }

    private LDContext getContext(String key, String value) {
        LDContext ldContext = LDContext.builder(AtlasFeatureFlagClient.UNQ_CONTEXT_KEY)
                .name(AtlasFeatureFlagClient.CONTEXT_NAME)
                .set(key, value)
                .build();

        return ldContext;
    }

    private LDContext getContext(String key, boolean value) {
        LDContext ldContext = LDContext.builder(AtlasFeatureFlagClient.UNQ_CONTEXT_KEY)
                .name(AtlasFeatureFlagClient.CONTEXT_NAME)
                .set(key, value)
                .build();

        return ldContext;
    }
}