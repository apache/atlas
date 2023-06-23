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

public interface FeatureFlagStore {

    static final String DISABLE_ACCESS_CONTROL_FEATURE_FLAG_KEY = "disable-access-control-ops";
    static final String IS_INSTANCE_MIGRATED_FEATURE_FLAG_KEY = "users_groups_list_api_migrated_response";
    static final String LINEAGE_EVENTS_FEATURE_FLAG_KEY =  "metastore-enable-lineage-events-for-pipeline";

    boolean evaluate(FeatureFlag flag, String key, boolean value);

    boolean evaluate(FeatureFlag flag, String key, String value);

    enum FeatureFlag {
        DISABLE_ACCESS_CONTROL(DISABLE_ACCESS_CONTROL_FEATURE_FLAG_KEY, false),
        IS_INSTANCE_MIGRATED(IS_INSTANCE_MIGRATED_FEATURE_FLAG_KEY, false),
        ENABLE_LINEAGE_EVENTS(LINEAGE_EVENTS_FEATURE_FLAG_KEY, false),
        ADD_CONNECTION_ROLE_IN_ADMIN_ROLE("add-connection-role-in-admin-role", false),
        ALLOW_CONNECTION_ADMIN_OPS("allow-connection-admins-ops", true);

        private final String key;
        private final boolean defaultValue;

        FeatureFlag(String key, boolean defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        public String getKey() {
            return key;
        }

        public boolean getDefaultValue() {
            return defaultValue;
        }
    }
}