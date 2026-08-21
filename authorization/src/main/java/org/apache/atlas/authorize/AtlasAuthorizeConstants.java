/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authorize;

/**
 * Constants for Atlas authorization resource types and related identifiers.
 * <p>
 * {@link #NOTIFICATION_TOPIC_RESOURCE_TYPE}
 * type {@code notification-topic} for topic-scoped POST authorization.
 */
public final class AtlasAuthorizeConstants {
    /**
     * Ranger / Atlas resource type for Kafka notification topics (e.g. ATLAS_HOOK, ATLAS_ENTITIES).
     */
    public static final String NOTIFICATION_TOPIC_RESOURCE_TYPE = "notification-topic";

    /**
     * Legacy privilege string removed in favor of {@link AtlasPrivilege#POST_NOTIFICATION}.
     * Documented for migration from older policies and Ranger service-def entries.
     */
    public static final String LEGACY_SERVICE_NOTIFICATION_POST_PRIVILEGE = "service-notification-post";

    private AtlasAuthorizeConstants() {
        // block instantiation
    }
}
