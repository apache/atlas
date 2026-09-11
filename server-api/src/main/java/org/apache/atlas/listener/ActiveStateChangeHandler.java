/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.listener;

import org.apache.atlas.AtlasException;

/**
 * Callback interface for components that need to react when this Atlas node becomes active.
 *
 * <p>In active-active peer mode every node transitions directly to {@code ACTIVE}; there
 * are no leader, follower, or passive states.  Implementations receive a single callback —
 * {@link #instanceIsActive()} — and should start their subsystem at that point.
 *
 * <p>Which subsystems actually start is further controlled by {@link org.apache.atlas.AtlasRunMode}
 * via the {@code RUN_MODE} environment variable or system property.
 */
public interface ActiveStateChangeHandler {
    enum HandlerOrder {
        AUDIT_REPOSITORY(0),
        GRAPH_BACKED_SEARCH_INDEXER(1),
        TYPEDEF_STORE_INITIALIZER(2),
        ATLAS_PATCH_SERVICE(3),
        DEFAULT_METADATA_SERVICE(4),
        NOTIFICATION_HOOK_CONSUMER(5),
        TASK_MANAGEMENT(6),
        INDEX_RECOVERY(7),
        IMPORT_TASK_LISTENER(8);

        private final int order;

        HandlerOrder(int order) {
            this.order = order;
        }

        public int getOrder() {
            return order;
        }
    }

    /**
     * Called when this Atlas node has fully activated.
     *
     * <p>Any initialisation that must happen only when the node is ready to serve
     * requests should be done here.
     *
     * @throws AtlasException if anything goes wrong during activation
     */
    void instanceIsActive() throws AtlasException;

    /**
     * Defines the order in which handlers are invoked during activation.
     * Lower values are called first.
     */
    int getHandlerOrder();
}
