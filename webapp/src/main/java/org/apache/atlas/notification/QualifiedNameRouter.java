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
package org.apache.atlas.notification;

import org.apache.atlas.AtlasConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Router class that determines which topic to use based on
 * the first two parts of the qualifiedName of entities in the notification.
 */
public class QualifiedNameRouter {
    private static final Logger LOG                  = LoggerFactory.getLogger(QualifiedNameRouter.class);
    private static final String DEFAULT_TOPIC_PREFIX = AtlasConfiguration.ATLAS_METADATA_TOPIC_PREFIX.getString();

    private final int                                maxTopicCount;
    private final String                             topicPrefix;
    private final ConcurrentHashMap<String, Integer> routingCache;
    private final AtomicInteger                      nextTopicIndex;

    public QualifiedNameRouter(int maxTopicCount) {
        this(maxTopicCount, DEFAULT_TOPIC_PREFIX);
    }

    public QualifiedNameRouter(int maxTopicCount, String topicPrefix) {
        this.maxTopicCount           = maxTopicCount > 0 ? maxTopicCount : 1;
        this.topicPrefix             = topicPrefix.endsWith("_") ? topicPrefix : topicPrefix + "_";
        this.routingCache            = new ConcurrentHashMap<>();
        this.nextTopicIndex          = new AtomicInteger(0);
    }

    public String getTargetTopic(String routingKey) {
        if (StringUtils.isNotBlank(routingKey)) {
            int    topicIndex  = getTopicIndexForRoutingKey(routingKey);
            String targetTopic = topicPrefix + topicIndex;

            LOG.debug("Routing message with key '{}' to topic '{}'", routingKey, targetTopic);
            return targetTopic;
        } else {
            // Fallback to round-robin if no routing key found
            int    topicIndex  = nextTopicIndex.getAndIncrement() % maxTopicCount;
            String targetTopic = topicPrefix + topicIndex;

            LOG.debug("No routing key found, using round-robin to topic '{}'", targetTopic);
            return targetTopic;
        }
    }

    /**
     * Gets the topic index for a routing key using consistent hashing.
     *
     * @param routingKey The routing key (first two parts of qualifiedName)
     * @return The topic index (0 to maxTopicCount-1)
     */
    private int getTopicIndexForRoutingKey(String routingKey) {
        return routingCache.computeIfAbsent(routingKey, key -> {
            // Use consistent hashing to ensure same routing key always goes to same topic
            int hash       = Math.abs(key.hashCode());
            int topicIndex = hash % maxTopicCount;

            LOG.debug("Mapped routing key '{}' to topic index {}", key, topicIndex);
            return topicIndex;
        });
    }

    /**
     * Gets statistics about the routing cache.
     *
     * @return A string with routing statistics
     */
    public String getRoutingStats() {
        return String.format("Routing cache size: %d, Max topics: %d",
                routingCache.size(), maxTopicCount);
    }

    /**
     * Clears the routing cache.
     */
    public void clearCache() {
        routingCache.clear();
        LOG.info("Routing cache cleared");
    }
}
