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
package org.apache.atlas.ha;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.net.InetAddress;
import java.util.Collections;
import java.util.UUID;

/**
 * Publishes a typedef-change signal to the {@value TypeDefSyncConsumer#DEFAULT_TOPIC}
 * Kafka topic whenever a typedef CRUD operation is committed on this node.
 *
 * <p>The signal payload is {@code "<nodeId>:<timestamp>"} where {@code timestamp} is
 * {@code System.currentTimeMillis()} at the moment the change is committed.
 * Using a wall-clock timestamp instead of a per-JVM counter means the signal is
 * always interpreted correctly after a node restart: signals published after a restart
 * carry a newer timestamp than any signal the consuming node has already applied, so
 * no counter-state restoration is needed on restart.
 *
 * <p>This bean has <em>no dependency on {@code AtlasTypeDefStore}</em>, which avoids
 * the circular reference that would arise if it were combined with
 * {@link TypeDefSyncConsumer}:
 * <pre>
 *   AtlasTypeDefGraphStoreV2
 *     → List&lt;TypeDefChangeListener&gt; → TypeDefChangeNotifier  (no AtlasTypeDefStore dep)
 *
 *   TypeDefSyncConsumer                                        (no TypeDefChangeListener dep)
 *     → AtlasTypeDefStore → AtlasTypeDefGraphStoreV2
 * </pre>
 *
 * <p>All Kafka security settings (TLS, SASL/Kerberos) are inherited automatically
 * via {@link KafkaNotification#sendInternal} which reuses the same producer pool
 * Atlas already maintains for {@code ATLAS_HOOK} / {@code ATLAS_ENTITIES}.
 */
@Component
@Singleton
public class TypeDefChangeNotifier implements TypeDefChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDefChangeNotifier.class);

    private final KafkaNotification kafkaNotification;
    private final String            topicName;
    private final String            nodeId;

    @Inject
    public TypeDefChangeNotifier(KafkaNotification kafkaNotification, Configuration configuration) {
        this.kafkaNotification = kafkaNotification;
        this.topicName         = configuration.getString(TypeDefSyncConsumer.TOPIC_CONFIG,
                TypeDefSyncConsumer.DEFAULT_TOPIC);
        this.nodeId            = resolveNodeId(configuration);

        LOG.info("TypeDefChangeNotifier: typedef-change signals will be sent to topic '{}' (nodeId='{}')",
                topicName, nodeId);
    }

    /**
     * Sends a timestamped signal to the typedef-changes topic so every peer node reloads
     * its type registry.  The payload is {@code "<nodeId>:<timestamp>"} where
     * {@code timestamp} is epoch-milliseconds.
     * Fire-and-forget — never delays the typedef CRUD operation.
     */
    @Override
    public void onChange(ChangedTypeDefs changedTypeDefs) throws AtlasBaseException {
        if (changedTypeDefs == null) {
            return;
        }

        boolean hasChanges = CollectionUtils.isNotEmpty(changedTypeDefs.getCreatedTypeDefs())
                || CollectionUtils.isNotEmpty(changedTypeDefs.getUpdatedTypeDefs())
                || CollectionUtils.isNotEmpty(changedTypeDefs.getDeletedTypeDefs());

        if (!hasChanges) {
            return;
        }

        long   ts      = System.currentTimeMillis();
        String payload = nodeId + ":" + ts;

        try {
            kafkaNotification.sendInternal(topicName, Collections.singletonList(payload));
            LOG.info("TypeDefChangeNotifier.onChange(): sent signal '{}' to topic '{}'", payload, topicName);
        } catch (Exception e) {
            LOG.warn("TypeDefChangeNotifier.onChange(): could not send typedef-change signal '{}' to '{}'",
                    payload, topicName, e);
        }
    }

    private String resolveNodeId(Configuration configuration) {
        try {
            return AtlasServerIdSelector.selectServerId(configuration);
        } catch (Exception e) {
            LOG.debug("TypeDefChangeNotifier: server ID not configured, falling back to hostname:port");
        }

        try {
            int port = configuration.getInt("atlas.server.http.port",
                    configuration.getInt("atlas.server.https.port", 21000));
            return InetAddress.getLocalHost().getHostName() + ":" + port;
        } catch (Exception e) {
            String fallback = "node-" + UUID.randomUUID().toString().substring(0, 8);
            LOG.warn("TypeDefChangeNotifier: could not determine hostname, using '{}'", fallback);
            return fallback;
        }
    }

    @Override
    public void onLoadCompletion() throws AtlasBaseException {
        // Initial load happens on every node at startup — no broadcast needed.
    }
}
