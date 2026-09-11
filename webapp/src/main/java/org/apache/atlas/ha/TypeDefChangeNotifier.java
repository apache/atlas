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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.TypeRegistryVersionGate;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
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
 * <p>The signal payload is {@code "<nodeId>:<version>"} where {@code version} is the
 * cluster-wide typedef-registry counter bumped immediately before this publish.
 * Peers compare that version before reloading, so a signal that is older than what
 * they have already applied is ignored.
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

    private final KafkaNotification        kafkaNotification;
    private final AtlasGraph               atlasGraph;
    private final TypeRegistryVersionGate  typeRegistryVersionGate;
    private final String                   topicName;
    private final String                   nodeId;

    @Inject
    public TypeDefChangeNotifier(KafkaNotification kafkaNotification, Configuration configuration,
                                 AtlasGraph atlasGraph, TypeRegistryVersionGate typeRegistryVersionGate) {
        this.kafkaNotification        = kafkaNotification;
        this.atlasGraph               = atlasGraph;
        this.typeRegistryVersionGate  = typeRegistryVersionGate;
        this.topicName                = configuration.getString(TypeDefSyncConsumer.TOPIC_CONFIG,
                TypeDefSyncConsumer.DEFAULT_TOPIC);
        this.nodeId                   = resolveNodeId(configuration);

        LOG.info("TypeDefChangeNotifier: typedef-change signals will be sent to topic '{}' (nodeId='{}')",
                topicName, nodeId);
    }

    /**
     * Bumps the shared typedef-registry version and publishes {@code "<nodeId>:<version>"}
     * so every peer can compare before reloading. Fire-and-forget — never delays the
     * typedef CRUD operation.
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

        long version;

        try {
            version = AtlasGraphUtilsV2.bumpTypeDefRegistryVersionAndCommit(atlasGraph);

            // This node already applied the typedef in-place; without advancing the watermark
            // the next local GET would rebuild the whole catalog for a version it already has.
            typeRegistryVersionGate.markSeen(version);

            LOG.info("TypeDefChangeNotifier.onChange(): bumped typedef registry version to {}", version);
        } catch (Exception e) {
            LOG.warn("TypeDefChangeNotifier.onChange(): could not bump typedef registry version; skipping Kafka signal", e);

            return;
        }

        String payload = nodeId + ":" + version;

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
