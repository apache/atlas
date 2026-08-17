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

import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.service.Service;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes typedef-change signals from the {@value #DEFAULT_TOPIC} Kafka topic
 * and reloads the in-memory type registry on this node by calling
 * {@link AtlasTypeDefStore#init()}.
 *
 * <p>This bean implements only {@link ActiveStateChangeHandler} — it does
 * <em>not</em> implement {@link org.apache.atlas.listener.TypeDefChangeListener},
 * so it is never added to the {@code List<TypeDefChangeListener>} that
 * {@code AtlasTypeDefGraphStoreV2} collects in its constructor.  That is what
 * breaks the circular dependency:
 * <pre>
 *   AtlasTypeDefGraphStoreV2
 *     → List&lt;TypeDefChangeListener&gt; → {@link TypeDefChangeNotifier}  (no store dep)
 *
 *   TypeDefSyncConsumer                                               (no listener dep)
 *     → AtlasTypeDefStore → AtlasTypeDefGraphStoreV2
 * </pre>
 *
 * <h3>Why every node gets every message</h3>
 * Each Atlas node uses a <em>unique consumer group ID</em> derived from its
 * configured server ID or, as a fallback, from {@code hostname:port}.  Kafka
 * delivers every typedef-change signal to every consumer group independently,
 * so all nodes reload their type registry on each CRUD operation.
 *
 * <h3>Timestamp-based stale detection</h3>
 * Signal payloads use the format {@code "<nodeId>:<timestamp>"} where
 * {@code timestamp} is epoch-milliseconds from the publishing node's clock.
 */
@Component
@Singleton
@Order(4)   // after AtlasTypeDefStoreInitializer (@Order 2)
public class TypeDefSyncConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDefSyncConsumer.class);

    public static final String TOPIC_CONFIG  = "atlas.server.typedef.kafka.topic";
    public static final String DEFAULT_TOPIC = "ATLAS_TYPEDEF_CHANGES";

    private static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaNotification kafkaNotification;
    private final AtlasTypeDefStore typeDefStore;
    private final Configuration     configuration;
    private final String            topicName;
    private final String            consumerGroupId;
    private final String            localNodeId;

    /**
     * Last successfully applied signal timestamp (epoch-ms) per source nodeId.
     * A new signal is only processed if its timestamp is strictly greater than
     * the last applied timestamp for that node.
     */
    private final Map<String, Long> appliedTimestamps = new ConcurrentHashMap<>();

    private volatile KafkaConsumer<String, String> consumer;
    private volatile Thread                         consumerThread;
    private volatile boolean                        running;

    @Inject
    public TypeDefSyncConsumer(KafkaNotification kafkaNotification,
                               AtlasTypeDefStore typeDefStore,
                               Configuration     configuration) {
        this.kafkaNotification = kafkaNotification;
        this.typeDefStore      = typeDefStore;
        this.configuration     = configuration;
        this.topicName         = configuration.getString(TOPIC_CONFIG, DEFAULT_TOPIC);
        this.localNodeId       = resolveNodeId(configuration);
        this.consumerGroupId   = "atlas-typedef-refresh-" + localNodeId;

        LOG.info("TypeDefSyncConsumer: topic='{}', consumerGroup='{}', localNodeId='{}'",
                topicName, consumerGroupId, localNodeId);
    }

    // -------------------------------------------------------------------------
    // Service
    // -------------------------------------------------------------------------

    /** No-op: activation happens via {@link #instanceIsActive()} called by {@code AtlasActivationService}. */
    @Override
    public void start() throws AtlasException {
        // consumer started in instanceIsActive()
    }

    @Override
    public void stop() throws AtlasException {
        stopConsumer();
    }

    // -------------------------------------------------------------------------
    // ActiveStateChangeHandler
    // -------------------------------------------------------------------------

    @Override
    public void instanceIsActive() {
        // Typedef-sync consumer runs on all long-lived server modes:
        // MONOLITHIC, METADATA_SERVER, NOTIFICATION_PROCESSOR.
        // Skipped for INITIALIZER (exits after init; no need to keep types current).
        if (!AtlasRunMode.current().runsServer()) {
            LOG.info("TypeDefSyncConsumer.instanceIsActive(): RUN_MODE={} — skipping typedef-sync consumer",
                    AtlasRunMode.current());
            return;
        }

        LOG.info("TypeDefSyncConsumer.instanceIsActive(): starting consumer");
        startConsumer();
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.DEFAULT_METADATA_SERVICE.getOrder(); // = 4
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private synchronized void startConsumer() {
        if (running) {
            return;
        }

        running        = true;
        consumerThread = new Thread(this::typeDefChangeConsumerLoop, "typedef-kafka-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private synchronized void stopConsumer() {
        running = false;
        KafkaConsumer<String, String> c = consumer;
        if (c != null) {
            c.wakeup(); // unblocks the blocking poll() call cleanly
        }
    }

    /**
     * Polls the typedef-changes Kafka topic and reloads the type registry on
     * every signal.  Runs on the dedicated {@code typedef-kafka-consumer} thread.
     */
    private void typeDefChangeConsumerLoop() {
        Properties props = kafkaNotification.getConsumerProperties(NotificationType.HOOK);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           consumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   "100");

        consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList(topicName));
            LOG.info("TypeDefSyncConsumer: subscribed to '{}' (group='{}')", topicName, consumerGroupId);

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL_TIMEOUT);

                if (records.isEmpty()) {
                    continue;
                }

                String latestSignal    = null;
                long   latestTimestamp = Long.MIN_VALUE;

                for (ConsumerRecord<String, String> record : records) {
                    String payload = record.value();
                    if (payload == null) {
                        continue;
                    }

                    ParsedSignal parsed = parseSignal(payload);
                    if (parsed == null) {
                        continue;
                    }

                    if (localNodeId.equals(parsed.nodeId)) {
                        appliedTimestamps.put(parsed.nodeId, parsed.timestamp);
                        LOG.debug("TypeDefSyncConsumer: own signal '{}' — timestamp recorded, no reload needed", payload);
                        continue;
                    }

                    long applied = appliedTimestamps.getOrDefault(parsed.nodeId, -1L);
                    if (parsed.timestamp <= applied) {
                        LOG.debug("TypeDefSyncConsumer: skipping stale signal '{}' — already applied timestamp {} for node '{}'",
                                payload, applied, parsed.nodeId);
                        continue;
                    }

                    if (parsed.timestamp > latestTimestamp) {
                        latestTimestamp = parsed.timestamp;
                        latestSignal    = payload;
                    }
                }

                if (latestSignal == null) {
                    consumer.commitSync();
                    continue;
                }

                ParsedSignal trigger           = parseSignal(latestSignal);
                long         previousTimestamp = appliedTimestamps.getOrDefault(trigger.nodeId, -1L);

                LOG.info("TypeDefSyncConsumer: applying signal '{}' (node='{}' ts {} → {}), reloading type registry",
                        latestSignal, trigger.nodeId,
                        previousTimestamp < 0 ? "new" : previousTimestamp, trigger.timestamp);

                try {
                    typeDefStore.init();
                    appliedTimestamps.put(trigger.nodeId, trigger.timestamp);
                    consumer.commitSync();
                    LOG.info("TypeDefSyncConsumer: type registry reloaded. Applied timestamps: {}", appliedTimestamps);
                } catch (Exception e) {
                    LOG.warn("TypeDefSyncConsumer: type registry reload failed for signal '{}' — will retry on next signal",
                            latestSignal, e);
                } finally {
                    RequestContext.clear();
                }
            }
        } catch (WakeupException e) {
            LOG.info("TypeDefSyncConsumer: consumer shutting down");
        } catch (Exception e) {
            LOG.error("TypeDefSyncConsumer: consumer loop exited unexpectedly", e);
        } finally {
            consumer.close();
            consumer = null;
        }
    }

    /**
     * Parses a signal payload of the form {@code "<nodeId>:<timestamp>"} where
     * {@code timestamp} is epoch-milliseconds.
     * Returns {@code null} if the payload is malformed.
     */
    private static ParsedSignal parseSignal(String payload) {
        if (payload == null) {
            return null;
        }

        int sep = payload.lastIndexOf(':');
        if (sep <= 0 || sep == payload.length() - 1) {
            return null;
        }

        try {
            String nodeId    = payload.substring(0, sep);
            long   timestamp = Long.parseLong(payload.substring(sep + 1));
            return new ParsedSignal(nodeId, timestamp);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static final class ParsedSignal {
        final String nodeId;
        final long   timestamp;

        ParsedSignal(String nodeId, long timestamp) {
            this.nodeId    = nodeId;
            this.timestamp = timestamp;
        }
    }

    /**
     * Returns a stable, unique identifier for this Atlas node used to build the
     * Kafka consumer group ID.
     */
    private String resolveNodeId(Configuration configuration) {
        try {
            return AtlasServerIdSelector.selectServerId(configuration);
        } catch (Exception e) {
            LOG.debug("TypeDefSyncConsumer: server ID not configured, falling back to hostname:port");
        }

        try {
            int port = configuration.getInt("atlas.server.http.port",
                    configuration.getInt("atlas.server.https.port", 21000));
            return InetAddress.getLocalHost().getHostName() + ":" + port;
        } catch (Exception e) {
            String fallback = "node-" + UUID.randomUUID().toString().substring(0, 8);
            LOG.warn("TypeDefSyncConsumer: could not determine hostname, using '{}'", fallback);
            return fallback;
        }
    }
}
