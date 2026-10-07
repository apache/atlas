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
package org.apache.atlas.semantic.indexer;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticEntityEmbedder;
import org.apache.atlas.semantic.SemanticEntityEmbedder.IndexStats;
import org.apache.atlas.semantic.SemanticIndexSetup;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.atlas.semantic.SemanticVectorStore;
import org.apache.atlas.utils.SSLUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standalone Kafka consumer that enriches entity metadata and writes semantic embeddings
 * to the JanusGraph OpenSearch vertex index.
 */
public class SemanticIndexer {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticIndexer.class);

    private static final long SHUTDOWN_TIMEOUT_MS   = 30_000L;
    private static final long BACKEND_RETRY_WAIT_MS = 10_000L;
    private static final String GRAPH_PROBE_GUID    = "00000000-0000-0000-0000-000000000000";

    private static volatile boolean running = true;

    public static void main(String[] args) {
        int exitCode = 1;
        SemanticIndexerHealthServer healthServer = null;
        KafkaNotification kafkaNotification = null;
        final AtomicReference<NotificationConsumer<EntityNotification>> consumerRef = new AtomicReference<>();
        final CountDownLatch stopped = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Semantic Indexer shutdown requested; finishing the current batch (up to {} ms)", SHUTDOWN_TIMEOUT_MS);
            running = false;
            NotificationConsumer<EntityNotification> consumer = consumerRef.get();
            if (consumer != null) {
                consumer.wakeup();
            }
            try {
                stopped.await(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "semantic-indexer-shutdown"));

        try {
            SSLUtil sslUtil = new SSLUtil();
            sslUtil.setSSLContext();

            SemanticIndexSetup.validateConfiguration(); // before opening the graph, which may create the vertex index

            Configuration config = ApplicationProperties.get();
            config.setProperty("atlas.kafka.entities.group.id", AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_GROUP_ID.getString());
            config.setProperty("atlas.kafka.poll.timeout.ms",
                    AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS.getLong());
            // a glossary-term update fans out to every assigned entity; allow long batches without a rebalance
            config.setProperty("atlas.kafka.max.poll.interval.ms",
                    AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_MAX_POLL_INTERVAL_MS.getInt());
            // Atlas defaults to 1 record per poll; a larger poll lets processMessages dedup guids across messages
            config.setProperty("atlas.kafka.max.poll.records",
                    AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS.getInt());

            if (AtlasConfiguration.SEMANTIC_INDEXER_HEALTH_ENABLED.getBoolean()) {
                healthServer = new SemanticIndexerHealthServer(
                        AtlasConfiguration.SEMANTIC_INDEXER_HEALTH_PORT.getInt(),
                        AtlasConfiguration.SEMANTIC_INDEXER_HEALTH_PATH.getString());
                healthServer.start();
            }

            AtlasJanusGraphDatabase.getGraphInstance();
            SemanticVectorStore semanticStore = new OpenSearchSemanticStore();
            semanticStore.initialize();

            SemanticEntityEmbedder embedder = new SemanticEntityEmbedder(new SemanticTextBuilder(), semanticStore);

            kafkaNotification = new KafkaNotification(config);
            @SuppressWarnings({"unchecked", "rawtypes"})
            NotificationConsumer consumer =
                    kafkaNotification.createConsumers(NotificationType.ENTITIES, 1, false).get(0);
            consumerRef.set(consumer);

            Set<String> subscription = consumer.subscription();
            if (subscription == null || subscription.isEmpty()) {
                throw new IllegalStateException("Kafka consumer could not be created or subscribed; check atlas.kafka.bootstrap.servers");
            }

            LOG.info("Semantic Indexer started (groupId={}, pollTimeoutMs={}, maxPollRecords={})",
                    AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_GROUP_ID.getString(),
                    AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS.getLong(),
                    AtlasConfiguration.SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS.getInt());

            while (running) {
                List<AtlasKafkaMessage<EntityNotification>> messages;
                try {
                    messages = consumer.receive();
                } catch (WakeupException e) {
                    LOG.info("Semantic Indexer poll interrupted during shutdown");
                    break;
                }

                if (messages == null || messages.isEmpty()) {
                    continue;
                }

                // A failure while OpenSearch or the graph is down is retried until both answer, without committing.
                // Any other failure (e.g. HTTP 400) is committed: atlas_semantic_repair.sh backfills it.
                boolean retry = processMessages(messages, embedder).hasFailures() && !backendsAvailable(semanticStore);
                while (retry && running) {
                    LOG.warn("OpenSearch or the graph is unavailable: retrying {} Kafka message(s) (first offset={}) in {} ms",
                            messages.size(), messages.get(0).getOffset(), BACKEND_RETRY_WAIT_MS);
                    Thread.sleep(BACKEND_RETRY_WAIT_MS);
                    retry = processMessages(messages, embedder).hasFailures() && !backendsAvailable(semanticStore);
                }

                if (!retry) {
                    commitOffsets(consumer, messages);
                }
            }

            exitCode = 0;
        } catch (Exception e) {
            LOG.error("Semantic Indexer failed", e);
        } finally {
            NotificationConsumer<EntityNotification> consumer = consumerRef.get();
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close Kafka consumer", e);
                }
            }

            if (kafkaNotification != null) {
                try {
                    kafkaNotification.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close Kafka notification", e);
                }
            }

            if (healthServer != null) {
                healthServer.close();
            }

            LOG.info("Semantic Indexer exiting with code {}", exitCode);
            stopped.countDown();

            // System.exit() blocks forever when called while shutdown hooks are running
            if (running) {
                System.exit(exitCode);
            }
        }
    }

    private static IndexStats processMessages(List<AtlasKafkaMessage<EntityNotification>> messages,
                                              SemanticEntityEmbedder embedder) {
        Set<String> guids          = new LinkedHashSet<>();
        int         expandFailures = 0;

        try {
            for (AtlasKafkaMessage<EntityNotification> kafkaMessage : messages) {
                EntityNotification notification = kafkaMessage.getMessage();
                if (!(notification instanceof EntityNotificationV2)) {
                    continue;
                }

                EntityNotificationV2 entityNotification = (EntityNotificationV2) notification;
                if (!SemanticNotificationFilter.shouldProcess(entityNotification)) {
                    continue;
                }

                // per message, so one unreadable message doesn't drop the rest of the poll
                try {
                    guids.addAll(SemanticNotificationGuidExpander.expandForIndexing(
                            SemanticNotificationFilter.extractGuidTypes(entityNotification)));
                } catch (RuntimeException e) {
                    expandFailures++;
                    LOG.error("Failed to expand guids of Kafka message at offset {} ({})",
                            kafkaMessage.getOffset(), kafkaMessage.getTopicPartition(), e);
                }
            }

            IndexStats stats = new IndexStats(0, 0, expandFailures);
            if (!guids.isEmpty()) {
                LOG.info("Processing {} guid(s) from {} Kafka message(s)", guids.size(), messages.size());
                stats = stats.add(embedder.embed(guids));
            }
            return stats;
        } finally {
            // end the thread-bound read tx so the next message never reads vertices cached before its commit
            AtlasGraphProvider.getGraphInstance().rollback();
        }
    }

    /**
     * Whether the graph storage backend and the vector store both answer. Atlas has no graph health API: any read
     * that reaches the storage backend tells whether it answers.
     */
    private static boolean backendsAvailable(SemanticVectorStore semanticStore) {
        try {
            AtlasGraphUtilsV2.findByGuid(GRAPH_PROBE_GUID);
            AtlasGraphProvider.getGraphInstance().rollback();
            return semanticStore.isAvailable();
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void commitOffsets(NotificationConsumer<EntityNotification> consumer,
                                      List<AtlasKafkaMessage<EntityNotification>> messages) {
        Map<TopicPartition, Long> nextOffsets = new HashMap<>();
        for (AtlasKafkaMessage<EntityNotification> kafkaMessage : messages) {
            nextOffsets.merge(kafkaMessage.getTopicPartition(), kafkaMessage.getOffset() + 1, Math::max);
        }

        for (Map.Entry<TopicPartition, Long> entry : nextOffsets.entrySet()) {
            for (int attempt = 1; ; attempt++) {
                try {
                    consumer.commit(entry.getKey(), entry.getValue());
                    break;
                } catch (WakeupException e) {
                    // shutdown hook called wakeup() while we were indexing; the flag is cleared once thrown, so retry
                    if (attempt < 2) {
                        continue;
                    }
                    LOG.warn("Failed to commit offset {} for {} during shutdown; batch may be redelivered", entry.getValue(), entry.getKey(), e);
                    break;
                } catch (KafkaException e) {
                    // e.g. CommitFailedException after a rebalance: the batch is redelivered (re-indexing is idempotent)
                    LOG.warn("Failed to commit offset {} for {}; batch may be redelivered", entry.getValue(), entry.getKey(), e);
                    break;
                }
            }
        }
    }
}
