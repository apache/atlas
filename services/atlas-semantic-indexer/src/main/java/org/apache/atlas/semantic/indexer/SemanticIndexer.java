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
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticIndexSetup;
import org.apache.atlas.semantic.SemanticNotificationFilter;
import org.apache.atlas.semantic.SemanticNotificationGuidExpander;
import org.apache.atlas.semantic.SemanticSearchConfiguration;
import org.apache.atlas.semantic.SemanticTextBuilder;
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

    private static final long SHUTDOWN_TIMEOUT_SEC = 30L;

    private static volatile boolean running = true;

    public static void main(String[] args) {
        int exitCode = 1;
        SemanticIndexerHealthServer healthServer = null;
        KafkaNotification kafkaNotification = null;
        final AtomicReference<NotificationConsumer<EntityNotification>> consumerRef = new AtomicReference<>();
        final CountDownLatch stopped = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            NotificationConsumer<EntityNotification> consumer = consumerRef.get();
            if (consumer != null) {
                consumer.wakeup();
            }
            try {
                stopped.await(SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "semantic-indexer-shutdown"));

        try {
            SSLUtil sslUtil = new SSLUtil();
            sslUtil.setSSLContext();

            SemanticSearchConfiguration.validate(); // before opening the graph, which may create the vertex index

            Configuration config = ApplicationProperties.get();
            config.setProperty("atlas.kafka.entities.group.id", SemanticSearchConfiguration.getSemanticIndexerKafkaGroupId());
            config.setProperty("atlas.kafka.poll.timeout.ms",
                    SemanticSearchConfiguration.getSemanticIndexerKafkaPollTimeoutMs());
            // a glossary-term update fans out to every assigned entity; allow long batches without a rebalance
            config.setProperty("atlas.kafka.max.poll.interval.ms",
                    SemanticSearchConfiguration.getSemanticIndexerKafkaMaxPollIntervalMs());
            // Atlas defaults to 1 record per poll; a larger poll lets processMessages dedup guids across messages
            config.setProperty("atlas.kafka.max.poll.records",
                    SemanticSearchConfiguration.getSemanticIndexerKafkaMaxPollRecords());

            if (SemanticSearchConfiguration.isSemanticIndexerHealthEnabled()) {
                healthServer = new SemanticIndexerHealthServer(
                        SemanticSearchConfiguration.getSemanticIndexerHealthPort(),
                        SemanticSearchConfiguration.getSemanticIndexerHealthPath(),
                        () -> running);
                healthServer.start();
            }

            AtlasJanusGraphDatabase.getGraphInstance();
            OpenSearchSemanticStore semanticStore = new OpenSearchSemanticStore();
            SemanticIndexSetup.initialize(semanticStore);

            SemanticTextBuilder textBuilder = new SemanticTextBuilder();
            SemanticEntityIndexer entityIndexer = new SemanticEntityIndexer(textBuilder, semanticStore);

            kafkaNotification = new KafkaNotification(config);
            @SuppressWarnings({"unchecked", "rawtypes"})
            NotificationConsumer consumer =
                    kafkaNotification.createConsumers(NotificationType.ENTITIES, 1, false).get(0);
            consumerRef.set(consumer);

            Set<String> subscription = consumer.subscription();
            if (subscription == null || subscription.isEmpty()) {
                throw new IllegalStateException("Kafka consumer could not be created or subscribed; check atlas.kafka.bootstrap.servers");
            }

            int batchSize = SemanticSearchConfiguration.getSemanticIndexerBatchSize();
            LOG.info("Semantic Indexer started (batchSize={}, groupId={}, pollTimeoutMs={}, maxPollRecords={})", batchSize,
                    SemanticSearchConfiguration.getSemanticIndexerKafkaGroupId(),
                    SemanticSearchConfiguration.getSemanticIndexerKafkaPollTimeoutMs(),
                    SemanticSearchConfiguration.getSemanticIndexerKafkaMaxPollRecords());

            while (running) {
                List<AtlasKafkaMessage<EntityNotification>> messages;
                try {
                    messages = consumer.receive();
                } catch (RuntimeException e) {
                    if (!running) {
                        LOG.info("Semantic Indexer poll interrupted during shutdown");
                        break;
                    }
                    throw e;
                }

                if (messages == null || messages.isEmpty()) {
                    continue;
                }

                try {
                    processMessages(messages, entityIndexer, batchSize);
                } catch (RuntimeException e) {
                    // commit anyway so one bad message can't block the partition; atlas_semantic_repair.sh backfills
                    LOG.error("Failed to process {} Kafka message(s) (first offset={}); committing and continuing",
                            messages.size(), messages.get(0).getOffset(), e);
                } finally {
                    // end the thread-bound read tx so the next message never reads vertices cached before its commit
                    AtlasGraphProvider.getGraphInstance().rollback();
                }

                commitOffsets(consumer, messages);
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

    private static void processMessages(List<AtlasKafkaMessage<EntityNotification>> messages,
                                        SemanticEntityIndexer entityIndexer,
                                        int batchSize) {
        Set<String> guids = new LinkedHashSet<>();
        for (AtlasKafkaMessage<EntityNotification> kafkaMessage : messages) {
            EntityNotification notification = kafkaMessage.getMessage();
            if (!(notification instanceof EntityNotificationV2)) {
                continue;
            }

            EntityNotificationV2 entityNotification = (EntityNotificationV2) notification;
            if (!SemanticNotificationFilter.shouldProcess(entityNotification)) {
                continue;
            }

            // per message, so one unreadable message is skipped without dropping the rest of the poll
            try {
                guids.addAll(SemanticNotificationGuidExpander.expandForIndexing(
                        SemanticNotificationFilter.extractGuids(entityNotification)));
            } catch (RuntimeException e) {
                LOG.error("Skipping Kafka message at offset {} ({}): failed to expand guids",
                        kafkaMessage.getOffset(), kafkaMessage.getTopicPartition(), e);
            }
        }

        if (guids.isEmpty()) {
            return;
        }

        LOG.info("Processing {} guid(s) from {} Kafka message(s)", guids.size(), messages.size());
        SemanticEntityIndexer.IndexStats stats = entityIndexer.indexGuidsInBatches(guids, batchSize);
        if (stats.hasFailures()) {
            LOG.warn("Completed batch with {} indexing failure(s) (skipped={}, indexed={})",
                    stats.getFailed(), stats.getSkipped(), stats.getIndexed());
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
