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
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticNotificationFilter;
import org.apache.atlas.semantic.SemanticNotificationGuidExpander;
import org.apache.atlas.semantic.SemanticSearchConfiguration;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.atlas.utils.SSLUtil;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Standalone Kafka consumer that enriches entity metadata and writes semantic embeddings
 * to the JanusGraph OpenSearch vertex index.
 */
public class SemanticIndexer {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticIndexer.class);

    private static volatile boolean running = true;

    public static void main(String[] args) {
        int exitCode = 1;
        SemanticIndexerHealthServer healthServer = null;

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> running = false));

            SSLUtil sslUtil = new SSLUtil();
            sslUtil.setSSLContext();

            Configuration config = ApplicationProperties.get();
            config.setProperty("atlas.kafka.entities.group.id", SemanticSearchConfiguration.getSemanticIndexerKafkaGroupId());
            config.setProperty("atlas.kafka.poll.timeout.ms",
                    SemanticSearchConfiguration.getSemanticIndexerKafkaPollTimeoutMs());

            if (SemanticSearchConfiguration.isSemanticIndexerHealthEnabled()) {
                healthServer = new SemanticIndexerHealthServer(
                        SemanticSearchConfiguration.getSemanticIndexerHealthPort(),
                        SemanticSearchConfiguration.getSemanticIndexerHealthPath(),
                        () -> running);
                healthServer.start();
            }

            AtlasJanusGraphDatabase.getGraphInstance();
            OpenSearchSemanticStore semanticStore = new OpenSearchSemanticStore();
            semanticStore.initialize();

            SemanticTextBuilder textBuilder = new SemanticTextBuilder();
            SemanticEntityIndexer entityIndexer = new SemanticEntityIndexer(textBuilder, semanticStore);

            KafkaNotification kafkaNotification = new KafkaNotification(config);
            NotificationConsumer consumer =
                    kafkaNotification.createConsumers(NotificationType.ENTITIES, 1, false).get(0);

            int batchSize = SemanticSearchConfiguration.getSemanticIndexerBatchSize();
            LOG.info("Semantic Indexer started (batchSize={}, groupId={}, pollTimeoutMs={})", batchSize,
                    SemanticSearchConfiguration.getSemanticIndexerKafkaGroupId(),
                    SemanticSearchConfiguration.getSemanticIndexerKafkaPollTimeoutMs());

            while (running) {
                @SuppressWarnings("unchecked")
                List<AtlasKafkaMessage<EntityNotification>> messages = consumer.receive();
                if (messages == null || messages.isEmpty()) {
                    continue;
                }

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

                    guids.addAll(SemanticNotificationGuidExpander.expandForIndexing(
                            SemanticNotificationFilter.extractGuids(entityNotification)));
                }

                SemanticEntityIndexer.IndexStats stats = SemanticEntityIndexer.IndexStats.EMPTY;
                if (!guids.isEmpty()) {
                    LOG.info("Processing {} guid(s) from {} Kafka message(s)", guids.size(), messages.size());
                    stats = entityIndexer.indexGuidsInBatches(guids, batchSize);
                }

                if (!stats.hasFailures()) {
                    for (AtlasKafkaMessage<EntityNotification> kafkaMessage : messages) {
                        consumer.commit(kafkaMessage.getTopicPartition(), kafkaMessage.getOffset() + 1);
                    }
                } else {
                    LOG.warn("Skipping Kafka commit for {} message(s) after {} indexing failure(s)",
                            messages.size(), stats.getFailed());
                }
            }

            exitCode = 0;
        } catch (Exception e) {
            LOG.error("Semantic Indexer failed", e);
        } finally {
            if (healthServer != null) {
                healthServer.close();
            }
            LOG.info("Semantic Indexer exiting with code {}", exitCode);
            System.exit(exitCode);
        }
    }
}
