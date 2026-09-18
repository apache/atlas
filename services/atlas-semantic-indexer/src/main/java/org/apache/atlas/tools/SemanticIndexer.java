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
package org.apache.atlas.tools;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.FullTextMapperV2;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticNotificationFilter;
import org.apache.atlas.semantic.SemanticSearchConfiguration;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.SSLUtil;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.TopicPartition;

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

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> running = false));

            SSLUtil sslUtil = new SSLUtil();
            sslUtil.setSSLContext();

            Configuration config = ApplicationProperties.get();
            config.setProperty("atlas.kafka.entities.group.id", SemanticSearchConfiguration.getSemanticIndexerKafkaGroupId());

            AtlasJanusGraphDatabase.getGraphInstance();
            OpenSearchSemanticStore semanticStore = new OpenSearchSemanticStore();
            semanticStore.initialize();

            AtlasGraph graph = AtlasGraphProvider.getGraphInstance();
            AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
            FullTextMapperV2 fullTextMapper = new FullTextMapperV2(graph, typeRegistry, config);
            SemanticTextBuilder textBuilder = new SemanticTextBuilder(fullTextMapper, typeRegistry);
            SemanticEntityIndexer entityIndexer = new SemanticEntityIndexer(textBuilder, semanticStore);

            KafkaNotification kafkaNotification = new KafkaNotification(config);
            NotificationConsumer consumer =
                    kafkaNotification.createConsumers(NotificationType.ENTITIES, 1, false).get(0);

            int batchSize = SemanticSearchConfiguration.getSemanticIndexerBatchSize();
            LOG.info("Semantic Indexer started (batchSize={}, groupId={})", batchSize,
                    SemanticSearchConfiguration.getSemanticIndexerKafkaGroupId());

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

                    guids.addAll(SemanticNotificationFilter.extractGuids(entityNotification));
                }

                if (!guids.isEmpty()) {
                    LOG.debug("Processing {} guid(s) from {} Kafka message(s)", guids.size(), messages.size());
                    processInBatches(entityIndexer, guids, batchSize);
                }

                for (AtlasKafkaMessage<EntityNotification> kafkaMessage : messages) {
                    consumer.commit(kafkaMessage.getTopicPartition(), kafkaMessage.getOffset() + 1);
                }
            }

            exitCode = 0;
        } catch (Exception e) {
            LOG.error("Semantic Indexer failed", e);
        } finally {
            LOG.info("Semantic Indexer exiting with code {}", exitCode);
            System.exit(exitCode);
        }
    }

    private static void processInBatches(SemanticEntityIndexer entityIndexer, Set<String> guids, int batchSize) {
        Set<String> batch = new LinkedHashSet<>();
        for (String guid : guids) {
            batch.add(guid);
            if (batch.size() >= batchSize) {
                entityIndexer.indexGuids(batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            entityIndexer.indexGuids(batch);
        }
    }
}
