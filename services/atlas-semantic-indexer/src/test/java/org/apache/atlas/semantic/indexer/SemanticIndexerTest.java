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

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.semantic.SemanticEntityEmbedder;
import org.apache.atlas.semantic.SemanticEntityEmbedder.IndexStats;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class SemanticIndexerTest {
    private static final String         TOPIC       = "ATLAS_ENTITIES";
    private static final TopicPartition PARTITION_0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition(TOPIC, 1);

    @Test
    public void commitOffsetsCommitsTheNextOffsetOfEachPartition() {
        NotificationConsumer<EntityNotification> consumer = consumer();

        SemanticIndexer.commitOffsets(consumer, Arrays.asList(
                message("g1", OperationType.ENTITY_UPDATE, 7, 0),
                message("g2", OperationType.ENTITY_UPDATE, 5, 0),
                message("g3", OperationType.ENTITY_UPDATE, 3, 1)));

        verify(consumer).commit(PARTITION_0, 8L);
        verify(consumer).commit(PARTITION_1, 4L);
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void commitOffsetsRetriesOnceAfterWakeup() {
        NotificationConsumer<EntityNotification> consumer = consumer();
        doThrow(new WakeupException()).doNothing().when(consumer).commit(PARTITION_0, 2L);

        SemanticIndexer.commitOffsets(consumer, Arrays.asList(message("g1", OperationType.ENTITY_UPDATE, 1, 0)));

        verify(consumer, times(2)).commit(PARTITION_0, 2L);
    }

    @Test
    public void commitOffsetsKeepsGoingWhenACommitFails() {
        NotificationConsumer<EntityNotification> consumer = consumer();
        doThrow(new KafkaException("rebalanced")).when(consumer).commit(PARTITION_0, 2L);

        SemanticIndexer.commitOffsets(consumer, Arrays.asList(
                message("g1", OperationType.ENTITY_UPDATE, 1, 0),
                message("g2", OperationType.ENTITY_UPDATE, 1, 1)));

        verify(consumer).commit(PARTITION_0, 2L);
        verify(consumer).commit(PARTITION_1, 2L);
    }

    @Test
    public void processMessagesEmbedsEachGuidOnceAndSkipsDeletes() {
        SemanticEntityEmbedder embedder = mock(SemanticEntityEmbedder.class);
        AtlasGraph             graph    = mock(AtlasGraph.class);
        when(embedder.embed(any())).thenReturn(new IndexStats(2, 0, 0));

        try (MockedStatic<AtlasGraphProvider> graphProvider = mockStatic(AtlasGraphProvider.class)) {
            graphProvider.when(AtlasGraphProvider::getGraphInstance).thenReturn(graph);

            IndexStats stats = SemanticIndexer.processMessages(Arrays.asList(
                    message("g1", OperationType.ENTITY_CREATE, 1, 0),
                    message("g1", OperationType.ENTITY_UPDATE, 2, 0),
                    message("g2", OperationType.CLASSIFICATION_ADD, 3, 0),
                    message("g3", OperationType.ENTITY_DELETE, 4, 0)), embedder);

            assertEquals(stats.getIndexed(), 2);
            verify(embedder).embed(new LinkedHashSet<>(Arrays.asList("g1", "g2")));
            verify(graph).rollback();
        }
    }

    @SuppressWarnings("unchecked")
    private static NotificationConsumer<EntityNotification> consumer() {
        NotificationConsumer<EntityNotification> consumer = mock(NotificationConsumer.class);
        doNothing().when(consumer).commit(any(), anyLong());
        return consumer;
    }

    private static AtlasKafkaMessage<EntityNotification> message(String guid, OperationType operation, long offset, int partition) {
        AtlasEntityHeader entity = new AtlasEntityHeader("hive_table");
        entity.setGuid(guid);

        return new AtlasKafkaMessage<>(new EntityNotificationV2(entity, operation), offset, TOPIC, partition);
    }
}
