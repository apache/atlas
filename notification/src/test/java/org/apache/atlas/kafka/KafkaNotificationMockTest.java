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
package org.apache.atlas.kafka;

import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class KafkaNotificationMockTest {
    @Test
    public void testCreateConsumers() {
        Properties properties = mock(Properties.class);

        when(properties.getProperty("entities.group.id")).thenReturn("atlas");

        Map<String, Integer> topicCountMap = new HashMap<>();

        topicCountMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, 1);

        final AtlasKafkaConsumer<String> consumer1 = mock(AtlasKafkaConsumer.class);
        final AtlasKafkaConsumer<String> consumer2 = mock(AtlasKafkaConsumer.class);

        KafkaNotification kafkaNotification = new TestKafkaNotification<>(properties, consumer1, consumer2);

        List<NotificationConsumer<AtlasKafkaConsumer<?>>> consumers = kafkaNotification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 2);

        assertEquals(consumers.size(), 2);
        assertTrue(consumers.contains(consumer1));
        assertTrue(consumers.contains(consumer2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendMessagesSuccessfully() throws NotificationException, ExecutionException, InterruptedException {
        Properties        configProperties  = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer<String, String> producer       = mock(Producer.class);
        String                   topicName      = kafkaNotification.getProducerTopicName(NotificationInterface.NotificationType.HOOK);
        String                   message        = "This is a test message";
        Future<RecordMetadata>   returnValue    = mock(Future.class);
        TopicPartition           topicPartition = new TopicPartition(topicName, 0);

        when(returnValue.get()).thenReturn(new RecordMetadata(topicPartition, 0, 0, 0, 0L, 0, 0));

        ProducerRecord<String, String> expectedRecord = new ProducerRecord<>(topicName, message);

        when(producer.send(expectedRecord)).thenReturn(returnValue);

        kafkaNotification.sendInternalToProducer(producer, NotificationInterface.NotificationType.HOOK, new ArrayList<>(Collections.singletonList(message)));

        verify(producer).send(expectedRecord);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowExceptionIfProducerFails() throws ExecutionException, InterruptedException {
        Properties        configProperties  = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer<String, String> producer    = mock(Producer.class);
        String                   topicName   = kafkaNotification.getProducerTopicName(NotificationInterface.NotificationType.HOOK);
        String                   message     = "This is a test message";
        Future<RecordMetadata>   returnValue = mock(Future.class);

        when(returnValue.get()).thenThrow(new RuntimeException("Simulating exception"));

        ProducerRecord<String, String> expectedRecord = new ProducerRecord<>(topicName, message);

        when(producer.send(expectedRecord)).thenReturn(returnValue);

        try {
            kafkaNotification.sendInternalToProducer(producer, NotificationInterface.NotificationType.HOOK, new ArrayList<>(Collections.singletonList(message)));

            fail("Should have thrown NotificationException");
        } catch (NotificationException e) {
            assertEquals(e.getFailedMessages().size(), 1);
            assertEquals(e.getFailedMessages().get(0), "This is a test message");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCollectAllFailedMessagesIfProducerFails() throws ExecutionException, InterruptedException {
        Properties        configProperties  = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer<String, String> producer     = mock(Producer.class);
        String                   topicName    = kafkaNotification.getProducerTopicName(NotificationInterface.NotificationType.HOOK);
        String                   message1     = "This is a test message1";
        String                   message2     = "This is a test message2";
        Future<RecordMetadata>   returnValue1 = mock(Future.class);

        when(returnValue1.get()).thenThrow(new RuntimeException("Simulating exception"));

        Future<RecordMetadata> returnValue2 = mock(Future.class);

        when(returnValue2.get()).thenThrow(new RuntimeException("Simulating exception"));

        ProducerRecord<String, String> expectedRecord1 = new ProducerRecord<>(topicName, message1);

        when(producer.send(expectedRecord1)).thenReturn(returnValue1);

        ProducerRecord<String, String> expectedRecord2 = new ProducerRecord<>(topicName, message2);

        when(producer.send(expectedRecord2)).thenReturn(returnValue1);

        try {
            kafkaNotification.sendInternalToProducer(producer, NotificationInterface.NotificationType.HOOK, Arrays.asList(message1, message2));

            fail("Should have thrown NotificationException");
        } catch (NotificationException e) {
            assertEquals(e.getFailedMessages().size(), 2);
            assertEquals(e.getFailedMessages().get(0), "This is a test message1");
            assertEquals(e.getFailedMessages().get(1), "This is a test message2");
        }
    }

    static class TestKafkaNotification<T> extends KafkaNotification {
        private final AtlasKafkaConsumer<T> consumer1;
        private final AtlasKafkaConsumer<T> consumer2;

        TestKafkaNotification(Properties properties, AtlasKafkaConsumer<T> consumer1, AtlasKafkaConsumer<T> consumer2) {
            super(properties);

            this.consumer1 = consumer1;
            this.consumer2 = consumer2;
        }

        @Override
        public List<NotificationConsumer<T>> createConsumers(NotificationType notificationType, int numConsumers) {
            List<NotificationConsumer<T>> consumerList = new ArrayList<>();

            consumerList.add(consumer1);
            consumerList.add(consumer2);

            return consumerList;
        }

        protected AtlasKafkaConsumer<T> createConsumers(Class<T> type, int consumerId, boolean autoCommitEnabled) {
            if (consumerId == 0) {
                return consumer1;
            } else if (consumerId == 1) {
                return consumer2;
            }

            return null;
        }
    }
}
