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
package org.apache.atlas.kafka;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import org.apache.atlas.notification.MessageDeserializer;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class KafkaNotificationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateConsumers() throws Exception {
        Properties properties = mock(Properties.class);
        when(properties.getProperty("entities.group.id")).thenReturn("atlas");
        final ConsumerConnector consumerConnector = mock(ConsumerConnector.class);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, 1);

        Map<String, List<KafkaStream<String, String>>> kafkaStreamsMap =
                new HashMap<>();
        List<KafkaStream<String, String>> kafkaStreams = new ArrayList<>();
        KafkaStream kafkaStream = mock(KafkaStream.class);
        kafkaStreams.add(kafkaStream);
        kafkaStreamsMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, kafkaStreams);

        when(consumerConnector.createMessageStreams(
                eq(topicCountMap), any(StringDecoder.class), any(StringDecoder.class))).thenReturn(kafkaStreamsMap);

        final KafkaConsumer consumer1 = mock(KafkaConsumer.class);
        final KafkaConsumer consumer2 = mock(KafkaConsumer.class);

        KafkaNotification kafkaNotification =
                new TestKafkaNotification(properties, consumerConnector, consumer1, consumer2);

        List<NotificationConsumer<String>> consumers =
                kafkaNotification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 2);

        verify(consumerConnector, times(2)).createMessageStreams(
                eq(topicCountMap), any(StringDecoder.class), any(StringDecoder.class));
        assertEquals(consumers.size(), 2);
        assertTrue(consumers.contains(consumer1));
        assertTrue(consumers.contains(consumer2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendMessagesSuccessfully() throws NotificationException,
            ExecutionException, InterruptedException {
        Properties configProperties = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer producer = mock(Producer.class);
        String topicName = kafkaNotification.getTopicName(NotificationInterface.NotificationType.HOOK);
        String message = "This is a test message";
        Future returnValue = mock(Future.class);
        when(returnValue.get()).thenReturn(new RecordMetadata(new TopicPartition(topicName, 0), 0, 0));
        ProducerRecord expectedRecord = new ProducerRecord(topicName, message);
        when(producer.send(expectedRecord)).thenReturn(returnValue);

        kafkaNotification.sendInternalToProducer(producer,
                NotificationInterface.NotificationType.HOOK, new String[]{message});

        verify(producer).send(expectedRecord);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowExceptionIfProducerFails() throws NotificationException,
            ExecutionException, InterruptedException {
        Properties configProperties = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer producer = mock(Producer.class);
        String topicName = kafkaNotification.getTopicName(NotificationInterface.NotificationType.HOOK);
        String message = "This is a test message";
        Future returnValue = mock(Future.class);
        when(returnValue.get()).thenThrow(new RuntimeException("Simulating exception"));
        ProducerRecord expectedRecord = new ProducerRecord(topicName, message);
        when(producer.send(expectedRecord)).thenReturn(returnValue);

        try {
            kafkaNotification.sendInternalToProducer(producer,
                NotificationInterface.NotificationType.HOOK, new String[]{message});
            fail("Should have thrown NotificationException");
        } catch (NotificationException e) {
            assertEquals(e.getFailedMessages().size(), 1);
            assertEquals(e.getFailedMessages().get(0), "This is a test message");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCollectAllFailedMessagesIfProducerFails() throws NotificationException,
            ExecutionException, InterruptedException {
        Properties configProperties = mock(Properties.class);
        KafkaNotification kafkaNotification = new KafkaNotification(configProperties);

        Producer producer = mock(Producer.class);
        String topicName = kafkaNotification.getTopicName(NotificationInterface.NotificationType.HOOK);
        String message1 = "This is a test message1";
        String message2 = "This is a test message2";
        Future returnValue1 = mock(Future.class);
        when(returnValue1.get()).thenThrow(new RuntimeException("Simulating exception"));
        Future returnValue2 = mock(Future.class);
        when(returnValue2.get()).thenThrow(new RuntimeException("Simulating exception"));
        ProducerRecord expectedRecord1 = new ProducerRecord(topicName, message1);
        when(producer.send(expectedRecord1)).thenReturn(returnValue1);
        ProducerRecord expectedRecord2 = new ProducerRecord(topicName, message2);
        when(producer.send(expectedRecord2)).thenReturn(returnValue1);

        try {
            kafkaNotification.sendInternalToProducer(producer,
                    NotificationInterface.NotificationType.HOOK, new String[]{message1, message2});
            fail("Should have thrown NotificationException");
        } catch (NotificationException e) {
            assertEquals(e.getFailedMessages().size(), 2);
            assertEquals(e.getFailedMessages().get(0), "This is a test message1");
            assertEquals(e.getFailedMessages().get(1), "This is a test message2");
        }
    }

    class TestKafkaNotification extends KafkaNotification {

        private final ConsumerConnector consumerConnector;
        private final KafkaConsumer consumer1;
        private final KafkaConsumer consumer2;

        TestKafkaNotification(Properties properties, ConsumerConnector consumerConnector,
                              KafkaConsumer consumer1, KafkaConsumer consumer2) {
            super(properties);
            this.consumerConnector = consumerConnector;
            this.consumer1 = consumer1;
            this.consumer2 = consumer2;
        }

        @Override
        protected ConsumerConnector createConsumerConnector(Properties consumerProperties) {
            return consumerConnector;
        }

        @Override
        protected <T> org.apache.atlas.kafka.KafkaConsumer<T>
        createKafkaConsumer(Class<T> type, MessageDeserializer<T> deserializer, KafkaStream stream,
                            int consumerId, ConsumerConnector connector, boolean autoCommitEnabled) {
            if (consumerId == 0) {
                return consumer1;
            } else if (consumerId == 1) {
                return consumer2;
            }
            return null;
        }


    }
}
