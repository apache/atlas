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

import com.google.inject.Inject;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import org.apache.atlas.AtlasException;
import org.apache.atlas.notification.MessageDeserializer;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = NotificationModule.class)
public class KafkaNotificationTest {

    @Inject
    private KafkaNotification kafka;

    @BeforeClass
    public void setUp() throws Exception {
        kafka.start();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateConsumers() throws Exception {
        Configuration configuration = mock(Configuration.class);
        Iterator iterator = mock(Iterator.class);
        ConsumerConnector consumerConnector = mock(ConsumerConnector.class);
        KafkaStream kafkaStream1 = mock(KafkaStream.class);
        KafkaStream kafkaStream2 = mock(KafkaStream.class);
        String groupId = "groupId9999";

        when(configuration.subset(KafkaNotification.PROPERTY_PREFIX)).thenReturn(configuration);
        when(configuration.getKeys()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn("entities." + KafkaNotification.CONSUMER_GROUP_ID_PROPERTY);
        when(configuration.getList("entities." + KafkaNotification.CONSUMER_GROUP_ID_PROPERTY))
                .thenReturn(Collections.<Object>singletonList(groupId));

        Map<String, List<KafkaStream<String, String>>> streamsMap = new HashMap<>();
        List<KafkaStream<String, String>> kafkaStreamList = new LinkedList<>();
        kafkaStreamList.add(kafkaStream1);
        kafkaStreamList.add(kafkaStream2);
        streamsMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, kafkaStreamList);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, 2);

        when(consumerConnector.createMessageStreams(
            eq(topicCountMap), any(StringDecoder.class), any(StringDecoder.class))).thenReturn(streamsMap);

        TestKafkaNotification kafkaNotification = new TestKafkaNotification(configuration, consumerConnector);

        List<NotificationConsumer<String>> consumers =
            kafkaNotification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 2);

        assertEquals(2, consumers.size());

        // assert that all of the given kafka streams were used to create kafka consumers
        List<KafkaStream> streams = kafkaNotification.kafkaStreams;
        assertTrue(streams.contains(kafkaStream1));
        assertTrue(streams.contains(kafkaStream2));

        // assert that the given consumer group id was added to the properties used to create the consumer connector
        Properties properties = kafkaNotification.myProperties;
        assertEquals(groupId, properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @AfterClass
    public void teardown() throws Exception {
        kafka.stop();
    }

    // Extended kafka notification class for testing.
    private static class TestKafkaNotification extends KafkaNotification {

        private final ConsumerConnector consumerConnector;

        private Properties myProperties;
        private List<KafkaStream> kafkaStreams = new LinkedList<>();

        public TestKafkaNotification(Configuration applicationProperties,
                                     ConsumerConnector consumerConnector) throws AtlasException {
            super(applicationProperties);
            this.consumerConnector = consumerConnector;
        }

        @Override
        protected ConsumerConnector createConsumerConnector(Properties consumerProperties) {
            this.myProperties = consumerProperties;
            kafkaStreams.clear();
            return consumerConnector;
        }

        @Override
        protected <T> org.apache.atlas.kafka.KafkaConsumer<T> createKafkaConsumer(Class<T> type,
                                                                                  MessageDeserializer<T> deserializer,
                                                                                  KafkaStream stream,
                                                                                  int consumerId) {
            kafkaStreams.add(stream);
            return super.createKafkaConsumer(type, deserializer, stream, consumerId);
        }
    }
}
