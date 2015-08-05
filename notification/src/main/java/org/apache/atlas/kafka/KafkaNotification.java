/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.inject.Singleton;
import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

@Singleton
public class KafkaNotification extends NotificationInterface {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaNotification.class);

    public static final String PROPERTY_PREFIX = NotificationInterface.PROPERTY_PREFIX + ".kafka";

    private static final int ATLAS_ZK_PORT = 9026;
    private static final int ATLAS_KAFKA_PORT = 9027;
    private static final String ATLAS_KAFKA_DATA = "data";

    public static final String ATLAS_HOOK_TOPIC = "ATLAS_HOOK";
    public static final String ATLAS_ENTITIES_TOPIC = "ATLAS_ENTITIES";
    public static final String ATLAS_TYPES_TOPIC = "ATLAS_TYPES";

    private static final String ATLAS_GROUP = "atlas";
    private KafkaServer kafkaServer;
    private ServerCnxnFactory factory;
    private Properties properties;

    private KafkaProducer producer = null;
    private List<ConsumerConnector> consumerConnectors = new ArrayList<>();

    private KafkaConsumer consumer;

    private static final Map<NotificationType, String> topicMap = new HashMap<NotificationType, String>() {{
        put(NotificationType.HOOK, ATLAS_HOOK_TOPIC);
        put(NotificationType.ENTITIES, ATLAS_ENTITIES_TOPIC);
        put(NotificationType.TYPES, ATLAS_TYPES_TOPIC);
    }};

    private synchronized void createProducer() {
        if (producer == null) {
            producer = new KafkaProducer(properties);
        }
    }

    @Override
    public void initialize(Configuration applicationProperties) throws AtlasException {
        super.initialize(applicationProperties);
        Configuration subsetConfiguration =
                ApplicationProperties.getSubsetConfiguration(applicationProperties, PROPERTY_PREFIX);
        properties = ConfigurationConverter.getProperties(subsetConfiguration);
        //override to store offset in kafka
        //todo do we need ability to replay?

        //Override default configs
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        //todo take group id as argument to allow multiple consumers??
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, ATLAS_GROUP);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");

        if (isEmbedded()) {
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + ATLAS_KAFKA_PORT);
            properties.setProperty("zookeeper.connect", "localhost:" + ATLAS_ZK_PORT);
        }

        //todo new APIs not available yet
//        consumer = new KafkaConsumer(properties);
//        consumer.subscribe(ATLAS_HOOK_TOPIC);
    }

    @Override
    protected void _startService() throws IOException {
        startZk();
        startKafka();
    }

    private String startZk() throws IOException {
        //todo read zk endpoint from config
        this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("0.0.0.0", ATLAS_ZK_PORT), 1024);
        File snapshotDir = constructDir("zk/txn");
        File logDir = constructDir("zk/snap");

        try {
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return factory.getLocalAddress().getAddress().toString();
    }

    private void startKafka() {
        Properties brokerConfig = properties;
        brokerConfig.setProperty("broker.id", "1");
        //todo read kafka endpoint from config
        brokerConfig.setProperty("host.name", "0.0.0.0");
        brokerConfig.setProperty("port", String.valueOf(ATLAS_KAFKA_PORT));
        brokerConfig.setProperty("log.dirs", constructDir("kafka").getAbsolutePath());
        brokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));

        kafkaServer = new KafkaServer(new KafkaConfig(brokerConfig), new SystemTime());
        kafkaServer.startup();
        LOG.debug("Embedded kafka server started with broker config {}", brokerConfig);
    }

    private static class SystemTime implements Time {
        @Override
        public long milliseconds() {
            return System.currentTimeMillis();
        }

        @Override
        public long nanoseconds() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long arg0) {
            try {
                Thread.sleep(arg0);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private File constructDir(String dirPrefix) {
        File file = new File(properties.getProperty(ATLAS_KAFKA_DATA), dirPrefix);
        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }
        return file;
    }

    @Override
    public void _shutdown() {
        if (producer != null) {
            producer.close();
        }

        if (consumer != null) {
            consumer.close();
        }

        for (ConsumerConnector consumerConnector : consumerConnectors) {
            consumerConnector.shutdown();
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (factory != null) {
            factory.shutdown();
        }
    }

    @Override
    public List<NotificationConsumer> createConsumers(NotificationType type, int numConsumers) {
        String topic = topicMap.get(type);

        ConsumerConnector consumerConnector =
                Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(properties));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, numConsumers);
        StringDecoder decoder = new StringDecoder(null);
        Map<String, List<KafkaStream<String, String>>> streamsMap =
                consumerConnector.createMessageStreams(topicCountMap, decoder, decoder);
        List<KafkaStream<String, String>> kafkaConsumers = streamsMap.get(topic);
        List<NotificationConsumer> consumers = new ArrayList<>(numConsumers);
        int consumerId = 0;
        for (KafkaStream stream : kafkaConsumers) {
            consumers.add(new org.apache.atlas.kafka.KafkaConsumer(stream, consumerId++));
        }
        consumerConnectors.add(consumerConnector);

        return consumers;
    }

    @Override
    public void send(NotificationType type, String... messages) throws NotificationException {
        if (producer == null) {
            createProducer();
        }

        String topic = topicMap.get(type);
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (String message : messages) {
            ProducerRecord record = new ProducerRecord(topic, message);
            LOG.debug("Sending message for topic {}: {}", topic, message);
            futures.add(producer.send(record));
        }

        for (Future<RecordMetadata> future : futures) {
            try {
                RecordMetadata response = future.get();
                LOG.debug("Sent message for topic - {}, partition - {}, offset - {}", response.topic(),
                        response.partition(), response.offset());
            } catch (Exception e) {
                throw new NotificationException(e);
            }
        }
    }

    //New API, not used now
    private List<String> receive(long timeout) throws NotificationException {
        Map<String, ConsumerRecords> recordsMap = consumer.poll(timeout);
        List<String> messages = new ArrayList<>();
        if (recordsMap != null) {
            for (ConsumerRecords records : recordsMap.values()) {
                List<ConsumerRecord> recordList = records.records();
                for (ConsumerRecord record : recordList) {
                    try {
                        String message = (String) record.value();
                        LOG.debug("Received message from topic {}: {}", ATLAS_HOOK_TOPIC, message);
                        messages.add(message);
                    } catch (Exception e) {
                        throw new NotificationException(e);
                    }
                }
            }
        }
        return messages;
    }
}
