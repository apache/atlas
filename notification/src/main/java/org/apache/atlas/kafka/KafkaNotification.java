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
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka specific access point to the Atlas notification framework.
 */
@Singleton
public class KafkaNotification extends AbstractNotification implements Service {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaNotification.class);

    public static final String PROPERTY_PREFIX = "atlas.kafka";

    private static final String ATLAS_KAFKA_DATA = "data";

    public static final String ATLAS_HOOK_TOPIC = "ATLAS_HOOK";
    public static final String ATLAS_ENTITIES_TOPIC = "ATLAS_ENTITIES";

    protected static final String CONSUMER_GROUP_ID_PROPERTY = "group.id";

    private KafkaServer kafkaServer;
    private ServerCnxnFactory factory;
    private Properties properties;

    private KafkaProducer producer = null;
    private List<ConsumerConnector> consumerConnectors = new ArrayList<>();

    private static final Map<NotificationType, String> TOPIC_MAP = new HashMap<NotificationType, String>() {
        {
            put(NotificationType.HOOK, ATLAS_HOOK_TOPIC);
            put(NotificationType.ENTITIES, ATLAS_ENTITIES_TOPIC);
        }
    };


    // ----- Constructors ----------------------------------------------------

    /**
     * Construct a KafkaNotification.
     *
     * @param applicationProperties  the application properties used to configure Kafka
     *
     * @throws AtlasException if the notification interface can not be created
     */
    public KafkaNotification(Configuration applicationProperties) throws AtlasException {
        super(applicationProperties);
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

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "roundrobin");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
    }


    // ----- Service ---------------------------------------------------------

    @Override
    public void start() throws AtlasException {
        if (isEmbedded()) {
            try {
                startZk();
                startKafka();
            } catch (Exception e) {
                throw new AtlasException("Failed to start embedded kafka", e);
            }
        }
    }

    @Override
    public void stop() {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (factory != null) {
            factory.shutdown();
        }
    }


    // ----- NotificationInterface -------------------------------------------

    @Override
    public <T> List<NotificationConsumer<T>> createConsumers(NotificationType notificationType,
                                                             int numConsumers) {
        String topic = TOPIC_MAP.get(notificationType);

        Properties consumerProperties = getConsumerProperties(notificationType);

        ConsumerConnector consumerConnector = createConsumerConnector(consumerProperties);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, numConsumers);
        StringDecoder decoder = new StringDecoder(null);
        Map<String, List<KafkaStream<String, String>>> streamsMap =
                consumerConnector.createMessageStreams(topicCountMap, decoder, decoder);
        List<KafkaStream<String, String>> kafkaConsumers = streamsMap.get(topic);
        List<NotificationConsumer<T>> consumers = new ArrayList<>(numConsumers);
        int consumerId = 0;
        for (KafkaStream stream : kafkaConsumers) {
            consumers.add(createKafkaConsumer(notificationType.getClassType(), stream, consumerId++));
        }
        consumerConnectors.add(consumerConnector);

        return consumers;
    }

    @Override
    public void sendInternal(NotificationType type, String... messages) throws NotificationException {
        if (producer == null) {
            createProducer();
        }

        String topic = TOPIC_MAP.get(type);
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

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }

        for (ConsumerConnector consumerConnector : consumerConnectors) {
            consumerConnector.shutdown();
        }
        consumerConnectors.clear();
    }


    // ----- helper methods --------------------------------------------------

    /**
     * Create a Kafka consumer connector from the given properties.
     *
     * @param consumerProperties  the properties for creating the consumer connector
     *
     * @return a new Kafka consumer connector
     */
    protected ConsumerConnector createConsumerConnector(Properties consumerProperties) {
        return Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerProperties));
    }

    /**
     * Create a Kafka consumer from the given Kafka stream.
     *
     * @param stream      the Kafka stream
     * @param consumerId  the id for the new consumer
     *
     * @return a new Kafka consumer
     */
    protected <T> org.apache.atlas.kafka.KafkaConsumer<T> createKafkaConsumer(Class<T> type, KafkaStream stream,
                                                                              int consumerId) {
        return new org.apache.atlas.kafka.KafkaConsumer<T>(type, stream, consumerId);
    }

    // Get properties for consumer request
    private Properties getConsumerProperties(NotificationType type) {
        // find the configured group id for the given notification type
        String groupId = properties.getProperty(type.toString().toLowerCase() + "." + CONSUMER_GROUP_ID_PROPERTY);

        if (groupId == null) {
            throw new IllegalStateException("No configuration group id set for the notification type " + type);
        }

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(properties);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return consumerProperties;
    }

    private File constructDir(String dirPrefix) {
        File file = new File(properties.getProperty(ATLAS_KAFKA_DATA), dirPrefix);
        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }
        return file;
    }

    private synchronized void createProducer() {
        if (producer == null) {
            producer = new KafkaProducer(properties);
        }
    }

    private URL getURL(String url) throws MalformedURLException {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            return new URL("http://" + url);
        }
    }

    private String startZk() throws IOException, InterruptedException, URISyntaxException {
        String zkValue = properties.getProperty("zookeeper.connect");
        LOG.debug("Starting zookeeper at {}", zkValue);

        URL zkAddress = getURL(zkValue);
        this.factory = NIOServerCnxnFactory.createFactory(
                new InetSocketAddress(zkAddress.getHost(), zkAddress.getPort()), 1024);
        File snapshotDir = constructDir("zk/txn");
        File logDir = constructDir("zk/snap");

        factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));
        return factory.getLocalAddress().getAddress().toString();
    }

    private void startKafka() throws IOException, URISyntaxException {
        String kafkaValue = properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        LOG.debug("Starting kafka at {}", kafkaValue);
        URL kafkaAddress = getURL(kafkaValue);

        Properties brokerConfig = properties;
        brokerConfig.setProperty("broker.id", "1");
        brokerConfig.setProperty("host.name", kafkaAddress.getHost());
        brokerConfig.setProperty("port", String.valueOf(kafkaAddress.getPort()));
        brokerConfig.setProperty("log.dirs", constructDir("kafka").getAbsolutePath());
        brokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));

        kafkaServer = new KafkaServer(KafkaConfig.fromProps(brokerConfig), new SystemTime(),
                Option.apply(this.getClass().getName()));
        kafkaServer.startup();
        LOG.debug("Embedded kafka server started with broker config {}", brokerConfig);
    }


    // ----- inner class : SystemTime ----------------------------------------

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
}
