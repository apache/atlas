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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import scala.Option;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka specific access point to the Atlas notification framework.
 */
@Component
@Order(3)
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
    private KafkaConsumer consumer = null;
    private KafkaProducer producer = null;
    private Long pollTimeOutMs = 1000L;

    private static final Map<NotificationType, String> TOPIC_MAP = new HashMap<NotificationType, String>() {
        {
            put(NotificationType.HOOK, ATLAS_HOOK_TOPIC);
            put(NotificationType.ENTITIES, ATLAS_ENTITIES_TOPIC);
        }
    };

    @VisibleForTesting
    String getTopicName(NotificationType notificationType) {
        return TOPIC_MAP.get(notificationType);
    }

    // ----- Constructors ----------------------------------------------------

    /**
     * Construct a KafkaNotification.
     *
     * @param applicationProperties  the application properties used to configure Kafka
     *
     * @throws AtlasException if the notification interface can not be created
     */
    @Inject
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
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        pollTimeOutMs = subsetConfiguration.getLong("poll.timeout.ms", 1000);
        boolean oldApiCommitEnbleFlag = subsetConfiguration.getBoolean("auto.commit.enable",false);
        //set old autocommit value if new autoCommit property is not set.
        properties.put("enable.auto.commit", subsetConfiguration.getBoolean("enable.auto.commit", oldApiCommitEnbleFlag));
        properties.put("session.timeout.ms", subsetConfiguration.getString("session.timeout.ms", "30000"));

    }

    @VisibleForTesting
    protected KafkaNotification(Properties properties) {
        this.properties = properties;
    }

    // ----- Service ---------------------------------------------------------

    @Override
    public void start() throws AtlasException {
        if (isHAEnabled()) {
            LOG.info("Not starting embedded instances when HA is enabled.");
            return;
        }
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
        return createConsumers(notificationType, numConsumers,
                Boolean.valueOf(properties.getProperty("enable.auto.commit", properties.getProperty("auto.commit.enable","false"))));
    }

    @VisibleForTesting
    public <T> List<NotificationConsumer<T>> createConsumers(NotificationType notificationType,
                                                      int numConsumers, boolean autoCommitEnabled) {

        Properties consumerProperties = getConsumerProperties(notificationType);

        List<NotificationConsumer<T>> consumers = new ArrayList<>();
        AtlasKafkaConsumer kafkaConsumer =new AtlasKafkaConsumer(notificationType, getKafkaConsumer(consumerProperties, notificationType, autoCommitEnabled), autoCommitEnabled, pollTimeOutMs);

        consumers.add(kafkaConsumer);
        return consumers;
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }


    // ----- AbstractNotification --------------------------------------------

    @Override
    public void sendInternal(NotificationType type, List<String> messages) throws NotificationException {
        if (producer == null) {
            createProducer();
        }
        sendInternalToProducer(producer, type, messages);
    }

    @VisibleForTesting
    void sendInternalToProducer(Producer p, NotificationType type, List<String> messages) throws NotificationException {
        String topic = TOPIC_MAP.get(type);
        List<MessageContext> messageContexts = new ArrayList<>();
        for (String message : messages) {
            ProducerRecord record = new ProducerRecord(topic, message);
            LOG.debug("Sending message for topic {}: {}", topic, message);
            Future future = p.send(record);
            messageContexts.add(new MessageContext(future, message));
        }

        List<String> failedMessages = new ArrayList<>();
        Exception lastFailureException = null;
        for (MessageContext context : messageContexts) {
            try {
                RecordMetadata response = context.getFuture().get();
                LOG.debug("Sent message for topic - {}, partition - {}, offset - {}", response.topic(),
                    response.partition(), response.offset());
            } catch (Exception e) {
                lastFailureException = e;
                failedMessages.add(context.getMessage());
            }
        }
        if (lastFailureException != null) {
            throw new NotificationException(lastFailureException, failedMessages);
        }
    }


    public KafkaConsumer  getKafkaConsumer(Properties consumerProperties, NotificationType type, boolean autoCommitEnabled) {
        if(this.consumer == null) {
            try {
                String topic = TOPIC_MAP.get(type);
                consumerProperties.put("enable.auto.commit", autoCommitEnabled);
                this.consumer = new KafkaConsumer(consumerProperties);
                this.consumer.subscribe(Arrays.asList(topic));
            }catch (Exception ee) {
                LOG.error("Exception in getKafkaConsumer ", ee);
            }
        }

        return this.consumer;
    }




    // Get properties for consumer request
    private Properties getConsumerProperties(NotificationType type) {
        // find the configured group id for the given notification type

        String groupId = properties.getProperty(type.toString().toLowerCase() + "." + CONSUMER_GROUP_ID_PROPERTY);
        if (StringUtils.isEmpty(groupId)) {
            throw new IllegalStateException("No configuration group id set for the notification type " + type);
        }

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(properties);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        LOG.info("Consumer property: atlas.kafka.enable.auto.commit: {}", consumerProperties.getProperty("enable.auto.commit"));
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

    private class MessageContext {

        private final Future<RecordMetadata> future;
        private final String message;

        public MessageContext(Future<RecordMetadata> future, String message) {
            this.future = future;
            this.message = message;
        }

        public Future<RecordMetadata> getFuture() {
            return future;
        }

        public String getMessage() {
            return message;
        }
    }
}
