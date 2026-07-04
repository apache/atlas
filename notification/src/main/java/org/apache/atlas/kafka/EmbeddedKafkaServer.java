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

import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@Component
@Order(3)
public class EmbeddedKafkaServer implements Service {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedKafkaServer.class);

    public static final String PROPERTY_PREFIX   = "atlas.kafka";
    public static final String PROPERTY_EMBEDDED = "atlas.notification.embedded";

    private static final String ATLAS_KAFKA_DATA = "data";

    private final boolean       isEmbedded;
    private final Configuration applicationProperties;
    private final Properties    properties;

    private KafkaClusterTestKit cluster;

    @Inject
    public EmbeddedKafkaServer(Configuration applicationProperties) {
        this.applicationProperties = applicationProperties;
        Configuration kafkaConf    = ApplicationProperties.getSubsetConfiguration(applicationProperties, PROPERTY_PREFIX);

        this.isEmbedded = applicationProperties.getBoolean(PROPERTY_EMBEDDED, false);
        this.properties = ConfigurationConverter.getProperties(kafkaConf);
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("==> EmbeddedKafkaServer.start(isEmbedded={})", isEmbedded);

        if (isEmbedded) {
            try {
                startKraftBroker();
            } catch (Exception e) {
                throw new AtlasException("Failed to start embedded kafka", e);
            }
        } else {
            LOG.info("==> EmbeddedKafkaServer.start(): not embedded..nothing todo");
        }

        LOG.info("<== EmbeddedKafkaServer.start(isEmbedded={})", isEmbedded);
    }

    @Override
    public void stop() {
        LOG.info("==> EmbeddedKafkaServer.stop(isEmbedded={})", isEmbedded);

        if (cluster != null) {
            AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();

            Utils.closeQuietly(cluster, "embedded Kafka cluster", shutdownFailure);

            if (shutdownFailure.get() != null) {
                LOG.warn("Failed to shut down embedded Kafka cluster", shutdownFailure.get());
            }

            cluster = null;
        }

        LOG.info("<== EmbeddedKafka.stop(isEmbedded={})", isEmbedded);
    }

    private void startKraftBroker() throws Exception {
        overrideExitMethods();

        File logDir = constructDir("kafka");

        LOG.info("Starting embedded KRaft kafka (log.dir={})", logDir.getAbsolutePath());

        KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setCombined(true)
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build());

        Properties brokerConfig = buildBrokerConfig(logDir);

        brokerConfig.forEach((key, value) -> clusterBuilder.setConfigProp(key.toString(), value));

        cluster = clusterBuilder.build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();

        String bootstrapServers = cluster.clientProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString();

        LOG.info("Embedded KRaft kafka server started at {}", bootstrapServers);

        applicationProperties.setProperty(PROPERTY_PREFIX + ".bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    private Properties buildBrokerConfig(File logDir) {
        Properties brokerConfig = new Properties();

        brokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        brokerConfig.setProperty("delete.topic.enable", "true");
        brokerConfig.setProperty("group.initial.rebalance.delay.ms", "0");
        brokerConfig.setProperty("offsets.topic.replication.factor", "1");
        brokerConfig.setProperty("transaction.state.log.replication.factor", "1");
        brokerConfig.setProperty("transaction.state.log.min.isr", "1");
        brokerConfig.setProperty("num.partitions", "1");

        String replicationFactor = properties.getProperty("offsets.topic.replication.factor");

        if (replicationFactor != null) {
            brokerConfig.setProperty("offsets.topic.replication.factor", replicationFactor);
        }

        return brokerConfig;
    }

    private void overrideExitMethods() {
        Exit.setExitProcedure((statusCode, message) ->
                LOG.warn("Kafka Exit.exit({}, {}) suppressed in embedded broker", statusCode, message));
        Exit.setHaltProcedure((statusCode, message) ->
                LOG.warn("Kafka Exit.halt({}, {}) suppressed in embedded broker", statusCode, message));
    }

    private File constructDir(String dirPrefix) {
        File file = new File(properties.getProperty(ATLAS_KAFKA_DATA), dirPrefix);

        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }

        return file;
    }
}
