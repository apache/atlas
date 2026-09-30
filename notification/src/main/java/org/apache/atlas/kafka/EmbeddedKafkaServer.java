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
import org.apache.atlas.util.CommandHandlerUtility;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.lang3.StringUtils;
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

    private static final String ATLAS_KAFKA_DATA          = "data";
    private static final int    MAX_RETRY_TO_ACQUIRE_PORT = 3;

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

        shutdownClusterQuietly();

        LOG.info("<== EmbeddedKafka.stop(isEmbedded={})", isEmbedded);
    }

    private void startKraftBroker() throws Exception {
        overrideExitMethods();

        File   logDir                = constructDir("kafka");
        String configuredBootstrap   = properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        LOG.info("Starting embedded KRaft kafka (log.dir={}, bootstrap.servers={})", logDir.getAbsolutePath(), configuredBootstrap);

        for (int attempt = 0; attempt < MAX_RETRY_TO_ACQUIRE_PORT; attempt++) {
            try {
                startKraftBrokerOnce(logDir, configuredBootstrap);
                return;
            } catch (Exception e) {
                LOG.warn("Attempt {}: failed to start embedded KRaft kafka", attempt, e);

                shutdownClusterQuietly();

                if (attempt == MAX_RETRY_TO_ACQUIRE_PORT - 1) {
                    throw e;
                }

                int port = parsePort(configuredBootstrap);

                if (port > 0) {
                    CommandHandlerUtility.tryKillingProcessUsingPort(port, attempt != 0);
                }
            }
        }
    }

    private void startKraftBrokerOnce(File logDir, String configuredBootstrap) throws Exception {
        KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setCombined(true)
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build());

        Properties brokerConfig = buildBrokerConfig(logDir, configuredBootstrap);

        brokerConfig.forEach((key, value) -> clusterBuilder.setConfigProp(key.toString(), value));

        cluster = clusterBuilder.build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();

        String bootstrapServers = StringUtils.isNotEmpty(configuredBootstrap)
                ? configuredBootstrap
                : cluster.clientProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).toString();

        LOG.info("Embedded KRaft kafka server started at {}", bootstrapServers);

        applicationProperties.setProperty(PROPERTY_PREFIX + ".bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    private Properties buildBrokerConfig(File logDir, String configuredBootstrap) {
        Properties brokerConfig = new Properties();

        brokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        brokerConfig.setProperty("delete.topic.enable", "true");
        brokerConfig.setProperty("auto.create.topics.enable", "true");
        brokerConfig.setProperty("group.initial.rebalance.delay.ms", "0");
        brokerConfig.setProperty("offsets.topic.replication.factor", "1");
        brokerConfig.setProperty("transaction.state.log.replication.factor", "1");
        brokerConfig.setProperty("transaction.state.log.min.isr", "1");
        brokerConfig.setProperty("num.partitions", "1");

        String listener = toExternalListenerAddress(configuredBootstrap);

        if (listener != null) {
            brokerConfig.setProperty("listeners", listener + ",CONTROLLER://localhost:0");
            brokerConfig.setProperty("advertised.listeners", listener);
        }

        String replicationFactor = properties.getProperty("offsets.topic.replication.factor");

        if (replicationFactor != null) {
            brokerConfig.setProperty("offsets.topic.replication.factor", replicationFactor);
        }

        return brokerConfig;
    }

    private void shutdownClusterQuietly() {
        if (cluster != null) {
            AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();

            Utils.closeQuietly(cluster, "embedded Kafka cluster", shutdownFailure);

            if (shutdownFailure.get() != null) {
                LOG.warn("Failed to shut down embedded Kafka cluster", shutdownFailure.get());
            }

            cluster = null;
        }
    }

    private static String toExternalListenerAddress(String bootstrapServers) {
        if (StringUtils.isEmpty(bootstrapServers)) {
            return null;
        }

        String hostPort = StringUtils.trim(bootstrapServers.split(",")[0]);

        if (StringUtils.isEmpty(hostPort)) {
            return null;
        }

        if (hostPort.contains("://")) {
            return hostPort.replaceFirst("^PLAINTEXT://", "EXTERNAL://");
        }

        return "EXTERNAL://" + hostPort;
    }

    private static int parsePort(String bootstrapServers) {
        if (StringUtils.isEmpty(bootstrapServers)) {
            return -1;
        }

        String hostPort = StringUtils.trim(bootstrapServers.split(",")[0]);
        int    colon    = hostPort.lastIndexOf(':');

        if (colon < 0) {
            return -1;
        }

        try {
            return Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
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
