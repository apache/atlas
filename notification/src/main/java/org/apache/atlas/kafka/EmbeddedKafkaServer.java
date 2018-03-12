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

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.collection.mutable.Buffer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;


@Component
@Order(2)
public class EmbeddedKafkaServer implements Service {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedKafkaServer.class);

    public  static final String PROPERTY_PREFIX   = "atlas.kafka";
    private static final String ATLAS_KAFKA_DATA  = "data";
    public  static final String PROPERTY_EMBEDDED = "atlas.notification.embedded";

    private final boolean           isEmbedded;
    private final Properties        properties;
    private       KafkaServer       kafkaServer;
    private       ServerCnxnFactory factory;


    @Inject
    public EmbeddedKafkaServer(Configuration applicationProperties) throws AtlasException {
        Configuration kafkaConf = ApplicationProperties.getSubsetConfiguration(applicationProperties, PROPERTY_PREFIX);

        this.isEmbedded = applicationProperties.getBoolean(PROPERTY_EMBEDDED, false);
        this.properties = ConfigurationConverter.getProperties(kafkaConf);
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("==> EmbeddedKafkaServer.start(isEmbedded={})", isEmbedded);

        if (isEmbedded) {
            try {
                startZk();
                startKafka();
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

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (factory != null) {
            factory.shutdown();
        }

        LOG.info("<== EmbeddedKafka.stop(isEmbedded={})", isEmbedded);
    }

    private String startZk() throws IOException, InterruptedException, URISyntaxException {
        String zkValue = properties.getProperty("zookeeper.connect");

        LOG.info("Starting zookeeper at {}", zkValue);

        URL zkAddress    = getURL(zkValue);
        File snapshotDir = constructDir("zk/txn");
        File logDir      = constructDir("zk/snap");

        factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress(zkAddress.getHost(), zkAddress.getPort()), 1024);

        factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));

        String ret = factory.getLocalAddress().getAddress().toString();

        LOG.info("Embedded zookeeper for Kafka started at {}", ret);

        return ret;
    }

    private void startKafka() throws IOException, URISyntaxException {
        String kafkaValue = properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        LOG.info("Starting kafka at {}", kafkaValue);

        URL        kafkaAddress = getURL(kafkaValue);
        Properties brokerConfig = properties;

        brokerConfig.setProperty("broker.id", "1");
        brokerConfig.setProperty("host.name", kafkaAddress.getHost());
        brokerConfig.setProperty("port", String.valueOf(kafkaAddress.getPort()));
        brokerConfig.setProperty("log.dirs", constructDir("kafka").getAbsolutePath());
        brokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));

        List<KafkaMetricsReporter>   metrics          = new ArrayList<>();
        Buffer<KafkaMetricsReporter> metricsReporters = scala.collection.JavaConversions.asScalaBuffer(metrics);

        kafkaServer = new KafkaServer(KafkaConfig.fromProps(brokerConfig), new SystemTime(), Option.apply(this.getClass().getName()), metricsReporters);

        kafkaServer.startup();

        LOG.info("Embedded kafka server started with broker config {}", brokerConfig);
    }

    private File constructDir(String dirPrefix) {
        File file = new File(properties.getProperty(ATLAS_KAFKA_DATA), dirPrefix);

        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }

        return file;
    }

    private URL getURL(String url) throws MalformedURLException {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            return new URL("http://" + url);
        }
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
        public long hiResClockMs() {
            return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
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
