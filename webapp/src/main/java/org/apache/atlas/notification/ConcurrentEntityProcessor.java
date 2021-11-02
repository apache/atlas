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
package org.apache.atlas.notification;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.pc.ConsumerBuilder;
import org.apache.atlas.notification.pc.Manager;
import org.apache.atlas.notification.pc.MiscUtils;
import org.apache.atlas.notification.pc.ReferenceKeeper;
import org.apache.atlas.notification.pc.Ticket;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

import java.util.Map;

public class ConcurrentEntityProcessor implements NotificationEntityProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentEntityProcessor.class);

    private static final String PROPERTY_CONSUMER_NUM_WORKERS = "atlas.notification.consumer.numWorkers";
    private static final String PROPERTY_CONSUMER_BATCH_SIZE  = "atlas.notification.consumer.batchSize";
    private static final int    CONSUMER_NUM_WORKERS_DEFAULT  = 10;
    private static final int    CONSUMER_BATCH_SIZE_DEFAULT   = 100;

    private static int     consumerNumWorkers = CONSUMER_NUM_WORKERS_DEFAULT;
    private static int     consumerBatchSize  = CONSUMER_BATCH_SIZE_DEFAULT;
    private final  Manager manager;
    private final  ReferenceKeeper referenceKeeper;

    public ConcurrentEntityProcessor(Configuration applicationProperties, AtlasMetricsUtil metricsUtil, Map<String, Authentication> authnCache,
            AtlasEntityStore entityStore, AtlasInstanceConverter instanceConverter, EntityCorrelationManager entityCorrelationManager,
            AtlasTypeRegistry typeRegistry, Logger failedLog, Logger largeMessagesLog,
            AsyncImporter asyncImporter) {
        this.referenceKeeper = new ReferenceKeeper();

        ConsumerBuilder consumerBuilder = new ConsumerBuilder(this.referenceKeeper, applicationProperties, metricsUtil, authnCache, entityStore,
                instanceConverter, entityCorrelationManager, typeRegistry, failedLog, largeMessagesLog, asyncImporter);

        this.manager = new Manager(this.referenceKeeper, consumerBatchSize, consumerNumWorkers, consumerBuilder);
        MiscUtils.extractAllProcessTypeNames(Ticket.getProcessNameTypes(), typeRegistry);
    }

    @Override
    public TopicPartitionOffsetResult handleMessage(AtlasKafkaMessage<HookNotification> msg) {
        return this.manager.submit(new Ticket(msg));
    }

    @Override
    public TopicPartitionOffsetResult collectResults() {
        return this.manager.getResult();
    }

    @Override
    public void shutdown() {
        this.manager.shutdown();
    }

    static {
        try {
            Configuration config = ApplicationProperties.get();
            consumerNumWorkers = config.getInt(PROPERTY_CONSUMER_NUM_WORKERS, CONSUMER_NUM_WORKERS_DEFAULT);
            if (consumerNumWorkers <= 0) {
                LOG.warn("{} set to invalid value: {}. Using default.", PROPERTY_CONSUMER_NUM_WORKERS, consumerNumWorkers);
                consumerNumWorkers = CONSUMER_NUM_WORKERS_DEFAULT;
            }

            LOG.info("{} = {}", PROPERTY_CONSUMER_NUM_WORKERS, consumerNumWorkers);
            consumerBatchSize = config.getInt(PROPERTY_CONSUMER_BATCH_SIZE, CONSUMER_BATCH_SIZE_DEFAULT);
            if (consumerBatchSize <= 0) {
                LOG.warn("{} set to invalid value: {}. Using default.", PROPERTY_CONSUMER_BATCH_SIZE, CONSUMER_BATCH_SIZE_DEFAULT);
                consumerBatchSize = CONSUMER_BATCH_SIZE_DEFAULT;
            }

            LOG.info("{} = {}", PROPERTY_CONSUMER_BATCH_SIZE, consumerBatchSize);
        } catch (Exception exception) {
            LOG.error("Error fetching configuration. Will use default!");
        }
    }
}
