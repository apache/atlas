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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityCorrelationStore;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AdaptiveWaiter;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.annotation.Order;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.notification.NotificationInterface.NotificationType.ASYNC_IMPORT;

/**
 * Consumer of notifications from hooks e.g., hive hook etc.
 */
@Component
@Order(5)
@DependsOn(value = {"atlasTypeDefStoreInitializer", "atlasTypeDefGraphStoreV2"})
public class NotificationHookConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG                = LoggerFactory.getLogger(NotificationHookConsumer.class);
    private static final Logger FAILED_LOG         = LoggerFactory.getLogger("FAILED");
    private static final Logger LARGE_MESSAGES_LOG = LoggerFactory.getLogger("LARGE_MESSAGES");

    // from org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
    public static final String DUMMY_DATABASE                                                = "_dummy_database";
    public static final String DUMMY_TABLE                                                   = "_dummy_table";
    public static final String VALUES_TMP_TABLE_NAME_PREFIX                                  = "Values__Tmp__Table__";
    public static final String CONSUMER_THREADS_PROPERTY                                     = "atlas.notification.hook.numthreads";
    public static final String CONSUMER_RETRIES_PROPERTY                                     = "atlas.notification.hook.maxretries";
    public static final String CONSUMER_FAILEDCACHESIZE_PROPERTY                             = "atlas.notification.hook.failedcachesize";
    public static final String CONSUMER_RETRY_INTERVAL                                       = "atlas.notification.consumer.retry.interval";
    public static final String CONSUMER_MIN_RETRY_INTERVAL                                   = "atlas.notification.consumer.min.retry.interval";
    public static final String CONSUMER_MAX_RETRY_INTERVAL                                   = "atlas.notification.consumer.max.retry.interval";
    public static final String CONSUMER_COMMIT_BATCH_SIZE                                    = "atlas.notification.consumer.commit.batch.size";
    public static final String CONSUMER_DISABLED                                             = "atlas.notification.consumer.disabled";
    public static final String CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633                  = "atlas.notification.consumer.skip.hive_column_lineage.hive-20633";
    public static final String CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD = "atlas.notification.consumer.skip.hive_column_lineage.hive-20633.inputs.threshold";
    public static final String CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN                = "atlas.notification.consumer.preprocess.entity.type.ignore.pattern";
    public static final String CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN                     = "atlas.notification.consumer.preprocess.entity.ignore.pattern";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN                 = "atlas.notification.consumer.preprocess.hive_table.ignore.pattern";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN                  = "atlas.notification.consumer.preprocess.hive_table.prune.pattern";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_CACHE_SIZE                     = "atlas.notification.consumer.preprocess.hive_table.cache.size";
    public static final String CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_ENABLED              = "atlas.notification.consumer.preprocess.hive_db.ignore.dummy.enabled";
    public static final String CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_NAMES                = "atlas.notification.consumer.preprocess.hive_db.ignore.dummy.names";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_ENABLED           = "atlas.notification.consumer.preprocess.hive_table.ignore.dummy.enabled";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_NAMES             = "atlas.notification.consumer.preprocess.hive_table.ignore.dummy.names";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES_ENABLED   = "atlas.notification.consumer.preprocess.hive_table.ignore.name.prefixes.enabled";
    public static final String CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES           = "atlas.notification.consumer.preprocess.hive_table.ignore.name.prefixes";
    public static final String CONSUMER_PREPROCESS_HIVE_PROCESS_UPD_NAME_WITH_QUALIFIED_NAME = "atlas.notification.consumer.preprocess.hive_process.update.name.with.qualified_name";
    public static final String CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS          = "atlas.notification.consumer.preprocess.hive_types.remove.ownedref.attrs";
    public static final String CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS         = "atlas.notification.consumer.preprocess.rdbms_types.remove.ownedref.attrs";
    public static final String CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX       = "atlas.notification.consumer.preprocess.s3_v2_directory.prune.object_prefix";
    public static final String CONSUMER_PREPROCESS_SPARK_PROCESS_ATTRIBUTES                  = "atlas.notification.consumer.preprocess.spark_process.attributes";
    public static final String CONSUMER_AUTHORIZE_USING_MESSAGE_USER                         = "atlas.notification.authorize.using.message.user";
    public static final String CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS                    = "atlas.notification.authorize.authn.cache.ttl.seconds";
    public static final int    SERVER_READY_WAIT_TIME_MS                                     = 1000;

    private static final int    KAFKA_CONSUMER_SHUTDOWN_WAIT             = 30000;
    private static final String ATLAS_HOOK_CONSUMER_THREAD_NAME          = "atlas-hook-consumer-thread";
    private static final String ATLAS_HOOK_UNSORTED_CONSUMER_THREAD_NAME = "atlas-hook-unsorted-consumer-thread";
    private static final String ATLAS_IMPORT_CONSUMER_THREAD_PREFIX      = "atlas-import-consumer-thread-";
    private static final String THREADNAME_PREFIX                        = NotificationHookConsumer.class.getSimpleName();

    private final AtlasEntityStore              atlasEntityStore;
    private final ServiceState                  serviceState;
    private final AtlasInstanceConverter        instanceConverter;
    private final AtlasTypeRegistry             typeRegistry;
    private final AtlasMetricsUtil              metricsUtil;
    private final int                           maxRetries;
    private final int                           minWaitDuration;
    private final int                           maxWaitDuration;
    private final int                           commitBatchSize;
    private final boolean                       skipHiveColumnLineageHive20633;
    private final int                           skipHiveColumnLineageHive20633InputsThreshold;
    private final boolean                       consumerDisabled;
    private final boolean                       hiveTypesRemoveOwnedRefAttrs;
    private final boolean                       rdbmsTypesRemoveOwnedRefAttrs;
    private final boolean                       s3V2DirectoryPruneObjectPrefix;
    private final boolean                       authorizeUsingMessageUser;

    private final Map<String, Authentication>   authnCache;
    private final NotificationInterface         notificationInterface;
    private final Configuration                 applicationProperties;
    private final Map<TopicPartition, Long>     lastCommittedPartitionOffset;
    private final EntityCorrelationManager      entityCorrelationManager;
    private final long                          consumerMsgBufferingIntervalMS;
    private final int                           consumerMsgBufferingBatchSize;
    private final AsyncImporter                 asyncImporter;

    private ExecutorService executors;

    @VisibleForTesting
    final int consumerRetryInterval;

    @VisibleForTesting
    List<HookConsumer> consumers;

    @Inject
    public NotificationHookConsumer(NotificationInterface notificationInterface, AtlasEntityStore atlasEntityStore, ServiceState serviceState, AtlasInstanceConverter instanceConverter, AtlasTypeRegistry typeRegistry, AtlasMetricsUtil metricsUtil, EntityCorrelationStore entityCorrelationStore, @Lazy AsyncImporter asyncImporter) throws AtlasException {
        this.notificationInterface        = notificationInterface;
        this.atlasEntityStore             = atlasEntityStore;
        this.serviceState                 = serviceState;
        this.instanceConverter            = instanceConverter;
        this.typeRegistry                 = typeRegistry;
        this.applicationProperties        = ApplicationProperties.get();
        this.metricsUtil                  = metricsUtil;
        this.lastCommittedPartitionOffset = new HashMap<>();
        this.asyncImporter                = asyncImporter;

        maxRetries            = applicationProperties.getInt(CONSUMER_RETRIES_PROPERTY, 3);
        consumerRetryInterval = applicationProperties.getInt(CONSUMER_RETRY_INTERVAL, 500);
        minWaitDuration       = applicationProperties.getInt(CONSUMER_MIN_RETRY_INTERVAL, consumerRetryInterval); // 500 ms  by default
        maxWaitDuration       = applicationProperties.getInt(CONSUMER_MAX_RETRY_INTERVAL, minWaitDuration * 60);  //  30 sec by default
        commitBatchSize       = applicationProperties.getInt(CONSUMER_COMMIT_BATCH_SIZE, 50);

        skipHiveColumnLineageHive20633                = applicationProperties.getBoolean(CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, false);
        skipHiveColumnLineageHive20633InputsThreshold = applicationProperties.getInt(CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, 15); // skip if avg # of inputs is > 15
        consumerDisabled                              = applicationProperties.getBoolean(CONSUMER_DISABLED, false);
        authorizeUsingMessageUser                     = applicationProperties.getBoolean(CONSUMER_AUTHORIZE_USING_MESSAGE_USER, false);
        consumerMsgBufferingIntervalMS                = AtlasConfiguration.NOTIFICATION_HOOK_CONSUMER_BUFFERING_INTERVAL.getInt() * 1000L;
        consumerMsgBufferingBatchSize                 = AtlasConfiguration.NOTIFICATION_HOOK_CONSUMER_BUFFERING_BATCH_SIZE.getInt();

        int authnCacheTtlSeconds = applicationProperties.getInt(CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS, 300);

        authnCache = (authorizeUsingMessageUser && authnCacheTtlSeconds > 0) ? new PassiveExpiringMap<>(authnCacheTtlSeconds * 1000L) : null;

        hiveTypesRemoveOwnedRefAttrs   = applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, true);
        rdbmsTypesRemoveOwnedRefAttrs  = applicationProperties.getBoolean(CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS, true);
        s3V2DirectoryPruneObjectPrefix = applicationProperties.getBoolean(CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX, true);

        entityCorrelationManager = new EntityCorrelationManager(entityCorrelationStore);
        LOG.info("{}={}", CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, skipHiveColumnLineageHive20633);
        LOG.info("{}={}", CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, skipHiveColumnLineageHive20633InputsThreshold);
        LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, hiveTypesRemoveOwnedRefAttrs);
        LOG.info("{}={}", CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS, rdbmsTypesRemoveOwnedRefAttrs);
        LOG.info("{}={}", CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX, s3V2DirectoryPruneObjectPrefix);
        LOG.info("{}={}", CONSUMER_COMMIT_BATCH_SIZE, commitBatchSize);
        LOG.info("{}={}", CONSUMER_DISABLED, consumerDisabled);
    }

    @Override
    public void start() throws AtlasException {
        startInternal(applicationProperties, null);
    }

    @Override
    public void stop() {
        //Allow for completion of outstanding work
        try {
            if (consumerDisabled && consumers.isEmpty()) {
                return;
            }

            stopConsumerThreads();
            if (executors != null) {
                executors.shutdown();

                if (!executors.awaitTermination(KAFKA_CONSUMER_SHUTDOWN_WAIT, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }

                executors = null;
            }

            notificationInterface.close();
        } catch (InterruptedException e) {
            LOG.error("Failure in shutting down consumers");
        }
    }

    /**
     * Start Kafka consumer threads that read from Kafka topic when server is activated.
     * <p>
     * Since the consumers create / update entities to the shared backend store, only the active instance
     * should perform this activity. Hence, these threads are started only on server activation.
     */
    @Override
    public void instanceIsActive() {
        if (executors == null) {
            executors = createExecutor();
            LOG.info("Executors initialized (Instance is active)");
        }

        if (consumerDisabled) {
            return;
        }

        LOG.info("Reacting to active state: initializing Kafka consumers");

        startHookConsumers();
    }

    /**
     * Stop Kafka consumer threads that read from Kafka topic when server is de-activated.
     * <p>
     * Since the consumers create / update entities to the shared backend store, only the active instance
     * should perform this activity. Hence, these threads are stopped only on server deactivation.
     */
    @Override
    public void instanceIsPassive() {
        if (consumerDisabled && consumers.isEmpty()) {
            return;
        }

        LOG.info("Reacting to passive state: shutting down Kafka consumers.");

        stop();
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.NOTIFICATION_HOOK_CONSUMER.getOrder();
    }

    public void closeImportConsumer(String importId, String topic) {
        try {
            LOG.info("==> closeImportConsumer(importId={}, topic={})", importId, topic);

            String                     consumerName      = ATLAS_IMPORT_CONSUMER_THREAD_PREFIX + importId;
            ListIterator<HookConsumer> consumersIterator = consumers.listIterator();

            while (consumersIterator.hasNext()) {
                HookConsumer consumer = consumersIterator.next();

                if (consumer.getName().startsWith(consumerName)) {
                    consumer.shutdown();
                    consumersIterator.remove();
                }
            }
            notificationInterface.closeConsumer(ASYNC_IMPORT, topic);
            notificationInterface.deleteTopic(ASYNC_IMPORT, topic);

            lastCommittedPartitionOffset.entrySet().removeIf(entry -> topic.equals(entry.getKey().topic()));
        } catch (Exception e) {
            LOG.error("Could not cleanup consumers for importId: {}", importId, e);
        } finally {
            LOG.info("<== closeImportConsumer(importId={}, topic={})", importId, topic);
        }
    }

    @VisibleForTesting
    void startInternal(Configuration configuration, ExecutorService executorService) {
        if (consumers == null) {
            consumers = new ArrayList<>();
        }

        if (executorService != null) {
            executors = executorService;
        }

        if (!HAConfiguration.isHAEnabled(configuration)) {
            if (executors == null) {
                executors = createExecutor();
                LOG.info("Executors initialized (HA is disabled)");
            }
            if (consumerDisabled) {
                LOG.info("No hook messages will be processed. {} = {}", CONSUMER_DISABLED, consumerDisabled);
                return;
            }

            LOG.info("HA is disabled, starting consumers inline.");

            startHookConsumers();
        }
    }

    @VisibleForTesting
    void startHookConsumers() {
        int                                                           numThreads                  = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 1);
        Map<NotificationConsumer<HookNotification>, NotificationType> notificationConsumersByType = new HashMap<>();
        List<NotificationConsumer<HookNotification>>                  notificationConsumers       = notificationInterface.createConsumers(NotificationType.HOOK, numThreads);

        for (NotificationConsumer<HookNotification> notificationConsumer : notificationConsumers) {
            notificationConsumersByType.put(notificationConsumer, NotificationType.HOOK);
        }

        if (AtlasHook.isHookMsgsSortEnabled) {
            List<NotificationConsumer<HookNotification>> unsortedNotificationConsumers = notificationInterface.createConsumers(NotificationType.HOOK_UNSORTED, numThreads);

            for (NotificationConsumer<HookNotification> unsortedNotificationConsumer : unsortedNotificationConsumers) {
                notificationConsumersByType.put(unsortedNotificationConsumer, NotificationType.HOOK_UNSORTED);
            }
        }

        List<HookConsumer> hookConsumers = new ArrayList<>();

        for (final NotificationConsumer<HookNotification> consumer : notificationConsumersByType.keySet()) {
            String hookConsumerName = ATLAS_HOOK_CONSUMER_THREAD_NAME;

            if (notificationConsumersByType.get(consumer).equals(NotificationType.HOOK_UNSORTED)) {
                hookConsumerName = ATLAS_HOOK_UNSORTED_CONSUMER_THREAD_NAME;
            }

            HookConsumer hookConsumer = new HookConsumer(hookConsumerName, consumer);

            hookConsumers.add(hookConsumer);
        }

        startConsumers(hookConsumers);
    }

    public void startAsyncImportConsumer(NotificationType notificationType, String importId, String topic) throws AtlasBaseException {
        if (topic != null) {
            notificationInterface.addTopicToNotificationType(notificationType, topic);
        }

        List<NotificationConsumer<HookNotification>> notificationConsumers = notificationInterface.createConsumers(notificationType, 1);
        List<HookConsumer> hookConsumers = new ArrayList<>();

        for (final NotificationConsumer<HookNotification> consumer : notificationConsumers) {
            String hookConsumerName = ATLAS_IMPORT_CONSUMER_THREAD_PREFIX + importId;
            HookConsumer hookConsumer = new HookConsumer(hookConsumerName, consumer);

            hookConsumers.add(hookConsumer);
        }

        startConsumers(hookConsumers);
    }

    @VisibleForTesting
    protected ExecutorService createExecutor() {
        return new ThreadPoolExecutor(
                0, // Core pool size
                Integer.MAX_VALUE, // Maximum pool size (dynamic scaling)
                60L, TimeUnit.SECONDS, // Idle thread timeout
                new SynchronousQueue<>(), // Direct handoff queue
                new ThreadFactoryBuilder().setNameFormat(THREADNAME_PREFIX + " thread-%d").build());
    }

    private void startConsumers(List<HookConsumer> hookConsumers) {
        if (consumers == null) {
            consumers = new ArrayList<>();
        }

        if (executors == null) {
            throw new IllegalStateException("Executors must be initialized before starting consumers.");
        }

        for (final HookConsumer consumer : hookConsumers) {
            consumers.add(consumer);
            executors.submit(consumer);
        }
    }

    private void stopConsumerThreads() {
        LOG.info("==> stopConsumerThreads()");

        if (consumers != null) {
            Iterator<HookConsumer> iterator = consumers.iterator();
            while (iterator.hasNext()) {
                HookConsumer consumer = iterator.next();
                consumer.shutdown();
                iterator.remove(); // Safe removal
            }
            consumers.clear();
        }

        LOG.info("<== stopConsumerThreads()");
    }

    static class Timer {
        public void sleep(int interval) throws InterruptedException {
            Thread.sleep(interval);
        }
    }

    @VisibleForTesting
    class HookConsumer extends Thread {
        private final NotificationConsumer<HookNotification> consumer;
        private final AtomicBoolean                          shouldRun      = new AtomicBoolean(false);
        private final NotificationEntityProcessor            entityProcessor;
        private final AdaptiveWaiter                         adaptiveWaiter = new AdaptiveWaiter(minWaitDuration, maxWaitDuration, minWaitDuration);

        private int duplicateKeyCounter = 1;

        public HookConsumer(NotificationConsumer<HookNotification> consumer) {
            this(ATLAS_HOOK_CONSUMER_THREAD_NAME, consumer);
        }

        public HookConsumer(String consumerThreadName, NotificationConsumer<HookNotification> consumer) {
            super(consumerThreadName);

            this.consumer        = consumer;
            this.entityProcessor = AtlasConfiguration.NOTIFICATION_CONCURRENT_PROCESSING.getBoolean()
                    ? new ConcurrentEntityProcessor(applicationProperties, metricsUtil, authnCache, atlasEntityStore, instanceConverter, entityCorrelationManager, typeRegistry, FAILED_LOG, LARGE_MESSAGES_LOG, asyncImporter)
                    : new SerialEntityProcessor(applicationProperties, metricsUtil, authnCache, atlasEntityStore, instanceConverter, entityCorrelationManager, typeRegistry, FAILED_LOG, LARGE_MESSAGES_LOG, asyncImporter);

            LOG.info("entityProcessor: {}", entityProcessor.getClass().getSimpleName());
        }

        @Override
        public void run() {
            LOG.info("==> HookConsumer run()");

            shouldRun.set(true);

            if (!serverAvailable(new NotificationHookConsumer.Timer())) {
                return;
            }

            try {
                while (shouldRun.get()) {
                    try {
                        if (StringUtils.equals(ATLAS_HOOK_UNSORTED_CONSUMER_THREAD_NAME, this.getName())) {
                            long                                             msgBufferingStartTime = System.currentTimeMillis();
                            Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer             = new TreeMap<>();

                            sortAndPublishMsgsToAtlasHook(msgBufferingStartTime, msgBuffer);
                        } else {
                            List<AtlasKafkaMessage<HookNotification>> messages;
                            TopicPartitionOffsetResult                result = entityProcessor.collectResults();

                            commit(result);

                            if (StringUtils.contains(this.getName(), ATLAS_IMPORT_CONSUMER_THREAD_PREFIX)) {
                                messages = consumer.receive();
                            } else {
                                messages = consumer.receiveWithCheckedCommit(lastCommittedPartitionOffset);
                            }

                            for (AtlasKafkaMessage<HookNotification> msg : messages) {
                                handleMessage(msg);
                            }
                        }
                    } catch (IllegalStateException ex) {
                        adaptiveWaiter.pause(ex);
                    } catch (Exception e) {
                        if (shouldRun.get()) {
                            LOG.warn("Exception in NotificationHookConsumer", e);

                            adaptiveWaiter.pause(e);
                        } else {
                            break;
                        }
                    }
                }
            } finally {
                if (consumer != null) {
                    LOG.info("closing NotificationConsumer");

                    consumer.close();
                }

                LOG.info("<== HookConsumer run()");
            }
        }

        public void shutdown() {
            LOG.info("==> HookConsumer shutdown()");
            this.entityProcessor.shutdown();

            // handle the case where thread was not started at all
            // and shutdown called
            if (!shouldRun.compareAndSet(true, false)) {
                return;
            }

            if (consumer != null) {
                consumer.wakeup();
            }

            LOG.info("<== HookConsumer shutdown()");
        }

        void sortAndPublishMsgsToAtlasHook(long msgBufferingStartTime, Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer) throws NotificationException {
            List<AtlasKafkaMessage<HookNotification>> messages           = consumer.receiveRawRecordsWithCheckedCommit(lastCommittedPartitionOffset);
            AtlasKafkaMessage<HookNotification>       maxOffsetMsg       = null;
            long                                      maxOffsetProcessed = 0;

            messages.forEach(x -> sortMessages(x, msgBuffer));

            if (msgBuffer.size() < consumerMsgBufferingBatchSize && System.currentTimeMillis() - msgBufferingStartTime < consumerMsgBufferingIntervalMS) {
                sortAndPublishMsgsToAtlasHook(msgBufferingStartTime, msgBuffer);

                return;
            }

            for (AtlasKafkaMessage<HookNotification> msg : msgBuffer.values()) {
                String hookTopic = StringUtils.isNotEmpty(msg.getTopic()) ? msg.getTopic().split(KafkaNotification.UNSORTED_POSTFIX)[0] : KafkaNotification.ATLAS_HOOK_TOPIC;

                if (maxOffsetProcessed == 0 || maxOffsetProcessed < msg.getOffset()) {
                    maxOffsetMsg       = msg;
                    maxOffsetProcessed = msg.getOffset();
                }

                ((KafkaNotification) notificationInterface).sendInternal(hookTopic, StringUtils.isNotEmpty(msg.getRawRecordData()) ? Collections.singletonList(msg.getRawRecordData()) : Collections.singletonList(msg.getMessage().toString()));
            }

            /** In case of failure while publishing sorted messages(above for loop), consuming of unsorted messages should start from the initial offset
             * Explicitly keeping this for loop separate to commit messages only after sending all batch messages to hook topic
             */
            for (AtlasKafkaMessage<HookNotification> msg : msgBuffer.values()) {
                commit(msg);
            }

            if (maxOffsetMsg != null) {
                commit(maxOffsetMsg);
            }

            msgBuffer.clear();
            resetDuplicateKeyCounter();
        }

        @VisibleForTesting
        void handleMessage(AtlasKafkaMessage<HookNotification> msg) {
            LOG.info("Message type: {}", msg.getMessage().getType().name());

            TopicPartitionOffsetResult result = entityProcessor.handleMessage(msg);
            commit(result);
        }

        boolean serverAvailable(Timer timer) {
            try {
                while (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                    try {
                        LOG.info("Atlas Server is not ready. Waiting for {} milliseconds to retry...", SERVER_READY_WAIT_TIME_MS);

                        timer.sleep(SERVER_READY_WAIT_TIME_MS);
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting for Atlas Server to become ready, " + "exiting consumer thread.", e);

                        return false;
                    }
                }
            } catch (Throwable e) {
                LOG.info("Handled AtlasServiceException while waiting for Atlas Server to become ready, exiting consumer thread.", e);

                return false;
            }

            LOG.info("Atlas Server is ready, can start reading Kafka events.");

            return true;
        }

        private void resetDuplicateKeyCounter() {
            duplicateKeyCounter = 1;
        }

        private String getKey(String msgCreated, String source) {
            return String.format("%s_%s", msgCreated, source);
        }

        private void sortMessages(AtlasKafkaMessage<HookNotification> msg, Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer) {
            String key = getKey(Long.toString(msg.getMsgCreated()), msg.getSource());

            if (msgBuffer.containsKey(key)) { //Duplicate key can possible for messages from same source with same msgCreationTime
                key = getKey(key, Integer.toString(duplicateKeyCounter));

                duplicateKeyCounter++;
            }

            msgBuffer.put(key, msg);
        }

        private void commit(TopicPartitionOffsetResult result) {
            if (result != null) {
                if (!lastCommittedPartitionOffset.containsKey(result.getTopicPartition())
                        || (lastCommittedPartitionOffset.containsKey(result.getTopicPartition())
                        && lastCommittedPartitionOffset.get(result.getTopicPartition()) != null
                        && lastCommittedPartitionOffset.get(result.getTopicPartition()) != result.getOffset())) {
                    LOG.info("Committing the result: {}", result.getKey());
                    commit(result.getTopicPartition(), result.getOffset());
                } else {
                    consumer.poll();
                }
            }
        }

        private void commit(AtlasKafkaMessage<HookNotification> kafkaMessage) {
            long commitOffset = kafkaMessage.getOffset() + 1;
            lastCommittedPartitionOffset.put(kafkaMessage.getTopicPartition(), commitOffset);
            consumer.commit(kafkaMessage.getTopicPartition(), commitOffset);
        }

        private void commit(TopicPartition topicPartition, long commitOffset) {
            long offsetToCommit = commitOffset + 1;
            consumer.commit(topicPartition, offsetToCommit);
            lastCommittedPartitionOffset.put(topicPartition, commitOffset);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing: topicPartition: {} with offset: {}", topicPartition, offsetToCommit);
            }
        }
    }
}