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
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.model.notification.ImportNotification.AtlasEntityImportNotification;
import org.apache.atlas.model.notification.ImportNotification.AtlasTypesDefImportNotification;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.notification.preprocessor.EntityPreprocessor;
import org.apache.atlas.notification.preprocessor.GenericEntityPreprocessor;
import org.apache.atlas.notification.preprocessor.PreprocessorContext;
import org.apache.atlas.notification.preprocessor.PreprocessorContext.PreprocessAction;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityCorrelationStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsCounter;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.util.AtlasMetricsUtil.NotificationStat;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.utils.LruCache;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityPartialUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.atlas.web.filters.AuditFilter;
import org.apache.atlas.web.filters.AuditFilter.AuditLog;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.apache.atlas.model.instance.AtlasObjectId.KEY_GUID;
import static org.apache.atlas.model.instance.AtlasObjectId.KEY_TYPENAME;
import static org.apache.atlas.model.instance.AtlasObjectId.KEY_UNIQUE_ATTRIBUTES;
import static org.apache.atlas.notification.preprocessor.EntityPreprocessor.TYPE_HIVE_PROCESS;
import static org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI;

/**
 * Consumer of notifications from hooks e.g., hive hook etc.
 */
@Component
@Order(5)
@DependsOn(value = {"atlasTypeDefStoreInitializer", "atlasTypeDefGraphStoreV2"})
public class NotificationHookConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG                = LoggerFactory.getLogger(NotificationHookConsumer.class);
    private static final Logger PERF_LOG           = AtlasPerfTracer.getPerfLogger(NotificationHookConsumer.class);
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

    private static final int    SC_OK                                           = 200;
    private static final int    SC_BAD_REQUEST                                  = 400;
    private static final String TYPE_HIVE_COLUMN_LINEAGE                        = "hive_column_lineage";
    private static final String ATTRIBUTE_INPUTS                                = "inputs";
    private static final String ATTRIBUTE_QUALIFIED_NAME                        = "qualifiedName";
    private static final String EXCEPTION_CLASS_NAME_JANUSGRAPH_EXCEPTION       = "JanusGraphException";
    private static final String EXCEPTION_CLASS_NAME_PERMANENTLOCKING_EXCEPTION = "PermanentLockingException";
    private static final int    KAFKA_CONSUMER_SHUTDOWN_WAIT                    = 30000;
    private static final String ATLAS_HOOK_CONSUMER_THREAD_NAME                 = "atlas-hook-consumer-thread";
    private static final String ATLAS_HOOK_UNSORTED_CONSUMER_THREAD_NAME        = "atlas-hook-unsorted-consumer-thread";
    private static final String ATLAS_IMPORT_CONSUMER_THREAD_PREFIX             = "atlas-import-consumer-thread-";
    private static final String THREADNAME_PREFIX                               = NotificationHookConsumer.class.getSimpleName();

    private final AtlasEntityStore              atlasEntityStore;
    private final ServiceState                  serviceState;
    private final AtlasInstanceConverter        instanceConverter;
    private final AtlasTypeRegistry             typeRegistry;
    private final AtlasMetricsUtil              metricsUtil;
    private final int                           maxRetries;
    private final int                           failedMsgCacheSize;
    private final int                           minWaitDuration;
    private final int                           maxWaitDuration;
    private final int                           commitBatchSize;
    private final boolean                       skipHiveColumnLineageHive20633;
    private final int                           skipHiveColumnLineageHive20633InputsThreshold;
    private final boolean                       updateHiveProcessNameWithQualifiedName;
    private final int                           largeMessageProcessingTimeThresholdMs;
    private final boolean                       consumerDisabled;
    private final List<Pattern>                 entityTypesToIgnore = new ArrayList<>();
    private final List<Pattern>                 entitiesToIgnore    = new ArrayList<>();
    private final List<Pattern>                 hiveTablesToIgnore  = new ArrayList<>();
    private final List<Pattern>                 hiveTablesToPrune   = new ArrayList<>();
    private final List<String>                  hiveDummyDatabasesToIgnore;
    private final List<String>                  hiveDummyTablesToIgnore;
    private final List<String>                  hiveTablePrefixesToIgnore;
    private final Map<String, PreprocessAction> hiveTablesCache;
    private final boolean                       hiveTypesRemoveOwnedRefAttrs;
    private final boolean                       rdbmsTypesRemoveOwnedRefAttrs;
    private final boolean                       s3V2DirectoryPruneObjectPrefix;
    private final boolean                       preprocessEnabled;
    private final boolean                       createShellEntityForNonExistingReference;
    private final boolean                       authorizeUsingMessageUser;
    private final boolean                       sparkProcessAttributes;

    private final Map<String, Authentication>   authnCache;
    private final NotificationInterface         notificationInterface;
    private final Configuration                 applicationProperties;
    private final Map<TopicPartition, Long>     lastCommittedPartitionOffset;
    private final EntityCorrelationManager      entityCorrelationManager;
    private final long                          consumerMsgBufferingIntervalMS;
    private final int                           consumerMsgBufferingBatchSize;
    private final AsyncImporter                 asyncImporter;

    private ExecutorService executors;
    private Instant         nextStatsLogTime = AtlasMetricsCounter.getNextHourStartTime(Instant.now());

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
        failedMsgCacheSize    = applicationProperties.getInt(CONSUMER_FAILEDCACHESIZE_PROPERTY, 1);
        consumerRetryInterval = applicationProperties.getInt(CONSUMER_RETRY_INTERVAL, 500);
        minWaitDuration       = applicationProperties.getInt(CONSUMER_MIN_RETRY_INTERVAL, consumerRetryInterval); // 500 ms  by default
        maxWaitDuration       = applicationProperties.getInt(CONSUMER_MAX_RETRY_INTERVAL, minWaitDuration * 60);  //  30 sec by default
        commitBatchSize       = applicationProperties.getInt(CONSUMER_COMMIT_BATCH_SIZE, 50);

        skipHiveColumnLineageHive20633                = applicationProperties.getBoolean(CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, false);
        skipHiveColumnLineageHive20633InputsThreshold = applicationProperties.getInt(CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, 15); // skip if avg # of inputs is > 15
        updateHiveProcessNameWithQualifiedName        = applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_PROCESS_UPD_NAME_WITH_QUALIFIED_NAME, true);
        consumerDisabled                              = applicationProperties.getBoolean(CONSUMER_DISABLED, false);
        largeMessageProcessingTimeThresholdMs         = applicationProperties.getInt("atlas.notification.consumer.large.message.processing.time.threshold.ms", 60 * 1000);  //  60 sec by default
        createShellEntityForNonExistingReference      = AtlasConfiguration.NOTIFICATION_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF.getBoolean();
        authorizeUsingMessageUser                     = applicationProperties.getBoolean(CONSUMER_AUTHORIZE_USING_MESSAGE_USER, false);
        consumerMsgBufferingIntervalMS                = AtlasConfiguration.NOTIFICATION_HOOK_CONSUMER_BUFFERING_INTERVAL.getInt() * 1000L;
        consumerMsgBufferingBatchSize                 = AtlasConfiguration.NOTIFICATION_HOOK_CONSUMER_BUFFERING_BATCH_SIZE.getInt();

        int authnCacheTtlSeconds = applicationProperties.getInt(CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS, 300);

        authnCache = (authorizeUsingMessageUser && authnCacheTtlSeconds > 0) ? new PassiveExpiringMap<>(authnCacheTtlSeconds * 1000L) : null;

        String[] patternEntityTypesToIgnore = applicationProperties.getStringArray(CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN);
        String[] patternEntitiesToIgnore    = applicationProperties.getStringArray(CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN);

        String[] patternHiveTablesToIgnore = applicationProperties.getStringArray(CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN);
        String[] patternHiveTablesToPrune  = applicationProperties.getStringArray(CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN);

        if (patternEntityTypesToIgnore != null) {
            for (String pattern : patternEntityTypesToIgnore) {
                try {
                    this.entityTypesToIgnore.add(Pattern.compile(pattern));
                } catch (Throwable t) {
                    LOG.warn("failed to compile pattern {}", pattern, t);
                    LOG.warn("Ignoring invalid pattern in configuration {}: {}", CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN, pattern);
                }
            }

            LOG.info("{}={}", CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN, entityTypesToIgnore);
        }

        if (patternEntitiesToIgnore != null) {
            for (String pattern : patternEntitiesToIgnore) {
                try {
                    this.entitiesToIgnore.add(Pattern.compile(pattern));
                } catch (Throwable t) {
                    LOG.warn("failed to compile pattern {}", pattern, t);
                    LOG.warn("Ignoring invalid pattern in configuration {}: {}", CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN, pattern);
                }
            }

            LOG.info("{}={}", CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN, entitiesToIgnore);
        }

        if (patternHiveTablesToIgnore != null) {
            for (String pattern : patternHiveTablesToIgnore) {
                try {
                    hiveTablesToIgnore.add(Pattern.compile(pattern));

                    LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN, pattern);
                } catch (Throwable t) {
                    LOG.warn("failed to compile pattern {}", pattern, t);
                    LOG.warn("Ignoring invalid pattern in configuration {}: {}", CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN, pattern);
                }
            }
        }

        if (patternHiveTablesToPrune != null) {
            for (String pattern : patternHiveTablesToPrune) {
                try {
                    hiveTablesToPrune.add(Pattern.compile(pattern));

                    LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN, pattern);
                } catch (Throwable t) {
                    LOG.warn("failed to compile pattern {}", pattern, t);
                    LOG.warn("Ignoring invalid pattern in configuration {}: {}", CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN, pattern);
                }
            }
        }

        if (!hiveTablesToIgnore.isEmpty() || !hiveTablesToPrune.isEmpty()) {
            hiveTablesCache = new LruCache<>(applicationProperties.getInt(CONSUMER_PREPROCESS_HIVE_TABLE_CACHE_SIZE, 10000), 0);
        } else {
            hiveTablesCache = Collections.emptyMap();
        }

        boolean hiveDbIgnoreDummyEnabled         = applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_ENABLED, true);
        boolean hiveTableIgnoreDummyEnabled      = applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_ENABLED, true);
        boolean hiveTableIgnoreNamePrefixEnabled = applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES_ENABLED, true);

        LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_ENABLED, hiveDbIgnoreDummyEnabled);
        LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_ENABLED, hiveTableIgnoreDummyEnabled);
        LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES_ENABLED, hiveTableIgnoreNamePrefixEnabled);

        if (hiveDbIgnoreDummyEnabled) {
            String[] dummyDatabaseNames = applicationProperties.getStringArray(CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_NAMES);

            hiveDummyDatabasesToIgnore = trimAndPurge(dummyDatabaseNames, DUMMY_DATABASE);

            LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_NAMES, StringUtils.join(hiveDummyDatabasesToIgnore, ','));
        } else {
            hiveDummyDatabasesToIgnore = Collections.emptyList();
        }

        if (hiveTableIgnoreDummyEnabled) {
            String[] dummyTableNames = applicationProperties.getStringArray(CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_NAMES);

            hiveDummyTablesToIgnore = trimAndPurge(dummyTableNames, DUMMY_TABLE);

            LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_NAMES, StringUtils.join(hiveDummyTablesToIgnore, ','));
        } else {
            hiveDummyTablesToIgnore = Collections.emptyList();
        }

        if (hiveTableIgnoreNamePrefixEnabled) {
            String[] ignoreNamePrefixes = applicationProperties.getStringArray(CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES);

            hiveTablePrefixesToIgnore = trimAndPurge(ignoreNamePrefixes, VALUES_TMP_TABLE_NAME_PREFIX);

            LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES, StringUtils.join(hiveTablePrefixesToIgnore, ','));
        } else {
            hiveTablePrefixesToIgnore = Collections.emptyList();
        }

        LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_PROCESS_UPD_NAME_WITH_QUALIFIED_NAME, updateHiveProcessNameWithQualifiedName);

        hiveTypesRemoveOwnedRefAttrs   = applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, true);
        rdbmsTypesRemoveOwnedRefAttrs  = applicationProperties.getBoolean(CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS, true);
        s3V2DirectoryPruneObjectPrefix = applicationProperties.getBoolean(CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX, true);
        sparkProcessAttributes      = this.applicationProperties.getBoolean(CONSUMER_PREPROCESS_SPARK_PROCESS_ATTRIBUTES, false);
        preprocessEnabled        = skipHiveColumnLineageHive20633 || updateHiveProcessNameWithQualifiedName || hiveTypesRemoveOwnedRefAttrs || rdbmsTypesRemoveOwnedRefAttrs || s3V2DirectoryPruneObjectPrefix || !hiveTablesToIgnore.isEmpty() || !hiveTablesToPrune.isEmpty() || !hiveDummyDatabasesToIgnore.isEmpty() || !hiveDummyTablesToIgnore.isEmpty() || !hiveTablePrefixesToIgnore.isEmpty() || sparkProcessAttributes;
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
//ATLAS_IMPORT_e22a73f9f6a16620a8655b36d71fb5be
            String                     consumerName      = ATLAS_IMPORT_CONSUMER_THREAD_PREFIX + importId;
            ListIterator<HookConsumer> consumersIterator = consumers.listIterator();

            while (consumersIterator.hasNext()) {
                HookConsumer consumer = consumersIterator.next();

                if (consumer.getName().startsWith(consumerName)) {
                    consumer.shutdown();
                    consumersIterator.remove();
                }
            }

            notificationInterface.closeConsumer(NotificationInterface.NotificationType.ASYNC_IMPORT, topic);
            notificationInterface.deleteTopic(NotificationInterface.NotificationType.ASYNC_IMPORT, topic);

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
        int numThreads = applicationProperties.getInt(CONSUMER_THREADS_PROPERTY, 1);
        Map<NotificationConsumer<HookNotification>, NotificationType> notificationConsumersByType = new HashMap<>();
        List<NotificationConsumer<HookNotification>> notificationConsumers = notificationInterface.createConsumers(NotificationType.HOOK, numThreads);

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

    private List<String> trimAndPurge(String[] values, String defaultValue) {
        final List<String> ret;

        if (values != null && values.length > 0) {
            ret = new ArrayList<>(values.length);

            for (String val : values) {
                if (StringUtils.isNotBlank(val)) {
                    ret.add(val.trim());
                }
            }
        } else if (StringUtils.isNotBlank(defaultValue)) {
            ret = Collections.singletonList(defaultValue.trim());
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }

    private void preprocessEntities(PreprocessorContext context) {
        GenericEntityPreprocessor genericEntityPreprocessor = new GenericEntityPreprocessor(this.entityTypesToIgnore, this.entitiesToIgnore);
        List<AtlasEntity>        entities                   = context.getEntities();

        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity entity = entities.get(i);

                genericEntityPreprocessor.preprocess(entity, context);

                if (context.isIgnoredEntity(entity.getGuid())) {
                    entities.remove(i--);
                }
            }
        }

        Map<String, AtlasEntity> referredEntities = context.getReferredEntities();

        if (referredEntities != null) {
            for (Iterator<Map.Entry<String, AtlasEntity>> iterator = referredEntities.entrySet().iterator(); iterator.hasNext(); ) {
                AtlasEntity entity = iterator.next().getValue();

                genericEntityPreprocessor.preprocess(entity, context);

                if (context.isIgnoredEntity(entity.getGuid())) {
                    iterator.remove();
                }
            }
        }
    }

    private PreprocessorContext preProcessNotificationMessage(AtlasKafkaMessage<HookNotification> kafkaMsg) {
        PreprocessorContext context = null;

        if (preprocessEnabled) {
            context = new PreprocessorContext(kafkaMsg, typeRegistry, hiveTablesToIgnore, hiveTablesToPrune, hiveTablesCache,
                    hiveDummyDatabasesToIgnore, hiveDummyTablesToIgnore, hiveTablePrefixesToIgnore, hiveTypesRemoveOwnedRefAttrs,
                    rdbmsTypesRemoveOwnedRefAttrs, s3V2DirectoryPruneObjectPrefix, updateHiveProcessNameWithQualifiedName, entityCorrelationManager);

            if (CollectionUtils.isNotEmpty(this.entityTypesToIgnore) || CollectionUtils.isNotEmpty(this.entitiesToIgnore)) {
                preprocessEntities(context);
            }

            if (context.isHivePreprocessEnabled()) {
                preprocessHiveTypes(context);
            }

            if (skipHiveColumnLineageHive20633) {
                skipHiveColumnLineage(context);
            }

            if (rdbmsTypesRemoveOwnedRefAttrs) {
                rdbmsTypeRemoveOwnedRefAttrs(context);
            }

            if (s3V2DirectoryPruneObjectPrefix) {
                pruneObjectPrefixForS3V2Directory(context);
            }

            if (sparkProcessAttributes) {
                preprocessSparkProcessAttributes(context);
            }

            context.moveRegisteredReferredEntities();

            if (context.isHivePreprocessEnabled() && CollectionUtils.isNotEmpty(context.getEntities()) && context.getEntities().size() > 1) {
                // move hive_process and hive_column_lineage entities to end of the list
                List<AtlasEntity> entities = context.getEntities();
                int               count    = entities.size();

                for (int i = 0; i < count; i++) {
                    AtlasEntity entity = entities.get(i);

                    switch (entity.getTypeName()) {
                        case TYPE_HIVE_PROCESS:
                        case TYPE_HIVE_COLUMN_LINEAGE:
                            entities.remove(i--);
                            entities.add(entity);
                            count--;
                            break;
                    }
                }

                if (entities.size() - count > 0) {
                    LOG.info("preprocess: moved {} hive_process/hive_column_lineage entities to end of list (listSize={}). topic={}, partition={}, offset={}", entities.size() - count, entities.size(), kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset());
                }
            }
        }

        return context;
    }

    private void rdbmsTypeRemoveOwnedRefAttrs(PreprocessorContext context) {
        List<AtlasEntity> entities = context.getEntities();

        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity        entity       = entities.get(i);
                EntityPreprocessor preprocessor = EntityPreprocessor.getRdbmsPreprocessor(entity.getTypeName());

                if (preprocessor != null) {
                    preprocessor.preprocess(entity, context);
                }
            }
        }
    }

    private void pruneObjectPrefixForS3V2Directory(PreprocessorContext context) {
        List<AtlasEntity> entities = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(context.getEntities())) {
            entities.addAll(context.getEntities());
        }

        if (MapUtils.isNotEmpty(context.getReferredEntities())) {
            entities.addAll(context.getReferredEntities().values());
        }

        if (CollectionUtils.isNotEmpty(entities)) {
            for (AtlasEntity entity : entities) {
                EntityPreprocessor preprocessor = EntityPreprocessor.getS3V2Preprocessor(entity.getTypeName());

                if (preprocessor != null) {
                    preprocessor.preprocess(entity, context);
                }
            }
        }
    }

    private void preprocessHiveTypes(PreprocessorContext context) {
        List<AtlasEntity> entities = context.getEntities();

        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity        entity       = entities.get(i);
                EntityPreprocessor preprocessor = EntityPreprocessor.getHivePreprocessor(entity.getTypeName());

                if (preprocessor != null) {
                    preprocessor.preprocess(entity, context);

                    if (context.isIgnoredEntity(entity.getGuid())) {
                        entities.remove(i--);
                    }
                }
            }

            Map<String, AtlasEntity> referredEntities = context.getReferredEntities();

            if (referredEntities != null) {
                for (Iterator<Map.Entry<String, AtlasEntity>> iter = referredEntities.entrySet().iterator(); iter.hasNext(); ) {
                    AtlasEntity        entity       = iter.next().getValue();
                    EntityPreprocessor preprocessor = EntityPreprocessor.getHivePreprocessor(entity.getTypeName());

                    if (preprocessor != null) {
                        preprocessor.preprocess(entity, context);

                        if (context.isIgnoredEntity(entity.getGuid())) {
                            iter.remove();
                        }
                    }
                }
            }

            int ignoredEntities = context.getIgnoredEntities().size();
            int prunedEntities  = context.getPrunedEntities().size();

            if (ignoredEntities > 0 || prunedEntities > 0) {
                LOG.info("preprocess: ignored entities={}; pruned entities={}. topic-offset={}, partition={}", ignoredEntities, prunedEntities, context.getKafkaMessageOffset(), context.getKafkaPartition());
            }
        }
    }

    private void preprocessSparkProcessAttributes(PreprocessorContext context) {
        List<AtlasEntity> entities = context.getEntities();

        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity entity = entities.get(i);
                EntityPreprocessor preprocessor = EntityPreprocessor.getSparkPreprocessor(entity.getTypeName());

                if (preprocessor != null) {
                    preprocessor.preprocess(entity, context);
                }
            }
        }
    }

    private void skipHiveColumnLineage(PreprocessorContext context) {
        List<AtlasEntity> entities = context.getEntities();

        if (entities != null) {
            int         lineageCount       = 0;
            int         lineageInputsCount = 0;
            int         numRemovedEntities = 0;
            Set<String> lineageQNames      = new HashSet<>();

            // find if all hive_column_lineage entities have same number of inputs, which is likely to be caused by HIVE-20633 that results in incorrect lineage in some cases
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity entity = entities.get(i);

                if (StringUtils.equals(entity.getTypeName(), TYPE_HIVE_COLUMN_LINEAGE)) {
                    final Object qName = entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME);

                    if (qName != null) {
                        final String qualifiedName = qName.toString();

                        if (lineageQNames.contains(qualifiedName)) {
                            entities.remove(i--);

                            LOG.warn("removed duplicate hive_column_lineage entity: qualifiedName={}. topic-offset={}, partition={}", qualifiedName, context.getKafkaMessageOffset(), context.getKafkaPartition());

                            numRemovedEntities++;

                            continue;
                        } else {
                            lineageQNames.add(qualifiedName);
                        }
                    }

                    lineageCount++;

                    Object objInputs = entity.getAttribute(ATTRIBUTE_INPUTS);

                    if (objInputs instanceof Collection) {
                        Collection<?> inputs = (Collection<?>) objInputs;

                        lineageInputsCount += inputs.size();
                    }
                }
            }

            float avgInputsCount = lineageCount > 0 ? (((float) lineageInputsCount) / lineageCount) : 0;

            if (avgInputsCount > skipHiveColumnLineageHive20633InputsThreshold) {
                for (int i = 0; i < entities.size(); i++) {
                    AtlasEntity entity = entities.get(i);

                    if (StringUtils.equals(entity.getTypeName(), TYPE_HIVE_COLUMN_LINEAGE)) {
                        entities.remove(i--);

                        numRemovedEntities++;
                    }
                }
            }

            if (numRemovedEntities > 0) {
                LOG.warn("removed {} hive_column_lineage entities. Average # of inputs={}, threshold={}, total # of inputs={}. topic-offset={}, partition={}", numRemovedEntities, avgInputsCount, skipHiveColumnLineageHive20633InputsThreshold, lineageInputsCount, context.getKafkaMessageOffset(), context.getKafkaPartition());
            }
        }
    }

    private boolean isEmptyMessage(AtlasKafkaMessage<HookNotification> kafkaMsg) {
        final boolean          ret;
        final HookNotification message = kafkaMsg.getMessage();

        switch (message.getType()) {
            case ENTITY_CREATE_V2: {
                AtlasEntitiesWithExtInfo entities = ((EntityCreateRequestV2) message).getEntities();

                ret = entities == null || CollectionUtils.isEmpty(entities.getEntities());
            }
            break;

            case ENTITY_FULL_UPDATE_V2: {
                AtlasEntitiesWithExtInfo entities = ((EntityUpdateRequestV2) message).getEntities();

                ret = entities == null || CollectionUtils.isEmpty(entities.getEntities());
            }
            break;

            default:
                ret = false;
                break;
        }

        return ret;
    }

    private void recordProcessedEntities(EntityMutationResponse mutationResponse, NotificationStat stats, PreprocessorContext context) {
        if (mutationResponse != null) {
            if (stats != null) {
                stats.updateStats(mutationResponse);
            }

            if (context != null) {
                if (MapUtils.isNotEmpty(mutationResponse.getGuidAssignments())) {
                    context.getGuidAssignments().putAll(mutationResponse.getGuidAssignments());
                }

                if (CollectionUtils.isNotEmpty(mutationResponse.getCreatedEntities())) {
                    for (AtlasEntityHeader entity : mutationResponse.getCreatedEntities()) {
                        if (entity != null && entity.getGuid() != null) {
                            context.getCreatedEntities().add(entity.getGuid());
                        }
                    }
                }

                if (CollectionUtils.isNotEmpty(mutationResponse.getDeletedEntities())) {
                    for (AtlasEntityHeader entity : mutationResponse.getDeletedEntities()) {
                        if (entity != null && entity.getGuid() != null) {
                            context.getDeletedEntities().add(entity.getGuid());
                        }
                    }
                }
            }
        }
    }

    private void updateProcessedEntityReferences(List<AtlasEntity> entities, Map<String, String> guidAssignments) {
        if (CollectionUtils.isNotEmpty(entities) && MapUtils.isNotEmpty(guidAssignments)) {
            for (AtlasEntity entity : entities) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                if (entityType == null) {
                    continue;
                }

                if (MapUtils.isNotEmpty(entity.getAttributes())) {
                    for (Map.Entry<String, Object> entry : entity.getAttributes().entrySet()) {
                        String attrName  = entry.getKey();
                        Object attrValue = entry.getValue();

                        if (attrValue == null) {
                            continue;
                        }

                        AtlasAttribute attribute = entityType.getAttribute(attrName);

                        if (attribute == null) { // look for a relationship attribute with the same name
                            attribute = entityType.getRelationshipAttribute(attrName, null);
                        }

                        if (attribute != null && attribute.isObjectRef()) {
                            updateProcessedEntityReferences(attrValue, guidAssignments);
                        }
                    }
                }

                if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
                    for (Map.Entry<String, Object> entry : entity.getRelationshipAttributes().entrySet()) {
                        Object attrValue = entry.getValue();

                        if (attrValue != null) {
                            updateProcessedEntityReferences(attrValue, guidAssignments);
                        }
                    }
                }
            }
        }
    }

    private void updateProcessedEntityReferences(Object objVal, Map<String, String> guidAssignments) {
        if (objVal instanceof AtlasObjectId) {
            updateProcessedEntityReferences((AtlasObjectId) objVal, guidAssignments);
        } else if (objVal instanceof Collection) {
            updateProcessedEntityReferences((Collection<?>) objVal, guidAssignments);
        } else if (objVal instanceof Map) {
            updateProcessedEntityReferences((Map<?, ?>) objVal, guidAssignments);
        }
    }

    private void updateProcessedEntityReferences(AtlasObjectId objId, Map<String, String> guidAssignments) {
        String guid = objId.getGuid();

        if (guid != null && guidAssignments.containsKey(guid)) {
            String assignedGuid = guidAssignments.get(guid);

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}(guid={}) is already processed; updating its reference to use assigned-guid={}", objId.getTypeName(), guid, assignedGuid);
            }

            objId.setGuid(assignedGuid);
            objId.setTypeName(null);
            objId.setUniqueAttributes(null);
        }
    }

    private void updateProcessedEntityReferences(Map objId, Map<String, String> guidAssignments) {
        Object guid = objId.get(KEY_GUID);

        if (guid != null && guidAssignments.containsKey(guid)) {
            String assignedGuid = guidAssignments.get(guid);

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}(guid={}) is already processed; updating its reference to use assigned-guid={}", objId.get(KEY_TYPENAME), guid, assignedGuid);
            }

            objId.put(KEY_GUID, assignedGuid);
            objId.remove(KEY_TYPENAME);
            objId.remove(KEY_UNIQUE_ATTRIBUTES);
        }
    }

    private void updateProcessedEntityReferences(Collection<?> objIds, Map<String, String> guidAssignments) {
        for (Object objId : objIds) {
            updateProcessedEntityReferences(objId, guidAssignments);
        }
    }

    private void setCurrentUser(String userName) {
        Authentication authentication = getAuthenticationForUser(userName);

        if (LOG.isDebugEnabled()) {
            if (authentication != null) {
                LOG.debug("setCurrentUser(): notification processing will be authorized as user '{}'", userName);
            } else {
                LOG.debug("setCurrentUser(): Failed to get authentication for user '{}'.", userName);
            }
        }

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    private Authentication getAuthenticationForUser(String userName) {
        Authentication ret = null;

        if (StringUtils.isNotBlank(userName)) {
            ret = authnCache != null ? authnCache.get(userName) : null;

            if (ret == null) {
                List<GrantedAuthority> grantedAuths = getAuthoritiesFromUGI(userName);
                UserDetails            principal    = new User(userName, "", grantedAuths);

                ret = new UsernamePasswordAuthenticationToken(principal, "");

                if (authnCache != null) {
                    authnCache.put(userName, ret);
                }
            }
        }

        return ret;
    }

    static class Timer {
        public void sleep(int interval) throws InterruptedException {
            Thread.sleep(interval);
        }
    }

    static class AdaptiveWaiter {
        private final long increment;
        private final long maxDuration;
        private final long minDuration;
        private final long resetInterval;

        @VisibleForTesting
        long waitDuration;

        private long lastWaitAt;

        public AdaptiveWaiter(long minDuration, long maxDuration, long increment) {
            this.minDuration   = minDuration;
            this.maxDuration   = maxDuration;
            this.increment     = increment;
            this.waitDuration  = minDuration;
            this.lastWaitAt    = 0;
            this.resetInterval = maxDuration * 2;
        }

        public void pause(Throwable ex) {
            setWaitDurations();

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} in NotificationHookConsumer. Waiting for {} ms for recovery.", ex.getClass().getName(), waitDuration, ex);
                }

                Thread.sleep(waitDuration);
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} in NotificationHookConsumer. Waiting for recovery interrupted.", ex.getClass().getName(), e);
                }
            }
        }

        private void setWaitDurations() {
            long timeSinceLastWait = (lastWaitAt == 0) ? 0 : System.currentTimeMillis() - lastWaitAt;

            lastWaitAt = System.currentTimeMillis();

            if (timeSinceLastWait > resetInterval) {
                waitDuration = minDuration;
            } else {
                waitDuration += increment;
                if (waitDuration > maxDuration) {
                    waitDuration = maxDuration;
                }
            }
        }
    }

    @VisibleForTesting
    class HookConsumer extends Thread {
        private final NotificationConsumer<HookNotification> consumer;
        private final AtomicBoolean                          shouldRun      = new AtomicBoolean(false);
        private final List<String>                           failedMessages = new ArrayList<>();
        private final AdaptiveWaiter                         adaptiveWaiter = new AdaptiveWaiter(minWaitDuration, maxWaitDuration, minWaitDuration);

        private int duplicateKeyCounter = 1;

        public HookConsumer(NotificationConsumer<HookNotification> consumer) {
            super(ATLAS_HOOK_CONSUMER_THREAD_NAME);

            this.consumer = consumer;
        }

        public HookConsumer(String consumerThreadName, NotificationConsumer<HookNotification> consumer) {
            super(consumerThreadName);

            this.consumer = consumer;
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
                            List<AtlasKafkaMessage<HookNotification>> messages = consumer.receiveWithCheckedCommit(lastCommittedPartitionOffset);

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
        void handleMessage(AtlasKafkaMessage<HookNotification> kafkaMsg) {
            AtlasPerfTracer  perf                   = null;
            HookNotification message                = kafkaMsg.getMessage();
            String           messageUser            = message.getUser();
            long             startTime              = System.currentTimeMillis();
            NotificationStat stats                  = new NotificationStat();
            AuditLog         auditLog               = null;
            boolean          importRequestComplete  = false;

            if (authorizeUsingMessageUser) {
                setCurrentUser(messageUser);
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, message.getType().name());
            }

            try {
                // covert V1 messages to V2 to enable preProcess
                try {
                    switch (message.getType()) {
                        case ENTITY_CREATE: {
                            final EntityCreateRequest      createRequest = (EntityCreateRequest) message;
                            final AtlasEntitiesWithExtInfo entities      = instanceConverter.toAtlasEntities(createRequest.getEntities());
                            final EntityCreateRequestV2    v2Request     = new EntityCreateRequestV2(message.getUser(), entities);

                            kafkaMsg = new AtlasKafkaMessage<>(v2Request, kafkaMsg.getOffset(), kafkaMsg.getTopic(), kafkaMsg.getPartition());
                            message  = kafkaMsg.getMessage();
                        }
                        break;

                        case ENTITY_FULL_UPDATE: {
                            final EntityUpdateRequest      updateRequest = (EntityUpdateRequest) message;
                            final AtlasEntitiesWithExtInfo entities      = instanceConverter.toAtlasEntities(updateRequest.getEntities());
                            final EntityUpdateRequestV2    v2Request     = new EntityUpdateRequestV2(messageUser, entities);

                            kafkaMsg = new AtlasKafkaMessage<>(v2Request, kafkaMsg.getOffset(), kafkaMsg.getTopic(), kafkaMsg.getPartition());
                            message  = kafkaMsg.getMessage();
                        }
                        break;
                    }
                } catch (AtlasBaseException excp) {
                    LOG.error("handleMessage({}): failed to convert V1 message to V2", message.getType().name());
                }

                PreprocessorContext context = preProcessNotificationMessage(kafkaMsg);

                if (isEmptyMessage(kafkaMsg)) {
                    commit(kafkaMsg);

                    return;
                }

                // Used for intermediate conversions during create and update
                String exceptionClassName = StringUtils.EMPTY;
                for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
                    LOG.debug("handleMessage({}): attempt {}", message.getType().name(), numRetries);

                    try {
                        RequestContext requestContext = RequestContext.get();

                        requestContext.setAttemptCount(numRetries + 1);
                        requestContext.setMaxAttempts(maxRetries);

                        requestContext.setUser(messageUser, null);
                        requestContext.setInNotificationProcessing(true);
                        requestContext.setCreateShellEntityForNonExistingReference(createShellEntityForNonExistingReference);

                        switch (message.getType()) {
                            case ENTITY_CREATE: {
                                final EntityCreateRequest      createRequest = (EntityCreateRequest) message;
                                final AtlasEntitiesWithExtInfo entities      = instanceConverter.toAtlasEntities(createRequest.getEntities());

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX, AtlasClient.API_V1.CREATE_ENTITY.getMethod(), AtlasClient.API_V1.CREATE_ENTITY.getNormalizedPath());
                                }

                                createOrUpdate(entities, false, stats, context);
                            }
                            break;

                            case ENTITY_PARTIAL_UPDATE: {
                                final EntityPartialUpdateRequest partialUpdateRequest = (EntityPartialUpdateRequest) message;
                                final Referenceable              referenceable        = partialUpdateRequest.getEntity();
                                final AtlasEntitiesWithExtInfo   entities             = instanceConverter.toAtlasEntity(referenceable);

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                            AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                            String.format(AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), partialUpdateRequest.getTypeName()));
                                }

                                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(partialUpdateRequest.getTypeName());
                                String          guid       = AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, Collections.singletonMap(partialUpdateRequest.getAttribute(), partialUpdateRequest.getAttributeValue()));

                                // There should only be one root entity
                                entities.getEntities().get(0).setGuid(guid);

                                createOrUpdate(entities, true, stats, context);
                            }
                            break;

                            case ENTITY_DELETE: {
                                final EntityDeleteRequest deleteRequest = (EntityDeleteRequest) message;

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                            AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                            String.format(AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), deleteRequest.getTypeName()));
                                }

                                try {
                                    AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(deleteRequest.getTypeName());

                                    EntityMutationResponse response = atlasEntityStore.deleteByUniqueAttributes(type, Collections.singletonMap(deleteRequest.getAttribute(), deleteRequest.getAttributeValue()));

                                    stats.updateStats(response);

                                    entityCorrelationManager.add(kafkaMsg.getSpooled(), kafkaMsg.getMsgCreated(), response.getDeletedEntities());
                                } catch (ClassCastException cle) {
                                    LOG.error("Failed to delete entity {}", deleteRequest);
                                }
                            }
                            break;

                            case ENTITY_FULL_UPDATE: {
                                final EntityUpdateRequest      updateRequest = (EntityUpdateRequest) message;
                                final AtlasEntitiesWithExtInfo entities      = instanceConverter.toAtlasEntities(updateRequest.getEntities());

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX, AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(), AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                                }

                                createOrUpdate(entities, false, stats, context);
                            }
                            break;

                            case ENTITY_CREATE_V2: {
                                final EntityCreateRequestV2    createRequestV2 = (EntityCreateRequestV2) message;
                                final AtlasEntitiesWithExtInfo entities        = createRequestV2.getEntities();

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX, AtlasClientV2.API_V2.CREATE_ENTITY.getMethod(), AtlasClientV2.API_V2.CREATE_ENTITY.getNormalizedPath());
                                }

                                createOrUpdate(entities, false, stats, context);
                            }
                            break;

                            case ENTITY_PARTIAL_UPDATE_V2: {
                                final EntityPartialUpdateRequestV2 partialUpdateRequest = (EntityPartialUpdateRequestV2) message;
                                final AtlasObjectId                entityId             = partialUpdateRequest.getEntityId();
                                final AtlasEntityWithExtInfo       entity               = partialUpdateRequest.getEntity();

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX, AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(), AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                                }

                                EntityMutationResponse response = atlasEntityStore.updateEntity(entityId, entity, true);

                                stats.updateStats(response);
                            }
                            break;

                            case ENTITY_FULL_UPDATE_V2: {
                                final EntityUpdateRequestV2    updateRequest = (EntityUpdateRequestV2) message;
                                final AtlasEntitiesWithExtInfo entities      = updateRequest.getEntities();

                                if (auditLog == null) {
                                    auditLog = new AuditLog(messageUser, THREADNAME_PREFIX, AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(), AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                                }

                                createOrUpdate(entities, false, stats, context);
                            }
                            break;

                            case ENTITY_DELETE_V2: {
                                final EntityDeleteRequestV2 deleteRequest = (EntityDeleteRequestV2) message;
                                final List<AtlasObjectId>   entities      = deleteRequest.getEntities();

                                try {
                                    for (AtlasObjectId entity : entities) {
                                        if (auditLog == null) {
                                            auditLog = new AuditLog(messageUser, THREADNAME_PREFIX,
                                                    AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                                    String.format(AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), entity.getTypeName()));
                                        }

                                        AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());

                                        EntityMutationResponse response = atlasEntityStore.deleteByUniqueAttributes(type, entity.getUniqueAttributes());

                                        stats.updateStats(response);

                                        entityCorrelationManager.add(kafkaMsg.getSpooled(), kafkaMsg.getMsgCreated(), response.getDeletedEntities());
                                    }
                                } catch (ClassCastException cle) {
                                    LOG.error("Failed to do delete entities {}", entities);
                                }
                            }
                            break;

                            case IMPORT_TYPES_DEF: {
                                final AtlasTypesDefImportNotification typesDefImportNotification = (AtlasTypesDefImportNotification) message;
                                final String                         importId                  = typesDefImportNotification.getImportId();
                                final AtlasTypesDef                  typesDef                  = typesDefImportNotification.getTypesDef();

                                try {
                                    asyncImporter.onImportTypeDef(typesDef, importId);
                                } catch (AtlasBaseException abe) {
                                    LOG.error("IMPORT_TYPE_DEF: {} failed to import type definition: {}", importId, typesDef);
                                    asyncImporter.onImportComplete(importId);
                                    importRequestComplete = true;
                                }
                            }
                            break;

                            case IMPORT_ENTITY: {
                                final AtlasEntityImportNotification entityImportNotification = (AtlasEntityImportNotification) message;
                                final String                        importId                 = entityImportNotification.getImportId();
                                final AtlasEntityWithExtInfo        entityWithExtInfo        = entityImportNotification.getEntity();
                                final int                           position                 = entityImportNotification.getPosition();
                                boolean                             completeImport           = false;

                                try {
                                    importRequestComplete = asyncImporter.onImportEntity(entityWithExtInfo, importId, position);
                                } catch (AtlasBaseException abe) {
                                    importRequestComplete = true;

                                    asyncImporter.onImportComplete(importId);

                                    LOG.error("IMPORT_ENTITY: {} failed to import entity: {}", importId, entityImportNotification);
                                }
                            }
                            break;

                            default:
                                throw new IllegalStateException("Unknown notification type: " + message.getType().name());
                        }

                        if (StringUtils.isNotEmpty(exceptionClassName)) {
                            LOG.warn("{}: Pausing & retry: Try: {}: Pause: {} ms. Handled!", exceptionClassName, numRetries, adaptiveWaiter.waitDuration);

                            exceptionClassName = StringUtils.EMPTY;
                        }
                        break;
                    } catch (Throwable e) {
                        RequestContext.get().resetEntityGuidUpdates();

                        exceptionClassName = e.getClass().getSimpleName();

                        // don't retry in following conditions:
                        //  1. number of retry attempts reached configured count
                        //  2. notification processing failed due to invalid data (non-existing type, entity, ..)
                        boolean        maxRetriesReached    = numRetries == (maxRetries - 1);
                        AtlasErrorCode errorCode            = (e instanceof AtlasBaseException) ? ((AtlasBaseException) e).getAtlasErrorCode() : null;
                        boolean        unrecoverableFailure = errorCode != null && (Response.Status.NOT_FOUND.equals(errorCode.getHttpCode()) || Response.Status.BAD_REQUEST.equals(errorCode.getHttpCode()));

                        if (maxRetriesReached || unrecoverableFailure) {
                            try {
                                String strMessage = AbstractNotification.getMessageJson(message);

                                if (unrecoverableFailure) {
                                    LOG.warn("Unrecoverable failure while processing message {}", strMessage, e);
                                } else {
                                    LOG.warn("Max retries exceeded for message {}", strMessage, e);
                                }

                                stats.isFailedMsg = true;

                                failedMessages.add(strMessage);

                                if (failedMessages.size() >= failedMsgCacheSize) {
                                    recordFailedMessages();
                                }
                            } catch (Throwable t) {
                                LOG.warn("error while recording failed message: type={}, topic={}, partition={}, offset={}", message.getType(), kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset(), t);
                            }

                            return;
                        } else if (e instanceof org.apache.atlas.repository.graphdb.AtlasSchemaViolationException) {
                            LOG.warn("{}: Continuing: {}", exceptionClassName, e.getMessage());
                        } else if (exceptionClassName.equals(EXCEPTION_CLASS_NAME_JANUSGRAPH_EXCEPTION) || exceptionClassName.equals(EXCEPTION_CLASS_NAME_PERMANENTLOCKING_EXCEPTION)) {
                            LOG.warn("{}: Pausing & retry: Try: {}: Pause: {} ms. {}", exceptionClassName, numRetries, adaptiveWaiter.waitDuration, e.getMessage());

                            adaptiveWaiter.pause(e);
                        } else {
                            LOG.warn("Error handling message", e);

                            try {
                                LOG.info("Sleeping for {} ms before retry", consumerRetryInterval);

                                Thread.sleep(consumerRetryInterval);
                            } catch (InterruptedException ie) {
                                LOG.error("Notification consumer thread sleep interrupted");
                            }
                        }
                    } finally {
                        RequestContext.clear();
                    }
                }

                commit(kafkaMsg);
            } finally {
                AtlasPerfTracer.log(perf);

                stats.timeTakenMs = System.currentTimeMillis() - startTime;

                metricsUtil.onNotificationProcessingComplete(kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset(), stats);

                if (stats.timeTakenMs > largeMessageProcessingTimeThresholdMs) {
                    try {
                        String strMessage = AbstractNotification.getMessageJson(message);

                        LOG.warn("msgProcessingTime={}, msgSize={}, topic={}, partition={}, offset={}}", stats.timeTakenMs, strMessage.length(), kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset());

                        LARGE_MESSAGES_LOG.warn("{\"msgProcessingTime\":{},\"msgSize\":{},\"topic\":{},\"partition\":{},\"topicOffset\":{},\"data\":{}}", stats.timeTakenMs, strMessage.length(), kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset(), strMessage);
                    } catch (Throwable t) {
                        LOG.warn("error while recording large message: msgProcessingTime={}, type={}, topic={}, partition={}, offset={}", stats.timeTakenMs, message.getType(), kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset(), t);
                    }
                }

                if (auditLog != null) {
                    auditLog.setHttpStatus(stats.isFailedMsg ? SC_BAD_REQUEST : SC_OK);
                    auditLog.setTimeTaken(stats.timeTakenMs);

                    AuditFilter.audit(auditLog);
                }

                Instant now = Instant.now();

                if (now.isAfter(nextStatsLogTime)) {
                    LOG.info("STATS: {}", AtlasJson.toJson(metricsUtil.getStats()));

                    nextStatsLogTime = AtlasMetricsCounter.getNextHourStartTime(now);
                }

                if (importRequestComplete) {
                    asyncImporter.onCompleteImportRequest(((AtlasEntityImportNotification) message).getImportId());
                }
            }
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

        private void createOrUpdate(AtlasEntitiesWithExtInfo entities, boolean isPartialUpdate, NotificationStat stats, PreprocessorContext context) throws AtlasBaseException {
            List<AtlasEntity> entitiesList = entities.getEntities();
            AtlasEntityStream entityStream = new AtlasEntityStream(entities);

            if (commitBatchSize <= 0 || entitiesList.size() <= commitBatchSize) {
                EntityMutationResponse response = atlasEntityStore.createOrUpdate(entityStream, isPartialUpdate);

                recordProcessedEntities(response, stats, context);
            } else {
                for (int fromIdx = 0; fromIdx < entitiesList.size(); fromIdx += commitBatchSize) {
                    int toIndex = fromIdx + commitBatchSize;

                    if (toIndex > entitiesList.size()) {
                        toIndex = entitiesList.size();
                    }

                    List<AtlasEntity> entitiesBatch = new ArrayList<>(entitiesList.subList(fromIdx, toIndex));

                    updateProcessedEntityReferences(entitiesBatch, context.getGuidAssignments());

                    AtlasEntitiesWithExtInfo batch       = new AtlasEntitiesWithExtInfo(entitiesBatch);
                    AtlasEntityStream        batchStream = new AtlasEntityStream(batch, entityStream);
                    EntityMutationResponse   response    = atlasEntityStore.createOrUpdate(batchStream, isPartialUpdate);

                    recordProcessedEntities(response, stats, context);

                    RequestContext.get().resetEntityGuidUpdates();

                    entityCorrelationManager.add(context.isSpooledMessage(), context.getMsgCreated(), response.getDeletedEntities());

                    RequestContext.get().clearCache();
                }
            }

            if (context != null) {
                context.prepareForPostUpdate();

                List<AtlasEntity> postUpdateEntities = context.getPostUpdateEntities();

                if (CollectionUtils.isNotEmpty(postUpdateEntities)) {
                    atlasEntityStore.createOrUpdate(new AtlasEntityStream(postUpdateEntities), true);
                }
            }
        }

        private void recordFailedMessages() {
            //logging failed messages
            for (String message : failedMessages) {
                FAILED_LOG.error("[DROPPED_NOTIFICATION] {}", message);
            }

            failedMessages.clear();
        }

        private void commit(AtlasKafkaMessage<HookNotification> kafkaMessage) {
            recordFailedMessages();

            long commitOffset = kafkaMessage.getOffset() + 1;

            lastCommittedPartitionOffset.put(kafkaMessage.getTopicPartition(), commitOffset);

            consumer.commit(kafkaMessage.getTopicPartition(), commitOffset);
        }
    }
}
