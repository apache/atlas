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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
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
import org.apache.atlas.notification.pc.Ticket;
import org.apache.atlas.notification.preprocessor.EntityPreprocessor;
import org.apache.atlas.notification.preprocessor.GenericEntityPreprocessor;
import org.apache.atlas.notification.preprocessor.PreprocessorContext;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AdaptiveWaiter;
import org.apache.atlas.util.AtlasMetricsCounter;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.util.AtlasMetricsUtil.NotificationStat;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.utils.LruCache;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.atlas.web.filters.AuditFilter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.atlas.AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND;
import static org.apache.atlas.model.instance.AtlasObjectId.KEY_GUID;
import static org.apache.atlas.model.instance.AtlasObjectId.KEY_TYPENAME;
import static org.apache.atlas.model.instance.AtlasObjectId.KEY_UNIQUE_ATTRIBUTES;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_AUTHORIZE_USING_MESSAGE_USER;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_COMMIT_BATCH_SIZE;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_FAILEDCACHESIZE_PROPERTY;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_MAX_RETRY_INTERVAL;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_MIN_RETRY_INTERVAL;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_ENABLED;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_NAMES;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_PROCESS_UPD_NAME_WITH_QUALIFIED_NAME;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_CACHE_SIZE;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_ENABLED;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_NAMES;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES_ENABLED;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_SPARK_PROCESS_ATTRIBUTES;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD;
import static org.apache.atlas.notification.NotificationHookConsumer.DUMMY_DATABASE;
import static org.apache.atlas.notification.NotificationHookConsumer.DUMMY_TABLE;
import static org.apache.atlas.notification.NotificationHookConsumer.VALUES_TMP_TABLE_NAME_PREFIX;
import static org.apache.atlas.notification.preprocessor.EntityPreprocessor.TYPE_HIVE_PROCESS;
import static org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI;

public class SerialEntityProcessor implements NotificationEntityProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(SerialEntityProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger(NotificationHookConsumer.class);

    private static final String EXCEPTION_CLASS_NAME_JANUSGRAPH_EXCEPTION       = "JanusGraphException";
    private static final String EXCEPTION_CLASS_NAME_PERMANENTLOCKING_EXCEPTION = "PermanentLockingException";
    private static final String THREADNAME_PREFIX                               = NotificationHookConsumer.class.getSimpleName();

    private static final int    SC_OK                    = 200;
    private static final int    SC_BAD_REQUEST           = 400;
    private static final String TYPE_HIVE_COLUMN_LINEAGE = "hive_column_lineage";
    private static final String ATTRIBUTE_INPUTS         = "inputs";
    private static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";

    private final int                                               commitBatchSize;
    private final AtlasEntityStore                                  atlasEntityStore;
    private final EntityCorrelationManager                          entityCorrelationManager;
    private final AtlasTypeRegistry                                 typeRegistry;
    private final Configuration                                     applicationProperties;
    private final int                                               minWaitDuration;
    private final int                                               maxWaitDuration;
    private final int                                               consumerRetryInterval = 500;
    private final AdaptiveWaiter                                    adaptiveWaiter;
    private final boolean                                           createShellEntityForNonExistingReference;
    private final boolean                                           authorizeUsingMessageUser;
    private final int                                               largeMessageProcessingTimeThresholdMs;
    private final List<String>                                      failedMessages;
    private final int                                               failedMsgCacheSize;
    private final boolean                                           s3V2DirectoryPruneObjectPrefix;
    private final int                                               skipHiveColumnLineageHive20633InputsThreshold;
    private final boolean                                           skipHiveColumnLineageHive20633;
    private final boolean                                           updateHiveProcessNameWithQualifiedName;
    private final boolean                                           hiveTypesRemoveOwnedRefAttrs;
    private final boolean                                           rdbmsTypesRemoveOwnedRefAttrs;
    private final boolean                                           sparkProcessAttributes;
    private final List<Pattern>                                     entityTypesToIgnore   = new ArrayList<>();
    private final List<Pattern>                                     entitiesToIgnore      = new ArrayList<>();
    private final List<Pattern>                                     hiveTablesToIgnore    = new ArrayList<>();
    private final List<Pattern>                                     hiveTablesToPrune     = new ArrayList<>();
    private final List<String>                                      hiveDummyDatabasesToIgnore;
    private final List<String>                                      hiveDummyTablesToIgnore;
    private final List<String>                                      hiveTablePrefixesToIgnore;
    private final Map<String, PreprocessorContext.PreprocessAction> hiveTablesCache;
    private final AsyncImporter                                     asyncImporter;
    private       int                                               maxRetries            = 3;
    private       Map<String, Authentication>                       authnCache;
    private       AtlasInstanceConverter                            instanceConverter;
    private       AtlasMetricsUtil metricsUtil;
    private       Logger                                            failedMessageLog;
    private       Logger                                            largeMessagesLog;
    private       Instant                                           nextStatsLogTime      = AtlasMetricsCounter.getNextHourStartTime(Instant.now());
    private       boolean                                           preprocessEnabled;

    public SerialEntityProcessor(Configuration applicationProperties, AtlasMetricsUtil metricsUtil, Map<String, Authentication> authnCache,
            AtlasEntityStore atlasEntityStore, AtlasInstanceConverter instanceConverter, EntityCorrelationManager entityCorrelationManager,
            AtlasTypeRegistry typeRegistry, Logger failedMessageLogger, Logger largeMessagesLogger, AsyncImporter asyncImporter) {
        this.failedMessageLog = failedMessageLogger;
        this.largeMessagesLog = largeMessagesLogger;
        this.metricsUtil      = metricsUtil;
        this.failedMessages        = new ArrayList<>();
        this.authnCache            = authnCache;
        this.instanceConverter     = instanceConverter;
        this.applicationProperties = applicationProperties;

        this.atlasEntityStore         = atlasEntityStore;
        this.entityCorrelationManager = entityCorrelationManager;
        this.typeRegistry             = typeRegistry;
        this.failedMsgCacheSize       = this.applicationProperties.getInt(CONSUMER_FAILEDCACHESIZE_PROPERTY, 1);

        this.maxRetries                                    = applicationProperties.getInt(CONSUMER_RETRIES_PROPERTY, 3);
        this.largeMessageProcessingTimeThresholdMs         = this.applicationProperties.getInt("atlas.notification.consumer.large.message.processing.time.threshold.ms", 60 * 1000);  //  60 sec by default
        this.minWaitDuration                               = this.applicationProperties.getInt(CONSUMER_MIN_RETRY_INTERVAL, consumerRetryInterval); // 500 ms  by default
        this.maxWaitDuration                               = this.applicationProperties.getInt(CONSUMER_MAX_RETRY_INTERVAL, minWaitDuration * 60);  //  30 sec by default
        this.commitBatchSize                               = this.applicationProperties.getInt(CONSUMER_COMMIT_BATCH_SIZE, 50);
        this.authorizeUsingMessageUser                     = this.applicationProperties.getBoolean(CONSUMER_AUTHORIZE_USING_MESSAGE_USER, false);
        this.skipHiveColumnLineageHive20633InputsThreshold = this.applicationProperties.getInt(CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, 15); // skip if avg # of inputs is > 15
        this.skipHiveColumnLineageHive20633                = this.applicationProperties.getBoolean(CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, false);
        this.updateHiveProcessNameWithQualifiedName        = this.applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_PROCESS_UPD_NAME_WITH_QUALIFIED_NAME, true);
        this.hiveTypesRemoveOwnedRefAttrs                  = this.applicationProperties.getBoolean(CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, true);
        this.rdbmsTypesRemoveOwnedRefAttrs                 = this.applicationProperties.getBoolean(CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS, true);
        this.s3V2DirectoryPruneObjectPrefix                = applicationProperties.getBoolean(CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX, true);
        this.sparkProcessAttributes                        = this.applicationProperties.getBoolean(CONSUMER_PREPROCESS_SPARK_PROCESS_ATTRIBUTES, true);
        this.adaptiveWaiter                                = new AdaptiveWaiter(minWaitDuration, maxWaitDuration, minWaitDuration);
        this.createShellEntityForNonExistingReference      = AtlasConfiguration.NOTIFICATION_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF.getBoolean();
        this.asyncImporter                                 = asyncImporter;

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
            hiveTablesCache = new LruCache<>(this.applicationProperties.getInt(CONSUMER_PREPROCESS_HIVE_TABLE_CACHE_SIZE, 10000), 0);
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

        preprocessEnabled = skipHiveColumnLineageHive20633 || updateHiveProcessNameWithQualifiedName || hiveTypesRemoveOwnedRefAttrs || rdbmsTypesRemoveOwnedRefAttrs || !hiveTablesToIgnore.isEmpty() || !hiveTablesToPrune.isEmpty() || !hiveDummyDatabasesToIgnore.isEmpty() || !hiveDummyTablesToIgnore.isEmpty() || !hiveTablePrefixesToIgnore.isEmpty() || sparkProcessAttributes;

        LOG.info("{}={}", CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, skipHiveColumnLineageHive20633);
        LOG.info("{}={}", CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, skipHiveColumnLineageHive20633InputsThreshold);
        LOG.info("{}={}", CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, hiveTypesRemoveOwnedRefAttrs);
        LOG.info("{}={}", CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS, rdbmsTypesRemoveOwnedRefAttrs);
        LOG.info("{}={}", CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX, s3V2DirectoryPruneObjectPrefix);
        LOG.info("{}={}", CONSUMER_COMMIT_BATCH_SIZE, commitBatchSize);
    }

    public TopicPartitionOffsetResult handleMessage(AtlasKafkaMessage<HookNotification> kafkaMsg) {
        return handleMessage(new Ticket(kafkaMsg));
    }

    @Override
    public TopicPartitionOffsetResult collectResults() {
        return null;
    }

    @Override
    public void shutdown() {
    }

    public TopicPartitionOffsetResult handleMessage(Ticket ticket) {
        AtlasPerfTracer                     perf                  = null;
        AtlasKafkaMessage<HookNotification> kafkaMsg              = ticket.getMessage();
        HookNotification                    message               = kafkaMsg.getMessage();
        String                              messageUser           = message.getUser();
        long                                startTime             = System.currentTimeMillis();
        AtlasMetricsUtil.NotificationStat   stats                 = new AtlasMetricsUtil.NotificationStat();
        AuditFilter.AuditLog                auditLog              = null;
        boolean                             importRequestComplete = false;

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
                        final HookNotificationV1.EntityCreateRequest createRequest = (HookNotificationV1.EntityCreateRequest) message;
                        final AtlasEntity.AtlasEntitiesWithExtInfo   entities      = instanceConverter.toAtlasEntities(createRequest.getEntities());
                        final EntityCreateRequestV2 v2Request                      = new EntityCreateRequestV2(message.getUser(), entities);

                        kafkaMsg = new AtlasKafkaMessage<>(v2Request, kafkaMsg.getOffset(), kafkaMsg.getTopic(), kafkaMsg.getPartition());
                        message  = kafkaMsg.getMessage();
                    }
                    break;

                    case ENTITY_FULL_UPDATE: {
                        final HookNotificationV1.EntityUpdateRequest updateRequest = (HookNotificationV1.EntityUpdateRequest) message;
                        final AtlasEntity.AtlasEntitiesWithExtInfo   entities      = instanceConverter.toAtlasEntities(updateRequest.getEntities());
                        final EntityUpdateRequestV2 v2Request                      = new EntityUpdateRequestV2(messageUser, entities);

                        kafkaMsg = new AtlasKafkaMessage<>(v2Request, kafkaMsg.getOffset(), kafkaMsg.getTopic(), kafkaMsg.getPartition());
                        message  = kafkaMsg.getMessage();
                    }
                    break;
                }
            } catch (AtlasBaseException excp) {
                LOG.error("handleMessage(): failed to convert V1 message to V2", message.getType().name());
            }

            PreprocessorContext context = preProcessNotificationMessage(kafkaMsg);

            if (isEmptyMessage(kafkaMsg)) {
                return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
            }

            // Used for intermediate conversions during create and update
            String exceptionClassName = StringUtils.EMPTY;
            for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("handleMessage({}): attempt {}", message.getType().name(), numRetries);
                }

                try {
                    RequestContext requestContext = RequestContext.get();

                    requestContext.setAttemptCount(numRetries + 1);
                    requestContext.setMaxAttempts(maxRetries);

                    requestContext.setUser(messageUser, null);
                    requestContext.setInNotificationProcessing(true);
                    requestContext.setCreateShellEntityForNonExistingReference(createShellEntityForNonExistingReference);

                    switch (message.getType()) {
                        case ENTITY_CREATE: {
                            final HookNotificationV1.EntityCreateRequest createRequest = (HookNotificationV1.EntityCreateRequest) message;
                            final AtlasEntity.AtlasEntitiesWithExtInfo   entities      = instanceConverter.toAtlasEntities(createRequest.getEntities());

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClient.API_V1.CREATE_ENTITY.getMethod(),
                                        AtlasClient.API_V1.CREATE_ENTITY.getNormalizedPath());
                            }

                            createOrUpdate(entities, false, stats, context);
                        }
                        break;

                        case ENTITY_PARTIAL_UPDATE: {
                            final HookNotificationV1.EntityPartialUpdateRequest partialUpdateRequest = (HookNotificationV1.EntityPartialUpdateRequest) message;
                            final Referenceable                                 referenceable        = partialUpdateRequest.getEntity();
                            final AtlasEntity.AtlasEntitiesWithExtInfo          entities             = instanceConverter.toAtlasEntity(referenceable);

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                        String.format(AtlasClientV2.API_V2.UPDATE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), partialUpdateRequest.getTypeName()));
                            }

                            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(partialUpdateRequest.getTypeName());
                            String          guid       = AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, Collections.singletonMap(partialUpdateRequest.getAttribute(), (Object) partialUpdateRequest.getAttributeValue()));

                            // There should only be one root entity
                            entities.getEntities().get(0).setGuid(guid);

                            createOrUpdate(entities, true, stats, context);
                        }
                        break;

                        case ENTITY_DELETE: {
                            final HookNotificationV1.EntityDeleteRequest deleteRequest = (HookNotificationV1.EntityDeleteRequest) message;

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getMethod(),
                                        String.format(AtlasClientV2.API_V2.DELETE_ENTITY_BY_ATTRIBUTE.getNormalizedPath(), deleteRequest.getTypeName()));
                            }

                            try {
                                AtlasEntityType type = (AtlasEntityType) typeRegistry.getType(deleteRequest.getTypeName());

                                EntityMutationResponse response = atlasEntityStore.deleteByUniqueAttributes(type, Collections.singletonMap(deleteRequest.getAttribute(), (Object) deleteRequest.getAttributeValue()));

                                stats.updateStats(response);
                                entityCorrelationManager.add(kafkaMsg.getSpooled(), kafkaMsg.getMsgCreated(), response.getDeletedEntities());
                            } catch (ClassCastException cle) {
                                LOG.error("Failed to delete entity {}", deleteRequest);
                            }
                        }
                        break;

                        case ENTITY_FULL_UPDATE: {
                            final HookNotificationV1.EntityUpdateRequest updateRequest = (HookNotificationV1.EntityUpdateRequest) message;
                            final AtlasEntity.AtlasEntitiesWithExtInfo   entities      = instanceConverter.toAtlasEntities(updateRequest.getEntities());

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(),
                                        AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                            }

                            createOrUpdate(entities, false, stats, context);
                        }
                        break;

                        case ENTITY_CREATE_V2: {
                            final EntityCreateRequestV2    createRequestV2 = (EntityCreateRequestV2) message;
                            final AtlasEntitiesWithExtInfo entities        = createRequestV2.getEntities();

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClientV2.API_V2.CREATE_ENTITY.getMethod(),
                                        AtlasClientV2.API_V2.CREATE_ENTITY.getNormalizedPath());
                            }

                            createOrUpdate(entities, false, stats, context);
                        }
                        break;

                        case ENTITY_PARTIAL_UPDATE_V2: {
                            final EntityPartialUpdateRequestV2 partialUpdateRequest = (EntityPartialUpdateRequestV2) message;
                            final AtlasObjectId                entityId             = partialUpdateRequest.getEntityId();
                            final AtlasEntityWithExtInfo       entity               = partialUpdateRequest.getEntity();

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(),
                                        AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
                            }

                            EntityMutationResponse response = atlasEntityStore.updateEntity(entityId, entity, true);

                            stats.updateStats(response);
                        }
                        break;

                        case ENTITY_FULL_UPDATE_V2: {
                            final EntityUpdateRequestV2    updateRequest = (EntityUpdateRequestV2) message;
                            final AtlasEntitiesWithExtInfo entities      = updateRequest.getEntities();

                            if (auditLog == null) {
                                auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
                                        AtlasClientV2.API_V2.UPDATE_ENTITY.getMethod(),
                                        AtlasClientV2.API_V2.UPDATE_ENTITY.getNormalizedPath());
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
                                        auditLog = new AuditFilter.AuditLog(messageUser, THREADNAME_PREFIX,
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
                            final AtlasTypesDefImportNotification typeDefImportNotification = (AtlasTypesDefImportNotification) message;
                            final String                          importId                  = typeDefImportNotification.getImportId();
                            final AtlasTypesDef                   typesDef                  = typeDefImportNotification.getTypesDef();
                            try {
                                asyncImporter.onImportTypeDef(typesDef, importId);
                            } catch (AtlasBaseException abe) {
                                LOG.error("IMPORT_TYPE_DEF: {} failed to import type definition: {}", importId, typesDef.toString());

                                asyncImporter.onImportComplete(importId);
                                importRequestComplete = true;
                            }
                        }
                        break;

                        case IMPORT_ENTITY: {
                            final AtlasEntityImportNotification entityImportNotification = (AtlasEntityImportNotification) message;
                            final String                                           importId                 = entityImportNotification.getImportId();
                            final AtlasEntity.AtlasEntityWithExtInfo               entityWithExtInfo        = entityImportNotification.getEntity();
                            final int                                              position                 = entityImportNotification.getPosition();
                            boolean                                                completeImport           = false;

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
                        LOG.warn("{}: Offset: {}: Pausing & retry: Try: {}: Pause: {} ms. Handled!",
                                exceptionClassName, kafkaMsg.getOffset(), numRetries, adaptiveWaiter.getWaitDuration());
                        exceptionClassName = StringUtils.EMPTY;
                    }
                    break;
                } catch (Throwable e) {
                    RequestContext.get().resetEntityGuidUpdates();
                    exceptionClassName = e.getClass().getSimpleName();
                    if (e instanceof AtlasBaseException) {
                        AtlasBaseException baseException = (AtlasBaseException) e;
                        if (baseException.getAtlasErrorCode().equals(INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)) {
                            LOG.warn("Error handling message: {}: {} - {} - {}", ticket.getMessage().getMessage().getType(), baseException.getAtlasErrorCode(), baseException.getMessage(), ticket.getQualifiedNamesSet());
                            return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
                        }
                    } else if (e instanceof InterruptedException) {
                        LOG.error("Interrupted!", e);
                        return null;
                    }

                    if (numRetries == (maxRetries - 1)) {
                        String strMessage = AbstractNotification.getMessageJson(message);

                        LOG.warn("Offset: {}: Max retries: {} exceeded for message {}", kafkaMsg.getOffset(), maxRetries, strMessage, e);

                        stats.isFailedMsg = true;

                        failedMessages.add(strMessage);

                        if (failedMessages.size() >= failedMsgCacheSize) {
                            recordFailedMessages(failedMessages);
                        }

                        return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
                    } else if (e instanceof org.apache.atlas.repository.graphdb.AtlasSchemaViolationException) {
                        LOG.warn("{} - {}: {}: Continuing: {}", kafkaMsg.getTopicPartition().toString(), kafkaMsg.getOffset(), exceptionClassName, e.getMessage());
                        return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
                    } else if (e instanceof java.lang.IllegalStateException || e instanceof NullPointerException) {
                        return null;
                    } else if (exceptionClassName.equals(EXCEPTION_CLASS_NAME_JANUSGRAPH_EXCEPTION)
                            || exceptionClassName.equals(EXCEPTION_CLASS_NAME_PERMANENTLOCKING_EXCEPTION)) {
                        LOG.warn("{}: Offset: {}: Pausing & retry: Try: {}: Pause: {} ms. {}",
                                exceptionClassName, kafkaMsg.getOffset(), numRetries, adaptiveWaiter.getWaitDuration(), e.getMessage());

                        adaptiveWaiter.pause((Exception) e);
                    } else {
                        LOG.warn("Error handling message", e);

                        LOG.info("Sleeping for {} ms before retry", adaptiveWaiter.getWaitDuration());

                        adaptiveWaiter.pause((Exception) e);
                    }
                } finally {
                    RequestContext.clear();
                }
            }

            return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
        } finally {
            AtlasPerfTracer.log(perf);

            stats.timeTakenMs = System.currentTimeMillis() - startTime;

            metricsUtil.onNotificationProcessingComplete(kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset(), stats);

            if (stats.timeTakenMs > largeMessageProcessingTimeThresholdMs) {
                String strMessage = AbstractNotification.getMessageJson(message);

                LOG.warn("msgProcessingTime={}, msgSize={}, topicOffset={}}", stats.timeTakenMs, strMessage.length(), kafkaMsg.getOffset());
                largeMessagesLog.warn("{\"msgProcessingTime\":{},\"msgSize\":{},\"topicOffset\":{},\"data\":{}}", stats.timeTakenMs, strMessage.length(), kafkaMsg.getOffset(), strMessage);
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

    public List<String> getFailedMessages() {
        return this.failedMessages;
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

                EntityMutationResponse response = atlasEntityStore.createOrUpdate(batchStream, isPartialUpdate);

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

    private void preprocessEntities(PreprocessorContext context) {
        GenericEntityPreprocessor genericEntityPreprocessor = new GenericEntityPreprocessor(this.entityTypesToIgnore, this.entitiesToIgnore);

        List<AtlasEntity> entities = context.getEntities();

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
                    LOG.debug("preprocess: moved {} hive_process/hive_column_lineage entities to end of list (listSize={}). topic-offset={}, partition={}", entities.size() - count, entities.size(), kafkaMsg.getOffset(), kafkaMsg.getPartition());
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
                        Collection inputs = (Collection) objInputs;

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
            updateProcessedEntityReferences((Collection) objVal, guidAssignments);
        } else if (objVal instanceof Map) {
            updateProcessedEntityReferences((Map) objVal, guidAssignments);
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

    private void updateProcessedEntityReferences(Collection objIds, Map<String, String> guidAssignments) {
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

    private void recordFailedMessages(List<String> failedMessages) {
        //logging failed messages
        for (String message : failedMessages) {
            failedMessageLog.error("[DROPPED_NOTIFICATION] {}", message);
        }

        failedMessages.clear();
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

    private void preprocessSparkProcessAttributes(PreprocessorContext context) {
        List<AtlasEntity> entities = context.getEntities();

        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                AtlasEntity        entity       = entities.get(i);
                EntityPreprocessor preprocessor = EntityPreprocessor.getSparkPreprocessor(entity.getTypeName());

                if (preprocessor != null) {
                    preprocessor.preprocess(entity, context);
                }
            }
        }
    }

    private boolean isMsgSourceVersion(String msgSourceVersion, String preprocessVersion) {
        if (StringUtils.isNotEmpty(msgSourceVersion) && StringUtils.isNotEmpty(preprocessVersion)) {
            return msgSourceVersion.contains(preprocessVersion);
        }
        return false;
    }
}
