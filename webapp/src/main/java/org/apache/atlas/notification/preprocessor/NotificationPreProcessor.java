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
package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.model.notification.MessageSource;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.NotificationEntityProcessor;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.QualifiedNameRouter;
import org.apache.atlas.notification.TopicPartitionOffsetResult;
import org.apache.atlas.notification.pc.Ticket;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.util.AtlasMetricsUtil.NotificationProcessorStats;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityPartialUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_FAILEDCACHESIZE_PROPERTY;
import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY;
import static org.apache.atlas.notification.preprocessor.EntityPreprocessor.getQualifiedName;

public class NotificationPreProcessor implements NotificationEntityProcessor, TypeDefChangeListener {
    private static final Logger LOG      = LoggerFactory.getLogger("NOTIFICATION_PROCESSOR");
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger(NotificationPreProcessor.class);

    private final        int                     metadataTopicCount;
    private final        int                     lineageTopicCount;
    private final        int                     maxRetries;
    private final        boolean                 lineageTopicEnabled;
    private final        Configuration           configuration;
    private final        NotificationInterface   notificationInterface;
    private final        QualifiedNameRouter     metadataRouter;
    private final        QualifiedNameRouter     lineageRouter;
    private final        AtlasMetricsUtil        metricsUtil;
    private final        AtlasTypeRegistry       typeRegistry;
    private final        List<String>            failedMessages;
    private final        int                     failedMsgCacheSize;
    private final        Map<String, Boolean>    processTypeCache         = new ConcurrentHashMap<>();
    private              Set<String>             allProcessTypes;
    private              Logger                  failedMessageLog;
    private static final java.util.regex.Pattern GUID_PATTERN             = java.util.regex.Pattern.compile("^[a-fA-F0-9-]{36}$");
    public static final  String                  ATTRIBUTE_GUID           = "guid";
    public static final  String                  ATTRIBUTE_NAME           = "name";
    public static final  String                  ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
    /**
     * Maximum size of ThreadLocal rename cache per thread.
     * Uses LRU eviction policy - when cache exceeds this size, oldest entries are automatically removed.
     */
    private static final int                     MAX_RENAME_CACHE_SIZE    = 1000;

    // ThreadLocal rename routing map - each thread maintains its own rename chain mappings
    private static final ThreadLocal<Map<String, RenameRoutingInfo>> renameRoutingInfoMap =
            ThreadLocal.withInitial(() -> new LinkedHashMap<String, RenameRoutingInfo>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, RenameRoutingInfo> eldest) {
                    return size() > MAX_RENAME_CACHE_SIZE;
                }
            });

    public NotificationPreProcessor(Configuration configuration, AtlasMetricsUtil metricsUtil,
            AtlasTypeRegistry typeRegistry, Logger failedMessageLogger) {
        this.configuration         = configuration;
        this.failedMessageLog      = failedMessageLogger;
        this.notificationInterface = NotificationProvider.get();
        this.metadataTopicCount    = this.configuration.getInt("atlas.notification.processor.metadata.topic.count", 5);
        this.lineageTopicCount     = this.configuration.getInt("atlas.notification.processor.lineage.topic.count", 3);
        this.lineageTopicEnabled   = this.configuration.getBoolean("atlas.notification.processor.lineage.topic.enabled", true);
        this.maxRetries            = this.configuration.getInt(CONSUMER_RETRIES_PROPERTY, 3);
        this.failedMsgCacheSize    = this.configuration.getInt(CONSUMER_FAILEDCACHESIZE_PROPERTY, 1);
        this.metadataRouter        = new QualifiedNameRouter(metadataTopicCount, AtlasConfiguration.ATLAS_METADATA_TOPIC_PREFIX.getString());
        this.lineageRouter         = new QualifiedNameRouter(lineageTopicCount, AtlasConfiguration.ATLAS_LINEAGE_TOPIC_PREFIX.getString());
        this.metricsUtil           = metricsUtil;
        this.typeRegistry          = typeRegistry;
        this.failedMessages        = new ArrayList<>();

        // Initialize type registry early in constructor to avoid null pointer issues
        try {
            initializeTypeRegistry();
        } catch (AtlasException e) {
            LOG.warn("Failed to initialize type registry during construction: {}. Will use heuristics fallback only.", e.getMessage());
            // Initialize to empty unmodifiable set to avoid null pointer exceptions
            this.allProcessTypes = Collections.emptySet();
        }
    }

    @Override
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

    private TopicPartitionOffsetResult handleMessage(Ticket ticket) {
        AtlasPerfTracer                     perf      = null;
        AtlasKafkaMessage<HookNotification> kafkaMsg  = ticket.getMessage();
        HookNotification                    message   = kafkaMsg.getMessage();
        long                                startTime = System.currentTimeMillis();
        NotificationProcessorStats          stats     = new NotificationProcessorStats();

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, message.getType().name());
        }

        for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
            try {
                // Extract original source from JSON before deserializing
                NotificationMetadata notificationMetadata = buildNotificationMetadataFromMessage(kafkaMsg, kafkaMsg.getTopic(), kafkaMsg.getOffset());

                routeNotification(message, notificationMetadata);

                break;
            } catch (Exception e) {
                LOG.error("Error processing notification: {}", e.getMessage(), e);

                if (numRetries == (maxRetries - 1)) {
                    String strMessage = AbstractNotification.getMessageJson(message);

                    LOG.warn("Offset: {}: Max retries: {} exceeded for message {}", kafkaMsg.getOffset(), maxRetries, strMessage, e);

                    stats.setFailed(true);

                    failedMessages.add(strMessage);

                    if (failedMessages.size() >= failedMsgCacheSize) {
                        recordFailedMessages(kafkaMsg.getTopic(), failedMessages);
                    }

                    return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
                }
            } finally {
                AtlasPerfTracer.log(perf);
                stats.setProcessingTimeMs(System.currentTimeMillis() - startTime);
                metricsUtil.onNotificationProcessorComplete(kafkaMsg.getTopic(), kafkaMsg.getPartition(), kafkaMsg.getOffset(), stats);
            }
        }
        return new TopicPartitionOffsetResult(kafkaMsg.getTopicPartition(), kafkaMsg.getOffset());
    }

    private NotificationMetadata buildNotificationMetadataFromMessage(AtlasKafkaMessage kafkaMessage, String sourceTopic, long sourceOffset) {
        NotificationMetadata notificationMetadata = new NotificationMetadata();
        MessageSource        source               = new MessageSource();
        String               sourceTopicAndOffset = String.format("%s-%s", sourceTopic, sourceOffset);

        try {
            String originalSourceName = null;
            if (kafkaMessage != null && kafkaMessage.getSource() != null) {
                originalSourceName = kafkaMessage.getSource();
            }
            if (StringUtils.isNotBlank(originalSourceName)) {
                source.setSource(String.format("%s-%s", sourceTopicAndOffset, originalSourceName));
                LOG.debug("Extracted original source from JSON: {}", originalSourceName);
            } else {
                LOG.debug("No source found in JSON message, using fallback default source: {}", sourceTopicAndOffset);
                source.setSource(sourceTopicAndOffset);
            }

            if (kafkaMessage != null) {
                notificationMetadata.setMsgCreationTime(kafkaMessage.getMsgCreated());
            }
        } catch (Exception e) {
            LOG.debug("Error parsing JSON to extract source, using fallback default source {}: {}", sourceTopicAndOffset, e.getMessage());
            source.setSource(sourceTopicAndOffset);
        }

        notificationMetadata.setSource(source);
        return notificationMetadata;
    }

    /**
     * Converts V1 notifications to V2 format for unified processing.
     * This eliminates the need for separate V1/V2 processing logic.
     */
    private HookNotification convertV1ToV2(HookNotification notification) {
        try {
            switch (notification.getType()) {
                case ENTITY_CREATE: {
                    EntityCreateRequest v1Request = (EntityCreateRequest) notification;
                    // Convert V1 to V2 keeping ALL referred entities (filtering happens later by topic)
                    AtlasEntity.AtlasEntitiesWithExtInfo v2Entities = convertReferenceablesToAtlasEntities(v1Request.getEntities());
                    return new EntityCreateRequestV2(v1Request.getUser(), v2Entities);
                }
                case ENTITY_FULL_UPDATE: {
                    EntityUpdateRequest v1Request = (EntityUpdateRequest) notification;
                    // Convert V1 to V2 keeping ALL referred entities (filtering happens later by topic)
                    AtlasEntity.AtlasEntitiesWithExtInfo v2Entities = convertReferenceablesToAtlasEntities(v1Request.getEntities());
                    return new EntityUpdateRequestV2(v1Request.getUser(), v2Entities);
                }
                case ENTITY_PARTIAL_UPDATE:
                case ENTITY_DELETE:
                    // These don't have bulk entity lists, so we'll keep them as-is for now
                    // They'll be handled separately in the routing logic
                    return notification;

                default:
                    // Already V2 or other types
                    return notification;
            }
        } catch (Exception e) {
            LOG.warn("Failed to convert V1 notification to V2 format, will process as V1: {}", e.getMessage());
            return notification;
        }
    }

    /**
     * Converts a list of V1 Referenceable objects to V2 AtlasEntitiesWithExtInfo.
     * This handles negative GUID processing during conversion.
     */
    private AtlasEntity.AtlasEntitiesWithExtInfo convertReferenceablesToAtlasEntities(List<Referenceable> referenceables) {
        List<AtlasEntity>        entities         = new ArrayList<>();
        Map<String, AtlasEntity> referredEntities = new HashMap<>();

        if (referenceables != null) {
            // First, build the GUID-to-unique-attributes map from V1 entities
            Map<String, Map<String, Object>> guidToUniqueAttrsMap = buildGuidToUniqueAttributesMapFromReferenceables(referenceables);

            for (Referenceable referenceable : referenceables) {
                AtlasEntity entity = convertReferenceableToAtlasEntity(referenceable, guidToUniqueAttrsMap);
                entities.add(entity);
            }
        }

        return new AtlasEntity.AtlasEntitiesWithExtInfo(entities, new AtlasEntity.AtlasEntityExtInfo(referredEntities));
    }

    /**
     * Builds a map from GUID to uniqueAttributes for V1 Referenceable entities.
     */
    private Map<String, Map<String, Object>> buildGuidToUniqueAttributesMapFromReferenceables(List<Referenceable> referenceables) {
        Map<String, Map<String, Object>> guidToUniqueAttrsMap = new HashMap<>();

        for (Referenceable entity : referenceables) {
            if (entity.getValues() != null) {
                String guid = (String) entity.getValues().get(ATTRIBUTE_GUID);
                if (guid != null && guid.startsWith("-")) {
                    Map<String, Object> uniqueAttrs = new HashMap<>();
                    // Common unique attributes
                    Object qualifiedName = entity.getValues().get(ATTRIBUTE_QUALIFIED_NAME);
                    Object name          = entity.getValues().get(ATTRIBUTE_NAME);

                    if (qualifiedName != null) {
                        uniqueAttrs.put(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
                    }
                    if (name != null) {
                        uniqueAttrs.put(ATTRIBUTE_NAME, name);
                    }

                    if (!uniqueAttrs.isEmpty()) {
                        guidToUniqueAttrsMap.put(guid, uniqueAttrs);
                    }
                }
            }
        }

        return guidToUniqueAttrsMap;
    }

    /**
     * Converts a single V1 Referenceable to V2 AtlasEntity.
     * This handles negative GUID processing during conversion.
     */
    private AtlasEntity convertReferenceableToAtlasEntity(Referenceable referenceable, Map<String, Map<String, Object>> guidToUniqueAttrsMap) {
        AtlasEntity entity = new AtlasEntity();

        entity.setTypeName(referenceable.getTypeName());

        if (referenceable.getValues() != null) {
            // Extract GUID if present
            Object guidObj = referenceable.getValues().get(ATTRIBUTE_GUID);
            if (guidObj != null) {
                entity.setGuid(guidObj.toString());
            }

            // Convert all other attributes
            Map<String, Object> attributes             = new HashMap<>();
            Map<String, Object> relationshipAttributes = new HashMap<>();

            for (Map.Entry<String, Object> entry : referenceable.getValues().entrySet()) {
                String key   = entry.getKey();
                Object value = entry.getValue();

                if (ATTRIBUTE_GUID.equals(key)) {
                    // Already handled above
                    continue;
                }

                // Simple heuristic: if the value is a Referenceable or contains entity references,
                // it might be a relationship attribute. Otherwise, treat as regular attribute.
                if (isLikelyRelationshipAttribute(value)) {
                    relationshipAttributes.put(key, convertAttributeValue(value, guidToUniqueAttrsMap, true)); // true = is relationship attribute
                } else {
                    attributes.put(key, convertAttributeValue(value, guidToUniqueAttrsMap, false)); // false = regular attribute
                }
            }

            entity.setAttributes(attributes);
            if (!relationshipAttributes.isEmpty()) {
                entity.setRelationshipAttributes(relationshipAttributes);
            }
        }

        // Convert classifications if present
        if (referenceable.getTraits() != null && !referenceable.getTraits().isEmpty()) {
            // For now, we'll skip classification conversion to keep the implementation simple
            // In a full implementation, we would need to convert V1 traits to V2 AtlasClassification objects
            LOG.debug("Skipping classification conversion for entity {} - {} traits present",
                    referenceable.getTypeName(), referenceable.getTraits().size());
        }

        LOG.debug("Converted V1 Referenceable {} to V2 AtlasEntity", referenceable.getTypeName());

        return entity;
    }

    /**
     * Heuristic to determine if an attribute is likely a relationship attribute.
     */
    private boolean isLikelyRelationshipAttribute(Object value) {
        if (value instanceof Referenceable) {
            return true;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return !list.isEmpty() && list.get(0) instanceof Referenceable;
        }
        return false;
    }

    /**
     * Converts attribute values during V1 to V2 conversion.
     * Handles negative GUID processing for relationship attributes.
     */
    private Object convertAttributeValue(Object value, Map<String, Map<String, Object>> guidToUniqueAttrsMap, boolean isRelationshipAttribute) {
        if (value instanceof Referenceable) {
            Referenceable ref = (Referenceable) value;
            // Convert to AtlasObjectId
            AtlasObjectId objectId = new AtlasObjectId();
            objectId.setTypeName(ref.getTypeName());

            if (ref == null || ref.getValues() == null) {
                return objectId;
            }

            Object guidObj = ref.getValues().get(ATTRIBUTE_GUID);
            String guid    = guidObj != null ? guidObj.toString() : null;

            // Handle negative GUIDs for relationship attributes
            if (isRelationshipAttribute && guid != null && guid.startsWith("-")) {
                // Look up uniqueAttributes from the original V1 entities
                Map<String, Object> uniqueAttrs = guidToUniqueAttrsMap.get(guid);
                if (uniqueAttrs != null && !uniqueAttrs.isEmpty()) {
                    // Create AtlasObjectId with uniqueAttributes only (no GUID)
                    objectId.setUniqueAttributes(uniqueAttrs);
                    LOG.debug("Replaced negative GUID {} with uniqueAttributes {} for type {} during V1 to V2 conversion",
                            guid, uniqueAttrs, ref.getTypeName());
                } else {
                    // Fallback: extract uniqueAttributes from the referenceable itself
                    Map<String, Object> uniqueAttributes = new HashMap<>();
                    Object              qualifiedName    = ref.getValues().get(ATTRIBUTE_QUALIFIED_NAME);
                    Object              name             = ref.getValues().get(ATTRIBUTE_NAME);

                    if (qualifiedName != null) {
                        uniqueAttributes.put(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
                    }
                    if (name != null) {
                        uniqueAttributes.put(ATTRIBUTE_NAME, name);
                    }

                    if (!uniqueAttributes.isEmpty()) {
                        objectId.setUniqueAttributes(uniqueAttributes);
                        LOG.debug("Used fallback uniqueAttributes {} for negative GUID {} during V1 to V2 conversion",
                                uniqueAttributes, guid);
                    } else {
                        // Last resort: keep the negative GUID
                        objectId.setGuid(guid);
                        LOG.warn("Could not resolve negative GUID {} for type {} during V1 to V2 conversion",
                                guid, ref.getTypeName());
                    }
                }
            } else {
                // Regular GUID or non-relationship attribute - preserve as-is
                return value;
            }
        } else if (value instanceof List) {
            List<?>      list          = (List<?>) value;
            List<Object> convertedList = new ArrayList<>();
            for (Object item : list) {
                convertedList.add(convertAttributeValue(item, guidToUniqueAttrsMap, isRelationshipAttribute));
            }
            return convertedList;
        } else if (value instanceof Map) {
            Map<?, ?>           map          = (Map<?, ?>) value;
            Map<Object, Object> convertedMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                convertedMap.put(entry.getKey(), convertAttributeValue(entry.getValue(), guidToUniqueAttrsMap, isRelationshipAttribute));
            }
            return convertedMap;
        }

        return value;
    }

    // Initialize local type registry (primary method for type hierarchy checking)
    private void initializeTypeRegistry() throws AtlasException {
        if (typeRegistry != null) {
            try {
                Set<String> processTypes = typeRegistry.getAllEntityTypes().stream()
                        .filter(x -> x.getTypeName().equals(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS) || x.isSubTypeOf(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS))
                        .map(x -> x.getTypeName())
                        .collect(Collectors.toSet());
                allProcessTypes = Collections.unmodifiableSet(processTypes);
                LOG.info("Successfully initialized process types cache with {} types (unmodifiable)", allProcessTypes.size());
            } catch (Exception e) {
                LOG.warn("Failed to build process types cache: {}. Initializing empty unmodifiable set.", e.getMessage());
                allProcessTypes = Collections.emptySet();
            }
        } else {
            LOG.warn("Type registry is null. Initializing empty unmodifiable process types set.");
            allProcessTypes = Collections.emptySet();
        }
    }

    /**
     * Determines if an entity is a lineage entity based on its type.
     * An entity is considered lineage if its type is "Process" or a subtype of "Process".
     */
    private boolean isLineageEntity(Object entity) {
        String entityTypeName = getEntityTypeName(entity);

        // Check cache first to avoid repeated API calls
        Boolean cachedResult = processTypeCache.get(entityTypeName);
        if (cachedResult != null) {
            return cachedResult;
        }

        boolean isProcessType = isProcessOrSubtype(entityTypeName);

        // Cache the result for future use
        processTypeCache.put(entityTypeName, isProcessType);

        return isProcessType;
    }

    /**
     * Checks if the given type name is "Process" or a subtype of "Process" using AtlasTypeRegistry
     */
    private boolean isProcessOrSubtype(String typeName) {
        // Direct match for Process
        if (AtlasBaseTypeDef.ATLAS_TYPE_PROCESS.equals(typeName)) {
            return true;
        }

        if (typeRegistry != null && allProcessTypes != null) {
            try {
                if (typeRegistry.isRegisteredType(typeName)) {
                    boolean isTypeOrSubTypeOfProcess = allProcessTypes.contains(typeName);
                    LOG.debug("Local type registry check for '{}': isSubTypeOfProcess = {}", typeName, isTypeOrSubTypeOfProcess);
                    return isTypeOrSubTypeOfProcess;
                } else {
                    LOG.debug("Type '{}' not found in local type registry", typeName);
                }
            } catch (ClassCastException e) {
                LOG.debug("Type '{}' is not an entity type in local registry: {}",
                        typeName, e.getMessage());
            }
        }

        return false;
    }

    /**
     * Gets the type name from different entity types.
     */
    private String getEntityTypeName(Object entity) {
        if (entity instanceof AtlasEntity) {
            return ((AtlasEntity) entity).getTypeName();
        } else if (entity instanceof Referenceable) {
            return ((Referenceable) entity).getTypeName();
        } else if (entity instanceof AtlasObjectId) {
            return ((AtlasObjectId) entity).getTypeName();
        } else if (entity instanceof EntityPartialUpdateRequest) {
            return ((EntityPartialUpdateRequest) entity).getTypeName();
        } else if (entity instanceof EntityDeleteRequest) {
            return ((EntityDeleteRequest) entity).getTypeName();
        }
        return "unknown";
    }

    /**
     * Extracts entity routing information from different types of HookNotifications.
     * After V1 to V2 conversion, we mainly handle V2 types plus the V1 types that don't get converted.
     * NOTE: This method does NOT process temporary GUIDs - that happens later per topic group.
     */
    private List<EntityRoutingInfo> extractEntityRoutingInfos(HookNotification notification, NotificationMetadata notificationMetadata) {
        switch (notification.getType()) {
            // V1 types that don't get converted to V2 (single entity operations)
            case ENTITY_PARTIAL_UPDATE:
                return extractRoutingInfoFromPartialUpdateRequest((EntityPartialUpdateRequest) notification, notificationMetadata);
            case ENTITY_DELETE:
                return extractRoutingInfoFromDeleteRequest((EntityDeleteRequest) notification, notificationMetadata);
            // V2 types (including converted V1 bulk operations)
            case ENTITY_CREATE_V2:
                return extractRoutingInfoFromCreateRequestV2((EntityCreateRequestV2) notification, notificationMetadata);
            case ENTITY_PARTIAL_UPDATE_V2:
                return extractRoutingInfoFromPartialUpdateRequestV2((EntityPartialUpdateRequestV2) notification, notificationMetadata);
            case ENTITY_FULL_UPDATE_V2:
                return extractRoutingInfoFromUpdateRequestV2((EntityUpdateRequestV2) notification, notificationMetadata);
            case ENTITY_DELETE_V2:
                return extractRoutingInfoFromDeleteRequestV2((EntityDeleteRequestV2) notification, notificationMetadata);

            // These should not occur after conversion, but handle them defensively
            case ENTITY_CREATE:
            case ENTITY_FULL_UPDATE:
                LOG.warn("Unexpected V1 bulk notification type after conversion: {}", notification.getType());
                // Fall back to V1 processing if somehow we get here
                if (notification.getType() == HookNotification.HookNotificationType.ENTITY_CREATE) {
                    return extractRoutingInfoFromCreateRequest((EntityCreateRequest) notification, notificationMetadata);
                } else {
                    return extractRoutingInfoFromUpdateRequest((EntityUpdateRequest) notification, notificationMetadata);
                }

            default:
                LOG.warn("Unknown notification type: {}", notification.getType());
        }

        return Collections.emptyList();
    }

    /**
     * Extracts routing infos for entities from EntityCreateRequest (V1).
     * Note: This should rarely be called since V1 CREATE requests are converted to V2.
     * Temporary GUID processing is deferred until topic grouping.
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromCreateRequest(EntityCreateRequest request, NotificationMetadata notificationMetadata) {
        List<EntityRoutingInfo> routingInfos = new ArrayList<>();
        if (request.getEntities() != null) {
            for (Referenceable entity : request.getEntities()) {
                routingInfos.add(createEntityRoutingInfo(request, entity, notificationMetadata));
            }
        }
        return routingInfos;
    }

    /**
     * Extracts routing infos for entities from EntityUpdateRequest (V1).
     * Note: This should rarely be called since V1 UPDATE requests are converted to V2.
     * Temporary GUID processing is deferred until topic grouping.
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromUpdateRequest(EntityUpdateRequest request, NotificationMetadata notificationMetadata) {
        List<EntityRoutingInfo> routingInfos = new ArrayList<>();
        if (request.getEntities() != null) {
            for (Referenceable entity : request.getEntities()) {
                routingInfos.add(createEntityRoutingInfo(request, entity, notificationMetadata));
            }
        }
        return routingInfos;
    }

    /**
     * Extracts routing infos for entity from EntityPartialUpdateRequest (V1).
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromPartialUpdateRequest(EntityPartialUpdateRequest request, NotificationMetadata notificationMetadata) {
        // For partial updates, we only have type and attribute, create a routing key from typename
        //String routingKey  = request.getTypeName();
        String routingKey  = getRoutingKey(request.getEntity(), notificationMetadata);
        String targetTopic = metadataRouter.getTargetTopic(routingKey);
        return Collections.singletonList(new EntityRoutingInfo(request, routingKey, targetTopic));
    }

    /**
     * Extracts routing infos for entity from EntityDeleteRequest (V1).
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromDeleteRequest(EntityDeleteRequest request, NotificationMetadata notificationMetadata) {
        // For deletes, create routing key from typename and attribute value
        String routingKey  = request.getTypeName() + ":" + request.getAttributeValue();
        String targetTopic = metadataRouter.getTargetTopic(routingKey);
        return Collections.singletonList(new EntityRoutingInfo(request, routingKey, targetTopic));
    }

    /**
     * Extracts routing infos for entities from EntityCreateRequestV2.
     * Temporary GUID processing is deferred until topic grouping.
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromCreateRequestV2(EntityCreateRequestV2 request, NotificationMetadata notificationMetadata) {
        List<EntityRoutingInfo> routingInfos = new ArrayList<>();
        if (request.getEntities() == null) {
            return routingInfos;
        }
        if (request.getEntities().getEntities() != null) {
            for (AtlasEntity entity : request.getEntities().getEntities()) {
                routingInfos.add(createEntityRoutingInfo(request, entity, notificationMetadata));
            }
        }

        return routingInfos;
    }

    /**
     * Extracts routing infos for entities from EntityUpdateRequestV2.
     * Temporary GUID processing is deferred until topic grouping.
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromUpdateRequestV2(EntityUpdateRequestV2 request, NotificationMetadata notificationMetadata) {
        List<EntityRoutingInfo> routingInfos = new ArrayList<>();
        if (request.getEntities() == null) {
            return routingInfos;
        }

        if (request.getEntities().getEntities() != null) {
            for (AtlasEntity entity : request.getEntities().getEntities()) {
                routingInfos.add(createEntityRoutingInfo(request, entity, notificationMetadata));
            }
        }

        return routingInfos;
    }

    /**
     * Extracts routing infos for entity from EntityPartialUpdateRequestV2.
     * Detects and records rename events in ThreadLocal rename map.
     * Temporary GUID processing is deferred until topic grouping.
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromPartialUpdateRequestV2(EntityPartialUpdateRequestV2 request, NotificationMetadata notificationMetadata) {
        AtlasObjectId entityId = request.getEntityId();

        // Detect and record rename events
        if (entityId != null && request.getEntity() != null && request.getEntity().getEntity() != null) {
            String oldEntityName     = getRoutingQualifiedName(entityId);
            String renamedEntityName = getRoutingQualifiedName(request.getEntity().getEntity());

            // Validate that this is actually a rename (qualifiedName changed)
            if (oldEntityName != null && renamedEntityName != null && !oldEntityName.equals(renamedEntityName)) {
                long                           renamedEventTimestamp = notificationMetadata.getMsgCreationTime();
                Map<String, RenameRoutingInfo> renameMap             = renameRoutingInfoMap.get();

                // Record rename (with chain resolution)
                if (renameMap.containsKey(oldEntityName)) {
                    // Chain: tb1 → tb1_new → tb1_latest
                    renameMap.put(renamedEntityName, renameMap.get(oldEntityName));
                    LOG.debug("Detected rename chain: {} → {} (original: {}, timestamp: {})",
                            oldEntityName, renamedEntityName, renameMap.get(oldEntityName).getOriginalEntityName(), renamedEventTimestamp);
                } else {
                    // First rename: tb1 → tb1_new
                    renameMap.put(renamedEntityName, new RenameRoutingInfo(oldEntityName, renamedEventTimestamp));
                    LOG.debug("Detected rename: {} → {} (timestamp: {})", oldEntityName, renamedEntityName, renamedEventTimestamp);
                }
            }
        }

        // Extract entity for routing
        List<EntityRoutingInfo> routingInfos = new ArrayList<>();
        if (request.getEntity() != null && request.getEntity().getEntity() != null) {
            AtlasEntity entity = request.getEntity().getEntity();
            routingInfos.add(createEntityRoutingInfo(request, entity, notificationMetadata));
        }
        return routingInfos;
    }

    /**
     * Extracts routing infos for entities from EntityDeleteRequestV2.
     */
    private List<EntityRoutingInfo> extractRoutingInfoFromDeleteRequestV2(EntityDeleteRequestV2 request, NotificationMetadata notificationMetadata) {
        List<EntityRoutingInfo> routingInfos = new ArrayList<>();
        if (request.getEntities() != null) {
            for (AtlasObjectId entity : request.getEntities()) {
                String routingKey  = getRoutingKey(entity, notificationMetadata);
                String targetTopic = metadataRouter.getTargetTopic(routingKey);
                routingInfos.add(new EntityRoutingInfo(entity, routingKey, targetTopic));
            }
        }
        return routingInfos;
    }

    private EntityRoutingInfo createEntityRoutingInfo(HookNotification request, Object entity, NotificationMetadata notificationMetadata) {
        Set<String> referencedGUIDs = Collections.emptySet();
        // Extract referenced GUIDs BEFORE processing relationship attributes
        if (request instanceof EntityCreateRequest || request instanceof EntityUpdateRequest) {
            referencedGUIDs = extractReferencedGUIDsFromReferenceable((Referenceable) entity);
        } else if (request instanceof EntityCreateRequestV2 || request instanceof EntityUpdateRequestV2 || request instanceof EntityPartialUpdateRequestV2) {
            referencedGUIDs = extractReferencedGUIDsFromEntity((AtlasEntity) entity);
        }

        // Skip temporary GUID processing here - will be done per topic group later
        String routingKey  = getRoutingKey(entity, notificationMetadata);
        String targetTopic = metadataRouter.getTargetTopic(routingKey);

        return new EntityRoutingInfo(entity, routingKey, targetTopic, referencedGUIDs);
    }

    private String getRoutingQualifiedName(Object obj) {
        String qualifiedName = getQualifiedName(obj);
        if (StringUtils.isNotBlank(qualifiedName)) {
            // Skip timestamp or anything after metadata namespace (part after ':' if any)
            String qualifiedNameToRoute = qualifiedName;
            int    atIndex              = qualifiedName.indexOf(':');
            if (atIndex != -1) {
                qualifiedNameToRoute = qualifiedName.substring(0, atIndex);
            }
            return qualifiedNameToRoute;
        }
        return null;
    }

    /**
     * Processes relationship attributes for entities to handle negative GUIDs.
     * For V2 entities (AtlasEntity). Only strips temporary GUIDs if the referred entity is not present in the message.
     */
    private void processEntityRelationshipAttributes(AtlasEntity entity, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        if (entity == null) {
            return;
        }

        // Process ONLY relationship attributes - not regular attributes
        // Only replace negative GUIDs in relationshipAttributes if the referred entity is not present in the message
        if (entity.getRelationshipAttributes() != null) {
            for (Map.Entry<String, Object> entry : entity.getRelationshipAttributes().entrySet()) {
                Object value          = entry.getValue();
                Object processedValue = processAttributeValue(value, guidToUniqueAttrsMap, availableEntityGUIDs);
                if (processedValue != value) {
                    entry.setValue(processedValue);
                }
            }
        }

        // DO NOT process regular attributes - negative GUIDs in regular attributes are intentional
        // for newly created entities that haven't been assigned permanent GUIDs yet
    }

    /**
     * Recursively processes attribute values to handle negative GUIDs.
     * Only strips temporary GUIDs if isRelationshipAttribute is true AND the referred entity is not present in the message.
     */
    private Object processAttributeValue(Object value, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        if (value == null) {
            return null;
        }

        if (value instanceof AtlasObjectId) {
            return processAtlasObjectId((AtlasObjectId) value, guidToUniqueAttrsMap, availableEntityGUIDs);
        } else if (value instanceof Referenceable) {
            return processReferenceable((Referenceable) value, guidToUniqueAttrsMap, availableEntityGUIDs);
        } else if (value instanceof List) {
            return processListValue((List<?>) value, guidToUniqueAttrsMap, availableEntityGUIDs);
        } else if (value instanceof Map) {
            return processMapValue((Map<?, ?>) value, guidToUniqueAttrsMap, availableEntityGUIDs);
        }

        return value;
    }

    /**
     * Processes AtlasObjectId to handle negative GUIDs.
     * Only strips temporary GUIDs if isRelationshipAttribute is true AND the referred entity is not present in the message.
     */
    private Object processAtlasObjectId(AtlasObjectId objectId, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        String guid = objectId.getGuid();

        // Only process negative GUIDs if we're in relationship attributes
        if (guid != null && guid.startsWith("-")) {
            // NEW LOGIC: Check if the referred entity with this GUID exists in the current message
            if (availableEntityGUIDs.contains(guid)) {
                // Referred entity is present in the message - keep the temporary GUID as-is
                LOG.debug("Keeping temporary GUID {} as-is because referred entity is present in message for type {}",
                        guid, objectId.getTypeName());
                return objectId;
            }

            // Referred entity is NOT present - strip the GUID and replace with unique attributes
            // Check if uniqueAttributes are already provided
            if (objectId.getUniqueAttributes() != null && !objectId.getUniqueAttributes().isEmpty()) {
                // Remove the negative GUID and keep only uniqueAttributes
                AtlasObjectId newObjectId = new AtlasObjectId(objectId.getTypeName(), objectId.getUniqueAttributes());
                LOG.debug("Stripped temporary GUID {} and kept existing uniqueAttributes for type {} (referred entity not in message)",
                        guid, objectId.getTypeName());
                return newObjectId;
            } else {
                // Look up uniqueAttributes from the original message
                Map<String, Object> uniqueAttrs = guidToUniqueAttrsMap.get(guid);
                if (uniqueAttrs != null && !uniqueAttrs.isEmpty()) {
                    AtlasObjectId newObjectId = new AtlasObjectId(objectId.getTypeName(), uniqueAttrs);
                    LOG.debug("Replaced temporary GUID {} with uniqueAttributes {} for type {} (referred entity not in message)",
                            guid, uniqueAttrs, objectId.getTypeName());
                    return newObjectId;
                } else {
                    LOG.warn("Could not find uniqueAttributes for temporary GUID {} of type {} (referred entity not in message)",
                            guid, objectId.getTypeName());
                }
            }
        }

        // For regular attributes or non-negative GUIDs, return as-is
        return objectId;
    }

    /**
     * Processes Referenceable (V1) to handle negative GUIDs.
     * Only strips temporary GUIDs if isRelationshipAttribute is true AND the referred entity is not present in the message.
     */
    private Object processReferenceable(Referenceable referenceable, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        String guid = (String) referenceable.getValues().get(ATTRIBUTE_GUID);

        // Only process negative GUIDs if we're in relationship attributes
        if (guid != null && guid.startsWith("-")) {
            // NEW LOGIC: Check if the referred entity with this GUID exists in the current message
            if (availableEntityGUIDs.contains(guid)) {
                // Referred entity is present in the message - keep the temporary GUID as-is
                LOG.debug("Keeping temporary GUID {} as-is because referred entity is present in message for V1 type {}",
                        guid, referenceable.getTypeName());
                return referenceable;
            }

            // Referred entity is NOT present - strip the GUID and replace with unique attributes
            // Look up uniqueAttributes from the original message
            Map<String, Object> uniqueAttrs = guidToUniqueAttrsMap.get(guid);
            if (uniqueAttrs != null && !uniqueAttrs.isEmpty()) {
                // Create a new Referenceable without the negative GUID
                Referenceable newRef = new Referenceable(referenceable.getTypeName());
                // Copy all attributes except guid
                for (Map.Entry<String, Object> entry : referenceable.getValues().entrySet()) {
                    if (!ATTRIBUTE_GUID.equals(entry.getKey())) {
                        newRef.set(entry.getKey(), entry.getValue());
                    }
                }
                // Add uniqueAttributes
                for (Map.Entry<String, Object> entry : uniqueAttrs.entrySet()) {
                    newRef.set(entry.getKey(), entry.getValue());
                }
                LOG.debug("Replaced temporary GUID {} with uniqueAttributes for V1 entity type {} (referred entity not in message)",
                        guid, referenceable.getTypeName());
                return newRef;
            } else {
                LOG.warn("Could not find uniqueAttributes for temporary GUID {} of V1 entity type {} (referred entity not in message)",
                        guid, referenceable.getTypeName());
            }
        }

        // For regular attributes or non-negative GUIDs, return as-is
        return referenceable;
    }

    /**
     * Processes List values recursively.
     */
    private Object processListValue(List<?> list, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        List<Object> newList = new ArrayList<>();
        boolean      changed = false;

        for (Object item : list) {
            Object processedItem = processAttributeValue(item, guidToUniqueAttrsMap, availableEntityGUIDs);
            newList.add(processedItem);
            if (processedItem != item) {
                changed = true;
            }
        }

        return changed ? newList : list;
    }

    /**
     * Processes Map values recursively.
     * Also detects Map-based entity references (with guid, typeName, etc.) and processes them like AtlasObjectId.
     */
    private Object processMapValue(Map<?, ?> map, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        // Check if this Map represents an entity reference structure
        if (isEntityReferenceMap(map)) {
            return processMapAsEntityReference(map, guidToUniqueAttrsMap, availableEntityGUIDs);
        }

        // Regular Map processing - recursively process each value
        Map<Object, Object> newMap  = new HashMap<>();
        boolean             changed = false;

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object processedValue = processAttributeValue(entry.getValue(), guidToUniqueAttrsMap, availableEntityGUIDs);
            newMap.put(entry.getKey(), processedValue);
            if (processedValue != entry.getValue()) {
                changed = true;
            }
        }

        return changed ? newMap : map;
    }

    /**
     * Checks if a Map represents an entity reference (contains guid, typeName, etc.).
     */
    private boolean isEntityReferenceMap(Map<?, ?> map) {
        if (map == null) {
            return false;
        }

        // Check for common entity reference keys
        boolean hasGuid     = map.containsKey(ATTRIBUTE_GUID);
        boolean hasTypeName = map.containsKey("typeName");

        // A Map is considered an entity reference if it has both guid and typeName
        // (this matches the structure shown in the user's example)
        return hasGuid && hasTypeName;
    }

    /**
     * Processes a Map that represents an entity reference, similar to AtlasObjectId processing.
     * Only strips temporary GUIDs if the referred entity is not present in the message.
     */
    private Object processMapAsEntityReference(Map<?, ?> map, Map<String, Map<String, Object>> guidToUniqueAttrsMap, Set<String> availableEntityGUIDs) {
        Object guidObj = map.get(ATTRIBUTE_GUID);
        String guid    = guidObj != null ? guidObj.toString() : null;

        // Only process negative GUIDs if we're in relationship attributes
        if (guid != null && guid.startsWith("-")) {
            // NEW LOGIC: Check if the referred entity with this GUID exists in the current message
            if (availableEntityGUIDs.contains(guid)) {
                // Referred entity is present in the message - keep the temporary GUID as-is
                LOG.debug("Keeping temporary GUID {} as-is because referred entity is present in message for Map type {}",
                        guid, map.get("typeName"));
                return map;
            }

            // Referred entity is NOT present - strip the GUID and replace with unique attributes
            // Check if uniqueAttributes are already provided
            Object uniqueAttrsObj = map.get("uniqueAttributes");
            if (uniqueAttrsObj instanceof Map && !((Map<?, ?>) uniqueAttrsObj).isEmpty()) {
                // Remove the negative GUID and keep only uniqueAttributes
                Map<Object, Object> newMap = new HashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!ATTRIBUTE_GUID.equals(entry.getKey())) {
                        newMap.put(entry.getKey(), entry.getValue());
                    }
                }
                LOG.debug("Stripped temporary GUID {} and kept existing uniqueAttributes for Map type {} (referred entity not in message)",
                        guid, map.get("typeName"));
                return newMap;
            } else {
                // Look up uniqueAttributes from the original message
                Map<String, Object> uniqueAttrs = guidToUniqueAttrsMap.get(guid);
                if (uniqueAttrs != null && !uniqueAttrs.isEmpty()) {
                    Map<Object, Object> newMap = new HashMap<>();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!ATTRIBUTE_GUID.equals(entry.getKey())) {
                            newMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    // Add or replace uniqueAttributes
                    newMap.put("uniqueAttributes", uniqueAttrs);
                    LOG.debug("Replaced temporary GUID {} with uniqueAttributes {} for Map type {} (referred entity not in message)",
                            guid, uniqueAttrs, map.get("typeName"));
                    return newMap;
                } else {
                    LOG.warn("Could not find uniqueAttributes for temporary GUID {} of Map type {} (referred entity not in message)",
                            guid, map.get("typeName"));
                }
            }
        }

        // For regular attributes or non-negative GUIDs, return as-is
        return map;
    }

    /**
     * Gets all V2 entities from the original request.
     */
    private List<AtlasEntity> getAllV2EntitiesFromRequest(Object request) {
        List<AtlasEntity> allEntities = new ArrayList<>();

        if (request instanceof EntityCreateRequestV2) {
            EntityCreateRequestV2 createRequest = (EntityCreateRequestV2) request;
            if (createRequest.getEntities() != null) {
                if (createRequest.getEntities().getEntities() != null) {
                    allEntities.addAll(createRequest.getEntities().getEntities());
                }
                if (createRequest.getEntities().getReferredEntities() != null) {
                    allEntities.addAll(createRequest.getEntities().getReferredEntities().values());
                }
            }
        } else if (request instanceof EntityUpdateRequestV2) {
            EntityUpdateRequestV2 updateRequest = (EntityUpdateRequestV2) request;
            if (updateRequest.getEntities() != null) {
                if (updateRequest.getEntities().getEntities() != null) {
                    allEntities.addAll(updateRequest.getEntities().getEntities());
                }
                if (updateRequest.getEntities().getReferredEntities() != null) {
                    allEntities.addAll(updateRequest.getEntities().getReferredEntities().values());
                }
            }
        } else if (request instanceof EntityPartialUpdateRequestV2) {
            EntityPartialUpdateRequestV2 partialRequest = (EntityPartialUpdateRequestV2) request;
            if (partialRequest.getEntity() != null && partialRequest.getEntity().getEntity() != null) {
                allEntities.add(partialRequest.getEntity().getEntity());
                if (partialRequest.getEntity().getReferredEntities() != null) {
                    allEntities.addAll(partialRequest.getEntity().getReferredEntities().values());
                }
            }
        }

        return allEntities;
    }

    /**
     * Builds a map from GUID to uniqueAttributes for V2 entities.
     */
    private Map<String, Map<String, Object>> buildGuidToUniqueAttributesMapV2(List<AtlasEntity> entities) {
        Map<String, Map<String, Object>> guidToUniqueAttrsMap = new HashMap<>();

        for (AtlasEntity entity : entities) {
            String guid = entity.getGuid();
            if (guid != null && guid.startsWith("-") && entity.getAttributes() != null) {
                Map<String, Object> uniqueAttrs = new HashMap<>();
                // Common unique attributes
                Object qualifiedName = entity.getAttributes().get(ATTRIBUTE_QUALIFIED_NAME);
                Object name          = entity.getAttributes().get(ATTRIBUTE_NAME);

                if (qualifiedName != null) {
                    uniqueAttrs.put(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
                }
                if (name != null) {
                    uniqueAttrs.put(ATTRIBUTE_NAME, name);
                }

                if (!uniqueAttrs.isEmpty()) {
                    guidToUniqueAttrsMap.put(guid, uniqueAttrs);
                }
            }
        }

        return guidToUniqueAttrsMap;
    }

    /**
     * Extracts referenced GUIDs from an AtlasEntity's relationship attributes BEFORE temp GUID processing.
     * This preserves GUID information that would otherwise be lost during processEntityRelationshipAttributes.
     */
    private Set<String> extractReferencedGUIDsFromEntity(AtlasEntity entity) {
        Set<String> referencedGUIDs = new HashSet<>();

        if (entity == null) {
            return referencedGUIDs;
        }

        // Extract from relationship attributes only (this is where referenced entities are)
        if (entity.getRelationshipAttributes() != null) {
            for (Object value : entity.getRelationshipAttributes().values()) {
                extractReferencesFromRelationshipAttr(value, referencedGUIDs);
            }
        }

        LOG.debug("Extracted {} referenced GUIDs from entity {} relationship attributes",
                referencedGUIDs.size(), entity.getTypeName());

        return referencedGUIDs;
    }

    /**
     * Extracts referenced GUIDs from a V1 Referenceable entity's attributes BEFORE conversion.
     */
    private Set<String> extractReferencedGUIDsFromReferenceable(Referenceable entity) {
        Set<String> referencedGUIDs = new HashSet<>();

        if (entity != null) {
            extractReferencesFromRelationshipAttr(entity, referencedGUIDs);
        }

        return referencedGUIDs;
    }

    /**
     * Recursively extracts referenced GUIDs from any value type.
     */
    private void extractReferencesFromRelationshipAttr(Object value, Set<String> referencedGUIDs) {
        if (value == null) {
            return;
        }

        if (value instanceof AtlasObjectId) {
            AtlasObjectId objectId = (AtlasObjectId) value;
            if (objectId.getGuid() != null) {
                referencedGUIDs.add(objectId.getGuid());
            }
        } else if (value instanceof Referenceable) {
            Referenceable       ref    = (Referenceable) value;
            Map<String, Object> values = ref.getValues();

            if (values != null) {
                Object guid = values.get(ATTRIBUTE_GUID);
                if (guid != null) {
                    referencedGUIDs.add(guid.toString());
                }
                // Recurse through all attributes of the Referenceable
                for (Object attrValue : values.values()) {
                    extractReferencesFromRelationshipAttr(attrValue, referencedGUIDs);
                }
            }
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            for (Object item : list) {
                extractReferencesFromRelationshipAttr(item, referencedGUIDs);
            }
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            for (Object mapValue : map.values()) {
                extractReferencesFromRelationshipAttr(mapValue, referencedGUIDs);
            }
        } else if (value instanceof String) {
            String strValue = (String) value;
            if (isPotentialGuid(strValue)) {
                referencedGUIDs.add(strValue);
            }
        }
    }

    /**
     * Filters referredEntities to only include those referenced by the given entities.
     */
    private Map<String, AtlasEntity> filterReferredEntities(Map<String, AtlasEntity> allReferredEntities,
            Set<String> referencedGUIDs) {
        if (allReferredEntities == null || allReferredEntities.isEmpty() || referencedGUIDs == null || referencedGUIDs.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, AtlasEntity> filteredReferredEntities = new HashMap<>();
        Set<String>              processedGUIDs           = new HashSet<>();

        // First pass: directly referenced entities
        for (String referencedGUID : referencedGUIDs) {
            AtlasEntity referredEntity = allReferredEntities.get(referencedGUID);
            if (referredEntity != null) {
                filteredReferredEntities.put(referencedGUID, referredEntity);
                processedGUIDs.add(referencedGUID);
            }
        }

        LOG.debug("Filtered referredEntities from {} to {} based on references",
                allReferredEntities.size(), filteredReferredEntities.size());

        return filteredReferredEntities;
    }

    /**
     * Processes entities for a specific topic group by applying intelligent temporary GUID handling.
     * Only strips temporary GUIDs if the referred entity is not available in this topic group.
     */
    private List<AtlasEntity> processEntitiesForTopic(List<AtlasEntity> entities, Set<String> entityGuidsForTopic, HookNotification originalRequest) {
        List<AtlasEntity> processedEntities = new ArrayList<>();

        // Build unique attributes map for GUID resolution
        Map<String, Map<String, Object>> guidToUniqueAttrsMap = buildGuidToUniqueAttributesMapFromOriginal(originalRequest);

        for (AtlasEntity entity : entities) {
            // Create a deep copy to avoid modifying the original entity
            AtlasEntity processedEntity = createEntityCopy(entity);

            // Apply temporary GUID processing
            processEntityRelationshipAttributes(processedEntity, guidToUniqueAttrsMap, entityGuidsForTopic);

            processedEntities.add(processedEntity);
        }

        return processedEntities;
    }

    /**
     * Builds GUID-to-unique-attributes map from the original notification request.
     */
    private Map<String, Map<String, Object>> buildGuidToUniqueAttributesMapFromOriginal(HookNotification originalRequest) {
        List<AtlasEntity> allEntities = getAllV2EntitiesFromRequest(originalRequest);
        return buildGuidToUniqueAttributesMapV2(allEntities);
    }

    /**
     * Creates a deep copy of an AtlasEntity for safe modification.
     */
    private AtlasEntity createEntityCopy(AtlasEntity original) {
        // For simplicity, we'll create a new entity with the same data
        // In a production environment, you might want to use a proper deep cloning mechanism
        AtlasEntity copy = new AtlasEntity();
        copy.setGuid(original.getGuid());
        copy.setTypeName(original.getTypeName());
        copy.setStatus(original.getStatus());
        copy.setVersion(original.getVersion());
        copy.setCreatedBy(original.getCreatedBy());
        copy.setUpdatedBy(original.getUpdatedBy());
        copy.setCreateTime(original.getCreateTime());
        copy.setUpdateTime(original.getUpdateTime());

        // Deep copy attributes
        if (original.getAttributes() != null) {
            copy.setAttributes(new HashMap<>(original.getAttributes()));
        }

        // Deep copy relationship attributes
        if (original.getRelationshipAttributes() != null) {
            copy.setRelationshipAttributes(new HashMap<>(original.getRelationshipAttributes()));
        }

        // Copy other fields as needed
        if (original.getClassifications() != null) {
            copy.setClassifications(new ArrayList<>(original.getClassifications()));
        }

        return copy;
    }

    /**
     * Determines the routing key for an entity object.
     * Extracts qualifiedName, name, and typeName from the entity and delegates to the main routing logic.
     *
     * @param entity the entity object (AtlasEntity, AtlasObjectId, or Referenceable)
     * @param notificationMetadata metadata containing message creation time
     * @return routing key for topic assignment
     */
    private String getRoutingKey(Object entity, NotificationMetadata notificationMetadata) {
        String qualifiedName = getRoutingQualifiedName(entity);
        Object name          = null;
        String typeName      = null;
        String routingKey    = null;

        // Extract name and typeName based on entity type
        if (entity instanceof AtlasEntity) {
            name     = ((AtlasEntity) entity).getAttributes().get(ATTRIBUTE_NAME);
            typeName = ((AtlasEntity) entity).getTypeName();
        } else if (entity instanceof AtlasObjectId) {
            name     = ((AtlasObjectId) entity).getUniqueAttributes().get(ATTRIBUTE_NAME);
            typeName = ((AtlasObjectId) entity).getTypeName();
        } else if (entity instanceof Referenceable) {
            name     = ((Referenceable) entity).getValues().get(ATTRIBUTE_NAME);
            typeName = ((Referenceable) entity).getTypeName();
        }

        switch (typeName) {
            // Future: Add custom routing logic per entity type here
            // case "hive_table":
            // case "hive_process":
            default:
                // Use default logic for all entity types
                routingKey = getRoutingKey(qualifiedName, typeName, name, notificationMetadata);
        }
        return routingKey;
    }

    /**
     * Determines the routing key for an entity based on its qualifiedName, with rename resolution.
     *
     * @param qualifiedName the entity's qualifiedName
     * @param typeName the entity's type name
     * @param name the entity's name attribute
     * @param notificationMetadata metadata containing message creation time
     * @return routing key for topic assignment
     */
    private String getRoutingKey(String qualifiedName, String typeName, Object name,
            NotificationMetadata notificationMetadata) {
        if (StringUtils.isNotBlank(qualifiedName)) {
            // Resolve through rename chain using ThreadLocal map
            Map<String, RenameRoutingInfo> renameMap = renameRoutingInfoMap.get();

            if (renameMap.containsKey(qualifiedName)) {
                RenameRoutingInfo routingInfo = renameMap.get(qualifiedName);

                if (notificationMetadata.getMsgCreationTime() >= routingInfo.getRenamedEventTimestamp()) {
                    String originalName = routingInfo.getOriginalEntityName();
                    LOG.debug("Routing renamed entity '{}' using original name '{}' (msgTime={}, renameTime={})",
                            qualifiedName, originalName, notificationMetadata.getMsgCreationTime(), routingInfo.getRenamedEventTimestamp());
                    qualifiedName = originalName;
                } else {
                    LOG.debug("Message for '{}' predates rename (msgTime={}, renameTime={}), using current name",
                            qualifiedName, notificationMetadata.getMsgCreationTime(), routingInfo.getRenamedEventTimestamp());
                }
            }

            int    namespaceIndex = qualifiedName.indexOf('@');
            String qNameWithoutNameSpace;
            if (namespaceIndex != -1) {
                qNameWithoutNameSpace = qualifiedName.substring(0, namespaceIndex);
            } else {
                qNameWithoutNameSpace = qualifiedName;
            }

            // Extract routing key from qualifiedName (first two parts separated by '.')
            String[] parts = qNameWithoutNameSpace.split("\\.");

            if (parts.length >= 2) {
                return parts[0] + "." + parts[1];
            } else if (parts.length == 1) {
                return parts[0];
            }

            // Fallback to original qualifiedName if empty after processing
            return StringUtils.isNotBlank(qNameWithoutNameSpace) ? qNameWithoutNameSpace : qualifiedName;
        }

        // Fallback to name or typeName if qualifiedName is not available
        return name != null ? name.toString() : typeName;
    }

    /**
     * Creates a new grouped notification containing only entities for a specific topic.
     */
    private HookNotification createGroupedNotification(HookNotification originalNotification, List<EntityRoutingInfo> entitiesForTopic) {
        // For simplicity, we'll create a new notification with the same type but filtered entities
        // This is a simplified approach - in practice, you might want more sophisticated grouping logic

        switch (originalNotification.getType()) {
            case ENTITY_CREATE:
            case ENTITY_FULL_UPDATE:
                return createGroupedEntityCreateOrUpdateRequest(originalNotification, entitiesForTopic);
            case ENTITY_CREATE_V2:
            case ENTITY_FULL_UPDATE_V2:
                return createGroupedEntityCreateOrUpdateRequestV2(originalNotification, entitiesForTopic);
            case ENTITY_PARTIAL_UPDATE_V2:
                // For partial updates, we might still need to filter referredEntities
                return createGroupedEntityPartialUpdateRequestV2((EntityPartialUpdateRequestV2) originalNotification, entitiesForTopic);
            case ENTITY_PARTIAL_UPDATE:
            case ENTITY_DELETE:
            case ENTITY_DELETE_V2:
                // For single-entity operations, return the original notification
                // since there should only be one entity per topic anyway
                return originalNotification;
            default:
                LOG.warn("Cannot create grouped notification for type: {}", originalNotification.getType());
                return originalNotification;
        }
    }

    /**
     * Creates grouped EntityCreateRequest or EntityUpdateRequest.
     */
    private HookNotification createGroupedEntityCreateOrUpdateRequest(HookNotification originalNotification, List<EntityRoutingInfo> entitiesForTopic) {
        String user = originalNotification.getUser();
        List<Referenceable> filteredEntities = entitiesForTopic.stream()
                .filter(info -> info.getEntity() instanceof Referenceable)
                .map(info -> (Referenceable) info.getEntity())
                .collect(Collectors.toList());

        return originalNotification instanceof EntityCreateRequest ? new EntityCreateRequest(user, filteredEntities) : new EntityUpdateRequest(user, filteredEntities);
    }

    /**
     * Creates grouped EntityCreateRequestV2 and EntityUpdateRequestV2.
     * Filters referred entities and applies temporary GUID processing based on topic group availability.
     */
    private HookNotification createGroupedEntityCreateOrUpdateRequestV2(HookNotification originalV2Request, List<EntityRoutingInfo> entitiesForTopic) {
        AtlasEntity.AtlasEntitiesWithExtInfo originalEntities = null;
        String                               user             = originalV2Request.getUser();

        if (originalV2Request instanceof EntityCreateRequestV2) {
            EntityCreateRequestV2 original = (EntityCreateRequestV2) originalV2Request;
            originalEntities = original.getEntities();
        } else if (originalV2Request instanceof EntityUpdateRequestV2) {
            EntityUpdateRequestV2 original = (EntityUpdateRequestV2) originalV2Request;
            originalEntities = original.getEntities();
        }

        if (originalEntities == null) {
            LOG.debug("Original V2 entities are null. Returning original request.");
            return originalV2Request;
        }

        Set<String> originalGUIDs = Collections.emptySet();
        if (originalEntities != null && originalEntities.getEntities() != null) {
            originalGUIDs = originalEntities.getEntities().stream().map(AtlasEntity::getGuid).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        }

        List<AtlasEntity> mainEntities       = new ArrayList<>();
        Set<String>       allReferencedGUIDs = new HashSet<>();

        // Filter main entities and aggregate referenced GUIDs for dependency filtering
        for (EntityRoutingInfo info : entitiesForTopic) {
            if (info.getEntity() instanceof AtlasEntity) {
                AtlasEntity entity = (AtlasEntity) info.getEntity();

                // Efficient check to ensure this entity was a "main" entity in the original request
                if (originalGUIDs.contains(entity.getGuid())) {
                    mainEntities.add(entity);
                    allReferencedGUIDs.addAll(info.getReferencedGUIDs());
                }
            }
        }

        // Filter referred entities to only include those referenced by entities in this topic
        Map<String, AtlasEntity> originalReferredEntities = originalEntities.getReferredEntities();
        Map<String, AtlasEntity> filteredReferredEntities = filterReferredEntities(originalReferredEntities, allReferencedGUIDs);

        // Create set of available entity GUIDs for this specific topic group
        Set<String> entityGuidsForTopic = new HashSet<>();

        // Add main entity GUIDs
        for (AtlasEntity entity : mainEntities) {
            if (entity.getGuid() != null) {
                entityGuidsForTopic.add(entity.getGuid());
            }
        }

        // Add referred entity GUIDs
        entityGuidsForTopic.addAll(filteredReferredEntities.keySet());

        // Process temporary GUIDs (strip if referred entity is not in this Kafka topic)
        List<AtlasEntity> processedMainEntities = processEntitiesForTopic(mainEntities, entityGuidsForTopic, originalV2Request);
        // Referred entities are kept as-is (filtering already happened)
        Map<String, AtlasEntity> processedReferredEntities = filteredReferredEntities;

        LOG.debug("Created grouped {} : {} main entities, {} referred entities (filtered from {}) with {} available GUIDs for topic group",
                originalV2Request instanceof EntityCreateRequestV2 ? "EntityCreateRequestV2" : "EntityUpdateRequestV2", processedMainEntities.size(), processedReferredEntities.size(),
                originalReferredEntities != null ? originalReferredEntities.size() : 0, entityGuidsForTopic.size());

        AtlasEntity.AtlasEntitiesWithExtInfo createOrUpdateRequestWithGroupedData = new AtlasEntity.AtlasEntitiesWithExtInfo(processedMainEntities, new AtlasEntity.AtlasEntityExtInfo(processedReferredEntities));

        return originalV2Request instanceof EntityCreateRequestV2 ? new EntityCreateRequestV2(user, createOrUpdateRequestWithGroupedData) : new EntityUpdateRequestV2(user, createOrUpdateRequestWithGroupedData);
    }

    /**
     * Creates grouped EntityPartialUpdateRequestV2.
     * Filters referred entities and applies intelligent temporary GUID processing based on topic group availability.
     */
    private EntityPartialUpdateRequestV2 createGroupedEntityPartialUpdateRequestV2(EntityPartialUpdateRequestV2 original, List<EntityRoutingInfo> entitiesForTopic) {
        if (original.getEntity() == null || original.getEntity().getEntity() == null) {
            return original;
        }

        // Collect referenced GUIDs from the entity being updated
        Set<String> allReferencedGUIDs = new HashSet<>();
        for (EntityRoutingInfo info : entitiesForTopic) {
            allReferencedGUIDs.addAll(info.getReferencedGUIDs());
        }

        // Filter referred entities to only include those referenced by the entity being updated
        Map<String, AtlasEntity> originalReferredEntities = original.getEntity().getReferredEntities();
        Map<String, AtlasEntity> filteredReferredEntities = filterReferredEntities(originalReferredEntities, allReferencedGUIDs);

        // Create set of available entity GUIDs for this specific topic group
        Set<String> topicAvailableGUIDs = new HashSet<>();

        // Add main entity GUID
        AtlasEntity mainEntity = original.getEntity().getEntity();
        if (mainEntity.getGuid() != null) {
            topicAvailableGUIDs.add(mainEntity.getGuid());
        }

        // Add referred entity GUIDs
        topicAvailableGUIDs.addAll(filteredReferredEntities.keySet());

        // Apply intelligent temporary GUID processing
        AtlasEntity                      processedMainEntity  = createEntityCopy(mainEntity);
        Map<String, Map<String, Object>> guidToUniqueAttrsMap = buildGuidToUniqueAttributesMapFromOriginal(original);
        processEntityRelationshipAttributes(processedMainEntity, guidToUniqueAttrsMap, topicAvailableGUIDs);
        Map<String, AtlasEntity> processedReferredEntities = filteredReferredEntities;

        LOG.debug("Created grouped EntityPartialUpdateRequestV2: {} referred entities (filtered from {}) with {} available GUIDs for topic group",
                processedReferredEntities.size(),
                originalReferredEntities != null ? originalReferredEntities.size() : 0,
                topicAvailableGUIDs.size());

        // Create new AtlasEntityWithExtInfo with processed entities
        AtlasEntity.AtlasEntityWithExtInfo newEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(
                processedMainEntity,
                new AtlasEntity.AtlasEntityExtInfo(processedReferredEntities));

        // Original entityId should be passed as it'll have uniqueAttributes reference of table which is renamed
        return new EntityPartialUpdateRequestV2(original.getUser(), original.getEntityId(), newEntityWithExtInfo);
    }

    /**
     * Processes a parsed notification with original metadata context and routes entities to appropriate topics.
     * First converts V1 notifications to V2 format, then separates lineage entities from metadata entities,
     * groups each by routing key, and sends lineage entities to ATLAS_LINEAGE topics and metadata entities to ATLAS_METADATA topics.
     *
     * @param notification The notification to process
     * @param notificationMetadata The original metadata (source, timestamp)
     */
    private void routeNotification(HookNotification notification, NotificationMetadata notificationMetadata) {
        try {
            // First, convert V1 notifications to V2 format for unified processing
            HookNotification v2Notification = convertV1ToV2(notification);

            // Extract all entities from the notification
            List<EntityRoutingInfo> entityRoutingInfos = extractEntityRoutingInfos(v2Notification, notificationMetadata);

            if (entityRoutingInfos.isEmpty()) {
                LOG.debug("No entities found in notification, skipping routing");
                return;
            }

            // Separate lineage entities from metadata entities
            List<EntityRoutingInfo> lineageEntities  = new ArrayList<>();
            List<EntityRoutingInfo> metadataEntities = new ArrayList<>();

            for (EntityRoutingInfo routingInfo : entityRoutingInfos) {
                if (lineageTopicEnabled && isLineageEntity(routingInfo.getEntity())) {
                    // Re-route lineage entities using lineage router
                    String lineageRoutingKey  = routingInfo.getRoutingKey();
                    String lineageTargetTopic = lineageRouter.getTargetTopic(lineageRoutingKey);
                    lineageEntities.add(new EntityRoutingInfo(routingInfo.getEntity(), lineageRoutingKey, lineageTargetTopic, routingInfo.getReferencedGUIDs()));
                } else {
                    metadataEntities.add(routingInfo);
                }
            }

            LOG.debug("Notification contains {} total entities: {} lineage, {} metadata from source: {}",
                    entityRoutingInfos.size(), lineageEntities.size(), metadataEntities.size(), notificationMetadata.getSource().getSource());

            // Process lineage entities with original notification's metadata
            if (!lineageEntities.isEmpty()) {
                routeEntitiesByTopic(v2Notification, lineageEntities, "lineage", notificationMetadata);
            }

            // Process metadata entities with original notification's metadata
            if (!metadataEntities.isEmpty()) {
                routeEntitiesByTopic(v2Notification, metadataEntities, "metadata", notificationMetadata);
            }
        } catch (Exception e) {
            LOG.error("Error processing notification: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process notification", e);
        }
    }

    /**
     * Processes entities by grouping them by target topic and sending separate notifications with original metadata.
     */
    private void routeEntitiesByTopic(HookNotification originalNotification,
            List<EntityRoutingInfo> entitiesWithRoutingInfo,
            String entityType,
            NotificationMetadata notificationMetadata) {
        // Group entities by their target topics
        Map<String, List<EntityRoutingInfo>> entitiesGroupedByTopic = entitiesWithRoutingInfo.stream()
                .collect(Collectors.groupingBy(EntityRoutingInfo::getTargetTopic));

        LOG.debug("Processing {} {} entities grouped into {} topics with original source: {}",
                entitiesWithRoutingInfo.size(), entityType, entitiesGroupedByTopic.size(), notificationMetadata.getSource().getSource());

        // Create and send separate notifications for each topic group
        for (Map.Entry<String, List<EntityRoutingInfo>> entry : entitiesGroupedByTopic.entrySet()) {
            String                  targetTopic      = entry.getKey();
            List<EntityRoutingInfo> entitiesForTopic = entry.getValue();

            // Create a new notification containing only entities for this topic
            HookNotification groupedNotification = createGroupedNotification(originalNotification, entitiesForTopic);

            if (groupedNotification != null) {
                sendToTopic(targetTopic, groupedNotification, notificationMetadata);
                LOG.debug("Sent {} {} entities to topic {} with original source: {}",
                        entitiesForTopic.size(), entityType, targetTopic, notificationMetadata.getSource().getSource());
            }
        }
    }

    /**
     * Send notification to topic with original source - preserves the original message source context.
     */
    private void sendToTopic(String topic, HookNotification notification, NotificationMetadata notificationMetadata) {
        try {
            notificationInterface.send(topic, Collections.singletonList(notification), notificationMetadata.getSource(), notificationMetadata.getMsgCreationTime());
            LOG.debug("Successfully sent notification to topic {} with original source: {}", topic, notificationMetadata.getSource().getSource());
        } catch (NotificationException e) {
            LOG.error("Failed to send notification to topic {} with original source {}: {}",
                    topic, notificationMetadata.getSource().getSource(), e.getMessage());
            throw new RuntimeException("Failed to send notification to topic " + topic +
                    " with original source " + notificationMetadata.getSource().getSource(), e);
        }
    }

    private boolean isPotentialGuid(String value) {
        return value != null && (value.startsWith("-") || GUID_PATTERN.matcher(value).matches());
    }

    private void recordFailedMessages(String topic, List<String> failedMessages) {
        //logging failed messages
        for (String message : failedMessages) {
            failedMessageLog.error("[{}-DROPPED_NOTIFICATION] {}", topic, message);
        }

        failedMessages.clear();
    }

    /**
     * Enhanced entity routing information that includes referenced GUIDs from relationship attributes.
     * The referenced GUIDs are collected BEFORE temp GUID resolution to preserve filtering context.
     */
    private static class EntityRoutingInfo {
        private final Object      entity;
        private final String      routingKey;
        private final String      targetTopic;
        private final Set<String> referencedGUIDs;

        public EntityRoutingInfo(Object entity, String routingKey, String targetTopic) {
            this(entity, routingKey, targetTopic, new HashSet<>());
        }

        public EntityRoutingInfo(Object entity, String routingKey, String targetTopic, Set<String> referencedGUIDs) {
            this.entity          = entity;
            this.routingKey      = routingKey;
            this.targetTopic     = targetTopic;
            this.referencedGUIDs = referencedGUIDs != null ? referencedGUIDs : new HashSet<>();
        }

        public Object getEntity() {
            return entity;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public String getTargetTopic() {
            return targetTopic;
        }

        public Set<String> getReferencedGUIDs() {
            return referencedGUIDs;
        }
    }

    private static class NotificationMetadata {
        private MessageSource source;
        private long          msgCreationTime;

        public NotificationMetadata() {
        }

        public MessageSource getSource() {
            return source;
        }

        public void setSource(MessageSource source) {
            this.source = source;
        }

        public long getMsgCreationTime() {
            return msgCreationTime;
        }

        public void setMsgCreationTime(long msgCreationTime) {
            this.msgCreationTime = msgCreationTime;
        }
    }

    private class RenameRoutingInfo {
        private String originalEntityName;
        private long   renamedEventTimestamp;

        public RenameRoutingInfo(String originalEntityName, long renamedEventTimestamp) {
            this.originalEntityName    = originalEntityName;
            this.renamedEventTimestamp = renamedEventTimestamp;
        }

        public String getOriginalEntityName() {
            return originalEntityName;
        }

        public void setOriginalEntityName(String originalEntityName) {
            this.originalEntityName = originalEntityName;
        }

        public long getRenamedEventTimestamp() {
            return renamedEventTimestamp;
        }

        public void setRenamedEventTimestamp(long renamedEventTimestamp) {
            this.renamedEventTimestamp = renamedEventTimestamp;
        }
    }

    /**
     * Handles type definition changes by refreshing the process types cache.
     * This ensures that the NotificationPreProcessor stays up-to-date with any
     * type changes that occur after Atlas startup.
     *
     * @param changedTypeDefs the changed type definitions
     * @throws AtlasBaseException if refresh fails
     */
    @Override
    public void onChange(ChangedTypeDefs changedTypeDefs) throws AtlasBaseException {
        if (changedTypeDefs == null) {
            return;
        }

        boolean needsRefresh = false;

        // Check if any entity types were added, updated, or deleted
        if (changedTypeDefs.getCreatedTypeDefs() != null && !changedTypeDefs.getCreatedTypeDefs().isEmpty()) {
            LOG.info("Type registry change detected: {} new type definitions created", changedTypeDefs.getCreatedTypeDefs().size());
            needsRefresh = true;
        }

        if (changedTypeDefs.getUpdatedTypeDefs() != null && !changedTypeDefs.getUpdatedTypeDefs().isEmpty()) {
            LOG.info("Type registry change detected: {} type definitions updated", changedTypeDefs.getUpdatedTypeDefs().size());
            needsRefresh = true;
        }

        if (changedTypeDefs.getDeletedTypeDefs() != null && !changedTypeDefs.getDeletedTypeDefs().isEmpty()) {
            LOG.info("Type registry change detected: {} type definitions deleted", changedTypeDefs.getDeletedTypeDefs().size());
            needsRefresh = true;
        }

        if (needsRefresh) {
            refreshTypeRegistry();
        }
    }

    /**
     * Handles type registry load completion.
     * This is called when the type registry finishes loading all type definitions.
     *
     * @throws AtlasBaseException if refresh fails
     */
    @Override
    public void onLoadCompletion() throws AtlasBaseException {
        LOG.info("Type registry load completion detected, refreshing process types cache");
        refreshTypeRegistry();
    }

    /**
     * Refreshes the process types cache with the latest type definitions from the registry.
     * This method is thread-safe and handles errors gracefully.
     */
    private void refreshTypeRegistry() {
        try {
            LOG.info("Refreshing type registry cache for NotificationPreProcessor");

            if (typeRegistry != null) {
                Set<String> newProcessTypes = typeRegistry.getAllEntityTypes().stream()
                        .filter(x -> x.getTypeName().equals(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS) || x.isSubTypeOf(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS))
                        .map(x -> x.getTypeName())
                        .collect(Collectors.toSet());

                // Create unmodifiable set for thread safety
                Set<String> oldProcessTypes = this.allProcessTypes;
                this.allProcessTypes = Collections.unmodifiableSet(newProcessTypes);

                // Clear the process type cache to force re-evaluation
                processTypeCache.clear();

                LOG.info("Successfully refreshed process types cache: {} types (was {} types)",
                        newProcessTypes.size(), oldProcessTypes != null ? oldProcessTypes.size() : 0);

                if (LOG.isDebugEnabled()) {
                    Set<String> addedTypes = new HashSet<>(newProcessTypes);
                    if (oldProcessTypes != null) {
                        addedTypes.removeAll(oldProcessTypes);
                    }

                    Set<String> removedTypes = new HashSet<>();
                    if (oldProcessTypes != null) {
                        removedTypes.addAll(oldProcessTypes);
                        removedTypes.removeAll(newProcessTypes);
                    }

                    if (!addedTypes.isEmpty()) {
                        LOG.debug("Added process types: {}", addedTypes);
                    }
                    if (!removedTypes.isEmpty()) {
                        LOG.debug("Removed process types: {}", removedTypes);
                    }
                }
            } else {
                LOG.warn("Type registry is null during refresh, keeping existing process types cache");
            }
        } catch (Exception e) {
            LOG.error("Failed to refresh type registry cache: {}. Process type detection may be outdated.", e.getMessage(), e);
            // Don't throw exception - continue with existing cache
        }
    }
}
