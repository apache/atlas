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
package org.apache.atlas.repository.store.graph.v2;

import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

import org.apache.atlas.*;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.*;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasRelationshipAttributeDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.observability.AtlasObservabilityData;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v1.RestoreHandlerV1;
import org.apache.atlas.repository.store.graph.v2.tags.PaginatedTagResult;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask;
import org.apache.atlas.repository.store.graph.v2.utils.TagAttributeMapper;
import org.apache.atlas.repository.util.TagDeNormAttributesUtil;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.*;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


import static org.apache.atlas.AtlasConfiguration.LABEL_MAX_LENGTH;
import static org.apache.atlas.AtlasConfiguration.STORE_DIFFERENTIAL_AUDITS;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.model.TypeCategory.CLASSIFICATION;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.instance.AtlasObjectId.KEY_TYPENAME;
import static org.apache.atlas.model.instance.AtlasRelatedObjectId.KEY_RELATIONSHIP_ATTRIBUTES;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.PARTIAL_UPDATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.IN_PROGRESS;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.SET;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEdge;
import static org.apache.atlas.repository.graph.GraphHelper.getCollectionElementsUsingRelationship;
import static org.apache.atlas.repository.graph.GraphHelper.getCreatedByAsString;
import static org.apache.atlas.repository.graph.GraphHelper.getCreatedTime;
import static org.apache.atlas.repository.graph.GraphHelper.getDelimitedClassificationNames;
import static org.apache.atlas.repository.graph.GraphHelper.getLabels;
import static org.apache.atlas.repository.graph.GraphHelper.getMapElementsProperty;
import static org.apache.atlas.repository.graph.GraphHelper.getModifiedByAsString;
import static org.apache.atlas.repository.graph.GraphHelper.getModifiedTime;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitLabel;
import static org.apache.atlas.repository.graph.GraphHelper.handleGetTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.graph.GraphHelper.isRelationshipEdge;
import static org.apache.atlas.repository.graph.GraphHelper.updateModificationMetadata;
import static org.apache.atlas.repository.graph.GraphHelper.getEntityHasLineage;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatedEdges;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEntityGuid;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.*;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_ADD;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_DELETE;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_NOOP;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_UPDATE;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_ENTITY_GUID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_SOURCE_VERTEX_ID;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;
import static org.apache.atlas.type.Constants.PENDING_TASKS_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.CATEGORIES_PARENT_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.CATEGORIES_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.GLOSSARY_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;
import static org.apache.atlas.type.Constants.MEANINGS_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANINGS_TEXT_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANING_NAMES_PROPERTY_KEY;


@Component
public class EntityGraphMapper {
    private static final Logger LOG      = LoggerFactory.getLogger(EntityGraphMapper.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("entityGraphMapper");

    private static final String  SOFT_REF_FORMAT                   = "%s:%s";
    private static final int     INDEXED_STR_SAFE_LEN              = AtlasConfiguration.GRAPHSTORE_INDEXED_STRING_SAFE_LENGTH.getInt();
    private static final boolean WARN_ON_NO_RELATIONSHIP           = AtlasConfiguration.RELATIONSHIP_WARN_NO_RELATIONSHIPS.getBoolean();
    private static final String  CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX = AtlasConfiguration.CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX.getString();

    private static final String  CLASSIFICATION_NAME_DELIMITER     = "|";
    private static final Pattern CUSTOM_ATTRIBUTE_KEY_REGEX        = Pattern.compile("^[a-zA-Z0-9_-]*$");
    private static final Pattern LABEL_REGEX                       = Pattern.compile("^[a-zA-Z0-9_-]*$");
    private static final int     CUSTOM_ATTRIBUTE_KEY_MAX_LENGTH   = AtlasConfiguration.CUSTOM_ATTRIBUTE_KEY_MAX_LENGTH.getInt();
    private static final int     CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH = AtlasConfiguration.CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH.getInt();

    private static final String TYPE_GLOSSARY= "AtlasGlossary";
    private static final String TYPE_CATEGORY= "AtlasGlossaryCategory";
    private static final String TYPE_TERM = "AtlasGlossaryTerm";
    private static final String TYPE_PRODUCT = "DataProduct";
    private static final String TYPE_DOMAIN = "DataDomain";
    private static final String TYPE_PROCESS = "Process";
    private static final String ATTR_MEANINGS = "meanings";
    private static final String ATTR_ANCHOR = "anchor";
    private static final String ATTR_CATEGORIES = "categories";
    private static final int ELASTICSEARCH_KEYWORD_MAX_BYTES = 32766;
    private static final String UTF8_CHARSET = "UTF-8";
    private static final List<String> ALLOWED_DATATYPES_FOR_DEFAULT_NULL = new ArrayList() {
        {
            add("int");
            add("long");
            add("float");
        }
    };

    private static final boolean ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES = AtlasConfiguration.ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES.getBoolean();
    private static final boolean CLASSIFICATION_PROPAGATION_DEFAULT                  = AtlasConfiguration.CLASSIFICATION_PROPAGATION_DEFAULT.getBoolean();
    private static final boolean RESTRICT_PROPAGATION_THROUGH_LINEAGE_DEFAULT        = false;

    private static final boolean RESTRICT_PROPAGATION_THROUGH_HIERARCHY_DEFAULT        = false;
    public static final int CLEANUP_BATCH_SIZE = 200000;
    public static final String GUID = "__guid";
    private              boolean DEFERRED_ACTION_ENABLED                             = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();
    private              boolean DIFFERENTIAL_AUDITS                                 = STORE_DIFFERENTIAL_AUDITS.getBoolean();

    private static final int MAX_NUMBER_OF_RETRIES = AtlasConfiguration.MAX_NUMBER_OF_RETRIES.getInt();
    private static final int CHUNK_SIZE            = AtlasConfiguration.TAG_CASSANDRA_BATCHING_CHUNK_SIZE.getInt();
    private static final int UD_REL_THRESHOLD = AtlasConfiguration.ATLAS_UD_RELATIONSHIPS_MAX_COUNT.getInt();

    private final GraphHelper               graphHelper;
    private final AtlasGraph                graph;
    private final DeleteHandlerDelegate     deleteDelegate;
    private final RestoreHandlerV1          restoreHandlerV1;
    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasRelationshipStore    relationshipStore;
    private final IAtlasEntityChangeNotifier entityChangeNotifier;
    private final AtlasInstanceConverter    instanceConverter;
    private final EntityGraphRetriever      entityRetriever;
    private final IFullTextMapper           fullTextMapperV2;
    private final TaskManagement            taskManagement;
    private final TransactionInterceptHelper   transactionInterceptHelper;
    private final EntityGraphRetriever       retrieverNoRelation;
    private final TagDAO                    tagDAO;
    private final TagAttributeMapper        tagAttributeMapper;
    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(TYPE_GLOSSARY, TYPE_CATEGORY, TYPE_TERM, TYPE_PRODUCT, TYPE_DOMAIN));

    @Inject
    public EntityGraphMapper(DeleteHandlerDelegate deleteDelegate, RestoreHandlerV1 restoreHandlerV1, AtlasTypeRegistry typeRegistry, AtlasGraph graph,
                             AtlasRelationshipStore relationshipStore, IAtlasEntityChangeNotifier entityChangeNotifier,
                             AtlasInstanceConverter instanceConverter, IFullTextMapper fullTextMapperV2,
                             TaskManagement taskManagement, TransactionInterceptHelper transactionInterceptHelper,
                             EntityGraphRetriever entityRetriever, TagAttributeMapper tagAttributeMapper) {
        this.restoreHandlerV1 = restoreHandlerV1;
        this.graphHelper          = new GraphHelper(graph);
        this.deleteDelegate       = deleteDelegate;
        this.typeRegistry         = typeRegistry;
        this.graph                = graph;
        this.relationshipStore    = relationshipStore;
        this.entityChangeNotifier = entityChangeNotifier;
        this.instanceConverter    = instanceConverter;
        this.entityRetriever      = entityRetriever;
        this.retrieverNoRelation  = new EntityGraphRetriever(entityRetriever, true);
        this.fullTextMapperV2     = fullTextMapperV2;
        this.taskManagement       = taskManagement;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.tagDAO = TagDAOCassandraImpl.getInstance();
        this.tagAttributeMapper = tagAttributeMapper;
    }

    @VisibleForTesting
    public void setTasksUseFlag(boolean value) {
        DEFERRED_ACTION_ENABLED = value;
    }

    public AtlasVertex createVertex(AtlasEntity entity) throws AtlasBaseException {
        final String guid = UUID.randomUUID().toString();
        return createVertexWithGuid(entity, guid);
    }

    public AtlasVertex createShellEntityVertex(AtlasObjectId objectId, EntityGraphDiscoveryContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createShellEntityVertex({})", objectId.getTypeName());
        }

        final String    guid       = UUID.randomUUID().toString();
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(objectId.getTypeName());
        AtlasVertex     ret        = createStructVertex(objectId);

        for (String superTypeName : entityType.getAllSuperTypes()) {
            AtlasGraphUtilsV2.addEncodedProperty(ret, SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        AtlasGraphUtilsV2.setEncodedProperty(ret, GUID_PROPERTY_KEY, guid);
        AtlasGraphUtilsV2.setEncodedProperty(ret, VERSION_PROPERTY_KEY, getEntityVersion(null));
        AtlasGraphUtilsV2.setEncodedProperty(ret, IS_INCOMPLETE_PROPERTY_KEY, INCOMPLETE_ENTITY_VALUE);

        // map unique attributes
        Map<String, Object>   uniqueAttributes = objectId.getUniqueAttributes();
        EntityMutationContext mutationContext  = new EntityMutationContext(context);

        for (AtlasAttribute attribute : entityType.getUniqAttributes().values()) {
            String attrName  = attribute.getName();

            if (uniqueAttributes.containsKey(attrName)) {
                Object attrValue = attribute.getAttributeType().getNormalizedValue(uniqueAttributes.get(attrName));

                mapAttribute(attribute, attrValue, ret, CREATE, mutationContext);
            }
        }

        GraphTransactionInterceptor.addToVertexCache(guid, ret);

        return ret;
    }

    public AtlasVertex createVertexWithGuid(AtlasEntity entity, String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createVertexWithGuid({})", entity.getTypeName());
        }

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasVertex     ret        = createStructVertex(entity);

        for (String superTypeName : entityType.getAllSuperTypes()) {
            AtlasGraphUtilsV2.addEncodedProperty(ret, SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        AtlasGraphUtilsV2.setEncodedProperty(ret, GUID_PROPERTY_KEY, guid);
        AtlasGraphUtilsV2.setEncodedProperty(ret, VERSION_PROPERTY_KEY, getEntityVersion(entity));

        setCustomAttributes(ret, entity);

        if (CollectionUtils.isNotEmpty(entity.getLabels())) {
            setLabels(ret, entity.getLabels());
        }

        GraphTransactionInterceptor.addToVertexCache(guid, ret);

        return ret;
    }

    public void updateSystemAttributes(AtlasVertex vertex, AtlasEntity entity) throws AtlasBaseException {
        if (entity.getVersion() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, VERSION_PROPERTY_KEY, entity.getVersion());
        }

        if (entity.getCreateTime() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, TIMESTAMP_PROPERTY_KEY, entity.getCreateTime().getTime());
        }

        if (entity.getUpdateTime() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, entity.getUpdateTime().getTime());
        }

        if (StringUtils.isNotEmpty(entity.getCreatedBy())) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, CREATED_BY_KEY, entity.getCreatedBy());
        }

        if (StringUtils.isNotEmpty(entity.getUpdatedBy())) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFIED_BY_KEY, entity.getUpdatedBy());
        }

        if (StringUtils.isNotEmpty(entity.getHomeId())) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, HOME_ID_KEY, entity.getHomeId());
        }

        if (entity.isProxy() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, IS_PROXY_KEY, entity.isProxy());
        }

        if (entity.getProvenanceType() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, PROVENANCE_TYPE_KEY, entity.getProvenanceType());
        }

        if (entity.getCustomAttributes() != null) {
            setCustomAttributes(vertex, entity);
        }

        if (entity.getLabels() != null) {
            setLabels(vertex, entity.getLabels());
        }
    }

    public EntityMutationResponse mapAttributesAndClassifications(EntityMutationContext context,
                                                                  final boolean isPartialUpdate,
                                                                  BulkRequestContext bulkRequestContext, AtlasObservabilityData observabilityData) throws AtlasBaseException {

        MetricRecorder metric = RequestContext.get().startMetricRecord("mapAttributesAndClassifications");

        EntityMutationResponse resp = new EntityMutationResponse();
        RequestContext reqContext = RequestContext.get();

        boolean distributedHasLineageCalculationEnabled = AtlasConfiguration.ATLAS_DISTRIBUTED_TASK_ENABLED.getBoolean()
                && AtlasConfiguration.ENABLE_DISTRIBUTED_HAS_LINEAGE_CALCULATION.getBoolean();


        if (CollectionUtils.isNotEmpty(context.getEntitiesToRestore())) {
            restoreHandlerV1.restoreEntities(context.getEntitiesToRestore());
            for (AtlasEntityHeader restoredEntity : reqContext.getRestoredEntities()) {
                AtlasEntity diffEntity;
                if (reqContext.getDifferentialEntity(restoredEntity.getGuid()) != null){
                    diffEntity = reqContext.getDifferentialEntity(restoredEntity.getGuid());
                } else {
                    diffEntity = new AtlasEntity(restoredEntity.getTypeName());
                    diffEntity.setGuid(restoredEntity.getGuid());
                }
                diffEntity.setUpdatedBy(RequestContext.get().getUser());
                diffEntity.setUpdateTime(new Date(RequestContext.get().getRequestTime()));
                diffEntity.setAttribute(STATE_PROPERTY_KEY, ACTIVE.name());
                reqContext.cacheDifferentialEntity(diffEntity);

                resp.addEntity(UPDATE, restoredEntity);
            }
        }

        Collection<AtlasEntity> createdEntities = context.getCreatedEntities();
        Collection<AtlasEntity> updatedEntities = context.getUpdatedEntities();
        Collection<AtlasEntity> appendEntities = context.getUpdatedEntitiesForAppendRelationshipAttribute();
        Collection<AtlasEntity> removeEntities = context.getEntitiesUpdatedWithRemoveRelationshipAttribute();

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntity createdEntity : createdEntities) {
                try {
                    reqContext.getDeletedEdgesIds().clear();

                    String guid = createdEntity.getGuid();
                    AtlasVertex vertex = context.getVertex(guid);
                    AtlasEntityType entityType = context.getType(guid);

                    mapAttributes(createdEntity, entityType, vertex, CREATE, context);
                    mapRelationshipAttributes(createdEntity, entityType, vertex, CREATE, context);

                    setCustomAttributes(vertex, createdEntity);
                    setSystemAttributesToEntity(vertex, createdEntity);
                    resp.addEntity(CREATE, constructHeader(createdEntity, vertex, entityType));

                    if (bulkRequestContext.isAppendTags()) {
                        if (CollectionUtils.isNotEmpty(createdEntity.getAddOrUpdateClassifications())) {
                            createdEntity.setClassifications(createdEntity.getAddOrUpdateClassifications());
                            createdEntity.setAddOrUpdateClassifications(null);
                        }

                        if (CollectionUtils.isNotEmpty(createdEntity.getRemoveClassifications())) {
                            createdEntity.setRemoveClassifications(null);
                        }
                    }

                    handleAddClassifications(context, guid, createdEntity.getClassifications());

                    if (MapUtils.isNotEmpty(createdEntity.getBusinessAttributes())) {
                        addOrUpdateBusinessAttributes(vertex, entityType, createdEntity.getBusinessAttributes());
                    }

                    Set<AtlasEdge> inOutEdges = getNewCreatedInputOutputEdges(guid);

                    if (inOutEdges != null && inOutEdges.size() > 0) {
                        boolean isRestoreEntity = false;
                        if (CollectionUtils.isNotEmpty(context.getEntitiesToRestore())) {
                            isRestoreEntity = context.getEntitiesToRestore().contains(vertex);
                        }
                        addHasLineage(inOutEdges, isRestoreEntity);
                    }

                    Set<AtlasEdge> removedEdges = getRemovedInputOutputEdges(guid);

                    if (!distributedHasLineageCalculationEnabled && CollectionUtils.isNotEmpty(removedEdges)) {
                        deleteDelegate.getHandler().resetHasLineageOnInputOutputDelete(removedEdges, null);
                    }

                    reqContext.cache(createdEntity);

                    if (DEFERRED_ACTION_ENABLED) {
                        Set<String> deletedEdgeIds = reqContext.getDeletedEdgesIds();
                        for (String deletedEdgeId : deletedEdgeIds) {
                            AtlasEdge edge = graph.getEdge(deletedEdgeId);
                            deleteDelegate.getHandler().createAndQueueClassificationRefreshPropagationTask(edge);
                        }
                    }
                } catch (AtlasBaseException baseException) {
                    setEntityGuidToException(createdEntity, baseException, context);
                    throw baseException;
                }
            }
        }

        EntityOperation updateType = isPartialUpdate ? PARTIAL_UPDATE : UPDATE;

        if (CollectionUtils.isNotEmpty(updatedEntities)) {
            for (AtlasEntity updatedEntity : updatedEntities) {
                try {
                    reqContext.getDeletedEdgesIds().clear();

                    String guid = updatedEntity.getGuid();
                    AtlasVertex vertex = context.getVertex(guid);
                    AtlasEntityType entityType = context.getType(guid);

                    mapAttributes(updatedEntity, entityType, vertex, updateType, context);
                    mapRelationshipAttributes(updatedEntity, entityType, vertex, UPDATE, context);

                    setCustomAttributes(vertex, updatedEntity);

                    if (bulkRequestContext.isReplaceClassifications()) {
                        deleteClassifications(guid);
                        handleAddClassifications(context, guid, updatedEntity.getClassifications());

                    } else {
                        Map<String, List<AtlasClassification>> diff = RequestContext.get().getAndRemoveTagsDiff(guid);

                        if (MapUtils.isNotEmpty(diff)) {
                            List<AtlasClassification> finalTags = new ArrayList<>();
                            if (diff.containsKey(PROCESS_DELETE)) {
                                for (AtlasClassification tag : diff.get(PROCESS_DELETE)) {
                                    handleDirectDeleteClassification(updatedEntity.getGuid(), tag.getTypeName());
                                }
                            }

                            if (diff.containsKey(PROCESS_UPDATE)) {
                                finalTags.addAll(diff.get(PROCESS_UPDATE));
                                handleUpdateClassifications(context, updatedEntity.getGuid(), diff.get(PROCESS_UPDATE));
                            }

                            if (diff.containsKey(PROCESS_ADD)) {
                                finalTags.addAll(diff.get(PROCESS_ADD));
                                handleAddClassifications(context, updatedEntity.getGuid(), diff.get(PROCESS_ADD));
                            }

                            if (diff.containsKey(PROCESS_NOOP)) {
                                finalTags.addAll(diff.get(PROCESS_NOOP));
                            }

                            RequestContext.get().getDifferentialEntity(guid).setClassifications(finalTags);  // For notifications
                        }
                    }

                    if (bulkRequestContext.isReplaceBusinessAttributes()) {
                        if (MapUtils.isEmpty(updatedEntity.getBusinessAttributes()) && bulkRequestContext.isOverwriteBusinessAttributes()) {
                            Map<String, Map<String, Object>> businessMetadata = entityRetriever.getBusinessMetadata(vertex);
                            if (MapUtils.isNotEmpty(businessMetadata)) {
                                removeBusinessAttributes(vertex, entityType, businessMetadata);
                            }
                        } else {
                            addOrUpdateBusinessAttributes(guid, updatedEntity.getBusinessAttributes(), bulkRequestContext.isOverwriteBusinessAttributes());
                        }
                    }

                    setSystemAttributesToEntity(vertex, updatedEntity);
                    resp.addEntity(updateType, constructHeader(updatedEntity, vertex, entityType));

                    // Add hasLineage for newly created edges
                    Set<AtlasEdge> newlyCreatedEdges = getNewCreatedInputOutputEdges(guid);
                    if (newlyCreatedEdges.size() > 0) {
                        addHasLineage(newlyCreatedEdges, false);
                    }

                    // Add hasLineage for restored edges
                    if (CollectionUtils.isNotEmpty(context.getEntitiesToRestore()) && context.getEntitiesToRestore().contains(vertex)) {
                        Set<AtlasEdge> restoredInputOutputEdges = getRestoredInputOutputEdges(vertex);
                        addHasLineage(restoredInputOutputEdges, true);
                    }

                    Set<AtlasEdge> removedEdges = getRemovedInputOutputEdges(guid);

                    if (!distributedHasLineageCalculationEnabled && CollectionUtils.isNotEmpty(removedEdges)) {
                        deleteDelegate.getHandler().resetHasLineageOnInputOutputDelete(removedEdges, null);
                    }

                    reqContext.cache(updatedEntity);

                    if (DEFERRED_ACTION_ENABLED) {
                        Set<String> deletedEdgeIds = reqContext.getDeletedEdgesIds();
                        for (String deletedEdgeId : deletedEdgeIds) {
                            AtlasEdge edge = graph.getEdge(deletedEdgeId);
                            deleteDelegate.getHandler().createAndQueueClassificationRefreshPropagationTask(edge);
                        }
                    }

                } catch (AtlasBaseException baseException) {
                    setEntityGuidToException(updatedEntity, baseException, context);
                    throw baseException;
                }
            }
        }

        // If an entity is both appended and removed, remove it from both lists
        if (CollectionUtils.isNotEmpty(appendEntities) && CollectionUtils.isNotEmpty(removeEntities)) {
            Set<String> appendGuids = appendEntities.stream()
                .map(AtlasEntity::getGuid)
                .collect(Collectors.toSet());

            Set<String> removeGuids = removeEntities.stream()
                .map(AtlasEntity::getGuid)
                .collect(Collectors.toSet());

            Set<String> commonGuids = new HashSet<>(appendGuids);
            commonGuids.retainAll(removeGuids);

            if (!commonGuids.isEmpty()) {
                appendEntities.removeIf(entity -> commonGuids.contains(entity.getGuid()));
                removeEntities.removeIf(entity -> commonGuids.contains(entity.getGuid()));
            }
        }
        if (CollectionUtils.isNotEmpty(appendEntities)) {
            for (AtlasEntity entity : appendEntities) {
                String guid = entity.getGuid();
                AtlasVertex vertex = context.getVertex(guid);
                AtlasEntityType entityType = context.getType(guid);
                mapAppendRemoveRelationshipAttributes(entity, entityType, vertex, UPDATE, context, true, false);

                // Update __hasLineage for edges impacted during append operation
                Set<AtlasEdge> newlyCreatedEdges = getNewCreatedInputOutputEdges(guid);
                if (CollectionUtils.isNotEmpty(newlyCreatedEdges)) {
                    addHasLineage(newlyCreatedEdges, false);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(removeEntities)) {
            for (AtlasEntity entity : removeEntities) {
                String guid = entity.getGuid();
                AtlasVertex vertex = context.getVertex(guid);
                AtlasEntityType entityType = context.getType(guid);
                mapAppendRemoveRelationshipAttributes(entity, entityType, vertex, UPDATE, context, false, true);

                // Update __hasLineage for edges impacted during remove operation
                Set<AtlasEdge> removedEdges = getRemovedInputOutputEdges(guid);
                if (!distributedHasLineageCalculationEnabled && CollectionUtils.isNotEmpty(removedEdges)) {
                    deleteDelegate.getHandler().resetHasLineageOnInputOutputDelete(removedEdges, null);
                }
            }
        }


        if (CollectionUtils.isNotEmpty(context.getEntitiesToDelete())) {
            // TODO : HR : This needs better context to take action for V2
            deleteDelegate.getHandler().deleteEntities(context.getEntitiesToDelete());
        }

        RequestContext req = RequestContext.get();

        if(!req.isPurgeRequested()) {
            for (AtlasEntityHeader entity : req.getDeletedEntities()) {
                resp.addEntity(DELETE, entity);
            }
        }

        for (AtlasEntityHeader entity : req.getUpdatedEntities()) {
            resp.addEntity(updateType, entity);
        }

        RequestContext.get().endMetricRecord(metric);

        if (observabilityData != null) {
            observabilityData.setLineageCalcTime(RequestContext.get().getLineageCalcTime());
        }

        return resp;
    }

    private void setSystemAttributesToEntity(AtlasVertex entityVertex, AtlasEntity createdEntity) {

        createdEntity.setCreatedBy(GraphHelper.getCreatedByAsString(entityVertex));
        createdEntity.setUpdatedBy(RequestContext.get().getUser());
        createdEntity.setCreateTime(new Date(GraphHelper.getCreatedTime(entityVertex)));
        createdEntity.setUpdateTime(new Date(RequestContext.get().getRequestTime()));


        if (DIFFERENTIAL_AUDITS) {
            AtlasEntity diffEntity = RequestContext.get().getDifferentialEntity(createdEntity.getGuid());
            if (diffEntity != null) {
                diffEntity.setUpdateTime(new Date(RequestContext.get().getRequestTime()));
                diffEntity.setUpdatedBy(RequestContext.get().getUser());
            }
        }
    }


    private void setEntityGuidToException(AtlasEntity entity, AtlasBaseException exception, EntityMutationContext context) {
        String guid;
        try {
            guid = context.getGuidAssignments().entrySet().stream().filter(x -> entity.getGuid().equals(x.getValue())).findFirst().get().getKey();
        } catch (NoSuchElementException noSuchElementException) {
            guid = entity.getGuid();
        }

        exception.setEntityGuid(guid);
    }

    public void setCustomAttributes(AtlasVertex vertex, AtlasEntity entity) {
        String customAttributesString = getCustomAttributesString(entity);

        if (customAttributesString != null) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, CUSTOM_ATTRIBUTES_PROPERTY_KEY, customAttributesString);
        }
    }

    public void mapGlossaryRelationshipAttribute(AtlasAttribute attribute, AtlasObjectId glossaryObjectId,
                                                 AtlasVertex entityVertex, EntityMutationContext context) throws AtlasBaseException {

        mapAttribute(attribute, glossaryObjectId, entityVertex, EntityMutations.EntityOperation.UPDATE, context);
    }

    public void setLabels(AtlasVertex vertex, Set<String> labels) throws AtlasBaseException {
        final Set<String> currentLabels = getLabels(vertex);
        final Set<String> addedLabels;
        final Set<String> removedLabels;

        if (CollectionUtils.isEmpty(currentLabels)) {
            addedLabels   = labels;
            removedLabels = null;
        } else if (CollectionUtils.isEmpty(labels)) {
            addedLabels   = null;
            removedLabels = currentLabels;
        } else {
            addedLabels   = new HashSet<String>(CollectionUtils.subtract(labels, currentLabels));
            removedLabels = new HashSet<String>(CollectionUtils.subtract(currentLabels, labels));
        }

        updateLabels(vertex, labels);

        entityChangeNotifier.onLabelsUpdatedFromEntity(graphHelper.getGuid(vertex), addedLabels, removedLabels);
    }

    public void addLabels(AtlasVertex vertex, Set<String> labels) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(labels)) {
            final Set<String> existingLabels = graphHelper.getLabels(vertex);
            final Set<String> updatedLabels;

            if (CollectionUtils.isEmpty(existingLabels)) {
                updatedLabels = labels;
            } else {
                updatedLabels = new HashSet<>(existingLabels);
                updatedLabels.addAll(labels);
            }
            if (!updatedLabels.equals(existingLabels)) {
                updateLabels(vertex, updatedLabels);
                updatedLabels.removeAll(existingLabels);
                entityChangeNotifier.onLabelsUpdatedFromEntity(graphHelper.getGuid(vertex), updatedLabels, null);
            }
        }
    }

    public void removeLabels(AtlasVertex vertex, Set<String> labels) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(labels)) {
            final Set<String> existingLabels = graphHelper.getLabels(vertex);
            Set<String> updatedLabels;

            if (CollectionUtils.isNotEmpty(existingLabels)) {
                updatedLabels = new HashSet<>(existingLabels);
                updatedLabels.removeAll(labels);

                if (!updatedLabels.equals(existingLabels)) {
                    updateLabels(vertex, updatedLabels);
                    existingLabels.removeAll(updatedLabels);
                    entityChangeNotifier.onLabelsUpdatedFromEntity(graphHelper.getGuid(vertex), null, existingLabels);
                }
            }
        }
    }

    public void addOrUpdateBusinessAttributes(String guid, Map<String, Map<String, Object>> businessAttrbutes, boolean isOverwrite) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid is null/empty");
        }

        if (MapUtils.isEmpty(businessAttrbutes)) {
            return;
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String                           typeName                     = getTypeName(entityVertex);
        AtlasEntityType                  entityType                   = typeRegistry.getEntityTypeByName(typeName);
        AtlasEntityHeader                entityHeader                 = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);
        Map<String, Map<String, Object>> currEntityBusinessAttributes = entityRetriever.getBusinessMetadata(entityVertex);
        Set<String>                      updatedBusinessMetadataNames = new HashSet<>();

        for (String bmName : entityType.getBusinessAttributes().keySet()) {
            Map<String, Object> bmAttrs     = businessAttrbutes.get(bmName);
            Map<String, Object> currBmAttrs = currEntityBusinessAttributes != null ? currEntityBusinessAttributes.get(bmName) : null;

            if (MapUtils.isEmpty(bmAttrs) && MapUtils.isEmpty(currBmAttrs)) { // no change
                continue;
            } else if (Objects.equals(bmAttrs, currBmAttrs)) { // no change
                continue;
            }

            updatedBusinessMetadataNames.add(bmName);
        }

        AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA, entityHeader);

        for (String bmName : updatedBusinessMetadataNames) {
            requestBuilder.setBusinessMetadata(bmName);

            AtlasAuthorizationUtils.verifyAccess(requestBuilder.build(), "add/update business-metadata: guid=", guid, ", business-metadata-name=", bmName);
        }

        if (isOverwrite) {
            setBusinessAttributes(entityVertex, entityType, businessAttrbutes);
        } else {
            addOrUpdateBusinessAttributes(entityVertex, entityType, businessAttrbutes);
        }
    }

    /*
     * reset/overwrite business attributes of the entity with given values
     */
    public void setBusinessAttributes(AtlasVertex entityVertex, AtlasEntityType entityType, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> setBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
        }

        validateProductStatus(entityVertex);

        validateBusinessAttributes(entityVertex, entityType, businessAttributes, true);

        Map<String, Map<String, AtlasBusinessAttribute>> entityTypeBusinessAttributes = entityType.getBusinessAttributes();
        Map<String, Map<String, Object>>                 updatedBusinessAttributes    = new HashMap<>();

        for (Map.Entry<String, Map<String, AtlasBusinessAttribute>> entry : entityTypeBusinessAttributes.entrySet()) {
            String                              bmName             = entry.getKey();
            Map<String, AtlasBusinessAttribute> bmAttributes       = entry.getValue();
            Map<String, Object>                 entityBmAttributes = MapUtils.isEmpty(businessAttributes) ? null : businessAttributes.get(bmName);

            for (AtlasBusinessAttribute bmAttribute : bmAttributes.values()) {
                String bmAttrName          = bmAttribute.getName();
                Object bmAttrExistingValue = null;
                boolean isArrayOfPrimitiveType = false;
                boolean isArrayOfEnum = false;
                if (bmAttribute.getAttributeType().getTypeCategory().equals(ARRAY)) {
                    AtlasArrayType bmAttributeType = (AtlasArrayType) bmAttribute.getAttributeType();
                    AtlasType elementType = bmAttributeType.getElementType();
                    isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
                    isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);
                }
                if (isArrayOfPrimitiveType || isArrayOfEnum) {
                    bmAttrExistingValue = entityVertex.getPropertyValues(bmAttribute.getVertexPropertyName(), Object.class);
                } else {
                    bmAttrExistingValue = entityVertex.getProperty(bmAttribute.getVertexPropertyName(), Object.class);
                }
                Object bmAttrNewValue      = MapUtils.isEmpty(entityBmAttributes) ? null : entityBmAttributes.get(bmAttrName);

                if (bmAttrExistingValue == null) {
                    if (bmAttrNewValue != null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("setBusinessAttributes(): adding {}.{}={}", bmName, bmAttribute.getName(), bmAttrNewValue);
                        }

                        mapAttribute(bmAttribute, bmAttrNewValue, entityVertex, CREATE, new EntityMutationContext());

                        addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, bmAttrNewValue);
                    }
                } else {
                    if (bmAttrNewValue != null) {
                        if (!Objects.equals(bmAttrExistingValue, bmAttrNewValue)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("setBusinessAttributes(): updating {}.{}={}", bmName, bmAttribute.getName(), bmAttrNewValue);
                            }

                            mapAttribute(bmAttribute, bmAttrNewValue, entityVertex, UPDATE, new EntityMutationContext());

                            addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, bmAttrNewValue);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("setBusinessAttributes(): removing {}.{}", bmName, bmAttribute.getName());
                        }

                        entityVertex.removeProperty(bmAttribute.getVertexPropertyName());

                        addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, bmAttrNewValue);
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(updatedBusinessAttributes)) {
            updateModificationMetadata(entityVertex);
            entityChangeNotifier.onBusinessAttributesUpdated(AtlasGraphUtilsV2.getIdFromVertex(entityVertex), updatedBusinessAttributes);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== setBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
        }
    }

    /*
     * add or update the given business attributes on the entity
     */
    public void addOrUpdateBusinessAttributes(AtlasVertex entityVertex, AtlasEntityType entityType, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addOrUpdateBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
        }

        validateProductStatus(entityVertex);

        validateBusinessAttributes(entityVertex, entityType, businessAttributes, true);

        Map<String, Map<String, AtlasBusinessAttribute>> entityTypeBusinessAttributes = entityType.getBusinessAttributes();
        Map<String, Map<String, Object>>                 updatedBusinessAttributes    = new HashMap<>();

        if (MapUtils.isNotEmpty(entityTypeBusinessAttributes) && MapUtils.isNotEmpty(businessAttributes)) {
            for (Map.Entry<String, Map<String, AtlasBusinessAttribute>> entry : entityTypeBusinessAttributes.entrySet()) {
                String                              bmName             = entry.getKey();
                Map<String, AtlasBusinessAttribute> bmAttributes       = entry.getValue();
                Map<String, Object>                 entityBmAttributes = businessAttributes.get(bmName);

                if (MapUtils.isEmpty(entityBmAttributes) && !businessAttributes.containsKey(bmName)) {
                    continue;
                }

                for (AtlasBusinessAttribute bmAttribute : bmAttributes.values()) {
                    String bmAttrName = bmAttribute.getName();

                    if (MapUtils.isEmpty(entityBmAttributes)) {
                        entityVertex.removeProperty(bmAttribute.getVertexPropertyName());
                        addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, null);
                        continue;

                    } else if (!entityBmAttributes.containsKey(bmAttrName)) {
                        //since overwriteBusinessAttributes is false, ignore in case BM attr is not passed at all
                        continue;
                    }

                    Object bmAttrValue   = entityBmAttributes.get(bmAttrName);
                    Object existingValue = null;
                    boolean isArrayOfPrimitiveType = false;
                    boolean isArrayOfEnum = false;
                    if (bmAttribute.getAttributeType().getTypeCategory().equals(ARRAY)) {
                        AtlasArrayType bmAttributeType = (AtlasArrayType) bmAttribute.getAttributeType();
                        AtlasType elementType = bmAttributeType.getElementType();
                        isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
                        isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);
                    }
                    if (isArrayOfPrimitiveType || isArrayOfEnum) {
                        existingValue = entityVertex.getPropertyValues(bmAttribute.getVertexPropertyName(), Object.class);
                    } else {
                        existingValue = entityVertex.getProperty(bmAttribute.getVertexPropertyName(), Object.class);
                    }

                    if (existingValue == null) {
                        if (bmAttrValue != null) {
                            mapAttribute(bmAttribute, bmAttrValue, entityVertex, CREATE, new EntityMutationContext());

                            addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, bmAttrValue);
                        }
                    } else {
                        if (!Objects.equals(existingValue, bmAttrValue)) {

                            if( bmAttrValue != null) {
                                mapAttribute(bmAttribute, bmAttrValue, entityVertex, UPDATE, new EntityMutationContext());

                                addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, bmAttrValue);
                            } else {
                                entityVertex.removeProperty(bmAttribute.getVertexPropertyName());
                                addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, null);
                            }
                        }
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(updatedBusinessAttributes)) {
            updateModificationMetadata(entityVertex);
            entityChangeNotifier.onBusinessAttributesUpdated(AtlasGraphUtilsV2.getIdFromVertex(entityVertex), updatedBusinessAttributes);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addOrUpdateBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
        }
    }

    /*
     * remove the given business attributes from the entity
     */
    public void removeBusinessAttributes(AtlasVertex entityVertex, AtlasEntityType entityType, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
        }

        validateProductStatus(entityVertex);

        AtlasEntityHeader               entityHeader   = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);
        AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA, entityHeader);

        for (String bmName : businessAttributes.keySet()) {
            requestBuilder.setBusinessMetadata(bmName);

            AtlasAuthorizationUtils.verifyAccess(requestBuilder.build(), "remove business-metadata: guid=", entityHeader.getGuid(), ", business-metadata=", bmName);
        }

        Map<String, Map<String, AtlasBusinessAttribute>> entityTypeBusinessAttributes = entityType.getBusinessAttributes();
        Map<String, Map<String, Object>>                 updatedBusinessAttributes    = new HashMap<>();

        if (MapUtils.isNotEmpty(entityTypeBusinessAttributes) && MapUtils.isNotEmpty(businessAttributes)) {
            for (Map.Entry<String, Map<String, AtlasBusinessAttribute>> entry : entityTypeBusinessAttributes.entrySet()) {
                String                              bmName       = entry.getKey();
                Map<String, AtlasBusinessAttribute> bmAttributes = entry.getValue();

                if (!businessAttributes.containsKey(bmName)) { // nothing to remove for this business-metadata
                    continue;
                }

                Map<String, Object> entityBmAttributes = businessAttributes.get(bmName);

                for (AtlasBusinessAttribute bmAttribute : bmAttributes.values()) {
                    // if (entityBmAttributes is empty) remove all attributes in this business-metadata
                    // else remove the attribute only if its given in entityBmAttributes
                    if (MapUtils.isEmpty(entityBmAttributes) || entityBmAttributes.containsKey(bmAttribute.getName())) {
                        entityVertex.removeProperty(bmAttribute.getVertexPropertyName());

                        addToUpdatedBusinessAttributes(updatedBusinessAttributes, bmAttribute, null);
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(updatedBusinessAttributes)) {
            updateModificationMetadata(entityVertex);
            entityChangeNotifier.onBusinessAttributesUpdated(AtlasGraphUtilsV2.getIdFromVertex(entityVertex), updatedBusinessAttributes);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== removeBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
        }
    }

    public static void validateProductStatus(AtlasVertex assetVertex) throws AtlasBaseException {
        if (assetVertex != null) {
            if (DATA_PRODUCT_ENTITY_TYPE.equals(assetVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class))) {
                String entityState = assetVertex.getProperty(STATE_PROPERTY_KEY, String.class);

                if ((AtlasEntity.Status.DELETED.name().equals(entityState))) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Cannot update DataProduct that is Archived!");
                }
            }
        }
    }

    private AtlasVertex createStructVertex(AtlasStruct struct) {
        return createStructVertex(struct.getTypeName());
    }

    private AtlasVertex createStructVertex(AtlasObjectId objectId) {
        return createStructVertex(objectId.getTypeName());
    }

    private AtlasVertex createStructVertex(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createStructVertex({})", typeName);
        }

        final AtlasVertex ret = graph.addVertex();

        AtlasGraphUtilsV2.setEncodedProperty(ret, ENTITY_TYPE_PROPERTY_KEY, typeName);
        AtlasGraphUtilsV2.setEncodedProperty(ret, STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
        AtlasGraphUtilsV2.setEncodedProperty(ret, TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        AtlasGraphUtilsV2.setEncodedProperty(ret, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        AtlasGraphUtilsV2.setEncodedProperty(ret, CREATED_BY_KEY, RequestContext.get().getUser());
        AtlasGraphUtilsV2.setEncodedProperty(ret, MODIFIED_BY_KEY, RequestContext.get().getUser());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createStructVertex({})", typeName);
        }

        return ret;
    }

    private AtlasVertex createClassificationVertex(AtlasClassification classification) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createVertex({})", classification.getTypeName());
        }

        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());

        AtlasVertex ret = createStructVertex(classification);

        AtlasGraphUtilsV2.addEncodedProperty(ret, SUPER_TYPES_PROPERTY_KEY, classificationType.getAllSuperTypes());
        AtlasGraphUtilsV2.setEncodedProperty(ret, CLASSIFICATION_ENTITY_GUID, classification.getEntityGuid());
        AtlasGraphUtilsV2.setEncodedProperty(ret, CLASSIFICATION_ENTITY_STATUS, classification.getEntityStatus().name());

        return ret;
    }

    private void mapAttributes(AtlasStruct struct, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        mapAttributes(struct, getStructType(struct.getTypeName()), vertex, op, context);
    }

    public void mapAttributes(AtlasStruct struct, AtlasStructType structType, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapAttributes({}, {})", op, struct.getTypeName());
        }

        if (MapUtils.isNotEmpty(struct.getAttributes())) {
            MetricRecorder metric = RequestContext.get().startMetricRecord("mapAttributes");

            List<String> timestampAutoUpdateAttributes = new ArrayList<>();
            List<String> userAutoUpdateAttributes = new ArrayList<>();

            if (op.equals(CREATE)) {
                for (AtlasAttribute attribute : structType.getAllAttributes().values()) {
                    Object attrValue = struct.getAttribute(attribute.getName());
                    Object attrOldValue = null;
                    boolean isArrayOfPrimitiveType = false;
                    boolean isArrayOfEnum = false;
                    if (attribute.getAttributeType().getTypeCategory().equals(ARRAY)) {
                        AtlasArrayType attributeType = (AtlasArrayType) attribute.getAttributeType();
                        AtlasType elementType = attributeType.getElementType();
                        isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
                        isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);
                    }
                    if (isArrayOfPrimitiveType || isArrayOfEnum) {
                        attrOldValue = vertex.getPropertyValues(attribute.getVertexPropertyName(),attribute.getClass());
                    } else {
                        attrOldValue = vertex.getProperty(attribute.getVertexPropertyName(),attribute.getClass());
                    }
                    if (attrValue!= null && !attrValue.equals(attrOldValue)) {
                        addValuesToAutoUpdateAttributesList(attribute, userAutoUpdateAttributes, timestampAutoUpdateAttributes);
                    }

                    mapAttribute(attribute, attrValue, vertex, op, context);
                }

            } else if (op.equals(UPDATE) || op.equals(PARTIAL_UPDATE)) {
                for (String attrName : struct.getAttributes().keySet()) {
                    AtlasAttribute attribute = structType.getAttribute(attrName);

                    if (attribute != null) {
                        Object attrValue = struct.getAttribute(attrName);
                        Object attrOldValue = null;
                        boolean isArrayOfPrimitiveType = false;
                        boolean isArrayOfEnum = false;

                        boolean isStruct = (TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory()
                                || TypeCategory.STRUCT == attribute.getAttributeType().getTypeCategory());

                        if (attribute.getAttributeType().getTypeCategory().equals(ARRAY)) {
                            AtlasArrayType attributeType = (AtlasArrayType) attribute.getAttributeType();
                            AtlasType elementType = attributeType.getElementType();
                            isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
                            isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);
                        }

                        if (isArrayOfPrimitiveType || isArrayOfEnum) {
                            attrOldValue = vertex.getPropertyValues(attribute.getVertexPropertyName(),attribute.getClass());
                        } else if (isStruct) {
                            String edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                            attrOldValue = getCollectionElementsUsingRelationship(vertex, attribute, edgeLabel);
                        } else {
                            attrOldValue = vertex.getProperty(attribute.getVertexPropertyName(),attribute.getClass());
                        }

                        if (attrValue != null && !attrValue.equals(attrOldValue)) {
                            addValuesToAutoUpdateAttributesList(attribute, userAutoUpdateAttributes, timestampAutoUpdateAttributes);
                        }

                        mapAttribute(attribute, attrValue, vertex, op, context);
                    } else {
                        LOG.warn("mapAttributes(): invalid attribute {}.{}. Ignored..", struct.getTypeName(), attrName);
                    }
                }
            }

            updateModificationMetadata(vertex);
            graphHelper.updateMetadataAttributes(vertex, timestampAutoUpdateAttributes, "timestamp");
            graphHelper.updateMetadataAttributes(vertex, userAutoUpdateAttributes, "user");

            RequestContext.get().endMetricRecord(metric);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapAttributes({}, {})", op, struct.getTypeName());
        }
    }

    private void addValuesToAutoUpdateAttributesList(AtlasAttribute attribute, List<String> userAutoUpdateAttributes, List<String> timestampAutoUpdateAttributes) {
        HashMap<String, ArrayList> autoUpdateAttributes =  attribute.getAttributeDef().getAutoUpdateAttributes();
        if (autoUpdateAttributes != null) {
            List<String> userAttributes = autoUpdateAttributes.get("user");
            if (userAttributes != null && userAttributes.size() > 0) {
                userAutoUpdateAttributes.addAll(userAttributes);
            }
            List<String> timestampAttributes = autoUpdateAttributes.get("timestamp");
            if (timestampAttributes != null && timestampAttributes.size() > 0) {
                timestampAutoUpdateAttributes.addAll(timestampAttributes);
            }
        }
    }

    private void mapRelationshipAttributes(AtlasEntity entity, AtlasEntityType entityType, AtlasVertex vertex, EntityOperation op,
                                           EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapRelationshipAttributes({}, {})", op, entity.getTypeName());
        }

        if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
            MetricRecorder metric = RequestContext.get().startMetricRecord("mapRelationshipAttributes");

            if (op.equals(CREATE)) {
                for (String attrName : entityType.getRelationshipAttributes().keySet()) {
                    Object         attrValue    = entity.getRelationshipAttribute(attrName);
                    String         relationType = AtlasEntityUtil.getRelationshipType(attrValue);
                    AtlasAttribute attribute    = entityType.getRelationshipAttribute(attrName, relationType);

                    mapAttribute(attribute, attrValue, vertex, op, context);
                }

            } else if (op.equals(UPDATE) || op.equals(PARTIAL_UPDATE)) {
                // relationship attributes mapping
                for (String attrName : entityType.getRelationshipAttributes().keySet()) {
                    if (entity.hasRelationshipAttribute(attrName)) {
                        Object         attrValue    = entity.getRelationshipAttribute(attrName);
                        String         relationType = AtlasEntityUtil.getRelationshipType(attrValue);
                        AtlasAttribute attribute    = entityType.getRelationshipAttribute(attrName, relationType);

                        mapAttribute(attribute, attrValue, vertex, op, context);
                    }
                }
            }
            updateModificationMetadata(vertex);

            RequestContext.get().endMetricRecord(metric);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapRelationshipAttributes({}, {})", op, entity.getTypeName());
        }
    }

    private void mapAppendRemoveRelationshipAttributes(AtlasEntity entity, AtlasEntityType entityType, AtlasVertex vertex, EntityOperation op,
                                                       EntityMutationContext context, boolean isAppendOp, boolean isRemoveOp) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapAppendRemoveRelationshipAttributes({}, {})", op, entity.getTypeName());
        }

        MetricRecorder metric = RequestContext.get().startMetricRecord("mapAppendRemoveRelationshipAttributes");

        if (isAppendOp && MapUtils.isNotEmpty(entity.getAppendRelationshipAttributes())) {
            if (op.equals(UPDATE) || op.equals(PARTIAL_UPDATE)) {
                // relationship attributes mapping
                for (String attrName : entityType.getRelationshipAttributes().keySet()) {
                    if (entity.hasAppendRelationshipAttribute(attrName)) {
                        Object         attrValue    = entity.getAppendRelationshipAttribute(attrName);
                        String         relationType = AtlasEntityUtil.getRelationshipType(attrValue);
                        AtlasAttribute attribute    = entityType.getRelationshipAttribute(attrName, relationType);
                        mapAttribute(attribute, attrValue, vertex, op, context, true, isRemoveOp);
                    }
                }
            }
        }

        if (isRemoveOp && MapUtils.isNotEmpty(entity.getRemoveRelationshipAttributes())) {
            if (op.equals(UPDATE) || op.equals(PARTIAL_UPDATE)) {
                // relationship attributes mapping
                for (String attrName : entityType.getRelationshipAttributes().keySet()) {
                    if (entity.hasRemoveRelationshipAttribute(attrName)) {
                        Object         attrValue    = entity.getRemoveRelationshipAttribute(attrName);
                        String         relationType = AtlasEntityUtil.getRelationshipType(attrValue);
                        AtlasAttribute attribute    = entityType.getRelationshipAttribute(attrName, relationType);
                        mapAttribute(attribute, attrValue, vertex, op, context, isAppendOp, true);
                    }
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapAppendRemoveRelationshipAttributes({}, {})", op, entity.getTypeName());
        }
    }

    private void mapAttribute(AtlasAttribute attribute, Object attrValue, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        mapAttribute(attribute, attrValue, vertex, op, context, false, false);
    }

    private void mapAttribute(AtlasAttribute attribute, Object attrValue, AtlasVertex vertex, EntityOperation op, EntityMutationContext context, boolean isAppendOp, boolean isRemoveOp) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapAttribute");
        try {
            boolean isDeletedEntity = context.isDeletedEntity(vertex);
            AtlasType         attrType     = attribute.getAttributeType();
            if (attrValue == null) {
                AtlasAttributeDef attributeDef = attribute.getAttributeDef();

                if (attrType.getTypeCategory() == TypeCategory.PRIMITIVE) {
                    if (attributeDef.getDefaultValue() != null) {
                        attrValue = attrType.createDefaultValue(attributeDef.getDefaultValue());
                    } else if (attributeDef.getIsDefaultValueNull() && ALLOWED_DATATYPES_FOR_DEFAULT_NULL.contains(attribute.getTypeName())) {
                        attrValue = null;
                    } else {
                        if (attribute.getAttributeDef().getIsOptional()) {
                            attrValue = attrType.createOptionalDefaultValue();
                        } else {
                            attrValue = attrType.createDefaultValue();
                        }
                    }
                }
            }

            if (attrType.getTypeCategory() == TypeCategory.PRIMITIVE || attrType.getTypeCategory() == TypeCategory.ENUM) {
                mapPrimitiveValue(vertex, attribute, attrValue, isDeletedEntity);
            } else {
                AttributeMutationContext ctx = new AttributeMutationContext(op, vertex, attribute, attrValue);
                mapToVertexByTypeCategory(ctx, context, isAppendOp, isRemoveOp);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Object mapToVertexByTypeCategory(AttributeMutationContext ctx, EntityMutationContext context, boolean isAppendOp, boolean isRemoveOp) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapToVertexByTypeCategory");

        try {
            if (ctx.getOp() == CREATE && ctx.getValue() == null) {
                return null;
            }

            switch (ctx.getAttrType().getTypeCategory()) {
                case PRIMITIVE:
                case ENUM:
                    return mapPrimitiveValue(ctx, context);

                case STRUCT: {
                    String    edgeLabel   = AtlasGraphUtilsV2.getEdgeLabel(ctx.getVertexProperty());
                    AtlasEdge currentEdge = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel);
                    AtlasEdge edge        = currentEdge != null ? currentEdge : null;

                    ctx.setExistingEdge(edge);

                    AtlasEdge newEdge = mapStructValue(ctx, context);

                    if (currentEdge != null && !currentEdge.equals(newEdge)) {
                        deleteDelegate.getHandler().deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), false, true, ctx.getReferringVertex());
                    }

                    return newEdge;
                }

                case OBJECT_ID_TYPE: {
                    if (ctx.getAttributeDef().isSoftReferenced()) {
                        return mapSoftRefValueWithUpdate(ctx, context);
                    }

                    AtlasRelationshipEdgeDirection edgeDirection = ctx.getAttribute().getRelationshipEdgeDirection();
                    String edgeLabel = ctx.getAttribute().getRelationshipEdgeLabel();

                    // if relationshipDefs doesn't exist, use legacy way of finding edge label.
                    if (StringUtils.isEmpty(edgeLabel)) {
                        edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(ctx.getVertexProperty());
                    }

                    String    relationshipGuid = getRelationshipGuid(ctx.getValue());
                    AtlasEdge currentEdge;

                    // if relationshipGuid is assigned in AtlasRelatedObjectId use it to fetch existing AtlasEdge
                    if (StringUtils.isNotEmpty(relationshipGuid) && !RequestContext.get().isImportInProgress()) {
                        currentEdge = graphHelper.getEdgeForGUID(relationshipGuid);
                    } else {
                        currentEdge = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel, edgeDirection);
                    }

                    AtlasEdge newEdge = null;

                    if (ctx.getValue() != null) {
                        AtlasEntityType instanceType = getInstanceType(ctx.getValue(), context);
                        AtlasEdge       edge         = currentEdge != null ? currentEdge : null;

                        ctx.setElementType(instanceType);
                        ctx.setExistingEdge(edge);

                        newEdge = mapObjectIdValueUsingRelationship(ctx, context);

                        // legacy case update inverse attribute
                        if (ctx.getAttribute().getInverseRefAttribute() != null) {
                            // Update the inverse reference using relationship on the target entity
                            addInverseReference(context, ctx.getAttribute().getInverseRefAttribute(), newEdge, getRelationshipAttributes(ctx.getValue()));
                        }
                    }

                    // created new relationship,
                    // record entity update on both vertices of the new relationship
                    if (currentEdge == null && newEdge != null) {

                        // based on relationship edge direction record update only on attribute vertex
                        if (edgeDirection == IN) {
                            recordEntityUpdate(newEdge.getOutVertex());

                        } else {
                            recordEntityUpdate(newEdge.getInVertex());
                        }
                    }

                    // update references, if current and new edge don't match
                    // record entity update on new reference and delete(edge) old reference.
                    if (currentEdge != null && !currentEdge.equals(newEdge)) {

                        //record entity update on new edge
                        if (isRelationshipEdge(newEdge)) {
                            AtlasVertex attrVertex = context.getDiscoveryContext().getResolvedEntityVertex(getGuid(ctx.getValue()));

                            recordEntityUpdate(attrVertex);
                        }

                        //delete old reference
                        deleteDelegate.getHandler().deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), ctx.getAttribute().isOwnedRef(),
                                true, ctx.getAttribute().getRelationshipEdgeDirection(), ctx.getReferringVertex());
                        if (IN == edgeDirection) {
                            recordEntityUpdate(currentEdge.getOutVertex(), ctx, false);
                        } else {
                            recordEntityUpdate(currentEdge.getInVertex(), ctx, false);
                        }
                    }

                    if (isAppendOp) {
                        // E.g. Appending Table relationship while updating a Column (1:n)
                        if (newEdge != null && getCreatedTime(newEdge) == RequestContext.get().getRequestTime()) {
                            // Only process if newly created the edge
                            AtlasVertex inverseVertex = newEdge.getInVertex();
                            if (IN == edgeDirection) {
                                inverseVertex = newEdge.getOutVertex();
                            }

                            Map<String, String> objectId = new HashMap<>();
                            objectId.put("typeName", getTypeName(inverseVertex));
                            objectId.put("guid", GraphHelper.getGuid(inverseVertex));

                            AtlasEntity diffEntity = entityRetriever.getOrInitializeDiffEntity(ctx.getReferringVertex());
                            diffEntity.setAddedRelationshipAttribute(ctx.getAttribute().getName(), objectId);
                        }
                    }

                    if (edgeLabel.equals(GLOSSARY_TERMS_EDGE_LABEL) || edgeLabel.equals(GLOSSARY_CATEGORY_EDGE_LABEL)) {
                        addGlossaryAttr(ctx, newEdge);
                    }

                    if (CATEGORY_PARENT_EDGE_LABEL.equals(edgeLabel)) {
                        addCatParentAttr(ctx, newEdge);
                    }

                    return newEdge;
                }

                case MAP:
                    return mapMapValue(ctx, context);

                case ARRAY:
                    if (isAppendOp){
                        return appendArrayValue(ctx, context);
                    }

                    if (isRemoveOp){
                        return removeArrayValue(ctx, context);
                    }

                    return mapArrayValue(ctx, context);

                default:
                    throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private String mapSoftRefValue(AttributeMutationContext ctx, EntityMutationContext context) {
        String ret = null;

        if (ctx.getValue() instanceof AtlasObjectId) {
            AtlasObjectId objectId = (AtlasObjectId) ctx.getValue();
            String        typeName = objectId.getTypeName();
            String        guid     = AtlasTypeUtil.isUnAssignedGuid(objectId.getGuid()) ? context.getGuidAssignments().get(objectId.getGuid()) : objectId.getGuid();

            ret = AtlasEntityUtil.formatSoftRefValue(typeName, guid);
        } else {
            if (ctx.getValue() != null) {
                LOG.warn("mapSoftRefValue: Was expecting AtlasObjectId, but found: {}", ctx.getValue().getClass());
            }
        }

        setAssignedGuid(ctx.getValue(), context);

        return ret;
    }

    private Object mapSoftRefValueWithUpdate(AttributeMutationContext ctx, EntityMutationContext context) {
        String softRefValue = mapSoftRefValue(ctx, context);

        AtlasGraphUtilsV2.setProperty(ctx.getReferringVertex(), ctx.getVertexProperty(), softRefValue);

        return softRefValue;
    }

    private void addInverseReference(EntityMutationContext context, AtlasAttribute inverseAttribute, AtlasEdge edge, Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        AtlasStructType inverseType      = inverseAttribute.getDefinedInType();
        AtlasVertex     inverseVertex    = edge.getInVertex();
        String          inverseEdgeLabel = inverseAttribute.getRelationshipEdgeLabel();
        AtlasEdge       inverseEdge      = graphHelper.getEdgeForLabel(inverseVertex, inverseEdgeLabel);
        String          propertyName     = AtlasGraphUtilsV2.getQualifiedAttributePropertyKey(inverseType, inverseAttribute.getName());

        // create new inverse reference
        AtlasEdge newEdge = createInverseReferenceUsingRelationship(context, inverseAttribute, edge, relationshipAttributes);

        boolean inverseUpdated = true;
        switch (inverseAttribute.getAttributeType().getTypeCategory()) {
            case OBJECT_ID_TYPE:
                if (inverseEdge != null) {
                    if (!inverseEdge.equals(newEdge)) {
                        // Disconnect old reference
                        deleteDelegate.getHandler().deleteEdgeReference(inverseEdge, inverseAttribute.getAttributeType().getTypeCategory(),
                                inverseAttribute.isOwnedRef(), true, inverseVertex);
                    }
                    else {
                        // Edge already exists for this attribute between these vertices.
                        inverseUpdated = false;
                    }
                }
                break;
            case ARRAY:
                // Add edge ID to property value
                List<String> elements = inverseVertex.getProperty(propertyName, List.class);
                if (newEdge != null && elements == null) {
                    elements = new ArrayList<>();
                    elements.add(newEdge.getId().toString());
                    inverseVertex.setProperty(propertyName, elements);
                }
                else {
                    if (newEdge != null && !elements.contains(newEdge.getId().toString())) {
                        elements.add(newEdge.getId().toString());
                        inverseVertex.setProperty(propertyName, elements);
                    }
                    else {
                        // Property value list already contains the edge ID.
                        inverseUpdated = false;
                    }
                }
                break;
            default:
                break;
        }

        if (inverseUpdated) {
            RequestContext requestContext = RequestContext.get();

            if (!requestContext.isDeletedEntity(graphHelper.getGuid(inverseVertex))) {
                updateModificationMetadata(inverseVertex);

                requestContext.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(inverseVertex));
            }
        }
    }

    private AtlasEdge createInverseReferenceUsingRelationship(EntityMutationContext context, AtlasAttribute inverseAttribute, AtlasEdge edge, Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createInverseReferenceUsingRelationship()");
        }

        String      inverseAttributeName   = inverseAttribute.getName();
        AtlasType   inverseAttributeType   = inverseAttribute.getDefinedInType();
        AtlasVertex inverseVertex          = edge.getInVertex();
        AtlasVertex vertex                 = edge.getOutVertex();
        AtlasEdge   ret;

        if (inverseAttributeType instanceof AtlasEntityType) {
            AtlasEntityType entityType = (AtlasEntityType) inverseAttributeType;

            if (entityType.hasRelationshipAttribute(inverseAttributeName)) {
                String relationshipName = graphHelper.getRelationshipTypeName(inverseVertex, entityType, inverseAttributeName);

                ret = getOrCreateRelationship(inverseVertex, vertex, relationshipName, relationshipAttributes);

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No RelationshipDef defined between {} and {} on attribute: {}", inverseAttributeType,
                            AtlasGraphUtilsV2.getTypeName(vertex), inverseAttributeName);
                }
                // if no RelationshipDef found, use legacy way to create edges
                ret = createInverseReference(inverseAttribute, (AtlasStructType) inverseAttributeType, inverseVertex, vertex);
            }
        } else {
            // inverseAttribute not of type AtlasEntityType, use legacy way to create edges
            ret = createInverseReference(inverseAttribute, (AtlasStructType) inverseAttributeType, inverseVertex, vertex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createInverseReferenceUsingRelationship()");
        }

        updateRelationshipGuidForImport(context, inverseAttributeName, inverseVertex, ret);

        return ret;
    }

    private void updateRelationshipGuidForImport(EntityMutationContext context, String inverseAttributeName, AtlasVertex inverseVertex, AtlasEdge edge) throws AtlasBaseException {
        if (!RequestContext.get().isImportInProgress()) {
            return;
        }

        String parentGuid = graphHelper.getGuid(inverseVertex);
        if(StringUtils.isEmpty(parentGuid)) {
            return;
        }

        AtlasEntity entity = context.getCreatedOrUpdatedEntity(parentGuid);
        if(entity == null) {
            return;
        }

        String parentRelationshipGuid = getRelationshipGuid(entity.getRelationshipAttribute(inverseAttributeName));
        if(StringUtils.isEmpty(parentRelationshipGuid)) {
            return;
        }

        AtlasGraphUtilsV2.setEncodedProperty(edge, RELATIONSHIP_GUID_PROPERTY_KEY, parentRelationshipGuid);
    }

    // legacy method to create edges for inverse reference
    private AtlasEdge createInverseReference(AtlasAttribute inverseAttribute, AtlasStructType inverseAttributeType,
                                             AtlasVertex inverseVertex, AtlasVertex vertex) throws AtlasBaseException {

        String propertyName     = AtlasGraphUtilsV2.getQualifiedAttributePropertyKey(inverseAttributeType, inverseAttribute.getName());
        String inverseEdgeLabel = AtlasGraphUtilsV2.getEdgeLabel(propertyName);
        AtlasEdge ret;

        try {
            ret = graphHelper.getOrCreateEdge(inverseVertex, vertex, inverseEdgeLabel);

        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        return ret;
    }

    private Object mapPrimitiveValue(AttributeMutationContext ctx, EntityMutationContext context) {
        return mapPrimitiveValue(ctx.getReferringVertex(), ctx.getAttribute(), ctx.getValue(), context.isDeletedEntity(ctx.referringVertex));
    }

    private Object mapPrimitiveValue(AtlasVertex vertex, AtlasAttribute attribute, Object valueFromEntity, boolean isDeletedEntity) {
        boolean isIndexableStrAttr = attribute.getAttributeDef().getIsIndexable() && attribute.getAttributeType() instanceof AtlasBuiltInTypes.AtlasStringType;

        Object ret = valueFromEntity;

        // Janus bug, when an indexed string attribute has a value longer than a certain length then the reverse indexed key generated by JanusGraph
        // exceeds the HBase row length's hard limit (Short.MAX). This trimming and hashing procedure is to circumvent that limitation
        if (ret != null && isIndexableStrAttr) {
            String value = ret.toString();

            if (value.length() > INDEXED_STR_SAFE_LEN) {
                RequestContext requestContext = RequestContext.get();

                final int trimmedLength;

                if (requestContext.getAttemptCount() <= 1) { // if this is the first attempt, try saving as it is; trim on retry
                    trimmedLength = value.length();
                } else if (requestContext.getAttemptCount() >= requestContext.getMaxAttempts()) { // if this is the last attempt, set to 'safe_len'
                    trimmedLength = INDEXED_STR_SAFE_LEN;
                } else if (requestContext.getAttemptCount() == 2) { // based on experimentation, string length of 4 times 'safe_len' succeeds
                    trimmedLength = Math.min(4 * INDEXED_STR_SAFE_LEN, value.length());
                } else if (requestContext.getAttemptCount() == 3) { // if length of 4 times 'safe_len' failed, try twice 'safe_len'
                    trimmedLength = Math.min(2 * INDEXED_STR_SAFE_LEN, value.length());
                } else { // if twice the 'safe_len' failed, trim to 'safe_len'
                    trimmedLength = INDEXED_STR_SAFE_LEN;
                }

                if (trimmedLength < value.length()) {
                    LOG.warn("Length of indexed attribute {} is {} characters, longer than safe-limit {}; trimming to {} - attempt #{}", attribute.getQualifiedName(), value.length(), INDEXED_STR_SAFE_LEN, trimmedLength, requestContext.getAttemptCount());

                    String checksumSuffix = ":" + DigestUtils.shaHex(value); // Storing SHA checksum in case verification is needed after retrieval

                    ret = value.substring(0, trimmedLength - checksumSuffix.length()) + checksumSuffix;
                } else {
                    LOG.warn("Length of indexed attribute {} is {} characters, longer than safe-limit {}", attribute.getQualifiedName(), value.length(), INDEXED_STR_SAFE_LEN);
                }
            }
        }

        AtlasGraphUtilsV2.setEncodedProperty(vertex, attribute.getVertexPropertyName(), ret);

        String uniqPropName = attribute != null ? attribute.getVertexUniquePropertyName() : null;

        if (uniqPropName != null) {
            // Removing AtlasGraphUtilsV2.getState(vertex) == DELETED condition below to keep the unique contrain even if asset is deleted.
            if (isDeletedEntity) {
                vertex.removeProperty(uniqPropName);
            } else {
                AtlasGraphUtilsV2.setEncodedProperty(vertex, uniqPropName, ret);
            }
        }

        return ret;
    }

    private AtlasEdge mapStructValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapStructValue({})", ctx);
        }

        AtlasEdge ret = null;

        if (ctx.getCurrentEdge() != null) {
            AtlasStruct structVal = null;
            if (ctx.getValue() instanceof AtlasStruct) {
                structVal = (AtlasStruct)ctx.getValue();
            } else if (ctx.getValue() instanceof Map) {
                structVal = new AtlasStruct(ctx.getAttrType().getTypeName(), (Map) AtlasTypeUtil.toStructAttributes((Map)ctx.getValue()));
            }

            if (structVal != null) {
                updateVertex(structVal, ctx.getCurrentEdge().getInVertex(), context);
                ret = ctx.getCurrentEdge();
            } else {
                ret = null;
            }

        } else if (ctx.getValue() != null) {
            String edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(ctx.getVertexProperty());

            AtlasStruct structVal = null;
            if (ctx.getValue() instanceof AtlasStruct) {
                structVal = (AtlasStruct) ctx.getValue();
            } else if (ctx.getValue() instanceof Map) {
                structVal = new AtlasStruct(ctx.getAttrType().getTypeName(), (Map) AtlasTypeUtil.toStructAttributes((Map)ctx.getValue()));
            }

            if (structVal != null) {
                ret = createVertex(structVal, ctx.getReferringVertex(), edgeLabel, context);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapStructValue({})", ctx);
        }

        return ret;
    }

    private AtlasEdge mapObjectIdValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapObjectIdValue({})", ctx);
        }

        AtlasEdge ret = null;

        String guid = getGuid(ctx.getValue());

        AtlasVertex entityVertex = context.getDiscoveryContext().getResolvedEntityVertex(guid);

        if (entityVertex == null) {
            if (AtlasTypeUtil.isAssignedGuid(guid)) {
                entityVertex = context.getVertex(guid);
            }

            if (entityVertex == null) {
                AtlasObjectId objId = getObjectId(ctx.getValue());

                if (objId != null) {
                    entityVertex = context.getDiscoveryContext().getResolvedEntityVertex(objId);
                }
            }
        }

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, (ctx.getValue() == null ? null : ctx.getValue().toString()));
        }

        if (ctx.getCurrentEdge() != null) {
            ret = updateEdge(ctx.getAttributeDef(), ctx.getValue(), ctx.getCurrentEdge(), entityVertex);
        } else if (ctx.getValue() != null) {
            String edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(ctx.getVertexProperty());

            try {
                ret = graphHelper.getOrCreateEdge(ctx.getReferringVertex(), entityVertex, edgeLabel);
            } catch (RepositoryException e) {
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapObjectIdValue({})", ctx);
        }

        return ret;
    }

    private AtlasEdge mapObjectIdValueUsingRelationship(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapObjectIdValueUsingRelationship");
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> mapObjectIdValueUsingRelationship({})", ctx);
            }

            String      guid            = getGuid(ctx.getValue());
            AtlasVertex attributeVertex = context.getDiscoveryContext().getResolvedEntityVertex(guid);
            AtlasVertex entityVertex    = ctx.getReferringVertex();
            AtlasEdge   ret;

            if (attributeVertex == null) {
                if (AtlasTypeUtil.isAssignedGuid(guid)) {
                    attributeVertex = context.getVertex(guid);
                }

                if (attributeVertex == null) {
                    AtlasObjectId objectId = getObjectId(ctx.getValue());

                    attributeVertex = (objectId != null) ? context.getDiscoveryContext().getResolvedEntityVertex(objectId) : null;
                }
            }

            if (attributeVertex == null) {
                if(RequestContext.get().isImportInProgress()) {
                    return null;
                }

                throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, (ctx.getValue() == null ? null : ctx.getValue().toString()));
            }

            AtlasType type = typeRegistry.getType(AtlasGraphUtilsV2.getTypeName(entityVertex));

            if (type instanceof AtlasEntityType) {
                AtlasEntityType entityType = (AtlasEntityType) type;
                AtlasAttribute  attribute     = ctx.getAttribute();
                String          attributeName = attribute.getName();

                // use relationship to create/update edges
                if (entityType.hasRelationshipAttribute(attributeName)) {
                    Map<String, Object> relationshipAttributes = getRelationshipAttributes(ctx.getValue());

                    if (ctx.getCurrentEdge() != null && getStatus(ctx.getCurrentEdge()) != DELETED) {
                        ret = updateRelationship(ctx, entityVertex, attributeVertex, attribute.getRelationshipEdgeDirection(), relationshipAttributes);
                    } else {
                        String      relationshipName = attribute.getRelationshipName();
                        AtlasVertex fromVertex;
                        AtlasVertex toVertex;

                        if (StringUtils.isEmpty(relationshipName)) {
                            relationshipName = graphHelper.getRelationshipTypeName(entityVertex, entityType, attributeName);
                        }

                        if (attribute.getRelationshipEdgeDirection() == IN) {
                            fromVertex = attributeVertex;
                            toVertex   = entityVertex;

                        } else {
                            fromVertex = entityVertex;
                            toVertex   = attributeVertex;
                        }

                        ret = getOrCreateRelationship(fromVertex, toVertex, relationshipName, relationshipAttributes);

                        boolean isCreated = getCreatedTime(ret) == RequestContext.get().getRequestTime();

                        if (isCreated) {
                            // if relationship did not exist before and new relationship was created
                            // record entity update on both relationship vertices
                            recordEntityUpdate(attributeVertex, ctx, true);
                        }

                        // for import use the relationship guid provided
                        if (RequestContext.get().isImportInProgress()) {
                            String relationshipGuid = getRelationshipGuid(ctx.getValue());

                            if(!StringUtils.isEmpty(relationshipGuid)) {
                                AtlasGraphUtilsV2.setEncodedProperty(ret, RELATIONSHIP_GUID_PROPERTY_KEY, relationshipGuid);
                            }
                        }
                    }
                } else {
                    // use legacy way to create/update edges
                    if (WARN_ON_NO_RELATIONSHIP || LOG.isDebugEnabled()) {
                        LOG.warn("No RelationshipDef defined between {} and {} on attribute: {}. This can lead to severe performance degradation.",
                                getTypeName(entityVertex), getTypeName(attributeVertex), attributeName);
                    }

                    ret = mapObjectIdValue(ctx, context);
                }

            } else {
                // if type is StructType having objectid as attribute
                ret = mapObjectIdValue(ctx, context);
            }

            setAssignedGuid(ctx.getValue(), context);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== mapObjectIdValueUsingRelationship({})", ctx);
            }

            return ret;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private AtlasEdge getEdgeUsingRelationship(AttributeMutationContext ctx, EntityMutationContext context, boolean createEdge) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEdgeUsingRelationship({})", ctx);
        }

        String      guid            = getGuid(ctx.getValue());
        AtlasVertex attributeVertex = context.getDiscoveryContext().getResolvedEntityVertex(guid);
        AtlasVertex entityVertex    = ctx.getReferringVertex();
        AtlasEdge   ret = null;

        if (attributeVertex == null) {
            if (AtlasTypeUtil.isAssignedGuid(guid)) {
                attributeVertex = context.getVertex(guid);
            }

            if (attributeVertex == null) {
                AtlasObjectId objectId = getObjectId(ctx.getValue());

                attributeVertex = (objectId != null) ? context.getDiscoveryContext().getResolvedEntityVertex(objectId) : null;
            }
        }


        if (attributeVertex == null) {
            if(RequestContext.get().isImportInProgress()) {
                return null;
            }

            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, (ctx.getValue() == null ? null : ctx.getValue().toString()));
        }

        AtlasType type = typeRegistry.getType(AtlasGraphUtilsV2.getTypeName(entityVertex));

        if (type instanceof AtlasEntityType) {
            AtlasEntityType entityType = (AtlasEntityType) type;
            AtlasAttribute  attribute     = ctx.getAttribute();
            AtlasType atlasType = attribute.getAttributeType();
            String          attributeName = attribute.getName();

            if (entityType.hasRelationshipAttribute(attributeName)) {
                String      relationshipName = attribute.getRelationshipName();
                AtlasVertex fromVertex;
                AtlasVertex toVertex;


                if (StringUtils.isEmpty(relationshipName)) {
                    relationshipName = graphHelper.getRelationshipTypeName(entityVertex, entityType, attributeName);
                }

                if (attribute.getRelationshipEdgeDirection() == IN) {
                    fromVertex = attributeVertex;
                    toVertex   = entityVertex;

                } else {
                    fromVertex = entityVertex;
                    toVertex   = attributeVertex;
                }

                AtlasEdge newEdge = null;

                Map<String, Object> relationshipAttributes = getRelationshipAttributes(ctx.getValue());
                AtlasRelationship relationship = new AtlasRelationship(relationshipName, relationshipAttributes);
                String relationshipLabel = StringUtils.EMPTY;

                if (createEdge) {
                    // hard delete the edge if it exists and is  soft deleted
                    if (relationshipStore instanceof  AtlasRelationshipStoreV2){
                        relationshipLabel = ((AtlasRelationshipStoreV2)relationshipStore).getRelationshipEdgeLabel(fromVertex, toVertex,  relationship.getTypeName());
                    }
                    if (StringUtils.isNotEmpty(relationshipLabel)) {
                        Iterator<AtlasEdge> edges = fromVertex.getEdges(AtlasEdgeDirection.OUT, relationshipLabel).iterator();
                        while (edges.hasNext()) {
                            AtlasEdge edge = edges.next();
                            if (edge.getInVertex().equals(toVertex) && getStatus(edge) == DELETED) {
                                // Hard delete the newEdge
                                if (atlasType instanceof AtlasArrayType) {
                                    deleteDelegate.getHandler(DeleteType.HARD).deleteEdgeReference(edge, ((AtlasArrayType) atlasType).getElementType().getTypeCategory(), attribute.isOwnedRef(),
                                            true, attribute.getRelationshipEdgeDirection(), entityVertex);
                                } else {
                                    deleteDelegate.getHandler(DeleteType.HARD).deleteEdgeReference(edge, attribute.getAttributeType().getTypeCategory(), attribute.isOwnedRef(),
                                            true, attribute.getRelationshipEdgeDirection(), entityVertex);
                                }

                            }
                        }
                    }
                    newEdge = relationshipStore.getOrCreate(fromVertex, toVertex, relationship, false);
                    boolean isCreated = graphHelper.getCreatedTime(newEdge) == RequestContext.get().getRequestTime();

                    if (isCreated) {
                        // if relationship did not exist before and new relationship was created
                        // record entity update on both relationship vertices
                        recordEntityUpdateForNonRelationsipAttribute(fromVertex);
                        recordEntityUpdateForNonRelationsipAttribute(toVertex);

                        recordEntityUpdate(toVertex, ctx, true);
                    }

                } else {
                    newEdge = relationshipStore.getRelationship(fromVertex, toVertex, relationship);
                    if (newEdge != null) {
                        recordEntityUpdate(toVertex, ctx, false);
                    }
                }
                ret = newEdge;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getEdgeUsingRelationship({})", ctx);
        }

        return ret;
    }

    private Map<String, Object> mapMapValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapMapValue({})", ctx);
        }

        Map<Object, Object> newVal      = (Map<Object, Object>) ctx.getValue();
        Map<String, Object> newMap      = new HashMap<>();
        AtlasMapType        mapType     = (AtlasMapType) ctx.getAttrType();
        AtlasAttribute      attribute   = ctx.getAttribute();
        Map<String, Object> currentMap  = getMapElementsProperty(mapType, ctx.getReferringVertex(), AtlasGraphUtilsV2.getEdgeLabel(ctx.getVertexProperty()), attribute);
        boolean             isReference = isReference(mapType.getValueType());
        boolean             isSoftReference = ctx.getAttribute().getAttributeDef().isSoftReferenced();
        String propertyName = ctx.getVertexProperty();

        if (PARTIAL_UPDATE.equals(ctx.getOp()) && attribute.getAttributeDef().isAppendOnPartialUpdate() && MapUtils.isNotEmpty(currentMap)) {
            if (MapUtils.isEmpty(newVal)) {
                newVal = new HashMap<>(currentMap);
            } else {
                Map<Object, Object> mergedVal = new HashMap<>(currentMap);

                for (Map.Entry<Object, Object> entry : newVal.entrySet()) {
                    String newKey = entry.getKey().toString();

                    mergedVal.put(newKey, entry.getValue());
                }

                newVal = mergedVal;
            }
        }

        boolean isNewValNull = newVal == null;

        if (isNewValNull) {
            newVal = new HashMap<>();
        }


        if (isReference) {
            for (Map.Entry<Object, Object> entry : newVal.entrySet()) {
                String    key          = entry.getKey().toString();
                AtlasEdge existingEdge = isSoftReference ? null : getEdgeIfExists(mapType, currentMap, key);

                AttributeMutationContext mapCtx =  new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), attribute, entry.getValue(),
                        propertyName, mapType.getValueType(), existingEdge);
                // Add/Update/Remove property value
                Object newEntry = mapCollectionElementsToVertex(mapCtx, context);

                if (!isSoftReference && newEntry instanceof AtlasEdge) {
                    AtlasEdge edge = (AtlasEdge) newEntry;

                    edge.setProperty(ATTRIBUTE_KEY_PROPERTY_KEY, key);

                    // If value type indicates this attribute is a reference, and the attribute has an inverse reference attribute,
                    // update the inverse reference value.
                    AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();

                    if (inverseRefAttribute != null) {
                        addInverseReference(context, inverseRefAttribute, edge, getRelationshipAttributes(ctx.getValue()));
                    }

                    updateInConsistentOwnedMapVertices(ctx, mapType, newEntry);

                    newMap.put(key, newEntry);
                }

                if (isSoftReference) {
                    newMap.put(key, newEntry);
                }
            }

            Map<String, Object> finalMap = removeUnusedMapEntries(attribute, ctx.getReferringVertex(), currentMap, newMap);
            newMap.putAll(finalMap);
        } else {
            // primitive type map
            if (isNewValNull) {
                ctx.getReferringVertex().setProperty(propertyName, null);
            } else {
                ctx.getReferringVertex().setProperty(propertyName, new HashMap<>(newVal));
            }
            newVal.forEach((key, value) -> newMap.put(key.toString(), value));
        }

        if (isSoftReference) {
            if (isNewValNull) {
                ctx.getReferringVertex().setProperty(propertyName,null);
            } else {
                ctx.getReferringVertex().setProperty(propertyName, new HashMap<>(newMap));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapMapValue({})", ctx);
        }

        return newMap;
    }

    public List mapArrayValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapArrayValue({})", ctx);
        }

        AtlasAttribute attribute           = ctx.getAttribute();
        List           newElements         = (List) ctx.getValue();
        AtlasArrayType arrType             = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      elementType         = arrType.getElementType();
        boolean        isStructType        = (TypeCategory.STRUCT == elementType.getTypeCategory()) ||
                (TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory());
        boolean        isReference         = isReference(elementType);
        boolean        isSoftReference     = ctx.getAttribute().getAttributeDef().isSoftReferenced();
        AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();
        Cardinality    cardinality         = attribute.getAttributeDef().getCardinality();
        List<AtlasEdge> removedElements    = new ArrayList<>();
        List<Object>   newElementsCreated  = new ArrayList<>();
        List<Object>   allArrayElements    = null;
        List<Object>   currentElements;
        boolean deleteExistingRelations = shouldDeleteExistingRelations(ctx, attribute);

        if (isReference && !isSoftReference) {
            currentElements = (List) getCollectionElementsUsingRelationship(ctx.getReferringVertex(), attribute, isStructType);
        } else {
            currentElements = (List) getArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty());
        }

        if (PARTIAL_UPDATE.equals(ctx.getOp()) && attribute.getAttributeDef().isAppendOnPartialUpdate() && CollectionUtils.isNotEmpty(currentElements)) {
            if (CollectionUtils.isEmpty(newElements)) {
                newElements = new ArrayList<>(currentElements);
            } else {
                List<Object> mergedVal = new ArrayList<>(currentElements);

                mergedVal.addAll(newElements);

                newElements = mergedVal;
            }
        }

        boolean isNewElementsNull = newElements == null;

        if (isNewElementsNull) {
            newElements = new ArrayList();
        }

        if (cardinality == SET) {
            newElements = (List) newElements.stream().distinct().collect(Collectors.toList());
        }

        for (int index = 0; index < newElements.size(); index++) {
            AtlasEdge               existingEdge = (isSoftReference) ? null : getEdgeAt(currentElements, index, elementType);
            AttributeMutationContext arrCtx      = new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), ctx.getAttribute(), newElements.get(index),
                    ctx.getVertexProperty(), elementType, existingEdge);
            if (deleteExistingRelations) {
                removeExistingRelationWithOtherVertex(arrCtx, ctx, context);
            }

            Object newEntry = mapCollectionElementsToVertex(arrCtx, context);
            if (isReference && newEntry != null && newEntry instanceof AtlasEdge && inverseRefAttribute != null) {
                // Update the inverse reference value.
                AtlasEdge newEdge = (AtlasEdge) newEntry;

                addInverseReference(context, inverseRefAttribute, newEdge, getRelationshipAttributes(ctx.getValue()));
            }

            // not null
            if(newEntry != null) {
                newElementsCreated.add(newEntry);
            }
        }

        if (isReference && !isSoftReference ) {
            boolean isAppendOnPartialUpdate = !isStructType ? getAppendOptionForRelationship(ctx.getReferringVertex(), attribute.getName()) : false;

            if (isAppendOnPartialUpdate) {
                allArrayElements = unionCurrentAndNewElements(attribute, (List) currentElements, (List) newElementsCreated);
            } else {
                removedElements = removeUnusedArrayEntries(attribute, (List) currentElements, (List) newElementsCreated, ctx);

                allArrayElements = unionCurrentAndNewElements(attribute, removedElements, (List) newElementsCreated);
            }
        } else {
            allArrayElements = newElementsCreated;
        }

        // add index to attributes of array type
        for (int index = 0; allArrayElements != null && index < allArrayElements.size(); index++) {
            Object element = allArrayElements.get(index);

            if ((element instanceof AtlasEdge)) {
                AtlasEdge edge = (AtlasEdge) element;
                if ((removedElements.contains(element)) && ((EDGE_LABELS_FOR_HARD_DELETION).contains(edge.getLabel()))) {
                    continue;
                }
                else {
                    AtlasGraphUtilsV2.setEncodedProperty((AtlasEdge) element, ATTRIBUTE_INDEX_PROPERTY_KEY, index);
                }
            }
        }

        if (isNewElementsNull) {
            setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(), null, null, cardinality);
        } else {
            // executes
            setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(), allArrayElements, currentElements, cardinality);
        }

        switch (ctx.getAttribute().getRelationshipEdgeLabel()) {
            case TERM_ASSIGNMENT_LABEL:
                addMeaningsToEntity(ctx, newElementsCreated, new ArrayList<>(0), false, currentElements);
                break;
            case CATEGORY_TERMS_EDGE_LABEL: addCategoriesToTermEntity(ctx, newElementsCreated, removedElements);
                break;

            case CATEGORY_PARENT_EDGE_LABEL: addCatParentAttr(ctx, newElementsCreated, removedElements);
                break;

            case PROCESS_INPUTS:
            case PROCESS_OUTPUTS: addEdgesToContext(GraphHelper.getGuid(ctx.referringVertex), newElementsCreated,  removedElements);
                break;

            case INPUT_PORT_PRODUCT_EDGE_LABEL:
            case OUTPUT_PORT_PRODUCT_EDGE_LABEL:
                addInternalProductAttr(ctx, newElementsCreated, removedElements, currentElements);
                break;

            case UD_RELATIONSHIP_EDGE_LABEL:
                validateCustomRelationship(ctx, newElementsCreated, false);
                break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapArrayValue({})", ctx);
        }

        return allArrayElements;
    }

    public List appendArrayValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapArrayValue({})", ctx);
        }


        AtlasAttribute attribute           = ctx.getAttribute();
        List           newElements         = (List) ctx.getValue();
        AtlasArrayType arrType             = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      elementType         = arrType.getElementType();
        boolean        isStructType        = (TypeCategory.STRUCT == elementType.getTypeCategory()) ||
                (TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory());
        boolean        isReference         = isReference(elementType);
        boolean        isSoftReference     = ctx.getAttribute().getAttributeDef().isSoftReferenced();
        AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();
        Cardinality    cardinality         = attribute.getAttributeDef().getCardinality();
        List<Object>   newElementsCreated  = new ArrayList<>();

        boolean isNewElementsNull = newElements == null;

        if (isNewElementsNull) {
            newElements = new ArrayList();
        }

        if (cardinality == SET) {
            newElements = (List) newElements.stream().distinct().collect(Collectors.toList());
        }


        for (int index = 0; index < newElements.size(); index++) {
            AttributeMutationContext arrCtx      = new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), ctx.getAttribute(), newElements.get(index),
                    ctx.getVertexProperty(), elementType);


            Object newEntry = getEdgeUsingRelationship(arrCtx, context, true);

            if (isReference && newEntry != null && newEntry instanceof AtlasEdge && inverseRefAttribute != null) {
                // Update the inverse reference value.
                AtlasEdge newEdge = (AtlasEdge) newEntry;

                addInverseReference(context, inverseRefAttribute, newEdge, getRelationshipAttributes(ctx.getValue()));
            }

            if(newEntry != null) {
                newElementsCreated.add(newEntry);
            }
        }

        // add index to attributes of array type
        for (int index = 0; newElements != null && index < newElements.size(); index++) {
            Object element = newElements.get(index);

            if (element instanceof AtlasEdge) {
                AtlasGraphUtilsV2.setEncodedProperty((AtlasEdge) element, ATTRIBUTE_INDEX_PROPERTY_KEY, index);
            }
        }

        if (isNewElementsNull) {
            setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(),  new ArrayList<>(0),  new ArrayList<>(0), cardinality);
        } else {
            setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(), newElements,  new ArrayList<>(0), cardinality);
        }

        if (CollectionUtils.isNotEmpty(newElementsCreated)
                && newElementsCreated.get(0) instanceof AtlasEdge
                ) {
            List<Map<String, String>> attrValues = new ArrayList<>();

            for (Object newItem : newElementsCreated) {
                if (getCreatedTime((AtlasEdge) newItem) == RequestContext.get().getRequestTime()) {
                    // Only process newly created edges
                    AtlasVertex inverseVertex = ((AtlasEdge) newItem).getInVertex();
                    Map<String, String> objectId = new HashMap<>();
                    objectId.put("typeName", getTypeName(inverseVertex));
                    objectId.put("guid", GraphHelper.getGuid(inverseVertex));
                    attrValues.add(objectId);
                }
            }

            AtlasEntity diffEntity = entityRetriever.getOrInitializeDiffEntity(ctx.getReferringVertex());
            diffEntity.setAddedRelationshipAttribute(attribute.getName(), attrValues);
        }

        switch (ctx.getAttribute().getRelationshipEdgeLabel()) {
            case TERM_ASSIGNMENT_LABEL:
                addMeaningsToEntity(ctx, newElementsCreated, new ArrayList<>(0), true, new ArrayList<>(0));
                break;

            case CATEGORY_TERMS_EDGE_LABEL: addCategoriesToTermEntity(ctx, newElementsCreated, new ArrayList<>(0));
                break;

            case CATEGORY_PARENT_EDGE_LABEL: addCatParentAttr(ctx, newElementsCreated, new ArrayList<>(0));
                break;

            case PROCESS_INPUTS:
            case PROCESS_OUTPUTS: addEdgesToContext(GraphHelper.getGuid(ctx.referringVertex), newElementsCreated,  new ArrayList<>(0));
                break;

            case INPUT_PORT_PRODUCT_EDGE_LABEL:
            case OUTPUT_PORT_PRODUCT_EDGE_LABEL:
                addInternalProductAttr(ctx, newElementsCreated, null, null);
                break;

            case UD_RELATIONSHIP_EDGE_LABEL:
                validateCustomRelationship(ctx, newElementsCreated, true);
                break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapArrayValue({})", ctx);
        }

        return newElementsCreated;
    }

    public List removeArrayValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeArrayValue({})", ctx);
        }

        AtlasAttribute attribute           = ctx.getAttribute();
        List           elementsDeleted         = (List) ctx.getValue();
        AtlasArrayType arrType             = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      elementType         = arrType.getElementType();
        boolean        isStructType        = (TypeCategory.STRUCT == elementType.getTypeCategory()) ||
                (TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory());
        Cardinality    cardinality         = attribute.getAttributeDef().getCardinality();
        List<AtlasEdge> removedElements    = new ArrayList<>();
        List<Object>   entityRelationsDeleted  = new ArrayList<>();


        boolean isNewElementsNull = elementsDeleted == null;

        if (isNewElementsNull) {
            elementsDeleted = new ArrayList();
        }

        if (cardinality == SET) {
            elementsDeleted = (List) elementsDeleted.stream().distinct().collect(Collectors.toList());
        }

        for (int index = 0; index < elementsDeleted.size(); index++) {
            AttributeMutationContext arrCtx      = new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), ctx.getAttribute(), elementsDeleted.get(index),
                    ctx.getVertexProperty(), elementType);

            Object deleteEntry =  getEdgeUsingRelationship(arrCtx, context, false);

            // avoid throwing error if relation does not exist but requested to remove
            if (deleteEntry == null) {
                LOG.warn("Relation does not exist for attribute {} for entity {}", attribute.getName(),
                        ctx.getReferringVertex());
            } else {
                entityRelationsDeleted.add(deleteEntry);
            }

        }

        removedElements = removeArrayEntries(attribute, (List)entityRelationsDeleted, ctx);

        if (CollectionUtils.isNotEmpty(removedElements)
                && removedElements.get(0) instanceof AtlasEdge) {

            List<Map<String, String>> attrValues = new ArrayList<>();

            for (Object removedItem : removedElements) {
                AtlasVertex inverseVertex = ((AtlasEdge) removedItem).getInVertex();
                Map<String, String> objectId = new HashMap<>();
                objectId.put("typeName", getTypeName(inverseVertex));
                objectId.put("guid", GraphHelper.getGuid(inverseVertex));
                attrValues.add(objectId);
            }

            AtlasEntity diffEntity = entityRetriever.getOrInitializeDiffEntity(ctx.getReferringVertex());
            diffEntity.setRemovedRelationshipAttribute(attribute.getName(), attrValues);
        }


        switch (ctx.getAttribute().getRelationshipEdgeLabel()) {
            case TERM_ASSIGNMENT_LABEL:
                addMeaningsToEntity(ctx, new ArrayList<>(0), removedElements, true, new ArrayList<>(0));
                break;
            case CATEGORY_TERMS_EDGE_LABEL: addCategoriesToTermEntity(ctx, new ArrayList<>(0), removedElements);
                break;

            case CATEGORY_PARENT_EDGE_LABEL: addCatParentAttr(ctx, new ArrayList<>(0), removedElements);
                break;

            case PROCESS_INPUTS:
            case PROCESS_OUTPUTS: addEdgesToContext(GraphHelper.getGuid(ctx.referringVertex), new ArrayList<>(0),  removedElements);
                break;

            case INPUT_PORT_PRODUCT_EDGE_LABEL:
            case OUTPUT_PORT_PRODUCT_EDGE_LABEL:
                addInternalProductAttr(ctx, null , removedElements, null);
                break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== removeArrayValue({})", ctx);
        }

        return entityRelationsDeleted;
    }

    private void addEdgesToContext(String guid, List<Object> newElementsCreated, List<AtlasEdge> removedElements) {

        if (newElementsCreated.size() > 0) {
            List<Object> elements = (RequestContext.get().getNewElementsCreatedMap()).get(guid);
            if (elements == null) {
                ArrayList newElements = new ArrayList<>();
                newElements.addAll(newElementsCreated);
                (RequestContext.get().getNewElementsCreatedMap()).put(guid, newElements);
            } else {
                elements.addAll(newElementsCreated);
                RequestContext.get().getNewElementsCreatedMap().put(guid, elements);
            }
        }

        if (removedElements.size() > 0) {
            List<Object> removedElement = (RequestContext.get().getRemovedElementsMap()).get(guid);

            if (removedElement == null) {
                removedElement = new ArrayList<>();
                removedElement.addAll(removedElements);
                (RequestContext.get().getRemovedElementsMap()).put(guid, removedElement);
            } else {
                removedElement.addAll(removedElements);
                (RequestContext.get().getRemovedElementsMap()).put(guid, removedElement);
            }
        }
    }

    public void validateCustomRelationship(AttributeMutationContext ctx, List<Object> newElements, boolean isAppend) throws AtlasBaseException {
        validateCustomRelationshipCount(ctx, newElements, isAppend);
        validateCustomRelationshipAttributes(ctx, newElements);
    }

    public void validateCustomRelationshipCount(AttributeMutationContext ctx, List<Object> newElements, boolean isAppend) throws AtlasBaseException {
        long currentSize;
        boolean isEdgeDirectionIn = ctx.getAttribute().getRelationshipEdgeDirection() == AtlasRelationshipEdgeDirection.IN;

        if (isAppend) {
            currentSize = ctx.getReferringVertex().getEdgesCount(isEdgeDirectionIn ? AtlasEdgeDirection.IN : AtlasEdgeDirection.OUT,
                    UD_RELATIONSHIP_EDGE_LABEL);
        } else {
            currentSize = newElements.size();
        }

        validateCustomRelationshipCount(currentSize, ctx.getReferringVertex());

        AtlasEdgeDirection direction;
        if (isEdgeDirectionIn) {
            direction = AtlasEdgeDirection.OUT;
        } else {
            direction = AtlasEdgeDirection.IN;
        }

        for (Object obj : newElements) {
            AtlasEdge edge = (AtlasEdge) obj;

            AtlasVertex targetVertex;
            if (isEdgeDirectionIn) {
                targetVertex = edge.getOutVertex();
            } else {
                targetVertex = edge.getInVertex();
            }

            currentSize = targetVertex.getEdgesCount(direction, UD_RELATIONSHIP_EDGE_LABEL);
            validateCustomRelationshipCount(currentSize, targetVertex);
        }
    }

    public void validateCustomRelationshipAttributes(AttributeMutationContext ctx, List<Object> newElements) throws AtlasBaseException {
        List<AtlasRelatedObjectId> customRelationships = (List<AtlasRelatedObjectId>) ctx.getValue();

        if (CollectionUtils.isNotEmpty(customRelationships)) {
            for (AtlasObjectId objectId : customRelationships) {
                if (objectId instanceof AtlasRelatedObjectId) {
                    AtlasRelatedObjectId relatedObjectId = (AtlasRelatedObjectId) objectId;
                    if (relatedObjectId.getRelationshipAttributes() != null) {
                        validateCustomRelationshipAttributeValueCase(relatedObjectId.getRelationshipAttributes().getAttributes());
                    }
                }
            }
        }
    }

    public static void validateCustomRelationshipAttributeValueCase(Map<String, Object> attributes) throws AtlasBaseException {
        if (MapUtils.isEmpty(attributes)) {
            return;
        }

        for (String key : attributes.keySet()) {
            if (key.equals("toTypeLabel") || key.equals("fromTypeLabel")) {
                String value = (String) attributes.get(key);

                if (StringUtils.isNotEmpty(value)) {
                    StringBuilder finalValue = new StringBuilder();

                    finalValue.append(Character.toUpperCase(value.charAt(0)));
                    String sub = value.substring(1);
                    if (StringUtils.isNotEmpty(sub)) {
                        finalValue.append(sub.toLowerCase());
                    }

                    attributes.put(key, finalValue.toString());
                }
            }
        }
    }

    public static void validateCustomRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex) throws AtlasBaseException {
        long currentSize = end1Vertex.getEdgesCount(AtlasEdgeDirection.OUT, UD_RELATIONSHIP_EDGE_LABEL) + 1;
        validateCustomRelationshipCount(currentSize, end1Vertex);

        currentSize = end2Vertex.getEdgesCount(AtlasEdgeDirection.IN, UD_RELATIONSHIP_EDGE_LABEL) + 1;
        validateCustomRelationshipCount(currentSize, end2Vertex);
    }

    private static void validateCustomRelationshipCount(long size, AtlasVertex vertex) throws AtlasBaseException {
        if (UD_REL_THRESHOLD < size) {
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED,
                    "Custom relationships size is more than " + UD_REL_THRESHOLD + ", current is " + size + " for " + vertex.getProperty(NAME, String.class));
        }
    }

    private void validateElasticsearchKeywordSize(AtlasBusinessAttribute bmAttribute, Object attrValue, String fieldName, List<String> messages) {
        if (!isKeywordMappedAttribute(bmAttribute)) {
            return;
        }

        if (attrValue == null) {
            return;
        }

        try {
            String valueAsString = attrValue.toString();
            byte[] valueBytes = valueAsString.getBytes(UTF8_CHARSET);

            if (valueBytes.length > ELASTICSEARCH_KEYWORD_MAX_BYTES) {
                messages.add(String.format("%s: Business attribute value exceeds Elasticsearch keyword limit. " +
                                "Size: %d bytes, Limit: %d bytes (32KB). Consider using a different field type or reducing the value size.",
                        fieldName, valueBytes.length, ELASTICSEARCH_KEYWORD_MAX_BYTES));
            }
        } catch (Exception e) {
            LOG.warn("Failed to validate Elasticsearch keyword size for field: {}", fieldName, e);
            messages.add(String.format("%s: Failed to validate Elasticsearch keyword size due to an error: %s",
                    fieldName, e.getMessage()));

        }
    }

    private boolean isKeywordMappedAttribute(AtlasBusinessAttribute bmAttribute) {
        AtlasAttributeDef attributeDef = bmAttribute.getAttributeDef();
        AtlasType attrType = bmAttribute.getAttributeType();

        if (attrType instanceof AtlasBuiltInTypes.AtlasStringType || attrType instanceof AtlasEnumType) {
            return true;
        }

        if (AtlasAttributeDef.IndexType.STRING == attributeDef.getIndexType()) {
            return true;
        }

        return false;
    }

    private void addInternalProductAttr(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements, List<Object> currentElements) throws AtlasBaseException {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addInternalProductAttrForAppend");
        AtlasVertex toVertex = ctx.getReferringVertex();
        String toVertexType = getTypeName(toVertex);

        if (CollectionUtils.isEmpty(createdElements) && CollectionUtils.isEmpty(deletedElements)){
            RequestContext.get().endMetricRecord(metricRecorder);
            return;
        }

        if (TYPE_PRODUCT.equals(toVertexType)) {
            String attrName = ctx.getAttribute().getRelationshipEdgeLabel().equals(OUTPUT_PORT_PRODUCT_EDGE_LABEL)
                    ? OUTPUT_PORT_GUIDS_ATTR
                    : INPUT_PORT_GUIDS_ATTR;

            addOrRemoveDaapInternalAttr(toVertex, attrName, createdElements, deletedElements, currentElements);
        }else{
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Can not update product relations while updating any asset");
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void addOrRemoveDaapInternalAttr(AtlasVertex toVertex, String internalAttr, List<Object> createdElements, List<AtlasEdge> deletedElements, List<Object> currentElements) throws AtlasBaseException {
        List<String> addedGuids = new ArrayList<>();
        List<String> removedGuids = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(createdElements)) {
            addedGuids = createdElements.stream().map(x -> ((AtlasEdge) x).getOutVertex().getProperty("__guid", String.class)).collect(Collectors.toList());
            addedGuids.forEach(guid -> AtlasGraphUtilsV2.addEncodedProperty(toVertex, internalAttr, guid));
        }

        if (CollectionUtils.isNotEmpty(deletedElements)) {
            removedGuids = deletedElements.stream().map(x -> x.getOutVertex().getProperty("__guid", String.class)).collect(Collectors.toList());
            removedGuids.forEach(guid -> AtlasGraphUtilsV2.removeItemFromListPropertyValue(toVertex, internalAttr, guid));
        }

        // Add more info to outputPort update event.
        if (internalAttr.equals(OUTPUT_PORT_GUIDS_ATTR)) {
            List<String> conflictingGuids = fetchConflictingGuids(toVertex, addedGuids);

            // When adding assets as outputPort, remove them from inputPorts if they already exist there.
            if (CollectionUtils.isNotEmpty(conflictingGuids)) {
                try {
                    removeInputPortReferences(toVertex, conflictingGuids);
                } catch (AtlasBaseException e) {
                    throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Failed to remove input port edges for conflicting GUIDs: " + conflictingGuids, String.valueOf(e));
                }
            }

            if (CollectionUtils.isNotEmpty(currentElements)) {
                List<String> currentElementGuids = currentElements.stream()
                        .filter(x -> ((AtlasEdge) x).getProperty(STATE_PROPERTY_KEY, String.class).equals("ACTIVE"))
                        .map(x -> ((AtlasEdge) x).getOutVertex().getProperty("__guid", String.class))
                        .collect(Collectors.toList());

                addedGuids = addedGuids.stream()
                        .filter(guid -> !currentElementGuids.contains(guid))
                        .collect(Collectors.toList());
            }

            String productGuid = toVertex.getProperty("__guid", String.class);
            AtlasEntity diffEntity = RequestContext.get().getDifferentialEntity(productGuid);
            if (diffEntity != null){
                // make change to not add following attributes to diff entity if they are empty
                diffEntity.setAddedRelationshipAttribute(OUTPUT_PORTS, addedGuids);
                diffEntity.setRemovedRelationshipAttribute(OUTPUT_PORTS, removedGuids);
            }
        } else if (internalAttr.equals(INPUT_PORT_GUIDS_ATTR)) {
            //  When adding assets as inputPort, fail if they already exist as outputPorts.
            validateInputPortUpdate(toVertex, addedGuids);
        }
    }

    private List<String> fetchConflictingGuids(AtlasVertex toVertex, List<String> addedGuids) {
        List<String> conflictingGuids = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(addedGuids)) {
            List<String> existingInputPortGuids = toVertex.getMultiValuedProperty(INPUT_PORT_GUIDS_ATTR, String.class);
            if (CollectionUtils.isNotEmpty(existingInputPortGuids)) {
                conflictingGuids = addedGuids.stream()
                        .filter(existingInputPortGuids::contains)
                        .collect(Collectors.toList());
            }
        }
        return conflictingGuids;
    }

    private void removeInputPortReferences(AtlasVertex toVertex, List<String> conflictingGuids) throws AtlasBaseException {
        for (String assetGuid: conflictingGuids) {
            AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(this.graph, assetGuid);
            if (assetVertex == null) {
                LOG.warn("Asset vertex not found for GUID: {}, skipping edge removal", assetGuid);
                continue;
            }

            Iterator<AtlasEdge> outEdges = assetVertex.getEdges(AtlasEdgeDirection.OUT, INPUT_PORT_PRODUCT_EDGE_LABEL).iterator();
            while(outEdges.hasNext()) {
                AtlasEdge edge = outEdges.next();
                if (edge.getInVertex().equals(toVertex)) {
                    deleteDelegate.getHandler(DeleteType.HARD).deleteEdgeReference(edge, TypeCategory.ENTITY, true,
                            true, AtlasRelationshipEdgeDirection.OUT, assetVertex);
                }
            }
        }

        conflictingGuids.forEach(guid ->
                AtlasGraphUtilsV2.removeItemFromListPropertyValue(toVertex, INPUT_PORT_GUIDS_ATTR, guid));
    }

    private void validateInputPortUpdate(AtlasVertex toVertex, List<String> addedGuids) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(addedGuids)) {
            List<String> existingOutputPortGuids = toVertex.getMultiValuedProperty(OUTPUT_PORT_GUIDS_ATTR, String.class);
            if (CollectionUtils.isNotEmpty(existingOutputPortGuids)) {
                List<String> conflictingGuids = addedGuids.stream()
                        .filter(existingOutputPortGuids::contains)
                        .collect(Collectors.toList());

                if (CollectionUtils.isNotEmpty(conflictingGuids)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Cannot add input ports that already exist as output ports: " + conflictingGuids);
                }
            }
        }
    }

    private boolean shouldDeleteExistingRelations(AttributeMutationContext ctx, AtlasAttribute attribute) {
        boolean ret = false;
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(AtlasGraphUtilsV2.getTypeName(ctx.getReferringVertex()));
        if (entityType !=null && entityType.hasRelationshipAttribute(attribute.getName())) {
            AtlasRelationshipDef relationshipDef = typeRegistry.getRelationshipDefByName(ctx.getAttribute().getRelationshipName());
            ret = !(relationshipDef.getEndDef1().getCardinality() == SET && relationshipDef.getEndDef2().getCardinality() == SET);
        }
        return ret;
    }

    /*
     * Before creating new edges between referring vertex & new vertex coming from array,
     * delete old relationship with same relationship type between new vertex coming from array & any other vertex.
     * e.g
     *   table_a has columns as col_0 & col_1
     *   create new table_b add columns col_0 & col_1
     *   Now creating new relationships between table_b -> col_0 & col_1
     *   This should also delete existing relationships between table_a -> col_0 & col_1
     *   this behaviour is needed because endDef1 has SINGLE cardinality
     *
     * This method will delete existing edges.
     * Skip if both ends are of SET cardinality, e.g. Catalog.inputs, Catalog.outputs
     * */
    private void removeExistingRelationWithOtherVertex(AttributeMutationContext arrCtx, AttributeMutationContext ctx,
                                                       EntityMutationContext context) throws AtlasBaseException {
        MetricRecorder metric = RequestContext.get().startMetricRecord("removeExistingRelationWithOtherVertex");

        AtlasObjectId entityObject = (AtlasObjectId) arrCtx.getValue();
        String entityGuid = entityObject.getGuid();

        AtlasVertex referredVertex = null;

        if (StringUtils.isNotEmpty(entityGuid)) {
            referredVertex = context.getVertex(entityGuid);
        }

        if (referredVertex == null) {
            try {
                if (StringUtils.isNotEmpty(entityGuid)) {
                    referredVertex = entityRetriever.getEntityVertex(((AtlasObjectId) arrCtx.getValue()).getGuid());
                } else {
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityObject.getTypeName());
                    if (entityType != null && MapUtils.isNotEmpty(entityObject.getUniqueAttributes())) {
                        referredVertex = AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, entityObject.getUniqueAttributes());
                    }
                }
            } catch (AtlasBaseException e) {
                //in case if importing zip, referredVertex might not have been create yet
                //e.g. importing zip with db & its tables, while processing db edges, tables vertices are not yet created
                LOG.warn("removeExistingRelationWithOtherVertex - vertex not found!", e);
            }
        }

        if (referredVertex != null) {
            Iterator<AtlasEdge> edgeIterator = referredVertex.getEdges(getInverseEdgeDirection(
                    arrCtx.getAttribute().getRelationshipEdgeDirection()), ctx.getAttribute().getRelationshipEdgeLabel()).iterator();

            while (edgeIterator.hasNext()) {
                AtlasEdge existingEdgeToReferredVertex = edgeIterator.next();

                if (existingEdgeToReferredVertex != null && getStatus(existingEdgeToReferredVertex) != DELETED) {
                    AtlasVertex referredVertexToExistingEdge;
                    if (arrCtx.getAttribute().getRelationshipEdgeDirection().equals(IN)) {
                        referredVertexToExistingEdge = existingEdgeToReferredVertex.getInVertex();
                    } else {
                        referredVertexToExistingEdge = existingEdgeToReferredVertex.getOutVertex();
                    }

                    if (!arrCtx.getReferringVertex().equals(referredVertexToExistingEdge)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Delete existing relation");
                        }

                        deleteDelegate.getHandler().deleteEdgeReference(existingEdgeToReferredVertex, ctx.getAttrType().getTypeCategory(),
                                ctx.getAttribute().isOwnedRef(), true, ctx.getAttribute().getRelationshipEdgeDirection(), ctx.getReferringVertex());
                    }
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);
    }

    private AtlasEdgeDirection getInverseEdgeDirection(AtlasRelationshipEdgeDirection direction) {
        switch (direction) {
            case IN: return AtlasEdgeDirection.OUT;
            case OUT: return AtlasEdgeDirection.IN;
            default: return AtlasEdgeDirection.BOTH;
        }
    }

    private void addGlossaryAttr(AttributeMutationContext ctx, AtlasEdge edge) {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addGlossaryAttr");
        AtlasVertex toVertex = ctx.getReferringVertex();
        String toVertexType = getTypeName(toVertex);

        if (TYPE_TERM.equals(toVertexType) || TYPE_CATEGORY.equals(toVertexType)) {
            // handle __glossary attribute of term or category entity
            String gloQname = edge.getOutVertex().getProperty(QUALIFIED_NAME, String.class);
            AtlasGraphUtilsV2.setEncodedProperty(toVertex, GLOSSARY_PROPERTY_KEY, gloQname);
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void addCatParentAttr(AttributeMutationContext ctx, AtlasEdge edge) {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addCatParentAttr");
        AtlasVertex toVertex = ctx.getReferringVertex();
        String toVertexType = getTypeName(toVertex);

        if (TYPE_CATEGORY.equals(toVertexType)) {
            if (edge == null) {
                toVertex.removeProperty(CATEGORIES_PARENT_PROPERTY_KEY);

            } else {
                //add __parentCategory attribute of category entity
                String parentQName = edge.getOutVertex().getProperty(QUALIFIED_NAME, String.class);
                AtlasGraphUtilsV2.setEncodedProperty(toVertex, CATEGORIES_PARENT_PROPERTY_KEY, parentQName);
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void removeAttrForCategoryDelete(Collection<AtlasVertex> categories) {
        for (AtlasVertex vertex : categories) {
            Iterator<AtlasEdge> edgeIterator = vertex.getEdges(AtlasEdgeDirection.OUT, CATEGORY_PARENT_EDGE_LABEL).iterator();
            while (edgeIterator.hasNext()) {
                AtlasEdge childEdge = edgeIterator.next();
                AtlasEntity.Status edgeStatus = getStatus(childEdge);
                if (ACTIVE.equals(edgeStatus)) {
                    childEdge.getInVertex().removeProperty(CATEGORIES_PARENT_PROPERTY_KEY);
                }
            }

            String catQualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
            edgeIterator = vertex.getEdges(AtlasEdgeDirection.OUT, CATEGORY_TERMS_EDGE_LABEL).iterator();
            while (edgeIterator.hasNext()) {
                AtlasEdge termEdge = edgeIterator.next();
                termEdge.getInVertex().removePropertyValue(CATEGORIES_PROPERTY_KEY, catQualifiedName);
            }

        }
    }

    private void addCatParentAttr(AttributeMutationContext ctx, List<Object> newElementsCreated, List<AtlasEdge> removedElements) {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addCatParentAttr_1");
        AtlasVertex toVertex = ctx.getReferringVertex();

        //add __parentCategory attribute of child category entities
        if (CollectionUtils.isNotEmpty(newElementsCreated)) {
            String parentQName = toVertex.getProperty(QUALIFIED_NAME, String.class);
            List<AtlasVertex> catVertices = newElementsCreated.stream().map(x -> ((AtlasEdge) x).getInVertex()).collect(Collectors.toList());
            catVertices.stream().forEach(v -> AtlasGraphUtilsV2.setEncodedProperty(v, CATEGORIES_PARENT_PROPERTY_KEY, parentQName));
        }

        if (CollectionUtils.isNotEmpty(removedElements)) {
            List<AtlasVertex> termVertices = removedElements.stream().map(x -> x.getInVertex()).collect(Collectors.toList());
            termVertices.stream().forEach(v -> v.removeProperty(CATEGORIES_PROPERTY_KEY));
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void addCategoriesToTermEntity(AttributeMutationContext ctx, List<Object> newElementsCreated, List<AtlasEdge> removedElements) {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addCategoriesToTermEntity");
        AtlasVertex termVertex = ctx.getReferringVertex();

        if (TYPE_CATEGORY.equals(getTypeName(termVertex))) {
            String catQName = ctx.getReferringVertex().getProperty(QUALIFIED_NAME, String.class);

            if (CollectionUtils.isNotEmpty(newElementsCreated)) {
                List<AtlasVertex> termVertices = newElementsCreated.stream().map(x -> ((AtlasEdge) x).getInVertex()).collect(Collectors.toList());
                termVertices.stream().forEach(v -> AtlasGraphUtilsV2.addEncodedProperty(v, CATEGORIES_PROPERTY_KEY, catQName));
            }

            if (CollectionUtils.isNotEmpty(removedElements)) {
                List<AtlasVertex> termVertices = removedElements.stream().map(x -> x.getInVertex()).collect(Collectors.toList());
                termVertices.stream().forEach(v -> AtlasGraphUtilsV2.removeItemFromListPropertyValue(v, CATEGORIES_PROPERTY_KEY, catQName));
            }
        }

        if (TYPE_TERM.equals(getTypeName(termVertex))) {
            List<AtlasVertex> categoryVertices = newElementsCreated.stream().map(x -> ((AtlasEdge)x).getOutVertex()).collect(Collectors.toList());
            Set<String> catQnames = categoryVertices.stream().map(x -> x.getProperty(QUALIFIED_NAME, String.class)).collect(Collectors.toSet());

            termVertex.removeProperty(CATEGORIES_PROPERTY_KEY);
            catQnames.stream().forEach(q -> AtlasGraphUtilsV2.addEncodedProperty(termVertex, CATEGORIES_PROPERTY_KEY, q));
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    /**
     * Add meanings to entity relations.
     * @param ctx
     * @param createdElements
     * @param deletedElements
     *
     * Notes : This case exists when user requests to add a meaning to multiple assets at a time.
     * TODO: Term.relationshipAttributes.assignedEntities case is not handled
     *
     */
    private void addMeaningsToEntityRelations(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements, List<Object> currentElements) throws AtlasBaseException {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addMeaningsToEntityRelations");
        try {
            if (!RequestContext.get().isSkipAuthorizationCheck()) {
                try {
                    verifyMeaningsAuthorization(ctx, createdElements, deletedElements, currentElements);
                } catch (AtlasBaseException e) {
                    throw e;
                }
            }

            List<AtlasVertex> assignedEntitiesVertices ;
            List<String> currentMeaningsQNames ;
            String newMeaningName = ctx.referringVertex.getProperty(NAME, String.class);
            String newMeaningPropertyKey = ctx.referringVertex.getProperty(QUALIFIED_NAME, String.class);

            // createdElements = all the entities user passes in assignedEntities
            // it may contain entities which are already assigned to the relations
            if (!createdElements.isEmpty()) {

                assignedEntitiesVertices = createdElements.stream()
                        .filter(Objects::nonNull)
                        .map(x -> ((AtlasEdge) x).getInVertex())
                        .filter(x -> ACTIVE.name().equals(x.getProperty(STATE_PROPERTY_KEY, String.class)))
                        .collect(Collectors.toList());

                for (AtlasVertex relationVertex : assignedEntitiesVertices) {
                    currentMeaningsQNames = relationVertex.getMultiValuedProperty(MEANINGS_PROPERTY_KEY, String.class);

                    if (!currentMeaningsQNames.contains(newMeaningPropertyKey)) {
                        addMeaningsAttributes(relationVertex, newMeaningPropertyKey, newMeaningName);
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(deletedElements)) {
                List<AtlasVertex> relations = deletedElements.stream().map(x -> x.getInVertex())
                        .collect(Collectors.toList());
                relations.forEach(relationVertex -> {
                    removeMeaningsAttribute(relationVertex, MEANINGS_PROPERTY_KEY, newMeaningPropertyKey);
                    removeMeaningsAttribute(relationVertex, MEANINGS_TEXT_PROPERTY_KEY, newMeaningName);
                    removeMeaningsAttribute(relationVertex, MEANING_NAMES_PROPERTY_KEY, newMeaningName);
                });
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void addMeaningsAttributes(AtlasVertex atlasVertex, String newMeaningPropertyKey, String newMeaningName){
        addMeaningsAttribute(atlasVertex, MEANINGS_PROPERTY_KEY, newMeaningPropertyKey);
        addMeaningsAttribute(atlasVertex, MEANINGS_TEXT_PROPERTY_KEY, newMeaningName);
        addMeaningsAttribute(atlasVertex, MEANING_NAMES_PROPERTY_KEY, newMeaningName);
    }

    private void addMeaningsToEntity(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements, boolean isAppend, List<Object> currentElements) throws AtlasBaseException {
        if (ctx.getReferringVertex().getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class).equals(ATLAS_GLOSSARY_TERM_ENTITY_TYPE) && isAppend) {
            addMeaningsToEntityRelations(ctx, createdElements, deletedElements, currentElements);
        } else {
            addMeaningsToEntityV1(ctx, createdElements, deletedElements, isAppend, currentElements);
        }
    }

    /***
     * Add meanings to entity.
     * @param ctx
     * @param createdElements
     * @param deletedElements
     * @param isAppend
     *
     * Notes : This case exists when user requests to add multiple meanings to an asset at a time.
     */
    private void addMeaningsToEntityV1(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements, boolean isAppend, List<Object> currentElements) throws AtlasBaseException {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addMeaningsToEntityV1");
        try {
            if (!RequestContext.get().isSkipAuthorizationCheck()) {
                try {
                    verifyMeaningsAuthorization(ctx, createdElements, deletedElements, currentElements);
                } catch (AtlasBaseException e) {
                    throw e;
                }
            }

            // handle __terms attribute of entity
            List<AtlasVertex> meanings = createdElements.stream()
                    .map(x -> ((AtlasEdge) x).getOutVertex())
                    .filter(x -> ACTIVE.name().equals(x.getProperty(STATE_PROPERTY_KEY, String.class)))
                    .collect(Collectors.toList());

            List<String> currentMeaningsQNames = ctx.getReferringVertex().getMultiValuedProperty(MEANINGS_PROPERTY_KEY,String.class);
            Set<String> qNames = meanings.stream().map(x -> x.getProperty(QUALIFIED_NAME, String.class)).collect(Collectors.toSet());
            List<String> names = meanings.stream().map(x -> x.getProperty(NAME, String.class)).collect(Collectors.toList());

            List<String> deletedMeaningsNames = deletedElements.stream().map(x -> x.getOutVertex())
                    . map(x -> x.getProperty(NAME,String.class))
                    .collect(Collectors.toList());

            // Extract qualified names of deleted meanings for removal from __meanings
            List<String> deletedMeaningsQNames = deletedElements.stream()
                    .map(x -> x.getOutVertex())
                    .map(x -> x.getProperty(QUALIFIED_NAME, String.class))
                    .collect(Collectors.toList());

            List<String> newMeaningsNames = meanings.stream()
                    .filter(x -> !currentMeaningsQNames.contains(x.getProperty(QUALIFIED_NAME,String.class)))
                    .map(x -> x.getProperty(NAME, String.class))
                    .collect(Collectors.toList());

            if (!isAppend){
                ctx.getReferringVertex().removeProperty(MEANINGS_PROPERTY_KEY);
                ctx.getReferringVertex().removeProperty(MEANINGS_TEXT_PROPERTY_KEY);
                ctx.getReferringVertex().removeProperty(MEANING_NAMES_PROPERTY_KEY);
            }

            if (CollectionUtils.isNotEmpty(qNames)) {
                qNames.forEach(q -> AtlasGraphUtilsV2.addEncodedProperty(ctx.getReferringVertex(), MEANINGS_PROPERTY_KEY, q));
            }

            // Remove deleted meanings from __meanings when in append mode
            if (isAppend && CollectionUtils.isNotEmpty(deletedMeaningsQNames)) {
                LOG.info("Removing {} deleted meanings from vertex {}", deletedMeaningsQNames.size(), ctx.getReferringVertex().getId());
                deletedMeaningsQNames.forEach(q -> AtlasGraphUtilsV2.removeItemFromListPropertyValue(ctx.getReferringVertex(), MEANINGS_PROPERTY_KEY, q));
            }
            
            // Update __meaningNames based on mode (must be done BEFORE updating __meaningsText)
            if (!isAppend) {
                // Full replace mode: add all names (already cleared above)
                if (CollectionUtils.isNotEmpty(names)) {
                    names.forEach(name -> AtlasGraphUtilsV2.addListProperty(ctx.getReferringVertex(), MEANING_NAMES_PROPERTY_KEY, name, true));
                }
            } else {
                // Append mode: add only new names
                if (CollectionUtils.isNotEmpty(newMeaningsNames)) {
                    newMeaningsNames.forEach(name -> AtlasGraphUtilsV2.addListProperty(ctx.getReferringVertex(), MEANING_NAMES_PROPERTY_KEY, name, true));
                }
                
                // Remove deleted names in append mode
                if (CollectionUtils.isNotEmpty(deletedMeaningsNames)) {
                    deletedMeaningsNames.forEach(name -> AtlasGraphUtilsV2.removeItemFromListPropertyValue(ctx.getReferringVertex(), MEANING_NAMES_PROPERTY_KEY, name));
                }
                
                // Remove-only operation: clear property if no meanings remain
                if (createdElements.isEmpty() && CollectionUtils.isNotEmpty(deletedElements)) {
                    List<String> remainingMeanings = ctx.getReferringVertex().getMultiValuedProperty(MEANINGS_PROPERTY_KEY, String.class);
                    if (CollectionUtils.isEmpty(remainingMeanings)) {
                        ctx.getReferringVertex().removeProperty(MEANING_NAMES_PROPERTY_KEY);
                    }
                }
            }

            // Update __meaningsText based on final state (must be done AFTER __meaningNames is updated)
            updateMeaningsTextProperty(ctx, isAppend, names, deletedMeaningsNames, qNames, deletedMeaningsQNames);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    /**
     * Updates the __meaningsText property on the referring vertex based on the meanings changes.
     * This method delegates to {@link MeaningsTextPropertyUpdater} for the actual implementation.
     *
     * @param ctx                      The attribute mutation context containing the referring vertex
     * @param isAppend                 Whether the operation is in append mode or full replace mode
     * @param newMeaningNames          List of names of newly added meanings
     * @param deletedMeaningsNames     List of names of deleted meanings
     * @param newMeaningsQNames        Set of qualified names of newly added meanings
     * @param deletedMeaningsQNames    List of qualified names of deleted meanings
     */
    @VisibleForTesting
    void updateMeaningsTextProperty(AttributeMutationContext ctx, boolean isAppend,
                                           List<String> newMeaningNames, List<String> deletedMeaningsNames,
                                           Set<String> newMeaningsQNames, List<String> deletedMeaningsQNames) {
        new MeaningsTextPropertyUpdater().update(ctx, isAppend, newMeaningNames, deletedMeaningsNames, 
                                                  newMeaningsQNames, deletedMeaningsQNames);
    }

    private void verifyMeaningsAuthorization(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements, List<Object> currentElements) throws AtlasBaseException {
        AtlasVertex targetEntityVertex = ctx.getReferringVertex();
        AtlasEntityHeader targetEntityHeader = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(targetEntityVertex);

        try {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, targetEntityHeader),
                    "update on entity: " + targetEntityHeader.getDisplayText());
        } catch(AtlasBaseException e) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "update on entity: " + targetEntityHeader.getDisplayText());
        }

        boolean isGlossaryTermContext = ATLAS_GLOSSARY_TERM_ENTITY_TYPE.equals(targetEntityVertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class));

        List<Object> changedEntities;

        if (CollectionUtils.isNotEmpty(currentElements)) {
            Set<Object> createdSet = new HashSet<>(createdElements);
            Set<Object> currentSet = new HashSet<>(currentElements);

            changedEntities = Stream.concat(
                    createdSet.stream().filter(e -> !currentSet.contains(e)),
                    currentSet.stream().filter(e -> !createdSet.contains(e))
            ).collect(Collectors.toList());

            changedEntities = changedEntities.stream()
                    .filter(Objects::nonNull)
                    .filter(edge -> {
                        try {
                            return ACTIVE.equals(GraphHelper.getStatus((AtlasEdge)edge));
                        } catch (Exception e) {
                            LOG.warn("Failed to get status for edge: {}", edge, e);
                            return false;
                        }
                    })
                    .collect(Collectors.toList());
        } else {
            changedEntities = new ArrayList<>(createdElements);
        }

        if (CollectionUtils.isNotEmpty(changedEntities)) {
            for (Object element : changedEntities) {
                AtlasEdge edge = (AtlasEdge) element;
                
                if (isGlossaryTermContext) {
                    AtlasVertex targetAssetVertex = edge.getInVertex();
                    AtlasEntityHeader targetAssetHeader = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(targetAssetVertex);

                    try {
                        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, targetAssetHeader),
                                "update on entity: " + targetAssetHeader.getDisplayText());
                    } catch(AtlasBaseException e) {
                        throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "update on entity: " + targetAssetHeader.getDisplayText());
                    }
                } else {
                    AtlasVertex termVertex = edge.getOutVertex();
                    AtlasEntityHeader termEntityHeader = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(termVertex);

                    try {
                        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, termEntityHeader),
                                "update on entity: " + termEntityHeader.getDisplayText());
                    } catch(AtlasBaseException e) {
                        throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "update on entity: " + termEntityHeader.getDisplayText());
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(deletedElements)) {
            for (AtlasEdge edge : deletedElements) {
                if (isGlossaryTermContext) {
                    AtlasVertex targetAssetVertex = edge.getInVertex();
                    AtlasEntityHeader targetAssetHeader = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(targetAssetVertex);

                    try {
                        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, targetAssetHeader),
                                "update on entity: " + targetAssetHeader.getDisplayText());
                    } catch(AtlasBaseException e) {
                        throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "update on entity: " + targetAssetHeader.getDisplayText());
                    }
                } else {
                    AtlasVertex termVertex = edge.getOutVertex();
                    AtlasEntityHeader termEntityHeader = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(termVertex);

                    try {
                        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, termEntityHeader),
                                "update on entity: " + termEntityHeader.getDisplayText());
                    } catch(AtlasBaseException e) {
                        throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "update on entity: " + termEntityHeader.getDisplayText());
                    }
                }
            }
        }
    }

    private void addMeaningsAttribute(AtlasVertex vertex, String propName, String propValue) {
        if (MEANINGS_PROPERTY_KEY.equals(propName)) {
            AtlasGraphUtilsV2.addEncodedProperty(vertex, propName, propValue);

        } else if (MEANINGS_TEXT_PROPERTY_KEY.equals(propName)) {
            String names = AtlasGraphUtilsV2.getProperty(vertex, MEANINGS_TEXT_PROPERTY_KEY, String.class);

            if (org.apache.commons.lang3.StringUtils.isNotEmpty(names)) {
                propValue = propValue + "," + names;
            }

            AtlasGraphUtilsV2.setEncodedProperty(vertex, MEANINGS_TEXT_PROPERTY_KEY, propValue);
        } else if (MEANING_NAMES_PROPERTY_KEY.equals(propName)){

            AtlasGraphUtilsV2.addListProperty(vertex, MEANING_NAMES_PROPERTY_KEY, propValue,true);
        }
    }

    private void removeMeaningsAttribute(AtlasVertex vertex, String propName, String propValue) {
        if (MEANINGS_PROPERTY_KEY.equals(propName) || CATEGORIES_PROPERTY_KEY.equals(propName)) {
            AtlasGraphUtilsV2.removeItemFromListPropertyValue(vertex, propName, propValue);

        }  else if (MEANINGS_TEXT_PROPERTY_KEY.equals(propName)) {
            String names = AtlasGraphUtilsV2.getProperty(vertex, propName, String.class);

            if (org.apache.commons.lang.StringUtils.isNotEmpty(names)){
                List<String> nameList = new ArrayList<>(Arrays.asList(names.split(",")));
                Iterator<String> iterator = nameList.iterator();
                while (iterator.hasNext()) {
                    if (propValue.equals(iterator.next())) {
                        iterator.remove();
                        break;
                    }
                }
                AtlasGraphUtilsV2.setEncodedProperty(vertex, propName, org.apache.commons.lang3.StringUtils.join(nameList, ","));
            }
        } else if (MEANING_NAMES_PROPERTY_KEY.equals(propName)){
            AtlasGraphUtilsV2.removeItemFromListPropertyValue(vertex, MEANING_NAMES_PROPERTY_KEY, propValue);

        }
    }


    private boolean getAppendOptionForRelationship(AtlasVertex entityVertex, String relationshipAttributeName) {
        boolean                             ret                       = false;
        String                              entityTypeName            = AtlasGraphUtilsV2.getTypeName(entityVertex);
        AtlasEntityDef                      entityDef                 = typeRegistry.getEntityDefByName(entityTypeName);
        List<AtlasRelationshipAttributeDef> relationshipAttributeDefs = entityDef.getRelationshipAttributeDefs();

        if (CollectionUtils.isNotEmpty(relationshipAttributeDefs)) {
            ret = relationshipAttributeDefs.stream().anyMatch(relationshipAttrDef -> relationshipAttrDef.getName().equals(relationshipAttributeName)
                    && relationshipAttrDef.isAppendOnPartialUpdate());
        }

        return ret;
    }

    private AtlasEdge createVertex(AtlasStruct struct, AtlasVertex referringVertex, String edgeLabel, EntityMutationContext context) throws AtlasBaseException {
        AtlasVertex vertex = createStructVertex(struct);

        mapAttributes(struct, vertex, CREATE, context);

        try {
            //TODO - Map directly in AtlasGraphUtilsV1
            return graphHelper.getOrCreateEdge(referringVertex, vertex, edgeLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }

    private void updateVertex(AtlasStruct struct, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        mapAttributes(struct, vertex, UPDATE, context);
    }

    private Long getEntityVersion(AtlasEntity entity) {
        Long ret = entity != null ? entity.getVersion() : null;
        return (ret != null) ? ret : 0;
    }

    private String getCustomAttributesString(AtlasEntity entity) {
        String              ret              = null;
        Map<String, String> customAttributes = entity.getCustomAttributes();

        if (customAttributes != null) {
            ret = AtlasType.toJson(customAttributes);
        }

        return ret;
    }

    private AtlasStructType getStructType(String typeName) throws AtlasBaseException {
        AtlasType objType = typeRegistry.getType(typeName);

        if (!(objType instanceof AtlasStructType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, typeName);
        }

        return (AtlasStructType)objType;
    }

    private AtlasEntityType getEntityType(String typeName) throws AtlasBaseException {
        AtlasType objType = typeRegistry.getType(typeName);

        if (!(objType instanceof AtlasEntityType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, typeName);
        }

        return (AtlasEntityType)objType;
    }

    private Object mapCollectionElementsToVertex(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        switch(ctx.getAttrType().getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
            case MAP:
            case ARRAY:
                return ctx.getValue();

            case STRUCT:
                return mapStructValue(ctx, context);

            case OBJECT_ID_TYPE:
                AtlasEntityType instanceType = getInstanceType(ctx.getValue(), context);
                ctx.setElementType(instanceType);
                if (ctx.getAttributeDef().isSoftReferenced()) {
                    return mapSoftRefValue(ctx, context);
                }

                return mapObjectIdValueUsingRelationship(ctx, context);

            default:
                throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }

    private static AtlasObjectId getObjectId(Object val) throws AtlasBaseException {
        AtlasObjectId ret = null;

        if (val != null) {
            if ( val instanceof  AtlasObjectId) {
                ret = ((AtlasObjectId) val);
            } else if (val instanceof Map) {
                Map map = (Map) val;

                if (map.containsKey(AtlasRelatedObjectId.KEY_RELATIONSHIP_TYPE)) {
                    ret = new AtlasRelatedObjectId(map);
                } else {
                    ret = new AtlasObjectId((Map) val);
                }

                if (!AtlasTypeUtil.isValid(ret)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, val.toString());
                }
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, val.toString());
            }
        }

        return ret;
    }

    private static String getGuid(Object val) throws AtlasBaseException {
        if (val != null) {
            if ( val instanceof  AtlasObjectId) {
                return ((AtlasObjectId) val).getGuid();
            } else if (val instanceof Map) {
                Object guidVal = ((Map)val).get(AtlasObjectId.KEY_GUID);

                return guidVal != null ? guidVal.toString() : null;
            }
        }

        return null;
    }

    private void setAssignedGuid(Object val, EntityMutationContext context) {
        if (val != null) {
            Map<String, String> guidAssignements = context.getGuidAssignments();

            if (val instanceof AtlasObjectId) {
                AtlasObjectId objId        = (AtlasObjectId) val;
                String        guid         = objId.getGuid();
                String        assignedGuid = null;

                if (StringUtils.isNotEmpty(guid)) {
                    if (!AtlasTypeUtil.isAssignedGuid(guid) && MapUtils.isNotEmpty(guidAssignements)) {
                        assignedGuid = guidAssignements.get(guid);
                    }
                } else {
                    AtlasVertex vertex = context.getDiscoveryContext().getResolvedEntityVertex(objId);

                    if (vertex != null) {
                        assignedGuid = graphHelper.getGuid(vertex);
                    }
                }

                if (StringUtils.isNotEmpty(assignedGuid)) {
                    RequestContext.get().recordEntityGuidUpdate(objId, guid);

                    objId.setGuid(assignedGuid);
                }
            } else if (val instanceof Map) {
                Map    mapObjId     = (Map) val;
                Object guidVal      = mapObjId.get(AtlasObjectId.KEY_GUID);
                String guid         = guidVal != null ? guidVal.toString() : null;
                String assignedGuid = null;

                if (StringUtils.isNotEmpty(guid) ) {
                    if (!AtlasTypeUtil.isAssignedGuid(guid) && MapUtils.isNotEmpty(guidAssignements)) {
                        assignedGuid = guidAssignements.get(guid);
                    }
                } else {
                    AtlasVertex vertex = context.getDiscoveryContext().getResolvedEntityVertex(new AtlasObjectId(mapObjId));

                    if (vertex != null) {
                        assignedGuid = graphHelper.getGuid(vertex);
                    }
                }

                if (StringUtils.isNotEmpty(assignedGuid)) {
                    RequestContext.get().recordEntityGuidUpdate(mapObjId, guid);

                    mapObjId.put(AtlasObjectId.KEY_GUID, assignedGuid);
                }
            }
        }
    }

    private static Map<String, Object> getRelationshipAttributes(Object val) throws AtlasBaseException {
        if (val instanceof AtlasRelatedObjectId) {
            AtlasStruct relationshipStruct = ((AtlasRelatedObjectId) val).getRelationshipAttributes();

            return (relationshipStruct != null) ? relationshipStruct.getAttributes() : null;
        } else if (val instanceof Map) {
            Object relationshipStruct = ((Map) val).get(KEY_RELATIONSHIP_ATTRIBUTES);

            if (relationshipStruct instanceof Map) {
                return AtlasTypeUtil.toStructAttributes(((Map) relationshipStruct));
            }
        } else if (val instanceof AtlasObjectId) {
            return ((AtlasObjectId) val).getAttributes();
        }

        return null;
    }

    private static String getRelationshipGuid(Object val) throws AtlasBaseException {
        if (val instanceof AtlasRelatedObjectId) {
            return ((AtlasRelatedObjectId) val).getRelationshipGuid();
        } else if (val instanceof Map) {
            Object relationshipGuidVal = ((Map) val).get(AtlasRelatedObjectId.KEY_RELATIONSHIP_GUID);

            return relationshipGuidVal != null ? relationshipGuidVal.toString() : null;
        }

        return null;
    }

    private AtlasEntityType getInstanceType(Object val, EntityMutationContext context) throws AtlasBaseException {
        AtlasEntityType ret = null;

        if (val != null) {
            String typeName = null;
            String guid     = null;

            if (val instanceof AtlasObjectId) {
                AtlasObjectId objId = (AtlasObjectId) val;

                typeName = objId.getTypeName();
                guid     = objId.getGuid();
            } else if (val instanceof Map) {
                Map map = (Map) val;

                Object typeNameVal = map.get(KEY_TYPENAME);
                Object guidVal     = map.get(AtlasObjectId.KEY_GUID);

                if (typeNameVal != null) {
                    typeName = typeNameVal.toString();
                }

                if (guidVal != null) {
                    guid = guidVal.toString();
                }
            }

            if (typeName == null) {
                if (guid != null) {
                    ret = context.getType(guid);

                    if (ret == null) {
                        AtlasVertex vertex = context.getDiscoveryContext().getResolvedEntityVertex(guid);

                        if (vertex != null) {
                            typeName = AtlasGraphUtilsV2.getTypeName(vertex);
                        }
                    }
                }
            }

            if (ret == null && typeName != null) {
                ret = typeRegistry.getEntityTypeByName(typeName);
            }

            if (ret == null) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, val.toString());
            }
        }

        return ret;
    }

    //Remove unused entries for reference map
    private Map<String, Object> removeUnusedMapEntries(AtlasAttribute attribute, AtlasVertex vertex, Map<String, Object> currentMap,
                                                       Map<String, Object> newMap) throws AtlasBaseException {
        Map<String, Object> additionalMap = new HashMap<>();
        AtlasMapType        mapType       = (AtlasMapType) attribute.getAttributeType();

        for (String currentKey : currentMap.keySet()) {
            //Delete the edge reference if its not part of new edges created/updated
            AtlasEdge currentEdge = (AtlasEdge) currentMap.get(currentKey);

            if (!newMap.values().contains(currentEdge)) {
                boolean deleted = deleteDelegate.getHandler().deleteEdgeReference(currentEdge, mapType.getValueType().getTypeCategory(), attribute.isOwnedRef(), true, vertex);

                if (!deleted) {
                    additionalMap.put(currentKey, currentEdge);
                }
            }
        }

        return additionalMap;
    }

    private static AtlasEdge getEdgeIfExists(AtlasMapType mapType, Map<String, Object> currentMap, String keyStr) {
        AtlasEdge ret = null;

        if (isReference(mapType.getValueType())) {
            Object val = currentMap.get(keyStr);

            if (val != null) {
                ret = (AtlasEdge) val;
            }
        }

        return ret;
    }

    private AtlasEdge updateEdge(AtlasAttributeDef attributeDef, Object value, AtlasEdge currentEdge, final AtlasVertex entityVertex) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating entity reference {} for reference attribute {}",  attributeDef.getName());
        }

        AtlasVertex currentVertex   = currentEdge.getInVertex();
        String      currentEntityId = getIdFromVertex(currentVertex);
        String      newEntityId     = getIdFromVertex(entityVertex);
        AtlasEdge   newEdge         = currentEdge;

        if (!currentEntityId.equals(newEntityId) && entityVertex != null) {
            try {
                newEdge = graphHelper.getOrCreateEdge(currentEdge.getOutVertex(), entityVertex, currentEdge.getLabel());
            } catch (RepositoryException e) {
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
            }
        }

        return newEdge;
    }


    private AtlasEdge updateRelationship(AttributeMutationContext ctx, final AtlasVertex parentEntityVertex, final AtlasVertex newEntityVertex,
                                         AtlasRelationshipEdgeDirection edgeDirection,  Map<String, Object> relationshipAttributes)
            throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating entity reference using relationship {} for reference attribute {}", getTypeName(newEntityVertex), ctx.getAttribute().getName());
        }

        AtlasEdge currentEdge = ctx.getCurrentEdge();

        // Max's manager updated from Jane to Julius (Max.manager --> Jane.subordinates)
        // manager attribute (OUT direction), current manager vertex (Jane) (IN vertex)

        // Max's mentor updated from John to Jane (John.mentee --> Max.mentor)
        // mentor attribute (IN direction), current mentee vertex (John) (OUT vertex)
        String currentEntityId;

        if (edgeDirection == IN) {
            currentEntityId = getIdFromOutVertex(currentEdge);
        } else if (edgeDirection == OUT) {
            currentEntityId = getIdFromInVertex(currentEdge);
        } else {
            currentEntityId = getIdFromBothVertex(currentEdge, parentEntityVertex);
        }

        String    newEntityId = getIdFromVertex(newEntityVertex);
        AtlasEdge ret         = currentEdge;

        if (StringUtils.isEmpty(currentEntityId) || !currentEntityId.equals(newEntityId)) {
            // Checking if currentEntityId is null or empty as corrupted vertex on the other side of the edge should result into creation of new edge
            // create a new relationship edge to the new attribute vertex from the instance
            String relationshipName = AtlasGraphUtilsV2.getTypeName(currentEdge);

            if (relationshipName == null) {
                relationshipName = currentEdge.getLabel();
            }

            if (edgeDirection == IN) {
                ret = getOrCreateRelationship(newEntityVertex, currentEdge.getInVertex(), relationshipName, relationshipAttributes);

            } else if (edgeDirection == OUT) {
                ret = getOrCreateRelationship(currentEdge.getOutVertex(), newEntityVertex, relationshipName, relationshipAttributes);
            } else {
                ret = getOrCreateRelationship(newEntityVertex, parentEntityVertex, relationshipName, relationshipAttributes);
            }

            boolean isCreated = getCreatedTime(ret) == RequestContext.get().getRequestTime();
            if (isCreated) {
                // This flow is executed even if edge was only updated, or even the different order in payload
                // Necessary to call recordEntityUpdate for only new edge creation, hence checking `isCreated`
                recordEntityUpdate(newEntityVertex, ctx, true);
            }
        }

        return ret;
    }

    public static List<Object> getArrayElementsProperty(AtlasType elementType, boolean isSoftReference, AtlasVertex vertex, String vertexPropertyName) {
        boolean isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
        boolean isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);
        if (!isSoftReference && isReference(elementType)) {
            return (List)vertex.getListProperty(vertexPropertyName, AtlasEdge.class);
        } else if (isArrayOfPrimitiveType || isArrayOfEnum) {
            return (List) vertex.getMultiValuedProperty(vertexPropertyName, elementType.getClass());
        } else {
            return (List)vertex.getListProperty(vertexPropertyName);
        }
    }

    private AtlasEdge getEdgeAt(List<Object> currentElements, int index, AtlasType elemType) {
        AtlasEdge ret = null;

        if (isReference(elemType)) {
            if (currentElements != null && index < currentElements.size()) {
                ret = (AtlasEdge) currentElements.get(index);
            }
        }

        return ret;
    }

    private List<AtlasEdge> unionCurrentAndNewElements(AtlasAttribute attribute, List<AtlasEdge> currentElements, List<AtlasEdge> newElements) {
        Collection<AtlasEdge> ret              = null;
        AtlasType             arrayElementType = ((AtlasArrayType) attribute.getAttributeType()).getElementType();

        if (arrayElementType != null && isReference(arrayElementType)) {
            ret = CollectionUtils.union(currentElements, newElements);
        }

        return CollectionUtils.isNotEmpty(ret) ? new ArrayList<>(ret) : Collections.emptyList();
    }

    //Removes unused edges from the old collection, compared to the new collection

    private List<AtlasEdge> removeUnusedArrayEntries(AtlasAttribute attribute, List<AtlasEdge> currentEntries, List<AtlasEdge> newEntries, AttributeMutationContext ctx) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(currentEntries)) {
            AtlasType entryType = ((AtlasArrayType) attribute.getAttributeType()).getElementType();
            AtlasVertex entityVertex = ctx.getReferringVertex();

            if (isReference(entryType)) {
                Collection<AtlasEdge> edgesToRemove = CollectionUtils.subtract(currentEntries, newEntries);

                if (CollectionUtils.isNotEmpty(edgesToRemove)) {
                    List<AtlasEdge> removedElements = new ArrayList<>();

                    for (AtlasEdge edge : edgesToRemove) {
                        if (getStatus(edge) == DELETED) {
                            continue;
                        }

                        try {
                            boolean deleted = deleteDelegate.getHandler().deleteEdgeReference(edge, entryType.getTypeCategory(), attribute.isOwnedRef(),
                                    true, attribute.getRelationshipEdgeDirection(), entityVertex);

                            if (!deleted) {
                                removedElements.add(edge);

                                if (IN == attribute.getRelationshipEdgeDirection()) {
                                    recordEntityUpdate(edge.getOutVertex(), ctx, false);
                                } else {
                                    recordEntityUpdate(edge.getInVertex(), ctx, false);
                                }
                            }
                        } catch (NullPointerException npe) {
                            LOG.warn("Ignoring deleting edge with corrupted vertex: {}", edge.getId());
                        }
                    }

                    return removedElements;
                }
            }
        }

        return Collections.emptyList();
    }

    private List<AtlasEdge> removeArrayEntries(AtlasAttribute attribute, List<AtlasEdge> tobeDeletedEntries, AttributeMutationContext ctx) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(tobeDeletedEntries)) {
            AtlasType entryType = ((AtlasArrayType) attribute.getAttributeType()).getElementType();
            AtlasVertex entityVertex = ctx.getReferringVertex();

            if (isReference(entryType)) {

                if (CollectionUtils.isNotEmpty(tobeDeletedEntries)) {
                    List<AtlasEdge> additionalElements = new ArrayList<>();

                    for (AtlasEdge edge : tobeDeletedEntries) {
                        if (edge == null || getStatus(edge) == DELETED) {
                            continue;
                        }

                        // update both sides of relationship wen edge is deleted
                        recordEntityUpdateForNonRelationsipAttribute(edge.getInVertex());
                        recordEntityUpdateForNonRelationsipAttribute(edge.getOutVertex());

                        deleteDelegate.getHandler().deleteEdgeReference(edge, entryType.getTypeCategory(), attribute.isOwnedRef(),
                                true, attribute.getRelationshipEdgeDirection(), entityVertex);

                        additionalElements.add(edge);

                    }

                    return additionalElements;
                }
            }
        }

        return Collections.emptyList();
    }
    private void setArrayElementsProperty(AtlasType elementType, boolean isSoftReference, AtlasVertex vertex, String vertexPropertyName, List<Object> allValues, List<Object> currentValues, Cardinality cardinality) {
        boolean isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
        boolean isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);

        if (!isReference(elementType) || isSoftReference) {
            if (isArrayOfPrimitiveType || isArrayOfEnum) {
                vertex.removeProperty(vertexPropertyName);
                if (CollectionUtils.isNotEmpty(allValues)) {
                    for (Object value: allValues) {
                        AtlasGraphUtilsV2.addEncodedProperty(vertex, vertexPropertyName, value);
                    }
                }
            } else {
                AtlasGraphUtilsV2.setEncodedProperty(vertex, vertexPropertyName, allValues);
            }
        }
    }


    private Set<AtlasEdge> getNewCreatedInputOutputEdges(String guid) {
        List<Object> newElementsCreated = RequestContext.get().getNewElementsCreatedMap().get(guid);

        Set<AtlasEdge> newEdge = new HashSet<>();
        if (newElementsCreated != null && newElementsCreated.size() > 0) {
            newEdge = newElementsCreated.stream().map(x -> (AtlasEdge) x).collect(Collectors.toSet());
        }

        return newEdge;
    }

    private Set<AtlasEdge> getRestoredInputOutputEdges(AtlasVertex vertex) {
        Set<AtlasEdge> activatedEdges = new HashSet<>();
        Iterator<AtlasEdge> iterator = vertex.getEdges(AtlasEdgeDirection.BOTH, new String[]{PROCESS_INPUTS, PROCESS_OUTPUTS}).iterator();
        while (iterator.hasNext()) {
            AtlasEdge edge = iterator.next();
            if (edge.getProperty(STATE_PROPERTY_KEY, String.class).equalsIgnoreCase(ACTIVE_STATE_VALUE)) {
                activatedEdges.add(edge);
            }
        }
        return activatedEdges;
    }

    private Set<AtlasEdge> getRemovedInputOutputEdges(String guid) {
        List<Object> removedElements = RequestContext.get().getRemovedElementsMap().get(guid);
        Set<AtlasEdge> removedEdges = null;

        if (removedElements != null) {
            removedEdges = removedElements.stream().map(x -> (AtlasEdge) x).collect(Collectors.toSet());
        }

        return removedEdges;
    }




    private AtlasEntityHeader constructHeader(AtlasEntity entity, AtlasVertex vertex, AtlasEntityType entityType) throws AtlasBaseException {
        Map<String, AtlasAttribute> attributeMap = entityType.getAllAttributes();
        AtlasEntityHeader header = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex, attributeMap.keySet());
        if (entity.getClassifications() == null) {
            entity.setClassifications(header.getClassifications());
        }

        return header;
    }

    private void updateInConsistentOwnedMapVertices(AttributeMutationContext ctx, AtlasMapType mapType, Object val) {
        if (mapType.getValueType().getTypeCategory() == TypeCategory.OBJECT_ID_TYPE && !ctx.getAttributeDef().isSoftReferenced()) {
            AtlasEdge edge = (AtlasEdge) val;

            if (ctx.getAttribute().isOwnedRef() && getStatus(edge) == DELETED && getStatus(edge.getInVertex()) == DELETED) {

                //Resurrect the vertex and edge to ACTIVE state
                AtlasGraphUtilsV2.setEncodedProperty(edge, STATE_PROPERTY_KEY, ACTIVE.name());
                AtlasGraphUtilsV2.setEncodedProperty(edge.getInVertex(), STATE_PROPERTY_KEY, ACTIVE.name());
            }
        }
    }


    public int cleanUpClassificationPropagation(String classificationName, int batchLimit) {
        int CLEANUP_MAX = batchLimit <= 0 ? CLEANUP_BATCH_SIZE : batchLimit * CLEANUP_BATCH_SIZE;
        int cleanedUpCount = 0;
        long classificationEdgeCount = 0;
        long classificationEdgeInMemoryCount = 0;
        Iterator<AtlasVertex> tagVertices = GraphHelper.getClassificationVertices(graph, classificationName, CLEANUP_BATCH_SIZE);
        List<AtlasVertex> tagVerticesProcessed = new ArrayList<>(0);
        List<AtlasVertex> currentAssetVerticesBatch = new ArrayList<>(0);

        while (tagVertices != null && tagVertices.hasNext()) {
            if (cleanedUpCount >= CLEANUP_MAX){
                return cleanedUpCount;
            }

            while (tagVertices.hasNext() && currentAssetVerticesBatch.size() < CLEANUP_BATCH_SIZE) {
                AtlasVertex tagVertex = tagVertices.next();

                int availableSlots = CLEANUP_BATCH_SIZE - currentAssetVerticesBatch.size();
                long assetCountForCurrentTagVertex = GraphHelper.getAssetsCountOfClassificationVertex(tagVertex);
                currentAssetVerticesBatch.addAll(GraphHelper.getAllAssetsWithClassificationVertex(tagVertex, availableSlots));
                LOG.info("Available slots : {}, assetCountForCurrentTagVertex : {}, queueSize : {}",availableSlots, assetCountForCurrentTagVertex, currentAssetVerticesBatch.size());
                if (assetCountForCurrentTagVertex <= availableSlots) {
                    tagVerticesProcessed.add(tagVertex);
                }
            }

            int currentAssetsBatchSize = currentAssetVerticesBatch.size();
            if (currentAssetsBatchSize > 0) {
                LOG.info("To clean up tag {} from {} entities", classificationName, currentAssetsBatchSize);
                int offset = 0;
                do {
                    try {
                        int toIndex = Math.min((offset + CHUNK_SIZE), currentAssetsBatchSize);
                        List<AtlasVertex> entityVertices = currentAssetVerticesBatch.subList(offset, toIndex);
                        for (AtlasVertex vertex : entityVertices) {
                            List<AtlasClassification> deletedClassifications = new ArrayList<>();
                            GraphTransactionInterceptor.lockObjectAndReleasePostCommit(graphHelper.getGuid(vertex));
                            List<AtlasEdge> classificationEdges = GraphHelper.getClassificationEdges(vertex, null, classificationName);
                            classificationEdgeCount += classificationEdges.size();
                            int batchSize = CHUNK_SIZE;
                            for (int i = 0; i < classificationEdges.size(); i += batchSize) {
                                int end = Math.min(i + batchSize, classificationEdges.size());
                                List<AtlasEdge> batch = classificationEdges.subList(i, end);
                                for (AtlasEdge edge : batch) {
                                    try {
                                        AtlasClassification classification = entityRetriever.toAtlasClassification(edge.getInVertex());
                                        deletedClassifications.add(classification);
                                        deleteDelegate.getHandler().deleteEdgeReference(edge, CLASSIFICATION, false, true, null, vertex);
                                        classificationEdgeInMemoryCount++;
                                    } catch (IllegalStateException | AtlasBaseException e) {
                                        e.printStackTrace();
                                    }
                                }
                                if(classificationEdgeInMemoryCount >= CHUNK_SIZE){
                                    transactionInterceptHelper.intercept();
                                    classificationEdgeInMemoryCount = 0;
                                }
                            }
                            try {
                                AtlasEntity entity = repairClassificationMappings(vertex);
                                entityChangeNotifier.onClassificationDeletedFromEntity(entity, deletedClassifications);
                            } catch (IllegalStateException | AtlasBaseException e) {
                                e.printStackTrace();
                            }
                        }

                        transactionInterceptHelper.intercept();

                        offset += CHUNK_SIZE;
                    } finally {
                        LOG.info("For offset {} , classificationEdge were : {}", offset, classificationEdgeCount);
                        classificationEdgeCount = 0;
                        LOG.info("Cleaned up {} entities for classification {}", offset, classificationName);
                    }

                } while (offset < currentAssetsBatchSize);

                for (AtlasVertex classificationVertex : tagVerticesProcessed) {
                    try {
                        deleteDelegate.getHandler().deleteClassificationVertex(classificationVertex, true);
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    }
                }
                transactionInterceptHelper.intercept();

                cleanedUpCount += currentAssetsBatchSize;
                currentAssetVerticesBatch.clear();
                tagVerticesProcessed.clear();
            }
            tagVertices = GraphHelper.getClassificationVertices(graph, classificationName, CLEANUP_BATCH_SIZE);
        }

        LOG.info("Completed cleaning up classification {}", classificationName);
        return cleanedUpCount;
    }

    public AtlasEntity repairClassificationMappings(AtlasVertex entityVertex) throws AtlasBaseException {
        String guid = GraphHelper.getGuid(entityVertex);
        AtlasEntity entity = instanceConverter.getEntity(guid, ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION, new AtlasEntityHeader(entity)), "repair classification mappings: guid=", guid);
        List<String> classificationNames = new ArrayList<>();
        List<String> propagatedClassificationNames = new ArrayList<>();

        if (entity.getClassifications() != null) {
            List<AtlasClassification> classifications = entity.getClassifications();
            for (AtlasClassification classification : classifications) {
                if (isPropagatedClassification(classification, guid)) {
                    propagatedClassificationNames.add(classification.getTypeName());
                } else {
                    classificationNames.add(classification.getTypeName());
                }
            }
        }
        //Delete array/set properties first
        entityVertex.removeProperty(TRAIT_NAMES_PROPERTY_KEY);
        entityVertex.removeProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);


        //Update classificationNames and propagatedClassificationNames in entityVertex
        entityVertex.setProperty(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(classificationNames));
        entityVertex.setProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(propagatedClassificationNames));
        entityVertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
        // Make classificationNames unique list as it is of type SET
        classificationNames = classificationNames.stream().distinct().collect(Collectors.toList());
        //Update classificationNames and propagatedClassificationNames in entityHeader
        for(String classificationName : classificationNames) {
            AtlasGraphUtilsV2.addEncodedProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, classificationName);
        }
        for (String classificationName : propagatedClassificationNames) {
            entityVertex.addListProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationName);
        }

        return entity;
    }

    public Map<String, String> repairClassificationMappingsV2(List<AtlasVertex> entityVertices) throws AtlasBaseException {
        Map<String, String> errorMap = new HashMap<>(0);

        for (AtlasVertex entityVertex : entityVertices) {
            List<AtlasClassification> currentTags = tagDAO.getAllClassificationsForVertex(entityVertex.getIdForDisplay());


            try {
                Map<String, Map<String, Object>> deNormMap = new HashMap<>();

                deNormMap.put(entityVertex.getIdForDisplay(),
                        TagDeNormAttributesUtil.getAllAttributesForAllTagsForRepair(GraphHelper.getGuid(entityVertex), currentTags, typeRegistry, fullTextMapperV2));

                // ES operation collected to be executed in the end
                RequestContext.get().addESDeferredOperation(
                        new ESDeferredOperation(
                                ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS,
                                entityVertex.getIdForDisplay(),
                                deNormMap
                        )
                );
            } catch (Exception e) {
                errorMap.put(GraphHelper.getGuid(entityVertex), e.getMessage());
            }
        }

        return errorMap;
    }

    public void addClassificationsV1(final EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(classifications)) {
            MetricRecorder metric = RequestContext.get().startMetricRecord("addClassifications");

            final AtlasVertex                              entityVertex          = context.getVertex(guid);
            final AtlasEntityType                          entityType            = context.getType(guid);
            List<AtlasVertex>                              entitiesToPropagateTo = null;
            Map<AtlasClassification, HashSet<AtlasVertex>> addedClassifications  = new HashMap<>();
            List<AtlasClassification>                      addClassifications    = new ArrayList<>(classifications.size());
            entityRetriever.verifyClassificationsPropagationMode(classifications);
            for (AtlasClassification c : classifications) {
                AtlasClassification classification      = new AtlasClassification(c);
                String              classificationName  = classification.getTypeName();
                Boolean             propagateTags       = classification.isPropagate();
                Boolean             removePropagations  = classification.getRemovePropagationsOnEntityDelete();
                Boolean restrictPropagationThroughLineage = classification.getRestrictPropagationThroughLineage();
                Boolean restrictPropagationThroughHierarchy = classification.getRestrictPropagationThroughHierarchy();

                if (propagateTags != null && propagateTags &&
                        classification.getEntityGuid() != null &&
                        !StringUtils.equals(classification.getEntityGuid(), guid)) {
                    continue;
                }

                if (propagateTags == null) {
                    RequestContext reqContext = RequestContext.get();

                    if(reqContext.isImportInProgress() || reqContext.isInNotificationProcessing()) {
                        propagateTags = false;
                    } else {
                        propagateTags = CLASSIFICATION_PROPAGATION_DEFAULT;
                    }

                    classification.setPropagate(propagateTags);
                }

                if (removePropagations == null) {
                    removePropagations = graphHelper.getDefaultRemovePropagations();

                    classification.setRemovePropagationsOnEntityDelete(removePropagations);
                }

                if (restrictPropagationThroughLineage == null) {
                    classification.setRestrictPropagationThroughLineage(RESTRICT_PROPAGATION_THROUGH_LINEAGE_DEFAULT);
                }

                if (restrictPropagationThroughHierarchy == null) {
                    classification.setRestrictPropagationThroughHierarchy(RESTRICT_PROPAGATION_THROUGH_HIERARCHY_DEFAULT);
                }

                // set associated entity id to classification
                if (classification.getEntityGuid() == null) {
                    classification.setEntityGuid(guid);
                }

                // set associated entity status to classification
                if (classification.getEntityStatus() == null) {
                    classification.setEntityStatus(ACTIVE);
                }

                // ignore propagated classifications

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding classification [{}] to [{}] using edge label: [{}]", classificationName, entityType.getTypeName(), getTraitLabel(classificationName));
                }

                addToClassificationNames(entityVertex, classificationName);

                // add a new AtlasVertex for the struct or trait instance
                AtlasVertex classificationVertex = createClassificationVertex(classification);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("created vertex {} for trait {}", GraphHelper.string(classificationVertex), classificationName);
                }

                if (propagateTags && taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    propagateTags = false;

                    createAndQueueTask(CLASSIFICATION_PROPAGATION_ADD, entityVertex, classificationVertex.getIdForDisplay(), getTypeName(classificationVertex));
                }

                // add the attributes for the trait instance
                mapClassification(EntityOperation.CREATE, context, classification, entityType, entityVertex, classificationVertex);
                updateModificationMetadata(entityVertex);
                if(addedClassifications.get(classification) == null) {
                    addedClassifications.put(classification, new HashSet<>());
                }
                //Add current Vertex to be notified
                addedClassifications.get(classification).add(entityVertex);

                if (propagateTags) {
                    // compute propagatedEntityVertices only once
                    if (entitiesToPropagateTo == null) {
                        String propagationMode;
                        propagationMode = entityRetriever.determinePropagationMode(classification.getRestrictPropagationThroughLineage(),classification.getRestrictPropagationThroughHierarchy());
                        Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true : false;
                        entitiesToPropagateTo = entityRetriever.getImpactedVerticesV2(entityVertex, CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude);
                    }

                    if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Propagating tag: [{}][{}] to {}", classificationName, entityType.getTypeName(), GraphHelper.getTypeNames(entitiesToPropagateTo));
                        }

                        List<AtlasVertex> entitiesPropagatedTo = deleteDelegate.getHandler().addTagPropagation(classificationVertex, entitiesToPropagateTo);

                        if (CollectionUtils.isNotEmpty(entitiesPropagatedTo)) {
                            addedClassifications.get(classification).addAll(entitiesPropagatedTo);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(" --> Not propagating classification: [{}][{}] - no entities found to propagate to.", getTypeName(classificationVertex), entityType.getTypeName());
                        }
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" --> Not propagating classification: [{}][{}] - propagation is disabled.", getTypeName(classificationVertex), entityType.getTypeName());
                    }
                }

                addClassifications.add(classification);
            }

            // notify listeners on classification addition
            List<AtlasVertex> notificationVertices = new ArrayList<AtlasVertex>() {{ add(entityVertex); }};

            if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                notificationVertices.addAll(entitiesToPropagateTo);
            }


            for (AtlasClassification classification : addedClassifications.keySet()) {
                Set<AtlasVertex>  vertices           = addedClassifications.get(classification);

                if (RequestContext.get().isDelayTagNotifications()) {
                    RequestContext.get().addAddedClassificationAndVertices(classification, new ArrayList<>(vertices));
                } else {
                    List<AtlasEntity> propagatedEntities = updateClassificationText(classification, vertices);

                    entityChangeNotifier.onClassificationsAddedToEntities(propagatedEntities, Collections.singletonList(classification), false);
                }
            }

            RequestContext.get().endMetricRecord(metric);
        }
    }

    public void handleAddClassifications(final EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if(!RequestContext.get().isSkipAuthorizationCheck() && FeatureFlagStore.isTagV2Enabled()){
            addClassificationsV2(context, guid, classifications);
        } else {
            addClassificationsV1(context, guid, classifications);
        }
    }

    public void addClassificationsV2(final EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(classifications)) {
            MetricRecorder metric = RequestContext.get().startMetricRecord("addClassificationsV2");

            final AtlasVertex                              entityVertex          = context.getVertex(guid);
            final AtlasEntityType                          entityType            = context.getType(guid);
            List<AtlasVertex>                              entitiesToPropagateTo = null;
            Map<AtlasClassification, HashSet<AtlasVertex>> addedClassifications  = new HashMap<>();
            List<AtlasClassification>                      addClassifications    = new ArrayList<>(classifications.size());
            entityRetriever.verifyClassificationsPropagationMode(classifications);

            for (AtlasClassification c : classifications) {
                validateClassificationTypeName(c);
            }

            classifications = mapClassificationsV2(classifications);

            for (AtlasClassification c : classifications) {
                AtlasClassification classification      = new AtlasClassification(c);
                String              classificationName  = classification.getTypeName();
                Boolean             propagateTags       = classification.isPropagate();
                Boolean             removePropagations  = classification.getRemovePropagationsOnEntityDelete();
                Boolean restrictPropagationThroughLineage = classification.getRestrictPropagationThroughLineage();
                Boolean restrictPropagationThroughHierarchy = classification.getRestrictPropagationThroughHierarchy();

                if (propagateTags != null && propagateTags &&
                        classification.getEntityGuid() != null &&
                        !StringUtils.equals(classification.getEntityGuid(), guid)) {
                    continue;
                }

                if (propagateTags == null) {
                    RequestContext reqContext = RequestContext.get();

                    if(reqContext.isImportInProgress() || reqContext.isInNotificationProcessing()) {
                        propagateTags = false;
                    } else {
                        propagateTags = CLASSIFICATION_PROPAGATION_DEFAULT;
                    }

                    classification.setPropagate(propagateTags);
                }

                if (removePropagations == null) {
                    removePropagations = graphHelper.getDefaultRemovePropagations();

                    classification.setRemovePropagationsOnEntityDelete(removePropagations);
                }

                if (restrictPropagationThroughLineage == null) {
                    classification.setRestrictPropagationThroughLineage(RESTRICT_PROPAGATION_THROUGH_LINEAGE_DEFAULT);
                }

                if (restrictPropagationThroughHierarchy == null) {
                    classification.setRestrictPropagationThroughHierarchy(RESTRICT_PROPAGATION_THROUGH_HIERARCHY_DEFAULT);
                }

                // set associated entity id to classification
                if (classification.getEntityGuid() == null) {
                    classification.setEntityGuid(guid);
                }

                // set associated entity status to classification
                if (classification.getEntityStatus() == null) {
                    classification.setEntityStatus(ACTIVE);
                }

                // ignore propagated classifications

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding classification [{}] to [{}] using edge label: [{}]", classificationName, entityType.getTypeName(), getTraitLabel(classificationName));
                }

                Map<String, Object> minAssetMap = getMinimalAssetMap(entityVertex);

                //addToClassificationNames(entityVertex, classificationName);
                List<AtlasClassification> currentTags = tagDAO.getAllClassificationsForVertex(entityVertex.getIdForDisplay());
                currentTags.add(classification);

                // add a new AtlasVertex for the struct or trait instance
                // AtlasVertex classificationVertex = createClassificationVertex(classification);
                tagDAO.putDirectTag(entityVertex.getIdForDisplay(), classificationName, classification, minAssetMap);

                // Adding to context for rollback purpose later
                RequestContext reqContext = RequestContext.get();
                reqContext.addCassandraTagOperation(guid,
                        new CassandraTagOperation(
                                entityVertex.getIdForDisplay(),
                                classificationName,
                                CassandraTagOperation.OperationType.INSERT,
                                classification,
                                minAssetMap)
                );

                // Update ES attributes
                Map<String, Map<String, Object>> deNormMap = new HashMap<>();
                deNormMap.put(entityVertex.getIdForDisplay(), TagDeNormAttributesUtil.getDirectTagAttachmentAttributesForAddTag(classification,
                        currentTags, typeRegistry, fullTextMapperV2));
                // ES operation collected to be executed in the end
                RequestContext.get().addESDeferredOperation(
                        new ESDeferredOperation(
                                ESDeferredOperation.OperationType.TAG_DENORM_FOR_ADD_CLASSIFICATIONS,
                                entityVertex.getIdForDisplay(),
                                deNormMap
                        )
                );

                if (LOG.isDebugEnabled()) {
                    LOG.debug("created direct tag {}", classificationName);
                }

                if (propagateTags && taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    deleteDelegate.getHandler().createAndQueueTaskWithoutCheckV2(CLASSIFICATION_PROPAGATION_ADD, entityVertex, null, classificationName);
                }

                // add the attributes for the trait instance
                //mapClassification(EntityOperation.CREATE, context, classification, entityType, entityVertex, classificationVertex);

                updateModificationMetadata(entityVertex);
                if(addedClassifications.get(classification) == null) {
                    addedClassifications.put(classification, new HashSet<>());
                }
                //Add current Vertex to be notified
                addedClassifications.get(classification).add(entityVertex);

                addClassifications.add(classification);
            }

            // notify listeners on classification addition
            List<AtlasVertex> notificationVertices = new ArrayList<AtlasVertex>() {{ add(entityVertex); }};

            if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                notificationVertices.addAll(entitiesToPropagateTo);
            }


            for (AtlasClassification classification : addedClassifications.keySet()) {
                Set<AtlasVertex>  vertices           = addedClassifications.get(classification);

                if (RequestContext.get().isDelayTagNotifications()) {
                    RequestContext.get().addAddedClassificationAndVertices(classification, new ArrayList<>(vertices));
                } else {
                    List<AtlasEntity> entities = updateClassificationText(classification, vertices);
                    entityChangeNotifier.onClassificationsAddedToEntities(entities, Collections.singletonList(classification), false);
                }
            }

            RequestContext.get().endMetricRecord(metric);
        }
    }

    private List<AtlasClassification> mapClassificationsV2(List<AtlasClassification> classifications) throws AtlasBaseException {
        List<AtlasClassification> mappedClassifications = new ArrayList<>(classifications.size());
        for (AtlasClassification c : classifications) {
            // Apply attribute mapping to ensure schema compatibility with v1
            AtlasClassification mappedClassification = tagAttributeMapper.mapClassificationAttributes(c);
            mappedClassifications.add(mappedClassification);
        }
        classifications = mappedClassifications;
        return classifications;
    }

    public int propagateClassification(String entityGuid, String classificationVertexId, String relationshipGuid, Boolean previousRestrictPropagationThroughLineage,Boolean previousRestrictPropagationThroughHierarchy) throws AtlasBaseException {
        try {

            if (StringUtils.isEmpty(entityGuid) || StringUtils.isEmpty(classificationVertexId)) {
                LOG.error("propagateClassification(entityGuid={}, classificationVertexId={}): entityGuid and/or classification vertex id is empty", entityGuid, classificationVertexId);

                throw new AtlasBaseException(String.format("propagateClassification(entityGuid=%s, classificationVertexId=%s): entityGuid and/or classification vertex id is empty", entityGuid, classificationVertexId));
            }

            AtlasVertex entityVertex = graphHelper.getVertexForGUID(entityGuid);
            if (entityVertex == null) {
                LOG.error("propagateClassification(entityGuid={}, classificationVertexId={}): entity vertex not found", entityGuid, classificationVertexId);

                throw new AtlasBaseException(String.format("propagateClassification(entityGuid=%s, classificationVertexId=%s): entity vertex not found", entityGuid, classificationVertexId));
            }

            AtlasVertex classificationVertex = graph.getVertex(classificationVertexId);
            if (classificationVertex == null) {
                LOG.error("propagateClassification(entityGuid={}, classificationVertexId={}): classification vertex not found", entityGuid, classificationVertexId);

                throw new AtlasBaseException(String.format("propagateClassification(entityGuid=%s, classificationVertexId=%s): classification vertex not found", entityGuid, classificationVertexId));
            }

            /*
                If restrictPropagateThroughLineage was false at past
                 then updated to true we need to delete the propagated
                 classifications and then put the classifications as intended
             */

            Boolean currentRestrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(classificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);

            Boolean currentRestrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(classificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);
            if (previousRestrictPropagationThroughLineage != null && currentRestrictPropagationThroughLineage != null && !previousRestrictPropagationThroughLineage && currentRestrictPropagationThroughLineage) {
                deleteDelegate.getHandler().removeTagPropagation(classificationVertex);
            }

            if (previousRestrictPropagationThroughHierarchy != null && currentRestrictPropagationThroughHierarchy != null && !previousRestrictPropagationThroughHierarchy && currentRestrictPropagationThroughHierarchy) {
                deleteDelegate.getHandler().removeTagPropagation(classificationVertex);
            }

            String propagationMode = entityRetriever.determinePropagationMode(currentRestrictPropagationThroughLineage, currentRestrictPropagationThroughHierarchy);

            List<String> edgeLabelsToCheck = CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode);
            Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true:false;
            List<AtlasVertex> impactedVertices = entityRetriever.getIncludedImpactedVerticesV2(entityVertex, relationshipGuid, edgeLabelsToCheck,toExclude);

            if (CollectionUtils.isEmpty(impactedVertices)) {
                LOG.debug("propagateClassification(entityGuid={}, classificationVertexId={}): found no entities to propagate the classification", entityGuid, classificationVertexId);
                return 0;
            }

            List<String> propagatedAssets = processClassificationPropagationAddition(impactedVertices, classificationVertex);
            return propagatedAssets.size();
        } catch (Exception e) {
            LOG.error("propagateClassification(entityGuid={}, classificationVertexId={}): error while propagating classification", entityGuid, classificationVertexId, e);
            throw new AtlasBaseException(e);
        }
    }


    /**
     * Performs a scalable, "add-only" propagation of classifications using a "read-free"
     * approach. This is the new, optimized implementation.
     */
    public int propagateClassificationV2_Optimised(Map<String, Object> parameters,
                                                    String entityGuid,
                                                    String tagTypeName, String parentEntityGuid, String toVertexGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("propagateClassificationV2_new");

        final int BATCH_SIZE_FOR_ADD_PROPAGATION = 200;

        try {
            if (StringUtils.isEmpty(toVertexGuid)) {
                if (StringUtils.isEmpty(entityGuid) || StringUtils.isEmpty(tagTypeName)) {
                    LOG.error("propagateClassificationV2_Optimised(entityGuid={}, tagTypeName={}): entityGuid and/or classification vertex id is empty", entityGuid, tagTypeName);
                    throw new AtlasBaseException(String.format("propagateClassificationV2_Optimised(entityGuid=%s, tagTypeName=%s): entityGuid and/or classification vertex id is empty", entityGuid, tagTypeName));
                }

                AtlasVertex entityVertex = graphHelper.getVertexForGUID(entityGuid);
                if (entityVertex == null) {
                    String warningMessage = String.format("propagateClassificationV2_Optimised(entityGuid=%s, tagTypeName=%s): entity vertex not found, skipping task execution", entityGuid, tagTypeName);
                    LOG.warn(warningMessage);
                    return 0;
                }

                AtlasClassification tag = tagDAO.findDirectTagByVertexIdAndTagTypeName(entityVertex.getIdForDisplay(), tagTypeName, false);
                if (tag == null) {
                    if (StringUtils.isNotEmpty(parentEntityGuid) && !parentEntityGuid.equals(entityGuid)) {
                        //fallback only to get tag
                        AtlasVertex parentEntityVertex = graphHelper.getVertexForGUID(parentEntityGuid);
                        if (parentEntityVertex == null) {
                            String warningMessage = String.format("propagateClassificationV2_Optimised(parentEntityGuid=%s, tagTypeName=%s): parentEntityVertex vertex not found, skipping task execution", parentEntityGuid, tagTypeName);
                            LOG.warn(warningMessage);
                            return 0;
                        }
                        tag = tagDAO.findDirectTagByVertexIdAndTagTypeName(parentEntityVertex.getIdForDisplay(), tagTypeName, false);
                    }
                    if (tag == null) {
                        String warningMessage = String.format("propagateClassificationV2_Optimised(entityGuid=%s,parentEntityGuid=%s, tagTypeName=%s): tag not found, skipping task execution", entityGuid, parentEntityGuid, tagTypeName);
                        LOG.warn(warningMessage);
                        return 0;
                    }
                }

                LOG.info("propagateClassificationV2_Optimised: Starting 'Add' propagation for tag '{}' from source {}", tagTypeName, entityGuid);
                // 1. Calculate the full set of impacted vertices directly from the graph.
                Set<String> impactedVerticeIds = new HashSet<>();
                String propagationMode = entityRetriever.determinePropagationMode(tag.getRestrictPropagationThroughLineage(), tag.getRestrictPropagationThroughHierarchy());
                Boolean toExclude = Objects.equals(propagationMode, CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE);

                // The traversal to find out impacted vertices
                entityRetriever.traverseImpactedVerticesByLevelV2(entityVertex, null, null, impactedVerticeIds, CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode), toExclude, null, null);

                if (parentEntityGuid != null && !entityGuid.equals(parentEntityGuid)) {
                    impactedVerticeIds.add(entityVertex.getIdForDisplay());
                }

                if (CollectionUtils.isEmpty(impactedVerticeIds)) {
                    LOG.info("propagateClassificationV2_Optimised: No entities found to propagate the classification to.");
                    return 0;
                }

                // 2. Process additions in batches.
                LOG.info("propagateClassificationV2_Optimised: Found {} total vertices for propagation. Processing in batches.", impactedVerticeIds.size());
                List<String> vertexIdsToAdd = new ArrayList<>(impactedVerticeIds);
                int assetsAffected = 0;
                for (int i = 0; i < vertexIdsToAdd.size(); i += BATCH_SIZE_FOR_ADD_PROPAGATION) {
                    int end = Math.min(i + BATCH_SIZE_FOR_ADD_PROPAGATION, vertexIdsToAdd.size());
                    List<String> batchIds = vertexIdsToAdd.subList(i, end);

                    List<AtlasVertex> impactedVertices = batchIds.stream()
                            .map(x -> graph.getVertex(x))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    if (!impactedVertices.isEmpty()) {
                        assetsAffected += impactedVertices.size();
                        LOG.info("propagateClassificationV2_Optimised: Processing batch of {} assets for tag addition.", impactedVertices.size());
                        processClassificationPropagationAdditionV2(parameters, entityVertex.getIdForDisplay(), impactedVertices, tag);
                    }
                }
                return assetsAffected;
            } else {
                // "Add on Relationship Change" Flow, this logic processes all tags on the `fromVertex`.
                int assetsAffected = 0;
                AtlasVertex fromVertex = entityRetriever.getEntityVertex(entityGuid);
                if (fromVertex == null) {
                    String warningMessage = String.format("propagateClassificationV2_Optimised(fromVertexId=%s, tagTypeName=%s): fromVertex not found, skipping task execution", entityGuid, tagTypeName);
                    LOG.warn(warningMessage);
                    return assetsAffected;
                }

                AtlasVertex toVertex = entityRetriever.getEntityVertex(toVertexGuid);
                if (toVertex == null) {
                    String warningMessage = String.format("propagateClassificationV2_Optimised(toVertexId=%s, tagTypeName=%s): toVertex not found, skipping task execution", toVertexGuid, tagTypeName);
                    LOG.warn(warningMessage);
                    return assetsAffected;
                }

                List<Tag> tags = tagDAO.getAllTagsByVertexId(fromVertex.getIdForDisplay());
                Map<String, Set<String>> impactedVertexIdsMap = new TreeMap<>();

                for (Tag tag : tags) {
                    if (tag.isPropagationEnabled()) {
                        AtlasClassification atlasClassification = tag.toAtlasClassification();
                        String sourceEntityGuid = atlasClassification.getEntityGuid();
                        AtlasVertex sourceVertex = entityRetriever.getEntityVertex(sourceEntityGuid);
                        if (sourceVertex == null) {
                            String warningMessage = String.format("propagateClassificationV2_Optimised(sourceVertex=%s, tagTypeName=%s): sourceVertex not found, skipping task execution", sourceEntityGuid, tagTypeName);
                            LOG.warn(warningMessage);
                            continue;
                        }

                        String propagationMode = entityRetriever.determinePropagationMode(tag.getRestrictPropagationThroughLineage(), tag.getRestrictPropagationThroughHierarchy());

                        Set<String> impactedVertexIds;

                        if (!impactedVertexIdsMap.containsKey(propagationMode)) {
                            LOG.info("propagateClassificationV2_Optimised: Cache miss for propagationMode '{}'. Performing graph traversal.", propagationMode);
                            Boolean toExclude = Objects.equals(propagationMode, CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE);
                            impactedVertexIds = new HashSet<>();
                            Set<String> vertexIdsToAddClassification = new HashSet<>();
                            Set<String> verticesToExcludeDuringTraversal = new HashSet<>(
                                    Arrays.asList(fromVertex.getIdForDisplay(), sourceVertex.getIdForDisplay())
                            );
                            entityRetriever.traverseImpactedVerticesByLevelV2(toVertex, null, null, impactedVertexIds, CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode), toExclude, vertexIdsToAddClassification, verticesToExcludeDuringTraversal);

                            impactedVertexIds.addAll(vertexIdsToAddClassification);
                            impactedVertexIdsMap.put(propagationMode, impactedVertexIds);
                        } else {
                            LOG.info("propagateClassificationV2_Optimised: Cache hit for propagationMode '{}'. Reusing traversal results.", propagationMode);
                            impactedVertexIds = impactedVertexIdsMap.get(propagationMode);
                        }

                        // 2. Process additions in batches.
                        LOG.info("propagateClassificationV2_Optimised: Found {} total vertices for propagation based on toVertex: {}. Processing in batches.", impactedVertexIds.size(), toVertexGuid);
                        List<String> vertexIdsToAdd = new ArrayList<>(impactedVertexIds);

                        for (int i = 0; i < vertexIdsToAdd.size(); i += BATCH_SIZE_FOR_ADD_PROPAGATION) {
                            int end = Math.min(i + BATCH_SIZE_FOR_ADD_PROPAGATION, vertexIdsToAdd.size());
                            List<String> batchIds = vertexIdsToAdd.subList(i, end);

                            List<AtlasVertex> impactedVertices = batchIds.stream()
                                    .map(x -> graph.getVertex(x))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());

                            if (!impactedVertices.isEmpty()) {
                                assetsAffected += impactedVertices.size();
                                LOG.info("propagateClassificationV2_Optimised: Processing batch of {} assets for tag addition based on toVertex: {}, sourceVertex: {}, tag_type_name: {}", impactedVertices.size(), toVertexGuid, sourceVertex.getIdForDisplay(), atlasClassification.getTypeName());
                                processClassificationPropagationAdditionV2(parameters, sourceVertex.getIdForDisplay(), impactedVertices, atlasClassification);
                            }
                        }
                    }
                }
                return assetsAffected;
            }
        } catch (Exception e) {
            LOG.error("propagateClassificationV2_Optimised(entityGuid={}, classificationTypeName={}): error while propagating classification", entityGuid, tagTypeName, e);
            throw new AtlasBaseException(e);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }


    public List<String> processClassificationPropagationAddition(List<AtlasVertex> verticesToPropagate, AtlasVertex classificationVertex) throws AtlasBaseException{
        AtlasPerfMetrics.MetricRecorder classificationPropagationMetricRecorder = RequestContext.get().startMetricRecord("processClassificationPropagationAddition");
        List<String> propagatedEntitiesGuids = new ArrayList<>();
        int impactedVerticesSize = verticesToPropagate.size();
        int offset = 0;
        int toIndex;
        LOG.info(String.format("Total number of vertices to propagate: %d", impactedVerticesSize));

        try {
            do {
                toIndex = offset + CHUNK_SIZE > impactedVerticesSize ? impactedVerticesSize : offset + CHUNK_SIZE;
                List<AtlasVertex> chunkedVerticesToPropagate = verticesToPropagate.subList(offset, toIndex);

                AtlasPerfMetrics.MetricRecorder metricRecorder  = RequestContext.get().startMetricRecord("lockObjectsAfterTraverse");
                List<String> impactedVerticesGuidsToLock        = chunkedVerticesToPropagate.stream().map(x -> GraphHelper.getGuid(x)).collect(Collectors.toList());
                GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedVerticesGuidsToLock);
                RequestContext.get().endMetricRecord(metricRecorder);

                AtlasClassification classification       = entityRetriever.toAtlasClassification(classificationVertex);
                List<AtlasVertex>   entitiesPropagatedTo = deleteDelegate.getHandler().addTagPropagation(classificationVertex, chunkedVerticesToPropagate);

                if (CollectionUtils.isEmpty(entitiesPropagatedTo)) {
                    return Collections.emptyList();
                }

                List<AtlasEntity>   propagatedEntitiesChunked       = updateClassificationText(classification, entitiesPropagatedTo);
                List<String>        chunkedPropagatedEntitiesGuids  = propagatedEntitiesChunked.stream().map(x -> x.getGuid()).collect(Collectors.toList());

                propagatedEntitiesGuids.addAll(chunkedPropagatedEntitiesGuids);
                offset += CHUNK_SIZE;
                transactionInterceptHelper.intercept();
                entityChangeNotifier.onClassificationsAddedToEntities(propagatedEntitiesChunked, Collections.singletonList(classification), false);
            } while (offset < impactedVerticesSize);
        } catch (AtlasBaseException exception) {
            LOG.error("Error occurred while adding classification propagation for classification with propagation id {}", classificationVertex.getIdForDisplay());
            throw exception;
        } finally {
            RequestContext.get().endMetricRecord(classificationPropagationMetricRecorder);
        }

        return propagatedEntitiesGuids;

    }

    public int processClassificationPropagationAdditionV2(Map<String, Object> parameters,
                                                           String entityVertexId,
                                                           List<AtlasVertex> verticesToPropagate,
                                                           AtlasClassification classification) throws AtlasBaseException{
        AtlasPerfMetrics.MetricRecorder classificationPropagationMetricRecorder = RequestContext.get().startMetricRecord("processClassificationPropagationAddition");
        int impactedVerticesSize = verticesToPropagate.size();

        int offset = 0;
        int toIndex;
        LOG.info(String.format("Total number of vertices to propagate: %d", impactedVerticesSize));

        try {
            do {
                toIndex = Math.min(offset + CHUNK_SIZE, impactedVerticesSize);
                List<AtlasVertex> chunkedVerticesToPropagate = verticesToPropagate.subList(offset, toIndex);
                Set<AtlasVertex> chunkedVerticesToPropagateSet = new HashSet<>(chunkedVerticesToPropagate);
                Map<String, Map<String, Object>> deNormAttributesMap = new HashMap<>();
                Map<String, Map<String, Object>> assetMinAttrsMap = new HashMap<>();

                List<AtlasEntity> propagatedEntitiesChunked = updateClassificationTextV2(classification, chunkedVerticesToPropagate, deNormAttributesMap, assetMinAttrsMap);

                tagDAO.putPropagatedTags(entityVertexId, classification.getTypeName(), deNormAttributesMap.keySet(), assetMinAttrsMap, classification);
                if (MapUtils.isNotEmpty(deNormAttributesMap)) {
                    ESConnector.writeTagProperties(deNormAttributesMap);
                }
                entityChangeNotifier.onClassificationPropagationAddedToEntitiesV2(chunkedVerticesToPropagateSet, Collections.singletonList(classification), true, RequestContext.get()); // Async call
                offset += CHUNK_SIZE;
                LOG.info("offset {}, impactedVerticesSize: {}", offset, impactedVerticesSize);
            } while (offset < impactedVerticesSize);
            LOG.info(String.format("Total number of vertices propagated: %d", impactedVerticesSize));
            return impactedVerticesSize;
        } catch (Exception exception) {
            LOG.error("Error occurred while adding classification propagation for classification with source entity id {}",
                    entityVertexId, exception);
            throw exception;
        } finally {
            RequestContext.get().endMetricRecord(classificationPropagationMetricRecorder);
        }
    }

    public void deleteClassification(String entityGuid, String classificationName, String associatedEntityGuid) throws AtlasBaseException {
        if (StringUtils.isEmpty(associatedEntityGuid) || associatedEntityGuid.equals(entityGuid)) {
            handleDirectDeleteClassification(entityGuid, classificationName);
        } else {
            deletePropagatedClassification(entityGuid, classificationName, associatedEntityGuid);
        }
    }

    private void deletePropagatedClassification(String entityGuid, String classificationName, String associatedEntityGuid) throws AtlasBaseException {
        if (StringUtils.isEmpty(classificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "delete", entityGuid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        deleteDelegate.getHandler().deletePropagatedClassification(entityVertex, classificationName, associatedEntityGuid);
    }

    public void deleteClassificationV1(String entityGuid, String classificationName) throws AtlasBaseException {
        if (StringUtils.isEmpty(classificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "delete", entityGuid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityGraphMapper.deleteClassification");
        }

        List<String> traitNames = handleGetTraitNames(entityVertex);

        if (CollectionUtils.isEmpty(traitNames)) {
            throw new AtlasBaseException(AtlasErrorCode.NO_CLASSIFICATIONS_FOUND_FOR_ENTITY, entityGuid);
        }

        validateClassificationExists(traitNames, classificationName);

        AtlasVertex         classificationVertex = GraphHelper.getClassificationVertex(graphHelper, entityVertex, classificationName);

        if (Objects.isNull(classificationVertex)) {
            LOG.error(AtlasErrorCode.CLASSIFICATION_NOT_FOUND.getFormattedErrorMessage(classificationName));
            return;
        }
        // Get in progress task to see if there already is a propagation for this particular vertex
        List<AtlasTask> inProgressTasks = taskManagement.getInProgressTasks();
        for (AtlasTask task : inProgressTasks) {
            if (IN_PROGRESS.equals(task.getStatus()) && isTaskMatchingWithVertexIdAndEntityGuid(task, classificationVertex.getIdForDisplay(), entityGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_CURRENTLY_BEING_PROPAGATED, classificationName);
            }
        }

        AtlasClassification classification       = entityRetriever.toAtlasClassification(classificationVertex);

        if (classification == null) {
            LOG.error(AtlasErrorCode.CLASSIFICATION_NOT_FOUND.getFormattedErrorMessage(classificationName));
            return;
        }

        // remove classification from propagated entities if propagation is turned on
        final List<AtlasVertex> entityVertices;

        if (GraphHelper.isPropagationEnabled(classificationVertex)) {
            if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                boolean propagateDelete = true;
                String classificationVertexId = classificationVertex.getIdForDisplay();

                List<String> entityTaskGuids = (List<String>) entityVertex.getPropertyValues(PENDING_TASKS_PROPERTY_KEY, String.class);

                if (CollectionUtils.isNotEmpty(entityTaskGuids)) {
                    List<AtlasTask> entityPendingTasks = taskManagement.getByGuidsES(entityTaskGuids);

                    boolean pendingTaskExists  = entityPendingTasks.stream()
                            .anyMatch(x -> isTaskMatchingWithVertexIdAndEntityGuid(x, classificationVertexId, entityGuid));

                    if (pendingTaskExists) {
                        List<AtlasTask> entityClassificationPendingTasks = entityPendingTasks.stream()
                                .filter(t -> t.getParameters().containsKey("entityGuid")
                                        && t.getParameters().containsKey("classificationVertexId"))
                                .filter(t -> t.getParameters().get("entityGuid").equals(entityGuid)
                                        && t.getParameters().get("classificationVertexId").equals(classificationVertexId)
                                        && t.getType().equals(CLASSIFICATION_PROPAGATION_ADD))
                                .collect(Collectors.toList());
                        for (AtlasTask entityClassificationPendingTask: entityClassificationPendingTasks) {
                            String taskGuid = entityClassificationPendingTask.getGuid();
                            taskManagement.deleteByGuid(taskGuid, TaskManagement.DeleteType.SOFT);
                            AtlasGraphUtilsV2.deleteProperty(entityVertex, PENDING_TASKS_PROPERTY_KEY, taskGuid);
//                            propagateDelete = false;  TODO: Uncomment when all unnecessary ADD tasks are resolved
                        }
                    }
                }

                if (propagateDelete) {
                    createAndQueueTask(CLASSIFICATION_PROPAGATION_DELETE, entityVertex, classificationVertex.getIdForDisplay(), classificationName);
                }

                entityVertices = new ArrayList<>();
            } else {
                entityVertices = deleteDelegate.getHandler().removeTagPropagation(classificationVertex);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Number of propagations to delete -> {}", entityVertices.size());
                }
            }
        } else {
            entityVertices = new ArrayList<>();
        }

        // add associated entity to entityVertices list
        if (!entityVertices.contains(entityVertex)) {
            entityVertices.add(entityVertex);
        }

        // remove classifications from associated entity
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing classification: [{}] from: [{}][{}] with edge label: [{}]", classificationName,
                    getTypeName(entityVertex), entityGuid, CLASSIFICATION_LABEL);
        }

        AtlasEdge edge = getClassificationEdge(entityVertex, classificationVertex);

        deleteDelegate.getHandler().deleteEdgeReference(edge, CLASSIFICATION, false, true, entityVertex);

        traitNames.remove(classificationName);

        // update 'TRAIT_NAMES_PROPERTY_KEY' property
        entityVertex.removePropertyValue(TRAIT_NAMES_PROPERTY_KEY, classificationName);

        // update 'CLASSIFICATION_NAMES_KEY' property
        entityVertex.removeProperty(CLASSIFICATION_NAMES_KEY);

        entityVertex.setProperty(CLASSIFICATION_NAMES_KEY, getClassificationNamesString(traitNames));

        updateModificationMetadata(entityVertex);

        if (RequestContext.get().isDelayTagNotifications()) {
            RequestContext.get().addDeletedClassificationAndVertices(classification, new ArrayList<>(entityVertices));
        } else if (CollectionUtils.isNotEmpty(entityVertices)) {
            List<AtlasEntity> propagatedEntities = updateClassificationText(classification, entityVertices);

            //Sending audit request for all entities at once
            entityChangeNotifier.onClassificationsDeletedFromEntities(propagatedEntities, Collections.singletonList(classification));
        }
        AtlasPerfTracer.log(perf);
    }

    public void handleDirectDeleteClassification(String entityGuid, String classificationName) throws AtlasBaseException {
        if(FeatureFlagStore.isTagV2Enabled()) {
            deleteClassificationV2(entityGuid, classificationName);
        } else {
            deleteClassificationV1(entityGuid, classificationName);
        }
    }

    public void deleteClassificationV2(String entityGuid, String classificationName) throws AtlasBaseException {
        if (StringUtils.isEmpty(classificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "delete", entityGuid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityGraphMapper.deleteClassification");
        }

        Tag currentTag = tagDAO.findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(entityVertex.getIdForDisplay(), classificationName, false);
        if (Objects.isNull(currentTag)) {
            LOG.error(AtlasErrorCode.CLASSIFICATION_NOT_FOUND.getFormattedErrorMessage(classificationName));
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classificationName);
        }

        // Get in progress task to see if there already is a propagation for this particular vertex
        List<AtlasTask> inProgressTasks = taskManagement.getInProgressTasks();
        for (AtlasTask task : inProgressTasks) {
            if (IN_PROGRESS.equals(task.getStatus()) && isTaskMatchingWithVertexIdAndEntityGuid(task, currentTag.getTagTypeName(), entityGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_CURRENTLY_BEING_PROPAGATED, classificationName);
            }
        }

        AtlasClassification currentClassification = entityRetriever.toAtlasClassification(currentTag);

        // remove classification from propagated entities if propagation is turned on
        if (currentClassification.isPropagate()) {
            if (DEFERRED_ACTION_ENABLED) {
                List<String> entityTaskGuids = (List<String>) entityVertex.getPropertyValues(PENDING_TASKS_PROPERTY_KEY, String.class);

                if (CollectionUtils.isNotEmpty(entityTaskGuids)) {
                    List<AtlasTask> entityPendingTasks = taskManagement.getByGuidsES(entityTaskGuids);

                    boolean pendingTaskExists  = entityPendingTasks.stream()
                            .anyMatch(x -> isTaskMatchingWithVertexIdAndEntityGuid(x, currentClassification.getTypeName(), entityGuid));

                    if (pendingTaskExists) {
                        List<AtlasTask> entityClassificationPendingTasks = entityPendingTasks.stream()
                                .filter(t -> t.getParameters().containsKey("entityGuid")
                                        && t.getParameters().containsKey("classificationVertexId"))
                                .filter(t -> t.getParameters().get("entityGuid").equals(entityGuid)
                                        && t.getParameters().get(Constants.TASK_CLASSIFICATION_TYPENAME).equals(currentClassification.getTypeName())
                                        && t.getType().equals(CLASSIFICATION_PROPAGATION_ADD))
                                .collect(Collectors.toList());
                        for (AtlasTask entityClassificationPendingTask: entityClassificationPendingTasks) {
                            String taskGuid = entityClassificationPendingTask.getGuid();
                            taskManagement.deleteByGuid(taskGuid, TaskManagement.DeleteType.SOFT);
                            AtlasGraphUtilsV2.deleteProperty(entityVertex, PENDING_TASKS_PROPERTY_KEY, taskGuid);
//                            propagateDelete = false;  TODO: Uncomment when all unnecessary ADD tasks are resolved
                        }
                    }
                }

                String  currentUser = RequestContext.getCurrentUser();

                Map<String, Object> taskParams  = new HashMap<>() {{
                    put(PARAM_ENTITY_GUID, entityGuid);
                    put(PARAM_SOURCE_VERTEX_ID, entityVertex.getIdForDisplay());
                    put(TASK_CLASSIFICATION_TYPENAME, currentClassification.getTypeName());
                    put("newMode", true);
                }};

                taskManagement.createTaskV2(CLASSIFICATION_PROPAGATION_DELETE, currentUser, taskParams, currentClassification.getTypeName(), entityGuid);
            }
        }

        // remove classifications from associated entity
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing classification: [{}] from: [{}][{}] with edge label: [{}]", classificationName,
                    getTypeName(entityVertex), entityGuid, CLASSIFICATION_LABEL);
        }

        tagDAO.deleteDirectTag(entityVertex.getIdForDisplay(), currentClassification);

        RequestContext reqContext = RequestContext.get();
        // Record cassandra tag operation in RequestContext
        reqContext.addCassandraTagOperation(entityGuid,
                new CassandraTagOperation(
                        entityVertex.getIdForDisplay(),
                        classificationName,
                        CassandraTagOperation.OperationType.DELETE,
                        currentClassification.deepCopy(),
                        currentTag.getAssetMetadata())
        );

        List<AtlasClassification> currentTags = tagDAO.getAllClassificationsForVertex(entityVertex.getIdForDisplay());

        Map<String, Map<String, Object>> deNormMap = new HashMap<>();
        deNormMap.put(entityVertex.getIdForDisplay(), TagDeNormAttributesUtil.getDirectTagAttachmentAttributesForDeleteTag(currentClassification, currentTags, typeRegistry, fullTextMapperV2));

        // ES operation collected to be executed in the end
        RequestContext.get().addESDeferredOperation(
                new ESDeferredOperation(
                        ESDeferredOperation.OperationType.TAG_DENORM_FOR_DELETE_CLASSIFICATIONS,
                        entityVertex.getIdForDisplay(),
                        deNormMap
                )
        );

        updateModificationMetadata(entityVertex);

        if (RequestContext.get().isDelayTagNotifications()) {
            RequestContext.get().addDeletedClassificationAndVertices(currentClassification, Collections.singleton(entityVertex));
        } else {
            entityChangeNotifier.onClassificationDeletedFromEntities(Collections.singletonList(entityRetriever.toAtlasEntity(entityGuid)), currentClassification);
        }
        AtlasPerfTracer.log(perf);
    }

    private boolean isTaskMatchingWithVertexIdAndEntityGuid(AtlasTask task, String tagTypeName, String entityGuid) {
        try {
            if (CLASSIFICATION_PROPAGATION_ADD.equals(task.getType())) {
                return task.getParameters().get(Constants.TASK_CLASSIFICATION_TYPENAME).equals(tagTypeName)
                        && task.getParameters().get(ClassificationTask.PARAM_ENTITY_GUID).equals(entityGuid);
            }
        } catch (NullPointerException npe) {
            LOG.warn("Task classificationVertexId or entityGuid is null");
        }
        return false;
    }

    private AtlasEntity updateClassificationText(AtlasVertex vertex) throws AtlasBaseException {
        String guid        = graphHelper.getGuid(vertex);
        AtlasEntity entity = instanceConverter.getAndCacheEntity(guid, ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);

        vertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
        return entity;
    }

    public void updateClassificationTextAndNames(AtlasVertex vertex) throws AtlasBaseException {
        if(CollectionUtils.isEmpty(vertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class)) &&
                CollectionUtils.isEmpty(vertex.getPropertyValues(Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, String.class))) {
            return;
        }

        String guid = graphHelper.getGuid(vertex);
        AtlasEntity entity = instanceConverter.getAndCacheEntity(guid, ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);
        List<String> classificationNames = new ArrayList<>();
        List<String> propagatedClassificationNames = new ArrayList<>();

        for (AtlasClassification classification : entity.getClassifications()) {
            if (isPropagatedClassification(classification, guid)) {
                propagatedClassificationNames.add(classification.getTypeName());
            } else {
                classificationNames.add(classification.getTypeName());
            }
        }

        vertex.setProperty(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(classificationNames));
        vertex.setProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(propagatedClassificationNames));
        vertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
    }

    private boolean isPropagatedClassification(AtlasClassification classification, String guid) {
        String classificationEntityGuid = classification.getEntityGuid();

        return StringUtils.isNotEmpty(classificationEntityGuid) && !StringUtils.equals(classificationEntityGuid, guid);
    }

    private void addToClassificationNames(AtlasVertex entityVertex, String classificationName) {
        AtlasGraphUtilsV2.addEncodedProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, classificationName);

        String delimitedClassificationNames = entityVertex.getProperty(CLASSIFICATION_NAMES_KEY, String.class);

        if (StringUtils.isEmpty(delimitedClassificationNames)) {
            delimitedClassificationNames = CLASSIFICATION_NAME_DELIMITER + classificationName + CLASSIFICATION_NAME_DELIMITER;
        } else {
            delimitedClassificationNames = delimitedClassificationNames + classificationName + CLASSIFICATION_NAME_DELIMITER;
        }

        entityVertex.setProperty(CLASSIFICATION_NAMES_KEY, delimitedClassificationNames);
    }

    private String getClassificationNamesString(List<String> traitNames) {
        String ret = StringUtils.join(traitNames, CLASSIFICATION_NAME_DELIMITER);

        return StringUtils.isEmpty(ret) ? ret : CLASSIFICATION_NAME_DELIMITER + ret + CLASSIFICATION_NAME_DELIMITER;
    }

    public void updateClassificationsV1(EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "update", guid);
        }
        entityRetriever.verifyClassificationsPropagationMode(classifications);

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityGraphMapper.updateClassifications");
        }

        String                    entityTypeName         = AtlasGraphUtilsV2.getTypeName(entityVertex);
        AtlasEntityType           entityType             = typeRegistry.getEntityTypeByName(entityTypeName);
        List<AtlasClassification> updatedClassifications = new ArrayList<>();
        List<AtlasVertex>         entitiesToPropagateTo  = new ArrayList<>();
        Set<AtlasVertex>          notificationVertices   = new HashSet<AtlasVertex>() {{ add(entityVertex); }};

        Map<AtlasVertex, List<AtlasClassification>> addedPropagations   = null;
        Map<AtlasClassification, List<AtlasVertex>> removedPropagations = new HashMap<>();
        String propagationType = null;

        for (AtlasClassification classification : classifications) {
            String classificationName       = classification.getTypeName();
            String classificationEntityGuid = classification.getEntityGuid();

            if (StringUtils.isEmpty(classificationEntityGuid)) {
                classification.setEntityGuid(guid);
            }

            if (StringUtils.isNotEmpty(classificationEntityGuid) && !StringUtils.equalsIgnoreCase(guid, classificationEntityGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_UPDATE_FROM_PROPAGATED_ENTITY, classificationName);
            }

            AtlasVertex classificationVertex = GraphHelper.getClassificationVertex(graphHelper, entityVertex, classificationName);

            if (classificationVertex == null) {
                LOG.error(AtlasErrorCode.CLASSIFICATION_NOT_FOUND.getFormattedErrorMessage(classificationName));
                continue;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Updating classification {} for entity {}", classification, guid);
            }

            AtlasClassification currentClassification = entityRetriever.toAtlasClassification(classificationVertex);

            if (currentClassification == null) {
                continue;
            }

            validateAndNormalizeForUpdate(classification);

            boolean isClassificationUpdated = false;

            // check for attribute update
            Map<String, Object> updatedAttributes = classification.getAttributes();

            if (MapUtils.isNotEmpty(updatedAttributes)) {
                for (String attributeName : updatedAttributes.keySet()) {
                    currentClassification.setAttribute(attributeName, updatedAttributes.get(attributeName));
                }

                createAndQueueTask(CLASSIFICATION_PROPAGATION_TEXT_UPDATE, entityVertex, classificationVertex.getIdForDisplay(), classification.getTypeName());
            }

            // check for validity period update
            List<TimeBoundary> currentValidityPeriods = currentClassification.getValidityPeriods();
            List<TimeBoundary> updatedValidityPeriods = classification.getValidityPeriods();

            if (!Objects.equals(currentValidityPeriods, updatedValidityPeriods)) {
                currentClassification.setValidityPeriods(updatedValidityPeriods);

                isClassificationUpdated = true;
            }

            boolean removePropagation = false;
            // check for removePropagationsOnEntityDelete update
            Boolean currentRemovePropagations = currentClassification.getRemovePropagationsOnEntityDelete();
            Boolean updatedRemovePropagations = classification.getRemovePropagationsOnEntityDelete();
            if (updatedRemovePropagations != null && !updatedRemovePropagations.equals(currentRemovePropagations)) {
                AtlasGraphUtilsV2.setEncodedProperty(classificationVertex, CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY, updatedRemovePropagations);
                isClassificationUpdated = true;

                boolean isEntityDeleted = DELETED.toString().equals(entityVertex.getProperty(STATE_PROPERTY_KEY, String.class));
                if (isEntityDeleted && updatedRemovePropagations) {
                    removePropagation = true;
                }
            }

            if (isClassificationUpdated) {
                List<AtlasVertex> propagatedEntityVertices = graphHelper.getAllPropagatedEntityVertices(classificationVertex);
                notificationVertices.addAll(propagatedEntityVertices);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("updating vertex {} for trait {}", GraphHelper.string(classificationVertex), classificationName);
            }

            mapClassification(EntityOperation.UPDATE, context, classification, entityType, entityVertex, classificationVertex);
            updateModificationMetadata(entityVertex);

            /* -----------------------------
               | Current Tag | Updated Tag |
               | Propagation | Propagation |
               |-------------|-------------|
               |   true      |    true     | => no-op
               |-------------|-------------|
               |   false     |    false    | => no-op
               |-------------|-------------|
               |   false     |    true     | => Add Tag Propagation (send ADD classification notifications)
               |-------------|-------------|
               |   true      |    false    | => Remove Tag Propagation (send REMOVE classification notifications)
               |-------------|-------------| */

            Boolean currentTagPropagation = currentClassification.isPropagate();
            Boolean updatedTagPropagation = classification.isPropagate();
            Boolean currentRestrictPropagationThroughLineage = currentClassification.getRestrictPropagationThroughLineage();
            Boolean updatedRestrictPropagationThroughLineage = classification.getRestrictPropagationThroughLineage();
            Boolean currentRestrictPropagationThroughHierarchy = currentClassification.getRestrictPropagationThroughHierarchy();
            Boolean updatedRestrictPropagationThroughHierarchy = classification.getRestrictPropagationThroughHierarchy();
            if (updatedRestrictPropagationThroughLineage == null) {
                updatedRestrictPropagationThroughLineage = currentRestrictPropagationThroughLineage;
                classification.setRestrictPropagationThroughLineage(updatedRestrictPropagationThroughLineage);
            }
            if (updatedRestrictPropagationThroughHierarchy == null) {
                updatedRestrictPropagationThroughHierarchy = currentRestrictPropagationThroughHierarchy;
                classification.setRestrictPropagationThroughHierarchy(updatedRestrictPropagationThroughHierarchy);
            }

            String propagationMode = CLASSIFICATION_PROPAGATION_MODE_DEFAULT;
            if (updatedTagPropagation) {
                // determinePropagationMode also validates the propagation restriction option values
                propagationMode = entityRetriever.determinePropagationMode(updatedRestrictPropagationThroughLineage, updatedRestrictPropagationThroughHierarchy);
            }

            if ((!Objects.equals(updatedRemovePropagations, currentRemovePropagations) ||
                    !Objects.equals(currentTagPropagation, updatedTagPropagation) ||
                    !Objects.equals(currentRestrictPropagationThroughLineage, updatedRestrictPropagationThroughLineage)) &&
                    taskManagement != null && DEFERRED_ACTION_ENABLED) {

                propagationType = CLASSIFICATION_PROPAGATION_ADD;
                if(currentRestrictPropagationThroughLineage != updatedRestrictPropagationThroughLineage || currentRestrictPropagationThroughHierarchy != updatedRestrictPropagationThroughHierarchy){
                    propagationType = CLASSIFICATION_REFRESH_PROPAGATION;
                }
                if (removePropagation || !updatedTagPropagation) {
                    propagationType = CLASSIFICATION_PROPAGATION_DELETE;
                }
                createAndQueueTask(propagationType, entityVertex, classificationVertex.getIdForDisplay(), classificationName, currentRestrictPropagationThroughLineage,currentRestrictPropagationThroughHierarchy);
                updatedTagPropagation = null;
            }

            // compute propagatedEntityVertices once and use it for subsequent iterations and notifications
            if (updatedTagPropagation != null && (currentTagPropagation != updatedTagPropagation || currentRestrictPropagationThroughLineage != updatedRestrictPropagationThroughLineage || currentRestrictPropagationThroughHierarchy != updatedRestrictPropagationThroughHierarchy)) {
                if (updatedTagPropagation) {
                    if (updatedRestrictPropagationThroughLineage != null && !currentRestrictPropagationThroughLineage && updatedRestrictPropagationThroughLineage) {
                        deleteDelegate.getHandler().removeTagPropagation(classificationVertex);
                    }
                    if (updatedRestrictPropagationThroughHierarchy != null && !currentRestrictPropagationThroughHierarchy && updatedRestrictPropagationThroughHierarchy) {
                        deleteDelegate.getHandler().removeTagPropagation(classificationVertex);
                    }
                    if (CollectionUtils.isEmpty(entitiesToPropagateTo)) {
                        if (updatedRemovePropagations ==null) {
                            propagationMode = CLASSIFICATION_PROPAGATION_MODE_DEFAULT;
                        }
                        Boolean toExclude = propagationMode == CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE ? true : false;
                        entitiesToPropagateTo = entityRetriever.getImpactedVerticesV2(entityVertex, null, classificationVertex.getIdForDisplay(), CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude);
                    }

                    if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                        if (addedPropagations == null) {
                            addedPropagations = new HashMap<>(entitiesToPropagateTo.size());

                            for (AtlasVertex entityToPropagateTo : entitiesToPropagateTo) {
                                addedPropagations.put(entityToPropagateTo, new ArrayList<>());
                            }
                        }

                        List<AtlasVertex> entitiesPropagatedTo = deleteDelegate.getHandler().addTagPropagation(classificationVertex, entitiesToPropagateTo);

                        if (entitiesPropagatedTo != null) {
                            for (AtlasVertex entityPropagatedTo : entitiesPropagatedTo) {
                                addedPropagations.get(entityPropagatedTo).add(classification);
                            }
                        }
                    }
                } else {
                    List<AtlasVertex> impactedVertices = deleteDelegate.getHandler().removeTagPropagation(classificationVertex);

                    if (CollectionUtils.isNotEmpty(impactedVertices)) {
                        /*
                            removedPropagations is a HashMap of entity against list of classifications i.e. for each entity 1 entry in the map.
                            Maintaining classification wise entity list lets us send the audit request in bulk,
                            since 1 classification is applied to many entities (including the child entities).
                            Eg. If a classification is being propagated to 1000 entities, its edge count would be 2000, as per removedPropagations map
                            we would have 2000 entries and value would always be 1 classification wrapped in a list.
                            By this rearrangement we maintain an entity list against each classification, as of now its entry size would be 1 (as per request from UI)
                            instead of 2000. Moreover this allows us to send audit request classification wise instead of separate requests for each entities.
                            This reduces audit calls from 2000 to 1.
                         */
                        removedPropagations.put(classification, impactedVertices);
                    }
                }
            }

            updatedClassifications.add(currentClassification);
        }

        if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
            notificationVertices.addAll(entitiesToPropagateTo);
        }

        for (AtlasVertex vertex : notificationVertices) {
            String      entityGuid = graphHelper.getGuid(vertex);
            AtlasEntity entity     = instanceConverter.getAndCacheEntity(entityGuid, ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);

            if (entity != null) {
                vertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
                entityChangeNotifier.onClassificationUpdatedToEntity(entity, updatedClassifications);
            }
        }

        if (MapUtils.isNotEmpty(removedPropagations)) {
            for (AtlasClassification classification : removedPropagations.keySet()) {
                List<AtlasVertex> propagatedVertices = removedPropagations.get(classification);
                List<AtlasEntity> propagatedEntities = updateClassificationText(classification, propagatedVertices);

                //Sending audit request for all entities at once
                entityChangeNotifier.onClassificationsDeletedFromEntities(propagatedEntities, Collections.singletonList(classification));
            }
        }

        AtlasPerfTracer.log(perf);
    }

    public void handleUpdateClassifications(EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (FeatureFlagStore.isTagV2Enabled()) {
            updateClassificationsV2(guid, classifications);
        } else {
            updateClassificationsV1(context, guid, classifications);
        }
    }

    public void updateClassificationsV2(String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "update", guid);
        }

        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityGraphMapper.updateClassificationsV2");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);
        if (entityVertex == null)
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);

        entityRetriever.verifyClassificationsPropagationMode(classifications);
        for (AtlasClassification classification : classifications) {
            String classificationName       = classification.getTypeName();
            String classificationEntityGuid = classification.getEntityGuid();

            if (StringUtils.isNotEmpty(classificationEntityGuid) && !StringUtils.equalsIgnoreCase(guid, classificationEntityGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_UPDATE_FROM_PROPAGATED_ENTITY, classificationName);
            }
            validateAndNormalizeForUpdate(classification);
        }

        classifications = mapClassificationsV2(classifications);

        List<AtlasClassification> updatedClassifications = new ArrayList<>();
        List<AtlasVertex>         entitiesToPropagateTo  = new ArrayList<>();
        Set<AtlasVertex>          notificationVertices   = new HashSet<>() {{ add(entityVertex); }};

        Map<AtlasClassification, List<AtlasVertex>> removedPropagations = new HashMap<>();
        String propagationType;

        for (AtlasClassification classification : classifications) {
            String classificationName       = classification.getTypeName();
            String classificationEntityGuid = classification.getEntityGuid();

            if (StringUtils.isEmpty(classificationEntityGuid)) {
                classification.setEntityGuid(guid);
            }

            //AtlasVertex classificationVertex = getClassificationVertex(graphHelper, entityVertex, classificationName);
            Tag currentTag = tagDAO.findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(entityVertex.getIdForDisplay(), classificationName, false);
            if (currentTag == null) {
                LOG.error(AtlasErrorCode.CLASSIFICATION_NOT_FOUND.getFormattedErrorMessage(classificationName));
                continue;
            }
            AtlasClassification currentClassification = entityRetriever.toAtlasClassification(currentTag);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Updating classification {} for entity {}", classification, guid);
            }

            List<AtlasClassification> currentTags = tagDAO.getAllClassificationsForVertex(entityVertex.getIdForDisplay());
            currentTags = currentTags.stream()
                    .filter(tag -> !(tag.getEntityGuid().equals(classification.getEntityGuid()) && tag.getTypeName().equals(classification.getTypeName())))
                    .collect(Collectors.toList());
            currentTags.add(classification);
            // Update tag
            Map<String, Object> minAssetMap = getMinimalAssetMap(entityVertex);
            tagDAO.putDirectTag(entityVertex.getIdForDisplay(), classificationName, classification, minAssetMap);

            RequestContext reqContext = RequestContext.get();
            // Record cassandra tag operation in RequestContext
            reqContext.addCassandraTagOperation(guid,
                    new CassandraTagOperation(
                            entityVertex.getIdForDisplay(),
                            classificationName,
                            CassandraTagOperation.OperationType.UPDATE,
                            currentClassification.deepCopy(),
                            currentTag.getAssetMetadata()
                    )
            );
            Map<String, Map<String, Object>> deNormMap = new HashMap<>();
            deNormMap.put(entityVertex.getIdForDisplay(), TagDeNormAttributesUtil.getDirectTagAttachmentAttributesForAddTag(classification,
                    currentTags, typeRegistry, fullTextMapperV2));
            // ES operation collected to be executed in the end
            RequestContext.get().addESDeferredOperation(
                    new ESDeferredOperation(
                            ESDeferredOperation.OperationType.TAG_DENORM_FOR_UPDATE_CLASSIFICATIONS,
                            entityVertex.getIdForDisplay(),
                            deNormMap
                    )
            );

            // check for attribute update
            Map<String, Object> updatedAttributes = classification.getAttributes();

            if (MapUtils.isNotEmpty(updatedAttributes)) {
                for (String attributeName : updatedAttributes.keySet()) {
                    currentClassification.setAttribute(attributeName, updatedAttributes.get(attributeName));
                }

                String              currentUser = RequestContext.getCurrentUser();
                String              entityGuid  = GraphHelper.getGuid(entityVertex);

                Map<String, Object> taskParams  = new HashMap<String, Object>() {{
                    put(PARAM_ENTITY_GUID, entityGuid);
                }};

                taskManagement.createTaskV2(CLASSIFICATION_PROPAGATION_TEXT_UPDATE, currentUser, taskParams, classification.getTypeName(), entityGuid);
            }

            // check for validity period update
            List<TimeBoundary> currentValidityPeriods = currentClassification.getValidityPeriods();
            List<TimeBoundary> updatedValidityPeriods = classification.getValidityPeriods();

            if (!Objects.equals(currentValidityPeriods, updatedValidityPeriods)) {
                currentClassification.setValidityPeriods(updatedValidityPeriods);
            }

            boolean removePropagation = false;
            // check for removePropagationsOnEntityDelete update
            Boolean currentRemovePropagations = currentClassification.getRemovePropagationsOnEntityDelete();
            Boolean updatedRemovePropagations = classification.getRemovePropagationsOnEntityDelete();
            if (updatedRemovePropagations != null && !updatedRemovePropagations.equals(currentRemovePropagations)) {

                boolean isEntityDeleted = DELETED.toString().equals(entityVertex.getProperty(STATE_PROPERTY_KEY, String.class));
                if (isEntityDeleted && updatedRemovePropagations) {
                    removePropagation = true;
                }
            }

            updateModificationMetadata(entityVertex);

            /* -----------------------------
               | Current Tag | Updated Tag |
               | Propagation | Propagation |
               |-------------|-------------|
               |   true      |    true     | => no-op
               |-------------|-------------|
               |   false     |    false    | => no-op
               |-------------|-------------|
               |   false     |    true     | => Add Tag Propagation (send ADD classification notifications)
               |-------------|-------------|
               |   true      |    false    | => Remove Tag Propagation (send REMOVE classification notifications)
               |-------------|-------------| */

            Boolean currentTagPropagation = currentClassification.isPropagate();
            Boolean updatedTagPropagation = classification.isPropagate();
            Boolean currentRestrictPropagationThroughLineage = currentClassification.getRestrictPropagationThroughLineage();
            Boolean updatedRestrictPropagationThroughLineage = classification.getRestrictPropagationThroughLineage();
            Boolean currentRestrictPropagationThroughHierarchy = currentClassification.getRestrictPropagationThroughHierarchy();
            Boolean updatedRestrictPropagationThroughHierarchy = classification.getRestrictPropagationThroughHierarchy();
            if (updatedRestrictPropagationThroughLineage == null) {
                updatedRestrictPropagationThroughLineage = currentRestrictPropagationThroughLineage;
                classification.setRestrictPropagationThroughLineage(updatedRestrictPropagationThroughLineage);
            }
            if (updatedRestrictPropagationThroughHierarchy == null) {
                updatedRestrictPropagationThroughHierarchy = currentRestrictPropagationThroughHierarchy;
                classification.setRestrictPropagationThroughHierarchy(updatedRestrictPropagationThroughHierarchy);
            }

            if (updatedTagPropagation)
                entityRetriever.validatePropagationRestrictionOptions(updatedRestrictPropagationThroughLineage, updatedRestrictPropagationThroughHierarchy);

            if ((!Objects.equals(updatedRemovePropagations, currentRemovePropagations) ||
                    !Objects.equals(currentTagPropagation, updatedTagPropagation) ||
                    !Objects.equals(currentRestrictPropagationThroughLineage, updatedRestrictPropagationThroughLineage) ||
                    !Objects.equals(currentRestrictPropagationThroughHierarchy, updatedRestrictPropagationThroughHierarchy)) &&
                    taskManagement != null && DEFERRED_ACTION_ENABLED) {

                propagationType = CLASSIFICATION_PROPAGATION_ADD;
                if(currentRestrictPropagationThroughLineage != updatedRestrictPropagationThroughLineage || currentRestrictPropagationThroughHierarchy != updatedRestrictPropagationThroughHierarchy){
                    propagationType = CLASSIFICATION_REFRESH_PROPAGATION;
                }
                if (removePropagation || !updatedTagPropagation) {
                    propagationType = CLASSIFICATION_PROPAGATION_DELETE;
                }

                String  currentUser = RequestContext.getCurrentUser();
                String  entityGuid  = GraphHelper.getGuid(entityVertex);

                Map<String, Object> taskParams  = new HashMap<>() {{
                    put(PARAM_ENTITY_GUID, entityGuid);
                    put(PARAM_SOURCE_VERTEX_ID, entityVertex.getIdForDisplay());
                }};

                taskManagement.createTaskV2(propagationType, currentUser, taskParams, classification.getTypeName(), entityGuid);
            }

            updatedClassifications.add(currentClassification);
        }

        if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
            notificationVertices.addAll(entitiesToPropagateTo);
        }

        for (AtlasVertex vertex : notificationVertices) {
            String      entityGuid = graphHelper.getGuid(vertex);
            AtlasEntity entity     = instanceConverter.getAndCacheEntity(entityGuid, ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);

            if (entity != null) {
                vertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
                entityChangeNotifier.onClassificationUpdatedToEntity(entity, updatedClassifications);
            }
        }

        if (MapUtils.isNotEmpty(removedPropagations)) {
            for (AtlasClassification classification : removedPropagations.keySet()) {
                List<AtlasVertex> propagatedVertices = removedPropagations.get(classification);
                List<AtlasEntity> propagatedEntities = updateClassificationText(classification, propagatedVertices);

                //Sending audit request for all entities at once
                entityChangeNotifier.onClassificationsDeletedFromEntities(propagatedEntities, Collections.singletonList(classification));
            }
        }

        AtlasPerfTracer.log(perf);
    }

    private AtlasEdge mapClassification(EntityOperation operation,  final EntityMutationContext context, AtlasClassification classification,
                                        AtlasEntityType entityType, AtlasVertex parentInstanceVertex, AtlasVertex traitInstanceVertex)
            throws AtlasBaseException {
        if (classification.getValidityPeriods() != null) {
            String strValidityPeriods = AtlasJson.toJson(classification.getValidityPeriods());

            AtlasGraphUtilsV2.setEncodedProperty(traitInstanceVertex, CLASSIFICATION_VALIDITY_PERIODS_KEY, strValidityPeriods);
        } else {
            // if 'null', don't update existing value in the classification
        }

        if (classification.isPropagate() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(traitInstanceVertex, CLASSIFICATION_VERTEX_PROPAGATE_KEY, classification.isPropagate());
        }

        if (classification.getRemovePropagationsOnEntityDelete() != null) {
            AtlasGraphUtilsV2.setEncodedProperty(traitInstanceVertex, CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY, classification.getRemovePropagationsOnEntityDelete());
        }

        if(classification.getRestrictPropagationThroughLineage() != null){
            AtlasGraphUtilsV2.setEncodedProperty(traitInstanceVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, classification.getRestrictPropagationThroughLineage());
        }

        if(classification.getRestrictPropagationThroughHierarchy() != null){
            AtlasGraphUtilsV2.setEncodedProperty(traitInstanceVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, classification.getRestrictPropagationThroughHierarchy());
        }

        // map all the attributes to this newly created AtlasVertex
        mapAttributes(classification, traitInstanceVertex, operation, context);

        AtlasEdge ret = getClassificationEdge(parentInstanceVertex, traitInstanceVertex);
        // TODO :  Edge is created with correct ref. but vertices are not connecting or referencing back to edge
        if (ret == null) {
            ret = graphHelper.addClassificationEdge(parentInstanceVertex, traitInstanceVertex, false);
        }

        return ret;
    }

    public void deleteClassifications(String guid) throws AtlasBaseException {
        AtlasVertex instanceVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

        if (instanceVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        List<String> traitNames = handleGetTraitNames(instanceVertex);

        if (CollectionUtils.isNotEmpty(traitNames)) {
            for (String traitName : traitNames) {
                handleDirectDeleteClassification(guid, traitName);
            }
        }
    }

    public int updateClassificationTextPropagation(String classificationVertexId) throws AtlasBaseException {
        if (StringUtils.isEmpty(classificationVertexId)) {
            LOG.warn("updateClassificationTextPropagation(classificationVertexId={}): classification vertex id is empty", classificationVertexId);
            return 0;
        }
        AtlasVertex classificationVertex = graph.getVertex(classificationVertexId);
        AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);
        LOG.info("Fetched classification : {} ", classification.toString());
        List<AtlasVertex> impactedVertices = graphHelper.getAllPropagatedEntityVertices(classificationVertex);
        LOG.info("impactedVertices : {}", impactedVertices.size());
        int batchSize = 100;
        int totalUpdated = 0;
        for (int i = 0; i < impactedVertices.size(); i += batchSize) {
            int end = Math.min(i + batchSize, impactedVertices.size());
            List<AtlasVertex> batch = impactedVertices.subList(i, end);
            List<AtlasEntity> entityBatch = new ArrayList<>();
            for (AtlasVertex vertex : batch) {
                String entityGuid = graphHelper.getGuid(vertex);
                AtlasEntity entity = instanceConverter.getAndCacheEntity(entityGuid, true);

                if (entity != null) {
                    vertex.setProperty(CLASSIFICATION_TEXT_KEY, fullTextMapperV2.getClassificationTextForEntity(entity));
                    entityBatch.add(entity);
                }
            }
            totalUpdated += entityBatch.size();
            transactionInterceptHelper.intercept();
            entityChangeNotifier.onClassificationUpdatedToEntities(entityBatch, classification); // Async call - fire and forget
            LOG.info("Updated classificationText from {} for {}", i, batchSize);
        }
        return totalUpdated;
    }

    public List<String> deleteClassificationPropagation(String entityGuid, String classificationVertexId) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(classificationVertexId)) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification vertex id is empty", classificationVertexId);
                return Collections.emptyList();
            }

            AtlasVertex classificationVertex = graph.getVertex(classificationVertexId);
            if (classificationVertex == null) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification vertex not found", classificationVertexId);
                return Collections.emptyList();
            }

            AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);

            List<AtlasEdge> propagatedEdges = getPropagatedEdges(classificationVertex);
            if (propagatedEdges.isEmpty()) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification edges empty", classificationVertexId);
                return Collections.emptyList();
            }

            int propagatedEdgesSize = propagatedEdges.size();

            LOG.info(String.format("Number of edges to be deleted : %s for classification vertex with id : %s", propagatedEdgesSize, classificationVertexId));

            List<String> deletedPropagationsGuid = processClassificationEdgeDeletionInChunk(classification, propagatedEdges);

            deleteDelegate.getHandler().deleteClassificationVertex(classificationVertex, true);

            transactionInterceptHelper.intercept();

            return deletedPropagationsGuid;
        } catch (Exception e) {
            LOG.error("Error while removing classification id {} with error {} ", classificationVertexId, e.getMessage());
            throw new AtlasBaseException(e);
        }
    }


    public int deleteClassificationPropagationV2(String sourceEntityGuid, String sourceVertexId, String parentEntityGuid, String tagTypeName) throws AtlasBaseException {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deleteClassificationPropagationNew");
        try {
            if (StringUtils.isEmpty(tagTypeName)) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification type name is empty", tagTypeName);
                return 0;
            }

            String vertexIdForPropagations = sourceVertexId;

            if (StringUtils.isNotEmpty(parentEntityGuid)) {
                AtlasVertex parentVertex = graphHelper.getVertexForGUID(parentEntityGuid);
                if (parentVertex != null) {
                    // If a parent is involved and still exists, use its ID.
                    vertexIdForPropagations = parentVertex.getIdForDisplay();
                }
            }

            int totalDeleted = 0;
            PaginatedTagResult pageToDelete;

            pageToDelete = tagDAO.getPropagationsForAttachmentBatch(vertexIdForPropagations, tagTypeName, null);

            List<Tag> batchToDelete = pageToDelete.getTags();
            AtlasClassification originalClassification;

            AtlasClassification deletedClassification = tagDAO.findDirectDeletedTagByVertexIdAndTagTypeName(vertexIdForPropagations, tagTypeName);
            if (deletedClassification != null)
                originalClassification = deletedClassification;
            else
                originalClassification = tagDAO.findDirectTagByVertexIdAndTagTypeName(vertexIdForPropagations, tagTypeName, false);

            if (originalClassification == null) {
                LOG.error("propagateClassification(entityGuid={}, tagTypeName={}): classification vertex not found", sourceEntityGuid, tagTypeName);
                throw new AtlasBaseException(String.format("propagateClassification(entityGuid=%s, tagTypeName=%s): classification vertex not found", sourceEntityGuid, tagTypeName));
            }

            while (!batchToDelete.isEmpty()) {
                // collect the vertex IDs in this batch
                List<String> vertexIds = batchToDelete.stream()
                        .map(Tag::getVertexId)
                        .toList();

                List<AtlasEntity> entities = batchToDelete.stream().map(x->getEntityForNotification(x.getAssetMetadata())).toList();

                // Delete from Cassandra. The DAO correctly performs a hard delete on the lookup table.
                deletePropagations(batchToDelete);

                // compute fresh classification‑text de‑norm attributes for this batch
                Map<String, Map<String, Object>> deNormMap = new HashMap<>();
                updateClassificationTextV2(originalClassification, vertexIds, batchToDelete, deNormMap);
                // push them to ES
                if (MapUtils.isNotEmpty(deNormMap)) {
                    ESConnector.writeTagProperties(deNormMap);
                }

                Set<AtlasVertex> vertices = graph.getVertices(vertexIds.toArray(new String[0]));

                // notify listeners (async)
                entityChangeNotifier.onClassificationPropagationDeletedV2(vertices, originalClassification, true, RequestContext.get());

                totalDeleted += batchToDelete.size();

                // grab next batch. The loop terminates correctly when the DAO reports it is done.
                if (pageToDelete.isDone()) {
                    break;
                }
                String pagingState = pageToDelete.getPagingState();
                pageToDelete = tagDAO.getPropagationsForAttachmentBatch(vertexIdForPropagations, tagTypeName, pagingState);
                batchToDelete = pageToDelete.getTags();
            }

            LOG.info("Updated classification text for {} propagations, taskId: {}",
                    totalDeleted, RequestContext.get().getCurrentTask().getGuid());
            return totalDeleted;
        } catch (Exception e) {
            LOG.error("Error while updating classification text for tag type {}: {}", tagTypeName, e.getMessage());
            throw new AtlasBaseException(e);
        } finally {
            // end metrics
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public int deletePropagations(List<Tag> batchToDelete) throws AtlasBaseException {
        if(batchToDelete.isEmpty())
            return 0;
        tagDAO.deleteTags(batchToDelete);
        return batchToDelete.size();
    }

    public int classificationRefreshPropagation(String classificationId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder classificationRefreshPropagationMetricRecorder = RequestContext.get().startMetricRecord("classificationRefreshPropagation");

        AtlasVertex currentClassificationVertex             = graph.getVertex(classificationId);
        if (currentClassificationVertex == null) {
            LOG.warn("Classification vertex with ID {} is deleted", classificationId);
            return 0;
        }

        String              sourceEntityId                  = getClassificationEntityGuid(currentClassificationVertex);
        AtlasVertex         sourceEntityVertex              = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityId);
        AtlasClassification classification                  = entityRetriever.toAtlasClassification(currentClassificationVertex);

        String propagationMode;

        Boolean restrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);
        Boolean restrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);
        // TODO : Why is refresh propagation being triggered for a child asset with the tag-vertex of the parent asset. The line :
        //  List<String> verticesIdsToRemove = (List<String>)CollectionUtils.subtract(propagatedVerticesIds, impactedVertices);
        //  Will Remove (whole-graph - sub-graph)
        // The above is resolved, but keeping the TODO until full release of the code incase the source-task-creation logic is under scrutiny
        propagationMode = entityRetriever.determinePropagationMode(restrictPropagationThroughLineage,restrictPropagationThroughHierarchy);
        Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true:false;

        List<String> propagatedVerticesIds = GraphHelper.getPropagatedVerticesIds(currentClassificationVertex); //  get this in whole, not in batch
        LOG.info("{} entity vertices have classification with id {} attached", propagatedVerticesIds.size(), classificationId);
        // verticesToRemove -> simple row removal from cassandra table
        List<String> verticesIdsToAddClassification =  new ArrayList<>();
        List<String> impactedVertices = entityRetriever.getImpactedVerticesIdsClassificationAttached(sourceEntityVertex , classificationId,
                CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude, verticesIdsToAddClassification);

        LOG.info("To add classification with id {} to {} vertices", classificationId, verticesIdsToAddClassification.size());

        List<String> verticesIdsToRemove = (List<String>)CollectionUtils.subtract(propagatedVerticesIds, impactedVertices);

        List<AtlasVertex> verticesToRemove = verticesIdsToRemove.stream()
                .map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());

        List<AtlasVertex> verticesToAddClassification  = verticesIdsToAddClassification.stream()
                .map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());

        //Remove classifications from unreachable vertices
        processPropagatedClassificationDeletionFromVertices(verticesToRemove, currentClassificationVertex, classification);

        //Add classification to the reachable vertices
        if (CollectionUtils.isEmpty(verticesToAddClassification)) {
            LOG.debug("propagateClassification(entityGuid={}, classificationVertexId={}): found no entities to propagate the classification", sourceEntityId, classificationId);
            // return only
            return verticesToRemove.size();
        }
        processClassificationPropagationAddition(verticesToAddClassification, currentClassificationVertex);

        LOG.info("Completed refreshing propagation for classification with vertex id {} with classification name {} and source entity {}",classificationId,
            classification.getTypeName(), classification.getEntityGuid());

        RequestContext.get().endMetricRecord(classificationRefreshPropagationMetricRecorder);
        return verticesToAddClassification.size() + verticesToRemove.size();
    }

    private void processPropagatedClassificationDeletionFromVertices(List<AtlasVertex> VerticesToRemoveTag, AtlasVertex classificationVertex, AtlasClassification classification) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder propagatedClassificationDeletionMetricRecorder = RequestContext.get().startMetricRecord("processPropagatedClassificationDeletionFromVertices");

        int propagatedVerticesSize = VerticesToRemoveTag.size();
        int toIndex;
        int offset = 0;

        LOG.info("To delete classification of vertex id {} from {} entity vertices", classificationVertex.getIdForDisplay(), propagatedVerticesSize);

        try {
            do {
                toIndex = ((offset + CHUNK_SIZE > propagatedVerticesSize) ? propagatedVerticesSize : (offset + CHUNK_SIZE));
                List<AtlasVertex> verticesChunkToRemoveTag = VerticesToRemoveTag.subList(offset, toIndex);

                List<String> impactedGuids = verticesChunkToRemoveTag.stream()
                        .map(entityVertex -> GraphHelper.getGuid(entityVertex))
                        .collect(Collectors.toList());
                GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedGuids);

                List<AtlasVertex> updatedVertices = deleteDelegate.getHandler().removeTagPropagation(classificationVertex, verticesChunkToRemoveTag);
                List<AtlasEntity> updatedEntities = updateClassificationText(classification, updatedVertices);

                offset += CHUNK_SIZE;

                transactionInterceptHelper.intercept();
                entityChangeNotifier.onClassificationsDeletedFromEntities(updatedEntities, Collections.singletonList(classification));
            } while (offset < propagatedVerticesSize);
        } catch (AtlasBaseException exception) {
            LOG.error("Error while removing classification from vertices with classification vertex id {}", classificationVertex.getIdForDisplay());
            throw exception;
        } finally {
            RequestContext.get().endMetricRecord(propagatedClassificationDeletionMetricRecorder);
        }
    }

    List<String> processClassificationEdgeDeletionInChunk(AtlasClassification classification, List<AtlasEdge> propagatedEdges) throws AtlasBaseException {
        List<String> deletedPropagationsGuid = new ArrayList<>();
        int propagatedEdgesSize = propagatedEdges.size();
        int toIndex;
        int offset = 0;

        do {
            toIndex = ((offset + CHUNK_SIZE > propagatedEdgesSize) ? propagatedEdgesSize : (offset + CHUNK_SIZE));

            List<AtlasVertex> entityVertices = deleteDelegate.getHandler().removeTagPropagation(classification, propagatedEdges.subList(offset, toIndex));
            List<String> impactedGuids = entityVertices.stream().map(x -> GraphHelper.getGuid(x)).collect(Collectors.toList());

            GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedGuids);

            List<AtlasEntity>  propagatedEntities = updateClassificationText(classification, entityVertices);

            Set<AtlasVertex> propagatedAtlasVertices = new HashSet<>(entityVertices);

            if(! propagatedEntities.isEmpty()) {
                deletedPropagationsGuid.addAll(propagatedEntities.stream().map(x -> x.getGuid()).collect(Collectors.toList()));
            }

            offset += CHUNK_SIZE;
            transactionInterceptHelper.intercept();
            entityChangeNotifier.onClassificationDeletedFromEntitiesV2(propagatedAtlasVertices, classification);
        } while (offset < propagatedEdgesSize);

        return deletedPropagationsGuid;
    }

    @GraphTransaction
    public void updateTagPropagations(String relationshipEdgeId, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasEdge relationshipEdge = graph.getEdge(relationshipEdgeId);

        deleteDelegate.getHandler().updateTagPropagations(relationshipEdge, relationship);

        entityChangeNotifier.notifyPropagatedEntities();
    }

    private void validateClassificationExists(List<String> existingClassifications, String suppliedClassificationName) throws AtlasBaseException {
        if (!existingClassifications.contains(suppliedClassificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, suppliedClassificationName);
        }
    }

    private AtlasEdge getOrCreateRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, String relationshipName,
                                              Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        return relationshipStore.getOrCreate(end1Vertex, end2Vertex, new AtlasRelationship(relationshipName, relationshipAttributes), false);
    }

    private void recordEntityUpdate(AtlasVertex vertex) throws AtlasBaseException {
        if (vertex != null) {
            RequestContext req = RequestContext.get();

            if (!req.isUpdatedEntity(graphHelper.getGuid(vertex))) {
                updateModificationMetadata(vertex);

                req.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(vertex));
            }
        }
    }

    /*
     * vertex - Opposite entity which is being referred in relationshipAttributes
     * ctx.getReferringVertex() - Original entity which is being created/updated
     *
     * */
    private void recordEntityUpdate(AtlasVertex vertex, AttributeMutationContext ctx, boolean isAdd) throws AtlasBaseException {
        if (vertex != null) {
            RequestContext req = RequestContext.get();

            AtlasEntityHeader header = new AtlasEntityHeader(getTypeName(vertex));
            header.setGuid(GraphHelper.getGuid(vertex));
            header.setCreateTime(new Date(getCreatedTime(vertex)));
            header.setUpdateTime(new Date(getModifiedTime(vertex)));
            header.setCreatedBy(getCreatedByAsString(vertex));
            header.setUpdatedBy(getModifiedByAsString(vertex));
            header.setAttribute(NAME, vertex.getProperty(NAME, String.class));
            header.setAttribute(QUALIFIED_NAME, vertex.getProperty(QUALIFIED_NAME, String.class));

            header.setDocId(LongEncoding.encode(Long.parseLong(vertex.getIdForDisplay())));
            header.setSuperTypeNames(typeRegistry.getEntityTypeByName(header.getTypeName()).getAllSuperTypes());

            if (!req.isUpdatedEntity(header.getGuid())) {
                updateModificationMetadata(vertex);
                req.recordEntityUpdate(header);
            }

            AtlasEntity entity = req.getDifferentialEntity(header.getGuid());
            if (entity == null) {
                entity = new AtlasEntity();
                entity.setGuid(header.getGuid());
                entity.setUpdateTime(header.getUpdateTime());
            }

            MetricRecorder recorderInverseMutatedDetails = req.startMetricRecord("addInverseMutatedDetails");
            try {
                AtlasRelationshipType type = typeRegistry.getRelationshipTypeByName(ctx.getAttribute().getRelationshipName());
                AtlasRelationshipEndDef currentEnd = ((AtlasRelationshipDef) type.getStructDef()).getEndDef1();
                AtlasRelationshipEndDef inverseEnd = ((AtlasRelationshipDef) type.getStructDef()).getEndDef2();

                if (ctx.getAttribute().getName().equals(inverseEnd.getName())) {
                    inverseEnd = ((AtlasRelationshipDef) type.getStructDef()).getEndDef1();
                    currentEnd = ((AtlasRelationshipDef) type.getStructDef()).getEndDef2();
                }

                entity.setTypeName(getTypeName(vertex));
                AtlasObjectId objectId = new AtlasObjectId(GraphHelper.getGuid(ctx.getReferringVertex()), currentEnd.getType());

                if (Cardinality.SINGLE == inverseEnd.getCardinality()) {
                    if (isAdd) {
                        entity.setAddedRelationshipAttribute(inverseEnd.getName(), objectId);
                    } else {
                        entity.setRemovedRelationshipAttribute(inverseEnd.getName(), objectId);
                    }
                } else {
                    if (isAdd) {
                        entity.addOrAppendAddedRelationshipAttribute(inverseEnd.getName(), objectId);
                    } else {
                        entity.addOrAppendRemovedRelationshipAttribute(inverseEnd.getName(), objectId);
                    }
                }

                req.cacheDifferentialEntity(entity);
            } finally {
                req.endMetricRecord(recorderInverseMutatedDetails);
            }
        }
    }

    private void recordEntityUpdateForNonRelationsipAttribute(AtlasVertex vertex) throws AtlasBaseException {
        if (vertex != null) {
            RequestContext req = RequestContext.get();

            if (!req.isUpdatedEntity(graphHelper.getGuid(vertex))) {
                updateModificationMetadata(vertex);

                req.recordEntityUpdateForNonRelationshipAttributes(entityRetriever.toAtlasEntityHeader(vertex));
            }
        }
    }


    private String getIdFromInVertex(AtlasEdge edge) {
        return getIdFromVertex(edge.getInVertex());
    }

    private String getIdFromOutVertex(AtlasEdge edge) {
        return getIdFromVertex(edge.getOutVertex());
    }

    private String getIdFromBothVertex(AtlasEdge currentEdge, AtlasVertex parentEntityVertex) {
        String parentEntityId  = getIdFromVertex(parentEntityVertex);
        String currentEntityId = getIdFromVertex(currentEdge.getInVertex());

        if (StringUtils.equals(currentEntityId, parentEntityId)) {
            currentEntityId = getIdFromOutVertex(currentEdge);
        }


        return currentEntityId;
    }

    public void validateAndNormalizeForUpdate(AtlasClassification classification) throws AtlasBaseException {
        AtlasClassificationType type = validateClassificationTypeName(classification);

        List<String> messages = new ArrayList<>();
        type.validateValueForUpdate(classification, classification.getTypeName(), messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, messages);
        }

        type.getNormalizedValueForUpdate(classification);
    }

    public AtlasClassificationType validateClassificationTypeName(AtlasClassification classification) throws AtlasBaseException {
        AtlasClassificationType type = typeRegistry.getClassificationTypeByName(classification.getTypeName());
        if (type == null) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classification.getTypeName());
        }
        return type;
    }

    public static String getSoftRefFormattedValue(AtlasObjectId objectId) {
        return getSoftRefFormattedString(objectId.getTypeName(), objectId.getGuid());
    }

    private static String getSoftRefFormattedString(String typeName, String resolvedGuid) {
        return String.format(SOFT_REF_FORMAT, typeName, resolvedGuid);
    }

    public void importActivateEntity(AtlasVertex vertex, AtlasEntity entity) {
        AtlasGraphUtilsV2.setEncodedProperty(vertex, STATE_PROPERTY_KEY, ACTIVE);

        if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
            Set<String> relatedEntitiesGuids = getRelatedEntitiesGuids(entity);
            activateEntityRelationships(vertex, relatedEntitiesGuids);
        }
    }

    private void activateEntityRelationships(AtlasVertex vertex, Set<String> relatedEntitiesGuids) {
        Iterator<AtlasEdge> edgeIterator = vertex.getEdges(AtlasEdgeDirection.BOTH).iterator();

        while (edgeIterator.hasNext()) {
            AtlasEdge edge = edgeIterator.next();

            if (AtlasGraphUtilsV2.getState(edge) != DELETED) {
                continue;
            }

            final String relatedEntityGuid;
            if (Objects.equals(edge.getInVertex().getId(), vertex.getId())) {
                relatedEntityGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getOutVertex());
            } else {
                relatedEntityGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex());
            }

            if (StringUtils.isEmpty(relatedEntityGuid) || !relatedEntitiesGuids.contains(relatedEntityGuid)) {
                continue;
            }

            edge.setProperty(STATE_PROPERTY_KEY, AtlasRelationship.Status.ACTIVE);
        }
    }

    private Set<String> getRelatedEntitiesGuids(AtlasEntity entity) {
        Set<String> relGuidsSet = new HashSet<>();

        for (Object o : entity.getRelationshipAttributes().values()) {
            if (o instanceof AtlasObjectId) {
                relGuidsSet.add(((AtlasObjectId) o).getGuid());
            } else if (o instanceof List) {
                for (Object id : (List) o) {
                    if (id instanceof AtlasObjectId) {
                        relGuidsSet.add(((AtlasObjectId) id).getGuid());
                    }
                }
            }
        }
        return relGuidsSet;
    }

    private void validateBusinessAttributes(AtlasVertex entityVertex, AtlasEntityType entityType, Map<String, Map<String, Object>> businessAttributes, boolean isOverwrite) throws AtlasBaseException {
        List<String> messages = new ArrayList<>();

        Map<String, Map<String, AtlasBusinessAttribute>> entityTypeBusinessMetadata = entityType.getBusinessAttributes();

        for (String bmName : businessAttributes.keySet()) {
            if (!entityTypeBusinessMetadata.containsKey(bmName)) {
                messages.add(bmName + ": invalid business-metadata for entity type " + entityType.getTypeName());

                continue;
            }

            Map<String, AtlasBusinessAttribute> entityTypeBusinessAttributes = entityTypeBusinessMetadata.get(bmName);
            Map<String, Object>                         entityBusinessAttributes     = businessAttributes.get(bmName);

            for (AtlasBusinessAttribute bmAttribute : entityTypeBusinessAttributes.values()) {
                AtlasType attrType  = bmAttribute.getAttributeType();
                String    attrName  = bmAttribute.getName();
                Object    attrValue = entityBusinessAttributes == null ? null : entityBusinessAttributes.get(attrName);
                String    fieldName = entityType.getTypeName() + "." + bmName + "." + attrName;

                if (attrValue != null) {
                    attrType.validateValue(attrValue, fieldName, messages);
                    boolean isValidLength = bmAttribute.isValidLength(attrValue);
                    if (!isValidLength) {
                        messages.add(fieldName + ":  Business attribute-value exceeds maximum length limit");
                    }

                    validateElasticsearchKeywordSize(bmAttribute, attrValue, fieldName, messages);

                } else if (!bmAttribute.getAttributeDef().getIsOptional()) {
                    final boolean isAttrValuePresent;

                    if (isOverwrite) {
                        isAttrValuePresent = false;
                    } else {
                        Object existingValue = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, bmAttribute.getVertexPropertyName(), Object.class);

                        isAttrValuePresent = existingValue != null;
                    }

                    if (!isAttrValuePresent) {
                        messages.add(fieldName + ": mandatory business-metadata attribute value missing in type " + entityType.getTypeName());
                    }
                }
            }
        }

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, messages);
        }
    }

    public static void validateCustomAttributes(AtlasEntity entity) throws AtlasBaseException {
        Map<String, String> customAttributes = entity.getCustomAttributes();

        if (MapUtils.isNotEmpty(customAttributes)) {
            for (Map.Entry<String, String> entry : customAttributes.entrySet()) {
                String key   = entry.getKey();
                String value = entry.getValue();

                if (key.length() > CUSTOM_ATTRIBUTE_KEY_MAX_LENGTH) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_CUSTOM_ATTRIBUTE_KEY_LENGTH, key);
                }

                Matcher matcher = CUSTOM_ATTRIBUTE_KEY_REGEX.matcher(key);

                if (!matcher.matches()) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_CUSTOM_ATTRIBUTE_KEY_CHARACTERS, key);
                }

                if (StringUtils.isNotEmpty(CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX) && key.startsWith(CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX)) {
                    continue;
                }

                if (!key.startsWith(CUSTOM_ATTRIBUTE_KEY_SPECIAL_PREFIX) && value.length() > CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_CUSTOM_ATTRIBUTE_VALUE, value, String.valueOf(CUSTOM_ATTRIBUTE_VALUE_MAX_LENGTH));
                }
            }
        }
    }

    public static void validateLabels(Set<String> labels) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(labels)) {
            for (String label : labels) {
                if (label.length() > LABEL_MAX_LENGTH.getInt()) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_LABEL_LENGTH, label, String.valueOf(LABEL_MAX_LENGTH.getInt()));
                }

                Matcher matcher = LABEL_REGEX.matcher(label);

                if (!matcher.matches()) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_LABEL_CHARACTERS, label);
                }
            }
        }
    }

    List<AtlasEntity> updateClassificationText(AtlasClassification classification, Collection<AtlasVertex> propagatedVertices) throws AtlasBaseException {
        List<AtlasEntity> propagatedEntities = new ArrayList<>();
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateClassificationText");

        if(CollectionUtils.isNotEmpty(propagatedVertices)) {
            for(AtlasVertex vertex : propagatedVertices) {
                AtlasEntity entity = null;
                for (int i = 1; i <= MAX_NUMBER_OF_RETRIES; i++) {
                    try {
                        entity = instanceConverter.getAndCacheEntity(graphHelper.getGuid(vertex), ENTITY_CHANGE_NOTIFY_IGNORE_RELATIONSHIP_ATTRIBUTES);
                        break; //do not retry on success
                    } catch (AtlasBaseException ex) {
                        if (i == MAX_NUMBER_OF_RETRIES) {
                            LOG.error(String.format("Maximum retries reached for fetching vertex with id %s from graph. Retried %s times. Skipping...", vertex.getId(), i));
                            continue;
                        }
                        LOG.warn(String.format("Vertex with id %s could not be fetched from graph. Retrying for %s time", vertex.getId(), i));
                    }
                }

                if (entity != null) {
                    String classificationTextForEntity = fullTextMapperV2.getClassificationTextForEntity(entity);
                    vertex.setProperty(CLASSIFICATION_TEXT_KEY, classificationTextForEntity);
                    propagatedEntities.add(entity);
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return propagatedEntities;
    }

    List<AtlasEntity> updateClassificationTextV2(AtlasClassification currentTag,
                                                 Collection<AtlasVertex> propagatedVertices,
                                                 Map<String, Map<String, Object>> deNormAttributesMap,
                                                 Map<String, Map<String, Object>> assetMinAttrsMap) throws AtlasBaseException {
        List<AtlasEntity> propagatedEntities = new ArrayList<>();
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateClassificationTextV2");

        if(CollectionUtils.isNotEmpty(propagatedVertices)) {
            for(AtlasVertex vertex : propagatedVertices) {
                Map<String, Object> assetMinAttrs = getMinimalAssetMap(vertex);
                assetMinAttrsMap.put(vertex.getIdForDisplay(), assetMinAttrs);

                //get current associated tags to asset ONLY from Cassandra namespace
                List<Tag> tags = tagDAO.getAllTagsByVertexId(vertex.getIdForDisplay());
                List<AtlasClassification> finalClassifications = tags.stream().map(t -> {
                    return TagDAOCassandraImpl.toAtlasClassification(t.getTagMetaJson());
                }).collect(Collectors.toList());

                tags = tags.stream().filter(Tag::isPropagated).toList();
                List<AtlasClassification> finalPropagatedClassifications = tags.stream().map(t -> {
                    return TagDAOCassandraImpl.toAtlasClassification(t.getTagMetaJson());
                }).collect(Collectors.toList());

                AtlasClassification copiedPropagatedClassification = new AtlasClassification(currentTag);
                copiedPropagatedClassification.setEntityGuid((String) assetMinAttrs.get(GUID_PROPERTY_KEY));
                finalClassifications.add(copiedPropagatedClassification);
                finalPropagatedClassifications.add(copiedPropagatedClassification);

                AtlasEntity entity = new AtlasEntity();
                entity.setClassifications(finalClassifications);

                entity.setGuid((String) assetMinAttrs.get(GUID_PROPERTY_KEY));

                entity.setTypeName((String) assetMinAttrs.get(TYPE_NAME_PROPERTY_KEY));

                entity.setCreatedBy((String) assetMinAttrs.get(CREATED_BY_KEY));
                entity.setUpdatedBy((String) assetMinAttrs.get(MODIFIED_BY_KEY));

                entity.setCreateTime((Date) assetMinAttrs.get(TIMESTAMP_PROPERTY_KEY));
                entity.setUpdateTime((Date) assetMinAttrs.get(MODIFICATION_TIMESTAMP_PROPERTY_KEY));

                entity.setAttribute(NAME, assetMinAttrs.get(NAME));
                entity.setAttribute(QUALIFIED_NAME, assetMinAttrs.get(QUALIFIED_NAME));


                Map<String, Object> deNormAttributes;
                if (CollectionUtils.isEmpty(finalClassifications)) {
                    deNormAttributes = TagDeNormAttributesUtil.getPropagatedAttributesForNoTags();
                } else {
                    deNormAttributes = TagDeNormAttributesUtil.getPropagatedAttributesForTags(currentTag, finalClassifications, finalPropagatedClassifications, typeRegistry, fullTextMapperV2);
                }

                deNormAttributesMap.put(vertex.getIdForDisplay(), deNormAttributes);
                propagatedEntities.add(entity);
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
        return propagatedEntities;
    }

    void updateClassificationTextV2(AtlasClassification currentTag,
                                                 List<String> propagatedVertexIds,
                                                 List<Tag> propagatedTags,
                                                 Map<String, Map<String, Object>> deNormAttributesMap) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateClassificationTextV2");

        if(CollectionUtils.isNotEmpty(propagatedVertexIds)) {
            for(Tag tagAttachment : propagatedTags) {
                //get current associated tags to asset ONLY from Cassandra namespace
                List<Tag> tags = tagDAO.getAllTagsByVertexId(tagAttachment.getVertexId());

                List<AtlasClassification> finalClassifications = tags.stream().map(t -> TagDAOCassandraImpl.toAtlasClassification(t.getTagMetaJson())).collect(Collectors.toList());

                tags = tags.stream().filter(Tag::isPropagated).toList();
                List<AtlasClassification> propagatedClassifications = tags.stream().map(t -> TagDAOCassandraImpl.toAtlasClassification(t.getTagMetaJson())).collect(Collectors.toList());

                Map<String, Object> deNormAttributes;
                if (CollectionUtils.isEmpty(finalClassifications)) {
                    deNormAttributes = TagDeNormAttributesUtil.getPropagatedAttributesForNoTags();
                } else {
                    deNormAttributes = TagDeNormAttributesUtil.getPropagatedAttributesForTags(currentTag, finalClassifications, propagatedClassifications, typeRegistry, fullTextMapperV2);
                }

                deNormAttributesMap.put(tagAttachment.getVertexId(), deNormAttributes);
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void updateLabels(AtlasVertex vertex, Set<String> labels) {
        if (CollectionUtils.isNotEmpty(labels)) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, LABELS_PROPERTY_KEY, getLabelString(labels));
        } else {
            vertex.removeProperty(LABELS_PROPERTY_KEY);
        }
    }

    private String getLabelString(Collection<String> labels) {
        String ret = null;

        if (!labels.isEmpty()) {
            ret = LABEL_NAME_DELIMITER + String.join(LABEL_NAME_DELIMITER, labels) + LABEL_NAME_DELIMITER;
        }

        return ret;
    }

    private void addToUpdatedBusinessAttributes(Map<String, Map<String, Object>> updatedBusinessAttributes, AtlasBusinessAttribute bmAttribute, Object attrValue) {
        String              bmName     = bmAttribute.getDefinedInType().getTypeName();
        Map<String, Object> attributes = updatedBusinessAttributes.get(bmName);

        if(attributes == null){
            attributes = new HashMap<>();

            updatedBusinessAttributes.put(bmName, attributes);
        }

        attributes.put(bmAttribute.getName(), attrValue);
    }

    private void createAndQueueTask(String taskType, AtlasVertex entityVertex, String classificationVertexId, String classificationName, Boolean currentPropagateThroughLineage, Boolean currentRestrictPropagationThroughHierarchy) throws AtlasBaseException{

        deleteDelegate.getHandler().createAndQueueTaskWithoutCheck(taskType, entityVertex, classificationVertexId,classificationName, null, currentPropagateThroughLineage,currentRestrictPropagationThroughHierarchy);
    }

    private void createAndQueueTask(String taskType, AtlasVertex entityVertex, String classificationVertexId, String classificationName) throws AtlasBaseException {
        deleteDelegate.getHandler().createAndQueueTaskWithoutCheck(taskType, entityVertex, classificationVertexId, classificationName, null);
    }

    public void removePendingTaskFromEntity(String entityGuid, String taskGuid) throws EntityNotFoundException {
        if (StringUtils.isEmpty(entityGuid) || StringUtils.isEmpty(taskGuid)) {
            return;
        }

        AtlasVertex entityVertex = graphHelper.getVertexForGUID(entityGuid);

        if (entityVertex == null) {
            LOG.warn("Error fetching vertex: {}", entityVertex);

            return;
        }

        entityVertex.removePropertyValue(PENDING_TASKS_PROPERTY_KEY, taskGuid);
    }

    public void removePendingTaskFromEdge(String edgeId, String taskGuid) throws AtlasBaseException {
        if (StringUtils.isEmpty(edgeId) || StringUtils.isEmpty(taskGuid)) {
            return;
        }

        AtlasEdge edge = graph.getEdge(edgeId);

        if (edge == null) {
            LOG.warn("Error fetching edge: {}", edgeId);

            return;
        }

        AtlasGraphUtilsV2.removeItemFromListProperty(edge, EDGE_PENDING_TASKS_PROPERTY_KEY, taskGuid);
    }


    public void addHasLineage(Set<AtlasEdge> inputOutputEdges, boolean isRestoreEntity) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addHasLineage");
        
        // Timing: Lineage calculation
        long lineageCalcStart = System.currentTimeMillis();

        for (AtlasEdge atlasEdge : inputOutputEdges) {

            boolean isOutputEdge = PROCESS_OUTPUTS.equals(atlasEdge.getLabel());

            AtlasVertex processVertex = atlasEdge.getOutVertex();
            AtlasVertex assetVertex = atlasEdge.getInVertex();

            if (getEntityHasLineage(processVertex)) {
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, true);
                AtlasEntity diffEntity = entityRetriever.getOrInitializeDiffEntity(assetVertex);
                diffEntity.setAttribute(HAS_LINEAGE, true);
                continue;
            }

            String oppositeEdgeLabel = isOutputEdge ? PROCESS_INPUTS : PROCESS_OUTPUTS;

            Iterator<AtlasEdge> oppositeEdges = processVertex.getEdges(AtlasEdgeDirection.BOTH, oppositeEdgeLabel).iterator();
            boolean isHasLineageSet = false;
            while (oppositeEdges.hasNext()) {
                AtlasEdge oppositeEdge = oppositeEdges.next();
                AtlasVertex oppositeEdgeAssetVertex = oppositeEdge.getInVertex();

                if (getStatus(oppositeEdge) == ACTIVE && getStatus(oppositeEdgeAssetVertex) == ACTIVE) {
                    if (!isHasLineageSet) {
                        AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, true);
                        AtlasGraphUtilsV2.setEncodedProperty(processVertex, HAS_LINEAGE, true);

                        AtlasEntity diffEntity = entityRetriever.getOrInitializeDiffEntity(assetVertex);
                        diffEntity.setAttribute(HAS_LINEAGE, true);

                        diffEntity = entityRetriever.getOrInitializeDiffEntity(processVertex);
                        diffEntity.setAttribute(HAS_LINEAGE, true);
                        isHasLineageSet = true;
                    }

                    if (isRestoreEntity) {
                        AtlasGraphUtilsV2.setEncodedProperty(oppositeEdgeAssetVertex, HAS_LINEAGE, true);
                        AtlasEntity diffEntity = entityRetriever.getOrInitializeDiffEntity(oppositeEdgeAssetVertex);
                        diffEntity.setAttribute(HAS_LINEAGE, true);
                    } else {
                        break;
                    }
                }
            }
        }
        
        // Record lineage calculation time
        long lineageCalcTime = System.currentTimeMillis() - lineageCalcStart;
        RequestContext.get().addLineageCalcTime(lineageCalcTime);
        
        RequestContext.get().endMetricRecord(metricRecorder);
    }


    public AtlasVertex linkBusinessPolicy(final BusinessPolicyRequest.AssetComplianceInfo data) {
        String assetGuid = data.getAssetId();
        AtlasVertex vertex = findByGuid(graph, assetGuid);

        // Retrieve existing policies
        Set<String> existingCompliant = getVertexPolicies(vertex, ASSET_POLICY_GUIDS);
        Set<String> existingNonCompliant = getVertexPolicies(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS);

        // Retrieve new policies
        Set<String> addCompliantGUIDs = getOrCreateEmptySet(data.getAddCompliantGUIDs());
        Set<String> addNonCompliantGUIDs = getOrCreateEmptySet(data.getAddNonCompliantGUIDs());
        Set<String> removeCompliantGUIDs = getOrCreateEmptySet(data.getRemoveCompliantGUIDs());
        Set<String> removeNonCompliantGUIDs = getOrCreateEmptySet(data.getRemoveNonCompliantGUIDs());


        // Update vertex properties
        addToAttribute(vertex, ASSET_POLICY_GUIDS, addCompliantGUIDs);
        removeFromAttribute(vertex, ASSET_POLICY_GUIDS, removeCompliantGUIDs);


        addToAttribute(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS, addNonCompliantGUIDs);
        removeFromAttribute(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS, removeNonCompliantGUIDs);

        // Count and set policies
        Set<String> effectiveCompliantGUIDs = getVertexPolicies(vertex, ASSET_POLICY_GUIDS);
        Set<String> effectiveNonCompliantGUIDs = getVertexPolicies(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS);

        int compliantPolicyCount = countPoliciesExcluding(effectiveCompliantGUIDs, "rule");
        int nonCompliantPolicyCount = countPoliciesExcluding(effectiveNonCompliantGUIDs, "rule");

        int totalPolicyCount = compliantPolicyCount + nonCompliantPolicyCount;

        vertex.setProperty(ASSET_POLICIES_COUNT, totalPolicyCount);
        updateModificationMetadata(vertex);

        // Create and cache differential entity
        AtlasEntity diffEntity = createDifferentialEntity(
                vertex, effectiveCompliantGUIDs, effectiveNonCompliantGUIDs, existingCompliant, existingNonCompliant, totalPolicyCount);

        RequestContext.get().cacheDifferentialEntity(diffEntity);
        return vertex;
    }

    private static Set<String> getOrCreateEmptySet(Set<String> input) {
        return input == null ? new HashSet<>() : input;
    }

    private void addToAttribute(AtlasVertex vertex, String propertyKey, Set<String> policies) {
        String targetProperty = determineTargetProperty(propertyKey);
        policies.stream()
                .filter(StringUtils::isNotEmpty)
                .forEach(policyGuid -> vertex.setProperty(targetProperty, policyGuid));
    }

    private void removeFromAttribute(AtlasVertex vertex, String propertyKey, Set<String> policies) {
        String targetProperty = determineTargetProperty(propertyKey);
        policies.stream()
                .filter(StringUtils::isNotEmpty)
                .forEach(policyGuid -> vertex.removePropertyValue(targetProperty, policyGuid));
    }

    private String determineTargetProperty(String propertyKey) {
        return ASSET_POLICY_GUIDS.equals(propertyKey)
                ? ASSET_POLICY_GUIDS
                : NON_COMPLIANT_ASSET_POLICY_GUIDS;
    }

    private int countPoliciesExcluding(Set<String> policies, String substring) {
        return (int) policies.stream().filter(policy -> !policy.contains(substring)).count();
    }

    private AtlasEntity createDifferentialEntity(AtlasVertex vertex, Set<String> effectiveCompliant, Set<String> effectiveNonCompliant,
                                                 Set<String> existingCompliant, Set<String> existingNonCompliant, int totalPolicyCount) {
        AtlasEntity diffEntity = new AtlasEntity(vertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        setEntityCommonAttributes(vertex, diffEntity);
        diffEntity.setAttribute(ASSET_POLICIES_COUNT, totalPolicyCount);

        if (!existingCompliant.equals(effectiveCompliant)) {
            diffEntity.setAttribute(ASSET_POLICY_GUIDS, effectiveCompliant);
        }
        if (!existingNonCompliant.equals(effectiveNonCompliant)) {
            diffEntity.setAttribute(NON_COMPLIANT_ASSET_POLICY_GUIDS, effectiveNonCompliant);
        }

        return diffEntity;
    }

    private Set<String> getVertexPolicies(AtlasVertex vertex, String propertyKey) {
        return Optional.ofNullable(vertex.getMultiValuedSetProperty(propertyKey, String.class))
                .orElse(Collections.emptySet());
    }


    public List<AtlasVertex> unlinkBusinessPolicy(String policyId, Set<String> unlinkGuids) {
        if (policyId == null || unlinkGuids == null || unlinkGuids.isEmpty()) {
            throw new IllegalArgumentException("PolicyId and unlinkGuids must not be null or empty");
        }

        return unlinkGuids.stream()
                .map(guid -> AtlasGraphUtilsV2.findByGuid(graph, guid))
                .filter(Objects::nonNull)
                .filter(vertex -> isPolicyLinked(vertex, policyId))
                .map(vertex -> updateVertexPolicy(vertex, policyId))
                .collect(Collectors.toList());
    }

    private boolean isPolicyLinked(AtlasVertex vertex, String policyId) {
        Set<String> compliantPolicies = getMultiValuedSetProperty(vertex, ASSET_POLICY_GUIDS);
        Set<String> nonCompliantPolicies = getMultiValuedSetProperty(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS);
        return compliantPolicies.contains(policyId) || nonCompliantPolicies.contains(policyId);
    }

    private AtlasVertex updateVertexPolicy(AtlasVertex vertex, String policyId) {
        Set<String> compliantPolicies = getMultiValuedSetProperty(vertex, ASSET_POLICY_GUIDS);
        Set<String> nonCompliantPolicies = getMultiValuedSetProperty(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS);

        boolean removed = removePolicyAndRule(compliantPolicies, policyId);
        removed |= removePolicyAndRule(nonCompliantPolicies,policyId);

        if (removed) {
            vertex.removePropertyValue(ASSET_POLICY_GUIDS, policyId);
            vertex.removePropertyValue(NON_COMPLIANT_ASSET_POLICY_GUIDS, policyId);

            int compliantPolicyCount = countPoliciesExcluding(compliantPolicies, "rule");
            int nonCompliantPolicyCount = countPoliciesExcluding(nonCompliantPolicies, "rule");
            int totalPolicyCount = compliantPolicyCount + nonCompliantPolicyCount;
            vertex.setProperty(ASSET_POLICIES_COUNT, totalPolicyCount);

            updateModificationMetadata(vertex);
            cacheDifferentialEntity(vertex, compliantPolicies, nonCompliantPolicies);
        }

        return vertex;
    }

    private boolean removePolicyAndRule(Set<String> policies, String policyId) {
        Set<String> toRemove = policies.stream().filter(i-> i.contains(policyId)).collect(Collectors.toSet());
        return policies.removeAll(toRemove);

    }

    private Set<String> getMultiValuedSetProperty(AtlasVertex vertex, String propertyName) {
        return Optional.ofNullable(vertex.getMultiValuedSetProperty(propertyName, String.class))
                .map(HashSet::new)
                .orElseGet(HashSet::new);
    }

    public List<AtlasVertex> linkMeshEntityToAssets(String meshEntityId, Set<String> linkGuids) throws AtlasBaseException {
        List<AtlasVertex> linkedVertices = new ArrayList<>();

        for (String guid : linkGuids) {
            AtlasVertex ev = findByGuid(graph, guid);

            if (ev != null) {
                String typeName = ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);
                if (excludedTypes.contains(typeName)){
                    LOG.warn("Type {} is not allowed to link with mesh entity", typeName);
                    continue;
                }
                Set<String> existingValues = ev.getMultiValuedSetProperty(DOMAIN_GUIDS_ATTR, String.class);

                if (!existingValues.contains(meshEntityId)) {
                    isAuthorizedToLink(ev);

                    updateDomainAttribute(ev, existingValues, meshEntityId);
                    existingValues.clear();
                    existingValues.add(meshEntityId);

                    updateModificationMetadata(ev);

                    cacheDifferentialMeshEntity(ev, existingValues);

                    linkedVertices.add(ev);
                }
            }
        }

        return linkedVertices;
    }

    public List<AtlasVertex> unlinkMeshEntityFromAssets(String meshEntityId, Set<String> unlinkGuids) throws AtlasBaseException {
        List<AtlasVertex> unlinkedVertices = new ArrayList<>();

        for (String guid : unlinkGuids) {
            AtlasVertex ev = AtlasGraphUtilsV2.findByGuid(graph, guid);

            if (ev != null) {
                String typeName = ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);
                if (excludedTypes.contains(typeName)){
                    LOG.warn("Type {} is not allowed to unlink with mesh entity", typeName);
                    continue;
                }

                Set<String> existingValues = ev.getMultiValuedSetProperty(DOMAIN_GUIDS_ATTR, String.class);

                if (meshEntityId.isEmpty() || existingValues.contains(meshEntityId)) {
                    isAuthorizedToLink(ev);

                    if (StringUtils.isEmpty(meshEntityId)) {
                        existingValues.clear();
                        ev.removeProperty(DOMAIN_GUIDS_ATTR);
                    } else {
                        existingValues.remove(meshEntityId);
                        ev.removePropertyValue(DOMAIN_GUIDS_ATTR, meshEntityId);
                    }

                    updateModificationMetadata(ev);
                    cacheDifferentialMeshEntity(ev, existingValues);

                    unlinkedVertices.add(ev);
                }
            }
        }

        return unlinkedVertices;
    }

    private void updateDomainAttribute(AtlasVertex vertex, Set<String> existingValues, String meshEntityId){
        existingValues.forEach(existingValue -> vertex.removePropertyValue(DOMAIN_GUIDS_ATTR, existingValue));
        vertex.setProperty(DOMAIN_GUIDS_ATTR, meshEntityId);
    }

    private void cacheDifferentialEntity(AtlasVertex ev, Set<String> complaint, Set<String> nonComplaint) {
        AtlasEntity diffEntity = new AtlasEntity(ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        setEntityCommonAttributes(ev, diffEntity);
        diffEntity.setAttribute(ASSET_POLICY_GUIDS, complaint);
        diffEntity.setAttribute(NON_COMPLIANT_ASSET_POLICY_GUIDS, nonComplaint);
        diffEntity.setAttribute(ASSET_POLICIES_COUNT, complaint.size() + nonComplaint.size());

        RequestContext requestContext = RequestContext.get();
        requestContext.cacheDifferentialEntity(diffEntity);
    }

    private void cacheDifferentialMeshEntity(AtlasVertex ev, Set<String> existingValues) {
        AtlasEntity diffEntity = new AtlasEntity(ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        setEntityCommonAttributes(ev, diffEntity);
        diffEntity.setAttribute(DOMAIN_GUIDS_ATTR, existingValues);

        RequestContext requestContext = RequestContext.get();
        requestContext.cacheDifferentialEntity(diffEntity);
    }

    private void setEntityCommonAttributes(AtlasVertex ev, AtlasEntity diffEntity) {
        diffEntity.setGuid(ev.getProperty(GUID_PROPERTY_KEY, String.class));
        diffEntity.setUpdatedBy(ev.getProperty(MODIFIED_BY_KEY, String.class));
        diffEntity.setUpdateTime(new Date(RequestContext.get().getRequestTime()));
    }

    private void isAuthorizedToLink(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntityHeader sourceEntity = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(vertex);

        // source -> UPDATE + READ
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceEntity),
                "update on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, sourceEntity),
                "read on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

    }

    private Map<String, Object> getMinimalAssetMap(AtlasVertex vertex) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(NAME, vertex.getProperty(NAME, String.class));
        ret.put(QUALIFIED_NAME, vertex.getProperty(QUALIFIED_NAME, String.class));

        ret.put(GUID_PROPERTY_KEY, vertex.getProperty(GUID_PROPERTY_KEY, String.class));
        ret.put(TYPE_NAME_PROPERTY_KEY, vertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));

        ret.put(CREATED_BY_KEY, vertex.getProperty(CREATED_BY_KEY, String.class));
        ret.put(MODIFIED_BY_KEY, vertex.getProperty(MODIFIED_BY_KEY, String.class));

        ret.put(TIMESTAMP_PROPERTY_KEY, new Date(vertex.getProperty(TIMESTAMP_PROPERTY_KEY, Long.class)));
        ret.put(MODIFICATION_TIMESTAMP_PROPERTY_KEY, new Date(vertex.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class)));

        return ret;
    }

    public AtlasEntity getMinimalAtlasEntityForNotification(AtlasVertex vertex) {
        AtlasEntity minEntity = new AtlasEntity();

        minEntity.setAttribute(NAME, vertex.getProperty(NAME, String.class));
        minEntity.setAttribute(QUALIFIED_NAME, vertex.getProperty(QUALIFIED_NAME, String.class));

        minEntity.setGuid(vertex.getProperty(GUID_PROPERTY_KEY, String.class));
        minEntity.setTypeName(vertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));

        minEntity.setCreatedBy(vertex.getProperty(CREATED_BY_KEY, String.class));
        minEntity.setUpdatedBy(vertex.getProperty(MODIFIED_BY_KEY, String.class));

        minEntity.setCreateTime(new Date(vertex.getProperty(TIMESTAMP_PROPERTY_KEY, Long.class)));
        minEntity.setUpdateTime(new Date(vertex.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class)));

        return minEntity;
    }

    public int updateClassificationTextPropagationV2(String sourceEntityGuid, String tagTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateClassificationTextPropagationNew");
        try {
            if (StringUtils.isEmpty(tagTypeName)) {
                LOG.warn("updateClassificationTextPropagation(classificationVertexId={}): classification type name is empty", tagTypeName);
                return 0;
            }

            AtlasVertex sourceEntityVertex = graphHelper.getVertexForGUID(sourceEntityGuid);
            if (sourceEntityVertex == null) {
                String warningMessage = String.format("updateClassificationTextPropagationV2(entityGuid=%s, tagTypeName=%s): entity vertex not found, skipping task execution", sourceEntityGuid, tagTypeName);
                LOG.warn(warningMessage);
                return 0;
            }

            int totalUpdated = 0;

            // fetch propagated‑tag attachments in batches
            PaginatedTagResult paginatedResult = tagDAO.getPropagationsForAttachmentBatch(sourceEntityVertex.getIdForDisplay(), tagTypeName, null);
            AtlasClassification originalClassification = tagDAO.findDirectTagByVertexIdAndTagTypeName(sourceEntityVertex.getIdForDisplay(), tagTypeName, false);
            if (originalClassification == null) {
                String warningMessage = String.format("updateClassificationTextPropagationV2(entityGuid=%s, tagTypeName=%s): classification not found, skipping task execution", sourceEntityGuid, tagTypeName);
                LOG.warn(warningMessage);
                return 0;
            }

            List<Tag> batchToUpdate = paginatedResult.getTags();

            while (!batchToUpdate.isEmpty()) {

                // collect the vertex IDs in this batch
                List<String> vertexIds = batchToUpdate.stream()
                        .map(Tag::getVertexId)
                        .toList();

                Map<String, Map<String, Object>> assetMinAttrsMap = batchToUpdate.stream()
                        .collect(Collectors.toMap(Tag::getVertexId, Tag::getAssetMetadata));

                List<AtlasEntity> entities = batchToUpdate.stream().map(x -> getEntityForNotification(x.getAssetMetadata())).toList();

                // Update all propagated tags in Cassandra
                tagDAO.putPropagatedTags(sourceEntityVertex.getIdForDisplay(), tagTypeName, new HashSet<>(vertexIds), assetMinAttrsMap, originalClassification);

                // compute fresh classification‑text de‑norm attributes for this batch
                Map<String, Map<String, Object>> deNormMap = new HashMap<>();
                updateClassificationTextV2(originalClassification, vertexIds, batchToUpdate, deNormMap);

                // push them to ES
                if (MapUtils.isNotEmpty(deNormMap)) {
                    ESConnector.writeTagProperties(deNormMap);
                }


                //new bulk method to fetch in batches
                Set<AtlasVertex> propagtedVertices = graph.getVertices(vertexIds.toArray(new String[0]));

                // notify listeners (async) that these entities got their classification text updated
                entityChangeNotifier.onClassificationUpdatedToEntitiesV2(propagtedVertices, originalClassification, true, RequestContext.get());

                totalUpdated += batchToUpdate.size();
                // grab next batch
                if (paginatedResult.isDone()) {
                    break;
                }
                String pagingState = paginatedResult.getPagingState();
                paginatedResult = tagDAO.getPropagationsForAttachmentBatch(sourceEntityVertex.getIdForDisplay(), tagTypeName, pagingState);
                batchToUpdate = paginatedResult.getTags();
            }

            LOG.info("Updated classification text for {} propagations, taskId: {}", totalUpdated, RequestContext.get().getCurrentTask().getGuid());
            return totalUpdated;
        } catch (Exception e) {
            LOG.error("Error while updating classification text for tag type {}: {}", tagTypeName, e.getMessage());
            throw new AtlasBaseException(e);
        } finally {
            // end metrics
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static AtlasEntity getEntityForNotification(Map<String, Object> assetMetadata) {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(NAME, assetMetadata.get(NAME));
        entity.setAttribute(QUALIFIED_NAME, assetMetadata.get(QUALIFIED_NAME));

        entity.setGuid((String) assetMetadata.get(GUID_PROPERTY_KEY));
        entity.setTypeName((String) assetMetadata.get(TYPE_NAME_PROPERTY_KEY));
        entity.setCreatedBy((String) assetMetadata.get(CREATED_BY_KEY));
        entity.setUpdatedBy((String) assetMetadata.get(MODIFIED_BY_KEY));

        entity.setCreateTime(safeParseDate(assetMetadata.get(TIMESTAMP_PROPERTY_KEY), TIMESTAMP_PROPERTY_KEY));
        entity.setUpdateTime(safeParseDate(assetMetadata.get(MODIFICATION_TIMESTAMP_PROPERTY_KEY), MODIFICATION_TIMESTAMP_PROPERTY_KEY));

        return entity;
    }

    private static Date safeParseDate(Object value, String fieldName) {
        long minValidTimestamp = 0L; // Jan 1, 1970 UTC
        long maxValidTimestamp = 4102444800000L; // Jan 1, 2100

        Long timestamp = null;
        if (value instanceof Long) {
            timestamp = (Long) value;
        } else if (value instanceof Integer) {
            timestamp = ((Integer) value).longValue();
        } else if (value instanceof String) {
            try {
                timestamp = Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid string timestamp for {}: '{}'", fieldName, value);
                return null;
            }
        } else if (value != null) {
            LOG.warn("Unexpected type for {}: {}", fieldName, value.getClass().getName());
            return null;
        }

        if (timestamp != null) {
            if (timestamp < minValidTimestamp || timestamp > maxValidTimestamp) {
                LOG.warn("Timestamp out of expected range for {}: {}", fieldName, timestamp);
                return null;
            }
            return new Date(timestamp);
        }
        return null;
    }

    /**
     * Performs a scalable, batch-oriented refresh of propagated classifications.
     *
     * @param parameters Task parameters.
     * @param parentEntityGuid The GUID of a potential parent entity.
     * @param sourceEntityGuid The GUID of the entity that is the source of the propagation.
     * @param classificationTypeName The type name of the classification to refresh.
     * @throws AtlasBaseException if a critical error occurs.
     */
    public int classificationRefreshPropagationV2_new(Map<String, Object> parameters, String parentEntityGuid, String sourceEntityGuid, String classificationTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("classificationRefreshPropagationV2_new");
        try {
            final int BATCH_SIZE = 10000;
            int assetsAffected = 0;
            LOG.info("classificationRefreshPropagationV2_new: Starting scalable refresh for tag '{}' from source entity {}", classificationTypeName, sourceEntityGuid);

            // === Phase 1: Initialization & Calculation ===
            AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityGuid);
            if (entityVertex == null) {
                String warningMessage = String.format("classificationRefreshPropagationV2_new(sourceEntityGuid=%s): entity vertex not found, skipping task execution", sourceEntityGuid);
                LOG.warn(warningMessage);
                return assetsAffected;
            }

            String entityVertexId = entityVertex.getIdForDisplay();
            AtlasClassification sourceTag = tagDAO.findDirectTagByVertexIdAndTagTypeName(entityVertexId, classificationTypeName, true);

            if (sourceTag == null) {
                if (StringUtils.isNotEmpty(parentEntityGuid) && !parentEntityGuid.equals(sourceEntityGuid)) {
                    entityVertex = AtlasGraphUtilsV2.findByGuid(this.graph, parentEntityGuid);
                    if (entityVertex == null) {
                        String warningMessage = String.format("classificationRefreshPropagationV2_new(parentEntityGuid=%s): parent entity vertex not found", parentEntityGuid);
                        LOG.warn(warningMessage);
                        return assetsAffected;
                    }
                    entityVertexId = entityVertex.getIdForDisplay();
                    sourceTag = tagDAO.findDirectTagByVertexIdAndTagTypeName(entityVertexId, classificationTypeName, true);
                }
            }

            if (sourceTag == null) {
                throw new AtlasBaseException(String.format("classificationRefreshPropagationV2_new: Classification '%s' not found on entity '%s' or its parent '%s'. Aborting.", classificationTypeName, sourceEntityGuid, parentEntityGuid));
            }

            if (!sourceTag.isPropagate()) {
                LOG.warn("classificationRefreshPropagationV2_new: Refresh task invalid as propagation is disabled for tag '{}' on entity {}. Aborting.", classificationTypeName, sourceEntityGuid);
                return assetsAffected;
            }

            Set<String> expectedPropagatedVertexIds = calculateExpectedPropagations(entityVertex, sourceTag);
            LOG.info("classificationRefreshPropagationV2_new: Graph traversal found {} assets that should have the tag '{}'", expectedPropagatedVertexIds.size(), classificationTypeName);

            // === Phase 2: Reconciliation Loop (Deletions) ===
            String pagingState = null;
            boolean hasMorePages = true;
            List<Tag> tagsToDelete = new ArrayList<>();

            int pageCount = 0;
            int totalCountOfPropagatedTags = 0;

            while (hasMorePages) {
                pageCount++;
                PaginatedTagResult result = tagDAO.getPropagationsForAttachmentBatchWithPagination(entityVertexId, classificationTypeName, pagingState, BATCH_SIZE);
                List<Tag> currentPageTags = result.getTags();

                int fetchedCount = currentPageTags.size();
                totalCountOfPropagatedTags += fetchedCount;

                for (Tag existingTag : result.getTags()) {
                    if (expectedPropagatedVertexIds.contains(existingTag.getVertexId())) {
                        expectedPropagatedVertexIds.remove(existingTag.getVertexId());
                    } else {
                        tagsToDelete.add(existingTag);
                    }
                }

                if (tagsToDelete.size() >= BATCH_SIZE) {
                    assetsAffected += tagsToDelete.size();
                    processDeletions_new(tagsToDelete, sourceTag);
                    tagsToDelete.clear();
                }

                pagingState = result.getPagingState();
                hasMorePages = result.hasMorePages();

                LOG.info("sourceVertexId={}, tagTypeName={}: Page {}: Fetched {} propagations. Total fetched: {}. Has next page: {}",
                        entityVertexId, classificationTypeName, pageCount, fetchedCount, totalCountOfPropagatedTags, hasMorePages);
            }

            if (!tagsToDelete.isEmpty()) {
                assetsAffected += tagsToDelete.size();
                processDeletions_new(tagsToDelete, sourceTag);
            }

            // === Phase 3: Process Net-New Additions ===
            if (!expectedPropagatedVertexIds.isEmpty()) {
                LOG.info("classificationRefreshPropagationV2_new: Found {} assets that need the tag '{}' to be newly propagated.", expectedPropagatedVertexIds.size(), classificationTypeName);
                List<String> vertexIdsToAdd = new ArrayList<>(expectedPropagatedVertexIds);
                for (int i = 0; i < vertexIdsToAdd.size(); i += BATCH_SIZE) {
                    int end = Math.min(i + BATCH_SIZE, vertexIdsToAdd.size());
                    List<String> batchIds = vertexIdsToAdd.subList(i, end);

                    List<AtlasVertex> verticesToPropagate = batchIds.stream()
                            .map(id -> graph.getVertex(id))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    assetsAffected += verticesToPropagate.size();

                    if (!verticesToPropagate.isEmpty()) {
                        processClassificationPropagationAdditionV2(parameters, entityVertexId, verticesToPropagate, sourceTag);
                    }
                }
            }
            LOG.info("classificationRefreshPropagationV2_new: Successfully completed scalable refresh for tag '{}' from source entity {}", classificationTypeName, sourceEntityGuid);
            return assetsAffected;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Set<String> calculateExpectedPropagations(AtlasVertex sourceVertex, AtlasClassification sourceTag) throws AtlasBaseException {
        String propagationMode = entityRetriever.determinePropagationMode(sourceTag.getRestrictPropagationThroughLineage(), sourceTag.getRestrictPropagationThroughHierarchy());
        boolean toExclude = Objects.equals(propagationMode, CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE);
        Set<String> impactedVertices = new HashSet<>();
        entityRetriever.traverseImpactedVerticesByLevelV2(sourceVertex, null, null, impactedVertices, CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode), toExclude, null, null);
        transactionInterceptHelper.intercept();
        return impactedVertices;
    }

    private void processDeletions_new(List<Tag> tagsToDelete, AtlasClassification sourceTag) throws AtlasBaseException {
        LOG.debug("Processing deletion of {} tags.", tagsToDelete.size());
        tagDAO.deleteTags(tagsToDelete);

        List<String> vertexIdsToDelete = tagsToDelete.stream()
                .map(Tag::getVertexId)
                .toList();

        Map<String, Map<String, Object>> deNormMap = new HashMap<>();
        updateClassificationTextV2(sourceTag, vertexIdsToDelete, tagsToDelete, deNormMap);
        if (MapUtils.isNotEmpty(deNormMap)) {
            ESConnector.writeTagProperties(deNormMap);
        }

        Set<AtlasVertex> vertices = graph.getVertices(vertexIdsToDelete.toArray(new String[0]));
        if (!vertices.isEmpty()) {
            entityChangeNotifier.onClassificationPropagationDeletedV2(vertices, sourceTag, true, RequestContext.get());
        }
    }

    private Map<String, Map<String, Object>> getMinimalAssetMapsFromGraph(List<String> vertexIds) {
        Map<String, Map<String, Object>> ret = new HashMap<>();
        for (String vertexId : vertexIds) {
            AtlasVertex vertex = graph.getVertex(vertexId);
            if (vertex != null) {
                ret.put(vertexId, getMinimalAssetMap(vertex));
            }
        }
        return ret;
    }

    public List<AtlasVertex> unlinkBusinessPolicyV2(Set<String> assetGuids, Set<String> unlinkGuids) {

        if (CollectionUtils.isEmpty(assetGuids) || CollectionUtils.isEmpty(unlinkGuids)) {
            throw new IllegalArgumentException("assets and unlinkGuids must not be empty");
        }

        return assetGuids.stream()
                .map(guid -> AtlasGraphUtilsV2.findByGuid(graph, guid))
                .filter(Objects::nonNull)
                .map(vertex -> updateVertexPolicyV2(vertex, unlinkGuids))
                .collect(Collectors.toList());
    }


    private AtlasVertex updateVertexPolicyV2(AtlasVertex vertex, Set<String> policyIds) {
        Set<String> compliantPolicies = getMultiValuedSetProperty(vertex, ASSET_POLICY_GUIDS);
        Set<String> nonCompliantPolicies = getMultiValuedSetProperty(vertex, NON_COMPLIANT_ASSET_POLICY_GUIDS);
        policyIds.forEach(policyId -> {
            vertex.removePropertyValue(ASSET_POLICY_GUIDS, policyId);
            vertex.removePropertyValue(NON_COMPLIANT_ASSET_POLICY_GUIDS, policyId);
        });

        int compliantPolicyCount = countPoliciesExcluding(compliantPolicies, "rule");
        int nonCompliantPolicyCount = countPoliciesExcluding(nonCompliantPolicies, "rule");
        int totalPolicyCount = compliantPolicyCount + nonCompliantPolicyCount;
        vertex.setProperty(ASSET_POLICIES_COUNT, totalPolicyCount);

        updateModificationMetadata(vertex);
        cacheDifferentialEntity(vertex, compliantPolicies, nonCompliantPolicies);

        return vertex;
    }

    public AtlasVertex attributeUpdate(AttributeUpdateRequest.AssetAttributeInfo data) {
        // Validate input
        if (data == null || StringUtils.isEmpty(data.getAssetId())) {
            LOG.warn("Invalid data provided for attribute update. Data: {}", data);
            return null;
        }
        String attributeName = data.getAttributeName();
        if (StringUtils.isEmpty(attributeName)) {
            LOG.warn("Attribute name is null or empty for asset ID: {}", data.getAssetId());
            return null;
        }
        switch (attributeName) {
            case "assetInternalPopularityScore":
                // validate value to be a number
                String value = data.getValue();
                //isNumber should work for now we have to upgrade apache-common to 3.6+ to use isCreatable more reliable
                if (StringUtils.isEmpty(value) || !NumberUtils.isNumber(value)) {
                    LOG.warn("Invalid value for internalPopularityScore: {} for assetId {}", value, data.getAssetId());
                    return null;
                }
                return updateAsset(data);
            default:
                LOG.warn("Unsupported attribute name: {} for asset ID: {}", attributeName, data.getAssetId());
                return null;
        }
    }

    private AtlasVertex updateAsset(AttributeUpdateRequest.AssetAttributeInfo assetAttributeInfo) {
        String assetGuid = assetAttributeInfo.getAssetId();
        AtlasVertex vertex = findByGuid(graph, assetGuid);
        if (vertex == null || GraphHelper.getStatus(vertex) == DELETED) {
            LOG.warn("Asset with GUID {} not found", assetGuid);
            return null;
        }
        vertex.setProperty(assetAttributeInfo.getAttributeName(), assetAttributeInfo.getValue());
        updateModificationMetadata(vertex);
        cacheDifferentialEntityAttributeUpdate(vertex, assetAttributeInfo.getAttributeName(), assetAttributeInfo.getValue());
        return vertex;
    }

    private void cacheDifferentialEntityAttributeUpdate(AtlasVertex ev, String property, String value) {
        AtlasEntity diffEntity = new AtlasEntity(ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        setEntityCommonAttributes(ev, diffEntity);
        diffEntity.setAttribute(property, value);

        RequestContext requestContext = RequestContext.get();
        requestContext.cacheDifferentialEntity(diffEntity);
    }

}