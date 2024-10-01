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
import org.apache.atlas.*;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.TimeBoundary;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasRelationshipAttributeDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
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
import org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.repository.store.graph.v1.RestoreHandlerV1;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.LABEL_MAX_LENGTH;
import static org.apache.atlas.AtlasConfiguration.STORE_DIFFERENTIAL_AUDITS;
import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.model.TypeCategory.CLASSIFICATION;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.instance.AtlasRelatedObjectId.KEY_RELATIONSHIP_ATTRIBUTES;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.PARTIAL_UPDATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.SET;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEdge;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationVertex;
import static org.apache.atlas.repository.graph.GraphHelper.getCollectionElementsUsingRelationship;
import static org.apache.atlas.repository.graph.GraphHelper.getDelimitedClassificationNames;
import static org.apache.atlas.repository.graph.GraphHelper.getLabels;
import static org.apache.atlas.repository.graph.GraphHelper.getMapElementsProperty;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeNames;
import static org.apache.atlas.repository.graph.GraphHelper.isPropagationEnabled;
import static org.apache.atlas.repository.graph.GraphHelper.isRelationshipEdge;
import static org.apache.atlas.repository.graph.GraphHelper.string;
import static org.apache.atlas.repository.graph.GraphHelper.updateModificationMetadata;
import static org.apache.atlas.repository.graph.GraphHelper.getEntityHasLineage;
import static org.apache.atlas.repository.graph.GraphHelper.isTermEntityEdge;
import static org.apache.atlas.repository.graph.GraphHelper.getRemovePropagations;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatedEdges;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatableClassifications;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEntityGuid;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.INPUT_PORT_GUIDS_ATTR;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.OUTPUT_PORT_GUIDS_ATTR;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.*;
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
    private              boolean DEFERRED_ACTION_ENABLED                             = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();
    private              boolean DIFFERENTIAL_AUDITS                                 = STORE_DIFFERENTIAL_AUDITS.getBoolean();

    private static final int     MAX_NUMBER_OF_RETRIES = AtlasConfiguration.MAX_NUMBER_OF_RETRIES.getInt();
    private static final int     CHUNK_SIZE            = AtlasConfiguration.TASKS_GRAPH_COMMIT_CHUNK_SIZE.getInt();

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

    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(TYPE_GLOSSARY, TYPE_CATEGORY, TYPE_TERM, TYPE_PRODUCT, TYPE_DOMAIN));

    @Inject
    public EntityGraphMapper(DeleteHandlerDelegate deleteDelegate, RestoreHandlerV1 restoreHandlerV1, AtlasTypeRegistry typeRegistry, AtlasGraph graph,
                             AtlasRelationshipStore relationshipStore, IAtlasEntityChangeNotifier entityChangeNotifier,
                             AtlasInstanceConverter instanceConverter, IFullTextMapper fullTextMapperV2,
                             TaskManagement taskManagement, TransactionInterceptHelper transactionInterceptHelper) {
        this.restoreHandlerV1 = restoreHandlerV1;
        this.graphHelper          = new GraphHelper(graph);
        this.deleteDelegate       = deleteDelegate;
        this.typeRegistry         = typeRegistry;
        this.graph                = graph;
        this.relationshipStore    = relationshipStore;
        this.entityChangeNotifier = entityChangeNotifier;
        this.instanceConverter    = instanceConverter;
        this.entityRetriever      = new EntityGraphRetriever(graph, typeRegistry);
        this.retrieverNoRelation  = new EntityGraphRetriever(graph, typeRegistry, true);
        this.fullTextMapperV2     = fullTextMapperV2;
        this.taskManagement       = taskManagement;
        this.transactionInterceptHelper = transactionInterceptHelper;}

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
                                                                  final boolean replaceClassifications,
                                                                  boolean replaceBusinessAttributes,
                                                                  boolean isOverwriteBusinessAttribute) throws AtlasBaseException {

        MetricRecorder metric = RequestContext.get().startMetricRecord("mapAttributesAndClassifications");

        EntityMutationResponse resp = new EntityMutationResponse();
        RequestContext reqContext = RequestContext.get();

        if (CollectionUtils.isNotEmpty(context.getEntitiesToRestore())) {
            restoreHandlerV1.restoreEntities(context.getEntitiesToRestore());
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
                    resp.addEntity(CREATE, constructHeader(createdEntity, vertex, entityType.getAllAttributes()));
                    addClassifications(context, guid, createdEntity.getClassifications());

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

                    if (removedEdges != null && removedEdges.size() > 0) {
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

                    if (replaceClassifications) {
                        deleteClassifications(guid);
                        addClassifications(context, guid, updatedEntity.getClassifications());
                    }

                    if (replaceBusinessAttributes) {
                        if (MapUtils.isEmpty(updatedEntity.getBusinessAttributes()) && isOverwriteBusinessAttribute) {
                            Map<String, Map<String, Object>> businessMetadata = entityRetriever.getBusinessMetadata(vertex);
                            if (MapUtils.isNotEmpty(businessMetadata)) {
                                removeBusinessAttributes(vertex, entityType, businessMetadata);
                            }
                        } else {
                            addOrUpdateBusinessAttributes(guid, updatedEntity.getBusinessAttributes(), isOverwriteBusinessAttribute);
                        }
                    }

                    setSystemAttributesToEntity(vertex, updatedEntity);
                    resp.addEntity(updateType, constructHeader(updatedEntity, vertex, entityType.getAllAttributes()));

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

                    if (removedEdges != null && removedEdges.size() > 0) {
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
        } else {

            if (CollectionUtils.isNotEmpty(appendEntities)) {
                for (AtlasEntity entity : appendEntities) {
                    String guid = entity.getGuid();
                    AtlasVertex vertex = context.getVertex(guid);
                    AtlasEntityType entityType = context.getType(guid);
                    mapAppendRemoveRelationshipAttributes(entity, entityType, vertex, UPDATE, context, true, false);
                }
            }

            if (CollectionUtils.isNotEmpty(removeEntities)) {
                for (AtlasEntity entity : removeEntities) {
                    String guid = entity.getGuid();
                    AtlasVertex vertex = context.getVertex(guid);
                    AtlasEntityType entityType = context.getType(guid);
                    mapAppendRemoveRelationshipAttributes(entity, entityType, vertex, UPDATE, context, false, true);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(context.getEntitiesToDelete())) {
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

        if (req.getRestoredEntities() != null && req.getRestoredEntities().size() > 0) {
            for (AtlasEntityHeader entity : req.getRestoredEntities()) {
                resp.addEntity(UPDATE, entity);
            }
        }

        RequestContext.get().endMetricRecord(metric);

        return resp;
    }

    private void setSystemAttributesToEntity(AtlasVertex entityVertex, AtlasEntity createdEntity) {

        createdEntity.setCreatedBy(GraphHelper.getCreatedByAsString(entityVertex));
        createdEntity.setUpdatedBy(GraphHelper.getModifiedByAsString(entityVertex));
        createdEntity.setCreateTime(new Date(GraphHelper.getCreatedTime(entityVertex)));
        createdEntity.setUpdateTime(new Date(GraphHelper.getModifiedTime(entityVertex)));


        if (DIFFERENTIAL_AUDITS) {
            AtlasEntity diffEntity = RequestContext.get().getDifferentialEntity(createdEntity.getGuid());
            if (diffEntity != null) {
                diffEntity.setUpdateTime(createdEntity.getUpdateTime());
                diffEntity.setUpdatedBy(createdEntity.getUpdatedBy());
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
            entityChangeNotifier.onBusinessAttributesUpdated(AtlasGraphUtilsV2.getIdFromVertex(entityVertex), updatedBusinessAttributes);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== removeBusinessAttributes(entityVertex={}, entityType={}, businessAttributes={}", entityVertex, entityType.getTypeName(), businessAttributes);
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

    private void mapAttributes(AtlasStruct struct, AtlasStructType structType, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
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
    }

    private Object mapToVertexByTypeCategory(AttributeMutationContext ctx, EntityMutationContext context, boolean isAppendOp, boolean isRemoveOp) throws AtlasBaseException {
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
                    ret = updateRelationship(ctx.getCurrentEdge(), entityVertex, attributeVertex, attribute.getRelationshipEdgeDirection(), relationshipAttributes);
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

                    boolean isCreated = graphHelper.getCreatedTime(ret) == RequestContext.get().getRequestTime();

                    if (isCreated) {
                        // if relationship did not exist before and new relationship was created
                        // record entity update on both relationship vertices
                        recordEntityUpdate(attributeVertex);
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

                AtlasEdge edge = null;

                if (createEdge) {
                    edge = relationshipStore.getOrCreate(fromVertex, toVertex, new AtlasRelationship(relationshipName));
                    boolean isCreated = graphHelper.getCreatedTime(edge) == RequestContext.get().getRequestTime();

                    if (isCreated) {
                        // if relationship did not exist before and new relationship was created
                        // record entity update on both relationship vertices
                        recordEntityUpdateForNonRelationsipAttribute(fromVertex);
                        recordEntityUpdateForNonRelationsipAttribute(toVertex);
                    }

                } else {
                    edge = relationshipStore.getRelationship(fromVertex, toVertex, new AtlasRelationship(relationshipName));
                }
                ret = edge;
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
        Map<String, Object> currentMap  = getMapElementsProperty(mapType, ctx.getReferringVertex(), ctx.getVertexProperty(), attribute);
        boolean             isReference = isReference(mapType.getValueType());
        boolean             isSoftReference = ctx.getAttribute().getAttributeDef().isSoftReferenced();

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

        String propertyName = ctx.getVertexProperty();

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

           if (element instanceof AtlasEdge) {
               AtlasGraphUtilsV2.setEncodedProperty((AtlasEdge) element, ATTRIBUTE_INDEX_PROPERTY_KEY, index);
            }
        }

        if (isNewElementsNull) {
            setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(), null, null, cardinality);
        } else {
            // executes
            setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(), allArrayElements, currentElements, cardinality);
        }

        switch (ctx.getAttribute().getRelationshipEdgeLabel()) {
            case TERM_ASSIGNMENT_LABEL: addMeaningsToEntity(ctx, newElementsCreated, removedElements);
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
                addInternalProductAttr(ctx, newElementsCreated, removedElements);
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

        switch (ctx.getAttribute().getRelationshipEdgeLabel()) {
            case TERM_ASSIGNMENT_LABEL: addMeaningsToEntity(ctx, newElementsCreated, new ArrayList<>(0));
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
                addInternalProductAttr(ctx, newElementsCreated, null);
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

            // throw error if relation does not exist but requested to remove
            if (deleteEntry == null) {
                AtlasVertex attributeVertex = context.getDiscoveryContext().getResolvedEntityVertex(getGuid(arrCtx.getValue()));
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_DOES_NOT_EXIST, attribute.getRelationshipName(),
                        AtlasGraphUtilsV2.getIdFromVertex(attributeVertex), AtlasGraphUtilsV2.getIdFromVertex(ctx.getReferringVertex()));
            }

            entityRelationsDeleted.add(deleteEntry);
        }

        removedElements = removeArrayEntries(attribute, (List)entityRelationsDeleted, ctx);


        switch (ctx.getAttribute().getRelationshipEdgeLabel()) {
            case TERM_ASSIGNMENT_LABEL: addMeaningsToEntity(ctx, new ArrayList<>(0) , removedElements);
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
                addInternalProductAttr(ctx, null , removedElements);
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

    private void addInternalProductAttr(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements) throws AtlasBaseException {
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

            addOrRemoveDaapInternalAttr(toVertex, attrName, createdElements, deletedElements);
        }else{
           throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Can not update product relations while updating any asset");
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void addOrRemoveDaapInternalAttr(AtlasVertex toVertex, String internalAttr, List<Object> createdElements, List<AtlasEdge> deletedElements) {
        if (CollectionUtils.isNotEmpty(createdElements)) {
            List<String> addedGuids = createdElements.stream().map(x -> ((AtlasEdge) x).getOutVertex().getProperty("__guid", String.class)).collect(Collectors.toList());
            addedGuids.forEach(guid -> AtlasGraphUtilsV2.addEncodedProperty(toVertex, internalAttr, guid));
        }

        if (CollectionUtils.isNotEmpty(deletedElements)) {
            List<String> removedGuids = deletedElements.stream().map(x -> x.getOutVertex().getProperty("__guid", String.class)).collect(Collectors.toList());
            removedGuids.forEach(guid -> AtlasGraphUtilsV2.removeItemFromListPropertyValue(toVertex, internalAttr, guid));
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

    private void addMeaningsToEntity(AttributeMutationContext ctx, List<Object> createdElements, List<AtlasEdge> deletedElements) {
        MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("addMeaningsToEntity");
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

        List<String> newMeaningsNames = meanings.stream()
                .filter(x -> !currentMeaningsQNames.contains(x.getProperty(QUALIFIED_NAME,String.class)))
                .map(x -> x.getProperty(NAME, String.class))
                .collect(Collectors.toList());

        ctx.getReferringVertex().removeProperty(MEANINGS_PROPERTY_KEY);
        ctx.getReferringVertex().removeProperty(MEANINGS_TEXT_PROPERTY_KEY);

        if (CollectionUtils.isNotEmpty(qNames)) {
            qNames.forEach(q -> AtlasGraphUtilsV2.addEncodedProperty(ctx.getReferringVertex(), MEANINGS_PROPERTY_KEY, q));
        }

        if (CollectionUtils.isNotEmpty(names)) {
            AtlasGraphUtilsV2.setEncodedProperty(ctx.referringVertex, MEANINGS_TEXT_PROPERTY_KEY, StringUtils.join(names, ","));
        }

        if (CollectionUtils.isNotEmpty(newMeaningsNames)) {
            newMeaningsNames.forEach(q -> AtlasGraphUtilsV2.addListProperty(ctx.getReferringVertex(), MEANING_NAMES_PROPERTY_KEY, q, true));
        }

        if(createdElements.isEmpty()){
            ctx.getReferringVertex().removeProperty(MEANING_NAMES_PROPERTY_KEY);

        } else if (CollectionUtils.isNotEmpty(deletedMeaningsNames)) {
            deletedMeaningsNames.forEach(q -> AtlasGraphUtilsV2.removeItemFromListPropertyValue(ctx.getReferringVertex(), MEANING_NAMES_PROPERTY_KEY, q));

        }

        RequestContext.get().endMetricRecord(metricRecorder);
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

                Object typeNameVal = map.get(AtlasObjectId.KEY_TYPENAME);
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


    private AtlasEdge updateRelationship(AtlasEdge currentEdge, final AtlasVertex parentEntityVertex, final AtlasVertex newEntityVertex,
                                         AtlasRelationshipEdgeDirection edgeDirection,  Map<String, Object> relationshipAttributes)
            throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating entity reference using relationship {} for reference attribute {}", getTypeName(newEntityVertex));
        }

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

        if (!currentEntityId.equals(newEntityId)) {
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

            //record entity update on new relationship vertex
            recordEntityUpdate(newEntityVertex);
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
                    List<AtlasEdge> additionalElements = new ArrayList<>();

                    for (AtlasEdge edge : edgesToRemove) {
                        if (getStatus(edge) == DELETED ) {
                            continue;
                        }

                        boolean deleted = deleteDelegate.getHandler().deleteEdgeReference(edge, entryType.getTypeCategory(), attribute.isOwnedRef(),
                                true, attribute.getRelationshipEdgeDirection(), entityVertex);

                        if (!deleted) {
                            additionalElements.add(edge);
                        }
                    }

                    return additionalElements;
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
                        if (getStatus(edge) == DELETED ) {
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


    private AtlasEntityHeader constructHeader(AtlasEntity entity, AtlasVertex vertex, Map<String, AtlasAttribute> attributeMap ) throws AtlasBaseException {
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


    public void cleanUpClassificationPropagation(String classificationName, int batchLimit) throws AtlasBaseException {
        int CLEANUP_MAX = batchLimit <= 0 ? CLEANUP_BATCH_SIZE : batchLimit * CLEANUP_BATCH_SIZE;
        int cleanedUpCount = 0;
        final int CHUNK_SIZE_TEMP = 50;
        long classificationEdgeCount = 0;
        Iterator<AtlasVertex> tagVertices = GraphHelper.getClassificationVertices(graph, classificationName, CLEANUP_BATCH_SIZE);
        List<AtlasVertex> tagVerticesProcessed = new ArrayList<>(0);
        List<AtlasVertex> currentAssetVerticesBatch = new ArrayList<>(0);

        while (tagVertices != null && tagVertices.hasNext()) {
            if (cleanedUpCount >= CLEANUP_MAX){
                return;
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
                        int toIndex = Math.min((offset + CHUNK_SIZE_TEMP), currentAssetsBatchSize);
                        List<AtlasVertex> entityVertices = currentAssetVerticesBatch.subList(offset, toIndex);
                        List<String> impactedGuids = entityVertices.stream().map(GraphHelper::getGuid).collect(Collectors.toList());
                        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedGuids);

                        for (AtlasVertex vertex : entityVertices) {
                            List<AtlasClassification> deletedClassifications = new ArrayList<>();
                            List<AtlasEdge> classificationEdges = GraphHelper.getClassificationEdges(vertex, null, classificationName);
                            classificationEdgeCount += classificationEdges.size();
                            for (AtlasEdge edge : classificationEdges) {
                                try {
                                    AtlasClassification classification = entityRetriever.toAtlasClassification(edge.getInVertex());
                                    deletedClassifications.add(classification);
                                    deleteDelegate.getHandler().deleteEdgeReference(edge, TypeCategory.CLASSIFICATION, false, true, null, vertex);
                                }
                                catch (IllegalStateException | AtlasBaseException e){
                                    e.printStackTrace();
                                }
                            }

                            AtlasEntity entity = repairClassificationMappings(vertex);

                            entityChangeNotifier.onClassificationDeletedFromEntity(entity, deletedClassifications);
                        }

                        transactionInterceptHelper.intercept();

                        offset += CHUNK_SIZE_TEMP;
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

    public void addClassifications(final EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
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
                    LOG.debug("created vertex {} for trait {}", string(classificationVertex), classificationName);
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
                            LOG.debug("Propagating tag: [{}][{}] to {}", classificationName, entityType.getTypeName(), getTypeNames(entitiesToPropagateTo));
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


    public List<String> propagateClassification(String entityGuid, String classificationVertexId, String relationshipGuid, Boolean previousRestrictPropagationThroughLineage,Boolean previousRestrictPropagationThroughHierarchy) throws AtlasBaseException {
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
            List<AtlasVertex> impactedVertices = entityRetriever.getIncludedImpactedVerticesV2(entityVertex, relationshipGuid, classificationVertexId, edgeLabelsToCheck,toExclude);

            if (CollectionUtils.isEmpty(impactedVertices)) {
                LOG.debug("propagateClassification(entityGuid={}, classificationVertexId={}): found no entities to propagate the classification", entityGuid, classificationVertexId);

                return null;
            }

            return processClassificationPropagationAddition(impactedVertices, classificationVertex);
        } catch (Exception e) {
            LOG.error("propagateClassification(entityGuid={}, classificationVertexId={}): error while propagating classification", entityGuid, classificationVertexId, e);

            throw new AtlasBaseException(e);
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
                toIndex = ((offset + CHUNK_SIZE > impactedVerticesSize) ? impactedVerticesSize : (offset + CHUNK_SIZE));
                List<AtlasVertex> chunkedVerticesToPropagate = verticesToPropagate.subList(offset, toIndex);

                AtlasPerfMetrics.MetricRecorder metricRecorder  = RequestContext.get().startMetricRecord("lockObjectsAfterTraverse");
                List<String> impactedVerticesGuidsToLock        = chunkedVerticesToPropagate.stream().map(x -> GraphHelper.getGuid(x)).collect(Collectors.toList());
                GraphTransactionInterceptor.lockObjectAndReleasePostCommit(impactedVerticesGuidsToLock);
                RequestContext.get().endMetricRecord(metricRecorder);

                AtlasClassification classification       = entityRetriever.toAtlasClassification(classificationVertex);
                List<AtlasVertex>   entitiesPropagatedTo = deleteDelegate.getHandler().addTagPropagation(classificationVertex, chunkedVerticesToPropagate);

                if (CollectionUtils.isEmpty(entitiesPropagatedTo)) {
                    return null;
                }

                List<AtlasEntity>   propagatedEntitiesChunked       = updateClassificationText(classification, entitiesPropagatedTo);
                List<String>        chunkedPropagatedEntitiesGuids  = propagatedEntitiesChunked.stream().map(x -> x.getGuid()).collect(Collectors.toList());
                entityChangeNotifier.onClassificationsAddedToEntities(propagatedEntitiesChunked, Collections.singletonList(classification), false);

                propagatedEntitiesGuids.addAll(chunkedPropagatedEntitiesGuids);

                offset += CHUNK_SIZE;

                transactionInterceptHelper.intercept();

            } while (offset < impactedVerticesSize);
        } catch (AtlasBaseException exception) {
            LOG.error("Error occurred while adding classification propagation for classification with propagation id {}", classificationVertex.getIdForDisplay());
            throw exception;
        } finally {
            RequestContext.get().endMetricRecord(classificationPropagationMetricRecorder);
        }

    return propagatedEntitiesGuids;

    }

    public void deleteClassification(String entityGuid, String classificationName, String associatedEntityGuid) throws AtlasBaseException {
        if (StringUtils.isEmpty(associatedEntityGuid) || associatedEntityGuid.equals(entityGuid)) {
            deleteClassification(entityGuid, classificationName);
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

    public void deleteClassification(String entityGuid, String classificationName) throws AtlasBaseException {
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

        List<String> traitNames = getTraitNames(entityVertex);

        if (CollectionUtils.isEmpty(traitNames)) {
            throw new AtlasBaseException(AtlasErrorCode.NO_CLASSIFICATIONS_FOUND_FOR_ENTITY, entityGuid);
        }

        validateClassificationExists(traitNames, classificationName);

        AtlasVertex         classificationVertex = getClassificationVertex(entityVertex, classificationName);

        // Get in progress task to see if there already is a propagation for this particular vertex
        List<AtlasTask> inProgressTasks = taskManagement.getInProgressTasks();
        for (AtlasTask task : inProgressTasks) {
            if (isTaskMatchingWithVertexIdAndEntityGuid(task, classificationVertex.getIdForDisplay(), entityGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_CURRENTLY_BEING_PROPAGATED, classificationName);
            }
        }

        AtlasClassification classification       = entityRetriever.toAtlasClassification(classificationVertex);

        if (classification == null) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classificationName);
        }

        // remove classification from propagated entities if propagation is turned on
        final List<AtlasVertex> entityVertices;

        if (isPropagationEnabled(classificationVertex)) {
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

    private boolean isTaskMatchingWithVertexIdAndEntityGuid(AtlasTask task, String classificationVertexId, String entityGuid) {
        try {
            if (CLASSIFICATION_PROPAGATION_ADD.equals(task.getType())) {
                return task.getParameters().get(ClassificationTask.PARAM_CLASSIFICATION_VERTEX_ID).equals(classificationVertexId)
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

    public void updateClassifications(EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
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

        for (AtlasClassification classification : classifications) {
            String classificationName       = classification.getTypeName();
            String classificationEntityGuid = classification.getEntityGuid();

            if (StringUtils.isEmpty(classificationEntityGuid)) {
                classification.setEntityGuid(guid);
            }

            if (StringUtils.isNotEmpty(classificationEntityGuid) && !StringUtils.equalsIgnoreCase(guid, classificationEntityGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_UPDATE_FROM_PROPAGATED_ENTITY, classificationName);
            }

            AtlasVertex classificationVertex = getClassificationVertex(entityVertex, classificationName);

            if (classificationVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, classificationName);
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

                isClassificationUpdated = true;
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
                LOG.debug("updating vertex {} for trait {}", string(classificationVertex), classificationName);
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

                String propagationType = CLASSIFICATION_PROPAGATION_ADD;
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

        List<String> traitNames = getTraitNames(instanceVertex);

        if (CollectionUtils.isNotEmpty(traitNames)) {
            for (String traitName : traitNames) {
                deleteClassification(guid, traitName);
            }
        }
    }

    public List<String> deleteClassificationPropagation(String entityGuid, String classificationVertexId) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(classificationVertexId)) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification vertex id is empty", classificationVertexId);

                return null;
            }

            AtlasVertex classificationVertex = graph.getVertex(classificationVertexId);
            if (classificationVertex == null) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification vertex not found", classificationVertexId);

                return null;
            }

            AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);

            List<AtlasEdge> propagatedEdges = getPropagatedEdges(classificationVertex);
            if (propagatedEdges.isEmpty()) {
                LOG.warn("deleteClassificationPropagation(classificationVertexId={}): classification edges empty", classificationVertexId);

                return null;
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

    public void deleteClassificationOnlyPropagation(Set<String> deletedEdgeIds) throws AtlasBaseException {
        RequestContext.get().getDeletedEdgesIds().clear();
        RequestContext.get().getDeletedEdgesIds().addAll(deletedEdgeIds);

        for (AtlasEdge edge : deletedEdgeIds.stream().map(x -> graph.getEdge(x)).collect(Collectors.toList())) {

            boolean isRelationshipEdge = deleteDelegate.getHandler().isRelationshipEdge(edge);
            String  relationshipGuid   = GraphHelper.getRelationshipGuid(edge);

            if (edge == null || !isRelationshipEdge) {
                continue;
            }

            List<AtlasVertex> currentClassificationVertices = getPropagatableClassifications(edge);

            for (AtlasVertex currentClassificationVertex : currentClassificationVertices) {
                LOG.info("Starting Classification {} Removal for deletion of edge {}",currentClassificationVertex.getIdForDisplay(), edge.getIdForDisplay());
                boolean isTermEntityEdge = isTermEntityEdge(edge);
                boolean removePropagationOnEntityDelete = getRemovePropagations(currentClassificationVertex);

                if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
                    LOG.debug("This edge is not term edge or remove propagation isn't enabled");
                    continue;
                }

                processClassificationDeleteOnlyPropagation(currentClassificationVertex, relationshipGuid);
                LOG.info("Finished Classification {} Removal for deletion of edge {}",currentClassificationVertex.getIdForDisplay(), edge.getIdForDisplay());
            }
        }
    }

    public void deleteClassificationOnlyPropagation(String deletedEdgeId, String classificationVertexId) throws AtlasBaseException {
        RequestContext.get().getDeletedEdgesIds().clear();
        RequestContext.get().getDeletedEdgesIds().add(deletedEdgeId);

        AtlasEdge edge = graph.getEdge(deletedEdgeId);

        boolean isRelationshipEdge = deleteDelegate.getHandler().isRelationshipEdge(edge);
        String  relationshipGuid   = GraphHelper.getRelationshipGuid(edge);

        if (edge == null || !isRelationshipEdge) {
            return;
        }

        AtlasVertex currentClassificationVertex = graph.getVertex(classificationVertexId);
        if (currentClassificationVertex == null) {
            LOG.warn("Classification Vertex with ID {} is not present or Deleted", classificationVertexId);
            return;
        }

        List<AtlasVertex> currentClassificationVertices = getPropagatableClassifications(edge);
        if (! currentClassificationVertices.contains(currentClassificationVertex)) {
            return;
        }

        boolean isTermEntityEdge = isTermEntityEdge(edge);
        boolean removePropagationOnEntityDelete = getRemovePropagations(currentClassificationVertex);

        if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
            LOG.debug("This edge is not term edge or remove propagation isn't enabled");
            return;
        }

        processClassificationDeleteOnlyPropagation(currentClassificationVertex, relationshipGuid);

        LOG.info("Finished Classification {} Removal for deletion of edge {}",currentClassificationVertex.getIdForDisplay(), edge.getIdForDisplay());
    }

    public void deleteClassificationOnlyPropagation(String classificationId, String referenceVertexId, boolean isTermEntityEdge) throws AtlasBaseException {
        AtlasVertex classificationVertex = graph.getVertex(classificationId);
        AtlasVertex referenceVertex = graph.getVertex(referenceVertexId);

        if (classificationVertex == null) {
            LOG.warn("Classification Vertex with ID {} is not present or Deleted", classificationId);
            return;
        }
        /*
            If reference vertex is deleted, we can consider that as this connected vertex was deleted
             some other task was created before it to remove propagations. No need to execute this task.
         */
        if (referenceVertex == null) {
            LOG.warn("Reference Vertex {} is deleted", referenceVertexId);
            return;
        }

        if (!GraphHelper.propagatedClassificationAttachedToVertex(classificationVertex, referenceVertex)) {
            LOG.warn("No Classification is attached to the reference vertex {} for classification {}", referenceVertexId, classificationId);
            return;
        }

        boolean removePropagationOnEntityDelete = getRemovePropagations(classificationVertex);

        if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
            LOG.debug("This edge is not term edge or remove propagation isn't enabled");
            return;
        }

        processClassificationDeleteOnlyPropagation(classificationVertex, null);

        LOG.info("Completed propagation removal via edge for classification {}", classificationId);
    }

    public void classificationRefreshPropagation(String classificationId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder classificationRefreshPropagationMetricRecorder = RequestContext.get().startMetricRecord("classificationRefreshPropagation");

        AtlasVertex currentClassificationVertex             = graph.getVertex(classificationId);
        if (currentClassificationVertex == null) {
            LOG.warn("Classification vertex with ID {} is deleted", classificationId);
            return;
        }

        String              sourceEntityId                  = getClassificationEntityGuid(currentClassificationVertex);
        AtlasVertex         sourceEntityVertex              = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityId);
        AtlasClassification classification                  = entityRetriever.toAtlasClassification(currentClassificationVertex);

        String propagationMode;

        Boolean restrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);
        Boolean restrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);

        propagationMode = entityRetriever.determinePropagationMode(restrictPropagationThroughLineage,restrictPropagationThroughHierarchy);
        Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true:false;

        List<String> propagatedVerticesIds = GraphHelper.getPropagatedVerticesIds(currentClassificationVertex);
        LOG.info("{} entity vertices have classification with id {} attached", propagatedVerticesIds.size(), classificationId);

        List<String> verticesIdsToAddClassification =  new ArrayList<>();
        List<String> propagatedVerticesIdWithoutEdge = entityRetriever.getImpactedVerticesIdsClassificationAttached(sourceEntityVertex , classificationId,
                CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude, verticesIdsToAddClassification);

        LOG.info("To add classification with id {} to {} vertices", classificationId, verticesIdsToAddClassification.size());

        List<String> verticesIdsToRemove = (List<String>)CollectionUtils.subtract(propagatedVerticesIds, propagatedVerticesIdWithoutEdge);

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
            return;
        }
        processClassificationPropagationAddition(verticesToAddClassification, currentClassificationVertex);

        LOG.info("Completed refreshing propagation for classification with vertex id {} with classification name {} and source entity {}",classificationId,
                classification.getTypeName(), classification.getEntityGuid());

        RequestContext.get().endMetricRecord(classificationRefreshPropagationMetricRecorder);
    }

    private void processClassificationDeleteOnlyPropagation(AtlasVertex currentClassificationVertex, String relationshipGuid) throws AtlasBaseException {
        String              classificationId                = currentClassificationVertex.getIdForDisplay();
        String              sourceEntityId                  = getClassificationEntityGuid(currentClassificationVertex);
        AtlasVertex         sourceEntityVertex              = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityId);
        AtlasClassification classification                  = entityRetriever.toAtlasClassification(currentClassificationVertex);

        String propagationMode;

        Boolean restrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);
        Boolean restrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(currentClassificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);
        propagationMode = entityRetriever.determinePropagationMode(restrictPropagationThroughLineage,restrictPropagationThroughHierarchy);
        Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true : false;
        List<String> propagatedVerticesIds = GraphHelper.getPropagatedVerticesIds(currentClassificationVertex);
        LOG.info("Traversed {} vertices including edge with relationship GUID {} for classification vertex {}", propagatedVerticesIds.size(), relationshipGuid, classificationId);

        List<String> propagatedVerticesIdWithoutEdge = entityRetriever.getImpactedVerticesIds(sourceEntityVertex, relationshipGuid , classificationId,
                CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude);

        LOG.info("Traversed {} vertices except edge with relationship GUID {} for classification vertex {}", propagatedVerticesIdWithoutEdge.size(), relationshipGuid, classificationId);

        List<String> verticesIdsToRemove = (List<String>)CollectionUtils.subtract(propagatedVerticesIds, propagatedVerticesIdWithoutEdge);

        List<AtlasVertex> verticesToRemove = verticesIdsToRemove.stream()
                .map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());

        propagatedVerticesIdWithoutEdge.clear();
        propagatedVerticesIds.clear();

        LOG.info("To delete classification from {} vertices for deletion of edge with relationship GUID {} and classification {}", verticesToRemove.size(), relationshipGuid, classificationId);

        processPropagatedClassificationDeletionFromVertices(verticesToRemove, currentClassificationVertex, classification);

        LOG.info("Completed remove propagation for edge with relationship GUID {} and classification vertex {} with classification name {} and source entity {}", relationshipGuid,
                classificationId, classification.getTypeName(), classification.getEntityGuid());
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
                entityChangeNotifier.onClassificationsDeletedFromEntities(updatedEntities, Collections.singletonList(classification));

                offset += CHUNK_SIZE;

                transactionInterceptHelper.intercept();

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

            entityChangeNotifier.onClassificationsDeletedFromEntities(propagatedEntities, Collections.singletonList(classification));
            if(! propagatedEntities.isEmpty()) {
                deletedPropagationsGuid.addAll(propagatedEntities.stream().map(x -> x.getGuid()).collect(Collectors.toList()));
            }

            offset += CHUNK_SIZE;

            transactionInterceptHelper.intercept();

        } while (offset < propagatedEdgesSize);

        return deletedPropagationsGuid;
    }

    @GraphTransaction
    public void updateTagPropagations(String relationshipEdgeId, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasEdge relationshipEdge = graph.getEdge(relationshipEdgeId);

        deleteDelegate.getHandler().updateTagPropagations(relationshipEdge, relationship);

        entityChangeNotifier.notifyPropagatedEntities();
    }

    private void validateClassificationExists(List<String> existingClassifications, List<String> suppliedClassifications) throws AtlasBaseException {
        Set<String> existingNames = new HashSet<>(existingClassifications);
        for (String classificationName : suppliedClassifications) {
            if (!existingNames.contains(classificationName)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, classificationName);
            }
        }
    }

    private void validateClassificationExists(List<String> existingClassifications, String suppliedClassificationName) throws AtlasBaseException {
        if (!existingClassifications.contains(suppliedClassificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, suppliedClassificationName);
        }
    }

    private AtlasEdge getOrCreateRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, String relationshipName,
                                              Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        return relationshipStore.getOrCreate(end1Vertex, end2Vertex, new AtlasRelationship(relationshipName, relationshipAttributes));
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
        AtlasClassificationType type = typeRegistry.getClassificationTypeByName(classification.getTypeName());

        if (type == null) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classification.getTypeName());
        }

        List<String> messages = new ArrayList<>();

        type.validateValueForUpdate(classification, classification.getTypeName(), messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, messages);
        }

        type.getNormalizedValueForUpdate(classification);
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

        for (AtlasEdge atlasEdge : inputOutputEdges) {

            boolean isOutputEdge = PROCESS_OUTPUTS.equals(atlasEdge.getLabel());

            AtlasVertex processVertex = atlasEdge.getOutVertex();
            AtlasVertex assetVertex = atlasEdge.getInVertex();

            if (getEntityHasLineage(processVertex)) {
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, true);
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
                        isHasLineageSet = true;
                    }

                    if (isRestoreEntity) {
                        AtlasGraphUtilsV2.setEncodedProperty(oppositeEdgeAssetVertex, HAS_LINEAGE, true);
                    } else {
                        break;
                    }
                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }


    public List<AtlasVertex> linkBusinessPolicy(String policyId, Set<String> linkGuids) {
        return linkGuids.stream().map(guid -> findByGuid(graph, guid)).filter(Objects::nonNull).filter(ev -> {
            Set<String> existingValues = ev.getMultiValuedSetProperty(ASSET_POLICY_GUIDS, String.class);
            return !existingValues.contains(policyId);
        }).peek(ev -> {
            Set<String> existingValues = ev.getMultiValuedSetProperty(ASSET_POLICY_GUIDS, String.class);
            existingValues.add(policyId);
            ev.setProperty(ASSET_POLICY_GUIDS, policyId);
            ev.setProperty(ASSET_POLICIES_COUNT, existingValues.size());

            updateModificationMetadata(ev);

            cacheDifferentialEntity(ev, existingValues);
        }).collect(Collectors.toList());
    }


    public List<AtlasVertex> unlinkBusinessPolicy(String policyId, Set<String> unlinkGuids) {
        return unlinkGuids.stream().map(guid -> AtlasGraphUtilsV2.findByGuid(graph, guid)).filter(Objects::nonNull).filter(ev -> {
            Set<String> existingValues = ev.getMultiValuedSetProperty(ASSET_POLICY_GUIDS, String.class);
            return existingValues.contains(policyId);
        }).peek(ev -> {
            Set<String> existingValues = ev.getMultiValuedSetProperty(ASSET_POLICY_GUIDS, String.class);
            existingValues.remove(policyId);
            ev.removePropertyValue(ASSET_POLICY_GUIDS, policyId);
            ev.setProperty(ASSET_POLICIES_COUNT, existingValues.size());

            updateModificationMetadata(ev);

            cacheDifferentialEntity(ev, existingValues);
        }).collect(Collectors.toList());
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
    public AtlasVertex moveBusinessPolicies(Set<String> policyIds, String assetId, String type) throws AtlasBaseException {
        // Retrieve the AtlasVertex for the given assetId
        AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(graph, assetId);

        if (assetVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset with guid not found");
        }

        // Get the sets of governed and non-compliant policy GUIDs
        Set<String> governedPolicies = assetVertex.getMultiValuedSetProperty(ASSET_POLICY_GUIDS, String.class);
        Set<String> nonCompliantPolicies = assetVertex.getMultiValuedSetProperty(NON_COMPLIANT_ASSET_POLICY_GUIDS, String.class);

        // Determine if the type is governed or non-compliant
        boolean isGoverned = MoveBusinessPolicyRequest.Type.GOVERNED.getDescription().equals(type);
        Set<String> currentPolicies = isGoverned ? new HashSet<>(governedPolicies) : new HashSet<>(nonCompliantPolicies);
        policyIds.removeAll(currentPolicies);

        // Check if the asset already has the given policy IDs
        if (policyIds.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Asset already has the given policy id");
        }

        // Move policies to the appropriate set
        policyIds.forEach(policyId -> {
            if (isGoverned) {
                assetVertex.setProperty(ASSET_POLICY_GUIDS, policyId);
                removeItemFromListPropertyValue(assetVertex, NON_COMPLIANT_ASSET_POLICY_GUIDS, policyId);
            } else {
                assetVertex.setProperty(NON_COMPLIANT_ASSET_POLICY_GUIDS, policyId);
                removeItemFromListPropertyValue(assetVertex, ASSET_POLICY_GUIDS, policyId);
            }
        });

        // Update the sets after processing
        if (isGoverned) {
            governedPolicies.addAll(policyIds);
            nonCompliantPolicies.removeAll(policyIds);
        } else {
            nonCompliantPolicies.addAll(policyIds);
            governedPolicies.removeAll(policyIds);
        }

        // Update the modification metadata
        updateModificationMetadata(assetVertex);

        // Create a differential AtlasEntity to reflect the changes
        AtlasEntity diffEntity = new AtlasEntity(assetVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        setEntityCommonAttributes(assetVertex, diffEntity);
        diffEntity.setAttribute(ASSET_POLICY_GUIDS, governedPolicies);
        diffEntity.setAttribute(NON_COMPLIANT_ASSET_POLICY_GUIDS, nonCompliantPolicies);

        // Cache the differential entity for further processing
        RequestContext.get().cacheDifferentialEntity(diffEntity);

        return assetVertex;
    }

    private void cacheDifferentialEntity(AtlasVertex ev, Set<String> existingValues) {
        AtlasEntity diffEntity = new AtlasEntity(ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        setEntityCommonAttributes(ev, diffEntity);
        diffEntity.setAttribute(ASSET_POLICY_GUIDS, existingValues);
        diffEntity.setAttribute(ASSET_POLICIES_COUNT, existingValues.size());

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
}
