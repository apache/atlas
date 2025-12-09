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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.TimeBoundary;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.glossary.enums.AtlasTermAssignmentStatus;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.EdgeVertexReference;
import org.apache.atlas.repository.VertexEdgePropertiesCache;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.*;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertex;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.repository.store.graph.v2.utils.TagAttributeMapper;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasObjectIdType;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.Cardinality;
import org.janusgraph.graphdb.relations.CacheVertexProperty;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_CLASSIFICATIONS;
import static org.apache.atlas.AtlasConfiguration.MAX_EDGES_SUPER_VERTEX;
import static org.apache.atlas.AtlasConfiguration.MIN_EDGES_SUPER_VERTEX;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_CONFIDENCE;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_CREATED_BY;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_DESCRIPTION;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_EXPRESSION;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_SOURCE;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_STATUS;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_STEWARD;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.DELETED;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGDECIMAL;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BOOLEAN;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BYTE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DATE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_FLOAT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_INT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_LONG;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_SHORT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_STRING;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.NONE;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromVertex;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.BOTH;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.encodePropertyKey;
import static org.apache.atlas.type.Constants.PENDING_TASKS_PROPERTY_KEY;

@Component
public class EntityGraphRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphRetriever.class);

    private static final String GLOSSARY_TERM_DISPLAY_NAME_ATTR = "name";
    public  static final String TERM_RELATION_NAME              = "AtlasGlossarySemanticAssignment";

    public static final String NAME           = "name";
    public static final String DISPLAY_NAME   = "displayName";
    public static final String DESCRIPTION    = "description";
    public static final String OWNER          = "owner";
    public static final String CREATE_TIME    = "createTime";
    public static final String QUALIFIED_NAME = "qualifiedName";

    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("entity-retriever-bfs-%d")
            .setDaemon(true)
            .build();
    private final ExecutorService executorService = Executors.newFixedThreadPool(AtlasConfiguration.GRAPH_TRAVERSAL_PARALLELISM.getInt(), threadFactory);;

    private static final TypeReference<List<TimeBoundary>> TIME_BOUNDARIES_LIST_TYPE = new TypeReference<List<TimeBoundary>>() {};
    private final GraphHelper graphHelper;

    private final AtlasTypeRegistry typeRegistry;

    private final boolean ignoreRelationshipAttr;
    private final boolean fetchOnlyMandatoryRelationshipAttr;
    private final AtlasGraph graph;
    private TagDAO tagDAO;

    @Inject
    public EntityGraphRetriever(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this(graph, typeRegistry, false);
    }

    public EntityGraphRetriever(EntityGraphRetriever retriever, boolean ignoreRelationshipAttr) {
        this.tagDAO                 = retriever.tagDAO;
        this.graph                  = retriever.graph;
        this.graphHelper            = retriever.graphHelper;
        this.typeRegistry           = retriever.typeRegistry;
        this.ignoreRelationshipAttr = ignoreRelationshipAttr;
        this.fetchOnlyMandatoryRelationshipAttr = false;
    }

    public EntityGraphRetriever(AtlasGraph graph, AtlasTypeRegistry typeRegistry, boolean ignoreRelationshipAttr) {
        this.graph                  = graph;
        this.graphHelper            = new GraphHelper(graph);
        this.typeRegistry           = typeRegistry;
        this.ignoreRelationshipAttr = ignoreRelationshipAttr;
        this.tagDAO                 = TagDAOCassandraImpl.getInstance();
        this.fetchOnlyMandatoryRelationshipAttr = false;
    }

    public EntityGraphRetriever(AtlasGraph graph, AtlasTypeRegistry typeRegistry, boolean ignoreRelationshipAttr, boolean fetchOnlyMandatoryRelationshipAttr) {
        this.graph                  = graph;
        this.graphHelper            = new GraphHelper(graph);
        this.typeRegistry           = typeRegistry;
        this.ignoreRelationshipAttr = ignoreRelationshipAttr;
        this.tagDAO                 = TagDAOCassandraImpl.getInstance();
        this.fetchOnlyMandatoryRelationshipAttr = fetchOnlyMandatoryRelationshipAttr;
    }

    public AtlasEntity toAtlasEntity(String guid, boolean includeReferences) throws AtlasBaseException {
        return mapVertexToAtlasEntity(getEntityVertex(guid), null, false, includeReferences);
    }

    public AtlasEntity toAtlasEntity(String guid) throws AtlasBaseException {
        return toAtlasEntity(getEntityVertex(guid));
    }

    public AtlasEntity toAtlasEntity(AtlasObjectId objId) throws AtlasBaseException {
        return toAtlasEntity(getEntityVertex(objId));
    }

    public AtlasEntity toAtlasEntity(AtlasVertex entityVertex) throws AtlasBaseException {
        return mapVertexToAtlasEntity(entityVertex, null);
    }

    public AtlasEntityWithExtInfo toAtlasEntityWithExtInfo(String guid) throws AtlasBaseException {
        return toAtlasEntityWithExtInfo(getEntityVertex(guid));
    }

    public AtlasEntityWithExtInfo toAtlasEntityWithExtInfo(String guid, boolean isMinExtInfo) throws AtlasBaseException {
        return toAtlasEntityWithExtInfo(getEntityVertex(guid), isMinExtInfo);
    }

    public AtlasEntityWithExtInfo toAtlasEntityWithExtInfo(AtlasObjectId objId) throws AtlasBaseException {
        return toAtlasEntityWithExtInfo(getEntityVertex(objId));
    }

    public AtlasEntityWithExtInfo toAtlasEntityWithExtInfo(AtlasVertex entityVertex) throws AtlasBaseException {
        return toAtlasEntityWithExtInfo(entityVertex, false);
    }

    public AtlasEntityWithExtInfo toAtlasEntityWithExtInfo(AtlasVertex entityVertex, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasEntityExtInfo     entityExtInfo = new AtlasEntityExtInfo();
        AtlasEntity            entity        = mapVertexToAtlasEntity(entityVertex, entityExtInfo, isMinExtInfo);
        AtlasEntityWithExtInfo ret           = new AtlasEntityWithExtInfo(entity, entityExtInfo);

        ret.compact();

        return ret;
    }

    public AtlasEntitiesWithExtInfo toAtlasEntitiesWithExtInfo(List<String> guids) throws AtlasBaseException {
        return toAtlasEntitiesWithExtInfo(guids, false);
    }

    public AtlasEntityHeader toAtlasEntityHeader(String guid) throws AtlasBaseException {
        return toAtlasEntityHeader(getEntityVertex(guid));
    }

    public AtlasEntityHeader toAtlasEntityHeader(String guid, Set<String> attributes) throws AtlasBaseException {
        return toAtlasEntityHeader(getEntityVertex(guid), attributes);
    }

    public AtlasEntityHeader toAtlasEntityHeader(AtlasVertex entityVertex) throws AtlasBaseException {
        return toAtlasEntityHeader(entityVertex, Collections.<String>emptySet());
    }

    public AtlasEntityHeader toAtlasEntityHeader(AtlasVertex atlasVertex, Set<String> attributes) throws AtlasBaseException {
        return atlasVertex != null ? mapVertexToAtlasEntityHeader(atlasVertex, attributes) : null;
    }

    public AtlasEntityHeader dynamicVertexToAtlasEntityHeader(AtlasVertex atlasVertex, Set<String> attributes) throws AtlasBaseException {
        return atlasVertex != null ? mapDynamicVertexToAtlasEntityHeader(atlasVertex, attributes) : null;
    }

    public AtlasEntityHeader toAtlasEntityHeader(AtlasVertex atlasVertex, Set<String> attributes, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        return atlasVertex != null ? mapVertexToAtlasEntityHeader(atlasVertex, attributes, vertexEdgePropertiesCache) : null;
    }

    public AtlasEntityHeader toAtlasEntityHeaderWithClassifications(String guid) throws AtlasBaseException {
        return toAtlasEntityHeaderWithClassifications(getEntityVertex(guid), Collections.emptySet());
    }

    public AtlasEntityHeader toAtlasEntityHeaderWithClassifications(AtlasVertex entityVertex) throws AtlasBaseException {
        return toAtlasEntityHeaderWithClassifications(entityVertex, Collections.emptySet());
    }

    public AtlasEntityHeader toAtlasEntityHeaderWithClassifications(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasEntityHeader ret = toAtlasEntityHeader(entityVertex, attributes);

        if (!RequestContext.get().isSkipAuthorizationCheck()) {
            // Avoid fetching tags if skip Auth check flag is enabled,
            // to avoid NPE while bootstrapping auth policies for the very frst time
            ret.setClassifications(handleGetAllClassifications(entityVertex));
        } else {
            ret.setClassifications(Collections.EMPTY_LIST);
        }

        return ret;
    }

    public Map<String, Map<String, Object>> getBusinessMetadata(AtlasVertex entityVertex) throws AtlasBaseException {
        Map<String, Map<String, Object>>                         ret             = null;
        String                                                   entityTypeName  = getTypeName(entityVertex);
        AtlasEntityType                                          entityType      = typeRegistry.getEntityTypeByName(entityTypeName);
        Map<String, Map<String, AtlasBusinessAttribute>> entityTypeBm    = entityType != null ? entityType.getBusinessAttributes() : null;

        if (MapUtils.isNotEmpty(entityTypeBm)) {
            for (Map.Entry<String, Map<String, AtlasBusinessAttribute>> entry : entityTypeBm.entrySet()) {
                String                                      bmName        = entry.getKey();
                Map<String, AtlasBusinessAttribute> bmAttributes  = entry.getValue();
                Map<String, Object>                         entityBmAttrs = null;

                for (AtlasBusinessAttribute bmAttribute : bmAttributes.values()) {
                    Object bmAttrValue = mapVertexToAttribute(entityVertex, bmAttribute, null, false, false);

                    if (bmAttrValue != null) {
                        if (ret == null) {
                            ret = new HashMap<>();
                        }

                        if (entityBmAttrs == null) {
                            entityBmAttrs = new HashMap<>();

                            ret.put(bmName, entityBmAttrs);
                        }

                        entityBmAttrs.put(bmAttribute.getName(), bmAttrValue);
                    }
                }
            }
        }

        return ret;
    }

    public Object getEntityAttribute(AtlasVertex entityVertex, AtlasAttribute attribute) {
        Object ret = null;

        try {
            ret = getVertexAttributeIgnoreInactive(entityVertex, attribute);
        } catch (AtlasBaseException excp) {
            // ignore
        }

        return ret;
    }

    public AtlasObjectId toAtlasObjectId(AtlasVertex entityVertex) throws AtlasBaseException {
        AtlasObjectId   ret        = null;
        String          typeName   = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        boolean enableJanusOptimisation =
                AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_RELATIONS.getBoolean()
                         && RequestContext.get().isInvokedByIndexSearch();
        Map<String, Object> referenceVertexProperties  = null;
        if (entityType != null) {
            Map<String, Object> uniqueAttributes = new HashMap<>();
            Set<String> relationAttributes = RequestContext.get().getRelationAttrsForSearch();
            if (enableJanusOptimisation) {
                //don't fetch edge labels for a relation attribute
                referenceVertexProperties  = preloadProperties(entityVertex, entityType, relationAttributes, false);
            }
            for (AtlasAttribute attribute : entityType.getUniqAttributes().values()) {
                Object attrValue = getVertexAttribute(entityVertex, attribute);

                if (attrValue != null) {
                    uniqueAttributes.put(attribute.getName(), attrValue);
                }
            }

            Map<String, Object> attributes = new HashMap<>();
            if (CollectionUtils.isNotEmpty(relationAttributes)) {
                for (String attributeName : relationAttributes) {
                    AtlasAttribute attribute = entityType.getAttribute(attributeName);
                    if (attribute != null
                            && !uniqueAttributes.containsKey(attributeName)) {
                        Object attrValue = null;
                        if (enableJanusOptimisation) {
                            attrValue = getVertexAttributePreFetchCache(entityVertex, attribute, referenceVertexProperties);
                        } else {
                            attrValue = getVertexAttribute(entityVertex, attribute);
                        }

                        if (attrValue != null) {
                            attributes.put(attribute.getName(), attrValue);
                        }
                    }
                }
            }
            ret = new AtlasObjectId(entityVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class), typeName, uniqueAttributes, attributes);
        }

        return ret;
    }

    public AtlasObjectId toAtlasObjectIdV2(String referenceVertexId, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        AtlasObjectId   ret        = null;
        AtlasVertex referenceVertex = vertexEdgePropertiesCache.getVertexById(referenceVertexId);
        if (referenceVertex == null) {
            return null;
        }
//        String          typeName   = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
        String typeName = vertexEdgePropertiesCache.getPropertyValue(referenceVertexId, TYPE_NAME_PROPERTY_KEY, String.class);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        Map<String, Object> referenceVertexProperties  = null;
        if (entityType != null) {
            Map<String, Object> uniqueAttributes = new HashMap<>();
            Set<String> relationAttributes = RequestContext.get().getRelationAttrsForSearch();

            for (AtlasAttribute attribute : entityType.getUniqAttributes().values()) {
                Object attrValue = getVertexAttribute(referenceVertex, attribute, vertexEdgePropertiesCache);

                if (attrValue != null) {
                    uniqueAttributes.put(attribute.getName(), attrValue);
                }
            }

            Map<String, Object> attributes = new HashMap<>();
            if (CollectionUtils.isNotEmpty(relationAttributes)) {
                for (String attributeName : relationAttributes) {
                    AtlasAttribute attribute = entityType.getAttribute(attributeName);
                    if (attribute != null
                            && !uniqueAttributes.containsKey(attributeName)) {
                        Object attrValue = null;
                        attrValue = getVertexAttribute(referenceVertex, attribute, vertexEdgePropertiesCache);

                        if (attrValue != null) {
                            attributes.put(attribute.getName(), attrValue);
                        }
                    }
                }
            }
            ret = new AtlasObjectId(referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class), typeName, uniqueAttributes, attributes);
        }

        return ret;
    }

    public AtlasObjectId toAtlasObjectId(AtlasEntity entity) {
        AtlasObjectId   ret        = null;
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        if (entityType != null) {
            Map<String, Object> uniqueAttributes = new HashMap<>();

            for (String attributeName : entityType.getUniqAttributes().keySet()) {
                Object attrValue = entity.getAttribute(attributeName);

                if (attrValue != null) {
                    uniqueAttributes.put(attributeName, attrValue);
                }
            }

            Map<String, Object> attributes = new HashMap<>();
            Set<String> relationAttributes = RequestContext.get().getRelationAttrsForSearch();
            if (CollectionUtils.isNotEmpty(relationAttributes)) {
                for (String attributeName : relationAttributes) {
                    AtlasAttribute attribute = entityType.getAttribute(attributeName);
                    if (attribute != null
                            && !uniqueAttributes.containsKey(attributeName)) {
                        Object attrValue = entity.getAttribute(attributeName);
                        if (attrValue != null) {
                            attributes.put(attribute.getName(), attrValue);
                        }
                    }
                }
            }

            ret = new AtlasObjectId(entity.getGuid(), entity.getTypeName(), uniqueAttributes, attributes);
        }

        return ret;
    }

    public AtlasObjectId toAtlasObjectIdWithoutGuid(AtlasEntity entity) {
        AtlasObjectId objectId = toAtlasObjectId(entity);
        objectId.setGuid(null);

        return objectId;
    }

    public AtlasClassification toAtlasClassification(AtlasVertex classificationVertex) throws AtlasBaseException {
        AtlasClassification ret                = null;
        String              classificationName = getTypeName(classificationVertex);

        if (StringUtils.isEmpty(classificationName)) {
            LOG.warn("Ignoring invalid classification vertex: {}", AtlasGraphUtilsV2.toString(classificationVertex));
        } else {
            ret = new AtlasClassification(classificationName);
            Map<String, Object> referenceProperties = Collections.emptyMap();
            boolean enableJanusOptimisation = AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_EXTENDED.getBoolean()
                    && RequestContext.get().isInvokedByIndexSearch();
            String strValidityPeriods;

            if (enableJanusOptimisation) {
                referenceProperties = preloadProperties(classificationVertex, typeRegistry.getClassificationTypeByName(classificationName), Collections.emptySet(), false);
                ret.setEntityGuid((String) referenceProperties.get(Constants.CLASSIFICATION_ENTITY_GUID));
                ret.setEntityStatus(referenceProperties.get(Constants.CLASSIFICATION_ENTITY_STATUS) != null ?
                        AtlasEntity.Status.valueOf((String) referenceProperties.get(Constants.CLASSIFICATION_ENTITY_STATUS)) : null);
                ret.setPropagate(referenceProperties.get(CLASSIFICATION_VERTEX_PROPAGATE_KEY) != null ? (Boolean) referenceProperties.get(CLASSIFICATION_VERTEX_PROPAGATE_KEY) : true);
                ret.setRemovePropagationsOnEntityDelete(referenceProperties.get(CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY) != null ? (Boolean) referenceProperties.get(CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY) : true);
                ret.setRestrictPropagationThroughLineage(referenceProperties.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE) != null ? (Boolean) referenceProperties.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE) : false);
                ret.setRestrictPropagationThroughHierarchy(referenceProperties.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY) != null ? (Boolean) referenceProperties.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY) : false);
                strValidityPeriods = referenceProperties.get(CLASSIFICATION_VALIDITY_PERIODS_KEY)!=null ? (String) referenceProperties.get(CLASSIFICATION_VALIDITY_PERIODS_KEY) : null;
            } else {
                ret.setEntityGuid(AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_GUID, String.class));
                ret.setEntityStatus(getClassificationEntityStatus(classificationVertex));
                ret.setPropagate(isPropagationEnabled(classificationVertex));
                ret.setRemovePropagationsOnEntityDelete(getRemovePropagations(classificationVertex));
                ret.setRestrictPropagationThroughLineage(getRestrictPropagationThroughLineage(classificationVertex));
                ret.setRestrictPropagationThroughHierarchy(getRestrictPropagationThroughHierarchy(classificationVertex));
                strValidityPeriods = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_VALIDITY_PERIODS_KEY, String.class);
            }

            ret.setValidityPeriods(AtlasJson.fromJson(strValidityPeriods, TIME_BOUNDARIES_LIST_TYPE));
            mapAttributes(classificationVertex, ret, null);
        }

        return ret;
    }

    public AtlasClassification toAtlasClassification(Tag tag) throws AtlasBaseException {
        AtlasClassification classification = TagDAOCassandraImpl.toAtlasClassification(tag.getTagMetaJson());
        return classification;
    }

    public AtlasVertex getReferencedEntityVertex(AtlasEdge edge, AtlasRelationshipEdgeDirection relationshipDirection, AtlasVertex parentVertex) throws AtlasBaseException {
        AtlasVertex entityVertex = null;

        if (relationshipDirection == OUT) {
            entityVertex = edge.getInVertex();
        } else if (relationshipDirection == IN) {
            entityVertex = edge.getOutVertex();
        } else if (relationshipDirection == BOTH){
            // since relationship direction is BOTH, edge direction can be inward or outward
            // compare with parent entity vertex and pick the right reference vertex
            if (StringUtils.equals(GraphHelper.getGuid(parentVertex), GraphHelper.getGuid(edge.getOutVertex()))) {
                entityVertex = edge.getInVertex();
            } else {
                entityVertex = edge.getOutVertex();
            }
        }

        return entityVertex;
    }

    public AtlasVertex getEntityVertex(String guid) throws AtlasBaseException {
        AtlasVertex ret = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        if (StringUtils.isEmpty(GraphHelper.getTypeName(ret))) {
            throw new AtlasBaseException(AtlasErrorCode.NO_TYPE_NAME_ON_VERTEX, guid);
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo toAtlasEntitiesWithExtInfo(List<String> guids, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasEntitiesWithExtInfo ret = new AtlasEntitiesWithExtInfo();

        for (String guid : guids) {
            AtlasVertex vertex = getEntityVertex(guid);

            AtlasEntity entity = mapVertexToAtlasEntity(vertex, ret, isMinExtInfo);

            ret.addEntity(entity);
        }

        ret.compact();

        return ret;
    }

    public Map<String, Object> getEntityUniqueAttribute(AtlasVertex entityVertex) throws AtlasBaseException {
        Map<String, Object> ret        = null;
        String              typeName   = AtlasGraphUtilsV2.getTypeName(entityVertex);
        AtlasEntityType     entityType = typeRegistry.getEntityTypeByName(typeName);

        if (entityType != null && MapUtils.isNotEmpty(entityType.getUniqAttributes())) {
            for (AtlasAttribute attribute : entityType.getUniqAttributes().values()) {
                Object val = mapVertexToAttribute(entityVertex, attribute, null, false);

                if (val != null) {
                    if (ret == null) {
                        ret = new HashMap<>();
                    }

                    ret.put(attribute.getName(), val);
                }
            }
        }

        return ret;
    }

    public AtlasEntitiesWithExtInfo getEntitiesByUniqueAttributes(String typeName, List<Map<String, Object>> uniqueAttributesList, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasEntitiesWithExtInfo ret        = new AtlasEntitiesWithExtInfo();
        AtlasEntityType          entityType = typeRegistry.getEntityTypeByName(typeName);

        if (entityType != null) {
            for (Map<String, Object> uniqAttributes : uniqueAttributesList) {
                try {
                    AtlasVertex vertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(this.graph, entityType, uniqAttributes);

                    if (vertex != null) {
                        AtlasEntity entity = mapVertexToAtlasEntity(vertex, ret, isMinExtInfo);

                        ret.addEntity(entity);
                    }
                } catch(AtlasBaseException e) {
                    if (e.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                        throw e;
                    }
                }
            }
        }

        ret.compact();

        return ret;
    }

    public void evaluateClassificationPropagation(AtlasVertex classificationVertex, List<AtlasVertex> entitiesToAddPropagation, List<AtlasVertex> entitiesToRemovePropagation) {
        if (classificationVertex != null) {
            String            entityGuid         = getClassificationEntityGuid(classificationVertex);
            AtlasVertex       entityVertex       = AtlasGraphUtilsV2.findByGuid(this.graph, entityGuid);
            String            classificationId   = classificationVertex.getIdForDisplay();
            List<AtlasVertex> propagatedEntities = getAllPropagatedEntityVertices(classificationVertex);
            List<AtlasVertex> impactedEntities   = getImpactedVerticesV2(entityVertex, null, classificationId);

            List<AtlasVertex> entityVertices = (List<AtlasVertex>) CollectionUtils.subtract(propagatedEntities, impactedEntities);

            if (CollectionUtils.isNotEmpty(entityVertices)) {
                entitiesToRemovePropagation.addAll(entityVertices);
            }

            entityVertices = (List<AtlasVertex>) CollectionUtils.subtract(impactedEntities, propagatedEntities);

            if (CollectionUtils.isNotEmpty(entityVertices)) {
                entitiesToAddPropagation.addAll(entityVertices);
            }
        }
    }

    public Map<AtlasVertex, List<AtlasVertex>> getClassificationPropagatedEntitiesMapping(List<AtlasVertex> classificationVertices) throws AtlasBaseException{
        return getClassificationPropagatedEntitiesMapping(classificationVertices, null);
    }

    public Map<AtlasVertex, List<AtlasVertex>> getClassificationPropagatedEntitiesMapping(List<AtlasVertex> classificationVertices, String relationshipGuidToExclude) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getClassificationPropagatedEntitiesMapping");
        Map<AtlasVertex, List<AtlasVertex>> ret = new HashMap<>();

        if (CollectionUtils.isNotEmpty(classificationVertices)) {
            for (AtlasVertex classificationVertex : classificationVertices) {
                String            classificationId      = classificationVertex.getIdForDisplay();
                String            sourceEntityId        = getClassificationEntityGuid(classificationVertex);
                AtlasVertex       sourceEntityVertex    = AtlasGraphUtilsV2.findByGuid(this.graph, sourceEntityId);
                String propagationMode;

                Boolean restrictPropagationThroughLineage = AtlasGraphUtilsV2.getProperty(classificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE, Boolean.class);
                Boolean restrictPropagationThroughHierarchy = AtlasGraphUtilsV2.getProperty(classificationVertex, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, Boolean.class);
                propagationMode = determinePropagationMode(restrictPropagationThroughLineage,restrictPropagationThroughHierarchy);
                Boolean toExclude = propagationMode == CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE ? true : false;
                List<AtlasVertex> entitiesPropagatingTo = getImpactedVerticesV2(sourceEntityVertex, relationshipGuidToExclude,
                        classificationId, CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP.get(propagationMode),toExclude);

                LOG.info("Traversed {} vertices for Classification vertex id {} excluding RelationShip GUID {}", entitiesPropagatingTo.size(), classificationId, relationshipGuidToExclude);

                ret.put(classificationVertex, entitiesPropagatingTo);
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }
    /**
     * Checks for if the AtlasClassification has valid config of restrict flags
     * Both Restrict flags can't be true with propagate flag allowed
     */
    public void verifyClassificationsPropagationMode(List<AtlasClassification> incomingClassifications) throws AtlasBaseException {
        for(AtlasClassification incomingClassification : incomingClassifications){
            if(Boolean.TRUE.equals(incomingClassification.isPropagate()))
                determinePropagationMode(incomingClassification.getRestrictPropagationThroughLineage(),incomingClassification.getRestrictPropagationThroughHierarchy());
        }
    }

    public String determinePropagationMode(Boolean currentRestrictPropagationThroughLineage, Boolean currentRestrictPropagationThroughHierarchy) throws AtlasBaseException {
        String propagationMode;

        validatePropagationRestrictionOptions(currentRestrictPropagationThroughLineage, currentRestrictPropagationThroughHierarchy);

        if (Boolean.TRUE.equals(currentRestrictPropagationThroughLineage)) {
            propagationMode = CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE;
        } else if (Boolean.TRUE.equals(currentRestrictPropagationThroughHierarchy)) {
            propagationMode = CLASSIFICATION_PROPAGATION_MODE_RESTRICT_HIERARCHY;
        } else {
            propagationMode = CLASSIFICATION_PROPAGATION_MODE_DEFAULT;
        }

        return propagationMode;
    }

    public void validatePropagationRestrictionOptions(Boolean currentRestrictPropagationThroughLineage, Boolean currentRestrictPropagationThroughHierarchy) throws AtlasBaseException {
        if (Boolean.TRUE.equals(currentRestrictPropagationThroughLineage) && Boolean.TRUE.equals(currentRestrictPropagationThroughHierarchy))
            throw new AtlasBaseException("Both restrictPropagationThroughLineage and restrictPropagationThroughHierarchy cannot be true simultaneously.");
    }

    public List<AtlasVertex> getImpactedVerticesV2(AtlasVertex entityVertex) {
        return getImpactedVerticesV2(entityVertex, (List<String>) null,false);
    }

    public List<AtlasVertex> getImpactedVerticesV2(AtlasVertex entityVertex,  List<String> edgeLabelsToCheck,Boolean toExclude){
        List<AtlasVertex> ret = new ArrayList<>();
        traverseImpactedVertices(entityVertex, null, null, ret, edgeLabelsToCheck,toExclude);

        return ret;
    }

    public List<AtlasVertex> getImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude) {
        List<AtlasVertex> ret = new ArrayList<>();

        traverseImpactedVertices(entityVertex, relationshipGuidToExclude, null, ret, null,false);

        return ret;
    }

    public List<AtlasVertex> getIncludedImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude) {
        return getIncludedImpactedVerticesV2(entityVertex, relationshipGuidToExclude, null);
    }

    public List<AtlasVertex> getIncludedImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude, String classificationId) {
        List<AtlasVertex> ret = new ArrayList<>(Arrays.asList(entityVertex));

        traverseImpactedVertices(entityVertex, relationshipGuidToExclude, classificationId, ret, null,false);

        return ret;
    }
    public List<AtlasVertex> getIncludedImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude, List<String> edgeLabelsToCheck,Boolean toExclude) {
        List<String> vertexIds = new ArrayList<>();
        traverseImpactedVerticesByLevel(entityVertex, relationshipGuidToExclude, null, vertexIds, edgeLabelsToCheck,toExclude, null);

        List<AtlasVertex> ret = vertexIds.stream().map(x -> graph.getVertex(x))
                .filter(vertex -> vertex != null)
                .collect(Collectors.toList());
        ret.add(entityVertex);

        return ret;
    }

    public List<AtlasVertex> getImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude, String classificationId) {
        List<AtlasVertex> ret = new ArrayList<>();

        traverseImpactedVertices(entityVertex, relationshipGuidToExclude, classificationId, ret, null,false);

        return ret;
    }

    public List<AtlasVertex> getImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude, String classificationId, List<String> edgeLabelsToCheck,Boolean toExclude) {
        List<AtlasVertex> ret = new ArrayList<>();

        traverseImpactedVertices(entityVertex, relationshipGuidToExclude, classificationId, ret, edgeLabelsToCheck,toExclude);

        return ret;
    }


    public List<String> getImpactedVerticesIds(AtlasVertex entityVertex, String relationshipGuidToExclude, String classificationId, List<String> edgeLabelsToCheck,Boolean toExclude) {
        List<String> ret = new ArrayList<>();

        traverseImpactedVerticesByLevel(entityVertex, relationshipGuidToExclude, classificationId, ret, edgeLabelsToCheck,toExclude, null);

        return ret;
    }

    public List<String> getImpactedVerticesIdsClassificationAttached(AtlasVertex entityVertex, String classificationId, List<String> edgeLabelsToCheck,Boolean toExclude, List<String> verticesWithoutClassification) {
        List<String> ret = new ArrayList<>();

        GraphHelper.getClassificationEdges(entityVertex).forEach(classificationEdge -> {
            AtlasVertex classificationVertex = classificationEdge.getInVertex();
            if (classificationVertex != null && classificationId.equals(classificationVertex.getIdForDisplay())) {
                traverseImpactedVerticesByLevel(entityVertex, null, classificationId, ret, edgeLabelsToCheck, toExclude, verticesWithoutClassification);
            }
        });

        return ret;
    }



    private void traverseImpactedVertices(final AtlasVertex entityVertexStart, final String relationshipGuidToExclude,
                                          final String classificationId, final List<AtlasVertex> result, List<String> edgeLabelsToCheck,Boolean toExclude) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("traverseImpactedVertices");
        Set<String>              visitedVertices = new HashSet<>();
        Queue<AtlasVertex>       queue           = new ArrayDeque<>();
        Map<String, AtlasVertex> resultsMap      = new HashMap<>();
        RequestContext requestContext = RequestContext.get();

        if (entityVertexStart != null) {
            queue.add(entityVertexStart);
        }

        while (!queue.isEmpty()) {
            AtlasVertex entityVertex   = queue.poll();
            String      entityVertexId = entityVertex.getIdForDisplay();

            if (visitedVertices.contains(entityVertexId)) {
                LOG.info("Already visited: {}", entityVertexId);

                continue;
            }

            visitedVertices.add(entityVertexId);

            AtlasEntityType entityType          = typeRegistry.getEntityTypeByName(getTypeName(entityVertex));
            String[]        tagPropagationEdges = entityType != null ? entityType.getTagPropagationEdgesArray() : null;

            if (tagPropagationEdges == null) {
                continue;
            }
            // Main Crux of toExclude over here
            if (edgeLabelsToCheck != null && !edgeLabelsToCheck.isEmpty()) {
                if (toExclude) {
                    tagPropagationEdges = Arrays.stream(tagPropagationEdges)
                            .filter(x -> !edgeLabelsToCheck.contains(x))
                            .toArray(String[]::new);
                } else {
                    tagPropagationEdges = Arrays.stream(tagPropagationEdges)
                            .filter(edgeLabelsToCheck::contains)
                            .toArray(String[]::new);
                }
            }


            Iterator<AtlasEdge> propagationEdges = entityVertex.getEdges(AtlasEdgeDirection.BOTH, tagPropagationEdges).iterator();

            while (propagationEdges.hasNext()) {
                AtlasEdge propagationEdge = propagationEdges.next();

                if (getEdgeStatus(propagationEdge) != ACTIVE && !(requestContext.getCurrentTask() != null && requestContext.getDeletedEdgesIds().contains(propagationEdge.getIdForDisplay())) ) {
                    continue;
                }

                PropagateTags tagPropagation = getPropagateTags(propagationEdge);

                if (tagPropagation == null || tagPropagation == NONE) {
                    continue;
                } else if (tagPropagation == TWO_TO_ONE) {
                    if (isOutVertex(entityVertex, propagationEdge)) {
                        continue;
                    }
                } else if (tagPropagation == ONE_TO_TWO) {
                    if (!isOutVertex(entityVertex, propagationEdge)) {
                        continue;
                    }
                }

                if (relationshipGuidToExclude != null) {
                    if (StringUtils.equals(getRelationshipGuid(propagationEdge), relationshipGuidToExclude)) {
                        continue;
                    }
                }

                if (classificationId != null) {
                    List<String> blockedClassificationIds = getBlockedClassificationIds(propagationEdge);

                    if (CollectionUtils.isNotEmpty(blockedClassificationIds) && blockedClassificationIds.contains(classificationId)) {
                        continue;
                    }
                }

                AtlasVertex adjacentVertex             = getOtherVertex(propagationEdge, entityVertex);
                String      adjacentVertexIdForDisplay = adjacentVertex.getIdForDisplay();

                if (!visitedVertices.contains(adjacentVertexIdForDisplay) && !resultsMap.containsKey(adjacentVertexIdForDisplay)) {
                    resultsMap.put(adjacentVertexIdForDisplay, adjacentVertex);

                    queue.add(adjacentVertex);
                }
            }
        }

        result.addAll(resultsMap.values());
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void traverseImpactedVerticesByLevel(final AtlasVertex entityVertexStart, final String relationshipGuidToExclude,
                                          final String classificationId, final List<String> result, List<String> edgeLabelsToCheck,Boolean toExclude, List<String> verticesWithoutClassification) {
        AtlasPerfMetrics.MetricRecorder metricRecorder                          = RequestContext.get().startMetricRecord("traverseImpactedVerticesByLevel");
        Set<String>                 visitedVerticesIds                          = new HashSet<>();
        Set<String>                 verticesAtCurrentLevel                      = new HashSet<>();
        Set<String>                 traversedVerticesIds                        = new HashSet<>();
        Set<String>                 verticesWithOutClassification               = new HashSet<>();
        RequestContext              requestContext                              = RequestContext.get();
        AtlasVertex                 classificationVertex                        = graph.getVertex(classificationId);
        boolean                     storeVerticesWithoutClassification          = verticesWithoutClassification == null ? false : true;

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Tasks-BFS-%d")
                .setDaemon(true)
                .build();

        ExecutorService executorService = Executors.newFixedThreadPool(AtlasConfiguration.GRAPH_TRAVERSAL_PARALLELISM.getInt(), threadFactory);

        //Add Source vertex to level 1
        if (entityVertexStart != null) {
            verticesAtCurrentLevel.add(entityVertexStart.getIdForDisplay());
        }
        /*
            Steps in each level:
                1. Add vertices to Visited Vertices set
                2. Then fetch adjacent vertices of that vertex using getAdjacentVerticesIds
                3. Use future to fetch it later as it is a Blocking call,so we want to execute it asynchronously
                4. Then fetch the result from futures
                5. It will return the adjacent vertices of the current level
            After processing current level:
                1. Add the adjacent vertices of current level to verticesToVisitNextLevel
                2. Clear the processed vertices of current level
                3. After that insert verticesToVisitNextLevel into current level
           Continue the steps until all vertices are processed by checking if verticesAtCurrentLevel is empty
         */
        try {
            while (!verticesAtCurrentLevel.isEmpty()) {
                Set<String> verticesToVisitNextLevel = new HashSet<>();
                List<CompletableFuture<Set<String>>> futures = verticesAtCurrentLevel.stream()
                        .map(t -> {
                            AtlasVertex entityVertex = graph.getVertex(t);
                            visitedVerticesIds.add(entityVertex.getIdForDisplay());
                            // If we want to store vertices without classification attached
                            // Check if vertices has classification attached or not using function isClassificationAttached

                            if(storeVerticesWithoutClassification && !GraphHelper.isClassificationAttached(entityVertex, classificationVertex)) {
                                verticesWithOutClassification.add(entityVertex.getIdForDisplay());
                            }

                            return CompletableFuture.supplyAsync(() -> getAdjacentVerticesIds(entityVertex, classificationId,
                                    relationshipGuidToExclude, edgeLabelsToCheck,toExclude, visitedVerticesIds), executorService);
                        }).collect(Collectors.toList());

                futures.stream().map(CompletableFuture::join).forEach(x -> {
                    verticesToVisitNextLevel.addAll(x);
                    traversedVerticesIds.addAll(x);
                });

                verticesAtCurrentLevel.clear();
                verticesAtCurrentLevel.addAll(verticesToVisitNextLevel);
            }
        } finally {
            executorService.shutdown();
        }
        result.addAll(traversedVerticesIds);

        if(storeVerticesWithoutClassification)
            verticesWithoutClassification.addAll(verticesWithOutClassification);

        requestContext.endMetricRecord(metricRecorder);
    }

    public void traverseImpactedVerticesByLevelV2(final AtlasVertex entityVertexStart, final String relationshipGuidToExclude,
                                                 final String classificationId, final Set<String> result, List<String> edgeLabelsToCheck,Boolean toExclude, Set<String> verticesWithClassification, Set<String> verticesToExcludeFromTraversal) {
        AtlasPerfMetrics.MetricRecorder metricRecorder                          = RequestContext.get().startMetricRecord("traverseImpactedVerticesByLevel");
        Set<String>                 visitedVerticesIds                          = new HashSet<>();
        Set<String>                 verticesAtCurrentLevel                      = new HashSet<>();
        Set<String>                 traversedVerticesIds                        = new HashSet<>();
        Set<String>                 verticesToPropagateTo                       = new HashSet<>();
        RequestContext              requestContext                              = RequestContext.get();
        boolean                     storeVerticesWithoutClassification          = verticesWithClassification == null ? false : true;

        //Add Source vertex to level 1
        if (entityVertexStart != null) {
            verticesAtCurrentLevel.add(entityVertexStart.getIdForDisplay());
        }

        if (CollectionUtils.isNotEmpty(verticesToExcludeFromTraversal)) {
            visitedVerticesIds.addAll(verticesToExcludeFromTraversal);
        }

        /*
            Steps in each level:
                1. Add vertices to Visited Vertices set
                2. Then fetch adjacent vertices of that vertex using getAdjacentVerticesIds
                3. Use future to fetch it later as it is a Blocking call,so we want to execute it asynchronously
                4. Then fetch the result from futures
                5. It will return the adjacent vertices of the current level
            After processing current level:
                1. Add the adjacent vertices of current level to verticesToVisitNextLevel
                2. Clear the processed vertices of current level
                3. After that insert verticesToVisitNextLevel into current level
           Continue the steps until all vertices are processed by checking if verticesAtCurrentLevel is empty
         */
        try {
            while (!verticesAtCurrentLevel.isEmpty()) {
                Set<String> verticesToVisitNextLevel = new HashSet<>();
                List<CompletableFuture<Set<String>>> futures = verticesAtCurrentLevel.stream()
                        .map(t -> {
                            AtlasVertex entityVertex = graph.getVertex(t);
                            visitedVerticesIds.add(entityVertex.getIdForDisplay());
                            // If we want to store vertices without classification attached
                            // Check if vertices has classification attached or not using function isClassificationAttached

                            if(storeVerticesWithoutClassification && !verticesWithClassification.contains(entityVertex.getIdForDisplay())) {
                                verticesToPropagateTo.add(entityVertex.getIdForDisplay());
                            }

                            return CompletableFuture.supplyAsync(() -> getAdjacentVerticesIds(entityVertex, classificationId,
                                    relationshipGuidToExclude, edgeLabelsToCheck,toExclude, visitedVerticesIds), executorService);
                        }).collect(Collectors.toList());

                futures.stream().map(CompletableFuture::join).forEach(x -> {
                    verticesToVisitNextLevel.addAll(x);
                    traversedVerticesIds.addAll(x);
                });

                verticesAtCurrentLevel.clear();
                verticesAtCurrentLevel.addAll(verticesToVisitNextLevel);
            }

            result.addAll(traversedVerticesIds);

            if(storeVerticesWithoutClassification) {
                verticesWithClassification.clear();
                verticesWithClassification.addAll(verticesToPropagateTo);
            }
        } finally {
            requestContext.endMetricRecord(metricRecorder);
        }
    }

    private Set<String> getAdjacentVerticesIds(AtlasVertex entityVertex,final String classificationId, final String relationshipGuidToExclude
            ,List<String> edgeLabelsToCheck,Boolean toExclude, Set<String> visitedVerticesIds) {

        AtlasEntityType         entityType          = typeRegistry.getEntityTypeByName(getTypeName(entityVertex));
        String[]                tagPropagationEdges = entityType != null ? entityType.getTagPropagationEdgesArray() : null;
        Set<String>             ret                 = new HashSet<>();
        RequestContext          requestContext      = RequestContext.get();

        if (tagPropagationEdges == null) {
            return null;
        }

        if (edgeLabelsToCheck != null && !edgeLabelsToCheck.isEmpty()) {
            if (toExclude) {
                tagPropagationEdges = Arrays.stream(tagPropagationEdges)
                        .filter(x -> !edgeLabelsToCheck.contains(x))
                        .collect(Collectors.toList())
                        .toArray(new String[0]);
            } else{
                tagPropagationEdges = Arrays.stream(tagPropagationEdges)
                        .filter(x -> edgeLabelsToCheck.contains(x))
                        .collect(Collectors.toList())
                        .toArray(new String[0]);
            }
        }

        Iterator<AtlasEdge> propagationEdges = entityVertex.getEdges(AtlasEdgeDirection.BOTH, tagPropagationEdges).iterator();

        while (propagationEdges.hasNext()) {
            AtlasEdge propagationEdge = propagationEdges.next();

            if (getEdgeStatus(propagationEdge) != ACTIVE && !(requestContext.getCurrentTask() != null && requestContext.getDeletedEdgesIds().contains(propagationEdge.getIdForDisplay())) ) {
                continue;
            }

            PropagateTags tagPropagation = getPropagateTags(propagationEdge);

            if (tagPropagation == null || tagPropagation == NONE) {
                continue;
            } else if (tagPropagation == TWO_TO_ONE) {
                if (isOutVertex(entityVertex, propagationEdge)) {
                    continue;
                }
            } else if (tagPropagation == ONE_TO_TWO) {
                if (!isOutVertex(entityVertex, propagationEdge)) {
                    continue;
                }
            }

            if (relationshipGuidToExclude != null) {
                if (StringUtils.equals(getRelationshipGuid(propagationEdge), relationshipGuidToExclude)) {
                    continue;
                }
            }

            if (classificationId != null) {
                List<String> blockedClassificationIds = getBlockedClassificationIds(propagationEdge);

                if (CollectionUtils.isNotEmpty(blockedClassificationIds) && blockedClassificationIds.contains(classificationId)) {
                    continue;
                }
            }

            AtlasVertex adjacentVertex             = getOtherVertex(propagationEdge, entityVertex);
            String      adjacentVertexIdForDisplay = adjacentVertex.getIdForDisplay();

            if (!visitedVerticesIds.contains(adjacentVertexIdForDisplay)) {
                ret.add(adjacentVertexIdForDisplay);
            }
        }

        return ret;
    }

    private boolean isOutVertex(AtlasVertex vertex, AtlasEdge edge) {
        return StringUtils.equals(vertex.getIdForDisplay(), edge.getOutVertex().getIdForDisplay());
    }

    private AtlasVertex getOtherVertex(AtlasEdge edge, AtlasVertex vertex) {
        AtlasVertex outVertex = edge.getOutVertex();
        AtlasVertex inVertex  = edge.getInVertex();

        return StringUtils.equals(outVertex.getIdForDisplay(), vertex.getIdForDisplay()) ? inVertex : outVertex;
    }

    public AtlasVertex getEntityVertex(AtlasObjectId objId) throws AtlasBaseException {
        AtlasVertex ret = null;

        if (! AtlasTypeUtil.isValid(objId)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, objId.toString());
        }

        if (AtlasTypeUtil.isAssignedGuid(objId)) {
            ret = AtlasGraphUtilsV2.findByGuid(this.graph, objId.getGuid());
        } else {
            AtlasEntityType     entityType     = typeRegistry.getEntityTypeByName(objId.getTypeName());
            Map<String, Object> uniqAttributes = objId.getUniqueAttributes();

            ret = AtlasGraphUtilsV2.getVertexByUniqueAttributes(this.graph, entityType, uniqAttributes);
        }

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, objId.toString());
        }

        return ret;
    }

    private static Map<String, List<?>> getStringArrayListMap(Map<Object, Object> properties) {
        Map<String, List<?>> vertexProperties = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String attributeName = entry.getKey().toString();
            Object attributeValue = entry.getValue();
            if (attributeValue instanceof List) {
                // Convert List to ArrayList
                ArrayList<?> arrayList = new ArrayList<>((List<?>) attributeValue);
                vertexProperties.put(attributeName, arrayList);
            }
        }
        return vertexProperties;
    }

    private Set<String> collectEdgeLabelsToProcess(VertexEdgePropertiesCache cache,
                                                   Set<String> vertexIds,
                                                   Set<String> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> typeNames = vertexIds.stream()
                .map(cache::getTypeName)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toSet());

        // Collect edge labels from relationship attributes
        Set<String> edgeLabels = new HashSet<>();

        for (String typeName : typeNames) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
            if (entityType == null) {
                continue;
            }

            for (String attribute : attributes) {
                processRelationshipAttribute(entityType, attribute, edgeLabels);
            }
        }

        return edgeLabels;
    }

    private void processRelationshipAttribute(AtlasEntityType entityType,
                                              String attribute,
                                              Set<String> edgeLabels) {

        RequestContext context = RequestContext.get();
        if (!entityType.getRelationshipAttributes().containsKey(attribute)) {
            return;
        }

        AtlasAttribute atlasAttribute = entityType.getRelationshipAttribute(attribute, null);
        if (atlasAttribute != null && atlasAttribute.getAttributeType() != null) {
            if (context.isInvokedByIndexSearch() && context.isInvokedByProduct() &&
                    CollectionUtils.isEmpty(context.getRelationAttrsForSearch())) {
                return;
            }
            edgeLabels.add(atlasAttribute.getRelationshipEdgeLabel());
        } else {
            LOG.debug("Ignoring non-relationship type attribute: {}", attribute);
        }
    }

    /*
    Returns a pair containing:
    1. A map of vertex IDs to their properties, where each property is a map of attribute names to lists of values.
    2. A map of vertex IDs to their corresponding AtlasVertex objects.
     */
    @SuppressWarnings("unchecked,rawtypes")
    private Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> getVertexPropertiesValueMap(Set<String> vertexIds, int batchSize) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getVertexPropertiesValueMap");
        try {
            if (CollectionUtils.isEmpty(vertexIds)) {
                return Pair.with(Collections.emptyMap(), Collections.emptyMap());
            }

            Map<String, Map<String, List<?>>> vertexPropertyMap = new HashMap<>();
            Map<String, AtlasVertex> vertexMap = new HashMap<>();

            ListUtils.partition(new ArrayList<>(vertexIds), batchSize).forEach(batch -> {
                GraphTraversal vertices  = graph.V(batch)
                        .project("vertex", "properties")
                        // Return the vertex only
                        .by()
                        .by(__.valueMap(true)); // Return the valueMap

                List<Map<String, Object>> results = vertices.toList();



                results.forEach(vertexInfo -> {
                    AtlasVertex vertex = new AtlasJanusVertex((AtlasJanusGraph) graph, (Vertex) vertexInfo.get("vertex"));
                    vertexMap.put(vertex.getIdForDisplay(), vertex);
                    Map<Object, Object> properties = (Map<Object, Object>) vertexInfo.get("properties");
                    if (properties != null) {
                        if (MapUtils.isNotEmpty(properties) && properties.containsKey(T.id)) {
                            String vertexId;
                            if (LEAN_GRAPH_ENABLED) {
                                vertexId = (String) properties.get(T.id);
                            } else {
                                Long id = (Long) properties.get(T.id);
                                vertexId = id.toString();
                            }
                            properties.remove(T.id);
                            properties.remove(T.label);
                            Map<String, List<?>> vertexProperties = getStringArrayListMap(properties);
                            vertexPropertyMap.put(vertexId, vertexProperties);
                        }
                    }
                });
            });

            return Pair.with(vertexPropertyMap, vertexMap);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public List<Map<String, Object>> getConnectedRelationEdges(Set<String> vertexIds, Set<String> edgeLabels, int relationAttrsSize) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getConnectedRelationEdges");
        try {
            if (CollectionUtils.isEmpty(vertexIds)) {
                return Collections.emptyList();
            }

            GraphTraversal<Edge, Map<String, Object>> edgeTraversal =
                    ((AtlasJanusGraph) graph).V(vertexIds)
                            .bothE();
            
            // Filter by edge labels if provided
            if (!CollectionUtils.isEmpty(edgeLabels)) {
                edgeTraversal = edgeTraversal.hasLabel(P.within(edgeLabels));
            }
            
            edgeTraversal = edgeTraversal
                            .has(STATE_PROPERTY_KEY, ACTIVE.name())
                            .has(RELATIONSHIP_GUID_PROPERTY_KEY)
                            .project( "id", "valueMap","label", "inVertexId", "outVertexId")
                            .by(__.id()) // Returns the edge id
                            .by(__.valueMap(true)) // Returns the valueMap
                            .by(__.label()) // Returns the edge label
                            .by(__.inV().id())  // Returns the inVertexId
                            .by(__.outV().id()); // Returns the outVertexId

            return edgeTraversal.toList();
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public List<Map<String, Object>> getConnectedRelationEdgesVertexBatching(
            Set<String> vertexIds, Set<String> edgeLabels, int relationAttrsSize) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getConnectedRelationEdgesVertexBatching");
        try {
            if (CollectionUtils.isEmpty(vertexIds)) {
                return Collections.emptyList();
            }

            List<Map<String, Object>> allResults = new ArrayList<>();
            List<String> vertexIdList = new ArrayList<>(vertexIds);
            int vertexBatchSize = AtlasConfiguration.ATLAS_INDEXSEARCH_EDGE_BULK_FETCH_BATCH_SIZE.getInt(); // Process 100 vertices at a time

            for (int i = 0; i < vertexIdList.size(); i += vertexBatchSize) {
                int end = Math.min(i + vertexBatchSize, vertexIdList.size());
                List<String> vertexBatch = vertexIdList.subList(i, end);

                GraphTraversal<Edge, Map<String, Object>> edgeTraversal =
                        ((AtlasJanusGraph) graph).V(vertexBatch)
                                .bothE();

                if (!CollectionUtils.isEmpty(edgeLabels)) {
                    edgeTraversal = edgeTraversal.hasLabel(P.within(edgeLabels));
                }

                List<Map<String, Object>> batchResults = edgeTraversal
                        .has(STATE_PROPERTY_KEY, ACTIVE.name())
                        .has(RELATIONSHIP_GUID_PROPERTY_KEY)
                        .dedup()
                        .project("id", "valueMap", "label", "inVertexId", "outVertexId")
                        .by(__.id())
                        .by(__.valueMap(true))
                        .by(__.label())
                        .by(__.inV().id())
                        .by(__.outV().id())
                        .toList();

                allResults.addAll(batchResults);

                LOG.debug("Processed vertex batch {}-{} of {}, found {} edges",
                        i, end, vertexIdList.size(), batchResults.size());
            }

            return allResults;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }


    public VertexEdgePropertiesCache enrichVertexPropertiesByVertexIds(Set<String> vertexIds, Set<String> attributes) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("enrichVertexPropertiesByVertexIds");
       try {
           RequestContext context = RequestContext.get();
           int relationAttrsSize = MAX_EDGES_SUPER_VERTEX.getInt();
           if (context.isInvokedByIndexSearch() && context.isInvokedByProduct()) {
               relationAttrsSize = MIN_EDGES_SUPER_VERTEX.getInt();
           }
           VertexEdgePropertiesCache vertexEdgePropertyCache = new VertexEdgePropertiesCache();
           if (CollectionUtils.isEmpty(vertexIds)) {
               return null;
           }

          /*
            Returns a pair containing:
            1. A map of vertex IDs to their properties, where each property is a map of attribute names to lists of values.
            2. A map of vertex IDs to their corresponding AtlasVertex objects.
          */
           Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> vertexCache = getVertexPropertiesValueMap(vertexIds, 100);

           for (Map.Entry<String, Map<String, List<?>>> entry : vertexCache.getValue0().entrySet()) {
               String vertexId = entry.getKey();
               Map<String, List<?>> properties = entry.getValue();

               if (MapUtils.isNotEmpty(properties)) {
                   vertexEdgePropertyCache.addVertexProperties(vertexId, properties);
               }
           }
           vertexEdgePropertyCache.addVertices(vertexCache.getValue1());

           Set<String> edgeLabelsToProcess = collectEdgeLabelsToProcess(vertexEdgePropertyCache, vertexIds, attributes);

           Set<String> vertexIdsToProcess = new HashSet<>();
           if (!CollectionUtils.isEmpty(edgeLabelsToProcess)) {
               List<Map<String, Object>> relationEdges;
               if (AtlasConfiguration.ATLAS_INDEXSEARCH_EDGE_BULK_FETCH_ENABLE.getBoolean()) {
                   relationEdges = getConnectedRelationEdgesVertexBatching(vertexIds, edgeLabelsToProcess, relationAttrsSize);
               } else {
                   relationEdges = getConnectedRelationEdges(vertexIds, edgeLabelsToProcess, relationAttrsSize);
               }


               for(String vertexId : vertexIds) {
                   for (Map<String, Object> relationEdge : relationEdges) {
                       if (!(relationEdge.containsKey("id") && relationEdge.containsKey("valueMap"))) {
                           continue;
                       }
                       LinkedHashMap<Object, Object> valueMap = (LinkedHashMap<Object, Object>) relationEdge.get("valueMap");

                       String edgeId = relationEdge.get("id").toString();
                       String edgeLabel = relationEdge.get("label").toString();
                       String outVertexId = relationEdge.get("outVertexId").toString();
                       String inVertexId = relationEdge.get("inVertexId").toString();

                       if (!edgeLabelsToProcess.contains(edgeLabel)) {
                           continue;
                       }

                       // Check how this vertex relates to the edge
                       boolean isSelfLoop = vertexId.equals(outVertexId) && vertexId.equals(inVertexId);
                       boolean isSourceVertex = vertexId.equals(outVertexId);
                       boolean isTargetVertex = vertexId.equals(inVertexId);

                        // Only process if this vertex is part of the edge
                       if (!isSelfLoop && !isSourceVertex && !isTargetVertex) {
                           continue;
                       }

                        // For self-loops (like similarity relationships), reference points to itself
                        // For regular edges, reference points to the other vertex
                       String referencedVertex;
                       if (isSelfLoop) {
                           referencedVertex = outVertexId;
                       } else if (isSourceVertex) {
                           referencedVertex = inVertexId;
                       } else {
                           referencedVertex = outVertexId;
                       }

                       EdgeVertexReference edgeRef = new EdgeVertexReference(
                               referencedVertex, edgeId, edgeLabel, inVertexId, outVertexId, valueMap
                       );

                       boolean wasAdded = vertexEdgePropertyCache.addEdgeLabelToVertexIds(
                               vertexId, edgeLabel, edgeRef, relationAttrsSize
                       );

                       if (wasAdded && !isSelfLoop) {
                           vertexIdsToProcess.add(referencedVertex);
                       }

                   }
               }
           }

           Pair<Map<String, Map<String, List<?>>>, Map<String, AtlasVertex>> referenceVertices = getVertexPropertiesValueMap(vertexIdsToProcess, 1000);
           for (Map.Entry<String, Map<String, List<?>>> entry : referenceVertices.getValue0().entrySet()) {
               String vertexId = entry.getKey();
               Map<String, List<?>> properties = entry.getValue();

               if (MapUtils.isNotEmpty(properties)) {
                   vertexEdgePropertyCache.addVertexProperties(vertexId, properties);
               }
           }
           vertexEdgePropertyCache.addVertices(referenceVertices.getValue1());

           return vertexEdgePropertyCache;
       } finally {
           RequestContext.get().endMetricRecord(metricRecorder);
       }
    }

    private AtlasEntity mapVertexToAtlasEntity(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        return mapVertexToAtlasEntity(entityVertex, entityExtInfo, false);
    }

    private AtlasEntity mapVertexToAtlasEntity(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        return mapVertexToAtlasEntity(entityVertex, entityExtInfo, isMinExtInfo, true);
    }

    private AtlasEntity mapVertexToAtlasEntity(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo, boolean includeReferences) throws AtlasBaseException {
        String      guid   = GraphHelper.getGuid(entityVertex);
        AtlasEntity entity = entityExtInfo != null ? entityExtInfo.getEntity(guid) : null;

        if (entity == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Mapping graph vertex to atlas entity for guid {}", guid);
            }

            entity = new AtlasEntity();

            if (entityExtInfo != null) {
                entityExtInfo.addReferredEntity(guid, entity);
            }

            mapSystemAttributes(entityVertex, entity);

            entity.setDocId(entityVertex.getDocId());
            entity.setSuperTypeNames(typeRegistry.getEntityTypeByName(entity.getTypeName()).getAllSuperTypes());

            mapBusinessAttributes(entityVertex, entity);

            mapAttributes(entityVertex, entity, entityExtInfo, isMinExtInfo, includeReferences);

            if (!ignoreRelationshipAttr) { // only map when really needed
                if (fetchOnlyMandatoryRelationshipAttr) {
                    // map only mandatory relationships
                    mapMandatoryRelationshipAttributes(entityVertex, entity);
                } else {
                    // map all relationships
                    mapRelationshipAttributes(entityVertex, entity, entityExtInfo, isMinExtInfo);
                }
            }

            if(!RequestContext.get().isSkipAuthorizationCheck() && FeatureFlagStore.isTagV2Enabled()) {
                entity.setClassifications(tagDAO.getAllClassificationsForVertex(entityVertex.getIdForDisplay()));
            } else {
                mapClassifications(entityVertex, entity);
            }
        }

        return entity;
    }

    private AtlasEntity mapVertexToAtlasEntityMin(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        return mapVertexToAtlasEntityMin(entityVertex, entityExtInfo, null);
    }

    private AtlasEntity mapVertexToAtlasEntityMin(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo, Set<String> attributes) throws AtlasBaseException {
        String      guid   = GraphHelper.getGuid(entityVertex);
        AtlasEntity entity = entityExtInfo != null ? entityExtInfo.getEntity(guid) : null;

        if (entity == null) {
            entity = new AtlasEntity();

            if (entityExtInfo != null) {
                entityExtInfo.addReferredEntity(guid, entity);
            }

            mapSystemAttributes(entityVertex, entity);

            mapClassifications(entityVertex, entity);

            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

            if (entityType != null) {
                for (AtlasAttribute attribute : entityType.getMinInfoAttributes().values()) {
                    Object attrValue = getVertexAttribute(entityVertex, attribute);

                    if (attrValue != null) {
                        entity.setAttribute(attribute.getName(), attrValue);
                    }
                }
            }
        }

        return entity;
    }

    private AtlasEntityHeader mapVertexToAtlasEntityHeader(AtlasVertex entityVertex) throws AtlasBaseException {
        return mapVertexToAtlasEntityHeader(entityVertex, Collections.<String>emptySet());
    }

    private Map<String, Object> preloadProperties(AtlasVertex entityVertex,  AtlasStructType structType, Set<String> attributes, boolean fetchEdgeLabels) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("preloadProperties");

        try {
            if (structType == null) {
                return new HashMap<>();
            }

            // Execute the traversal to fetch properties
            Iterator<VertexProperty<Object>> traversal = ((AtlasJanusVertex)entityVertex).getWrappedElement().properties();
            Map<String, Object> propertiesMap = new HashMap<>();

            // Fetch edges in both directions
            // if the vertex in scope is root then call below otherwise skip
            // we don't support relation attributes of a relation
            if (fetchEdgeLabels && structType instanceof AtlasEntityType) {
                //  retrieve all the valid relationships for this entityType
                Map<String, Set<String>> relationshipsLookup = fetchEdgeNames((AtlasEntityType) structType);
                retrieveEdgeLabels(entityVertex, attributes, relationshipsLookup, propertiesMap);
            }

            // for attributes that are complexType and passed by BE, set them to empty string
            if (!fetchEdgeLabels){
                attributes.forEach(attribute -> {
                    propertiesMap.putIfAbsent(attribute, StringUtils.SPACE);
                });

            }

            // Iterate through the resulting VertexProperty objects
            while (traversal.hasNext()) {
                try {
                    VertexProperty<Object> property = traversal.next();

                    AtlasAttribute attribute = structType.getAttribute(property.key()) != null ? structType.getAttribute(property.key()) : null;
                    TypeCategory typeCategory = attribute != null ? attribute.getAttributeType().getTypeCategory() : null;
                    TypeCategory elementTypeCategory = attribute != null && attribute.getAttributeType().getTypeCategory() == TypeCategory.ARRAY ? ((AtlasArrayType) attribute.getAttributeType()).getElementType().getTypeCategory() : null;

                    if (property.isPresent()) {

                        // If the attribute is not known (null)
                        // validate if prefetched property is multi-valued
                        boolean isMultiValuedProperty = (property instanceof CacheVertexProperty && ((CacheVertexProperty) property).propertyKey().cardinality().equals(Cardinality.SET));
                        if (typeCategory == TypeCategory.ARRAY && (elementTypeCategory == TypeCategory.PRIMITIVE|| elementTypeCategory == TypeCategory.ENUM)) {
                            updateAttrValue(propertiesMap, property);
                        } else if (attribute == null && isMultiValuedProperty) {
                            updateAttrValue(propertiesMap, property);
                        } else if (propertiesMap.get(property.key()) == null) {
                            propertiesMap.put(property.key(), property.value());
                        }
                    }
                } catch (RuntimeException e) {
                    LOG.error("Error preloading properties for entityVertex: {}", entityVertex.getId(), e);
                    throw e; // Re-throw the exception after logging it
                }
            }
            return propertiesMap;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public Map<String, Set<String>> fetchEdgeNames(AtlasEntityType entityType){
        Map<String, Map<String, AtlasAttribute>> relationships = entityType.getRelationshipAttributes();
        Map<String, Set<String>> edgeNames = new HashMap<>();
        relationships.forEach((k,v) -> {
            v.forEach((k1,v1) -> {
                edgeNames.putIfAbsent(k1, new HashSet<>());
                edgeNames.get(k1).add(k);
            });
        });
        return edgeNames;
    }

    private void retrieveEdgeLabels(AtlasVertex entityVertex, Set<String> attributes, Map<String, Set<String>> relationshipsLookup,Map<String, Object> propertiesMap) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("retrieveEdgeLabels");
        try {
            Set<AbstractMap.SimpleEntry<String, String>> edgeLabelAndTypeName = graphHelper.retrieveEdgeLabelsAndTypeName(entityVertex);

            Set<String> edgeLabels = new HashSet<>();
            edgeLabelAndTypeName.stream().filter(Objects::nonNull).forEach(edgeLabelMap -> attributes.forEach(attribute->{

                if (edgeLabelMap.getKey().contains(attribute)){
                    edgeLabels.add(attribute);
                    return;
                }

                String edgeTypeName = edgeLabelMap.getValue();

                if (MapUtils.isNotEmpty(relationshipsLookup) && relationshipsLookup.containsKey(edgeTypeName) && relationshipsLookup.get(edgeTypeName).contains(attribute)) {
                    edgeLabels.add(attribute);
                }
            }));

            edgeLabels.stream().forEach(e -> propertiesMap.put(e, StringUtils.SPACE));
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void updateAttrValue( Map<String, Object> propertiesMap, VertexProperty<Object> property){
        Object value = propertiesMap.get(property.key());
        if (value instanceof List) {
            ((List) value).add(property.value());
        } else {
            List<Object> values = new ArrayList<>();
            values.add(property.value());
            propertiesMap.put(property.key(), values);
        }
    }

    private boolean isPolicyAttribute(Set<String> attributes) {
        Set<String> exclusionSet = new HashSet<>(Arrays.asList(AccessControlUtils.ATTR_POLICY_TYPE,
                AccessControlUtils.ATTR_POLICY_USERS,
                AccessControlUtils.ATTR_POLICY_GROUPS,
                AccessControlUtils.ATTR_POLICY_ROLES,
                AccessControlUtils.ATTR_POLICY_ACTIONS,
                AccessControlUtils.ATTR_POLICY_CATEGORY,
                AccessControlUtils.ATTR_POLICY_SUB_CATEGORY,
                AccessControlUtils.ATTR_POLICY_RESOURCES,
                AccessControlUtils.ATTR_POLICY_IS_ENABLED,
                AccessControlUtils.ATTR_POLICY_RESOURCES_CATEGORY,
                AccessControlUtils.ATTR_POLICY_SERVICE_NAME,
                AccessControlUtils.ATTR_POLICY_PRIORITY,
                AccessControlUtils.REL_ATTR_POLICIES,
                AccessControlUtils.ATTR_SERVICE_SERVICE_TYPE,
                AccessControlUtils.ATTR_SERVICE_TAG_SERVICE,
                AccessControlUtils.ATTR_SERVICE_IS_ENABLED,
                AccessControlUtils.ATTR_SERVICE_LAST_SYNC)
        );

        return exclusionSet.stream().anyMatch(attributes::contains);
    }

    private AtlasEntityHeader mapVertexToAtlasEntityHeader(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        String typeName = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
        boolean isS3Bucket = StringUtils.isNotEmpty(typeName)
                && typeName.equals("S3Bucket");


        boolean shouldPrefetch = (RequestContext.get().isInvokedByIndexSearch()
                || (RequestContext.get().isInvokedByLineage()
                && AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_LINEAGE.getBoolean()))
                && !isS3Bucket
                && AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION.getBoolean();

        // remove isPolicyAttribute from shouldPrefetch check
        // prefetch properties for policies
        // if there is some exception in fetching properties,
        // then we will fetch properties again without prefetch

        if (shouldPrefetch) {
            try {
                return mapVertexToAtlasEntityHeaderWithPrefetch(entityVertex, attributes);
            } catch (AtlasBaseException e) {
                if (isPolicyAttribute(attributes)) {
                    RequestContext.get().endMetricRecord(RequestContext.get().startMetricRecord("policiesPrefetchFailed"));
                    LOG.error("Error fetching properties for entity vertex: {}. Retrying without prefetch", entityVertex.getId(), e);
                    return mapVertexToAtlasEntityHeaderWithoutPrefetch(entityVertex, attributes);
                }
                throw e;
            }
        } else {
            return mapVertexToAtlasEntityHeaderWithoutPrefetch(entityVertex, attributes);
        }
    }

    private AtlasEntityHeader mapDynamicVertexToAtlasEntityHeader(AtlasVertex atlasVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapDynamicVertexToAtlasEntityHeader");
        AtlasEntityHeader ret = new AtlasEntityHeader();

        try {
            DynamicVertex dynamicVertex = ((AtlasJanusVertex) atlasVertex).getDynamicVertex();
            Map<String, Object> dynamicProperties = dynamicVertex.getAllProperties();

            if (MapUtils.isEmpty(dynamicProperties)) {
                LOG.warn("Dynamic properties map is empty for vertex {}", atlasVertex.getIdForDisplay());
                return null;
            }

            String  typeName     = (String) dynamicProperties.get(Constants.TYPE_NAME_PROPERTY_KEY);
            String  guid         = (String) dynamicProperties.get(Constants.GUID_PROPERTY_KEY);
            Boolean isIncomplete = isEntityIncomplete(dynamicVertex);

            ret.setTypeName(typeName);
            ret.setGuid(guid);
            ret.setIsIncomplete(isIncomplete);
            ret.setStatus(GraphHelper.getStatus(dynamicVertex));

            ret.setCreatedBy((String) dynamicProperties.get(CREATED_BY_KEY));
            ret.setUpdatedBy((String) dynamicProperties.get(MODIFIED_BY_KEY));

            // Set entity creation time if available
            long createdTime = GraphHelper.getCreatedTime(dynamicVertex, atlasVertex.getIdForDisplay());
            if (createdTime != 0L) {
                ret.setCreateTime(new Date(createdTime));
            }

            // Set entity last update time if available
            long updatedTime = GraphHelper.getModifiedTime(dynamicVertex, atlasVertex.getIdForDisplay());
            if (updatedTime != 0L) {
                ret.setUpdateTime(new Date(updatedTime));
            }

            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            ret.setDocId(atlasVertex.getDocId());

            RequestContext context = RequestContext.get();
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();

            if(includeClassifications || includeClassificationNames){
                List<AtlasClassification> tags = handleGetAllClassifications(atlasVertex);

                if(includeClassifications){
                    ret.setClassifications(tags);
                }
                ret.setClassificationNames(getAllTagNames(tags));
            }

            if (entityType != null) {
                ret.setSuperTypeNames(entityType.getAllSuperTypes());
                attributes.addAll(entityType.getHeaderAttributes().keySet());

                if (CollectionUtils.isNotEmpty(attributes)) {
                    EntityDiscoveryService.filterMapByKeys(entityType, dynamicVertex, attributes);
                }
            } else {
                LOG.warn("Entity type not found for type name: {} for entityVertexId {}", typeName, atlasVertex.getIdForDisplay());
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        return ret;
    }

    private AtlasEntityHeader mapVertexToAtlasEntityHeader(AtlasVertex entityVertex, Set<String> attributes, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToAtlasEntityHeaderWithoutPrefetch");
        AtlasEntityHeader ret = new AtlasEntityHeader();
        String vertexId = entityVertex.getIdForDisplay();
        try {
            String typeName = vertexEdgePropertiesCache.getPropertyValue(vertexId, Constants.TYPE_NAME_PROPERTY_KEY, String.class);
            String guid = vertexEdgePropertiesCache.getPropertyValue(vertexId, Constants.GUID_PROPERTY_KEY, String.class);

            ret.setTypeName(typeName);
            ret.setGuid(guid);
            String status = vertexEdgePropertiesCache.getPropertyValue(vertexId, STATE_PROPERTY_KEY, String.class);
            ret.setStatus(status != null ? AtlasEntity.Status.valueOf(status) : AtlasEntity.Status.ACTIVE);
            RequestContext context = RequestContext.get();
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();
            if(includeClassifications || includeClassificationNames){
                List<AtlasClassification> tags = handleGetAllClassifications(entityVertex);

                if (includeClassifications) {
                    ret.setClassifications(tags);
                }
                ret.setClassificationNames(getAllTagNames(tags));
            }
            ret.setLabels(getLabels(entityVertex));

            ret.setCreatedBy(vertexEdgePropertiesCache.getPropertyValue(vertexId, CREATED_BY_KEY, String.class));
            ret.setUpdatedBy(vertexEdgePropertiesCache.getPropertyValue(vertexId, MODIFIED_BY_KEY, String.class));

            Long createdTime = vertexEdgePropertiesCache.getPropertyValue(vertexId, TIMESTAMP_PROPERTY_KEY, Long.class);
            Long updatedTime = vertexEdgePropertiesCache.getPropertyValue(vertexId, MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);

            if (createdTime == null) {
                LOG.warn("DATA INCONSISTENCY ISSUE!! Vertex {} doesn't have created time", vertexId);
            } else {
                ret.setCreateTime(new Date(createdTime));
                ret.setUpdateTime(new Date(Optional.ofNullable(updatedTime).orElse(createdTime)));
            }

            if(RequestContext.get().includeMeanings()) {
                // TODO: This should be optimized to use vertexEdgePropertiesCache
                List<AtlasTermAssignmentHeader> termAssignmentHeaders = mapAssignedTerms(entityVertex);
                ret.setMeanings(termAssignmentHeaders);
                ret.setMeaningNames(
                        termAssignmentHeaders.stream().map(AtlasTermAssignmentHeader::getDisplayText)
                                .collect(Collectors.toList()));
            }
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            ret.setDocId(entityVertex.getDocId());
            if (entityType != null) {
                ret.setSuperTypeNames(entityType.getAllSuperTypes());
                for (AtlasAttribute headerAttribute : entityType.getHeaderAttributes().values()) {
                    Object attrValue = getVertexAttribute(entityVertex, headerAttribute, vertexEdgePropertiesCache);

                    if (attrValue != null) {
                        ret.setAttribute(headerAttribute.getName(), attrValue);
                    }
                }

                Object displayText = getDisplayText(vertexId, entityType,vertexEdgePropertiesCache);
                if (displayText != null) {
                    ret.setDisplayText(displayText.toString());
                }

                if (CollectionUtils.isNotEmpty(attributes)) {
                    for (String attrName : attributes) {
                        AtlasAttribute attribute = entityType.getAttribute(attrName);

                        if (attribute == null) {
                            attrName = toNonQualifiedName(attrName);

                            if (ret.hasAttribute(attrName)) {
                                continue;
                            }

                            attribute = entityType.getAttribute(attrName);

                            if (attribute == null) {
                                attribute = entityType.getRelationshipAttribute(attrName, null);

                                if (attribute != null && context.isInvokedByIndexSearch() && context.isInvokedByProduct() &&
                                        CollectionUtils.isEmpty(context.getRelationAttrsForSearch())) {
                                    continue;
                                }
                            }
                        }


                        Object attrValue = getVertexAttribute(entityVertex, attribute, vertexEdgePropertiesCache);

                        if (attrValue != null) {
                            ret.setAttribute(attrName, attrValue);
                        }
                    }
                }
            } else {
                LOG.warn("Entity type not found for type name: {} for entityVertexId {}", typeName, entityVertex.getIdForDisplay());
            }
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return ret;
    }


    private AtlasEntityHeader mapVertexToAtlasEntityHeaderWithoutPrefetch(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToAtlasEntityHeaderWithoutPrefetch");
        AtlasEntityHeader ret = new AtlasEntityHeader();
        try {
            String  typeName     = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
            String  guid         = entityVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
            Boolean isIncomplete = isEntityIncomplete(entityVertex);

            ret.setTypeName(typeName);
            ret.setGuid(guid);
            ret.setStatus(GraphHelper.getStatus(entityVertex));
            RequestContext context = RequestContext.get();
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();

            if(includeClassifications || includeClassificationNames){
                List<AtlasClassification> tags = handleGetAllClassifications(entityVertex);

                if(includeClassifications){
                    ret.setClassifications(tags);
                }
                ret.setClassificationNames(getAllTagNames(tags));
            }

            ret.setIsIncomplete(isIncomplete);
            ret.setLabels(getLabels(entityVertex));

            ret.setCreatedBy(GraphHelper.getCreatedByAsString(entityVertex));
            ret.setUpdatedBy(GraphHelper.getModifiedByAsString(entityVertex));

            // Set entity creation time if available
            long createdTime = GraphHelper.getCreatedTime(entityVertex);
            if (createdTime != 0L) {
                ret.setCreateTime(new Date(createdTime));
            }

            // Set entity last update time if available
            long updatedTime = GraphHelper.getModifiedTime(entityVertex);
            if (updatedTime != 0L) {
                ret.setUpdateTime(new Date(updatedTime));
            }

            if(RequestContext.get().includeMeanings()) {
                List<AtlasTermAssignmentHeader> termAssignmentHeaders = mapAssignedTerms(entityVertex);
                ret.setMeanings(termAssignmentHeaders);
                ret.setMeaningNames(
                        termAssignmentHeaders.stream().map(AtlasTermAssignmentHeader::getDisplayText)
                                .collect(Collectors.toList()));
            }
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            ret.setDocId(entityVertex.getDocId());

            if (entityType != null) {
                ret.setSuperTypeNames(entityType.getAllSuperTypes());
                for (AtlasAttribute headerAttribute : entityType.getHeaderAttributes().values()) {
                    Object attrValue = getVertexAttribute(entityVertex, headerAttribute);

                    if (attrValue != null) {
                        ret.setAttribute(headerAttribute.getName(), attrValue);
                    }
                }

                Object displayText = getDisplayText(entityVertex, entityType);

                if (displayText != null) {
                    ret.setDisplayText(displayText.toString());
                }

                if (CollectionUtils.isNotEmpty(attributes)) {
                    for (String attrName : attributes) {
                        // structs are processed here
                        AtlasAttribute attribute = entityType.getAttribute(attrName);

                        if (attribute == null) {
                            attrName = toNonQualifiedName(attrName);

                            if (ret.hasAttribute(attrName)) {
                                continue;
                            }

                            attribute = entityType.getAttribute(attrName);

                            if (attribute == null) {
                                attribute = entityType.getRelationshipAttribute(attrName, null);
                                // if it is relationshipAttribute but UI does not want to show it, skip processing
                                if (attribute != null
                                        && context.isInvokedByIndexSearch()
                                        && context.isInvokedByProduct() &&
                                        CollectionUtils.isEmpty(RequestContext.get().getRelationAttrsForSearch())) {
                                    continue;
                                }
                            }
                        }

                        Object attrValue = getVertexAttribute(entityVertex, attribute);

                        if (attrValue != null) {
                            ret.setAttribute(attrName, attrValue);
                        }
                    }
                }
            } else {
                LOG.warn("Entity type not found for type name: {} for entityVertexId {}", typeName, entityVertex.getIdForDisplay());
            }
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return ret;
    }

    public AtlasEntityHeader mapVertexToAtlasEntityHeaderWithPrefetch(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToAtlasEntityHeaderWithPrefetch");
        AtlasEntityHeader ret = new AtlasEntityHeader();
        try {
            //pre-fetching the properties
            String typeName = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class); //properties.get returns null
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName); // this is not costly
            Map<String, Object> properties = preloadProperties(entityVertex, entityType, attributes, true);

            String guid = (String) properties.get(Constants.GUID_PROPERTY_KEY);

            Integer value = (Integer)properties.get(Constants.IS_INCOMPLETE_PROPERTY_KEY);
            Boolean isIncomplete = value != null && value.equals(INCOMPLETE_ENTITY_VALUE) ? Boolean.TRUE : Boolean.FALSE;

            ret.setTypeName(typeName);
            ret.setGuid(guid);

            ret.setDocId(entityVertex.getDocId());
            if (entityType != null) {
                ret.setSuperTypeNames(entityType.getAllSuperTypes());
            } else {
                LOG.warn("Entity type not found for type name: {} for entityVertexId {}", typeName, entityVertex.getIdForDisplay());
            }

            String state = (String)properties.get(Constants.STATE_PROPERTY_KEY);
            Id.EntityState entityState = state == null ? null : Id.EntityState.valueOf(state);
            ret.setStatus((entityState == Id.EntityState.DELETED) ? AtlasEntity.Status.DELETED : AtlasEntity.Status.ACTIVE);

            RequestContext context = RequestContext.get();
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();

            if(includeClassifications || includeClassificationNames){
                List<AtlasClassification> tags = handleGetAllClassifications(entityVertex);

                if (includeClassifications) {
                    ret.setClassifications(tags);
                }
                ret.setClassificationNames(getAllTagNames(tags));
            }

            ret.setIsIncomplete(isIncomplete);
            ret.setLabels(getLabels(entityVertex));

            ret.setCreatedBy(properties.get(CREATED_BY_KEY) != null ? (String) properties.get(CREATED_BY_KEY) : null);
            ret.setUpdatedBy(properties.get(MODIFIED_BY_KEY) != null ? (String) properties.get(MODIFIED_BY_KEY) : null);
            ret.setCreateTime(properties.get(TIMESTAMP_PROPERTY_KEY) != null ? new Date((Long)properties.get(TIMESTAMP_PROPERTY_KEY)) : null);
            ret.setUpdateTime(properties.get(MODIFICATION_TIMESTAMP_PROPERTY_KEY) != null ? new Date((Long)properties.get(MODIFICATION_TIMESTAMP_PROPERTY_KEY)) : null);

            if(RequestContext.get().includeMeanings()) {
                List<AtlasTermAssignmentHeader> termAssignmentHeaders = mapAssignedTerms(entityVertex);
                ret.setMeanings(termAssignmentHeaders);
                ret.setMeaningNames(
                        termAssignmentHeaders.stream().map(AtlasTermAssignmentHeader::getDisplayText)
                                .collect(Collectors.toList()));
            }

            if (entityType != null) {
                for (AtlasAttribute headerAttribute : entityType.getHeaderAttributes().values()) {
                    Object attrValue = getVertexAttributePreFetchCache(entityVertex, headerAttribute, properties);

                    if (attrValue != null) {
                        ret.setAttribute(headerAttribute.getName(), attrValue);
                    }
                }

                if(properties.get(NAME) != null){
                    ret.setDisplayText(properties.get(NAME).toString());
                } else if(properties.get(DISPLAY_NAME) != null) {
                    ret.setDisplayText(properties.get(DISPLAY_NAME).toString());
                } else if(properties.get(QUALIFIED_NAME) != null) {
                    ret.setDisplayText(properties.get(QUALIFIED_NAME).toString());
                }



                //attributes = only the attributes of entityType
                if (CollectionUtils.isNotEmpty(attributes)) {
                    for (String attrName : attributes) {
                        AtlasAttribute attribute = entityType.getAttribute(attrName);

                        if (attribute == null) {
                            attrName = toNonQualifiedName(attrName);

                            if (ret.hasAttribute(attrName)) {
                                continue;
                            }

                            attribute = entityType.getAttribute(attrName);

                            if (attribute == null) {
                                // dataContractLatest, meanings, links
                                attribute = entityType.getRelationshipAttribute(attrName, null);

                                if (attribute != null
                                        && context.isInvokedByIndexSearch()
                                        && context.isInvokedByProduct() &&
                                        CollectionUtils.isEmpty(RequestContext.get().getRelationAttrsForSearch())) {
                                    continue;
                                }
                            }
                        }

                        //this is a call to cassandra
                        Object attrValue = getVertexAttributePreFetchCache(entityVertex, attribute, properties); //use prefetch cache

                        if (attrValue != null) {
                            ret.setAttribute(attrName, attrValue);
                        }
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return ret;
    }

    private String toNonQualifiedName(String attrName) {
        String ret;
        if (attrName.contains(".")) {
            String[] attributeParts = attrName.split("\\.");
            ret = attributeParts[attributeParts.length - 1];
        } else {
            ret = attrName;
        }
        return ret;
    }

    public AtlasEntity mapSystemAttributes(AtlasVertex entityVertex, AtlasEntity entity) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapSystemAttributes");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping system attributes for type {}", entity.getTypeName());
        }

        boolean enableJanusOptimization = AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_EXTENDED.getBoolean() && RequestContext.get().isInvokedByIndexSearch();

        try {

            if (entityVertex != null) {
                if (enableJanusOptimization) {
                    String typeName = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
                    Map<String, Object> properties = preloadProperties(entityVertex, entityType, Collections.emptySet(), false);

                    entity.setGuid((String) properties.get(GUID_PROPERTY_KEY));
                    entity.setTypeName(typeName);
                    String state = (String) properties.get(Constants.STATE_PROPERTY_KEY);
                    Id.EntityState entityState = state == null ? null : Id.EntityState.valueOf(state);
                    entity.setStatus((entityState == Id.EntityState.DELETED) ? AtlasEntity.Status.DELETED : AtlasEntity.Status.ACTIVE);
                    entity.setVersion(properties.get(VERSION_PROPERTY_KEY) != null ? (Long) properties.get(VERSION_PROPERTY_KEY) : 0);

                    entity.setCreatedBy(properties.get(CREATED_BY_KEY) != null ? (String) properties.get(CREATED_BY_KEY) : null);
                    entity.setUpdatedBy(properties.get(MODIFIED_BY_KEY) != null ? (String) properties.get(MODIFIED_BY_KEY) : null);

                    entity.setCreateTime(properties.get(TIMESTAMP_PROPERTY_KEY) != null ? new Date((Long) properties.get(TIMESTAMP_PROPERTY_KEY)) : null);
                    entity.setUpdateTime(properties.get(MODIFICATION_TIMESTAMP_PROPERTY_KEY) != null ? new Date((Long) properties.get(MODIFICATION_TIMESTAMP_PROPERTY_KEY)) : null);


                    entity.setHomeId(properties.get(HOME_ID_KEY) != null ? (String) properties.get(HOME_ID_KEY) : null);

                    entity.setIsProxy(properties.get(IS_PROXY_KEY) != null ? (Boolean) properties.get(IS_PROXY_KEY) : false);
                    Integer value = properties.get(Constants.IS_INCOMPLETE_PROPERTY_KEY) != null ? (Integer) properties.get(Constants.IS_INCOMPLETE_PROPERTY_KEY) : 0;
                    Boolean isIncomplete = value.equals(INCOMPLETE_ENTITY_VALUE) ? Boolean.TRUE : Boolean.FALSE;
                    entity.setIsIncomplete(isIncomplete);

                    entity.setProvenanceType(properties.get(PROVENANCE_TYPE_KEY) != null ? (int) properties.get(PROVENANCE_TYPE_KEY) : 0);
                    String customAttrsString = properties.get(CUSTOM_ATTRIBUTES_PROPERTY_KEY) != null ? (String) properties.get(CUSTOM_ATTRIBUTES_PROPERTY_KEY) : null;
                    entity.setCustomAttributes(StringUtils.isNotEmpty(customAttrsString) ? AtlasType.fromJson(customAttrsString, Map.class) : null);

                    String labels = properties.get(LABELS_PROPERTY_KEY) != null ? (String) properties.get(LABELS_PROPERTY_KEY) : null;
                    entity.setLabels(GraphHelper.parseLabelsString(labels));
                    Object pendingTasks = properties.get(PENDING_TASKS_PROPERTY_KEY);
                    if (pendingTasks instanceof List) {
                        entity.setPendingTasks(new HashSet<>((List<String>) pendingTasks));
                    }

                } else {
                    entity.setGuid(getGuid(entityVertex));
                    entity.setTypeName(getTypeName(entityVertex));
                    entity.setStatus(GraphHelper.getStatus(entityVertex));
                    entity.setVersion(GraphHelper.getVersion(entityVertex));

                    entity.setCreatedBy(GraphHelper.getCreatedByAsString(entityVertex));
                    entity.setUpdatedBy(GraphHelper.getModifiedByAsString(entityVertex));

                    entity.setCreateTime(new Date(GraphHelper.getCreatedTime(entityVertex)));
                    entity.setUpdateTime(new Date(GraphHelper.getModifiedTime(entityVertex)));

                    entity.setHomeId(GraphHelper.getHomeId(entityVertex));

                    entity.setIsProxy(GraphHelper.isProxy(entityVertex));
                    entity.setIsIncomplete(isEntityIncomplete(entityVertex));

                    entity.setProvenanceType(GraphHelper.getProvenanceType(entityVertex));
                    entity.setCustomAttributes(getCustomAttributes(entityVertex));
                    entity.setLabels(getLabels(entityVertex));
                    entity.setPendingTasks(getPendingTasks(entityVertex));
                }
            }
        } catch (Throwable t) {
            LOG.warn("Got exception while mapping system attributes for type {} : ", entity.getTypeName(), t);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return entity;
    }

    private void mapAttributes(AtlasVertex entityVertex, AtlasStruct struct, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        mapAttributes(entityVertex, struct, entityExtInfo, false);
    }

    private void mapAttributes(AtlasVertex entityVertex, AtlasStruct struct, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        mapAttributes(entityVertex, struct, entityExtInfo, isMinExtInfo, true);
    }

    private void mapAttributes(AtlasVertex entityVertex, AtlasStruct struct, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo, boolean includeReferences) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("retriever.mapAttributes");
        AtlasType objType = typeRegistry.getType(struct.getTypeName());

        if (!(objType instanceof AtlasStructType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, struct.getTypeName());
        }

        AtlasStructType structType = (AtlasStructType) objType;
        Map<String,Object> referenceProperties = Collections.emptyMap();
        boolean enableJanusOptimisation = AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_EXTENDED.getBoolean() && RequestContext.get().isInvokedByIndexSearch();

        if (enableJanusOptimisation){
            referenceProperties = preloadProperties(entityVertex, structType, structType.getAllAttributes().keySet(), false);
        }

        for (AtlasAttribute attribute : structType.getAllAttributes().values()) {
            Object attrValue;
            if (enableJanusOptimisation) {
                attrValue = getVertexAttributePreFetchCache(entityVertex, attribute, referenceProperties, entityExtInfo, isMinExtInfo, includeReferences);
            } else {
                attrValue = mapVertexToAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo, includeReferences);
            }

            struct.setAttribute(attribute.getName(), attrValue);
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void mapBusinessAttributes(AtlasVertex entityVertex, AtlasEntity entity) throws AtlasBaseException {
        entity.setBusinessAttributes(getBusinessMetadata(entityVertex));
    }

    public List<AtlasClassification> handleGetAllClassifications(AtlasVertex entityVertex) throws AtlasBaseException {
        if(!RequestContext.get().isSkipAuthorizationCheck() && FeatureFlagStore.isTagV2Enabled()) {
            return getAllClassifications_V2(entityVertex);
        } else {
            return getAllClassifications_V1(entityVertex);
        }
    }

    public List<AtlasClassification> getAllClassifications_V2(AtlasVertex entityVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getAllClassifications");
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("Performing getAllClassifications_V2");

            List<AtlasClassification> classifications = tagDAO.getAllClassificationsForVertex(entityVertex.getIdForDisplay());

            // Map each classification's attributes with defaults
            if (CollectionUtils.isNotEmpty(classifications)) {
                classifications = classifications.stream()
                        .map(classification -> {
                            try {
                                return TagAttributeMapper.mapClassificationAttributesWithDefaults(classification, typeRegistry);
                            } catch (AtlasBaseException e) {
                                LOG.error("Error mapping classification attributes with defaults for classification: {}", classification.getTypeName(), e);
                                return classification;
                            }
                        })
                        .collect(Collectors.toList());
            }
            return classifications;
        } catch (Exception e) {
            LOG.error("Error while getting all classifications", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public List<AtlasClassification> getAllClassifications(String idForDisplay) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getAllClassifications");
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing getAllClassifications");
            }
            return tagDAO.getAllClassificationsForVertex(idForDisplay);
        } catch (Exception e) {
            LOG.error("Error while getting all classifications", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }


    public List<AtlasClassification> getAllClassifications_V1(AtlasVertex entityVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getAllClassifications");
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing getAllClassifications");
            }

            // use optimised path only for indexsearch and when flag is enabled!
            if (ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION_FOR_CLASSIFICATIONS.getBoolean() && RequestContext.get().isInvokedByIndexSearch()) {
                // Fetch classification vertices directly
                List<Map<String, Object>> classificationProperties = new ArrayList<>();
                List<AtlasClassification> ret = new ArrayList<>();
                ((AtlasJanusGraph) graph).getGraph().traversal()
                        .V(entityVertex.getId())  // Start from the entity vertex
                        .outE(CLASSIFICATION_LABEL) // Get outgoing classification edges
                        .inV() // Move to classification vertex
                        .project(TYPE_NAME_PROPERTY_KEY, CLASSIFICATION_ENTITY_GUID, CLASSIFICATION_ENTITY_STATUS,
                                CLASSIFICATION_VERTEX_PROPAGATE_KEY, CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE,
                                CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY) // Fetch only needed properties
                        .by(__.values(TYPE_NAME_PROPERTY_KEY))
                        .by(__.values(CLASSIFICATION_ENTITY_GUID))
                        .by(__.values(CLASSIFICATION_ENTITY_STATUS))
                        .by(__.values(CLASSIFICATION_VERTEX_PROPAGATE_KEY))
                        .by(__.values(CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY))
                        .by(__.values(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE))
                        .by(__.values(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY))
                        .toList()
                        .forEach(obj -> {
                            if (obj instanceof Map) {
                                classificationProperties.add((Map<String, Object>) obj);
                            }
                        });
                classificationProperties.forEach(classificationProperty -> {
                    if (classificationProperty != null) {
                        AtlasClassification atlasClassification = new AtlasClassification();
                        atlasClassification.setTypeName(classificationProperty.get(TYPE_NAME_PROPERTY_KEY) != null ? (String) classificationProperty.get(TYPE_NAME_PROPERTY_KEY) : "");
                        atlasClassification.setEntityGuid(classificationProperty.get(CLASSIFICATION_ENTITY_GUID) != null ? (String) classificationProperty.get(CLASSIFICATION_ENTITY_GUID) : "");
                        atlasClassification.setEntityStatus(classificationProperty.get(CLASSIFICATION_ENTITY_STATUS) != null ? AtlasEntity.Status.valueOf((String) classificationProperty.get(CLASSIFICATION_ENTITY_STATUS)) : null);
                        atlasClassification.setPropagate(classificationProperty.get(CLASSIFICATION_VERTEX_PROPAGATE_KEY) != null ? (Boolean) classificationProperty.get(CLASSIFICATION_VERTEX_PROPAGATE_KEY) : null);
                        atlasClassification.setRemovePropagationsOnEntityDelete(classificationProperty.get(CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY) != null ? (Boolean) classificationProperty.get(CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY) : true);
                        atlasClassification.setRestrictPropagationThroughLineage(classificationProperty.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE) != null ? (Boolean) classificationProperty.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE) : false);
                        atlasClassification.setRestrictPropagationThroughHierarchy(classificationProperty.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY) != null ? (Boolean) classificationProperty.get(CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY) : false);
                        ret.add(atlasClassification);
                    }
                });
                return ret;
            } else {
                List<AtlasClassification> ret = new ArrayList<>();
                Iterable edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL).edges();

                if (edges != null) {
                    Iterator<AtlasEdge> iterator = edges.iterator();

                    while (iterator.hasNext()) {
                        AtlasEdge classificationEdge = iterator.next();
                        AtlasVertex classificationVertex = classificationEdge != null ? classificationEdge.getInVertex() : null;
                        AtlasClassification classification = toAtlasClassification(classificationVertex);

                        if (classification != null) {
                            ret.add(classification);
                        }
                    }
                }

                RequestContext.get().endMetricRecord(metricRecorder);
                return ret;
            }
        } catch (Exception e) {
            LOG.error("Error while getting all classifications", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }
    public List<AtlasClassification> getDirectClassifications(AtlasVertex entityVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getDirectClassifications");
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing getDirectClassifications");
            }

            return tagDAO.getAllDirectClassificationsForVertex(entityVertex.getIdForDisplay());

        } catch (Exception e) {
            LOG.error("Error while getting direct classifications", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public List<AtlasTermAssignmentHeader> mapAssignedTerms(AtlasVertex entityVertex) {
        List<AtlasTermAssignmentHeader> ret = new ArrayList<>();

        Iterable edges = entityVertex.query().direction(AtlasEdgeDirection.IN).label(TERM_ASSIGNMENT_LABEL).edges();

        if (edges != null) {
            for (final AtlasEdge edge : (Iterable<AtlasEdge>) edges) {
                if (edge != null && GraphHelper.getStatus(edge) != AtlasEntity.Status.DELETED) {
                    ret.add(toTermAssignmentHeader(edge));
                }
            }
        }

        return ret;
    }
    private AtlasTermAssignmentHeader toTermAssignmentHeader(final AtlasEdge edge) {
        AtlasTermAssignmentHeader ret = new AtlasTermAssignmentHeader();

        AtlasVertex termVertex = edge.getOutVertex();

        String guid = GraphHelper.getGuid(termVertex);
        if (guid != null) {
            ret.setTermGuid(guid);
        }

        String relationGuid = edge.getProperty(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        if (relationGuid != null) {
            ret.setRelationGuid(relationGuid);
        }

        Object displayName = AtlasGraphUtilsV2.getEncodedProperty(termVertex, GLOSSARY_TERM_DISPLAY_NAME_ATTR, Object.class);
        if (displayName instanceof String) {
            ret.setDisplayText((String) displayName);
        }

        String description = edge.getProperty(TERM_ASSIGNMENT_ATTR_DESCRIPTION, String.class);
        if (description != null) {
            ret.setDescription(description);
        }

        String expression    = edge.getProperty(TERM_ASSIGNMENT_ATTR_EXPRESSION, String.class);
        if (expression != null) {
            ret.setExpression(expression);
        }

        String status = edge.getProperty(TERM_ASSIGNMENT_ATTR_STATUS, String.class);
        if (status != null) {
            AtlasTermAssignmentStatus assignmentStatus = AtlasTermAssignmentStatus.valueOf(status);
            ret.setStatus(assignmentStatus);
        }

        Integer confidence = edge.getProperty(TERM_ASSIGNMENT_ATTR_CONFIDENCE, Integer.class);
        if (confidence != null) {
            ret.setConfidence(confidence);
        }

        String createdBy = edge.getProperty(TERM_ASSIGNMENT_ATTR_CREATED_BY, String.class);
        if (createdBy != null) {
            ret.setCreatedBy(createdBy);
        }

        String steward = edge.getProperty(TERM_ASSIGNMENT_ATTR_STEWARD, String.class);
        if (steward != null) {
            ret.setSteward(steward);
        }

        String source = edge.getProperty(TERM_ASSIGNMENT_ATTR_SOURCE, String.class);
        if (source != null) {
            ret.setSource(source);
        }

        return ret;
    }

    public void mapClassifications(AtlasVertex entityVertex, AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapClassifications");
        List<AtlasEdge> edges = getAllClassificationEdges(entityVertex);

        if (CollectionUtils.isNotEmpty(edges)) {
            List<AtlasClassification> allClassifications = new ArrayList<>();

            for (AtlasEdge edge : edges) {
                AtlasVertex         classificationVertex = edge.getInVertex();
                AtlasClassification classification       = toAtlasClassification(classificationVertex);

                if (classification != null) {
                    allClassifications.add(classification);
                }
            }

            entity.setClassifications(allClassifications);
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private Object mapVertexToAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) throws AtlasBaseException {
        return mapVertexToAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo, true);
    }

    private Object mapVertexToAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo, boolean includeReferences) throws AtlasBaseException {
        return mapVertexToAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo, includeReferences, false, null);
    }

    private Object mapVertexToAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo, boolean includeReferences, boolean ignoreInactive, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToAttribute");
        try {
            Object    ret                = null;
            AtlasType attrType           = attribute.getAttributeType();
            String    edgeLabel          = attribute.getRelationshipEdgeLabel();
            boolean   isOwnedAttribute   = attribute.isOwnedRef();
            AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();
            String   vertexId           = entityVertex.getIdForDisplay();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Mapping vertex {} to atlas entity {}.{}", entityVertex, attribute.getDefinedInDef().getName(), attribute.getName());
            }

            switch (attrType.getTypeCategory()) { // indexsearch && bulk
                case PRIMITIVE:
                    AtlasPerfMetrics.MetricRecorder recorder0 = RequestContext.get().startMetricRecord("mapVertexToAttribute.PRIMITIVE");

                    if (vertexEdgePropertiesCache != null) {
                        ret = mapVertexToPrimitiveV2(vertexId, attribute.getVertexPropertyName(), attribute.getAttributeDef(), vertexEdgePropertiesCache);
                    } else {
                        ret = mapVertexToPrimitive(entityVertex, attribute.getVertexPropertyName(), attribute.getAttributeDef());
                    }
                    RequestContext.get().endMetricRecord(recorder0);
                    break;
                case ENUM:
                    AtlasPerfMetrics.MetricRecorder recorder1 = RequestContext.get().startMetricRecord("mapVertexToAttribute.ENUM.getEncodedProperty");
                    if (vertexEdgePropertiesCache != null) {
                        ret = vertexEdgePropertiesCache.getPropertyValue(vertexId, attribute.getVertexPropertyName(), Object.class);
                    } else {
                        ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, attribute.getVertexPropertyName(), Object.class);
                    }
                    RequestContext.get().endMetricRecord(recorder1);
                    break;
                case STRUCT:
                    AtlasPerfMetrics.MetricRecorder recorder2 = RequestContext.get().startMetricRecord("mapVertexToAttribute.STRUCT");
                    if (LEAN_GRAPH_ENABLED) {
                        Object val = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, attribute.getVertexPropertyName(), Object.class);
                        if (val instanceof String) {
                            ret = mapStringToStruct(attrType, (String) val);
                        } else  {
                            ret = val;
                        }

                    } else {
                        edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                        ret = mapVertexToStruct(entityVertex, edgeLabel, null, entityExtInfo, isMinExtInfo);
                    }
                    RequestContext.get().endMetricRecord(recorder2);
                    break;
                case OBJECT_ID_TYPE:
                    AtlasPerfMetrics.MetricRecorder recorder3 = RequestContext.get().startMetricRecord("mapVertexToAttribute.OBJECT_ID_TYPE");
                    if (includeReferences) {
                        if (TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory()) {
                            //Struct attribute having ObjectId as type
                            edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                        }
                        if (vertexEdgePropertiesCache != null) {
                            ret = attribute.getAttributeDef().isSoftReferenced() ? mapVertexToObjectIdForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo) :
                            mapVertexToObjectIdV2(entityVertex, vertexEdgePropertiesCache.getRelationShipElement(vertexId, edgeLabel, edgeDirection), edgeDirection, vertexEdgePropertiesCache);

                        } else {
                            ret = attribute.getAttributeDef().isSoftReferenced() ? mapVertexToObjectIdForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo) :
                                    mapVertexToObjectId(entityVertex, edgeLabel, null, entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo);
                        }

                    } else {
                        ret = null;
                    }
                    RequestContext.get().endMetricRecord(recorder3);
                    break;
                case ARRAY: {
                    AtlasPerfMetrics.MetricRecorder recorder4 = RequestContext.get().startMetricRecord("mapVertexToAttribute.ARRAY");
                    final boolean skipAttribute;

                    if (!includeReferences) {
                        AtlasType elementType = ((AtlasArrayType) attrType).getElementType();

                        skipAttribute = (elementType instanceof AtlasObjectIdType || elementType instanceof AtlasEntityType);
                    } else {
                        skipAttribute = false;
                    }

                    if (skipAttribute) {
                        ret = null;
                    } else {
                        if (attribute.getAttributeDef().isSoftReferenced()) {
                            ret = mapVertexToArrayForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                        } else {
                            ret = mapVertexToArray(entityVertex, entityExtInfo, isOwnedAttribute, attribute, isMinExtInfo, includeReferences, ignoreInactive, vertexEdgePropertiesCache);
                            if (ret == null && !attribute.getAttributeDef().getIsDefaultValueNull()) {
                                ret = Collections.emptyList();
                            }
                        }
                    }
                    RequestContext.get().endMetricRecord(recorder4);
                }
                break;
                case MAP: {
                    AtlasPerfMetrics.MetricRecorder recorder5 = RequestContext.get().startMetricRecord("mapVertexToAttribute.MAP");
                    final boolean skipAttribute;

                    if (!includeReferences) {
                        AtlasType valueType = ((AtlasMapType) attrType).getValueType();

                        skipAttribute = (valueType instanceof AtlasObjectIdType || valueType instanceof AtlasEntityType);
                    } else {
                        skipAttribute = false;
                    }

                    if (skipAttribute) {
                        ret = null;
                    } else {
                        if (attribute.getAttributeDef().isSoftReferenced()) {
                            ret = mapVertexToMapForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                        } else {
                            ret = mapVertexToMap(entityVertex, entityExtInfo, isOwnedAttribute, attribute, isMinExtInfo, includeReferences);
                        }
                    }
                    RequestContext.get().endMetricRecord(recorder5);
                }
                break;
                case CLASSIFICATION:
                    // do nothing
                    break;
            }

            return ret;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Map<String, AtlasObjectId> mapVertexToMapForSoftRef(AtlasVertex entityVertex,  AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) {
        Map<String, AtlasObjectId> ret        = null;
        Map                        softRefVal = entityVertex.getProperty(attribute.getVertexPropertyName(), Map.class);

        if (MapUtils.isEmpty(softRefVal)) {
            return softRefVal;
        } else {
            ret = new HashMap<>();

            for (Object mapKey : softRefVal.keySet()) {
                AtlasObjectId objectId = getAtlasObjectIdFromSoftRefFormat(Objects.toString(softRefVal.get(mapKey)), attribute, entityExtInfo, isMinExtInfo);

                if (objectId != null) {
                    ret.put(Objects.toString(mapKey), objectId);
                }
            }
        }
        return ret;
    }

    private List<AtlasObjectId> mapVertexToArrayForSoftRef(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) {
        List<AtlasObjectId> ret        = null;
        List                softRefVal = entityVertex.getListProperty(attribute.getVertexPropertyName(), List.class);

        if (CollectionUtils.isEmpty(softRefVal)) {
            return softRefVal;
        } else {
            ret = new ArrayList<>();

            for (Object o : softRefVal) {
                AtlasObjectId objectId = getAtlasObjectIdFromSoftRefFormat(Objects.toString(o), attribute, entityExtInfo, isMinExtInfo);

                if(objectId != null) {
                    ret.add(objectId);
                }
            }
        }

        return ret;
    }

    private AtlasObjectId mapVertexToObjectIdForSoftRef(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) {
        String softRefVal = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, attribute.getVertexPropertyName(), String.class);

        return StringUtils.isNotEmpty(softRefVal) ? getAtlasObjectIdFromSoftRefFormat(softRefVal, attribute, entityExtInfo, isMinExtInfo) : null;
    }

    private AtlasObjectId getAtlasObjectIdFromSoftRefFormat(String softRefVal, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) {
        AtlasObjectId ret = AtlasEntityUtil.parseSoftRefValue(softRefVal);

        if(ret != null) {
            if (entityExtInfo != null && attribute.isOwnedRef()) {
                try {
                    AtlasVertex referenceVertex = getEntityVertex(ret.getGuid());

                    if (referenceVertex != null) {
                        final AtlasEntity entity;

                        if (isMinExtInfo) {
                            entity = mapVertexToAtlasEntityMin(referenceVertex, entityExtInfo);
                        } else {
                            entity = mapVertexToAtlasEntity(referenceVertex, entityExtInfo);
                        }

                        if (entity != null) {
                            ret = toAtlasObjectId(entity);
                        }
                    }
                } catch (AtlasBaseException excp) {
                    LOG.info("failed to retrieve soft-referenced entity(typeName={}, guid={}); errorCode={}. Ignoring", ret.getTypeName(), ret.getGuid(), excp.getAtlasErrorCode());
                }
            }
        }

        return ret;
    }

    private Map<String, Object> mapVertexToMap(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo,
                                               boolean isOwnedAttribute, AtlasAttribute attribute, final boolean isMinExtInfo, boolean includeReferences) throws AtlasBaseException {

        Map<String, Object> ret          = null;
        AtlasMapType        mapType      = (AtlasMapType) attribute.getAttributeType();
        AtlasType           mapValueType = mapType.getValueType();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping map attribute {} for vertex {}", mapType.getTypeName(), entityVertex);
        }

        if (isReference(mapValueType)) {
            Map<String, Object> currentMap = getReferenceMap(entityVertex, attribute, AtlasGraphUtilsV2.getEdgeLabel(attribute.getVertexPropertyName()
            ));

            if (MapUtils.isNotEmpty(currentMap)) {
                ret = new HashMap<>();

                for (Map.Entry<String, Object> entry : currentMap.entrySet()) {
                    String mapKey    = entry.getKey();
                    Object keyValue  = entry.getValue();
                    Object mapValue  = mapVertexToCollectionEntry(mapValueType, entityVertex, mapValueType, keyValue, attribute.getRelationshipEdgeLabel(),
                                                                  entityExtInfo, isOwnedAttribute, attribute.getRelationshipEdgeDirection(), isMinExtInfo, includeReferences, null);
                    if (mapValue != null) {
                        ret.put(mapKey, mapValue);
                    }
                }
            }
        } else {
            ret = getPrimitiveMap(entityVertex, attribute.getVertexPropertyName());
        }

        return ret;
    }

    private List<Object> mapVertexToArray(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo,
                                          boolean isOwnedAttribute, AtlasAttribute attribute, final boolean isMinExtInfo,
                                          boolean includeReferences, boolean ignoreInactive, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {

        AtlasArrayType arrayType        = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      arrayElementType = arrayType.getElementType();
        List<Object>   arrayElements    = getArrayElementsProperty(arrayElementType, entityVertex, attribute, vertexEdgePropertiesCache);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping array attribute {} for vertex {}", arrayElementType.getTypeName(), entityVertex);
        }

        if (CollectionUtils.isEmpty(arrayElements)) {
            return arrayElements;
        }

        List                           arrValues     = new ArrayList(arrayElements.size());
        String                         edgeLabel     = attribute.getRelationshipEdgeLabel();
        AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();

        for (Object element : arrayElements) {
            // When internal types are deleted, sometimes the collection type attribute will contain a null value
            // Graph layer does erroneous mapping of the null element, hence avoiding the processing of the null element
            if (element == null) {
                LOG.debug("Skipping null arrayElement");
                continue;
            }

            if (isInactiveEdge(element, ignoreInactive)) {
                continue;
            }

            AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("mapVertexToCollectionEntry");
            Object arrValue = mapVertexToCollectionEntry(arrayElementType, entityVertex, arrayElementType, element, edgeLabel,
                                                         entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo, includeReferences,
                                                        vertexEdgePropertiesCache);
            RequestContext.get().endMetricRecord(recorder);

            if (arrValue != null) {
                arrValues.add(arrValue);
            }
        }

        return arrValues;
    }

    private Object mapVertexToCollectionEntry(AtlasType atlasType,
                                              AtlasVertex entityVertex, AtlasType arrayElement, Object value,
                                              String edgeLabel, AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute,
                                              AtlasRelationshipEdgeDirection edgeDirection, final boolean isMinExtInfo, boolean includeReferences, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        Object ret = null;

        switch (arrayElement.getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
            case ARRAY:
                ret = value;
                break;

            case MAP:
                if (LEAN_GRAPH_ENABLED && value instanceof String) { // For indexsearch & diff & constructHeader (probably all reads)
                    return AtlasType.fromJson((String) value, Map.class);
                } else {
                    ret = value;
                }
                break;

            case CLASSIFICATION:
                break;

            case STRUCT:
                if (LEAN_GRAPH_ENABLED) {
                    ret = value instanceof String ? mapStringToStruct(atlasType, (String) value) : value;
                } else {
                    ret = mapVertexToStruct(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo, isMinExtInfo);
                }
                break;

            case OBJECT_ID_TYPE:
                if(vertexEdgePropertiesCache != null) {
                    ret = includeReferences ? mapVertexToObjectIdV2(entityVertex, (Pair<String, EdgeVertexReference.EdgeInfo>) value, edgeDirection, vertexEdgePropertiesCache) : null;
                } else {
                    ret = includeReferences ? mapVertexToObjectId(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo) : null;
                }
                break;

            default:
                break;
        }

        return ret;
    }

    public static Object mapVertexToPrimitive(AtlasElement entityVertex, final String vertexPropertyName, AtlasAttributeDef attrDef) {
        Object ret = null;

        if (AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Object.class) == null) {
            return null;
        }

        switch (attrDef.getTypeName().toLowerCase()) {
            case ATLAS_TYPE_STRING:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, String.class);
                break;
            case ATLAS_TYPE_SHORT:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Short.class);
                break;
            case ATLAS_TYPE_INT:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Integer.class);
                break;
            case ATLAS_TYPE_BIGINTEGER:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, BigInteger.class);
                break;
            case ATLAS_TYPE_BOOLEAN:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Boolean.class);
                break;
            case ATLAS_TYPE_BYTE:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Byte.class);
                break;
            case ATLAS_TYPE_LONG:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Long.class);
                break;
            case ATLAS_TYPE_FLOAT:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Float.class);
                break;
            case ATLAS_TYPE_DOUBLE:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Double.class);
                break;
            case ATLAS_TYPE_BIGDECIMAL:
                ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, BigDecimal.class);
                break;
            case ATLAS_TYPE_DATE:
                ret = new Date(AtlasGraphUtilsV2.getEncodedProperty(entityVertex, vertexPropertyName, Long.class));
                break;
            default:
                break;
        }

        return ret;
    }

    private AtlasStruct mapStringToStruct(AtlasType atlasType, String value) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapStringToStruct");
        try {

            String typeName = atlasType.getTypeName();
            AtlasStructType structType = typeRegistry.getStructTypeByName(typeName);

            return structType.getStructFromValue(value);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public static Object mapVertexToPrimitiveV2(String elementId, final String vertexPropertyName, AtlasAttributeDef attrDef, VertexEdgePropertiesCache vertexEdgePropertiesCache) {
        Object ret = null;

        if (vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Object.class) == null) {
            return null;
        }

        switch (attrDef.getTypeName().toLowerCase()) {
            case ATLAS_TYPE_STRING:
                ret  = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), String.class);
                break;
            case ATLAS_TYPE_SHORT:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Short.class);
                break;
            case ATLAS_TYPE_INT:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Integer.class);
                break;
            case ATLAS_TYPE_BIGINTEGER:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), BigInteger.class);
                break;
            case ATLAS_TYPE_BOOLEAN:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Boolean.class);
                break;
            case ATLAS_TYPE_BYTE:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Byte.class);
                break;
            case ATLAS_TYPE_LONG:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Long.class);
                break;
            case ATLAS_TYPE_FLOAT:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Float.class);
                break;
            case ATLAS_TYPE_DOUBLE:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Double.class);
                break;
            case ATLAS_TYPE_BIGDECIMAL:
                ret = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), BigDecimal.class);
                break;
            case ATLAS_TYPE_DATE:
                Long dateValue = vertexEdgePropertiesCache.getPropertyValue(elementId, encodePropertyKey(vertexPropertyName), Long.class);
                ret = new Date(dateValue != null ? dateValue : 0L);
                break;
            default:
                break;
        }

        return ret;
    }

    private AtlasObjectId mapVertexToObjectId(AtlasVertex entityVertex, String edgeLabel, AtlasEdge edge,
                                              AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute,
                                              AtlasRelationshipEdgeDirection edgeDirection, final boolean isMinExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("mapVertexToObjectId");
        AtlasObjectId ret = null;

        if (edge == null) {
            edge = graphHelper.getEdgeForLabel(entityVertex, edgeLabel, edgeDirection);
        }

        if (GraphHelper.elementExists(edge)) {
            if (!RequestContext.get().isAllowDeletedRelationsIndexsearch() && getState(edge) == Id.EntityState.DELETED ) {
                return null;
            }
            AtlasVertex referenceVertex = edge.getInVertex();



            if (StringUtils.equals(getIdFromVertex(referenceVertex), getIdFromVertex(entityVertex))) {
                referenceVertex = edge.getOutVertex();
            }

            if (referenceVertex != null) {
                if (entityExtInfo != null && isOwnedAttribute) {
                    final AtlasEntity entity;

                    if (isMinExtInfo) {
                        entity = mapVertexToAtlasEntityMin(referenceVertex, entityExtInfo);
                    } else {
                        entity = mapVertexToAtlasEntity(referenceVertex, entityExtInfo);
                    }

                    if (entity != null) {
                        ret = AtlasTypeUtil.getAtlasObjectId(entity);
                    }
                } else {
                    ret = toAtlasObjectId(referenceVertex);
                }

                if (ret == null) {
                    LOG.warn("Found corrupted vertex with Id: {}", referenceVertex.getIdForDisplay());
                }
            }

            if (ret != null && RequestContext.get().isIncludeRelationshipAttributes()) {
                String relationshipTypeName = GraphHelper.getTypeName(edge);
                boolean isRelationshipAttribute = typeRegistry.getRelationshipDefByName(relationshipTypeName) != null;
                if (isRelationshipAttribute) {
                    AtlasRelationship relationship = mapEdgeToAtlasRelationship(edge);
                    Map<String, Object> relationshipAttributes = mapOf("typeName", relationshipTypeName);
                    relationshipAttributes.put("attributes", relationship.getAttributes());

                    if (ret.getAttributes() == null) {
                        ret.setAttributes(mapOf("relationshipAttributes", relationshipAttributes));
                    } else {
                        ret.getAttributes().put("relationshipAttributes", relationshipAttributes);
                    }
                }
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    private AtlasObjectId  mapVertexToObjectIdV2(AtlasVertex entityVertex, Pair<String, EdgeVertexReference.EdgeInfo> referencedElementPair, AtlasRelationshipEdgeDirection edgeDirection,
                                                VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        if (referencedElementPair == null || referencedElementPair.getValue1() == null) {
            return null;
        }
        AtlasObjectId ret;
        EdgeVertexReference.EdgeInfo edgeInfo = referencedElementPair.getValue1();
        String edgeLabel = edgeInfo.getEdgeLabel();
        String elementId = referencedElementPair.getValue0();
        String edgeId = edgeInfo.getEdgeId();


        EdgeVertexReference edgeVertexReference = vertexEdgePropertiesCache.getReferenceVertexByEdgeLabelAndId(entityVertex.getIdForDisplay(), edgeLabel, elementId, edgeId, edgeDirection);

        if (edgeVertexReference == null) {
            LOG.debug("EdgeVertexReference not found for vertexId: {}, edgeLabel: {}, elementId: {}, edgeId: {}", entityVertex.getIdForDisplay(), edgeLabel, elementId, edgeId);
            return null;
        }

        ret = toAtlasObjectIdV2(elementId, vertexEdgePropertiesCache);

        if (ret == null) {
            LOG.debug("Found corrupted vertex with Id: {}", entityVertex.getIdForDisplay());
            return null;
        }


        String relationshipTypeName = edgeVertexReference.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
        boolean isRelationshipAttribute = typeRegistry.getRelationshipDefByName(relationshipTypeName) != null;


        if (isRelationshipAttribute && RequestContext.get().isIncludeRelationshipAttributes()) {             // Map Attributes
            AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);
            if (relationshipType == null) {
                LOG.warn("Relationship type not found for typeName: {}", relationshipTypeName);
                return ret;
            }
            AtlasRelationship relationship = edgeVertexReference.toAtlasRelationship(vertexEdgePropertiesCache);
            // Map Attributes
            for (AtlasAttribute attribute : relationshipType.getAllAttributes().values()) {
                // mapping only primitive attributes
                Object attrValue = mapVertexToPrimitiveV2(edgeId, attribute.getVertexPropertyName(), attribute.getAttributeDef(), vertexEdgePropertiesCache);
                relationship.setAttribute(attribute.getName(), attrValue);
            }

            Map<String, Object> relationshipAttributes = mapOf("typeName", relationshipTypeName);
                relationshipAttributes.put("attributes", relationship.getAttributes());

                if (ret.getAttributes() == null) {
                    ret.setAttributes(mapOf("relationshipAttributes", relationshipAttributes));
                } else {
                    ret.getAttributes().put("relationshipAttributes", relationshipAttributes);
                }
        }

        return ret;
    }

    private AtlasStruct mapVertexToStruct(AtlasVertex entityVertex, String edgeLabel, AtlasEdge edge, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToStruct");
        try {
            AtlasStruct ret = null;

            if (edge == null) {
                edge = graphHelper.getEdgeForLabel(entityVertex, edgeLabel);
            }

            if (GraphHelper.elementExists(edge)) {
                final AtlasVertex referenceVertex = edge.getInVertex();

                if (referenceVertex == null) {
                    LOG.error("reference vertex not found  on edge {} from vertex {} ", edge.getId(), getGuid(entityVertex));
                    return ret;
                }

                String typeName = null;
                try {
                    typeName = getTypeName(referenceVertex);
                } catch (IllegalStateException ile) {
                    String entityVertexId = entityVertex.getIdForDisplay();
                    String entityGuid = getGuid(entityVertex);
                    LOG.error("IllegalStateException for vertexId {}, entityGuid {}, GraphHelper.elementExists(referenceVertex) {}",
                            entityVertexId, entityGuid, GraphHelper.elementExists(referenceVertex));
                    if (!GraphHelper.elementExists(referenceVertex)) {
                        return null;
                    } else {
                        throw ile;
                    }
                }

                if (StringUtils.isEmpty(typeName)) {
                    LOG.error("typeName not found on edge {} from vertex {} ", edge.getId(), getGuid(entityVertex));
                    return ret;
                }

                ret = new AtlasStruct(typeName);

                mapAttributes(referenceVertex, ret, entityExtInfo, isMinExtInfo);
            }

            return ret;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public Object getVertexAttribute(AtlasVertex vertex, AtlasAttribute attribute) throws AtlasBaseException {
        return vertex != null && attribute != null ? mapVertexToAttribute(vertex, attribute, null, false) : null;
    }

    public Object getVertexAttribute(AtlasVertex vertex, AtlasAttribute attribute, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        return vertex != null && attribute != null ? mapVertexToAttribute(vertex, attribute, null, false, true, false, vertexEdgePropertiesCache) : null;
    }

    public Object getVertexAttributePreFetchCache(AtlasVertex vertex, AtlasAttribute attribute, Map<String, Object> properties,  AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo, final boolean includeReferences) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getVertexAttributePreFetchCache");

        try{
            if (vertex == null || attribute == null) {
                return null;
            }

            TypeCategory typeCategory = attribute.getAttributeType().getTypeCategory();
            TypeCategory elementTypeCategory = typeCategory == TypeCategory.ARRAY ? ((AtlasArrayType) attribute.getAttributeType()).getElementType().getTypeCategory() : null;
            boolean isArrayOfPrimitives = typeCategory.equals(TypeCategory.ARRAY) && elementTypeCategory.equals(TypeCategory.PRIMITIVE);
            boolean isArrayOfMap = typeCategory.equals(TypeCategory.ARRAY) && elementTypeCategory.equals(TypeCategory.MAP);
            boolean isArrayOfEnum = typeCategory.equals(TypeCategory.ARRAY) && elementTypeCategory.equals(TypeCategory.ENUM);
            boolean isPrefetchValueFinal = (typeCategory.equals(TypeCategory.PRIMITIVE) || typeCategory.equals(TypeCategory.ENUM) || typeCategory.equals(TypeCategory.MAP) || isArrayOfPrimitives || isArrayOfMap || isArrayOfEnum);
            boolean isMultiValueBusinessAttribute = attribute.getDefinedInType() != null && attribute.getDefinedInType().getTypeCategory() == TypeCategory.BUSINESS_METADATA && (isArrayOfPrimitives || isArrayOfEnum);


            // value is present and value is not marker (SPACE for further lookup) and type is primitive or array of primitives
            if (properties.get(attribute.getName()) != null && properties.get(attribute.getName()) != StringUtils.SPACE && (isMultiValueBusinessAttribute || isPrefetchValueFinal)) {
                return properties.get(attribute.getName());
            }

            // if value is empty && element is array and not inward relation, return empty list
            if (properties.get(attribute.getName()) == null && typeCategory.equals(TypeCategory.ARRAY)) {
                return new ArrayList<>();
            }

            //when value is not present and type is primitive, return null
            if(properties.get(attribute.getName()) == null && isPrefetchValueFinal) {
                return null;
            }

            // value is present as marker , fetch the value from the vertex
            if (properties.get(attribute.getName()) != null && properties.get(attribute.getName()).equals(StringUtils.SPACE)) {
                return mapVertexToAttribute(vertex, attribute, entityExtInfo , isMinExtInfo, includeReferences);
            }

            return null;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public Object getVertexAttributePreFetchCache(AtlasVertex vertex, AtlasAttribute attribute, Map<String, Object> properties) throws AtlasBaseException {
        return getVertexAttributePreFetchCache(vertex, attribute, properties, null, false, true);
    }


    private Object getVertexAttributeIgnoreInactive(AtlasVertex vertex, AtlasAttribute attribute) throws AtlasBaseException {
        return vertex != null && attribute != null ? mapVertexToAttribute(vertex, attribute, null, false, true, true, null) : null;
    }

    private void mapRelationshipAttributes(AtlasVertex entityVertex, AtlasEntity entity, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("EntityGraphRetriever.mapRelationshipAttributes");

        try {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, entity.getTypeName());
            }

            for (String attributeName : entityType.getRelationshipAttributes().keySet()) {
                mapVertexToRelationshipAttribute(entityVertex, entityType, attributeName, entity, entityExtInfo, isMinExtInfo);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void mapMandatoryRelationshipAttributes(AtlasVertex entityVertex, AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("EntityGraphRetriever.mapMandatoryRelationshipAttributes");

        try {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, entity.getTypeName());
            }

            for (Map<String, AtlasAttribute> attrs : entityType.getRelationshipAttributes().values()) {
                for (AtlasAttribute attr : attrs.values()) {
                    if (!attr.getAttributeDef().getIsOptional()) {
                        String attrName = attr.getAttributeDef().getName();
                        mapVertexToRelationshipAttribute(entityVertex, entityType, attrName, entity, null, false);
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Object mapVertexToRelationshipAttribute(AtlasVertex entityVertex, AtlasEntityType entityType, String attributeName, AtlasEntity entity, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToRelationshipAttribute");

        try {
            Object                ret                  = null;
            String                relationshipTypeName = graphHelper.getRelationshipTypeName(entityVertex, entityType, attributeName);
            AtlasRelationshipType relationshipType     = relationshipTypeName != null ? typeRegistry.getRelationshipTypeByName(relationshipTypeName) : null;

            if (relationshipType == null) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_INVALID, "relationshipDef is null");
            }

            AtlasAttribute          attribute       = entityType.getRelationshipAttribute(attributeName, relationshipTypeName);
            AtlasRelationshipDef    relationshipDef = relationshipType.getRelationshipDef();
            AtlasRelationshipEndDef endDef1         = relationshipDef.getEndDef1();
            AtlasRelationshipEndDef endDef2         = relationshipDef.getEndDef2();
            AtlasEntityType         endDef1Type     = typeRegistry.getEntityTypeByName(endDef1.getType());
            AtlasEntityType         endDef2Type     = typeRegistry.getEntityTypeByName(endDef2.getType());
            AtlasRelationshipEndDef attributeEndDef = null;

            if (endDef1Type.isTypeOrSuperTypeOf(entityType.getTypeName()) && StringUtils.equals(endDef1.getName(), attributeName)) {
                attributeEndDef = endDef1;
            } else if (endDef2Type.isTypeOrSuperTypeOf(entityType.getTypeName()) && StringUtils.equals(endDef2.getName(), attributeName)) {
                attributeEndDef = endDef2;
            }

            if (attributeEndDef == null) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_INVALID, relationshipDef.toString());
            }

            switch (attributeEndDef.getCardinality()) {
                case SINGLE:
                    ret = mapRelatedVertexToObjectId(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                    break;

                case LIST:
                case SET:
                    ret = mapRelationshipArrayAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                    break;
            }

            // Set Relationship attributes, even if the value is null
            entity.setRelationshipAttribute(attributeName, ret);

            if (attributeEndDef.getIsLegacyAttribute() && !entity.hasAttribute(attributeName)) {
                entity.setAttribute(attributeName, toLegacyAttribute(ret));
            }

            return ret;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Object toLegacyAttribute(Object obj) {
        final Object ret;

        if (obj instanceof AtlasRelatedObjectId) {
            ret = toLegacyAttribute((AtlasRelatedObjectId) obj);
        } else if (obj instanceof Collection) {
            ret = toLegacyAttribute((Collection) obj);
        } else if (obj instanceof Map) {
            ret = toLegacyAttribute((Map) obj);
        } else {
            ret = obj;
        }

        return ret;
    }

    private AtlasObjectId toLegacyAttribute(AtlasRelatedObjectId relatedObjId) {
        final AtlasObjectId ret;

        if (relatedObjId.getRelationshipStatus() == DELETED && relatedObjId.getEntityStatus() == AtlasEntity.Status.ACTIVE) {
            ret = null;
        } else {
            ret = new AtlasObjectId(relatedObjId);
        }

        return ret;
    }

    private Collection toLegacyAttribute(Collection collection) {
        final List ret = new ArrayList();

        for (Object elem : collection) {
            Object objId = toLegacyAttribute(elem);

            if (objId != null) {
                ret.add(objId);
            }
        }

        return ret;
    }

    private Map toLegacyAttribute(Map map) {
        final Map ret = new HashMap();

        for (Object key : map.keySet()) {
            Object elem = toLegacyAttribute(map.get(key));

            if (elem != null) {
                ret.put(key, elem);
            }
        }

        return ret;
    }

    private AtlasObjectId mapRelatedVertexToObjectId(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasEdge edge = graphHelper.getEdgeForLabel(entityVertex, attribute.getRelationshipEdgeLabel(), attribute.getRelationshipEdgeDirection());

        return mapVertexToRelatedObjectId(entityVertex, edge, attribute.isOwnedRef(), entityExtInfo, isMinExtInfo);
    }

    private List<AtlasRelatedObjectId> mapRelationshipArrayAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        List<AtlasRelatedObjectId> ret   = new ArrayList<>();
        Iterator<AtlasEdge>        edges = null;

        if (attribute.getRelationshipEdgeDirection() == IN) {
            edges = getIncomingEdgesByLabel(entityVertex, attribute.getRelationshipEdgeLabel());
        } else if (attribute.getRelationshipEdgeDirection() == OUT) {
            edges = getOutGoingEdgesByLabel(entityVertex, attribute.getRelationshipEdgeLabel());
        } else if (attribute.getRelationshipEdgeDirection() == BOTH) {
            edges = getAdjacentEdgesByLabel(entityVertex, AtlasEdgeDirection.BOTH, attribute.getRelationshipEdgeLabel());
        }

        if (edges != null) {
            while (edges.hasNext()) {
                AtlasEdge relationshipEdge = edges.next();

                AtlasRelatedObjectId relatedObjectId = mapVertexToRelatedObjectId(entityVertex, relationshipEdge, attribute.isOwnedRef(), entityExtInfo, isMinExtInfo);

                ret.add(relatedObjectId);
            }
        }

        return ret;
    }

    private AtlasRelatedObjectId mapVertexToRelatedObjectId(AtlasVertex entityVertex, AtlasEdge edge, boolean isOwnedRef, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasRelatedObjectId ret = null;

        if (GraphHelper.elementExists(edge)) {
            AtlasVertex referenceVertex = edge.getInVertex();

            if (StringUtils.equals(getIdFromVertex(referenceVertex), getIdFromVertex(entityVertex))) {
                referenceVertex = edge.getOutVertex();
            }

            if (referenceVertex != null) {
                String             entityTypeName = getTypeName(referenceVertex);
                String             entityGuid     = getGuid(referenceVertex);
                AtlasEntity.Status entityStatus   = GraphHelper.getStatus(referenceVertex);
                AtlasRelationship  relationship   = mapEdgeToAtlasRelationship(edge);

                ret = new AtlasRelatedObjectId(entityGuid, entityTypeName, entityStatus,
                                               relationship.getGuid(), relationship.getStatus(),
                                               new AtlasStruct(relationship.getTypeName(), relationship.getAttributes()));

                Object displayText = getDisplayText(referenceVertex, entityTypeName);

                if (displayText != null) {
                    ret.setDisplayText(displayText.toString());
                }

                if (isOwnedRef && entityExtInfo != null) {
                    if (isMinExtInfo) {
                        mapVertexToAtlasEntityMin(referenceVertex, entityExtInfo);
                    } else {
                        mapVertexToAtlasEntity(referenceVertex, entityExtInfo);
                    }
                }

            }
        }

        return ret;
    }

    private Object getDisplayText(AtlasVertex entityVertex, String entityTypeName) throws AtlasBaseException {
        return getDisplayText(entityVertex, typeRegistry.getEntityTypeByName(entityTypeName));
    }

    private Object getDisplayText(String vertexId, AtlasEntityType entityType, VertexEdgePropertiesCache vertexEdgePropertiesCache) throws AtlasBaseException {
        Object ret = null;

        if (entityType != null) {
            String displayTextAttribute = entityType.getDisplayTextAttribute();

            if (displayTextAttribute != null) {
                ret = vertexEdgePropertiesCache.getPropertyValue(vertexId, DISPLAY_NAME, Object.class);
            }
            if (ret == null) {
                ret = vertexEdgePropertiesCache.getPropertyValue(vertexId, NAME, Object.class);

                if (ret == null) {
                    ret = vertexEdgePropertiesCache.getPropertyValue(vertexId, QUALIFIED_NAME, Object.class);
                }
            }
        }

        return ret;
    }

    private Object getDisplayText(AtlasVertex entityVertex, AtlasEntityType entityType) throws AtlasBaseException {
        Object ret = null;

        if (entityType != null) {
            String displayTextAttribute = entityType.getDisplayTextAttribute();

            if (displayTextAttribute != null) {
                ret = getVertexAttribute(entityVertex, entityType.getAttribute(displayTextAttribute));
            }

            if (ret == null) {
                ret = getVertexAttribute(entityVertex, entityType.getAttribute(NAME));

                if (ret == null) {
                    ret = getVertexAttribute(entityVertex, entityType.getAttribute(DISPLAY_NAME));

                    if (ret == null) {
                        ret = getVertexAttribute(entityVertex, entityType.getAttribute(QUALIFIED_NAME));
                    }
                }
            }
        }

        return ret;
    }

    public AtlasRelationship mapEdgeToAtlasRelationship(AtlasEdge edge) throws AtlasBaseException {
        return mapEdgeToAtlasRelationship(edge, false).getRelationship();
    }

    public AtlasRelationshipWithExtInfo mapEdgeToAtlasRelationshipWithExtInfo(AtlasEdge edge) throws AtlasBaseException {
        return mapEdgeToAtlasRelationship(edge, true);
    }

    public AtlasRelationshipWithExtInfo mapEdgeToAtlasRelationship(AtlasEdge edge, boolean extendedInfo) throws AtlasBaseException {
        AtlasRelationshipWithExtInfo ret = new AtlasRelationshipWithExtInfo();

        mapSystemAttributes(edge, ret, extendedInfo);

        mapAttributes(edge, ret);

        return ret;
    }

    public AtlasEntity getOrInitializeDiffEntity(AtlasVertex vertex) {
        AtlasEntity diffEntity = RequestContext.get().getDifferentialEntity(GraphHelper.getGuid(vertex));
        if (diffEntity == null) {
            diffEntity = new AtlasEntity();
            diffEntity.setTypeName(GraphHelper.getTypeName(vertex));
            diffEntity.setGuid(GraphHelper.getGuid(vertex));
            diffEntity.setUpdateTime(new Date(RequestContext.get().getRequestTime()));
            RequestContext.get().cacheDifferentialEntity(diffEntity, vertex);
        }
        return diffEntity;
    }
    private AtlasRelationshipWithExtInfo mapSystemAttributes(AtlasEdge edge, AtlasRelationshipWithExtInfo relationshipWithExtInfo, boolean extendedInfo) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping system attributes for relationship");
        }

        AtlasRelationship relationship = relationshipWithExtInfo.getRelationship();

        if (relationship == null) {
            relationship = new AtlasRelationship();

            relationshipWithExtInfo.setRelationship(relationship);
        }

        relationship.setGuid(getRelationshipGuid(edge));
        relationship.setTypeName(getTypeName(edge));

        relationship.setCreatedBy(GraphHelper.getCreatedByAsString(edge));
        relationship.setUpdatedBy(GraphHelper.getModifiedByAsString(edge));

        relationship.setCreateTime(new Date(GraphHelper.getCreatedTime(edge)));
        relationship.setUpdateTime(new Date(GraphHelper.getModifiedTime(edge)));

        Long version = GraphHelper.getVersion(edge);
        if (version == null) {
            version = Long.valueOf(1L);
        }
        relationship.setVersion(version);

        Integer provenanceType = GraphHelper.getProvenanceType(edge);
        if (provenanceType == null) {
            provenanceType = Integer.valueOf(0);
        }
        relationship.setProvenanceType(provenanceType);
        relationship.setHomeId(GraphHelper.getHomeId(edge));

        relationship.setStatus(GraphHelper.getEdgeStatus(edge));

        AtlasVertex end1Vertex = edge.getOutVertex();
        AtlasVertex end2Vertex = edge.getInVertex();

        relationship.setEnd1(new AtlasObjectId(getGuid(end1Vertex), getTypeName(end1Vertex), getEntityUniqueAttribute(end1Vertex)));
        relationship.setEnd2(new AtlasObjectId(getGuid(end2Vertex), getTypeName(end2Vertex), getEntityUniqueAttribute(end2Vertex)));

        relationship.setLabel(edge.getLabel());
        relationship.setPropagateTags(getPropagateTags(edge));

        if (extendedInfo) {
            addToReferredEntities(relationshipWithExtInfo, end1Vertex);
            addToReferredEntities(relationshipWithExtInfo, end2Vertex);
        }

        // set propagated and blocked propagated classifications
        readClassificationsFromEdge(edge, relationshipWithExtInfo, extendedInfo);

        return relationshipWithExtInfo;
    }

    private void readClassificationsFromEdge(AtlasEdge edge, AtlasRelationshipWithExtInfo relationshipWithExtInfo, boolean extendedInfo) throws AtlasBaseException {
        List<AtlasVertex>        classificationVertices    = getPropagatableClassifications(edge);
        List<String>             blockedClassificationIds  = getBlockedClassificationIds(edge);
        AtlasRelationship        relationship              = relationshipWithExtInfo.getRelationship();
        Set<AtlasClassification> propagatedClassifications = new HashSet<>();
        Set<AtlasClassification> blockedClassifications    = new HashSet<>();

        for (AtlasVertex classificationVertex : classificationVertices) {
            String              classificationId = classificationVertex.getIdForDisplay();
            AtlasClassification classification   = toAtlasClassification(classificationVertex);

            if (classification == null) {
                continue;
            }

            if (blockedClassificationIds.contains(classificationId)) {
                blockedClassifications.add(classification);
            } else {
                propagatedClassifications.add(classification);
            }

            // add entity headers to referred entities
            if (extendedInfo) {
                addToReferredEntities(relationshipWithExtInfo, classification.getEntityGuid());
            }
        }

        relationship.setPropagatedClassifications(propagatedClassifications);
        relationship.setBlockedPropagatedClassifications(blockedClassifications);
    }

    private void addToReferredEntities(AtlasRelationshipWithExtInfo relationshipWithExtInfo, String guid) throws AtlasBaseException {
        if (!relationshipWithExtInfo.referredEntitiesContains(guid)) {
            addToReferredEntities(relationshipWithExtInfo, getEntityVertex(guid));
        }
    }

    private void addToReferredEntities(AtlasRelationshipWithExtInfo relationshipWithExtInfo, AtlasVertex entityVertex) throws AtlasBaseException {
        String entityGuid = getGuid(entityVertex);

        if (!relationshipWithExtInfo.referredEntitiesContains(entityGuid)) {
            relationshipWithExtInfo.addReferredEntity(entityGuid, toAtlasEntityHeader(entityVertex));
        }
    }

    private void mapAttributes(AtlasEdge edge, AtlasRelationshipWithExtInfo relationshipWithExtInfo) throws AtlasBaseException {
        AtlasRelationship relationship = relationshipWithExtInfo.getRelationship();
        AtlasType         objType      = typeRegistry.getType(relationship.getTypeName());

        if (!(objType instanceof AtlasRelationshipType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, relationship.getTypeName());
        }

        AtlasRelationshipType relationshipType = (AtlasRelationshipType) objType;

        for (AtlasAttribute attribute : relationshipType.getAllAttributes().values()) {
            // mapping only primitive attributes
            Object attrValue = mapVertexToPrimitive(edge, attribute.getVertexPropertyName(), attribute.getAttributeDef());

            relationship.setAttribute(attribute.getName(), attrValue);
        }
    }

    private Set<String> getPendingTasks(AtlasVertex entityVertex) {
        Collection<String> ret = entityVertex.getPropertyValues(PENDING_TASKS_PROPERTY_KEY, String.class);

        if (CollectionUtils.isEmpty(ret)) {
            return null;
        }

        return new HashSet<>(ret);
    }


    private boolean isInactiveEdge(Object element, boolean ignoreInactive) {
        return ignoreInactive && element instanceof AtlasEdge && getStatus((AtlasEdge) element) != AtlasEntity.Status.ACTIVE;
    }

    /**
     * Gracefully shuts down the thread pool when the application is stopping.
     */
    @PreDestroy
    public void shutdownExecutor() {
        LOG.info("Shutting down EntityGraphRetriever's executor service.");
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
