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
import org.apache.atlas.exception.AtlasBaseException;
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
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusEdge;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusVertex;
import org.apache.atlas.repository.util.AccessControlUtils;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.graphdb.relations.CacheVertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

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

    private static final TypeReference<List<TimeBoundary>> TIME_BOUNDARIES_LIST_TYPE = new TypeReference<List<TimeBoundary>>() {};
    private final GraphHelper graphHelper;

    private final AtlasTypeRegistry typeRegistry;

    private final boolean ignoreRelationshipAttr;
    private final AtlasGraph graph;

    @Inject
    public EntityGraphRetriever(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this(graph, typeRegistry, false);
    }

    public EntityGraphRetriever(AtlasGraph graph, AtlasTypeRegistry typeRegistry, boolean ignoreRelationshipAttr) {
        this.graph                  = graph;
        this.graphHelper            = new GraphHelper(graph);
        this.typeRegistry           = typeRegistry;
        this.ignoreRelationshipAttr = ignoreRelationshipAttr;

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

    public AtlasEntityHeader toAtlasEntityHeaderWithClassifications(String guid) throws AtlasBaseException {
        return toAtlasEntityHeaderWithClassifications(getEntityVertex(guid), Collections.emptySet());
    }

    public AtlasEntityHeader toAtlasEntityHeaderWithClassifications(AtlasVertex entityVertex) throws AtlasBaseException {
        return toAtlasEntityHeaderWithClassifications(entityVertex, Collections.emptySet());
    }

    public AtlasEntityHeader toAtlasEntityHeaderWithClassifications(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasEntityHeader ret = toAtlasEntityHeader(entityVertex, attributes);

        ret.setClassifications(getAllClassifications(entityVertex));

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

        if (entityType != null) {
            Map<String, Object> uniqueAttributes = new HashMap<>();

            for (AtlasAttribute attribute : entityType.getUniqAttributes().values()) {
                Object attrValue = getVertexAttribute(entityVertex, attribute);

                if (attrValue != null) {
                    uniqueAttributes.put(attribute.getName(), attrValue);
                }
            }

            Map<String, Object> attributes = new HashMap<>();
            Set<String> relationAttributes = RequestContext.get().getRelationAttrsForSearch();
            if (CollectionUtils.isNotEmpty(relationAttributes)) {
                for (String attributeName : relationAttributes) {
                    AtlasAttribute attribute = entityType.getAttribute(attributeName);
                    if (attribute != null
                            && !uniqueAttributes.containsKey(attributeName)) {
                        Object attrValue = getVertexAttribute(entityVertex, attribute);
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

            ret.setEntityGuid(AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_GUID, String.class));
            ret.setEntityStatus(getClassificationEntityStatus(classificationVertex));
            ret.setPropagate(isPropagationEnabled(classificationVertex));
            ret.setRemovePropagationsOnEntityDelete(getRemovePropagations(classificationVertex));
            ret.setRestrictPropagationThroughLineage(getRestrictPropagationThroughLineage(classificationVertex));
            ret.setRestrictPropagationThroughHierarchy(getRestrictPropagationThroughHierarchy(classificationVertex));

            String strValidityPeriods = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_VALIDITY_PERIODS_KEY, String.class);

            if (strValidityPeriods != null) {
                ret.setValidityPeriods(AtlasJson.fromJson(strValidityPeriods, TIME_BOUNDARIES_LIST_TYPE));
            }

            mapAttributes(classificationVertex, ret, null);
        }

        return ret;
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

        if (Boolean.TRUE.equals(currentRestrictPropagationThroughLineage) && Boolean.TRUE.equals(currentRestrictPropagationThroughHierarchy)) {
            throw new AtlasBaseException("Both restrictPropagationThroughLineage and restrictPropagationThroughHierarchy cannot be true simultaneously.");
        } else if (Boolean.TRUE.equals(currentRestrictPropagationThroughLineage)) {
            propagationMode = CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE;
        } else if (Boolean.TRUE.equals(currentRestrictPropagationThroughHierarchy)) {
            propagationMode = CLASSIFICATION_PROPAGATION_MODE_RESTRICT_HIERARCHY;
        } else {
            propagationMode = CLASSIFICATION_PROPAGATION_MODE_DEFAULT;
        }

        return propagationMode;
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
    public List<AtlasVertex> getIncludedImpactedVerticesV2(AtlasVertex entityVertex, String relationshipGuidToExclude, String classificationId, List<String> edgeLabelsToCheck,Boolean toExclude) {
        List<String> vertexIds = new ArrayList<>();
        traverseImpactedVerticesByLevel(entityVertex, relationshipGuidToExclude, classificationId, vertexIds, edgeLabelsToCheck,toExclude, null);

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

            mapBusinessAttributes(entityVertex, entity);

            mapAttributes(entityVertex, entity, entityExtInfo, isMinExtInfo, includeReferences);

            if (!ignoreRelationshipAttr) { // only map when really needed
                mapRelationshipAttributes(entityVertex, entity, entityExtInfo, isMinExtInfo);
            }

            mapClassifications(entityVertex, entity);
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

    private Map<String, Object> preloadProperties(AtlasVertex entityVertex, AtlasEntityType entityType, Set<String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("preloadProperties");

        try {
            if (entityType == null) {
                return new HashMap<>();
            }
            Map<String, Object> propertiesMap = new HashMap<>();

            // Execute the traversal to fetch properties
            Iterator<VertexProperty<Object>> traversal = ((AtlasJanusVertex)entityVertex).getWrappedElement().properties();

            //  retrieve all the valid relationships for this entityType
            Map<String, Set<String>> relationshipsLookup = fetchEdgeNames(entityType);

            // Fetch edges in both directions
            retrieveEdgeLabels(entityVertex, attributes, relationshipsLookup, propertiesMap);

            // Iterate through the resulting VertexProperty objects
            while (traversal.hasNext()) {
                try {
                    VertexProperty<Object> property = traversal.next();

                    AtlasAttribute attribute = entityType.getAttribute(property.key()) != null ? entityType.getAttribute(property.key()) : null;
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
                    LOG.error("Error preloading properties for entity vertex: {}", entityVertex.getId(), e);
                    throw e; // Re-throw the exception after logging it
                }
            }
            return propertiesMap;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Map<String, Set<String>> fetchEdgeNames(AtlasEntityType entityType){
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
        } finally {
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
        boolean shouldPrefetch = RequestContext.get().isInvokedByIndexSearch()
                && !isPolicyAttribute(attributes)
                && !entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class).equals("S3Bucket")
                && AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_JANUS_OPTIMISATION.getBoolean();

        if (shouldPrefetch) {
            return mapVertexToAtlasEntityHeaderWithPrefetch(entityVertex, attributes);
        } else {
            return mapVertexToAtlasEntityHeaderWithoutPrefetch(entityVertex, attributes);
        }
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
            if(includeClassifications){
                ret.setClassificationNames(getAllTraitNamesFromAttribute(entityVertex));
            } else if (!includeClassifications && includeClassificationNames) {
                ret.setClassificationNames(getAllTraitNamesFromAttribute(entityVertex));
            }
            ret.setIsIncomplete(isIncomplete);
            ret.setLabels(getLabels(entityVertex));

            ret.setCreatedBy(GraphHelper.getCreatedByAsString(entityVertex));
            ret.setUpdatedBy(GraphHelper.getModifiedByAsString(entityVertex));
            ret.setCreateTime(new Date(GraphHelper.getCreatedTime(entityVertex)));
            ret.setUpdateTime(new Date(GraphHelper.getModifiedTime(entityVertex)));

            if(RequestContext.get().includeMeanings()) {
                List<AtlasTermAssignmentHeader> termAssignmentHeaders = mapAssignedTerms(entityVertex);
                ret.setMeanings(termAssignmentHeaders);
                ret.setMeaningNames(
                        termAssignmentHeaders.stream().map(AtlasTermAssignmentHeader::getDisplayText)
                                .collect(Collectors.toList()));
            }
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (entityType != null) {
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
                        AtlasAttribute attribute = entityType.getAttribute(attrName);

                        if (attribute == null) {
                            attrName = toNonQualifiedName(attrName);

                            if (ret.hasAttribute(attrName)) {
                                continue;
                            }

                            attribute = entityType.getAttribute(attrName);

                            if (attribute == null) {
                                attribute = entityType.getRelationshipAttribute(attrName, null);
                            }
                        }


                        Object attrValue = getVertexAttribute(entityVertex, attribute);

                        if (attrValue != null) {
                            ret.setAttribute(attrName, attrValue);
                        }
                    }
                }
            }
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return ret;
    }

    private AtlasEntityHeader mapVertexToAtlasEntityHeaderWithPrefetch(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToAtlasEntityHeaderWithPrefetch");
        AtlasEntityHeader ret = new AtlasEntityHeader();
        try {
            //pre-fetching the properties
            String typeName = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class); //properties.get returns null
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName); // this is not costly
            Map<String, Object> properties = preloadProperties(entityVertex, entityType, attributes);

            String guid = (String) properties.get(Constants.GUID_PROPERTY_KEY);

            Integer value = (Integer)properties.get(Constants.IS_INCOMPLETE_PROPERTY_KEY);
            Boolean isIncomplete = value != null && value.equals(INCOMPLETE_ENTITY_VALUE) ? Boolean.TRUE : Boolean.FALSE;

            ret.setTypeName(typeName);
            ret.setGuid(guid);

            String state = (String)properties.get(Constants.STATE_PROPERTY_KEY);
            Id.EntityState entityState = state == null ? null : Id.EntityState.valueOf(state);
            ret.setStatus((entityState == Id.EntityState.DELETED) ? AtlasEntity.Status.DELETED : AtlasEntity.Status.ACTIVE);

            RequestContext context = RequestContext.get();
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();
            if(includeClassifications){
                ret.setClassificationNames(getAllTraitNamesFromAttribute(entityVertex));
            } else if (!includeClassifications && includeClassificationNames) {
                ret.setClassificationNames(getAllTraitNamesFromAttribute(entityVertex));
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

        try {
            if (entityVertex != null) {
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
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapAttributes");
        AtlasType objType = typeRegistry.getType(struct.getTypeName());

        if (!(objType instanceof AtlasStructType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, struct.getTypeName());
        }

        AtlasStructType structType = (AtlasStructType) objType;

        for (AtlasAttribute attribute : structType.getAllAttributes().values()) {
            Object attrValue = mapVertexToAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo, includeReferences);

            struct.setAttribute(attribute.getName(), attrValue);
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void mapBusinessAttributes(AtlasVertex entityVertex, AtlasEntity entity) throws AtlasBaseException {
        entity.setBusinessAttributes(getBusinessMetadata(entityVertex));
    }

    public List<AtlasClassification> getAllClassifications(AtlasVertex entityVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getAllClassifications");

        if(LOG.isDebugEnabled()){
            LOG.debug("Performing getAllClassifications");
        }
        List<AtlasClassification> ret   = new ArrayList<>();
        Iterable                  edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL).edges();

        if (edges != null) {
            Iterator<AtlasEdge> iterator = edges.iterator();

            while (iterator.hasNext()) {
                AtlasEdge           classificationEdge   = iterator.next();
                AtlasVertex         classificationVertex = classificationEdge != null ? classificationEdge.getInVertex() : null;
                AtlasClassification classification       = toAtlasClassification(classificationVertex);

                if (classification != null) {
                    ret.add(classification);
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
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
        return mapVertexToAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo, includeReferences, false);
    }

    private Object mapVertexToAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo, boolean includeReferences, boolean ignoreInactive) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapVertexToAttribute");
        try {
            Object    ret                = null;
            AtlasType attrType           = attribute.getAttributeType();
            String    edgeLabel          = attribute.getRelationshipEdgeLabel();
            boolean   isOwnedAttribute   = attribute.isOwnedRef();
            AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Mapping vertex {} to atlas entity {}.{}", entityVertex, attribute.getDefinedInDef().getName(), attribute.getName());
            }

            switch (attrType.getTypeCategory()) {
                case PRIMITIVE:
                    ret = mapVertexToPrimitive(entityVertex, attribute.getVertexPropertyName(), attribute.getAttributeDef());
                    break;
                case ENUM:
                    ret = AtlasGraphUtilsV2.getEncodedProperty(entityVertex, attribute.getVertexPropertyName(), Object.class);
                    break;
                case STRUCT:
                    edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                    ret = mapVertexToStruct(entityVertex, edgeLabel, null, entityExtInfo, isMinExtInfo);
                    break;
                case OBJECT_ID_TYPE:
                    if (includeReferences) {
                        if (attribute.getDefinedInType().getTypeCategory() == TypeCategory.STRUCT) {
                            //Struct attribute having ObjectId as type
                            edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                        }
                        ret = attribute.getAttributeDef().isSoftReferenced() ? mapVertexToObjectIdForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo) :
                                mapVertexToObjectId(entityVertex, edgeLabel, null, entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo);
                    } else {
                        ret = null;
                    }
                    break;
                case ARRAY: {
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
                            ret = mapVertexToArray(entityVertex, entityExtInfo, isOwnedAttribute, attribute, isMinExtInfo, includeReferences, ignoreInactive);
                        }
                    }
                }
                break;
                case MAP: {
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
            Map<String, Object> currentMap = getReferenceMap(entityVertex, attribute);

            if (MapUtils.isNotEmpty(currentMap)) {
                ret = new HashMap<>();

                for (Map.Entry<String, Object> entry : currentMap.entrySet()) {
                    String mapKey    = entry.getKey();
                    Object keyValue  = entry.getValue();
                    Object mapValue  = mapVertexToCollectionEntry(entityVertex, mapValueType, keyValue, attribute.getRelationshipEdgeLabel(),
                                                                  entityExtInfo, isOwnedAttribute, attribute.getRelationshipEdgeDirection(), isMinExtInfo, includeReferences);
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
                                          boolean includeReferences, boolean ignoreInactive) throws AtlasBaseException {

        AtlasArrayType arrayType        = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      arrayElementType = arrayType.getElementType();
        List<Object>   arrayElements    = getArrayElementsProperty(arrayElementType, entityVertex, attribute);

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

            Object arrValue = mapVertexToCollectionEntry(entityVertex, arrayElementType, element, edgeLabel,
                                                         entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo, includeReferences);

            if (arrValue != null) {
                arrValues.add(arrValue);
            }
        }

        return arrValues;
    }

    private Object mapVertexToCollectionEntry(AtlasVertex entityVertex, AtlasType arrayElement, Object value,
                                              String edgeLabel, AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute,
                                              AtlasRelationshipEdgeDirection edgeDirection, final boolean isMinExtInfo, boolean includeReferences) throws AtlasBaseException {
        Object ret = null;

        switch (arrayElement.getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
                ret = value;
                break;

            case CLASSIFICATION:
                break;

            case STRUCT:
                ret = mapVertexToStruct(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo, isMinExtInfo);
                break;

            case OBJECT_ID_TYPE:
                ret = includeReferences ? mapVertexToObjectId(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo) : null;
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

    private AtlasObjectId mapVertexToObjectId(AtlasVertex entityVertex, String edgeLabel, AtlasEdge edge,
                                              AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute,
                                              AtlasRelationshipEdgeDirection edgeDirection, final boolean isMinExtInfo) throws AtlasBaseException {
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
                        ret.setAttributes(new HashMap<>());
                    }
                    ret.getAttributes().put("relationshipAttributes", relationshipAttributes);
                }
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

                String typeName = getTypeName(referenceVertex);

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

    public Object getVertexAttributePreFetchCache(AtlasVertex vertex, AtlasAttribute attribute, Map<String, Object> properties) throws AtlasBaseException {
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
                return mapVertexToAttribute(vertex, attribute, null, false);
            }

            return null;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private Object getVertexAttributeIgnoreInactive(AtlasVertex vertex, AtlasAttribute attribute) throws AtlasBaseException {
        return vertex != null && attribute != null ? mapVertexToAttribute(vertex, attribute, null, false, true, true) : null;
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
}
