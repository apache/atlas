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
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TimeBoundary;
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
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasArrayType;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_CONFIDENCE;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_CREATED_BY;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_DESCRIPTION;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_EXPRESSION;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_SOURCE;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_STATUS;
import static org.apache.atlas.glossary.GlossaryUtils.TERM_ASSIGNMENT_ATTR_STEWARD;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_ENTITY_GUID;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_LABEL;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_VALIDITY_PERIODS_KEY;
import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;
import static org.apache.atlas.repository.graph.GraphHelper.EDGE_LABEL_PREFIX;
import static org.apache.atlas.repository.graph.GraphHelper.addToPropagatedTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getAdjacentEdgesByLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getAllClassificationEdges;
import static org.apache.atlas.repository.graph.GraphHelper.getAllTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getAssociatedEntityVertex;
import static org.apache.atlas.repository.graph.GraphHelper.getBlockedClassificationIds;
import static org.apache.atlas.repository.graph.GraphHelper.getArrayElementsProperty;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEntityStatus;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationVertices;
import static org.apache.atlas.repository.graph.GraphHelper.getGuid;
import static org.apache.atlas.repository.graph.GraphHelper.getIncomingEdgesByLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getPrimitiveMap;
import static org.apache.atlas.repository.graph.GraphHelper.getReferenceMap;
import static org.apache.atlas.repository.graph.GraphHelper.getOutGoingEdgesByLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagateTags;
import static org.apache.atlas.repository.graph.GraphHelper.getRelationshipGuid;
import static org.apache.atlas.repository.graph.GraphHelper.getRemovePropagations;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.graph.GraphHelper.isPropagationEnabled;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromVertex;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.BOTH;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;

@Component
public class EntityGraphRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphRetriever.class);

    private static final String GLOSSARY_TERM_DISPLAY_NAME_ATTR = "AtlasGlossaryTerm.name";
    public  static final String TERM_RELATION_NAME              = "AtlasGlossarySemanticAssignment";

    public static final String NAME           = "name";
    public static final String DISPLAY_NAME   = "displayName";
    public static final String DESCRIPTION    = "description";
    public static final String OWNER          = "owner";
    public static final String CREATE_TIME    = "createTime";
    public static final String QUALIFIED_NAME = "qualifiedName";

    private static final TypeReference<List<TimeBoundary>> TIME_BOUNDARIES_LIST_TYPE = new TypeReference<List<TimeBoundary>>() {};
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final AtlasTypeRegistry typeRegistry;

    private final boolean ignoreRelationshipAttr;

    @Inject
    public EntityGraphRetriever(AtlasTypeRegistry typeRegistry) {
        this(typeRegistry, false);
    }

    public EntityGraphRetriever(AtlasTypeRegistry typeRegistry, boolean ignoreRelationshipAttr) {
        this.typeRegistry = typeRegistry;
        this.ignoreRelationshipAttr = ignoreRelationshipAttr;
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

    public Object getEntityAttribute(AtlasVertex entityVertex, AtlasAttribute attribute) {
        Object ret = null;

        try {
            ret = getVertexAttribute(entityVertex, attribute);
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

            ret = new AtlasObjectId(entityVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class), typeName, uniqueAttributes);
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

            ret = new AtlasObjectId(entity.getGuid(), entity.getTypeName(), uniqueAttributes);
        }

        return ret;
    }

    public AtlasClassification toAtlasClassification(AtlasVertex classificationVertex) throws AtlasBaseException {
        AtlasClassification ret = new AtlasClassification(getTypeName(classificationVertex));

        ret.setEntityGuid(AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_GUID, String.class));
        ret.setEntityStatus(getClassificationEntityStatus(classificationVertex));
        ret.setPropagate(isPropagationEnabled(classificationVertex));
        ret.setRemovePropagationsOnEntityDelete(getRemovePropagations(classificationVertex));

        String strValidityPeriods = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_VALIDITY_PERIODS_KEY, String.class);

        if (strValidityPeriods != null) {
            ret.setValidityPeriods(AtlasJson.fromJson(strValidityPeriods, TIME_BOUNDARIES_LIST_TYPE));
        }

        mapAttributes(classificationVertex, ret, null);

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
        AtlasVertex ret = AtlasGraphUtilsV2.findByGuid(guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
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
                    AtlasVertex vertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(entityType, uniqAttributes);

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

    private AtlasVertex getEntityVertex(AtlasObjectId objId) throws AtlasBaseException {
        AtlasVertex ret = null;

        if (! AtlasTypeUtil.isValid(objId)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, objId.toString());
        }

        if (AtlasTypeUtil.isAssignedGuid(objId)) {
            ret = AtlasGraphUtilsV2.findByGuid(objId.getGuid());
        } else {
            AtlasEntityType     entityType     = typeRegistry.getEntityTypeByName(objId.getTypeName());
            Map<String, Object> uniqAttributes = objId.getUniqueAttributes();

            ret = AtlasGraphUtilsV2.getVertexByUniqueAttributes(entityType, uniqAttributes);
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

            mapAttributes(entityVertex, entity, entityExtInfo, isMinExtInfo);

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

    private AtlasEntityHeader mapVertexToAtlasEntityHeader(AtlasVertex entityVertex, Set<String> attributes) throws AtlasBaseException {
        AtlasEntityHeader ret = new AtlasEntityHeader();

        String typeName = entityVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
        String guid     = entityVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);

        ret.setTypeName(typeName);
        ret.setGuid(guid);
        ret.setStatus(GraphHelper.getStatus(entityVertex));
        ret.setClassificationNames(getAllTraitNames(entityVertex));

        List<AtlasTermAssignmentHeader> termAssignmentHeaders = mapAssignedTerms(entityVertex);
        ret.setMeanings(termAssignmentHeaders);
        ret.setMeaningNames(termAssignmentHeaders.stream().map(AtlasTermAssignmentHeader::getDisplayText).collect(Collectors.toList()));

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

        if (entityType != null) {
            for (AtlasAttribute headerAttribute : entityType.getHeaderAttributes().values()) {
                Object attrValue = getVertexAttribute(entityVertex, headerAttribute);

                if (attrValue != null) {
                    ret.setAttribute(headerAttribute.getName(), attrValue);
                }
            }

            Object name        = ret.getAttribute(NAME);
            Object displayText = name != null ? name : ret.getAttribute(QUALIFIED_NAME);

            if (displayText != null) {
                ret.setDisplayText(displayText.toString());
            }

            if (CollectionUtils.isNotEmpty(attributes)) {
                for (String attrName : attributes) {
                    String nonQualifiedAttrName = toNonQualifiedName(attrName);
                    if (ret.hasAttribute(attrName)) {
                        continue;
                    }

                    AtlasAttribute attribute = entityType.getAttribute(nonQualifiedAttrName);

                    if (attribute == null) {
                        attribute = entityType.getRelationshipAttribute(nonQualifiedAttrName, null);
                    }

                    Object attrValue = getVertexAttribute(entityVertex, attribute);

                    if (attrValue != null) {
                        ret.setAttribute(nonQualifiedAttrName, attrValue);
                    }
                }
            }
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

    private AtlasEntity mapSystemAttributes(AtlasVertex entityVertex, AtlasEntity entity) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping system attributes for type {}", entity.getTypeName());
        }

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

        entity.setProvenanceType(GraphHelper.getProvenanceType(entityVertex));

        return entity;
    }

    private void mapAttributes(AtlasVertex entityVertex, AtlasStruct struct, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        mapAttributes(entityVertex, struct, entityExtInfo, false);
    }

    private void mapAttributes(AtlasVertex entityVertex, AtlasStruct struct, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasType objType = typeRegistry.getType(struct.getTypeName());

        if (!(objType instanceof AtlasStructType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, struct.getTypeName());
        }

        AtlasStructType structType = (AtlasStructType) objType;

        for (AtlasAttribute attribute : structType.getAllAttributes().values()) {
            Object attrValue = mapVertexToAttribute(entityVertex, attribute, entityExtInfo, isMinExtInfo);

            struct.setAttribute(attribute.getName(), attrValue);
        }
    }

    public List<AtlasClassification> getAllClassifications(AtlasVertex entityVertex) throws AtlasBaseException {
        List<AtlasClassification> ret   = new ArrayList<>();
        Iterable                  edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL).edges();

        if (edges != null) {
            Iterator<AtlasEdge> iterator = edges.iterator();

            while (iterator.hasNext()) {
                AtlasEdge edge = iterator.next();

                if (edge != null) {
                    ret.add(toAtlasClassification(edge.getInVertex()));
                }
            }
        }

        return ret;
    }

    public List<AtlasTermAssignmentHeader> mapAssignedTerms(AtlasVertex entityVertex) throws AtlasBaseException {
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

    private void mapClassifications(AtlasVertex entityVertex, AtlasEntity entity) throws AtlasBaseException {
        List<AtlasEdge> edges = getAllClassificationEdges(entityVertex);

        if (CollectionUtils.isNotEmpty(edges)) {
            List<AtlasClassification> allClassifications                 = new ArrayList<>();

            for (AtlasEdge edge : edges) {
                AtlasVertex classificationVertex = edge.getInVertex();

                allClassifications.add(toAtlasClassification(classificationVertex));
            }

            entity.setClassifications(allClassifications);
        }
    }

    private Object mapVertexToAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) throws AtlasBaseException {
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
                ret = mapVertexToStruct(entityVertex, edgeLabel, null, entityExtInfo, isMinExtInfo);
                break;
            case OBJECT_ID_TYPE:
                if(attribute.getAttributeDef().isSoftReferenced()) {
                    ret = mapVertexToObjectIdForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                } else {
                	ret = mapVertexToObjectId(entityVertex, edgeLabel, null, entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo);
				}
                break;
            case ARRAY:
                if(attribute.getAttributeDef().isSoftReferenced()) {
                    ret = mapVertexToArrayForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                } else {
                	ret = mapVertexToArray(entityVertex, entityExtInfo, isOwnedAttribute, attribute, isMinExtInfo);
				}
                break;
            case MAP:
                if(attribute.getAttributeDef().isSoftReferenced()) {
                    ret = mapVertexToMapForSoftRef(entityVertex, attribute, entityExtInfo, isMinExtInfo);
                } else {
                	ret = mapVertexToMap(entityVertex, entityExtInfo, isOwnedAttribute, attribute, isMinExtInfo);
				}
                break;
            case CLASSIFICATION:
                // do nothing
                break;
        }

        return ret;
    }

    private Map<String, AtlasObjectId> mapVertexToMapForSoftRef(AtlasVertex entityVertex,  AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) {
        Map<String, AtlasObjectId> ret        = null;
        Map                        softRefVal = entityVertex.getProperty(attribute.getVertexPropertyName(), Map.class);

        if (MapUtils.isNotEmpty(softRefVal)) {
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

        if (CollectionUtils.isNotEmpty(softRefVal)) {
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
                                               boolean isOwnedAttribute, AtlasAttribute attribute, final boolean isMinExtInfo) throws AtlasBaseException {

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
                                                                  entityExtInfo, isOwnedAttribute, attribute.getRelationshipEdgeDirection(), isMinExtInfo);
                    if (mapValue != null) {
                        ret.put(mapKey, mapValue);
                    }
                }
            }
        } else {
            ret = getPrimitiveMap(entityVertex, attribute.getVertexPropertyName());
        }

        if (MapUtils.isEmpty(ret)) {
            ret = null;
        }

        return ret;
    }

    private List<Object> mapVertexToArray(AtlasVertex entityVertex, AtlasEntityExtInfo entityExtInfo,
                                          boolean isOwnedAttribute, AtlasAttribute attribute, final boolean isMinExtInfo) throws AtlasBaseException {

        AtlasArrayType arrayType        = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      arrayElementType = arrayType.getElementType();
        List<Object>   arrayElements    = getArrayElementsProperty(arrayElementType, entityVertex, attribute);

        if (CollectionUtils.isEmpty(arrayElements)) {
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping array attribute {} for vertex {}", arrayElementType.getTypeName(), entityVertex);
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

            Object arrValue = mapVertexToCollectionEntry(entityVertex, arrayElementType, element, edgeLabel,
                                                         entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo);

            if (arrValue != null) {
                arrValues.add(arrValue);
            }
        }

        return arrValues;
    }

    private Object mapVertexToCollectionEntry(AtlasVertex entityVertex, AtlasType arrayElement, Object value,
                                              String edgeLabel, AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute,
                                              AtlasRelationshipEdgeDirection edgeDirection, final boolean isMinExtInfo) throws AtlasBaseException {
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
                ret = mapVertexToObjectId(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo, isOwnedAttribute, edgeDirection, isMinExtInfo);
                break;

            default:
                break;
        }

        return ret;
    }

    public Object mapVertexToPrimitive(AtlasElement entityVertex, final String vertexPropertyName, AtlasAttributeDef attrDef) {
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
            }
        }

        return ret;
    }

    private AtlasStruct mapVertexToStruct(AtlasVertex entityVertex, String edgeLabel, AtlasEdge edge, AtlasEntityExtInfo entityExtInfo, final boolean isMinExtInfo) throws AtlasBaseException {
        AtlasStruct ret = null;

        if (edge == null) {
            edge = graphHelper.getEdgeForLabel(entityVertex, edgeLabel);
        }

        if (GraphHelper.elementExists(edge)) {
            final AtlasVertex referenceVertex = edge.getInVertex();
            ret = new AtlasStruct(getTypeName(referenceVertex));

            mapAttributes(referenceVertex, ret, entityExtInfo, isMinExtInfo);
        }

        return ret;
    }

    private Object getVertexAttribute(AtlasVertex vertex, AtlasAttribute attribute) throws AtlasBaseException {
        return vertex != null && attribute != null ? mapVertexToAttribute(vertex, attribute, null, false) : null;
    }

    private void mapRelationshipAttributes(AtlasVertex entityVertex, AtlasEntity entity, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, entity.getTypeName());
        }

        for (String attributeName : entityType.getRelationshipAttributes().keySet()) {
            mapVertexToRelationshipAttribute(entityVertex, entityType, attributeName, entity, entityExtInfo, isMinExtInfo);
        }
    }

    private Object mapVertexToRelationshipAttribute(AtlasVertex entityVertex, AtlasEntityType entityType, String attributeName, AtlasEntity entity, AtlasEntityExtInfo entityExtInfo, boolean isMinExtInfo) throws AtlasBaseException {
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

        if (ret != null) {
            entity.setRelationshipAttribute(attributeName, ret);

            if (attributeEndDef.getIsLegacyAttribute() && !entity.hasAttribute(attributeName)) {
                entity.setAttribute(attributeName, toAtlasObjectId(ret));
            }
        }

        return ret;
    }

    private Object toAtlasObjectId(Object obj) {
        final Object ret;

        if (obj instanceof AtlasObjectId) {
            ret = new AtlasObjectId((AtlasObjectId) obj);
        } else if (obj instanceof Collection) {
            List list = new ArrayList();

            for (Object elem : (Collection) obj) {
                list.add(toAtlasObjectId(elem));
            }

            ret = list;
        } else if (obj instanceof Map) {
            Map map = new HashMap();

            for (Object key : ((Map) obj).keySet()) {
                map.put(key, toAtlasObjectId(((Map) obj).get(key)));
            }

            ret = map;
        } else {
            ret = obj;
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
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);
        Object          ret        = null;

        if (entityType != null) {
            ret = getVertexAttribute(entityVertex, entityType.getAttribute(NAME));

            if (ret == null) {
                ret = getVertexAttribute(entityVertex, entityType.getAttribute(DISPLAY_NAME));
            }

            if (ret == null) {
                ret = getVertexAttribute(entityVertex, entityType.getAttribute(QUALIFIED_NAME));
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

        relationship.setStatus(GraphHelper.getEdgeStatus(edge));

        AtlasVertex end1Vertex = edge.getOutVertex();
        AtlasVertex end2Vertex = edge.getInVertex();

        relationship.setEnd1(new AtlasObjectId(getGuid(end1Vertex), getTypeName(end1Vertex)));
        relationship.setEnd2(new AtlasObjectId(getGuid(end2Vertex), getTypeName(end2Vertex)));

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
        List<AtlasVertex>        classificationVertices    = getClassificationVertices(edge);
        List<String>             blockedClassificationIds  = getBlockedClassificationIds(edge);
        AtlasRelationship        relationship              = relationshipWithExtInfo.getRelationship();
        Set<AtlasClassification> propagatedClassifications = new HashSet<>();
        Set<AtlasClassification> blockedClassifications    = new HashSet<>();

        for (AtlasVertex classificationVertex : classificationVertices) {
            String              classificationId = classificationVertex.getIdForDisplay();
            AtlasClassification classification   = toAtlasClassification(classificationVertex);
            String              entityGuid       = classification.getEntityGuid();

            if (blockedClassificationIds.contains(classificationId)) {
                blockedClassifications.add(classification);
            } else {
                propagatedClassifications.add(classification);
            }

            // add entity headers to referred entities
            if (extendedInfo) {
                addToReferredEntities(relationshipWithExtInfo, entityGuid);
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
}
