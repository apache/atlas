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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipDefStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

@Component
public class AtlasRelationshipStoreV1 implements AtlasRelationshipStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipStoreV1.class);

    private final AtlasTypeRegistry    typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final GraphHelper          graphHelper = GraphHelper.getInstance();

    @Inject
    public AtlasRelationshipStoreV1(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry    = typeRegistry;
        this.entityRetriever = new EntityGraphRetriever(typeRegistry);
    }

    @Override
    @GraphTransaction
    public AtlasRelationship create(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> create({})", relationship);
        }

        validateRelationship(relationship);

        String      relationshipLabel = relationship.getRelationshipLabel();
        AtlasVertex end1Vertex        = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex end2Vertex        = getVertexFromEndPoint(relationship.getEnd2());

        AtlasRelationship ret;

        // create relationship between two vertex
        try {
            AtlasEdge relationshipEdge = getRelationshipEdge(end1Vertex, end2Vertex, relationshipLabel);

            if (relationshipEdge == null) {
                relationshipEdge = createRelationEdge(end1Vertex, end2Vertex, relationship);

                AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

                if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
                    for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                        String attrName  = attr.getName();
                        Object attrValue = relationship.getAttribute(attrName);

                        AtlasGraphUtilsV1.setProperty(relationshipEdge, attr.getVertexPropertyName(), attrValue);
                    }
                }

                // create legacy edges if mentioned in relationDef
                createLegacyEdges(relationType.getRelationshipDef(), end1Vertex, end2Vertex);

                ret = mapEdgeToAtlasRelationship(relationshipEdge);

            } else {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_EXISTS, relationship.getTypeName(),
                                             relationship.getEnd1().getGuid(), relationship.getEnd2().getGuid());
            }
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== create({}): {}", relationship, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship update(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> update({})", relationship);
        }

        AtlasRelationship ret = null;

        // TODO: update(relationship) implementation

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== update({}): {}", relationship, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship getById(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getById({})", guid);
        }

        AtlasRelationship ret;

        try {
            AtlasEdge edge = graphHelper.getEdgeForGUID(guid);

            ret = mapEdgeToAtlasRelationship(edge);
        } catch (EntityNotFoundException ex) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getById({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public void deleteById(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteById({})", guid);
        }

        // TODO: deleteById(guid) implementation

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteById({}): {}", guid);
        }
    }

    private void validateRelationship(AtlasRelationship relationship) throws AtlasBaseException {
        if (relationship == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "AtlasRelationship is null");
        }

        String                relationshipName = relationship.getTypeName();
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipName);

        if (relationshipType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "unknown relationship '" + relationshipName + "'");
        }

        AtlasObjectId end1 = relationship.getEnd1();

        if (end1 == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "end1 is null");
        }

        String end1TypeName = end1.getTypeName();

        if (StringUtils.isBlank(end1TypeName)) {
            end1TypeName = AtlasGraphUtilsV1.getTypeNameFromGuid(end1.getGuid());
        }

        if (!relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end1TypeName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName,
                                         relationshipType.getEnd1Type().getTypeName(), end1TypeName);
        }

        AtlasObjectId end2 = relationship.getEnd2();

        if (end2 == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "end2 is null");
        }

        String end2TypeName = end2.getTypeName();

        if (StringUtils.isBlank(end2TypeName)) {
            end2TypeName = AtlasGraphUtilsV1.getTypeNameFromGuid(end2.getGuid());
        }

        if (!relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end2TypeName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName,
                                         relationshipType.getEnd2Type().getTypeName(), end2TypeName);
        }

        validateEnd(end1);

        validateEnd(end2);
    }

    private void validateEnd(AtlasObjectId end) throws AtlasBaseException {
        String              guid             = end.getGuid();
        String              typeName         = end.getTypeName();
        Map<String, Object> uniqueAttributes = end.getUniqueAttributes();
        AtlasVertex         endVertex        = AtlasGraphUtilsV1.findByGuid(guid);

        if (!AtlasTypeUtil.isValidGuid(guid) || endVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        } else if (MapUtils.isNotEmpty(uniqueAttributes))  {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (AtlasGraphUtilsV1.findByUniqueAttributes(entityType, uniqueAttributes) == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, uniqueAttributes.toString());
            }
        }
    }

    private AtlasEdge getRelationshipEdge(AtlasVertex fromVertex, AtlasVertex toVertex, String relationshipLabel) {
        AtlasEdge ret = graphHelper.getEdgeForLabel(fromVertex, relationshipLabel);

        if (ret != null) {
            AtlasVertex inVertex = ret.getInVertex();

            if (inVertex != null) {
                if (!StringUtils.equals(AtlasGraphUtilsV1.getIdFromVertex(inVertex),
                                       AtlasGraphUtilsV1.getIdFromVertex(toVertex))) {
                    ret = null;
                }
            }
        }

        return ret;
    }

    private int getRelationVersion(AtlasRelationship relationship) {
        Long ret = relationship != null ? relationship.getVersion() : null;

        return (ret != null) ? ret.intValue() : 0;
    }

    private AtlasVertex getVertexFromEndPoint(AtlasObjectId endPoint) {
        AtlasVertex ret = null;

        if (StringUtils.isNotEmpty(endPoint.getGuid())) {
            ret = AtlasGraphUtilsV1.findByGuid(endPoint.getGuid());

        } else if (StringUtils.isNotEmpty(endPoint.getTypeName()) && MapUtils.isNotEmpty(endPoint.getUniqueAttributes())) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(endPoint.getTypeName());

            ret = AtlasGraphUtilsV1.findByUniqueAttributes(entityType, endPoint.getUniqueAttributes());
        }

        return ret;
    }

    private void createLegacyEdges(AtlasRelationshipDef relationshipDef, AtlasVertex fromVertex, AtlasVertex toVertex) throws RepositoryException {
        if (relationshipDef != null) {
            AtlasRelationshipEndDef endDef1 = relationshipDef.getEndDef1();
            AtlasRelationshipEndDef endDef2 = relationshipDef.getEndDef2();

            if (endDef1 != null && endDef1.hasLegacyRelation()) {
                graphHelper.getOrCreateEdge(fromVertex, toVertex, endDef1.getLegacyLabel());
            }

            if (endDef2 != null && endDef2.hasLegacyRelation()) {
                graphHelper.getOrCreateEdge(toVertex, fromVertex, endDef2.getLegacyLabel());
            }
        }
    }

    private AtlasEdge createRelationEdge(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) throws RepositoryException {
        AtlasEdge ret = graphHelper.getOrCreateEdge(fromVertex, toVertex, relationship.getRelationshipLabel());

        // add additional properties to edge
        if (ret != null) {
            final String guid = UUID.randomUUID().toString();

            AtlasGraphUtilsV1.setProperty(ret, Constants.ENTITY_TYPE_PROPERTY_KEY, relationship.getTypeName());
            AtlasGraphUtilsV1.setProperty(ret, Constants.GUID_PROPERTY_KEY, guid);
            AtlasGraphUtilsV1.setProperty(ret, Constants.VERSION_PROPERTY_KEY, getRelationVersion(relationship));
        }

        return ret;
    }

    private AtlasRelationship mapEdgeToAtlasRelationship(AtlasEdge edge) throws AtlasBaseException {
        AtlasRelationship ret = new AtlasRelationship();

        mapSystemAttributes(edge, ret);

        mapAttributes(edge, ret);

        return ret;
    }

    private AtlasRelationship mapSystemAttributes(AtlasEdge edge, AtlasRelationship relationship) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping system attributes for relationship");
        }

        relationship.setGuid(GraphHelper.getGuid(edge));
        relationship.setTypeName(GraphHelper.getTypeName(edge));

        relationship.setCreatedBy(GraphHelper.getCreatedByAsString(edge));
        relationship.setUpdatedBy(GraphHelper.getModifiedByAsString(edge));

        relationship.setCreateTime(new Date(GraphHelper.getCreatedTime(edge)));
        relationship.setUpdateTime(new Date(GraphHelper.getModifiedTime(edge)));

        relationship.setVersion(GraphHelper.getVersion(edge).longValue());
        relationship.setStatus(GraphHelper.getEdgeStatus(edge));

        AtlasVertex end1Vertex = edge.getOutVertex();
        AtlasVertex end2Vertex = edge.getInVertex();

        relationship.setEnd1(new AtlasObjectId(GraphHelper.getGuid(end1Vertex), GraphHelper.getTypeName(end1Vertex)));
        relationship.setEnd2(new AtlasObjectId(GraphHelper.getGuid(end2Vertex), GraphHelper.getTypeName(end2Vertex)));

        return relationship;
    }

    private void mapAttributes(AtlasEdge edge, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasType objType = typeRegistry.getType(relationship.getTypeName());

        if (!(objType instanceof AtlasRelationshipType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, relationship.getTypeName());
        }

        AtlasRelationshipType relationshipType = (AtlasRelationshipType) objType;

        for (AtlasAttribute attribute : relationshipType.getAllAttributes().values()) {
            // mapping only primitive attributes
            Object attrValue = entityRetriever.mapVertexToPrimitive(edge, attribute.getQualifiedName(),
                                                                    attribute.getAttributeDef());

            relationship.setAttribute(attribute.getName(), attrValue);
        }
    }
}