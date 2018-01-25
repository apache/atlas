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

import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
import static org.apache.atlas.repository.graphdb.AtlasEdgeDirection.BOTH;
import static org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1.getState;
import static org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1.getTypeName;

@Component
public class AtlasRelationshipStoreV1 implements AtlasRelationshipStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipStoreV1.class);
    private static final Long DEFAULT_RELATIONSHIP_VERSION = 0L;

    private final AtlasTypeRegistry    typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final DeleteHandlerV1      deleteHandler;
    private final GraphHelper          graphHelper = GraphHelper.getInstance();

    @Inject
    public AtlasRelationshipStoreV1(AtlasTypeRegistry typeRegistry, DeleteHandlerV1 deleteHandler) {
        this.typeRegistry               = typeRegistry;
        this.entityRetriever            = new EntityGraphRetriever(typeRegistry);
        this.deleteHandler              = deleteHandler;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship create(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> create({})", relationship);
        }

        validateRelationship(relationship);

        AtlasVertex       end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex       end2Vertex = getVertexFromEndPoint(relationship.getEnd2());
        AtlasRelationship ret        = createRelationship(relationship, end1Vertex, end2Vertex);

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

        String guid = relationship.getGuid();

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
        }

        validateRelationship(relationship);

        AtlasRelationship ret = updateRelationship(relationship);


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

        AtlasEdge edge = graphHelper.getEdgeForGUID(guid);

        ret = entityRetriever.mapEdgeToAtlasRelationship(edge);

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

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CRUD_INVALID_PARAMS, " empty/null guid");
        }

        AtlasEdge edge = graphHelper.getEdgeForGUID(guid);

        if (edge == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
        }

        if (getState(edge) == DELETED) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_DELETED, guid);
        }

        deleteHandler.deleteRelationships(Collections.singleton(edge));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteById({}): {}", guid);
        }
    }

    public AtlasRelationship getOrCreate(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getOrCreate({})", relationship);
        }

        validateRelationship(relationship);

        AtlasVertex       end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex       end2Vertex = getVertexFromEndPoint(relationship.getEnd2());
        AtlasRelationship ret;

        // check if relationship exists
        AtlasEdge relationshipEdge = getRelationshipEdge(end1Vertex, end2Vertex, relationship);

        if (relationshipEdge != null) {
            ret = entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);

        } else {
            validateRelationship(relationship);
            ret = createRelationship(relationship, end1Vertex, end2Vertex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getOrCreate({}): {}", relationship, ret);
        }

        return ret;
    }

    private AtlasRelationship createRelationship(AtlasRelationship relationship, AtlasVertex end1Vertex, AtlasVertex end2Vertex)
                                                 throws AtlasBaseException {
        AtlasRelationship ret;

        try {
            AtlasEdge relationshipEdge = getRelationshipEdge(end1Vertex, end2Vertex, relationship);

            if (relationshipEdge == null) {
                relationshipEdge = createRelationshipEdge(end1Vertex, end2Vertex, relationship);

                AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

                if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
                    for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                        String attrName           = attr.getName();
                        String attrVertexProperty = attr.getVertexPropertyName();
                        Object attrValue          = relationship.getAttribute(attrName);

                        AtlasGraphUtilsV1.setProperty(relationshipEdge, attrVertexProperty, attrValue);
                    }
                }

                ret = entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);

            } else {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_EXISTS, relationship.getTypeName(),
                                             relationship.getEnd1().getGuid(), relationship.getEnd2().getGuid());
            }
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        return ret;
    }

    private AtlasRelationship updateRelationship(AtlasRelationship relationship) throws AtlasBaseException {
        AtlasEdge relationshipEdge = graphHelper.getEdgeForGUID(relationship.getGuid());

        if (relationshipEdge == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, relationship.getGuid());
        }

        AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

        AtlasGraphUtilsV1.setProperty(relationshipEdge, Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, relationship.getPropagateTags().name());

        if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
            for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                String attrName           = attr.getName();
                String attrVertexProperty = attr.getVertexPropertyName();

                if (relationship.hasAttribute(attrName)) {
                    Object attrValue = relationship.getAttribute(attrName);

                    AtlasGraphUtilsV1.setProperty(relationshipEdge, attrVertexProperty, attrValue);
                }
            }
        }

        return entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);
    }

    private void validateRelationship(AtlasRelationship relationship) throws AtlasBaseException {
        if (relationship == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "AtlasRelationship is null");
        }

        String                relationshipName = relationship.getTypeName();
        String                end1TypeName     = getTypeNameFromObjectId(relationship.getEnd1());
        String                end2TypeName     = getTypeNameFromObjectId(relationship.getEnd2());
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipName);

        if (relationshipType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "unknown relationship type'" + relationshipName + "'");
        }

        if (relationship.getEnd1() == null || relationship.getEnd2() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "end1/end2 is null");
        }

        if (!relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end1TypeName) &&
                !relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end1TypeName)) {

            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName,
                                         relationshipType.getEnd2Type().getTypeName(), end1TypeName);
        }

        if (!relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end2TypeName) &&
                !relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end2TypeName)) {

            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName,
                                         relationshipType.getEnd1Type().getTypeName(), end2TypeName);
        }

        validateEnds(relationship);

        validateAndNormalize(relationship);
    }

    /**
     * Validate the ends of the passed relationship
     * @param relationship
     * @throws AtlasBaseException
     */
    private void validateEnds(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("validateEnds entry relationship:" + relationship);
        }
        List<AtlasObjectId>           ends                 = new ArrayList<>();
        List<AtlasRelationshipEndDef> endDefs              = new ArrayList<>();
        String                        relationshipTypeName = relationship.getTypeName();
        AtlasRelationshipDef          relationshipDef      = typeRegistry.getRelationshipDefByName(relationshipTypeName);

        ends.add(relationship.getEnd1());
        ends.add(relationship.getEnd2());
        endDefs.add(relationshipDef.getEndDef1());
        endDefs.add(relationshipDef.getEndDef2());

        for (int i = 0; i < ends.size(); i++) {
            AtlasObjectId       end              = ends.get(i);
            String              guid             = end.getGuid();
            String              typeName         = end.getTypeName();
            Map<String, Object> uniqueAttributes = end.getUniqueAttributes();
            AtlasVertex         endVertex        = AtlasGraphUtilsV1.findByGuid(guid);

            if (!AtlasTypeUtil.isValidGuid(guid) || endVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);

            } else if (MapUtils.isNotEmpty(uniqueAttributes)) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

                if (AtlasGraphUtilsV1.findByUniqueAttributes(entityType, uniqueAttributes) == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, uniqueAttributes.toString());
                }
            } else {
                // check whether the guid is the correct type
                String vertexTypeName = endVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);

                if (!Objects.equals(vertexTypeName, typeName)) {
                    String attrName = endDefs.get(i).getName();

                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_INVALID_ENDTYPE, attrName, guid, vertexTypeName, typeName);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("validateEnds exit successfully validated relationship:" + relationship);
        }
    }

    private void validateAndNormalize(AtlasRelationship relationship) throws AtlasBaseException {
        List<String> messages = new ArrayList<>();

        if (! AtlasTypeUtil.isValidGuid(relationship.getGuid())) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, relationship.getGuid());
        }

        AtlasRelationshipType type = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

        if (type == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.RELATIONSHIP.name(), relationship.getTypeName());
        }

        type.validateValue(relationship, relationship.getTypeName(), messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CRUD_INVALID_PARAMS, messages);
        }

        type.getNormalizedValue(relationship);
    }

    public AtlasEdge getRelationshipEdge(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) {
        String    relationshipLabel = getRelationshipEdgeLabel(fromVertex, toVertex, relationship);
        AtlasEdge ret               = graphHelper.getEdgeForLabel(fromVertex, relationshipLabel);

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

    private Long getRelationshipVersion(AtlasRelationship relationship) {
        Long ret = relationship != null ? relationship.getVersion() : null;

        return (ret != null) ? ret : DEFAULT_RELATIONSHIP_VERSION;
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

    private AtlasEdge createRelationshipEdge(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) throws RepositoryException {
        String        relationshipLabel = getRelationshipEdgeLabel(fromVertex, toVertex, relationship);
        PropagateTags tagPropagation    = getRelationshipTagPropagation(fromVertex, toVertex, relationship);
        AtlasEdge     ret               = graphHelper.getOrCreateEdge(fromVertex, toVertex, relationshipLabel);

        // map additional properties to relationship edge
        if (ret != null) {
            final String guid = UUID.randomUUID().toString();

            AtlasGraphUtilsV1.setProperty(ret, Constants.ENTITY_TYPE_PROPERTY_KEY, relationship.getTypeName());
            AtlasGraphUtilsV1.setProperty(ret, Constants.GUID_PROPERTY_KEY, guid);
            AtlasGraphUtilsV1.setProperty(ret, Constants.VERSION_PROPERTY_KEY, getRelationshipVersion(relationship));
            AtlasGraphUtilsV1.setProperty(ret, Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, tagPropagation.name());
        }

        return ret;
    }

    private PropagateTags getRelationshipTagPropagation(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) {
        AtlasRelationshipType   relationshipType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());
        AtlasRelationshipEndDef endDef1          = relationshipType.getRelationshipDef().getEndDef1();
        AtlasRelationshipEndDef endDef2          = relationshipType.getRelationshipDef().getEndDef2();
        Set<String>             fromVertexTypes  = getTypeAndAllSuperTypes(getTypeName(fromVertex));
        Set<String>             toVertexTypes    = getTypeAndAllSuperTypes(getTypeName(toVertex));
        PropagateTags           ret              = relationshipType.getRelationshipDef().getPropagateTags();

        // relationshipDef is defined as end1 (hive_db) and end2 (hive_table) and tagPropagation = ONE_TO_TWO
        // relationship edge exists from [hive_table --> hive_db]
        // swap the tagPropagation property for such cases.
        if (fromVertexTypes.contains(endDef2.getType()) && toVertexTypes.contains(endDef1.getType())) {
            if (ret == ONE_TO_TWO) {
                ret = TWO_TO_ONE;
            } else if (ret == TWO_TO_ONE) {
                ret = ONE_TO_TWO;
            }
        }

        return ret;
    }

    private String getRelationshipEdgeLabel(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("validateEnds entry relationship:"+relationship);
        }
        AtlasRelationshipType   relationshipType   = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());
        String                  ret                = relationshipType.getRelationshipDef().getRelationshipLabel();
        AtlasRelationshipEndDef endDef1            = relationshipType.getRelationshipDef().getEndDef1();
        AtlasRelationshipEndDef endDef2            = relationshipType.getRelationshipDef().getEndDef2();
        Set<String>             fromVertexTypes    = getTypeAndAllSuperTypes(AtlasGraphUtilsV1.getTypeName(fromVertex));
        Set<String>             toVertexTypes      = getTypeAndAllSuperTypes(AtlasGraphUtilsV1.getTypeName(toVertex));
        AtlasAttribute          attribute          = null;

        // validate entity type and all its supertypes contains relationshipDefs end type
        // e.g. [hive_process -> hive_table] -> [Process -> DataSet]
        if (fromVertexTypes.contains(endDef1.getType()) && toVertexTypes.contains(endDef2.getType())) {
            String attributeName = endDef1.getName();

            attribute = relationshipType.getEnd1Type().getRelationshipAttribute(attributeName);

        } else if (fromVertexTypes.contains(endDef2.getType()) && toVertexTypes.contains(endDef1.getType())) {
            String attributeName = endDef2.getName();

            attribute = relationshipType.getEnd2Type().getRelationshipAttribute(attributeName);
        }

        if (attribute != null) {
            ret = attribute.getRelationshipEdgeLabel();
        }

        return ret;
    }

    public Set<String> getTypeAndAllSuperTypes(String entityTypeName) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);

        return (entityType != null) ? entityType.getTypeAndAllSuperTypes() : new HashSet<String>();
    }

    private String getTypeNameFromObjectId(AtlasObjectId objectId) {
        String typeName = objectId.getTypeName();

        if (StringUtils.isBlank(typeName)) {
            typeName = AtlasGraphUtilsV1.getTypeNameFromGuid(objectId.getGuid());
        }

        return typeName;
    }

    /**
     * Check whether this vertex has a relationship associated with this relationship type.
     * @param vertex
     * @param relationshipTypeName
     * @return true if found an edge with this relationship type in.
     */
    private boolean vertexHasRelationshipWithType(AtlasVertex vertex, String relationshipTypeName) {
        String relationshipEdgeLabel = getRelationshipEdgeLabel(getTypeName(vertex), relationshipTypeName);
        Iterator<AtlasEdge> iter     = graphHelper.getAdjacentEdgesByLabel(vertex, BOTH, relationshipEdgeLabel);

        return (iter != null) ? iter.hasNext() : false;
    }

    private String getRelationshipEdgeLabel(String typeName, String relationshipTypeName) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);
        AtlasRelationshipDef  relationshipDef  = relationshipType.getRelationshipDef();
        AtlasEntityType       end1Type         = relationshipType.getEnd1Type();
        AtlasEntityType       end2Type         = relationshipType.getEnd2Type();
        Set<String>           vertexTypes      = getTypeAndAllSuperTypes(typeName);
        AtlasAttribute        attribute        = null;

        if (vertexTypes.contains(end1Type.getTypeName())) {
            String attributeName = relationshipDef.getEndDef1().getName();

            attribute = (attributeName != null) ? end1Type.getAttribute(attributeName) : null;
        } else if (vertexTypes.contains(end2Type.getTypeName())) {
            String attributeName = relationshipDef.getEndDef2().getName();

            attribute = (attributeName != null) ? end2Type.getAttribute(attributeName) : null;
        }

        return (attribute != null) ? attribute.getRelationshipEdgeLabel() : null;
    }
}