
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
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
import static org.apache.atlas.repository.graph.GraphHelper.getBlockedClassificationIds;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEntityGuid;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationName;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationVertices;
import static org.apache.atlas.repository.graph.GraphHelper.getOutGoingEdgesByLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagateTags;
import static org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1.getIdFromVertex;
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
        this.typeRegistry    = typeRegistry;
        this.entityRetriever = new EntityGraphRetriever(typeRegistry);
        this.deleteHandler   = deleteHandler;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship create(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> create({})", relationship);
        }

        AtlasVertex end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex end2Vertex = getVertexFromEndPoint(relationship.getEnd2());

        validateRelationship(end1Vertex, end2Vertex, relationship.getTypeName(), relationship.getAttributes());

        AtlasEdge edge = createRelationship(end1Vertex, end2Vertex, relationship);

        AtlasRelationship ret = edge != null ? entityRetriever.mapEdgeToAtlasRelationship(edge) : null;

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

        AtlasEdge   edge       = graphHelper.getEdgeForGUID(guid);
        String      edgeType   = AtlasGraphUtilsV1.getTypeName(edge);
        AtlasVertex end1Vertex = edge.getOutVertex();
        AtlasVertex end2Vertex = edge.getInVertex();

        // update shouldn't change endType
        if (StringUtils.isNotEmpty(relationship.getTypeName()) && !StringUtils.equalsIgnoreCase(edgeType, relationship.getTypeName())) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_UPDATE_TYPE_CHANGE_NOT_ALLOWED, guid, edgeType, relationship.getTypeName());
        }

        // update shouldn't change ends
        if (relationship.getEnd1() != null) {
            String updatedEnd1Guid = relationship.getEnd1().getGuid();

            if (updatedEnd1Guid == null) {
                AtlasVertex updatedEnd1Vertex = getVertexFromEndPoint(relationship.getEnd1());

                updatedEnd1Guid = updatedEnd1Vertex == null ? null : AtlasGraphUtilsV1.getIdFromVertex(updatedEnd1Vertex);
            }

            if (updatedEnd1Guid != null) {
                String end1Guid = AtlasGraphUtilsV1.getIdFromVertex(end1Vertex);

                if (!StringUtils.equalsIgnoreCase(relationship.getEnd1().getGuid(), end1Guid)) {
                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_UPDATE_END_CHANGE_NOT_ALLOWED, edgeType, guid, end1Guid, relationship.getEnd1().getGuid());
                }
            }
        }

        // update shouldn't change ends
        if (relationship.getEnd2() != null) {
            String updatedEnd2Guid = relationship.getEnd2().getGuid();

            if (updatedEnd2Guid == null) {
                AtlasVertex updatedEnd2Vertex = getVertexFromEndPoint(relationship.getEnd2());

                updatedEnd2Guid = updatedEnd2Vertex == null ? null : AtlasGraphUtilsV1.getIdFromVertex(updatedEnd2Vertex);
            }

            if (updatedEnd2Guid != null) {
                String end2Guid = AtlasGraphUtilsV1.getIdFromVertex(end2Vertex);

                if (!StringUtils.equalsIgnoreCase(relationship.getEnd2().getGuid(), end2Guid)) {
                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_UPDATE_END_CHANGE_NOT_ALLOWED, AtlasGraphUtilsV1.getTypeName(edge), guid, end2Guid, relationship.getEnd2().getGuid());
                }
            }
        }


        validateRelationship(end1Vertex, end2Vertex, edgeType, relationship.getAttributes());

        AtlasRelationship ret = updateRelationship(edge, relationship);

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

    @Override
    public AtlasEdge getOrCreate(AtlasVertex end1Vertex, AtlasVertex end2Vertex, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasEdge ret = getRelationshipEdge(end1Vertex, end2Vertex, relationship.getTypeName());

        if (ret == null) {
            validateRelationship(end1Vertex, end2Vertex, relationship.getTypeName(), relationship.getAttributes());

            ret = createRelationship(end1Vertex, end2Vertex, relationship);
        }

        return ret;
    }

    @Override
    public AtlasRelationship getOrCreate(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getOrCreate({})", relationship);
        }

        validateRelationship(relationship);

        AtlasVertex       end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex       end2Vertex = getVertexFromEndPoint(relationship.getEnd2());
        AtlasRelationship ret        = null;

        // check if relationship exists
        AtlasEdge relationshipEdge = getRelationshipEdge(end1Vertex, end2Vertex, relationship.getTypeName());

        if (relationshipEdge == null) {
            validateRelationship(relationship);

            relationshipEdge = createRelationship(end1Vertex, end2Vertex, relationship);
        }

        if (relationshipEdge != null){
            ret = entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getOrCreate({}): {}", relationship, ret);
        }

        return ret;
    }

    private AtlasEdge createRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasEdge ret = null;

        try {
            ret = getRelationshipEdge(end1Vertex, end2Vertex, relationship.getTypeName());

            if (ret == null) {
                ret = createRelationshipEdge(end1Vertex, end2Vertex, relationship);

                AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

                if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
                    for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                        String attrName           = attr.getName();
                        String attrVertexProperty = attr.getVertexPropertyName();
                        Object attrValue          = relationship.getAttribute(attrName);

                        AtlasGraphUtilsV1.setProperty(ret, attrVertexProperty, attrValue);
                    }
                }
            } else {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_EXISTS, relationship.getTypeName(),
                                             AtlasGraphUtilsV1.getIdFromVertex(end1Vertex), AtlasGraphUtilsV1.getIdFromVertex(end2Vertex));
            }
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        return ret;
    }

    private AtlasRelationship updateRelationship(AtlasEdge relationshipEdge, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

        updateTagPropagations(relationshipEdge, relationship);

        if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
            for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                String attrName           = attr.getName();
                String attrVertexProperty = attr.getVertexPropertyName();

                if (relationship.hasAttribute(attrName)) {
                    AtlasGraphUtilsV1.setProperty(relationshipEdge, attrVertexProperty, relationship.getAttribute(attrName));
                }
            }
        }

        return entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);
    }

    private void handleBlockedClassifications(AtlasEdge edge, List<AtlasClassification> blockedPropagatedClassifications) throws AtlasBaseException {
        if (blockedPropagatedClassifications != null) {
            List<AtlasVertex> propagatedClassificationVertices               = getClassificationVertices(edge);
            List<String>      currentClassificationIds                       = getBlockedClassificationIds(edge);
            List<AtlasVertex> currentBlockedPropagatedClassificationVertices = getBlockedClassificationVertices(propagatedClassificationVertices, currentClassificationIds);
            List<AtlasVertex> updatedBlockedPropagatedClassificationVertices = new ArrayList<>();
            List<String>      updatedClassificationIds                       = new ArrayList<>();

            for (AtlasClassification classification : blockedPropagatedClassifications) {
                AtlasVertex classificationVertex = validateBlockedPropagatedClassification(propagatedClassificationVertices, classification);

                // ignore invalid blocked propagated classification
                if (classificationVertex == null) {
                    continue;
                }

                updatedBlockedPropagatedClassificationVertices.add(classificationVertex);

                String classificationId = classificationVertex.getIdForDisplay();

                updatedClassificationIds.add(classificationId);
            }

            addToBlockedClassificationIds(edge, updatedClassificationIds);

            // remove propagated tag for added entry
            List<AtlasVertex> addedBlockedClassifications = (List<AtlasVertex>) CollectionUtils.subtract(updatedBlockedPropagatedClassificationVertices, currentBlockedPropagatedClassificationVertices);

            for (AtlasVertex classificationVertex : addedBlockedClassifications) {
                List<AtlasVertex> removePropagationFromVertices = graphHelper.getPropagatedEntityVertices(classificationVertex);

                deleteHandler.removeTagPropagation(classificationVertex, removePropagationFromVertices);
            }

            // add propagated tag for removed entry
            List<AtlasVertex> removedBlockedClassifications = (List<AtlasVertex>) CollectionUtils.subtract(currentBlockedPropagatedClassificationVertices, updatedBlockedPropagatedClassificationVertices);

            for (AtlasVertex classificationVertex : removedBlockedClassifications) {
                List<AtlasVertex> addPropagationToVertices = graphHelper.getPropagatedEntityVertices(classificationVertex);

                deleteHandler.addTagPropagation(classificationVertex, addPropagationToVertices);
            }
        }
    }

    private List<AtlasVertex> getBlockedClassificationVertices(List<AtlasVertex> classificationVertices, List<String> blockedClassificationIds) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(blockedClassificationIds)) {
            for (AtlasVertex classificationVertex : classificationVertices) {
                String classificationId = classificationVertex.getIdForDisplay();

                if (blockedClassificationIds.contains(classificationId)) {
                    ret.add(classificationVertex);
                }
            }
        }

        return ret;
    }

    // propagated classifications should contain blocked propagated classification
    private AtlasVertex validateBlockedPropagatedClassification(List<AtlasVertex> classificationVertices, AtlasClassification classification) {
        AtlasVertex ret = null;

        for (AtlasVertex vertex : classificationVertices) {
            String classificationName = getClassificationName(vertex);
            String entityGuid         = getClassificationEntityGuid(vertex);

            if (classificationName.equals(classification.getTypeName()) && entityGuid.equals(classification.getEntityGuid())) {
                ret = vertex;
                break;
            }
        }

        return ret;
    }

    private void addToBlockedClassificationIds(AtlasEdge edge, List<String> classificationIds) {
        if (edge != null) {
            if (classificationIds.isEmpty()) {
                edge.removeProperty(Constants.RELATIONSHIPTYPE_BLOCKED_PROPAGATED_CLASSIFICATIONS_KEY);
            } else {
                edge.setListProperty(Constants.RELATIONSHIPTYPE_BLOCKED_PROPAGATED_CLASSIFICATIONS_KEY, classificationIds);
            }
        }
    }

    private void updateTagPropagations(AtlasEdge edge, AtlasRelationship relationship) throws AtlasBaseException {
        PropagateTags oldTagPropagation = getPropagateTags(edge);
        PropagateTags newTagPropagation = relationship.getPropagateTags();

        if (newTagPropagation != oldTagPropagation) {
            List<AtlasVertex>                   currentClassificationVertices = getClassificationVertices(edge);
            Map<AtlasVertex, List<AtlasVertex>> currentClassificationsMap     = getClassificationPropagatedEntitiesMapping(currentClassificationVertices);

            // Update propagation edge
            AtlasGraphUtilsV1.setProperty(edge, Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, newTagPropagation.name());

            List<AtlasVertex>                   updatedClassificationVertices = getClassificationVertices(edge);
            List<AtlasVertex>                   classificationVerticesUnion   = (List<AtlasVertex>) CollectionUtils.union(currentClassificationVertices, updatedClassificationVertices);
            Map<AtlasVertex, List<AtlasVertex>> updatedClassificationsMap     = getClassificationPropagatedEntitiesMapping(classificationVerticesUnion);

            // compute add/remove propagations list
            Map<AtlasVertex, List<AtlasVertex>> addPropagationsMap    = new HashMap<>();
            Map<AtlasVertex, List<AtlasVertex>> removePropagationsMap = new HashMap<>();

            if (MapUtils.isEmpty(currentClassificationsMap) && MapUtils.isNotEmpty(updatedClassificationsMap)) {
                addPropagationsMap.putAll(updatedClassificationsMap);

            } else if (MapUtils.isNotEmpty(currentClassificationsMap) && MapUtils.isEmpty(updatedClassificationsMap)) {
                removePropagationsMap.putAll(currentClassificationsMap);

            } else {
                for (AtlasVertex classificationVertex : updatedClassificationsMap.keySet()) {
                    List<AtlasVertex> currentPropagatingEntities = currentClassificationsMap.containsKey(classificationVertex) ? currentClassificationsMap.get(classificationVertex) : Collections.emptyList();
                    List<AtlasVertex> updatedPropagatingEntities = updatedClassificationsMap.containsKey(classificationVertex) ? updatedClassificationsMap.get(classificationVertex) : Collections.emptyList();

                    List<AtlasVertex> entitiesAdded   = (List<AtlasVertex>) CollectionUtils.subtract(updatedPropagatingEntities, currentPropagatingEntities);
                    List<AtlasVertex> entitiesRemoved = (List<AtlasVertex>) CollectionUtils.subtract(currentPropagatingEntities, updatedPropagatingEntities);

                    if (CollectionUtils.isNotEmpty(entitiesAdded)) {
                        addPropagationsMap.put(classificationVertex, entitiesAdded);
                    }

                    if (CollectionUtils.isNotEmpty(entitiesRemoved)) {
                        removePropagationsMap.put(classificationVertex, entitiesRemoved);
                    }
                }
            }

            for (AtlasVertex classificationVertex : addPropagationsMap.keySet()) {
                deleteHandler.addTagPropagation(classificationVertex, addPropagationsMap.get(classificationVertex));
            }

            for (AtlasVertex classificationVertex : removePropagationsMap.keySet()) {
                deleteHandler.removeTagPropagation(classificationVertex, removePropagationsMap.get(classificationVertex));
            }
        } else {
            // update blocked propagated classifications only if there is no change is tag propagation (don't update both)
            handleBlockedClassifications(edge, relationship.getBlockedPropagatedClassifications());
        }
    }

    private Map<AtlasVertex, List<AtlasVertex>> getClassificationPropagatedEntitiesMapping(List<AtlasVertex> classificationVertices) throws AtlasBaseException {
        Map<AtlasVertex, List<AtlasVertex>> ret = new HashMap<>();

        if (CollectionUtils.isNotEmpty(classificationVertices)) {
            for (AtlasVertex classificationVertex : classificationVertices) {
                String            classificationId      = classificationVertex.getIdForDisplay();
                String            sourceEntityId        = getClassificationEntityGuid(classificationVertex);
                List<AtlasVertex> entitiesPropagatingTo = graphHelper.getImpactedVerticesWithRestrictions(sourceEntityId, classificationId);

                ret.put(classificationVertex, entitiesPropagatingTo);
            }
        }

        return ret;
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

        boolean validEndTypes = false;

        if (relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end2TypeName);
        } else if (relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end2TypeName);
        }

        if (!validEndTypes) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName, relationshipType.getEnd2Type().getTypeName(), end1TypeName);
        }

        validateEnds(relationship);

        validateAndNormalize(relationship);
    }

    private void validateRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, String relationshipName, Map<String, Object> attributes) throws AtlasBaseException {
        String                end1TypeName     = AtlasGraphUtilsV1.getTypeName(end1Vertex);
        String                end2TypeName     = AtlasGraphUtilsV1.getTypeName(end2Vertex);
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipName);

        if (relationshipType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "unknown relationship type'" + relationshipName + "'");
        }

        boolean validEndTypes = false;

        if (relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end2TypeName);
        } else if (relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end2TypeName);
        }

        if (!validEndTypes) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName, relationshipType.getEnd2Type().getTypeName(), end1TypeName);
        }

        List<String>      messages     = new ArrayList<>();
        AtlasRelationship relationship = new AtlasRelationship(relationshipName, attributes);

        relationshipType.validateValue(relationship, relationshipName, messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CRUD_INVALID_PARAMS, messages);
        }

        relationshipType.getNormalizedValue(relationship);
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

    public AtlasEdge getRelationshipEdge(AtlasVertex fromVertex, AtlasVertex toVertex, String relationshipType) {
        String              relationshipLabel = getRelationshipEdgeLabel(fromVertex, toVertex, relationshipType);
        Iterator<AtlasEdge> edgesIterator     = getOutGoingEdgesByLabel(fromVertex, relationshipLabel);
        AtlasEdge           ret               = null;

        while (edgesIterator != null && edgesIterator.hasNext()) {
            AtlasEdge edge = edgesIterator.next();

            if (edge != null) {
                Status status = graphHelper.getStatus(edge);

                if ((status == null || status == ACTIVE) &&
                        StringUtils.equals(getIdFromVertex(edge.getInVertex()), getIdFromVertex(toVertex))) {
                    ret = edge;
                    break;
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

    private AtlasEdge createRelationshipEdge(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) throws RepositoryException, AtlasBaseException {
        String        relationshipLabel = getRelationshipEdgeLabel(fromVertex, toVertex, relationship.getTypeName());
        PropagateTags tagPropagation    = getRelationshipTagPropagation(fromVertex, toVertex, relationship);
        AtlasEdge     ret               = graphHelper.getOrCreateEdge(fromVertex, toVertex, relationshipLabel);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created relationship edge from [{}] --> [{}] using edge label: [{}]", getTypeName(fromVertex), getTypeName(toVertex), relationshipLabel);
        }

        // map additional properties to relationship edge
        if (ret != null) {
            final String guid = UUID.randomUUID().toString();

            AtlasGraphUtilsV1.setProperty(ret, Constants.ENTITY_TYPE_PROPERTY_KEY, relationship.getTypeName());
            AtlasGraphUtilsV1.setProperty(ret, Constants.RELATIONSHIP_GUID_PROPERTY_KEY, guid);
            AtlasGraphUtilsV1.setProperty(ret, Constants.VERSION_PROPERTY_KEY, getRelationshipVersion(relationship));
            AtlasGraphUtilsV1.setProperty(ret, Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, tagPropagation.name());

            // blocked propagated classifications
            handleBlockedClassifications(ret, relationship.getBlockedPropagatedClassifications());

            // propagate tags
            entityRetriever.addTagPropagation(ret, tagPropagation);
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

    private String getRelationshipEdgeLabel(AtlasVertex fromVertex, AtlasVertex toVertex, String relationshipTypeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRelationshipEdgeLabel({})", relationshipTypeName);
        }

        AtlasRelationshipType   relationshipType   = typeRegistry.getRelationshipTypeByName(relationshipTypeName);
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
        Iterator<AtlasEdge> iter     = graphHelper.getAdjacentEdgesByLabel(vertex, AtlasEdgeDirection.BOTH, relationshipEdgeLabel);

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