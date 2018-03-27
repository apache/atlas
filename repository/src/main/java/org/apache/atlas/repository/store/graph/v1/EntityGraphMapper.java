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
package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TimeBoundary;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.PropagationState;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.model.TypeCategory.CLASSIFICATION;
import static org.apache.atlas.model.instance.AtlasRelatedObjectId.KEY_RELATIONSHIP_ATTRIBUTES;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.PARTIAL_UPDATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.SET;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_EDGE_STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_ENTITY_GUID;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_LABEL;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.addToPropagatedTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEdge;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEdgeState;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationVertex;
import static org.apache.atlas.repository.graph.GraphHelper.getPropagatedClassificationEdge;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeNames;
import static org.apache.atlas.repository.graph.GraphHelper.isPropagationEnabled;
import static org.apache.atlas.repository.graph.GraphHelper.isRelationshipEdge;
import static org.apache.atlas.repository.graph.GraphHelper.removeFromPropagatedTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.string;
import static org.apache.atlas.repository.graph.GraphHelper.updateModificationMetadata;
import static org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1.getIdFromVertex;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;

@Component
public class EntityGraphMapper {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphMapper.class);

    private final GraphHelper               graphHelper = GraphHelper.getInstance();
    private final AtlasGraph                graph;
    private final DeleteHandlerV1           deleteHandler;
    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasRelationshipStore    relationshipStore;
    private final AtlasEntityChangeNotifier entityChangeNotifier;
    private final AtlasInstanceConverter    instanceConverter;
    private final EntityGraphRetriever      entityRetriever;

    @Inject
    public EntityGraphMapper(DeleteHandlerV1 deleteHandler, AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph,
                             AtlasRelationshipStore relationshipStore, AtlasEntityChangeNotifier entityChangeNotifier,
                             AtlasInstanceConverter instanceConverter) {
        this.deleteHandler        = deleteHandler;
        this.typeRegistry         = typeRegistry;
        this.graph                = atlasGraph;
        this.relationshipStore    = relationshipStore;
        this.entityChangeNotifier = entityChangeNotifier;
        this.instanceConverter    = instanceConverter;
        this.entityRetriever      = new EntityGraphRetriever(typeRegistry);
    }

    public AtlasVertex createVertex(AtlasEntity entity) {
        final String guid = UUID.randomUUID().toString();
        return createVertexWithGuid(entity, guid);
    }

    public AtlasVertex createVertexWithGuid(AtlasEntity entity, String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createVertex({})", entity.getTypeName());
        }

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        AtlasVertex ret = createStructVertex(entity);

        for (String superTypeName : entityType.getAllSuperTypes()) {
            AtlasGraphUtilsV1.addProperty(ret, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        AtlasGraphUtilsV1.setProperty(ret, Constants.GUID_PROPERTY_KEY, guid);
        AtlasGraphUtilsV1.setProperty(ret, Constants.VERSION_PROPERTY_KEY, getEntityVersion(entity));

        return ret;
    }

    public void updateSystemAttributes(AtlasVertex vertex, AtlasEntity entity) {
        if (entity.getStatus() != null) {
            AtlasGraphUtilsV1.setProperty(vertex, Constants.STATE_PROPERTY_KEY, entity.getStatus().name());
        }

        if (entity.getCreateTime() != null) {
            AtlasGraphUtilsV1.setProperty(vertex, Constants.TIMESTAMP_PROPERTY_KEY, entity.getCreateTime().getTime());
        }

        if (entity.getUpdateTime() != null) {
            AtlasGraphUtilsV1.setProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, entity.getUpdateTime().getTime());
        }

        if (StringUtils.isNotEmpty(entity.getCreatedBy())) {
            AtlasGraphUtilsV1.setProperty(vertex, Constants.CREATED_BY_KEY, entity.getCreatedBy());
        }

        if (StringUtils.isNotEmpty(entity.getUpdatedBy())) {
            AtlasGraphUtilsV1.setProperty(vertex, Constants.MODIFIED_BY_KEY, entity.getUpdatedBy());
        }
    }

    public EntityMutationResponse mapAttributesAndClassifications(EntityMutationContext context, final boolean isPartialUpdate, final boolean replaceClassifications) throws AtlasBaseException {
        EntityMutationResponse resp = new EntityMutationResponse();

        Collection<AtlasEntity> createdEntities = context.getCreatedEntities();
        Collection<AtlasEntity> updatedEntities = context.getUpdatedEntities();

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntity createdEntity : createdEntities) {
                String          guid       = createdEntity.getGuid();
                AtlasVertex     vertex     = context.getVertex(guid);
                AtlasEntityType entityType = context.getType(guid);

                compactAttributes(createdEntity);

                mapRelationshipAttributes(createdEntity, vertex, CREATE, context);

                mapAttributes(createdEntity, vertex, CREATE, context);

                resp.addEntity(CREATE, constructHeader(createdEntity, entityType, vertex));
                addClassifications(context, guid, createdEntity.getClassifications());
            }
        }

        if (CollectionUtils.isNotEmpty(updatedEntities)) {
            for (AtlasEntity updatedEntity : updatedEntities) {
                String          guid       = updatedEntity.getGuid();
                AtlasVertex     vertex     = context.getVertex(guid);
                AtlasEntityType entityType = context.getType(guid);

                compactAttributes(updatedEntity);

                mapRelationshipAttributes(updatedEntity, vertex, UPDATE, context);

                mapAttributes(updatedEntity, vertex, UPDATE, context);

                if (isPartialUpdate) {
                    resp.addEntity(PARTIAL_UPDATE, constructHeader(updatedEntity, entityType, vertex));
                } else {
                    resp.addEntity(UPDATE, constructHeader(updatedEntity, entityType, vertex));
                }

                if ( replaceClassifications ) {
                    deleteClassifications(guid);
                    addClassifications(context, guid, updatedEntity.getClassifications());
                }
            }
        }

        RequestContextV1 req = RequestContextV1.get();

        for (AtlasObjectId entity : req.getDeletedEntities()) {
            resp.addEntity(DELETE, entity);
        }

        for (AtlasObjectId entity : req.getUpdatedEntities()) {
            if (isPartialUpdate) {
                resp.addEntity(PARTIAL_UPDATE, entity);
            }
            else {
                resp.addEntity(UPDATE, entity);
            }
        }

        return resp;
    }

    private AtlasVertex createStructVertex(AtlasStruct struct) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createStructVertex({})", struct.getTypeName());
        }

        final AtlasVertex ret = graph.addVertex();

        AtlasGraphUtilsV1.setProperty(ret, Constants.ENTITY_TYPE_PROPERTY_KEY, struct.getTypeName());
        AtlasGraphUtilsV1.setProperty(ret, Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
        AtlasGraphUtilsV1.setProperty(ret, Constants.TIMESTAMP_PROPERTY_KEY, RequestContextV1.get().getRequestTime());
        AtlasGraphUtilsV1.setProperty(ret, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContextV1.get().getRequestTime());
        AtlasGraphUtilsV1.setProperty(ret, Constants.CREATED_BY_KEY, RequestContextV1.get().getUser());
        GraphHelper.setProperty(ret, Constants.MODIFIED_BY_KEY, RequestContextV1.get().getUser());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createStructVertex({})", struct.getTypeName());
        }

        return ret;
    }

    private AtlasVertex createClassificationVertex(AtlasClassification classification) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createVertex({})", classification.getTypeName());
        }

        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());

        AtlasVertex ret = createStructVertex(classification);

        AtlasGraphUtilsV1.addProperty(ret, Constants.SUPER_TYPES_PROPERTY_KEY, classificationType.getAllSuperTypes());
        AtlasGraphUtilsV1.setProperty(ret, Constants.CLASSIFICATION_ENTITY_GUID, classification.getEntityGuid());

        return ret;
    }


    private void mapAttributes(AtlasStruct struct, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapAttributes({}, {})", op, struct.getTypeName());
        }

        if (MapUtils.isNotEmpty(struct.getAttributes())) {
            AtlasStructType structType = getStructType(struct.getTypeName());

            if (op.equals(CREATE)) {
                for (AtlasAttribute attribute : structType.getAllAttributes().values()) {
                    Object attrValue = struct.getAttribute(attribute.getName());

                    mapAttribute(attribute, attrValue, vertex, op, context);
                }

            } else if (op.equals(UPDATE)) {
                for (String attrName : struct.getAttributes().keySet()) {
                    AtlasAttribute attribute = structType.getAttribute(attrName);

                    if (attribute != null) {
                        Object attrValue = struct.getAttribute(attrName);

                        mapAttribute(attribute, attrValue, vertex, op, context);
                    } else {
                        LOG.warn("mapAttributes(): invalid attribute {}.{}. Ignored..", struct.getTypeName(), attrName);
                    }
                }
            }

            updateModificationMetadata(vertex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapAttributes({}, {})", op, struct.getTypeName());
        }
    }

    private void mapRelationshipAttributes(AtlasEntity entity, AtlasVertex vertex, EntityOperation op,
                                           EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapRelationshipAttributes({}, {})", op, entity.getTypeName());
        }

        if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
            AtlasEntityType entityType = getEntityType(entity.getTypeName());

            if (op.equals(CREATE)) {
                for (AtlasAttribute attribute : entityType.getRelationshipAttributes().values()) {
                    Object attrValue = entity.getRelationshipAttribute(attribute.getName());

                    mapAttribute(attribute, attrValue, vertex, op, context);
                }

            } else if (op.equals(UPDATE)) {
                // relationship attributes mapping
                for (AtlasAttribute attribute : entityType.getRelationshipAttributes().values()) {
                    if (attribute != null && entity.hasRelationshipAttribute(attribute.getName())) {
                        Object attrValue = entity.getRelationshipAttribute(attribute.getName());

                        mapAttribute(attribute, attrValue, vertex, op, context);
                    }
                }
            }

            updateModificationMetadata(vertex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapRelationshipAttributes({}, {})", op, entity.getTypeName());
        }
    }

    private void mapAttribute(AtlasAttribute attribute, Object attrValue, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        if (attrValue == null) {
            AtlasAttributeDef attributeDef = attribute.getAttributeDef();
            AtlasType attrType = attribute.getAttributeType();
            if (attrType.getTypeCategory() == TypeCategory.PRIMITIVE) {
                if (attributeDef.getDefaultValue() != null) {
                    attrValue = attrType.createDefaultValue(attributeDef.getDefaultValue());
                } else {
                    if (attribute.getAttributeDef().getIsOptional()) {
                        attrValue = attrType.createOptionalDefaultValue();
                    } else {
                        attrValue = attrType.createDefaultValue();
                    }
                }
            }
        }

        AttributeMutationContext ctx = new AttributeMutationContext(op, vertex, attribute, attrValue);

        mapToVertexByTypeCategory(ctx, context);
    }

    private Object mapToVertexByTypeCategory(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (ctx.getOp() == CREATE && ctx.getValue() == null) {
            return null;
        }

        switch (ctx.getAttrType().getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
                return mapPrimitiveValue(ctx);

            case STRUCT: {
                String    edgeLabel   = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexProperty());
                AtlasEdge currentEdge = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel);
                AtlasEdge edge        = currentEdge != null ? currentEdge : null;

                ctx.setExistingEdge(edge);

                AtlasEdge newEdge = mapStructValue(ctx, context);

                if (currentEdge != null && !currentEdge.equals(newEdge)) {
                    deleteHandler.deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), false, true, ctx.getReferringVertex());
                }

                return newEdge;
            }

            case OBJECT_ID_TYPE: {
                AtlasRelationshipEdgeDirection edgeDirection = ctx.getAttribute().getRelationshipEdgeDirection();
                String edgeLabel = ctx.getAttribute().getRelationshipEdgeLabel();

                // if relationshipDefs doesn't exist, use legacy way of finding edge label.
                if (StringUtils.isEmpty(edgeLabel)) {
                    edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexProperty());
                }

                String    relationshipGuid = getRelationshipGuid(ctx.getValue());
                AtlasEdge currentEdge;

                // if relationshipGuid is assigned in AtlasRelatedObjectId use it to fetch existing AtlasEdge
                if (StringUtils.isNotEmpty(relationshipGuid) && !context.isImport()) {
                    currentEdge = graphHelper.getEdgeForGUID(relationshipGuid);
                } else {
                    currentEdge = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel, edgeDirection);
                }

                AtlasEdge newEdge = null;

                if (ctx.getValue() != null) {
                    AtlasEntityType instanceType = getInstanceType(ctx.getValue());
                    AtlasEdge       edge         = currentEdge != null ? currentEdge : null;

                    ctx.setElementType(instanceType);
                    ctx.setExistingEdge(edge);

                    newEdge = mapObjectIdValueUsingRelationship(ctx, context);

                    // legacy case update inverse attribute
                    if (ctx.getAttribute().getInverseRefAttribute() != null) {
                        // Update the inverse reference using relationship on the target entity
                        addInverseReference(ctx.getAttribute().getInverseRefAttribute(), newEdge, getRelationshipAttributes(ctx.getValue()));
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
                    deleteHandler.deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), ctx.getAttribute().isOwnedRef(),
                                                      true, ctx.getAttribute().getRelationshipEdgeDirection(), ctx.getReferringVertex());
                }

                return newEdge;
            }

            case MAP:
                return mapMapValue(ctx, context);

            case ARRAY:
                return mapArrayValue(ctx, context);

            default:
                throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }

    private void addInverseReference(AtlasAttribute inverseAttribute, AtlasEdge edge, Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        AtlasStructType inverseType      = inverseAttribute.getDefinedInType();
        AtlasVertex     inverseVertex    = edge.getInVertex();
        String          inverseEdgeLabel = inverseAttribute.getRelationshipEdgeLabel();
        AtlasEdge       inverseEdge      = graphHelper.getEdgeForLabel(inverseVertex, inverseEdgeLabel);
        String          propertyName     = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(inverseType, inverseAttribute.getName());

        // create new inverse reference
        AtlasEdge newEdge = createInverseReferenceUsingRelationship(inverseAttribute, edge, relationshipAttributes);

        boolean inverseUpdated = true;
        switch (inverseAttribute.getAttributeType().getTypeCategory()) {
        case OBJECT_ID_TYPE:
            if (inverseEdge != null) {
                if (!inverseEdge.equals(newEdge)) {
                    // Disconnect old reference
                    deleteHandler.deleteEdgeReference(inverseEdge, inverseAttribute.getAttributeType().getTypeCategory(),
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
            RequestContextV1 requestContext = RequestContextV1.get();

            if (!requestContext.isDeletedEntity(GraphHelper.getGuid(inverseVertex))) {
                updateModificationMetadata(inverseVertex);

                requestContext.recordEntityUpdate(entityRetriever.toAtlasObjectId(inverseVertex));
            }
        }
    }

    private AtlasEdge createInverseReferenceUsingRelationship(AtlasAttribute inverseAttribute, AtlasEdge edge, Map<String, Object> relationshipAttributes) throws AtlasBaseException {
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
                String relationshipName = graphHelper.getRelationshipDefName(inverseVertex, entityType, inverseAttributeName);

                ret = getOrCreateRelationship(inverseVertex, vertex, relationshipName, relationshipAttributes);

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No RelationshipDef defined between {} and {} on attribute: {}", inverseAttributeType,
                                                        AtlasGraphUtilsV1.getTypeName(vertex), inverseAttributeName);
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

        return ret;
    }

    // legacy method to create edges for inverse reference
    private AtlasEdge createInverseReference(AtlasAttribute inverseAttribute, AtlasStructType inverseAttributeType,
                                             AtlasVertex inverseVertex, AtlasVertex vertex) throws AtlasBaseException {

        String propertyName     = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(inverseAttributeType, inverseAttribute.getName());
        String inverseEdgeLabel = AtlasGraphUtilsV1.getEdgeLabel(propertyName);
        AtlasEdge ret;

        try {
            ret = graphHelper.getOrCreateEdge(inverseVertex, vertex, inverseEdgeLabel);

        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        return ret;
    }

    private Object mapPrimitiveValue(AttributeMutationContext ctx) {
        AtlasGraphUtilsV1.setProperty(ctx.getReferringVertex(), ctx.getVertexProperty(), ctx.getValue());

        return ctx.getValue();
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
            }

            ret = ctx.getCurrentEdge();
        } else if (ctx.getValue() != null) {
            String edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexProperty());

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
            AtlasObjectId objId = getObjectId(ctx.getValue());

            if (objId != null) {
                entityVertex = context.getDiscoveryContext().getResolvedEntityVertex(objId);
            }
        }

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, (ctx.getValue() == null ? null : ctx.getValue().toString()));
        }

        if (ctx.getCurrentEdge() != null) {
            ret = updateEdge(ctx.getAttributeDef(), ctx.getValue(), ctx.getCurrentEdge(), entityVertex);
        } else if (ctx.getValue() != null) {
            String edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexProperty());

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

        AtlasVertex attributeVertex = context.getDiscoveryContext().getResolvedEntityVertex(getGuid(ctx.getValue()));
        AtlasVertex entityVertex    = ctx.getReferringVertex();
        AtlasEdge   ret;

        if (attributeVertex == null) {
            AtlasObjectId objectId = getObjectId(ctx.getValue());

            attributeVertex = (objectId != null) ? context.getDiscoveryContext().getResolvedEntityVertex(objectId) : null;
        }

        if (attributeVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, (ctx.getValue() == null ? null : ctx.getValue().toString()));
        }

        String    attributeName = ctx.getAttribute().getName();
        AtlasType type          = typeRegistry.getType(AtlasGraphUtilsV1.getTypeName(entityVertex));

        AtlasRelationshipEdgeDirection edgeDirection = ctx.getAttribute().getRelationshipEdgeDirection();
        String                         edgeLabel     = ctx.getAttribute().getRelationshipEdgeLabel();

        if (type instanceof AtlasEntityType) {
            AtlasEntityType entityType = (AtlasEntityType) type;

            // use relationship to create/update edges
            if (entityType.hasRelationshipAttribute(attributeName)) {
                Map<String, Object> relationshipAttributes = getRelationshipAttributes(ctx.getValue());

                if (ctx.getCurrentEdge() != null) {
                    ret = updateRelationship(ctx.getCurrentEdge(), entityVertex, attributeVertex, edgeDirection, relationshipAttributes);

                } else {
                    String      relationshipName = graphHelper.getRelationshipDefName(entityVertex, entityType, attributeName);
                    AtlasVertex fromVertex;
                    AtlasVertex toVertex;

                    if (edgeDirection == IN) {
                        fromVertex = attributeVertex;
                        toVertex   = entityVertex;

                    } else {
                        fromVertex = entityVertex;
                        toVertex   = attributeVertex;
                    }
                    boolean relationshipExists = isRelationshipExists(fromVertex, toVertex, edgeLabel);

                    ret = getOrCreateRelationship(fromVertex, toVertex, relationshipName, relationshipAttributes);

                    // for import use the relationship guid provided
                    if (context.isImport()) {
                        AtlasGraphUtilsV1.setProperty(ret, Constants.GUID_PROPERTY_KEY, getRelationshipGuid(ctx.getValue()));
                    }

                    // if relationship did not exist before and new relationship was created
                    // record entity update on both relationship vertices
                    if (!relationshipExists) {
                        recordEntityUpdate(attributeVertex);
                    }
                }
            } else {
                // use legacy way to create/update edges
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No RelationshipDef defined between {} and {} on attribute: {}",  getTypeName(entityVertex),
                               getTypeName(attributeVertex), attributeName);
                }

                ret = mapObjectIdValue(ctx, context);
            }

        } else {
            // if type is StructType having objectid as attribute
            ret = mapObjectIdValue(ctx, context);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapObjectIdValueUsingRelationship({})", ctx);
        }

        return ret;
    }

    private Map<String, Object> mapMapValue(AttributeMutationContext ctx, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> mapMapValue({})", ctx);
        }

        @SuppressWarnings("unchecked")
        Map<Object, Object> newVal  = (Map<Object, Object>) ctx.getValue();
        Map<String, Object> newMap  = new HashMap<>();
        AtlasMapType        mapType = (AtlasMapType) ctx.getAttrType();

        try {
            AtlasAttribute      attribute   = ctx.getAttribute();
            List<String> currentKeys = GraphHelper.getListProperty(ctx.getReferringVertex(), ctx.getVertexProperty());
            Map<String, Object> currentMap  = new HashMap<>();

            if (CollectionUtils.isNotEmpty(currentKeys)) {
                for (String key : currentKeys) {
                    String propertyNameForKey  = GraphHelper.getQualifiedNameForMapKey(ctx.getVertexProperty(), GraphHelper.encodePropertyKey(key));
                    Object propertyValueForKey = getMapValueProperty(mapType.getValueType(), ctx.getReferringVertex(), propertyNameForKey);

                    currentMap.put(key, propertyValueForKey);
                }
            }

            if (MapUtils.isNotEmpty(newVal)) {
                boolean isReference = AtlasGraphUtilsV1.isReference(mapType.getValueType());
                AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();
                for (Map.Entry<Object, Object> entry : newVal.entrySet()) {
                    String    key          = entry.getKey().toString();
                    String    propertyName = GraphHelper.getQualifiedNameForMapKey(ctx.getVertexProperty(), GraphHelper.encodePropertyKey(key));
                    AtlasEdge existingEdge = getEdgeIfExists(mapType, currentMap, key);

                    AttributeMutationContext mapCtx =  new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), attribute, entry.getValue(), propertyName, mapType.getValueType(), existingEdge);

                    //Add/Update/Remove property value
                    Object newEntry = mapCollectionElementsToVertex(mapCtx, context);
                    setMapValueProperty(mapType.getValueType(), ctx.getReferringVertex(), propertyName, newEntry);

                    newMap.put(key, newEntry);

                    // If value type indicates this attribute is a reference, and the attribute has an inverse reference attribute,
                    // update the inverse reference value.
                    if (isReference && newEntry instanceof AtlasEdge && inverseRefAttribute != null) {
                        AtlasEdge newEdge = (AtlasEdge) newEntry;

                        addInverseReference(inverseRefAttribute, newEdge, getRelationshipAttributes(ctx.getValue()));
                    }
                }
            }

            Map<String, Object> finalMap = removeUnusedMapEntries(attribute, ctx.getReferringVertex(), ctx.getVertexProperty(), currentMap, newMap);

            for (Object newEntry : newMap.values()) {
                updateInConsistentOwnedMapVertices(ctx, mapType, newEntry);
            }

            Set<String> newKeys = new LinkedHashSet<>(newMap.keySet());
            newKeys.addAll(finalMap.keySet());

            // for dereference on way out
            GraphHelper.setListProperty(ctx.getReferringVertex(), ctx.getVertexProperty(), new ArrayList<>(newKeys));
        } catch (AtlasException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
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
        boolean        isReference         = AtlasGraphUtilsV1.isReference(elementType);
        AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();
        Cardinality    cardinality         = attribute.getAttributeDef().getCardinality();
        List<Object>   newElementsCreated  = new ArrayList<>();
        List<Object>   currentElements;

        if (isRelationshipAttribute(attribute)) {
            currentElements = getArrayElementsUsingRelationship(ctx.getReferringVertex(), attribute, elementType);
        } else {
            currentElements = getArrayElementsProperty(elementType, ctx.getReferringVertex(), ctx.getVertexProperty());
        }

        if (CollectionUtils.isNotEmpty(newElements)) {
            if (cardinality == SET) {
                newElements = (List) newElements.stream().distinct().collect(Collectors.toList());
            }

            for (int index = 0; index < newElements.size(); index++) {
                AtlasEdge               existingEdge = getEdgeAt(currentElements, index, elementType);
                AttributeMutationContext arrCtx      = new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), ctx.getAttribute(), newElements.get(index),
                                                                                     ctx.getVertexProperty(), elementType, existingEdge);

                Object newEntry = mapCollectionElementsToVertex(arrCtx, context);

                if (isReference && newEntry instanceof AtlasEdge && inverseRefAttribute != null) {
                    // Update the inverse reference value.
                    AtlasEdge newEdge = (AtlasEdge) newEntry;

                    addInverseReference(inverseRefAttribute, newEdge, getRelationshipAttributes(ctx.getValue()));
                }

                newElementsCreated.add(newEntry);
            }
        }

        if (isReference) {
            List<AtlasEdge> additionalEdges = removeUnusedArrayEntries(attribute, (List) currentElements, (List) newElementsCreated, ctx.getReferringVertex());
            newElementsCreated.addAll(additionalEdges);
        }

        // for dereference on way out
        setArrayElementsProperty(elementType, ctx.getReferringVertex(), ctx.getVertexProperty(), newElementsCreated);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapArrayValue({})", ctx);
        }

        return newElementsCreated;
    }

    private boolean isRelationshipAttribute(AtlasAttribute attribute) {
        boolean ret = false;

        if (attribute != null) {
            AtlasStructType structType    = attribute.getDefinedInType();
            String          attributeName = attribute.getName();

            if (structType instanceof AtlasEntityType) {
                ret = ((AtlasEntityType) structType).hasRelationshipAttribute(attributeName);
            }
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
            AtlasEntityType instanceType = getInstanceType(ctx.getValue());
            ctx.setElementType(instanceType);
            return mapObjectIdValueUsingRelationship(ctx, context);

        default:
                throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }

    private static AtlasObjectId getObjectId(Object val) throws AtlasBaseException {
        if (val != null) {
            if ( val instanceof  AtlasObjectId) {
                return ((AtlasObjectId) val);
            } else if (val instanceof Map) {
                AtlasObjectId ret = new AtlasObjectId((Map)val);

                if (AtlasTypeUtil.isValid(ret)) {
                    return ret;
                }
            }

            throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, val.toString());
        }

        return null;
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

    private AtlasEntityType getInstanceType(Object val) throws AtlasBaseException {
        AtlasEntityType ret = null;

        if (val != null) {
            String typeName = null;

            if (val instanceof AtlasObjectId) {
                typeName = ((AtlasObjectId)val).getTypeName();
            } else if (val instanceof Map) {
                Object typeNameVal = ((Map)val).get(AtlasObjectId.KEY_TYPENAME);

                if (typeNameVal != null) {
                    typeName = typeNameVal.toString();
                }
            }

            ret = typeName != null ? typeRegistry.getEntityTypeByName(typeName) : null;

            if (ret == null) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, val.toString());
            }
        }

        return ret;
    }


    public static Object getMapValueProperty(AtlasType elementType, AtlasVertex vertex, String vertexPropertyName) {
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            return vertex.getProperty(vertexPropertyName, AtlasEdge.class);
        } else if (elementType instanceof AtlasArrayType) {
            return vertex.getProperty(vertexPropertyName, List.class);
        } else if (elementType instanceof AtlasMapType) {
            return vertex.getProperty(vertexPropertyName, Map.class);
        }
        else {
            return vertex.getProperty(vertexPropertyName, String.class).toString();
        }
    }

    private static void setMapValueProperty(AtlasType elementType, AtlasVertex vertex, String vertexPropertyName, Object value) {
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            vertex.setPropertyFromElementId(vertexPropertyName, (AtlasEdge)value);
        }
        else {
            vertex.setProperty(vertexPropertyName, value);
        }
    }

    //Remove unused entries from map
    private Map<String, Object> removeUnusedMapEntries(AtlasAttribute attribute, AtlasVertex vertex, String propertyName,
                                                       Map<String, Object> currentMap, Map<String, Object> newMap)
                                                                             throws AtlasException, AtlasBaseException {
        AtlasMapType        mapType       = (AtlasMapType) attribute.getAttributeType();
        Map<String, Object> additionalMap = new HashMap<>();

        for (String currentKey : currentMap.keySet()) {
            boolean shouldDeleteKey = !newMap.containsKey(currentKey);

            if (AtlasGraphUtilsV1.isReference(mapType.getValueType())) {
                //Delete the edge reference if its not part of new edges created/updated
                AtlasEdge currentEdge = (AtlasEdge)currentMap.get(currentKey);

                if (!newMap.values().contains(currentEdge)) {
                    boolean deleted = deleteHandler.deleteEdgeReference(currentEdge, mapType.getValueType().getTypeCategory(), attribute.isOwnedRef(), true, vertex);

                    if (!deleted) {
                        additionalMap.put(currentKey, currentEdge);
                        shouldDeleteKey = false;
                    }
                }
            }

            if (shouldDeleteKey) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, GraphHelper.encodePropertyKey(currentKey));
                GraphHelper.setProperty(vertex, propertyNameForKey, null);
            }
        }

        return additionalMap;
    }

    private static AtlasEdge getEdgeIfExists(AtlasMapType mapType, Map<String, Object> currentMap, String keyStr) {
        AtlasEdge ret = null;

        if (AtlasGraphUtilsV1.isReference(mapType.getValueType())) {
            Object val = currentMap.get(keyStr);

            if (val != null) {
                ret = (AtlasEdge) val;
            }
        }

        return ret;
    }

    private AtlasEdge updateEdge(AtlasAttributeDef attributeDef, Object value, AtlasEdge currentEdge, final AtlasVertex entityVertex) throws AtlasBaseException {

        LOG.debug("Updating entity reference {} for reference attribute {}",  attributeDef.getName());
        // Update edge if it exists

        AtlasVertex currentVertex = currentEdge.getInVertex();
        String currentEntityId = getIdFromVertex(currentVertex);
        String newEntityId = getIdFromVertex(entityVertex);

        AtlasEdge newEdge = currentEdge;
        if (!currentEntityId.equals(newEntityId)) {
            // add an edge to the class vertex from the instance
            if (entityVertex != null) {
                try {
                    newEdge = graphHelper.getOrCreateEdge(currentEdge.getOutVertex(), entityVertex, currentEdge.getLabel());
                } catch (RepositoryException e) {
                    throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
                }

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
            String relationshipName = AtlasGraphUtilsV1.getTypeName(currentEdge);

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

    public static List<Object> getArrayElementsProperty(AtlasType elementType, AtlasVertex vertex, String vertexPropertyName) {
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            return (List)vertex.getListProperty(vertexPropertyName, AtlasEdge.class);
        }
        else {
            return (List)vertex.getListProperty(vertexPropertyName);
        }
    }

    public static List<Object> getArrayElementsUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute, AtlasType elementType) {
        List<Object> ret = null;

        if (AtlasGraphUtilsV1.isReference(elementType)) {

            AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();
            String                         edgeLabel = attribute.getRelationshipEdgeLabel();

            Iterator<AtlasEdge> edgesForLabel = GraphHelper.getEdgesForLabel(vertex, edgeLabel, edgeDirection);

            ret = IteratorUtils.toList(edgesForLabel);
        }
        return ret;
    }

    private AtlasEdge getEdgeAt(List<Object> currentElements, int index, AtlasType elemType) {
        AtlasEdge ret = null;

        if (AtlasGraphUtilsV1.isReference(elemType)) {
            if (currentElements != null && index < currentElements.size()) {
                ret = (AtlasEdge) currentElements.get(index);
            }
        }

        return ret;
    }

    //Removes unused edges from the old collection, compared to the new collection

    private List<AtlasEdge> removeUnusedArrayEntries(AtlasAttribute attribute, List<AtlasEdge> currentEntries, List<AtlasEdge> newEntries, AtlasVertex entityVertex) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(currentEntries)) {
            AtlasType entryType = ((AtlasArrayType) attribute.getAttributeType()).getElementType();

            if (AtlasGraphUtilsV1.isReference(entryType)) {
                Collection<AtlasEdge> edgesToRemove = CollectionUtils.subtract(currentEntries, newEntries);

                if (CollectionUtils.isNotEmpty(edgesToRemove)) {
                    List<AtlasEdge> additionalElements = new ArrayList<>();

                    for (AtlasEdge edge : edgesToRemove) {
                        boolean deleted = deleteHandler.deleteEdgeReference(edge, entryType.getTypeCategory(), attribute.isOwnedRef(),
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
    private void setArrayElementsProperty(AtlasType elementType, AtlasVertex vertex, String vertexPropertyName, List<Object> values) {
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            GraphHelper.setListPropertyFromElementIds(vertex, vertexPropertyName, (List) values);
        }
        else {
            GraphHelper.setProperty(vertex, vertexPropertyName, values);
        }
    }


    private AtlasEntityHeader constructHeader(AtlasEntity entity, final AtlasEntityType type, AtlasVertex vertex) {
        AtlasEntityHeader header = new AtlasEntityHeader(entity.getTypeName());

        header.setGuid(getIdFromVertex(vertex));

        for (AtlasAttribute attribute : type.getUniqAttributes().values()) {
            header.setAttribute(attribute.getName(), entity.getAttribute(attribute.getName()));
        }

        return header;
    }

    public static AtlasEntityHeader constructHeader(AtlasObjectId id) {
        return new AtlasEntityHeader(id.getTypeName(), id.getGuid(), id.getUniqueAttributes());
    }

    private void updateInConsistentOwnedMapVertices(AttributeMutationContext ctx, AtlasMapType mapType, Object val) {
        if (mapType.getValueType().getTypeCategory() == TypeCategory.OBJECT_ID_TYPE) {
            AtlasEdge edge = (AtlasEdge) val;
            if (ctx.getAttribute().isOwnedRef() &&
                GraphHelper.getStatus(edge) == AtlasEntity.Status.DELETED &&
                GraphHelper.getStatus(edge.getInVertex()) == AtlasEntity.Status.DELETED) {
                //Resurrect the vertex and edge to ACTIVE state
                GraphHelper.setProperty(edge, STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
                GraphHelper.setProperty(edge.getInVertex(), STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
            }
        }
    }

    public void addClassifications(final EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(classifications)) {
            AtlasVertex entityVertex = AtlasGraphUtilsV1.findByGuid(guid);

            if (entityVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            final String                                entityTypeName        = AtlasGraphUtilsV1.getTypeName(entityVertex);
            final AtlasEntityType                       entityType            = typeRegistry.getEntityTypeByName(entityTypeName);
            List<AtlasVertex>                           entitiesToPropagateTo = null;
            Map<AtlasVertex, List<AtlasClassification>> propagations          = null;

            for (AtlasClassification classification : classifications) {
                String  classificationName = classification.getTypeName();
                boolean propagateTags      = classification.isPropagate();

                // set associated entity id to classification
                classification.setEntityGuid(guid);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding classification [{}] to [{}] using edge label: [{}]", classificationName, entityTypeName, getTraitLabel(classificationName));
                }

                GraphHelper.addProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, classificationName);

                // add a new AtlasVertex for the struct or trait instance
                AtlasVertex classificationVertex = createClassificationVertex(classification);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("created vertex {} for trait {}", string(classificationVertex), classificationName);
                }

                // add the attributes for the trait instance
                mapClassification(EntityOperation.CREATE, context, classification, entityType, entityVertex, classificationVertex);

                if (propagateTags) {
                    // compute propagatedEntityVertices only once
                    if (entitiesToPropagateTo == null) {
                        entitiesToPropagateTo = graphHelper.getImpactedVertices(guid);
                    }

                    if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                        if (propagations == null) {
                            propagations = new HashMap<>(entitiesToPropagateTo.size());

                            for (AtlasVertex entityToPropagateTo : entitiesToPropagateTo) {
                                propagations.put(entityToPropagateTo, new ArrayList<>());
                            }
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Propagating tag: [{}][{}] to {}", classificationName, entityTypeName, getTypeNames(entitiesToPropagateTo));
                        }

                        List<AtlasVertex> entitiesPropagatedTo = addTagPropagation(classificationVertex, entitiesToPropagateTo);

                        if (entitiesPropagatedTo != null) {
                            for (AtlasVertex entityPropagatedTo : entitiesPropagatedTo) {
                                propagations.get(entityPropagatedTo).add(classification);
                            }
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(" --> Not propagating classification: [{}][{}] - no entities found to propagate to.", getTypeName(classificationVertex), entityTypeName);
                        }
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" --> Not propagating classification: [{}][{}] - propagation is disabled.", getTypeName(classificationVertex), entityTypeName);
                    }
                }
            }

            // notify listeners on classification addition
            List<AtlasVertex> notificationVertices = new ArrayList<AtlasVertex>() {{ add(entityVertex); }};

            if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                notificationVertices.addAll(entitiesToPropagateTo);
            }

            for (AtlasVertex vertex : notificationVertices) {
                String                    entityGuid           = GraphHelper.getGuid(vertex);
                AtlasEntityWithExtInfo    entityWithExtInfo    = instanceConverter.getAndCacheEntity(entityGuid);
                AtlasEntity               entity               = (entityWithExtInfo != null) ? entityWithExtInfo.getEntity() : null;
                List<AtlasClassification> addedClassifications = StringUtils.equals(entityGuid, guid) ? classifications : propagations.get(vertex);

                if (CollectionUtils.isNotEmpty(addedClassifications)) {
                    entityChangeNotifier.onClassificationAddedToEntity(entity, addedClassifications);
                }
            }
        }
    }

    public void deleteClassifications(String entityGuid, List<String> classificationNames) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(classificationNames)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "delete", entityGuid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV1.findByGuid(entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        List<String> traitNames = getTraitNames(entityVertex);

        if (CollectionUtils.isEmpty(traitNames)) {
            throw new AtlasBaseException(AtlasErrorCode.NO_CLASSIFICATIONS_FOUND_FOR_ENTITY, entityGuid);
        }

        validateClassificationExists(traitNames, classificationNames);

        Map<AtlasVertex, List<String>> removedClassifications = new HashMap<>();

        for (String classificationName : classificationNames) {
            AtlasVertex classificationVertex = getClassificationVertex(entityVertex, classificationName);

            // remove classification from propagated entities if propagation is turned on
            if (isPropagationEnabled(classificationVertex)) {
                List<AtlasVertex> impactedVertices = removeTagPropagation(classificationVertex);

                if (CollectionUtils.isNotEmpty(impactedVertices)) {
                    for (AtlasVertex impactedVertex : impactedVertices) {
                        List<String> classifications = removedClassifications.get(impactedVertex);

                        if (classifications == null) {
                            classifications = new ArrayList<>();

                            removedClassifications.put(impactedVertex, classifications);
                        }

                        classifications.add(classificationName);
                    }
                }
            }

            // remove classifications from associated entity
            if (LOG.isDebugEnabled()) {
                LOG.debug("Removing classification: [{}] from: [{}][{}] with edge label: [{}]", classificationName,
                        getTypeName(entityVertex), entityGuid, CLASSIFICATION_LABEL);
            }

            AtlasEdge edge = getClassificationEdge(entityVertex, classificationVertex);

            deleteHandler.deleteEdgeReference(edge, CLASSIFICATION, false, true, entityVertex);

            traitNames.remove(classificationName);
        }

        removedClassifications.put(entityVertex, classificationNames);

        updateTraitNamesProperty(entityVertex, traitNames);

        updateModificationMetadata(entityVertex);

        for (Map.Entry<AtlasVertex, List<String>> entry : removedClassifications.entrySet()) {
            String                 guid                       = GraphHelper.getGuid(entry.getKey());
            List<String>           deletedClassificationNames = entry.getValue();
            AtlasEntityWithExtInfo entityWithExtInfo          = instanceConverter.getAndCacheEntity(guid);
            AtlasEntity            entity                     = (entityWithExtInfo != null) ? entityWithExtInfo.getEntity() : null;

            entityChangeNotifier.onClassificationDeletedFromEntity(entity, deletedClassificationNames);
        }
    }

    public void updateClassifications(EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "update", guid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV1.findByGuid(guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String                    entityTypeName         = AtlasGraphUtilsV1.getTypeName(entityVertex);
        AtlasEntityType           entityType             = typeRegistry.getEntityTypeByName(entityTypeName);
        List<AtlasClassification> updatedClassifications = new ArrayList<>();
        List<AtlasVertex>         entitiesToPropagateTo  = new ArrayList<>();

        Map<AtlasVertex, List<AtlasClassification>> addedPropagations   = null;
        Map<AtlasVertex, List<String>>              removedPropagations = null;

        for (AtlasClassification classification : classifications) {
            String classificationName       = classification.getTypeName();
            String classificationEntityGuid = classification.getEntityGuid();

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

            if (isClassificationUpdated && CollectionUtils.isEmpty(entitiesToPropagateTo)) {
                entitiesToPropagateTo = graphHelper.getImpactedVertices(guid);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("updating vertex {} for trait {}", string(classificationVertex), classificationName);
            }

            mapClassification(EntityOperation.UPDATE, context, classification, entityType, entityVertex, classificationVertex);

            // handle update of 'propagate' flag
            boolean currentTagPropagation = currentClassification.isPropagate();
            boolean updatedTagPropagation = classification.isPropagate();

            // compute propagatedEntityVertices once and use it for subsequent iterations and notifications
            if (currentTagPropagation != updatedTagPropagation) {
                if (updatedTagPropagation) {
                    if (CollectionUtils.isEmpty(entitiesToPropagateTo)) {
                        entitiesToPropagateTo = graphHelper.getImpactedVertices(guid);
                    }

                    if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
                        if (addedPropagations == null) {
                            addedPropagations = new HashMap<>(entitiesToPropagateTo.size());

                            for (AtlasVertex entityToPropagateTo : entitiesToPropagateTo) {
                                addedPropagations.put(entityToPropagateTo, new ArrayList<>());
                            }
                        }

                        List<AtlasVertex> entitiesPropagatedTo = addTagPropagation(classificationVertex, entitiesToPropagateTo);

                        if (entitiesPropagatedTo != null) {
                            for (AtlasVertex entityPropagatedTo : entitiesPropagatedTo) {
                                addedPropagations.get(entityPropagatedTo).add(classification);
                            }
                        }
                    }
                } else {
                    List<AtlasVertex> impactedVertices = removeTagPropagation(classificationVertex);

                    if (CollectionUtils.isNotEmpty(impactedVertices)) {
                        if (removedPropagations == null) {
                            removedPropagations = new HashMap<>();

                            for (AtlasVertex impactedVertex : impactedVertices) {
                                List<String> removedClassifications = removedPropagations.get(impactedVertex);

                                if (removedClassifications == null) {
                                    removedClassifications = new ArrayList<>();

                                    removedPropagations.put(impactedVertex, removedClassifications);
                                }

                                removedClassifications.add(classification.getTypeName());
                            }
                        }
                    }
                }
            }

            updatedClassifications.add(currentClassification);
        }

        // notify listeners on classification update
        List<AtlasVertex> notificationVertices = new ArrayList<AtlasVertex>() {{ add(entityVertex); }};

        if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
            notificationVertices.addAll(entitiesToPropagateTo);
        }

        for (AtlasVertex vertex : notificationVertices) {
            String                    entityGuid                = GraphHelper.getGuid(vertex);
            AtlasEntityWithExtInfo    entityWithExtInfo         = instanceConverter.getAndCacheEntity(entityGuid);
            AtlasEntity               entity                    = (entityWithExtInfo != null) ? entityWithExtInfo.getEntity() : null;
            List<AtlasClassification> updatedClassificationList = StringUtils.equals(entityGuid, guid) ? updatedClassifications : Collections.emptyList();

            entityChangeNotifier.onClassificationUpdatedToEntity(entity, updatedClassificationList);
        }

        if (removedPropagations != null) {
            for (Map.Entry<AtlasVertex, List<String>> entry : removedPropagations.entrySet()) {
                AtlasVertex            vertex                 = entry.getKey();
                List<String>           removedClassifications = entry.getValue();
                String                 entityGuid             = GraphHelper.getGuid(vertex);
                AtlasEntityWithExtInfo entityWithExtInfo      = instanceConverter.getAndCacheEntity(entityGuid);
                AtlasEntity            entity                 = (entityWithExtInfo != null) ? entityWithExtInfo.getEntity() : null;

                entityChangeNotifier.onClassificationDeletedFromEntity(entity, removedClassifications);
            }
        }
    }

    public void setPropagatedClassificationState(String entityGuid, String classificationName, String sourceEntityGuid, boolean disablePropagation) throws AtlasBaseException {
        AtlasVertex entityVertex = AtlasGraphUtilsV1.findByGuid(entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        AtlasEdge propagatedEdge = getPropagatedClassificationEdge(entityVertex, classificationName, sourceEntityGuid);

        if (propagatedEdge == null) {
            throw new AtlasBaseException(AtlasErrorCode.PROPAGATED_CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, classificationName);
        }

        PropagationState currentState = getClassificationEdgeState(propagatedEdge);
        PropagationState updatedState = (disablePropagation) ? PropagationState.DELETED : PropagationState.ACTIVE;

        if (currentState != updatedState) {
            AtlasGraphUtilsV1.setProperty(propagatedEdge, CLASSIFICATION_EDGE_STATE_PROPERTY_KEY, updatedState);

            if (disablePropagation) {
                removeFromPropagatedTraitNames(entityVertex, classificationName);
            } else {
                addToPropagatedTraitNames(entityVertex, classificationName);
            }

            updateModificationMetadata(entityVertex);

            AtlasEntityWithExtInfo entityWithExtInfo = instanceConverter.getAndCacheEntity(entityGuid);
            AtlasEntity            entity            = (entityWithExtInfo != null) ? entityWithExtInfo.getEntity() : null;

            if (updatedState == PropagationState.DELETED) {
                entityChangeNotifier.onClassificationDeletedFromEntity(entity, Collections.singletonList(classificationName));
            } else {
                AtlasClassification classification = entityRetriever.toAtlasClassification(propagatedEdge.getInVertex());

                entityChangeNotifier.onClassificationAddedToEntity(entity, Collections.singletonList(classification));
            }
        }
    }

    private List<AtlasVertex> addTagPropagation(AtlasVertex classificationVertex, List<AtlasVertex> propagatedEntityVertices) {
        List<AtlasVertex> ret = null;

        if (CollectionUtils.isNotEmpty(propagatedEntityVertices) && classificationVertex != null) {
            String                  classificationName = getTypeName(classificationVertex);
            AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classificationName);

            for (AtlasVertex propagatedEntityVertex : propagatedEntityVertices) {
                AtlasEdge existingEdge = getPropagatedClassificationEdge(propagatedEntityVertex, classificationVertex);

                if (existingEdge != null) {
                    continue;
                }

                String          entityTypeName = getTypeName(propagatedEntityVertex);
                AtlasEntityType entityType     = typeRegistry.getEntityTypeByName(entityTypeName);

                if (classificationType.canApplyToEntityType(entityType)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" --> Adding propagated classification: [{}] to {} ({}) using edge label: [{}]", classificationName, getTypeName(propagatedEntityVertex),
                                   GraphHelper.getGuid(propagatedEntityVertex), CLASSIFICATION_LABEL);
                    }

                    if (ret == null) {
                        ret = new ArrayList<>();
                    }

                    ret.add(propagatedEntityVertex);

                    graphHelper.addClassificationEdge(propagatedEntityVertex, classificationVertex, true);

                    addToPropagatedTraitNames(propagatedEntityVertex, classificationName);
                }
            }
        }

        return ret;
    }

    private List<AtlasVertex> removeTagPropagation(AtlasVertex classificationVertex) throws AtlasBaseException {
        return deleteHandler.removeTagPropagation(classificationVertex);
    }

    private AtlasEdge mapClassification(EntityOperation operation,  final EntityMutationContext context, AtlasClassification classification,
                                        AtlasEntityType entityType, AtlasVertex parentInstanceVertex, AtlasVertex traitInstanceVertex)
                                        throws AtlasBaseException {
        if (classification.getValidityPeriods() != null) {
            String strValidityPeriods = AtlasJson.toJson(classification.getValidityPeriods());

            AtlasGraphUtilsV1.setProperty(traitInstanceVertex, Constants.CLASSIFICATION_VALIDITY_PERIODS_KEY, strValidityPeriods);
        } else {
            // if 'null', don't update existing value in the classification
        }

        AtlasGraphUtilsV1.setProperty(traitInstanceVertex, Constants.CLASSIFICATION_VERTEX_PROPAGATE_KEY, classification.isPropagate());

        // map all the attributes to this newly created AtlasVertex
        mapAttributes(classification, traitInstanceVertex, operation, context);

        AtlasEdge ret = getClassificationEdge(parentInstanceVertex, traitInstanceVertex);

        if (ret == null) {
            ret = graphHelper.addClassificationEdge(parentInstanceVertex, traitInstanceVertex, false);
        }

        return ret;
    }

    public void deleteClassifications(String guid) throws AtlasBaseException {
        AtlasVertex instanceVertex = AtlasGraphUtilsV1.findByGuid(guid);

        if (instanceVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        List<String> traitNames = getTraitNames(instanceVertex);

        if (CollectionUtils.isNotEmpty(traitNames)) {
            deleteClassifications(guid, traitNames);
        }
    }

    private void updateTraitNamesProperty(AtlasVertex entityVertex, List<String> traitNames) {
        if (entityVertex != null) {
            entityVertex.removeProperty(TRAIT_NAMES_PROPERTY_KEY);

            for (String traitName : traitNames) {
                GraphHelper.addProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, traitName);
            }
        }
    }

    private void validateClassificationExists(List<String> existingClassifications, List<String> suppliedClassifications) throws AtlasBaseException {
        Set<String> existingNames = new HashSet<>(existingClassifications);
        for (String classificationName : suppliedClassifications) {
            if (!existingNames.contains(classificationName)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, classificationName);
            }
        }
    }

    private AtlasEdge getOrCreateRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, String relationshipName,
                                              Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        return relationshipStore.getOrCreate(end1Vertex, end2Vertex, new AtlasRelationship(relationshipName, relationshipAttributes));
    }

    private boolean isRelationshipExists(AtlasVertex fromVertex, AtlasVertex toVertex, String edgeLabel) {
        boolean             ret   = false;
        Iterator<AtlasEdge> edges = graphHelper.getOutGoingEdgesByLabel(fromVertex, edgeLabel);

        while (edges != null && edges.hasNext()) {
            AtlasEdge   edge     = edges.next();
            AtlasVertex inVertex = edge.getInVertex();

            if (inVertex != null && StringUtils.equals(getIdFromVertex(inVertex), getIdFromVertex(toVertex))) {
                ret = true;
            }
        }

        return ret;
    }

    private void recordEntityUpdate(AtlasVertex vertex) throws AtlasBaseException {
        RequestContextV1 req  = RequestContextV1.get();

        if (!req.isUpdatedEntity(GraphHelper.getGuid(vertex))) {
            updateModificationMetadata(vertex);

            req.recordEntityUpdate(entityRetriever.toAtlasObjectId(vertex));
        }
    }

    private static void compactAttributes(AtlasEntity entity) {
        if (entity != null) {
            Map<String, Object> relationshipAttributes = entity.getRelationshipAttributes();
            Map<String, Object> attributes = entity.getAttributes();

            if (MapUtils.isNotEmpty(relationshipAttributes) && MapUtils.isNotEmpty(attributes)) {
                for (String attrName : relationshipAttributes.keySet()) {
                    if (attributes.containsKey(attrName)) {
                        entity.removeAttribute(attrName);
                    }
                }
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
}
