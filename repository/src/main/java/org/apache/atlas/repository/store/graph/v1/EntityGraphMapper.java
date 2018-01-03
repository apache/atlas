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
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.PARTIAL_UPDATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.string;

@Component
public class EntityGraphMapper {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphMapper.class);

    private final GraphHelper       graphHelper = GraphHelper.getInstance();
    private final AtlasGraph        graph;
    private final DeleteHandlerV1   deleteHandler;
    private final AtlasTypeRegistry typeRegistry;


    @Inject
    public EntityGraphMapper(DeleteHandlerV1 deleteHandler, AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph) {
        this.deleteHandler = deleteHandler;
        this.typeRegistry  = typeRegistry;
        this.graph         = atlasGraph;
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

        for (AtlasObjectId id : req.getDeletedEntityIds()) {
            resp.addEntity(DELETE, constructHeader(id));
        }

        for (AtlasObjectId id : req.getUpdatedEntityIds()) {
            if (isPartialUpdate) {
                resp.addEntity(PARTIAL_UPDATE, constructHeader(id));
            }
            else {
                resp.addEntity(UPDATE, constructHeader(id));
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

    private void mapAttribute(AtlasAttribute attribute, Object attrValue, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        if (attrValue == null) {
            AtlasType attrType = attribute.getAttributeType();

            if (attrType.getTypeCategory() == TypeCategory.PRIMITIVE) {
                if (attribute.getAttributeDef().getIsOptional()) {
                    attrValue = attrType.createOptionalDefaultValue();
                } else {
                    attrValue = attrType.createDefaultValue();
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
                    deleteHandler.deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), false, true);
                }

                return newEdge;
            }

            case OBJECT_ID_TYPE: {
                String    edgeLabel    = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexProperty());
                AtlasEdge currentEdge  = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel);
                AtlasEdge newEdge      = null;

                if (ctx.getValue() != null) {
                    AtlasEntityType instanceType = getInstanceType(ctx.getValue());
                    AtlasEdge       edge         = currentEdge != null ? currentEdge : null;

                    ctx.setElementType(instanceType);
                    ctx.setExistingEdge(edge);

                    newEdge = mapObjectIdValue(ctx, context);
                    if (ctx.getAttribute().getInverseRefAttribute() != null) {
                        // Update the inverse reference on the target entity
                        addInverseReference(ctx, ctx.getAttribute().getInverseRefAttribute(), newEdge);
                    }
                }
                if (currentEdge != null && !currentEdge.equals(newEdge)) {
                    deleteHandler.deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), ctx.getAttribute().isOwnedRef(), true);
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

    private void addInverseReference(AttributeMutationContext ctx, AtlasAttribute inverseAttribute, AtlasEdge edge) throws AtlasBaseException {

        AtlasStructType inverseType = inverseAttribute.getDefinedInType();
        String propertyName = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(inverseType, inverseAttribute.getName());
        AtlasVertex vertex = edge.getOutVertex();
        AtlasVertex inverseVertex = edge.getInVertex();
        String inverseEdgeLabel = AtlasGraphUtilsV1.getEdgeLabel(propertyName);
        AtlasEdge inverseEdge = graphHelper.getEdgeForLabel(inverseVertex, inverseEdgeLabel);

        AtlasEdge newEdge;
        try {
            newEdge = graphHelper.getOrCreateEdge(inverseVertex, vertex, inverseEdgeLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        boolean inverseUpdated = true;
        switch (inverseAttribute.getAttributeType().getTypeCategory()) {
        case OBJECT_ID_TYPE:
            if (inverseEdge != null) {
                if (!inverseEdge.equals(newEdge)) {
                    // Disconnect old reference
                    deleteHandler.deleteEdgeReference(inverseEdge, inverseAttribute.getAttributeType().getTypeCategory(),
                        inverseAttribute.isOwnedRef(), true);
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
            if (elements == null) {
                elements = new ArrayList<>();
                elements.add(newEdge.getId().toString());
                inverseVertex.setProperty(propertyName, elements);
            }
            else {
               if (!elements.contains(newEdge.getId().toString())) {
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
            updateModificationMetadata(inverseVertex);
            AtlasObjectId inverseEntityId = new AtlasObjectId(AtlasGraphUtilsV1.getIdFromVertex(inverseVertex), inverseType.getTypeName());
            RequestContextV1.get().recordEntityUpdate(inverseEntityId);
        }
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
                        addInverseReference(mapCtx, inverseRefAttribute, newEdge);
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

        AtlasAttribute attribute       = ctx.getAttribute();
        List           newElements     = (List) ctx.getValue();
        AtlasArrayType arrType         = (AtlasArrayType) attribute.getAttributeType();
        AtlasType      elementType     = arrType.getElementType();
        List<Object>   currentElements = getArrayElementsProperty(elementType, ctx.getReferringVertex(), ctx.getVertexProperty());
        boolean isReference = AtlasGraphUtilsV1.isReference(elementType);
        AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();
        List<Object> newElementsCreated = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(newElements)) {
            for (int index = 0; index < newElements.size(); index++) {
                AtlasEdge               existingEdge = getEdgeAt(currentElements, index, elementType);
                AttributeMutationContext arrCtx      = new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), ctx.getAttribute(), newElements.get(index),
                                                                                     ctx.getVertexProperty(), elementType, existingEdge);

                Object newEntry = mapCollectionElementsToVertex(arrCtx, context);
                if (isReference && newEntry instanceof AtlasEdge && inverseRefAttribute != null) {
                    // Update the inverse reference value.
                    AtlasEdge newEdge = (AtlasEdge) newEntry;
                    addInverseReference(arrCtx, inverseRefAttribute, newEdge);
                }
                newElementsCreated.add(newEntry);
            }
        }

        if (isReference) {
            List<AtlasEdge> additionalEdges = removeUnusedArrayEntries(attribute, (List) currentElements, (List) newElementsCreated);
            newElementsCreated.addAll(additionalEdges);
        }

        // for dereference on way out
        setArrayElementsProperty(elementType, ctx.getReferringVertex(), ctx.getVertexProperty(), newElementsCreated);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapArrayValue({})", ctx);
        }

        return newElementsCreated;
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

    private void updateModificationMetadata(AtlasVertex vertex) {
        AtlasGraphUtilsV1.setProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContextV1.get().getRequestTime());
        GraphHelper.setProperty(vertex, Constants.MODIFIED_BY_KEY, RequestContextV1.get().getUser());
    }

    private int getEntityVersion(AtlasEntity entity) {
        Long ret = entity != null ? entity.getVersion() : null;

        return (ret != null) ? ret.intValue() : 0;
    }

    private AtlasStructType getStructType(String typeName) throws AtlasBaseException {
        AtlasType objType = typeRegistry.getType(typeName);

        if (!(objType instanceof AtlasStructType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, typeName);
        }

        return (AtlasStructType)objType;
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
            return mapObjectIdValue(ctx, context);

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
                    boolean deleted = deleteHandler.deleteEdgeReference(currentEdge, mapType.getValueType().getTypeCategory(), attribute.isOwnedRef(), true);

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
        String currentEntityId = AtlasGraphUtilsV1.getIdFromVertex(currentVertex);
        String newEntityId = AtlasGraphUtilsV1.getIdFromVertex(entityVertex);

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

    public static List<Object> getArrayElementsProperty(AtlasType elementType, AtlasVertex vertex, String vertexPropertyName) {
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            return (List)vertex.getListProperty(vertexPropertyName, AtlasEdge.class);
        }
        else {
            return (List)vertex.getListProperty(vertexPropertyName);
        }
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
    private List<AtlasEdge> removeUnusedArrayEntries(AtlasAttribute attribute, List<AtlasEdge> currentEntries, List<AtlasEdge> newEntries) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(currentEntries)) {
            AtlasStructType entityType = attribute.getDefinedInType();
            AtlasType       entryType  = ((AtlasArrayType)attribute.getAttributeType()).getElementType();

            if (AtlasGraphUtilsV1.isReference(entryType)) {
                Collection<AtlasEdge> edgesToRemove = CollectionUtils.subtract(currentEntries, newEntries);

                if (CollectionUtils.isNotEmpty(edgesToRemove)) {
                    List<AtlasEdge> additionalElements = new ArrayList<>();

                    for (AtlasEdge edge : edgesToRemove) {
                        boolean deleted = deleteHandler.deleteEdgeReference(edge, entryType.getTypeCategory(), attribute.isOwnedRef(), true);

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

        header.setGuid(AtlasGraphUtilsV1.getIdFromVertex(vertex));

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

    public void addClassifications(final EntityMutationContext context, String guid, List<AtlasClassification> classifications)
        throws AtlasBaseException {

        if ( CollectionUtils.isNotEmpty(classifications)) {

            AtlasVertex instanceVertex = AtlasGraphUtilsV1.findByGuid(guid);
            if (instanceVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            String entityTypeName = AtlasGraphUtilsV1.getTypeName(instanceVertex);

            final AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);

            for (AtlasClassification classification : classifications) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mapping classification {}", classification);
                }

                GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, classification.getTypeName());
                // add a new AtlasVertex for the struct or trait instance
                AtlasVertex classificationVertex = createClassificationVertex(classification);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("created vertex {} for trait {}", string(classificationVertex), classification.getTypeName());
                }

                // add the attributes for the trait instance
                mapClassification(EntityOperation.CREATE, context, classification, entityType, instanceVertex, classificationVertex);
            }
        }
    }

    public void updateClassification(final EntityMutationContext context, String guid, AtlasClassification classification)
                                     throws AtlasBaseException {

        AtlasVertex instanceVertex = AtlasGraphUtilsV1.findByGuid(guid);

        if (instanceVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String entityTypeName = AtlasGraphUtilsV1.getTypeName(instanceVertex);

        final AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating classification {} for entity {}", classification, guid);
        }

        // get the classification vertex from entity
        String      relationshipLabel    = GraphHelper.getTraitLabel(entityTypeName, classification.getTypeName());
        AtlasEdge   classificationEdge   = graphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
        AtlasVertex classificationVertex = classificationEdge.getInVertex();

        if (LOG.isDebugEnabled()) {
            LOG.debug("updating vertex {} for trait {}", string(classificationVertex), classification.getTypeName());
        }

        mapClassification(EntityOperation.UPDATE, context, classification, entityType, instanceVertex, classificationVertex);
    }

    private AtlasEdge mapClassification(EntityOperation operation,  final EntityMutationContext context, AtlasClassification classification, AtlasEntityType entityType, AtlasVertex parentInstanceVertex, AtlasVertex traitInstanceVertex)
        throws AtlasBaseException {

        // map all the attributes to this newly created AtlasVertex
        mapAttributes(classification, traitInstanceVertex, operation, context);

        // add an edge to the newly created AtlasVertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getTypeName(), classification.getTypeName());
        try {
           return graphHelper.getOrCreateEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }

    public void deleteClassifications(String guid) throws AtlasBaseException {

        AtlasVertex instanceVertex = AtlasGraphUtilsV1.findByGuid(guid);
        if (instanceVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);

        deleteClassifications(guid, traitNames);
    }

    public void deleteClassifications(String guid, List<String> classificationNames) throws AtlasBaseException {

        AtlasVertex instanceVertex = AtlasGraphUtilsV1.findByGuid(guid);
        if (instanceVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);

        validateClassificationExists(traitNames, classificationNames);

        for (String classificationName : classificationNames) {
            try {
                final String entityTypeName = GraphHelper.getTypeName(instanceVertex);
                String relationshipLabel = GraphHelper.getTraitLabel(entityTypeName, classificationName);
                AtlasEdge edge = graphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
                if (edge != null) {
                    deleteHandler.deleteEdgeReference(edge, TypeCategory.CLASSIFICATION, false, true);

                    // update the traits in entity once trait removal is successful
                    traitNames.remove(classificationName);

                }
            } catch (Exception e) {
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
            }
        }

        // remove the key
        instanceVertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);

        // add it back again
        for (String traitName : traitNames) {
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
        }
        updateModificationMetadata(instanceVertex);
    }

    private void validateClassificationExists(List<String> existingClassifications, List<String> suppliedClassifications) throws AtlasBaseException {
        Set<String> existingNames = new HashSet<>(existingClassifications);
        for (String classificationName : suppliedClassifications) {
            if (!existingNames.contains(classificationName)) {
                throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classificationName);
            }
        }
    }
}
