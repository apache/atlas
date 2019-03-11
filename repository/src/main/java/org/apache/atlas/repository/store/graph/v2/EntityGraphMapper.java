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


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TimeBoundary;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes;
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
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.model.TypeCategory.CLASSIFICATION;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.instance.AtlasRelatedObjectId.KEY_RELATIONSHIP_ATTRIBUTES;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.PARTIAL_UPDATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.SET;
import static org.apache.atlas.repository.Constants.ATTRIBUTE_KEY_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_ENTITY_GUID;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_ENTITY_STATUS;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_LABEL;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_VALIDITY_PERIODS_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_VERTEX_PROPAGATE_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY;
import static org.apache.atlas.repository.Constants.CREATED_BY_KEY;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.HOME_ID_KEY;
import static org.apache.atlas.repository.Constants.IS_PROXY_KEY;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.PROVENANCE_TYPE_KEY;
import static org.apache.atlas.repository.Constants.RELATIONSHIP_GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.SUPER_TYPES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.ATTRIBUTE_INDEX_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERSION_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.getCollectionElementsUsingRelationship;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationEdge;
import static org.apache.atlas.repository.graph.GraphHelper.getClassificationVertex;
import static org.apache.atlas.repository.graph.GraphHelper.getDefaultRemovePropagations;
import static org.apache.atlas.repository.graph.GraphHelper.getMapElementsProperty;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitLabel;
import static org.apache.atlas.repository.graph.GraphHelper.getTraitNames;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeNames;
import static org.apache.atlas.repository.graph.GraphHelper.isActive;
import static org.apache.atlas.repository.graph.GraphHelper.isPropagationEnabled;
import static org.apache.atlas.repository.graph.GraphHelper.isRelationshipEdge;
import static org.apache.atlas.repository.graph.GraphHelper.string;
import static org.apache.atlas.repository.graph.GraphHelper.updateModificationMetadata;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromVertex;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;

@Component
public class EntityGraphMapper {
    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphMapper.class);

    private static final String SOFT_REF_FORMAT      = "%s:%s";
    private static final int INDEXED_STR_SAFE_LEN = AtlasConfiguration.GRAPHSTORE_INDEXED_STRING_SAFE_LENGTH.getInt();

    private final GraphHelper               graphHelper = GraphHelper.getInstance();
    private final AtlasGraph                graph;
    private final DeleteHandlerDelegate     deleteDelegate;
    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasRelationshipStore    relationshipStore;
    private final AtlasEntityChangeNotifier entityChangeNotifier;
    private final AtlasInstanceConverter    instanceConverter;
    private final EntityGraphRetriever      entityRetriever;

    @Inject
    public EntityGraphMapper(DeleteHandlerDelegate deleteDelegate, AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph,
                             AtlasRelationshipStore relationshipStore, AtlasEntityChangeNotifier entityChangeNotifier,
                             AtlasInstanceConverter instanceConverter) {
        this.deleteDelegate       = deleteDelegate;
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
            AtlasGraphUtilsV2.addEncodedProperty(ret, SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        AtlasGraphUtilsV2.setEncodedProperty(ret, GUID_PROPERTY_KEY, guid);
        AtlasGraphUtilsV2.setEncodedProperty(ret, VERSION_PROPERTY_KEY, getEntityVersion(entity));

        GraphTransactionInterceptor.addToVertexCache(guid, ret);

        return ret;
    }

    public void updateSystemAttributes(AtlasVertex vertex, AtlasEntity entity) {
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
    }

    public EntityMutationResponse mapAttributesAndClassifications(EntityMutationContext context, final boolean isPartialUpdate, final boolean replaceClassifications) throws AtlasBaseException {
        MetricRecorder metric = RequestContext.get().startMetricRecord("mapAttributesAndClassifications");

        EntityMutationResponse resp = new EntityMutationResponse();

        Collection<AtlasEntity> createdEntities = context.getCreatedEntities();
        Collection<AtlasEntity> updatedEntities = context.getUpdatedEntities();

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntity createdEntity : createdEntities) {
                String          guid       = createdEntity.getGuid();
                AtlasVertex     vertex     = context.getVertex(guid);
                AtlasEntityType entityType = context.getType(guid);

                mapRelationshipAttributes(createdEntity, entityType, vertex, CREATE, context);

                mapAttributes(createdEntity, entityType, vertex, CREATE, context);

                resp.addEntity(CREATE, constructHeader(createdEntity, entityType, vertex));
                addClassifications(context, guid, createdEntity.getClassifications());
            }
        }

        if (CollectionUtils.isNotEmpty(updatedEntities)) {
            for (AtlasEntity updatedEntity : updatedEntities) {
                String          guid       = updatedEntity.getGuid();
                AtlasVertex     vertex     = context.getVertex(guid);
                AtlasEntityType entityType = context.getType(guid);

                mapRelationshipAttributes(updatedEntity, entityType, vertex, UPDATE, context);

                mapAttributes(updatedEntity, entityType, vertex, UPDATE, context);

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

        if (CollectionUtils.isNotEmpty(context.getEntitiesToDelete())) {
            deleteDelegate.getHandler().deleteEntities(context.getEntitiesToDelete());
        }

        RequestContext req = RequestContext.get();

        for (AtlasEntityHeader entity : req.getDeletedEntities()) {
            resp.addEntity(DELETE, entity);
        }

        for (AtlasEntityHeader entity : req.getUpdatedEntities()) {
            if (isPartialUpdate) {
                resp.addEntity(PARTIAL_UPDATE, entity);
            }
            else {
                resp.addEntity(UPDATE, entity);
            }
        }

        RequestContext.get().endMetricRecord(metric);

        return resp;
    }

    private AtlasVertex createStructVertex(AtlasStruct struct) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createStructVertex({})", struct.getTypeName());
        }

        final AtlasVertex ret = graph.addVertex();

        AtlasGraphUtilsV2.setEncodedProperty(ret, ENTITY_TYPE_PROPERTY_KEY, struct.getTypeName());
        AtlasGraphUtilsV2.setEncodedProperty(ret, STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
        AtlasGraphUtilsV2.setEncodedProperty(ret, TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        AtlasGraphUtilsV2.setEncodedProperty(ret, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        AtlasGraphUtilsV2.setEncodedProperty(ret, CREATED_BY_KEY, RequestContext.get().getUser());
        AtlasGraphUtilsV2.setEncodedProperty(ret, MODIFIED_BY_KEY, RequestContext.get().getUser());

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

            RequestContext.get().endMetricRecord(metric);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== mapAttributes({}, {})", op, struct.getTypeName());
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

            } else if (op.equals(UPDATE)) {
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

    private void mapAttribute(AtlasAttribute attribute, Object attrValue, AtlasVertex vertex, EntityOperation op, EntityMutationContext context) throws AtlasBaseException {
        if (attrValue == null) {
            AtlasAttributeDef attributeDef = attribute.getAttributeDef();
            AtlasType         attrType     = attribute.getAttributeType();

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

            if (!requestContext.isDeletedEntity(GraphHelper.getGuid(inverseVertex))) {
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

        String parentGuid = GraphHelper.getGuid(inverseVertex);
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
        boolean isIndexableStrAttr = ctx.getAttributeDef().getIsIndexable() && ctx.getAttrType() instanceof AtlasBuiltInTypes.AtlasStringType;

        Object ret = ctx.getValue();

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
                    LOG.warn("Length of indexed attribute {} is {} characters, longer than safe-limit {}; trimming to {} - attempt #{}", ctx.getAttribute().getQualifiedName(), value.length(), INDEXED_STR_SAFE_LEN, trimmedLength, requestContext.getAttemptCount());

                    String checksumSuffix = ":" + DigestUtils.shaHex(value); // Storing SHA checksum in case verification is needed after retrieval

                    ret = value.substring(0, trimmedLength - checksumSuffix.length()) + checksumSuffix;
                } else {
                    LOG.warn("Length of indexed attribute {} is {} characters, longer than safe-limit {}", ctx.getAttribute().getQualifiedName(), value.length(), INDEXED_STR_SAFE_LEN);
                }
            }
        }

        AtlasGraphUtilsV2.setEncodedProperty(ctx.getReferringVertex(), ctx.getVertexProperty(), ret);

        String uniqPropName = ctx.getAttribute() != null ? ctx.getAttribute().getVertexUniquePropertyName() : null;

        if (uniqPropName != null) {
            if (context.isDeletedEntity(ctx.getReferringVertex()) || AtlasGraphUtilsV2.getState(ctx.getReferringVertex()) == DELETED) {
                ctx.getReferringVertex().removeProperty(uniqPropName);
            } else {
                AtlasGraphUtilsV2.setEncodedProperty(ctx.getReferringVertex(), uniqPropName, ret);
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
            }

            ret = ctx.getCurrentEdge();
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

        AtlasVertex attributeVertex = context.getDiscoveryContext().getResolvedEntityVertex(getGuid(ctx.getValue()));
        AtlasVertex entityVertex    = ctx.getReferringVertex();
        AtlasEdge   ret;

        if (attributeVertex == null) {
            AtlasObjectId objectId = getObjectId(ctx.getValue());

            attributeVertex = (objectId != null) ? context.getDiscoveryContext().getResolvedEntityVertex(objectId) : null;
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

                if (ctx.getCurrentEdge() != null) {
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

                    boolean isCreated = GraphHelper.getCreatedTime(ret) == RequestContext.get().getRequestTime();

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

        Map<Object, Object> newVal      = (Map<Object, Object>) ctx.getValue();
        Map<String, Object> newMap      = new HashMap<>();
        AtlasMapType        mapType     = (AtlasMapType) ctx.getAttrType();
        AtlasAttribute      attribute   = ctx.getAttribute();
        Map<String, Object> currentMap  = getMapElementsProperty(mapType, ctx.getReferringVertex(), ctx.getVertexProperty(), attribute);
        boolean             isReference = isReference(mapType.getValueType());
        boolean             isSoftReference = ctx.getAttribute().getAttributeDef().isSoftReferenced();

        if (MapUtils.isNotEmpty(newVal)) {
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
                ctx.getReferringVertex().setProperty(propertyName, new HashMap<>(newVal));

                newVal.forEach((key, value) -> newMap.put(key.toString(), value));
            }

            if (isSoftReference) {
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
        boolean        isReference         = isReference(elementType);
        boolean        isSoftReference     = ctx.getAttribute().getAttributeDef().isSoftReferenced();
        AtlasAttribute inverseRefAttribute = attribute.getInverseRefAttribute();
        Cardinality    cardinality         = attribute.getAttributeDef().getCardinality();
        List<Object>   newElementsCreated  = new ArrayList<>();
        List<Object>   currentElements;

        if (isReference && !isSoftReference) {
            currentElements = (List) getCollectionElementsUsingRelationship(ctx.getReferringVertex(), attribute);
        } else {
            currentElements = (List) getArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty());
        }

        if (CollectionUtils.isNotEmpty(newElements)) {
            if (cardinality == SET) {
                newElements = (List) newElements.stream().distinct().collect(Collectors.toList());
            }

            for (int index = 0; index < newElements.size(); index++) {
                AtlasEdge               existingEdge = (isSoftReference) ? null : getEdgeAt(currentElements, index, elementType);
                AttributeMutationContext arrCtx      = new AttributeMutationContext(ctx.getOp(), ctx.getReferringVertex(), ctx.getAttribute(), newElements.get(index),
                                                                                     ctx.getVertexProperty(), elementType, existingEdge);

                Object newEntry = mapCollectionElementsToVertex(arrCtx, context);

                if (isReference && newEntry != null && newEntry instanceof AtlasEdge && inverseRefAttribute != null) {
                    // Update the inverse reference value.
                    AtlasEdge newEdge = (AtlasEdge) newEntry;

                    addInverseReference(context, inverseRefAttribute, newEdge, getRelationshipAttributes(ctx.getValue()));
                }

                if(newEntry != null) {
                    newElementsCreated.add(newEntry);
                }
            }
        }

        if (isReference && !isSoftReference) {
            List<AtlasEdge> additionalEdges = removeUnusedArrayEntries(attribute, (List) currentElements, (List) newElementsCreated, ctx.getReferringVertex());
            newElementsCreated.addAll(additionalEdges);
        }

        // add index to attributes of array type
       for (int index = 0; index < newElementsCreated.size(); index++) {
           Object element = newElementsCreated.get(index);

           if (element instanceof AtlasEdge) {
               AtlasGraphUtilsV2.setEncodedProperty((AtlasEdge) element, ATTRIBUTE_INDEX_PROPERTY_KEY, index);
            }
        }

        // for dereference on way out
        setArrayElementsProperty(elementType, isSoftReference, ctx.getReferringVertex(), ctx.getVertexProperty(), newElementsCreated);

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
        if (!isSoftReference && isReference(elementType)) {
            return (List)vertex.getListProperty(vertexPropertyName, AtlasEdge.class);
        }
        else {
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

    //Removes unused edges from the old collection, compared to the new collection

    private List<AtlasEdge> removeUnusedArrayEntries(AtlasAttribute attribute, List<AtlasEdge> currentEntries, List<AtlasEdge> newEntries, AtlasVertex entityVertex) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(currentEntries)) {
            AtlasType entryType = ((AtlasArrayType) attribute.getAttributeType()).getElementType();

            if (isReference(entryType)) {
                Collection<AtlasEdge> edgesToRemove = CollectionUtils.subtract(currentEntries, newEntries);

                if (CollectionUtils.isNotEmpty(edgesToRemove)) {
                    List<AtlasEdge> additionalElements = new ArrayList<>();

                    for (AtlasEdge edge : edgesToRemove) {
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
    private void setArrayElementsProperty(AtlasType elementType, boolean isSoftReference, AtlasVertex vertex, String vertexPropertyName, List<Object> values) {
        if (!isReference(elementType) || isSoftReference) {
            AtlasGraphUtilsV2.setEncodedProperty(vertex, vertexPropertyName, values);
        }
    }

    private AtlasEntityHeader constructHeader(AtlasEntity entity, final AtlasEntityType type, AtlasVertex vertex) {
        AtlasEntityHeader header = new AtlasEntityHeader(entity.getTypeName());

        header.setGuid(getIdFromVertex(vertex));
        header.setStatus(entity.getStatus());

        for (AtlasAttribute attribute : type.getUniqAttributes().values()) {
            header.setAttribute(attribute.getName(), entity.getAttribute(attribute.getName()));
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

    public void addClassifications(final EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(classifications)) {
            MetricRecorder metric = RequestContext.get().startMetricRecord("addClassifications");

            final AtlasVertex                           entityVertex          = context.getVertex(guid);
            final AtlasEntityType                       entityType            = context.getType(guid);
            List<AtlasVertex>                           entitiesToPropagateTo = null;
            Map<AtlasVertex, List<AtlasClassification>> propagations          = null;
            List<AtlasClassification>                   addClassifications    = new ArrayList<>(classifications.size());

            for (AtlasClassification c : classifications) {
                AtlasClassification classification      = new AtlasClassification(c);
                String              classificationName  = classification.getTypeName();
                Boolean             propagateTags       = classification.isPropagate();
                Boolean             removePropagations  = classification.getRemovePropagationsOnEntityDelete();

                if (propagateTags != null && propagateTags &&
                        classification.getEntityGuid() != null &&
                        !StringUtils.equals(classification.getEntityGuid(), guid)) {
                    continue;
                }

                if (propagateTags == null) {
                    RequestContext reqContext = RequestContext.get();

                    if(reqContext.isImportInProgress() || reqContext.isInNotificationProcessing()) {
                        propagateTags = false;
                        classification.setPropagate(propagateTags);
                    } else {
                        propagateTags = true;
                    }
                }

                if (removePropagations == null) {
                    removePropagations = getDefaultRemovePropagations();

                    classification.setRemovePropagationsOnEntityDelete(removePropagations);
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

                AtlasGraphUtilsV2.addEncodedProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, classificationName);

                // add a new AtlasVertex for the struct or trait instance
                AtlasVertex classificationVertex = createClassificationVertex(classification);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("created vertex {} for trait {}", string(classificationVertex), classificationName);
                }

                // add the attributes for the trait instance
                mapClassification(EntityOperation.CREATE, context, classification, entityType, entityVertex, classificationVertex);
                updateModificationMetadata(entityVertex);

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
                            LOG.debug("Propagating tag: [{}][{}] to {}", classificationName, entityType.getTypeName(), getTypeNames(entitiesToPropagateTo));
                        }

                        List<AtlasVertex> entitiesPropagatedTo = deleteDelegate.getHandler().addTagPropagation(classificationVertex, entitiesToPropagateTo);

                        if (entitiesPropagatedTo != null) {
                            for (AtlasVertex entityPropagatedTo : entitiesPropagatedTo) {
                                propagations.get(entityPropagatedTo).add(classification);
                            }
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

            for (AtlasVertex vertex : notificationVertices) {
                String                    entityGuid           = GraphHelper.getGuid(vertex);
                AtlasEntity               entity               = instanceConverter.getAndCacheEntity(entityGuid);
                List<AtlasClassification> addedClassifications = StringUtils.equals(entityGuid, guid) ? addClassifications : propagations.get(vertex);

                if (CollectionUtils.isNotEmpty(addedClassifications)) {
                    entityChangeNotifier.onClassificationAddedToEntity(entity, addedClassifications);
                }
            }

            RequestContext.get().endMetricRecord(metric);
        }
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

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        deleteDelegate.getHandler().deletePropagatedClassification(entityVertex, classificationName, associatedEntityGuid);
    }

    public void deleteClassification(String entityGuid, String classificationName) throws AtlasBaseException {
        if (StringUtils.isEmpty(classificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "delete", entityGuid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(entityGuid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, entityGuid);
        }

        List<String> traitNames = getTraitNames(entityVertex);

        if (CollectionUtils.isEmpty(traitNames)) {
            throw new AtlasBaseException(AtlasErrorCode.NO_CLASSIFICATIONS_FOUND_FOR_ENTITY, entityGuid);
        }

        validateClassificationExists(traitNames, classificationName);

        Map<AtlasVertex, List<AtlasClassification>> removedClassifications = new HashMap<>();

        AtlasVertex         classificationVertex = getClassificationVertex(entityVertex, classificationName);
        AtlasClassification classification       = entityRetriever.toAtlasClassification(classificationVertex);

        // remove classification from propagated entities if propagation is turned on
        if (isPropagationEnabled(classificationVertex)) {
            List<AtlasVertex> propagatedEntityVertices = deleteDelegate.getHandler().removeTagPropagation(classificationVertex);

            // add propagated entities and deleted classification details to removeClassifications map
            if (CollectionUtils.isNotEmpty(propagatedEntityVertices)) {
                for (AtlasVertex propagatedEntityVertex : propagatedEntityVertices) {
                    List<AtlasClassification> classifications = removedClassifications.get(propagatedEntityVertex);

                    if (classifications == null) {
                        classifications = new ArrayList<>();

                        removedClassifications.put(propagatedEntityVertex, classifications);
                    }

                    classifications.add(classification);
                }
            }
        }

        // add associated entity and deleted classification details to removeClassifications map
        List<AtlasClassification> classifications = removedClassifications.get(entityVertex);

        if (classifications == null) {
            classifications = new ArrayList<>();

            removedClassifications.put(entityVertex, classifications);
        }

        classifications.add(classification);

        // remove classifications from associated entity
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing classification: [{}] from: [{}][{}] with edge label: [{}]", classificationName,
                    getTypeName(entityVertex), entityGuid, CLASSIFICATION_LABEL);
        }

        AtlasEdge edge = getClassificationEdge(entityVertex, classificationVertex);

        deleteDelegate.getHandler().deleteEdgeReference(edge, CLASSIFICATION, false, true, entityVertex);

        traitNames.remove(classificationName);

        updateTraitNamesProperty(entityVertex, traitNames);

        updateModificationMetadata(entityVertex);

        for (Map.Entry<AtlasVertex, List<AtlasClassification>> entry : removedClassifications.entrySet()) {
            String                    guid                       = GraphHelper.getGuid(entry.getKey());
            List<AtlasClassification> deletedClassificationNames = entry.getValue();
            AtlasEntity               entity                     = instanceConverter.getAndCacheEntity(guid);

            entityChangeNotifier.onClassificationDeletedFromEntity(entity, deletedClassificationNames);
        }
    }

    public void updateClassifications(EntityMutationContext context, String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_CLASSIFICATION_PARAMS, "update", guid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String                    entityTypeName         = AtlasGraphUtilsV2.getTypeName(entityVertex);
        AtlasEntityType           entityType             = typeRegistry.getEntityTypeByName(entityTypeName);
        List<AtlasClassification> updatedClassifications = new ArrayList<>();
        List<AtlasVertex>         entitiesToPropagateTo  = new ArrayList<>();
        Set<AtlasVertex>          notificationVertices   = new HashSet<AtlasVertex>() {{ add(entityVertex); }};

        Map<AtlasVertex, List<AtlasClassification>> addedPropagations   = null;
        Map<AtlasVertex, List<AtlasClassification>> removedPropagations = null;

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

            // check for removePropagationsOnEntityDelete update
            Boolean currentRemovePropagations = currentClassification.getRemovePropagationsOnEntityDelete();
            Boolean updatedRemovePropagations = classification.getRemovePropagationsOnEntityDelete();

            if (updatedRemovePropagations != null && (updatedRemovePropagations != currentRemovePropagations)) {
                AtlasGraphUtilsV2.setEncodedProperty(classificationVertex, CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY, updatedRemovePropagations);

                isClassificationUpdated = true;
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

            // handle update of 'propagate' flag
            Boolean currentTagPropagation = currentClassification.isPropagate();
            Boolean updatedTagPropagation = classification.isPropagate();

            // compute propagatedEntityVertices once and use it for subsequent iterations and notifications
            if (updatedTagPropagation != null && currentTagPropagation != updatedTagPropagation) {
                if (updatedTagPropagation) {
                    if (CollectionUtils.isEmpty(entitiesToPropagateTo)) {
                        entitiesToPropagateTo = graphHelper.getImpactedVerticesWithRestrictions(guid, classificationVertex.getIdForDisplay());
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
                        if (removedPropagations == null) {
                            removedPropagations = new HashMap<>();

                            for (AtlasVertex impactedVertex : impactedVertices) {
                                List<AtlasClassification> removedClassifications = removedPropagations.get(impactedVertex);

                                if (removedClassifications == null) {
                                    removedClassifications = new ArrayList<>();

                                    removedPropagations.put(impactedVertex, removedClassifications);
                                }

                                removedClassifications.add(classification);
                            }
                        }
                    }
                }
            }

            updatedClassifications.add(currentClassification);
        }

        if (CollectionUtils.isNotEmpty(entitiesToPropagateTo)) {
            notificationVertices.addAll(entitiesToPropagateTo);
        }

        for (AtlasVertex vertex : notificationVertices) {
            String      entityGuid = GraphHelper.getGuid(vertex);
            AtlasEntity entity     = instanceConverter.getAndCacheEntity(entityGuid);

            if (isActive(entity)) {
                entityChangeNotifier.onClassificationUpdatedToEntity(entity, updatedClassifications);
            }
        }

        if (removedPropagations != null) {
            for (Map.Entry<AtlasVertex, List<AtlasClassification>> entry : removedPropagations.entrySet()) {
                AtlasVertex               vertex                 = entry.getKey();
                List<AtlasClassification> removedClassifications = entry.getValue();
                String                    entityGuid             = GraphHelper.getGuid(vertex);
                AtlasEntity               entity                 = instanceConverter.getAndCacheEntity(entityGuid);

                if (isActive(entity)) {
                    entityChangeNotifier.onClassificationDeletedFromEntity(entity, removedClassifications);
                }
            }
        }
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

        // map all the attributes to this newly created AtlasVertex
        mapAttributes(classification, traitInstanceVertex, operation, context);

        AtlasEdge ret = getClassificationEdge(parentInstanceVertex, traitInstanceVertex);

        if (ret == null) {
            ret = graphHelper.addClassificationEdge(parentInstanceVertex, traitInstanceVertex, false);
        }

        return ret;
    }

    public void deleteClassifications(String guid) throws AtlasBaseException {
        AtlasVertex instanceVertex = AtlasGraphUtilsV2.findByGuid(guid);

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

    private void updateTraitNamesProperty(AtlasVertex entityVertex, List<String> traitNames) {
        if (entityVertex != null) {
            entityVertex.removeProperty(TRAIT_NAMES_PROPERTY_KEY);

            for (String traitName : traitNames) {
                AtlasGraphUtilsV2.addEncodedProperty(entityVertex, TRAIT_NAMES_PROPERTY_KEY, traitName);
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
        RequestContext req = RequestContext.get();

        if (!req.isUpdatedEntity(GraphHelper.getGuid(vertex))) {
            updateModificationMetadata(vertex);

            req.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(vertex));
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
}
