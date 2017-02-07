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

import com.google.common.base.Optional;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StructVertexMapper implements InstanceGraphMapper<AtlasEdge> {

    private final AtlasGraph graph;

    private final GraphHelper graphHelper = GraphHelper.getInstance();

    private final MapVertexMapper mapVertexMapper;

    private final ArrayVertexMapper arrVertexMapper;

    private EntityGraphMapper entityVertexMapper;

    private DeleteHandlerV1 deleteHandler;

    private static final Logger LOG = LoggerFactory.getLogger(StructVertexMapper.class);

    public StructVertexMapper(ArrayVertexMapper arrayVertexMapper, MapVertexMapper mapVertexMapper, DeleteHandlerV1 deleteHandler) {
        this.graph = AtlasGraphProvider.getGraphInstance();;
        this.mapVertexMapper = mapVertexMapper;
        this.arrVertexMapper = arrayVertexMapper;
        this.deleteHandler = deleteHandler;
    }

    void init(final EntityGraphMapper entityVertexMapper) {
        this.entityVertexMapper = entityVertexMapper;
    }

    @Override
    public AtlasEdge toGraph(GraphMutationContext ctx) throws AtlasBaseException {
        AtlasEdge ret = null;

        if ( ctx.getCurrentEdge().isPresent() ) {
            updateVertex(ctx.getParentType(), (AtlasStructType) ctx.getAttrType(), ctx.getAttributeDef(), (AtlasStruct) ctx.getValue(), ctx.getCurrentEdge().get().getInVertex());
            ret = ctx.getCurrentEdge().get();
        } else if (ctx.getValue() != null) {
            String edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexPropertyKey());
            ret = createVertex(ctx.getParentType(), (AtlasStructType) ctx.getAttrType(), ctx.getAttributeDef(), (AtlasStruct) ctx.getValue(), ctx.getReferringVertex(), edgeLabel);
        }

        return ret;
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
    }

    public static boolean shouldManageChildReferences(AtlasStructType type, String attributeName) {
        AtlasStructType.AtlasAttribute attribute = type.getAttribute(attributeName);

        return attribute != null ? attribute.isOwnedRef() : false;
    }

    /**
     * Map attributes for entity, struct or trait
     *
     * @param op
     * @param structType
     * @param struct
     * @param vertex
     * @return
     * @throws AtlasBaseException
     */
    public AtlasVertex mapAttributestoVertex(final EntityMutations.EntityOperation op, AtlasStructType structType, AtlasStruct struct, AtlasVertex vertex) throws AtlasBaseException {
        if (struct.getAttributes() != null) {
            if (op.equals(EntityMutations.EntityOperation.CREATE)) {
                final Map<String, AtlasStructType.AtlasAttribute> allAttributes = structType.getAllAttributes();
                for (String attrName : allAttributes.keySet()) {
                    Object value = struct.getAttribute(attrName);

                    mapAttribute(op, structType, attrName, value, vertex);
                }
            } else if (op.equals(EntityMutations.EntityOperation.UPDATE)) {
                for (String attrName : struct.getAttributes().keySet()) {
                    Object value = struct.getAttribute(attrName);
                    mapAttribute(op, structType, attrName, value, vertex);
                }
            }
            updateModificationMetadata(vertex);
        }
        return vertex;
    }

    private void mapAttribute(final EntityMutations.EntityOperation op, AtlasStructType structType, String attrName, Object value, AtlasVertex vertex) throws AtlasBaseException {
        AtlasType attributeType = structType.getAttributeType(attrName);
        if (attributeType != null) {
            final AtlasStructType.AtlasAttribute attribute = structType.getAttribute(attrName);

            if (value == null) {
                if ( attribute.getAttributeType().getTypeCategory() == TypeCategory.PRIMITIVE) {
                    if ( attribute.getAttributeDef().getIsOptional()) {
                        value = attribute.getAttributeType().createOptionalDefaultValue();
                    } else {
                        value = attribute.getAttributeType().createDefaultValue();
                    }
                }
            }

            final String vertexProperty = structType.getQualifiedAttributeName(attrName);
            GraphMutationContext ctx = new GraphMutationContext.Builder(op, attribute, value)
                .referringVertex(vertex)
                .vertexProperty(GraphHelper.encodePropertyKey(vertexProperty)).build();
            mapToVertexByTypeCategory(ctx);
        }
    }

    private void updateModificationMetadata(AtlasVertex vertex) {
        //Set updated timestamp
        AtlasGraphUtilsV1.setProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContextV1.get().getRequestTime());
        GraphHelper.setProperty(vertex, Constants.MODIFIED_BY_KEY, RequestContextV1.get().getUser());
    }


    protected Object mapToVertexByTypeCategory(GraphMutationContext ctx) throws AtlasBaseException {
        if (ctx.getOp() == EntityMutations.EntityOperation.CREATE && ctx.getValue() == null) {
            return null;
        }

        switch (ctx.getAttrType().getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return primitivesToVertex(ctx);
        case STRUCT:
            String edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexPropertyKey());
            AtlasEdge currentEdge = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel);
            Optional<AtlasEdge> edge = currentEdge != null ? Optional.of(currentEdge) : Optional.<AtlasEdge>absent();
            ctx.setExistingEdge(edge);
            AtlasEdge newEdge = toGraph(ctx);

            if (currentEdge != null && !currentEdge.equals(newEdge)) {
                deleteHandler.deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), false, true);
            }
            return newEdge;
        case ENTITY:
            edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexPropertyKey());
            currentEdge = graphHelper.getEdgeForLabel(ctx.getReferringVertex(), edgeLabel);
            AtlasEntityType instanceType = entityVertexMapper.getInstanceType(ctx.getValue());
            edge = currentEdge != null ? Optional.of(currentEdge) : Optional.<AtlasEdge>absent();
            ctx.setElementType(instanceType);
            ctx.setExistingEdge(edge);
            newEdge = entityVertexMapper.toGraph(ctx);

            if (currentEdge != null && !currentEdge.equals(newEdge)) {
                deleteHandler.deleteEdgeReference(currentEdge, ctx.getAttrType().getTypeCategory(), shouldManageChildReferences(ctx.getParentType(), ctx.getAttributeDef().getName()), true);
            }
            return newEdge;
        case MAP:
            return mapVertexMapper.toGraph(ctx);
        case ARRAY:
            return arrVertexMapper.toGraph(ctx);
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }

    protected Object primitivesToVertex(GraphMutationContext ctx) {
        AtlasGraphUtilsV1.setProperty(ctx.getReferringVertex(), ctx.getVertexPropertyKey(), ctx.getValue());
        return ctx.getValue();
    }

    private AtlasEdge createVertex(AtlasStructType parentType, AtlasStructType attrType, AtlasAttributeDef attributeDef, AtlasStruct struct, AtlasVertex referringVertex, String edgeLabel) throws AtlasBaseException {
        AtlasVertex vertex = createVertexTemplate(struct, attrType);
        mapAttributestoVertex(EntityMutations.EntityOperation.CREATE, attrType, struct, vertex);

        try {
            //TODO - Map directly in AtlasGraphUtilsV1
            return graphHelper.getOrCreateEdge(referringVertex, vertex, edgeLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }

    private void updateVertex(AtlasStructType parentType, AtlasStructType structAttributeType, AtlasAttributeDef attributeDef, AtlasStruct value, AtlasVertex structVertex) throws AtlasBaseException {
        mapAttributestoVertex(EntityMutations.EntityOperation.CREATE, structAttributeType, value, structVertex);
    }

    protected AtlasVertex createVertexTemplate(final AtlasStruct instance, final AtlasStructType structType) {
        LOG.debug("Creating AtlasVertex for type {}", instance.getTypeName());
        final AtlasVertex vertexWithoutIdentity = graph.addVertex();

        // add type information
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.ENTITY_TYPE_PROPERTY_KEY, instance.getTypeName());

        // add state information
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());

        // add timestamp information
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, RequestContextV1.get().getRequestTime());
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
            RequestContextV1.get().getRequestTime());

        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.CREATED_BY_KEY, RequestContextV1.get().getUser());

        GraphHelper.setProperty(vertexWithoutIdentity, Constants.MODIFIED_BY_KEY, RequestContextV1.get().getUser());

        return vertexWithoutIdentity;
    }

    protected Object mapCollectionElementsToVertex(GraphMutationContext ctx) throws AtlasBaseException {
        switch(ctx.getAttrType().getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return ctx.getValue();
        case STRUCT:
            return toGraph(ctx);
        case ENTITY:
            AtlasEntityType instanceType = entityVertexMapper.getInstanceType(ctx.getValue());
            ctx.setElementType(instanceType);
            return entityVertexMapper.toGraph(ctx);
        case MAP:
        case ARRAY:
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }
}
