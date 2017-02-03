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


import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class EntityGraphMapper implements InstanceGraphMapper<AtlasEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(EntityGraphMapper.class);

    protected final GraphHelper graphHelper = GraphHelper.getInstance();

    protected EntityMutationContext context;

    protected final StructVertexMapper structVertexMapper;

    @Inject
    public EntityGraphMapper(ArrayVertexMapper arrayVertexMapper, MapVertexMapper mapVertexMapper, DeleteHandlerV1 deleteHandler) {
        this.structVertexMapper = new StructVertexMapper(arrayVertexMapper, mapVertexMapper, deleteHandler);
        arrayVertexMapper.init(structVertexMapper);
        mapVertexMapper.init(structVertexMapper);
    }

    public AtlasVertex createVertexTemplate(final AtlasStruct instance, final AtlasStructType structType) {
        AtlasVertex vertex = structVertexMapper.createVertexTemplate(instance, structType);
        
        AtlasEntityType entityType = (AtlasEntityType) structType;
        AtlasEntity entity = (AtlasEntity) instance;

        // add super types
        for (String superTypeName : entityType.getAllSuperTypes()) {
            AtlasGraphUtilsV1.addProperty(vertex, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        final String guid = UUID.randomUUID().toString();

        // add identity
        AtlasGraphUtilsV1.setProperty(vertex, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        AtlasGraphUtilsV1.setProperty(vertex, Constants.VERSION_PROPERTY_KEY, Integer.valueOf(entity.getVersion().intValue()));

        return vertex;
    }


    @Override
    public AtlasEdge toGraph(GraphMutationContext ctx) throws AtlasBaseException {
        AtlasEdge result = null;

        AtlasObjectId guid = getId(ctx.getValue());
        AtlasVertex entityVertex = context.getDiscoveryContext().getResolvedEntityVertex(guid);
        if ( ctx.getCurrentEdge().isPresent() ) {
            result = updateEdge(ctx.getAttributeDef(), ctx.getValue(), ctx.getCurrentEdge().get(), entityVertex);
        } else if (ctx.getValue() != null) {
            String edgeLabel = AtlasGraphUtilsV1.getEdgeLabel(ctx.getVertexPropertyKey());
            try {
                result = graphHelper.getOrCreateEdge(ctx.getReferringVertex(), entityVertex, edgeLabel);
            } catch (RepositoryException e) {
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
            }
        }

        return result;
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
    }

    private AtlasEdge updateEdge(AtlasAttributeDef attributeDef, Object value,  AtlasEdge currentEdge, final AtlasVertex entityVertex) throws AtlasBaseException {

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

    public EntityMutationResponse
    mapAttributes(EntityMutationContext ctx) throws AtlasBaseException {

        this.context = ctx;
        structVertexMapper.init(this);

        EntityMutationResponse resp = new EntityMutationResponse();
        //Map attributes
        if (ctx.getCreatedEntities() != null) {
            for (AtlasEntity createdEntity : ctx.getCreatedEntities()) {
                AtlasVertex vertex = ctx.getVertex(createdEntity);
                structVertexMapper.mapAttributestoVertex(EntityMutations.EntityOperation.CREATE, ctx.getType(createdEntity), createdEntity, vertex);
                resp.addEntity(EntityMutations.EntityOperation.CREATE, constructHeader(createdEntity, ctx.getType(createdEntity), vertex));
            }
        }

        if (ctx.getUpdatedEntities() != null) {
            for (AtlasEntity updated : ctx.getUpdatedEntities()) {
                AtlasVertex vertex = ctx.getVertex(updated);
                structVertexMapper.mapAttributestoVertex(EntityMutations.EntityOperation.UPDATE, ctx.getType(updated), updated, vertex);

                resp.addEntity(EntityMutations.EntityOperation.UPDATE, constructHeader(updated, ctx.getType(updated), vertex));
            }
        }

        RequestContextV1 req = RequestContextV1.get();
        for (AtlasObjectId id : req.getDeletedEntityIds()) {
            resp.addEntity(EntityMutations.EntityOperation.DELETE, constructHeader(id));
        }

        return resp;
    }


    public AtlasObjectId getId(Object value) throws AtlasBaseException {
        if (value != null) {
            if ( value instanceof  AtlasObjectId) {
                return ((AtlasObjectId) value);
            } else if (value instanceof AtlasEntity) {
                return ((AtlasEntity) value).getAtlasObjectId();
            } else if (value instanceof Map) {
                AtlasObjectId ret = new AtlasObjectId((Map)value);

                if (ret.isValid()) {
                    return ret;
                }
            }

            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, (String) value);
        }

        return null;
    }

    private AtlasEntityHeader constructHeader(AtlasEntity entity, final AtlasEntityType type, AtlasVertex vertex) {
        //TODO - enhance to return only selective attributes
        AtlasEntityHeader header = new AtlasEntityHeader(entity.getTypeName(), AtlasGraphUtilsV1.getIdFromVertex(vertex), entity.getAttributes());
        final Map<String, AtlasStructType.AtlasAttribute> allAttributes = type.getAllAttributes();
        for (String attribute : allAttributes.keySet()) {
            AtlasType attributeType = allAttributes.get(attribute).getAttributeType();
            AtlasAttributeDef attributeDef = allAttributes.get(attribute).getAttributeDef();
            if ( header.getAttribute(attribute) == null && (TypeCategory.PRIMITIVE == attributeType.getTypeCategory())) {

                if ( attributeDef.getIsOptional()) {
                    header.setAttribute(attribute, attributeType.createOptionalDefaultValue());
                } else {
                    header.setAttribute(attribute, attributeType.createDefaultValue());
                }
            }
        }
        return header;
    }

    private AtlasEntityHeader constructHeader(AtlasObjectId id) {
        AtlasEntityHeader entity = new AtlasEntityHeader(id.getTypeName());
        entity.setGuid(id.getGuid());

        return entity;
    }

    public EntityMutationContext getContext() {
        return context;
    }

    public AtlasEntityType getInstanceType(Object val) throws AtlasBaseException {
        AtlasObjectId guid = getId(val);

        if ( guid != null) {
            return (AtlasEntityType) getContext().getType(guid);
        }

        return null;
    }
}
