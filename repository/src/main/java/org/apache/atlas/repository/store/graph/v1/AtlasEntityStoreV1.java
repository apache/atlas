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
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class AtlasEntityStoreV1 implements AtlasEntityStore {

    protected AtlasTypeRegistry typeRegistry;

    private final EntityGraphMapper graphMapper;
    private final AtlasGraph        graph;

    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1.class);

    @Inject
    public AtlasEntityStoreV1(EntityGraphMapper vertexMapper) {
        this.graphMapper  = vertexMapper;
        this.graph        = AtlasGraphProvider.getGraphInstance();
    }

    @Inject
    public void init(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public AtlasEntityWithExtInfo getById(final String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieving entity with guid={}", guid);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(typeRegistry);

        return entityRetriever.toAtlasEntityWithExtInfo(guid);
    }

    @Override
    public AtlasEntityWithExtInfo getByUniqueAttribute(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException {
        String entityTypeName = entityType.getTypeName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieving entity with type={} and attributes={}: values={}", entityTypeName, uniqAttributes);
        }

        AtlasGraphQuery query = graph.query();

        for (Map.Entry<String, Object> e : uniqAttributes.entrySet()) {
            String attrName = e.getKey();
            Object attrValue = e.getValue();

            query = query.has(entityType.getQualifiedAttributeName(attrName), attrValue);
        }

        Iterator<AtlasVertex> result = query.has(Constants.ENTITY_TYPE_PROPERTY_KEY, entityTypeName)
                                            .has(Constants.STATE_PROPERTY_KEY, Status.ACTIVE.name())
                                            .vertices().iterator();
        AtlasVertex entityVertex = result.hasNext() ? result.next() : null;

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityTypeName, uniqAttributes.keySet().toString(), uniqAttributes.values().toString());
        }

        String guid = GraphHelper.getGuid(entityVertex);

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(typeRegistry);

        return entityRetriever.toAtlasEntityWithExtInfo(guid);
    }

    @Override
    public EntityMutationResponse deleteById(final String guid) {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(final Map<String, AtlasEntity> entities) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityStoreV1.createOrUpdate({}, {})", entities);
        }

        //Validate
        List<AtlasEntity> normalizedEntities = validateAndNormalize(entities);

        //Discover entities, create vertices
        EntityMutationContext ctx = preCreateOrUpdate(normalizedEntities);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.createOrUpdate({}, {}): {}", entities);
        }

        return graphMapper.mapAttributes(ctx);
    }

    @Override
    public AtlasEntitiesWithExtInfo getByIds(final List<String> guids) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse updateByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue) throws AtlasBaseException {
        return null;
    }

    @Override
    public void addClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    public void updateClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    public void deleteClassifications(final String guid, final List<String> classificationNames) throws AtlasBaseException {

    }

    private EntityMutationContext preCreateOrUpdate(final List<AtlasEntity> atlasEntities) throws AtlasBaseException {
        List<EntityResolver> entityResolvers = new ArrayList<>();

        entityResolvers.add(new IDBasedEntityResolver());
        entityResolvers.add(new UniqAttrBasedEntityResolver(typeRegistry));

        EntityGraphDiscovery        graphDiscoverer    = new AtlasEntityGraphDiscoveryV1(typeRegistry, entityResolvers);
        EntityGraphDiscoveryContext discoveredEntities = graphDiscoverer.discoverEntities(atlasEntities);
        EntityMutationContext       context            = new EntityMutationContext(discoveredEntities);

        for (AtlasEntity entity : discoveredEntities.getRootEntities()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasEntityStoreV1.preCreateOrUpdate({}): {}", entity);
            }

            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
            }

            final AtlasVertex vertex;
            AtlasObjectId     objId = entity.getAtlasObjectId();

            if (discoveredEntities.isResolvedId(objId) ) {
                vertex = discoveredEntities.getResolvedEntityVertex(objId);

                context.addUpdated(entity, entityType, vertex);

                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                RequestContextV1.get().recordEntityUpdate(new AtlasObjectId(entityType.getTypeName(), guid));
            } else {
                //Create vertices which do not exist in the repository
                vertex = graphMapper.createVertexTemplate(entity, entityType);

                context.addCreated(entity, entityType, vertex);

                discoveredEntities.addResolvedId(objId, vertex);
                discoveredEntities.removeUnResolvedId(objId);

                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                RequestContextV1.get().recordEntityCreate(new AtlasObjectId(entityType.getTypeName(), guid));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasEntityStoreV1.preCreateOrUpdate({}): {}", entity, vertex);
            }
        }

        return context;
    }

    private List<AtlasEntity> validateAndNormalize(final Map<String, AtlasEntity> entities) throws AtlasBaseException {
        List<AtlasEntity> normalizedEntities = new ArrayList<>();
        List<String>      messages           = new ArrayList<>();

        for (String entityId : entities.keySet()) {
            if ( !AtlasEntity.isAssigned(entityId) && !AtlasEntity.isUnAssigned(entityId)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, ": Guid in map key is invalid " + entityId);
            }

            AtlasEntity entity = entities.get(entityId);

            if ( entity == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, ": Entity is null for guid " + entityId);
            }

            AtlasEntityType type = typeRegistry.getEntityTypeByName(entity.getTypeName());
            if (type == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
            }

            type.validateValue(entity, entity.getTypeName(), messages);

            if ( !messages.isEmpty()) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, messages);
            }

            AtlasEntity normalizedEntity = (AtlasEntity) type.getNormalizedValue(entity);

            normalizedEntities.add(normalizedEntity);
        }

        return normalizedEntities;
    }

    public void cleanUp() throws AtlasBaseException {
    }
}
