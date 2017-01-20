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


import atlas.shaded.hbase.guava.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AtlasEntityStoreV1 implements AtlasEntityStore {

    protected EntityGraphDiscovery graphDiscoverer;
    protected AtlasTypeRegistry typeRegistry;

    private EntityGraphMapper graphMapper;

    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1.class);

    @Inject
    public AtlasEntityStoreV1(EntityGraphMapper vertexMapper) {
        this.graphMapper = vertexMapper;
    }

    @Inject
    public void init(AtlasTypeRegistry typeRegistry, EntityGraphDiscovery graphDiscoverer) throws AtlasBaseException {
        this.graphDiscoverer = graphDiscoverer;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public EntityMutationResponse createOrUpdate(final AtlasEntity entity) throws AtlasBaseException {
        return createOrUpdate(new ArrayList<AtlasEntity>() {{ add(entity); }});
    }

    public EntityMutationContext preCreateOrUpdate(final List<AtlasEntity> atlasEntities) throws AtlasBaseException {

        EntityGraphDiscoveryContext discoveredEntities = graphDiscoverer.discoverEntities(atlasEntities);
        EntityMutationContext context = new EntityMutationContext(discoveredEntities);
        for (AtlasEntity entity : discoveredEntities.getRootEntities()) {

            AtlasVertex vertex = null;
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasEntityStoreV1.preCreateOrUpdate({}): {}", entity);
            }

            AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());

            if ( discoveredEntities.isResolved(entity.getGuid()) ) {
                vertex = discoveredEntities.getResolvedReference(entity.getGuid());
                context.addUpdated(entity, entityType, vertex);

                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);
                RequestContextV1.get().recordEntityUpdate(guid);
            } else {
                //Create vertices which do not exist in the repository
                vertex = graphMapper.createVertexTemplate(entity, entityType);
                context.addCreated(entity, entityType, vertex);
                discoveredEntities.addRepositoryResolvedReference(new AtlasObjectId(entityType.getTypeName(), entity.getGuid()), vertex);

                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);
                RequestContextV1.get().recordEntityCreate(guid);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasEntityStoreV1.preCreateOrUpdate({}): {}", entity, vertex);
            }
        }

        return context;
    }

    @Override
    public EntityMutationResponse updateById(final String guid, final AtlasEntity entity) {
        return null;
    }

    @Override
    public AtlasEntity getById(final String guid) {
        return null;
    }

    @Override
    public EntityMutationResponse deleteById(final String guid) {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(final List<AtlasEntity> entities) throws AtlasBaseException {

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
    public EntityMutationResponse updateByIds(final String guid, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntity.AtlasEntities getByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntityWithAssociations getWithAssociationsByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntity getByUniqueAttribute(final String typeName, final String attrName, final String attrValue) {
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
    public EntityMutationResponse batchMutate(final EntityMutations mutations) throws AtlasBaseException {
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

    @Override
    public AtlasEntity.AtlasEntities searchEntities(final SearchFilter searchFilter) throws AtlasBaseException {
        // TODO: Add checks here to ensure that typename and supertype are mandatory in the request
        return null;
    }

    private List<AtlasEntity> validateAndNormalize(final List<AtlasEntity> entities) throws AtlasBaseException {

        List<AtlasEntity> normalizedEntities = new ArrayList<>();
        List<String> messages = new ArrayList<>();

        for (AtlasEntity entity : entities) {
            AtlasType type = typeRegistry.getType(entity.getTypeName());
            if (type.getTypeCategory() != TypeCategory.ENTITY) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, type.getTypeCategory().name(), TypeCategory.ENTITY.name());
            }

            type.validateValue(entity, entity.getTypeName(), messages);

            if ( !messages.isEmpty()) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, messages);
            }
            AtlasEntity normalizedEntity = (AtlasEntity) type.getNormalizedValue(entity);
            if ( normalizedEntity == null) {
                //TODO - Fix this. Should not come here. Should ideally fail above
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, "Failed to validate entity");
            }
            normalizedEntities.add(normalizedEntity);
        }

        return normalizedEntities;
    }

    @VisibleForTesting
    EntityGraphDiscovery getGraphDiscoverer() {
        return graphDiscoverer;
    }

    @VisibleForTesting
    void setGraphDiscoverer(EntityGraphDiscovery discoverer) {
        this.graphDiscoverer = discoverer;
    }

    public void cleanUp() throws AtlasBaseException {
        this.graphDiscoverer.cleanUp();
    }
}
