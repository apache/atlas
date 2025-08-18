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
package org.apache.atlas.repository.converters;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.VertexEdgePropertiesCache;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.*;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.repository.converters.AtlasFormatConverter.ConverterContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
@Component
public class AtlasInstanceConverter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasInstanceConverter.class);

    private final AtlasTypeRegistry     typeRegistry;
    private final AtlasFormatConverters instanceFormatters;
    private final EntityGraphRetriever  entityGraphRetriever;
    private final EntityGraphRetriever  entityGraphRetrieverIgnoreRelationshipAttrs;
    private final EntityGraphRetriever  entityGraphRetrieverIncludeMandatoryRelAttrs;

    @Inject
    public AtlasInstanceConverter(AtlasGraph graph, AtlasTypeRegistry typeRegistry, AtlasFormatConverters instanceFormatters,
                                  EntityGraphRetriever entityGraphRetriever) {
        this.typeRegistry                                = typeRegistry;
        this.instanceFormatters                          = instanceFormatters;
        this.entityGraphRetriever                        = entityGraphRetriever;
        this.entityGraphRetrieverIgnoreRelationshipAttrs = new EntityGraphRetriever(entityGraphRetriever, true);
        this.entityGraphRetrieverIncludeMandatoryRelAttrs= new EntityGraphRetriever(graph, typeRegistry, false, true);
    }


    public Referenceable getReferenceable(String guid) throws AtlasBaseException {
        AtlasEntityWithExtInfo entity = getAndCacheEntityExtInfo(guid);

        return getReferenceable(entity);
    }

    public Referenceable getReferenceable(AtlasEntityWithExtInfo entity) throws AtlasBaseException {
        AtlasFormatConverter.ConverterContext ctx = new AtlasFormatConverter.ConverterContext();

        ctx.addEntity(entity.getEntity());
        for(Map.Entry<String, AtlasEntity> entry : entity.getReferredEntities().entrySet()) {
            ctx.addEntity(entry.getValue());
        }

        return getReferenceable(entity.getEntity(), ctx);
    }

    public Referenceable getReferenceable(AtlasEntity entity, final ConverterContext ctx) throws AtlasBaseException {
        AtlasFormatConverter converter  = instanceFormatters.getConverter(TypeCategory.ENTITY);
        AtlasType            entityType = typeRegistry.getType(entity.getTypeName());
        Referenceable        ref        = (Referenceable) converter.fromV2ToV1(entity, entityType, ctx);

        return ref;
    }

    public Struct getTrait(AtlasClassification classification) throws AtlasBaseException {
        AtlasFormatConverter converter          = instanceFormatters.getConverter(TypeCategory.CLASSIFICATION);
        AtlasType            classificationType = typeRegistry.getType(classification.getTypeName());
        Struct               trait               = (Struct)converter.fromV2ToV1(classification, classificationType, new ConverterContext());

        return trait;
    }

    public AtlasClassification toAtlasClassification(Struct classification) throws AtlasBaseException {
        AtlasFormatConverter    converter          = instanceFormatters.getConverter(TypeCategory.CLASSIFICATION);
        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());

        if (classificationType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.CLASSIFICATION.name(), classification.getTypeName());
        }

        AtlasClassification  ret = (AtlasClassification)converter.fromV1ToV2(classification, classificationType, new AtlasFormatConverter.ConverterContext());

        return ret;
    }

    public AtlasEntitiesWithExtInfo toAtlasEntity(Referenceable referenceable) throws AtlasBaseException {
        AtlasEntityFormatConverter converter  = (AtlasEntityFormatConverter) instanceFormatters.getConverter(TypeCategory.ENTITY);
        AtlasEntityType            entityType = typeRegistry.getEntityTypeByName(referenceable.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), referenceable.getTypeName());
        }

        ConverterContext ctx    = new ConverterContext();
        AtlasEntity      entity = converter.fromV1ToV2(referenceable, entityType, ctx);

        ctx.addEntity(entity);

        return ctx.getEntities();
    }


    public AtlasEntity.AtlasEntitiesWithExtInfo toAtlasEntities(List<Referenceable> referenceables) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> toAtlasEntities({})", referenceables);
        }

        AtlasFormatConverter.ConverterContext context = new AtlasFormatConverter.ConverterContext();

        for (Referenceable referenceable : referenceables) {
            AtlasEntity entity = fromV1toV2Entity(referenceable, context);

            context.addEntity(entity);
        }

        AtlasEntity.AtlasEntitiesWithExtInfo ret = context.getEntities();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== toAtlasEntities({}): ret=", referenceables, ret);
        }

        return ret;
    }

    private AtlasEntity fromV1toV2Entity(Referenceable referenceable, AtlasFormatConverter.ConverterContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> fromV1toV2Entity({})", referenceable);
        }

        AtlasEntityFormatConverter converter = (AtlasEntityFormatConverter) instanceFormatters.getConverter(TypeCategory.ENTITY);

        AtlasEntity entity = converter.fromV1ToV2(referenceable, typeRegistry.getType(referenceable.getTypeName()), context);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== fromV1toV2Entity({}): {}", referenceable, entity);
        }

        return entity;
    }

    public AtlasEntity getAndCacheEntity(String guid) throws AtlasBaseException {
        return getAndCacheEntity(guid, false);
    }

    public AtlasEntity getAndCacheEntity(String guid, boolean ignoreRelationshipAttributes) throws AtlasBaseException {
        RequestContext context = RequestContext.get();
        AtlasEntity    entity  = context.getEntity(guid);

        if (entity == null) {
            if (ignoreRelationshipAttributes) {
                entity = entityGraphRetrieverIgnoreRelationshipAttrs.toAtlasEntity(guid);
            } else {
                entity = entityGraphRetriever.toAtlasEntity(guid);
            }

            if (entity != null) {
                context.cache(entity);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache miss -> GUID = {}", guid);
                }
            }
        }

        return entity;
    }

    public AtlasEntity getEntityWithMandatoryRelations(String guid) throws AtlasBaseException {
        RequestContext context = RequestContext.get();
        AtlasEntity    entity  = context.getEntity(guid);

        if (entity == null) {
            entity = entityGraphRetrieverIncludeMandatoryRelAttrs.toAtlasEntity(guid);
        }

        return entity;
    }

    public AtlasEntity getEntity(String guid, boolean ignoreRelationshipAttributes) throws AtlasBaseException {
        AtlasEntity entity = null;
        if (ignoreRelationshipAttributes) {
            entity = entityGraphRetrieverIgnoreRelationshipAttrs.toAtlasEntity(guid);
        } else {
            entity = entityGraphRetriever.toAtlasEntity(guid);
        }
        return entity;
    }


    public AtlasEntityWithExtInfo getAndCacheEntityExtInfo(String guid) throws AtlasBaseException {
        RequestContext         context           = RequestContext.get();
        AtlasEntityWithExtInfo entityWithExtInfo = context.getEntityWithExtInfo(guid);

        if (entityWithExtInfo == null) {
            entityWithExtInfo = entityGraphRetriever.toAtlasEntityWithExtInfo(guid);

            if (entityWithExtInfo != null) {
                context.cache(entityWithExtInfo);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cache miss -> GUID = {}", guid);
                }
            }
        }

        return entityWithExtInfo;
    }

    public List<AtlasEntity> getEnrichedEntitiesWithPrimitiveAttributes(Set<AtlasVertex> vertices, Set<AtlasStructType.AtlasAttribute> attributes) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(vertices)) {
            return Collections.emptyList();
        }

        List<AtlasEntity> enrichedEntities =  new ArrayList<>(vertices.size());
        Set<String> vertexIds = vertices.stream()
                .map(AtlasVertex::getIdForDisplay)
                .collect(Collectors.toSet());

        Set<String> attributeNames = attributes.stream()
                .map(AtlasStructType.AtlasAttribute::getName)
                .collect(Collectors.toSet());

        // Enrich vertex properties
        VertexEdgePropertiesCache propertiesCache = entityGraphRetriever.enrichVertexPropertiesByVertexIds(
                vertexIds, attributeNames);

        if (propertiesCache == null) {
            LOG.debug("No vertex properties found for vertexIds: {}", vertexIds);
            return null;
        }


        for (AtlasVertex vertex : vertices) {
            AtlasEntityHeader entityHeader =  entityGraphRetriever.toAtlasEntityHeader(vertex, attributeNames, propertiesCache);
            enrichedEntities.add(new AtlasEntity(entityHeader));
        }

        return enrichedEntities;

    }

}