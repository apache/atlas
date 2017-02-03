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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UniqAttrBasedEntityResolver implements EntityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(UniqAttrBasedEntityResolver.class);

    private final GraphHelper           graphHelper = GraphHelper.getInstance();
    private final AtlasTypeRegistry     typeRegistry;
    private EntityGraphDiscoveryContext context;

    @Inject
    public UniqAttrBasedEntityResolver(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void init(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        this.context = context;
    }

    @Override
    public EntityGraphDiscoveryContext resolveEntityReferences() throws AtlasBaseException {
        if (context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Unique attribute based entity resolver not initialized");
        }

        //Resolve attribute references
        List<AtlasObjectId> resolvedReferences = new ArrayList<>();

        for (AtlasObjectId entityId : context.getUnresolvedIdsByUniqAttribs()) {
            //query in graph repo that given unique attribute - check for deleted also?
            Optional<AtlasVertex> vertex = resolveByUniqueAttribute(entityId);

            if (vertex.isPresent()) {
                context.addResolvedId(entityId, vertex.get());
                resolvedReferences.add(entityId);
            }
        }

        context.removeUnresolvedIdsByUniqAttribs(resolvedReferences);

        //Resolve root references
        for (AtlasEntity entity : context.getRootEntities()) {
            AtlasObjectId entityId = entity.getAtlasObjectId();

            if (!context.isResolvedId(entityId) ) {
                Optional<AtlasVertex> vertex = resolveByUniqueAttribute(entity);

                if (vertex.isPresent()) {
                    context.addResolvedId(entityId, vertex.get());
                    context.removeUnResolvedId(entityId);
                }
            }
        }

        return context;
    }

    Optional<AtlasVertex> resolveByUniqueAttribute(AtlasEntity entity) throws AtlasBaseException {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
        }

        for (AtlasStructType.AtlasAttribute attr : entityType.getAllAttributes().values()) {
            if (attr.getAttributeDef().getIsUnique()) {
                Object attrVal = entity.getAttribute(attr.getName());

                if (attrVal == null) {
                    continue;
                }

                Optional<AtlasVertex> vertex = findByTypeAndQualifiedName(entityType.getTypeName(), attr.getQualifiedAttributeName(), attrVal);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found vertex by unique attribute : " + attr.getQualifiedAttributeName() + "=" + attrVal);
                }

                if (!vertex.isPresent()) {
                    vertex = findBySuperTypeAndQualifiedName(entityType.getTypeName(), attr.getQualifiedAttributeName(), attrVal);
                }

                if (vertex.isPresent()) {
                    return vertex;
                }
            }
        }

        return Optional.absent();
    }

    Optional<AtlasVertex> resolveByUniqueAttribute(AtlasObjectId entityId) throws AtlasBaseException {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityId.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entityId.getTypeName());
        }

        final Map<String, Object> uniqueAttributes = entityId.getUniqueAttributes();
        if (MapUtils.isNotEmpty(uniqueAttributes)) {
            for (String attrName : uniqueAttributes.keySet()) {
                AtlasStructType.AtlasAttribute attr = entityType.getAttribute(attrName);

                if (attr.getAttributeDef().getIsUnique()) {
                    Object attrVal = uniqueAttributes.get(attr.getName());

                    if (attrVal == null) {
                        continue;
                    }

                    Optional<AtlasVertex> vertex = findByTypeAndQualifiedName(entityId.getTypeName(), attr.getQualifiedAttributeName(), attrVal);

                    if (!vertex.isPresent()) {
                        vertex = findBySuperTypeAndQualifiedName(entityId.getTypeName(), attr.getQualifiedAttributeName(), attrVal);
                    }

                    if (vertex.isPresent()) {
                        return vertex;
                    }
                }
            }
        }
        return Optional.absent();
    }

    Optional<AtlasVertex> findByTypeAndQualifiedName(String typeName, String qualifiedAttrName, Object attrVal) {
        AtlasVertex vertex = null;
        try {
            vertex = graphHelper.findVertex(qualifiedAttrName, attrVal,
                Constants.ENTITY_TYPE_PROPERTY_KEY, typeName,
                Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE
                    .name());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Found vertex by unique attribute and type {} {} ", qualifiedAttrName + "=" + attrVal, typeName);
            }
            if (vertex != null) {
                return Optional.of(vertex);
            }
        } catch (EntityNotFoundException e) {
            //Ignore if not found
        }
        return Optional.absent();
    }

    Optional<AtlasVertex> findBySuperTypeAndQualifiedName(String typeName, String qualifiedAttrName, Object attrVal) {
        AtlasVertex vertex = null;
        try {
            vertex = graphHelper.findVertex(qualifiedAttrName, attrVal,
                Constants.SUPER_TYPES_PROPERTY_KEY, typeName,
                Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE
                    .name());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Found vertex by unique attribute and supertype {} ", qualifiedAttrName + "=" + attrVal, typeName);
            }
            if (vertex != null) {
                return Optional.of(vertex);
            }
        } catch (EntityNotFoundException e) {
            //Ignore if not found
        }
        return Optional.absent();
    }

    @Override
    public void cleanUp() {
        //Nothing to cleanup
        this.context = null;
    }

}

