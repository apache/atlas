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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UniqAttrBasedEntityResolver implements EntityResolver {
    private static final Logger     LOG = LoggerFactory.getLogger(UniqAttrBasedEntityResolver.class);
    private final        AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphMapper entityGraphMapper;

    public UniqAttrBasedEntityResolver(AtlasGraph graph, AtlasTypeRegistry typeRegistry, EntityGraphMapper entityGraphMapper) {
        this.graph             = graph;
        this.typeRegistry      = typeRegistry;
        this.entityGraphMapper = entityGraphMapper;
    }

    @Override
    public EntityGraphDiscoveryContext resolveEntityReferences(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        if (context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "UniqAttrBasedEntityResolver.resolveEntityReferences(): context is null");
        }

        //Resolve attribute references
        List<AtlasObjectId> resolvedReferences = new ArrayList<>();
        AtlasVertex         vertex;

        for (AtlasObjectId objId : context.getReferencedByUniqAttribs()) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(objId.getTypeName());

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), objId.getTypeName());
            }

            final Long                msgTimestamp   = RequestContext.get().getCreateEventMsgTime();
            final String              typeName       = entityType.getTypeName();
            final Map<String, Object> uniqAttributes = objId.getUniqueAttributes();

            if (isParallelProcessingAllowed(uniqAttributes, msgTimestamp)) {
                LOG.debug("Parallel Processing-{}: Using temporal lookup (findActiveByUniqueAttributesAsOf) with entityType={}, attrValues={}, asOfTimestamp={}",
                        Thread.currentThread().getName(), typeName, uniqAttributes, msgTimestamp);
                vertex = AtlasGraphUtilsV2.findActiveByUniqueAttributesAsOf(this.graph, entityType, uniqAttributes, msgTimestamp);
            } else {
                vertex = AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, uniqAttributes);
            }

            if (vertex == null && RequestContext.get().isCreateShellEntityForNonExistingReference()) {
                vertex = entityGraphMapper.createShellEntityVertex(objId, context);
            }

            if (vertex != null) {
                context.addResolvedIdByUniqAttribs(objId, vertex);
                resolvedReferences.add(objId);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, objId.toString());
            }
        }

        return context;
    }

    private boolean isParallelProcessingAllowed(Map<String, Object> uniqAttributes, Long msgTimestamp) {
        return AtlasConfiguration.ATLAS_PARALLEL_PROCESSING_ENABLED.getBoolean() && MapUtils.isNotEmpty(uniqAttributes) && msgTimestamp != null;
    }
}
