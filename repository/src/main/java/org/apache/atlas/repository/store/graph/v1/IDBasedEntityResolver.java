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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IDBasedEntityResolver implements EntityResolver {

    private final GraphHelper              graphHelper   = GraphHelper.getInstance();
    private final Map<String, AtlasEntity> idToEntityMap = new HashMap<>();
    private EntityGraphDiscoveryContext    context;

    @Override
    public void init(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        this.context = context;

        for (AtlasEntity entity : context.getRootEntities()) {
            idToEntityMap.put(entity.getGuid(), entity);
        }
    }

    public EntityGraphDiscoveryContext resolveEntityReferences() throws AtlasBaseException {
        if (context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Entity resolver not initialized");
        }

        List<AtlasObjectId> resolvedReferences = new ArrayList<>();

        for (AtlasObjectId objId : context.getUnresolvedIds()) {
            if (objId.isAssignedGuid()) {
                //validate in graph repo that given guid, typename exists
                Optional<AtlasVertex> vertex = resolveGuid(objId);

                if (vertex.isPresent()) {
                    context.addResolvedId(objId, vertex.get());
                    resolvedReferences.add(objId);
                }
            } else {
                //check if root references have this temporary id
               if (!idToEntityMap.containsKey(objId.getGuid()) ) {
                   throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, objId.toString());
               }
                resolvedReferences.add(objId);
            }

        }

        context.removeUnResolvedIds(resolvedReferences);

        //Resolve root references
        for (AtlasEntity entity : context.getRootEntities()) {
            AtlasObjectId objId = entity.getAtlasObjectId();

            if (!context.isResolvedId(objId) && AtlasEntity.isAssigned(entity.getGuid())) {
                Optional<AtlasVertex> vertex = resolveGuid(objId);

                if (vertex.isPresent()) {
                    context.addResolvedId(objId, vertex.get());
                    context.removeUnResolvedId(objId);
                }
            }
        }

        return context;
    }

    private Optional<AtlasVertex> resolveGuid(AtlasObjectId objId) throws AtlasBaseException {
        //validate in graph repo that given guid, typename exists
        AtlasVertex vertex = null;
        try {
            vertex = graphHelper.findVertex(Constants.GUID_PROPERTY_KEY, objId.getGuid(),
                Constants.TYPE_NAME_PROPERTY_KEY, objId.getTypeName(),
                Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        } catch (EntityNotFoundException e) {
            //Ignore
        }
        if ( vertex != null ) {
            return Optional.of(vertex);
        } else {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, objId.getGuid());
        }
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
        idToEntityMap.clear();
        this.context = null;
    }

}
