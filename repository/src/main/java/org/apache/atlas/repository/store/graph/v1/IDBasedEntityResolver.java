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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IDBasedEntityResolver implements EntityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(IDBasedEntityResolver.class);


    private final GraphHelper graphHelper = GraphHelper.getInstance();


    public EntityGraphDiscoveryContext resolveEntityReferences(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        if (context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "IDBasedEntityResolver.resolveEntityReferences(): context is null");
        }

        EntityStream entityStream = context.getEntityStream();

        for (String guid : context.getReferencedGuids()) {
            if (AtlasEntity.isAssigned(guid)) { // validate in graph repo that given guid exists
                AtlasVertex vertex = resolveGuid(guid);

                context.addResolvedGuid(guid, vertex);
            } else  if (entityStream.getByGuid(guid) != null) { //check if entity stream have this reference id
                context.addLocalGuidReference(guid);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, guid);
            }
        }

        return context;
    }

    private AtlasVertex resolveGuid(String guid) throws AtlasBaseException {
        //validate in graph repo that given guid, typename exists
        AtlasVertex vertex = null;
        try {
            vertex = graphHelper.findVertex(Constants.GUID_PROPERTY_KEY, guid,
                                            Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        } catch (EntityNotFoundException e) {
            //Ignore
        }

        if (vertex != null) {
            return vertex;
        } else {
            throw new AtlasBaseException(AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, guid);
        }
    }
}
