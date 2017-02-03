/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.web.adapters.AtlasInstanceRestAdapters;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toAtlasBaseException;
import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toEntityMutationResponse;


@Path("v2/entities")
@Singleton
public class EntitiesREST {
    private static final Logger LOG = LoggerFactory.getLogger(EntitiesREST.class);

    private AtlasEntityStore entitiesStore;

    @Context
    private HttpServletRequest httpServletRequest;

    private final MetadataService metadataService;

    private final AtlasInstanceRestAdapters restAdapters;

    @Inject
    public EntitiesREST(AtlasEntityStore entitiesStore, MetadataService metadataService, AtlasInstanceRestAdapters restAdapters) {
        LOG.info("EntitiesRest Init");
        this.entitiesStore = entitiesStore;
        this.metadataService = metadataService;
        this.restAdapters = restAdapters;
    }

    /*******
     * Entity Creation/Updation if it already exists in ATLAS
     * An existing entity is matched by its guid if supplied or by its unique attribute eg: qualifiedName
     * Any associations like Classifications, Business Terms will have to be handled through the respective APIs
     *******/

    @POST
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse createOrUpdate(Map<String, AtlasEntity> entities) throws AtlasBaseException {
        EntityMutationResponse response = null;

        ITypedReferenceableInstance[] entitiesInOldFormat = restAdapters.getITypedReferenceables(entities.values());

        try {
            final AtlasClient.EntityResult result = metadataService.updateEntities(entitiesInOldFormat);
            response = toEntityMutationResponse(result);
        } catch (AtlasException e) {
            LOG.error("Exception while getting a typed reference for the entity ", e);
            throw AtlasInstanceRestAdapters.toAtlasBaseException(e);
        }
        return response;
    }

    /*******
     * Entity Updation - Allows full update of the specified entities.
     * Any associations like Classifications, Business Terms will have to be handled through the respective APIs
     * Null updates are supported i.e Set an attribute value to Null if its an optional attribute
     *******/
    @PUT
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse update(Map<String, AtlasEntity> entities) throws AtlasBaseException {
       return createOrUpdate(entities);
    }

    @GET
    @Path("/guids")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntity.AtlasEntities getById(@QueryParam("guid") List<String> guids) throws AtlasBaseException {

        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guids);
        }

        AtlasEntity.AtlasEntities entities = new AtlasEntity.AtlasEntities();

        List<AtlasEntity> entityList = new ArrayList<>();

        for (String guid : guids) {
            try {
               ITypedReferenceableInstance ref = metadataService.getEntityDefinition(guid);
               Map<String, AtlasEntityWithAssociations> entityRet = restAdapters.getAtlasEntity(ref);

               addToEntityList(entityList, entityRet.values());

            } catch (AtlasException e) {
                throw toAtlasBaseException(e);
            }
        }

        entities.setList(entityList);
        return entities;
    }

    private void addToEntityList(final List<AtlasEntity> entityList, final Collection<AtlasEntityWithAssociations> values) {
        for (AtlasEntityWithAssociations val : values) {
            if ( !entityList.contains(val)) {
                entityList.add(val);
            }
        }
    }

    /*******
     * Entity Delete
     *******/

    @DELETE
    @Path("/guids")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse deleteById(@QueryParam("guid") final List<String> guids) throws AtlasBaseException {

        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guids);
        }
        try {
            AtlasClient.EntityResult result = metadataService.deleteEntities(guids);
            return toEntityMutationResponse(result);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }

    /**
     * Bulk API to associate a tag to multiple entities
     *
     */
    @POST
    @Path("/classification")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void addClassification(ClassificationAssociateRequest request) throws AtlasBaseException {
        AtlasClassification classification = request == null ? null : request.getClassification();
        List<String>        entityGuids    = request == null ? null : request.getEntityGuids();

        if (classification == null || StringUtils.isEmpty(classification.getTypeName())) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no classification");
        }

        if (CollectionUtils.isEmpty(entityGuids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "empty entity list");
        }

        final ITypedStruct trait = restAdapters.getTrait(classification);

        try {
            metadataService.addTrait(entityGuids, trait);
        } catch (IllegalArgumentException e) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, e);
        } catch (AtlasException e) {
            throw toAtlasBaseException(e);
        }
    }
}
