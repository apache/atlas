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
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v1.EntityStream;
import org.apache.atlas.services.MetadataService;
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
import java.util.List;

import static org.apache.atlas.web.adapters.AtlasInstanceRestAdapters.toAtlasBaseException;


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
