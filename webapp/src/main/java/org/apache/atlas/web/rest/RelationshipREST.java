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

package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * REST interface for entity relationships.
 */
@Path("relationship")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class RelationshipREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.RelationshipREST");

    private final AtlasRelationshipStore relationshipStore;
    private final EntityMutationService entityMutationService;

    @Inject
    public RelationshipREST(AtlasRelationshipStore relationshipStore, EntityMutationService entityMutationService) {
        this.relationshipStore = relationshipStore;
        this.entityMutationService = entityMutationService;
    }

    /**
     * Create a new relationship between entities.
     */
    @POST
    @Timed
    public AtlasRelationship create(AtlasRelationship relationship) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.create(" + relationship + ")");
            }

            return relationshipStore.create(relationship);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create a new relationship or update existing relationship between entities.
     */
    @POST
    @Path("/bulk")
    public List<AtlasRelationship> createOrUpdate(List<AtlasRelationship> relationships) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.createOrUpdate(" + relationships + ")");
            }

            return relationshipStore.createOrUpdate(relationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Update an existing relationship between entities.
     */
    @PUT
    @Timed
    public AtlasRelationship update(AtlasRelationship relationship) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.update(" + relationship + ")");
            }

            return relationshipStore.update(relationship);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get relationship information between entities using relationship guid.
     */
    @GET
    @Path("/guid/{guid}")
    @Timed
    public AtlasRelationshipWithExtInfo getById(@PathParam("guid") String guid,
                                                @QueryParam("extendedInfo") @DefaultValue("false") boolean extendedInfo)
                                                throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        AtlasRelationshipWithExtInfo ret;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.getById(" + guid + ")");
            }

            if (extendedInfo) {
                ret = relationshipStore.getExtInfoById(guid);
            } else {
                ret = new AtlasRelationshipWithExtInfo(relationshipStore.getById(guid));
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete a relationship between entities using guid.
     */
    @DELETE
    @Path("/guid/{guid}")
    @Timed
    public void deleteById(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.deleteById(" + guid + ")");

            entityMutationService.deleteRelationshipById(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Delete a relationship between entities using guid.
     */
    @DELETE
    @Path("/guid/bulk")
    @Timed
    public void deleteByIds(List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Need list of GUIDs");
        }

        for (String guid : guids) {
            Servlets.validateQueryParamLength("guid", guid);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.deleteById(" + guids.size() + ")");
            }
            entityMutationService.deleteRelationshipsByIds(guids);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}