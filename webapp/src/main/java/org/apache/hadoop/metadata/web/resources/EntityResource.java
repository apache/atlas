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

package org.apache.hadoop.metadata.web.resources;

import com.google.common.base.Preconditions;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.services.MetadataService;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Entity management operations as REST API.
 *
 * An entity is an "instance" of a Type.  Entities conform to the definition
 * of the Type they correspond with.
 */
@Path("entities")
@Singleton
public class EntityResource {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResource.class);

    private final MetadataService metadataService;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured MetadataService.
     *
     * @param metadataService metadata service handle
     */
    @Inject
    public EntityResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    /**
     * Submits an entity definition (instance) corresponding to a given type.
     *
     * @param typeName name of a type which is unique.
     */
    @POST
    @Path("submit/{typeName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(@Context HttpServletRequest request,
                           @PathParam("typeName") final String typeName) {
        try {
            final String entity = Servlets.getRequestPayload(request);
            LOG.debug("submitting entity {} ", entity);

            final String guid = metadataService.createEntity(typeName, entity);
            JSONObject response = new JSONObject();
            response.put("GUID", guid);
            response.put("requestId", Thread.currentThread().getName());

            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Unable to persist instance for type {}", typeName, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     */
    @GET
    @Path("definition/{guid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityDefinition(@PathParam("guid") String guid) {
        Preconditions.checkNotNull(guid, "guid cannot be null");

        try {
            final String entityDefinition = metadataService.getEntityDefinition(guid);

            JSONObject response = new JSONObject();
            response.put("requestId", Thread.currentThread().getName());

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put("definition", entityDefinition);
                status = Response.Status.OK;
            }

            return Response.status(status).entity(response).build();

        } catch (MetadataException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(e, Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(e.getMessage())
                    .type(MediaType.APPLICATION_JSON)
                    .build());
        } catch (JSONException e) {
            LOG.error("Unable to get instance definition for GUID {}", guid, e);
            throw new WebApplicationException(e, Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .type(MediaType.APPLICATION_JSON)
                    .build());
        }
    }

    /**
     * Gets the list of entities for a given entity type.
     *
     * @param entityType     name of a type which is unique
     * @param offset         starting offset for pagination
     * @param resultsPerPage number of results for pagination
     */
    @GET
    @Path("list/{entityType}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityList(@PathParam("entityType") String entityType,
                                  @DefaultValue("0") @QueryParam("offset") Integer offset,
                                  @QueryParam("numResults") Integer resultsPerPage) {
        try {
            final List<String> entityList = metadataService.getEntityList(entityType);

            JSONObject response = new JSONObject();
            response.put("requestId", Thread.currentThread().getName());
            response.put("list", new JSONArray(entityList));

            return Response.ok(response).build();
        } catch (MetadataException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
