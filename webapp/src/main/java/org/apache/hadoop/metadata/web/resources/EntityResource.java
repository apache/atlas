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
import org.apache.hadoop.metadata.services.MetadataService;
import org.apache.hadoop.metadata.web.util.Servlets;
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

    @POST
    @Path("submit/{entityType}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(@Context HttpServletRequest request,
                           @PathParam("entityType") final String entityType) {
        try {
            final String entity = Servlets.getRequestPayload(request);
            System.out.println("entity = " + entity);

            final String guid = metadataService.createEntity(entity, entityType);
            JSONObject response = new JSONObject();
            response.put("GUID", guid);
            response.put("requestId", Thread.currentThread().getName());

            return Response.ok(response).build();
        } catch (Exception e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }

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

        } catch (Exception e) {
            LOG.error("Action failed: {}\nError: {}",
                    Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            throw new WebApplicationException(e, Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .type(MediaType.APPLICATION_JSON)
                    .build());
        }
    }

    @GET
    @Path("definition/{entityType}/{entityName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityDefinition(@PathParam("entityType") String entityType,
                                        @PathParam("entityName") String entityName) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    @GET
    @Path("list/{entityType}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityList(@PathParam("entityType") String entityType,
                                  @DefaultValue("0") @QueryParam("offset") Integer offset,
                                  @QueryParam("numResults") Integer resultsPerPage) {
        return Response.ok().build();
    }
}
