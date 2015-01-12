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

import java.io.IOException;
import java.io.StringWriter;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.services.MetadataService;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
     * @param metadataService
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
            final String entity = getEntity(request, entityType);
            System.out.println("entity = " + entity);
            validateEntity(entity, entityType);

            final String guid = metadataService.createEntity(entity, entityType);
            JSONObject response = new JSONObject();
            response.put("GUID", guid);

            return Response.ok(response).build();
        } catch (Exception e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }

    private String getEntity(HttpServletRequest request,
                             String entityType) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(request.getInputStream(), writer);
        return writer.toString();
    }

    private void validateEntity(String entity, String entityType) throws ParseException {
        Preconditions.checkNotNull(entity, "entity cannot be null");
        Preconditions.checkNotNull(entityType, "entity type cannot be null");
        JSONValue.parseWithException(entity);
    }

    @GET
    @Path("definition/{guid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityDefinition(@PathParam("guid") String guid) {
        Preconditions.checkNotNull(guid, "guid cannot be null");

        try {
            final String entityDefinition = metadataService.getEntityDefinition(guid);
            return (entityDefinition == null)
                    ? Response.status(Response.Status.NOT_FOUND).build()
                    : Response.ok(entityDefinition).build();
        } catch (MetadataException e) {
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

    @POST
    @Path("validate/{entityType}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response validate(@Context HttpServletRequest request,
                             @PathParam("entityType") String entityType) {
        return Response.ok().build();
    }

    @DELETE
    @Path("delete/{entityType}/{entityName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(
            @Context HttpServletRequest request,
            @PathParam("entityType") final String entityType,
            @PathParam("entityName") final String entityName) {
        return Response.ok().build();
    }

    @POST
    @Path("update/{entityType}/{entityName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response update(@Context HttpServletRequest request,
                           @PathParam("entityType") final String entityType,
                           @PathParam("entityName") final String entityName) {
        return Response.ok().build();
    }

    @GET
    @Path("status/{entityType}/{entityName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStatus(@PathParam("entityType") String entityType,
                              @PathParam("entityName") String entityName) {
        return Response.ok().build();
    }

    @GET
    @Path("dependencies/{entityType}/{entityName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDependencies(@PathParam("entityType") String entityType,
                                    @PathParam("entityName") String entityName) {
        return Response.ok().build();
    }
}
