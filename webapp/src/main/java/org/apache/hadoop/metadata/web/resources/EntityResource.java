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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.metadata.service.Services;
import org.apache.hadoop.metadata.services.GraphBackedMetadataRepositoryService;
import org.apache.hadoop.metadata.services.MetadataRepositoryService;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

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
import java.io.IOException;
import java.io.StringWriter;

/**
 * Entity management operations as REST API.
 * 
 * An entity is an "instance" of a Type.  Entities conform to the definition
 * of the Type they correspond with.
 */
@Path("entities")
public class EntityResource {

    private MetadataRepositoryService repositoryService;

    public EntityResource() {
        repositoryService = Services.get().getService(GraphBackedMetadataRepositoryService.NAME);
        if (repositoryService == null) {
            throw new RuntimeException("graph service is not initialized");
        }
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

            final String guid = repositoryService.submitEntity(entity, entityType);
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

        return Response.ok().build();
    }

    @GET
    @Path("definition/{entityType}/{entityName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityDefinition(@PathParam("entityType") String entityType,
                                        @PathParam("entityName") String entityName) {
        final String entityDefinition = repositoryService.getEntityDefinition(entityName, entityType);
        return (entityDefinition == null)
                ? Response.status(Response.Status.NOT_FOUND).build()
                : Response.ok(entityDefinition).build();
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

    @GET
    @Path("list/{entityType}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityList(@PathParam("entityType") String entityType,
                                  @DefaultValue("0") @QueryParam("offset") Integer offset,
                                  @QueryParam("numResults") Integer resultsPerPage) {
        return Response.ok().build();
    }
}
