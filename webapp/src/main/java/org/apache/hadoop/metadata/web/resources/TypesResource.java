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

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.MetadataServiceClient;
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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * This class provides RESTful API for Types.
 *
 * A type is the description of any representable item;
 * e.g. a Hive table
 *
 * You could represent any meta model representing any domain using these types.
 */
@Path("types")
@Singleton
public class TypesResource {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResource.class);

    private final MetadataService metadataService;

    @Inject
    public TypesResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    /**
     * Submits a type definition corresponding to a given type representing a meta model of a
     * domain. Could represent things like Hive Database, Hive Table, etc.
     *
     * @param typeName name of a type, should be unique.
     */
    @POST
    @Path("submit/{typeName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(@Context HttpServletRequest request,
                           @PathParam("typeName") String typeName) {
        try {
            final String typeDefinition = Servlets.getRequestPayload(request);
            LOG.debug("creating type {} with definition {} ", typeName, typeDefinition);

            JSONObject typesAdded = metadataService.createType(typeName, typeDefinition);

            JSONObject response = new JSONObject();
            response.put("typeName", typeName);
            response.put("types", typesAdded);
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Unable to persist type {}", typeName, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }

    /**
     * Fetch the complete definition of a given type name which is unique.
     *
     * @param typeName name of a type which is unique.
     */
    @GET
    @Path("definition/{typeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDefinition(@Context HttpServletRequest request,
                                  @PathParam("typeName") String typeName) {
        try {
            final String typeDefinition = metadataService.getTypeDefinition(typeName);

            JSONObject response = new JSONObject();
            response.put("typeName", typeName);
            response.put("definition", typeDefinition);
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (MetadataException e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (JSONException e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }

    /**
     * Gets the list of type names registered in the type system.
     */
    @GET
    @Path("list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTypeNames(@Context HttpServletRequest request) {
        try {
            final List<String> typeNamesList = metadataService.getTypeNamesList();

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.RESULTS, new JSONArray(typeNamesList));
            response.put(MetadataServiceClient.TOTAL_SIZE, typeNamesList.size());
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Unable to get types list", e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }

    /**
     * Gets the list of trait type names registered in the type system.
     */
    @GET
    @Path("traits/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTraitNames(@Context HttpServletRequest request) {
        try {
            final List<String> traitNamesList = metadataService.getTraitNamesList();

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.RESULTS, new JSONArray(traitNamesList));
            response.put(MetadataServiceClient.TOTAL_SIZE, traitNamesList.size());
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (Exception e) {
            LOG.error("Unable to get types list", e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }
}
