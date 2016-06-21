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

package org.apache.atlas.web.resources;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.web.util.Servlets;
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
import javax.ws.rs.PUT;
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

    private static final Logger LOG = LoggerFactory.getLogger(TypesResource.class);

    private final MetadataService metadataService;

    static final String TYPE_ALL = "all";

    @Inject
    public TypesResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    /**
     * Submits a type definition corresponding to a given type representing a meta model of a
     * domain. Could represent things like Hive Database, Hive Table, etc.
     */
    @POST
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response submit(@Context HttpServletRequest request) {
        try {
            final String typeDefinition = Servlets.getRequestPayload(request);
            LOG.info("Creating type with definition {} ", typeDefinition);

            JSONObject typesJson = metadataService.createType(typeDefinition);
            final JSONArray typesJsonArray = typesJson.getJSONArray(AtlasClient.TYPES);

            JSONArray typesResponse = new JSONArray();
            for (int i = 0; i < typesJsonArray.length(); i++) {
                final String name = typesJsonArray.getString(i);
                typesResponse.put(new JSONObject() {{
                    put(AtlasClient.NAME, name);
                }});
            }

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPES, typesResponse);
            return Response.status(ClientResponse.Status.CREATED).entity(response).build();
        } catch (TypeExistsException e) {
            LOG.error("Type already exists", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Update of existing types - if the given type doesn't exist, creates new type
     * Allowed updates are:
     * 1. Add optional attribute
     * 2. Change required to optional attribute
     * 3. Add super types - super types shouldn't contain any required attributes
     * @param request
     * @return
     */
    @PUT
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response update(@Context HttpServletRequest request) {
        try {
            final String typeDefinition = Servlets.getRequestPayload(request);
            LOG.info("Updating type with definition {} ", typeDefinition);

            JSONObject typesJson = metadataService.updateType(typeDefinition);
            final JSONArray typesJsonArray = typesJson.getJSONArray(AtlasClient.TYPES);

            JSONArray typesResponse = new JSONArray();
            for (int i = 0; i < typesJsonArray.length(); i++) {
                final String name = typesJsonArray.getString(i);
                typesResponse.put(new JSONObject() {{
                    put(AtlasClient.NAME, name);
                }});
            }

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPES, typesResponse);
            return Response.ok().entity(response).build();
        } catch (TypeExistsException e) {
            LOG.error("Type already exists", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Fetch the complete definition of a given type name which is unique.
     *
     * @param typeName name of a type which is unique.
     */
    @GET
    @Path("{typeName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getDefinition(@Context HttpServletRequest request, @PathParam("typeName") String typeName) {
        try {
            final String typeDefinition = metadataService.getTypeDefinition(typeName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.TYPENAME, typeName);
            response.put(AtlasClient.DEFINITION, new JSONObject(typeDefinition));
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (AtlasException e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (JSONException | IllegalArgumentException e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Gets the list of trait type names registered in the type system.
     *
     * @param type type should be the name of enum
     *             org.apache.atlas.typesystem.types.DataTypes.TypeCategory
     *             Typically, would be one of all, TRAIT, CLASS, ENUM, STRUCT
     * @return entity names response payload as json
     */
    @GET
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTypesByFilter(@Context HttpServletRequest request,
            @DefaultValue(TYPE_ALL) @QueryParam("type") String type) {
        try {
            List<String> result;
            if (TYPE_ALL.equals(type)) {
                result = metadataService.getTypeNamesList();
            } else {
                DataTypes.TypeCategory typeCategory = DataTypes.TypeCategory.valueOf(type);
                result = metadataService.getTypeNamesByCategory(typeCategory);
            }

            JSONObject response = new JSONObject();
            response.put(AtlasClient.RESULTS, new JSONArray(result));
            response.put(AtlasClient.COUNT, result.size());
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (IllegalArgumentException | AtlasException ie) {
            LOG.error("Unsupported typeName while retrieving type list {}", type);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(new Exception("Unsupported type " + type, ie), Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get types list", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
