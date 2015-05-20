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

package org.apache.hadoop.metadata.web.resources;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.services.MetadataService;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.web.listeners.GuiceServletConfig;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    static final String TYPE_ALL = "all";
    private static final String URI_TYPES = "types";

    @Context
    UriInfo uriInfo;

    @Inject
    public TypesResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    /**
     * Submits a type definition corresponding to a given type representing a meta model of a
     * domain. Could represent things like Hive Database, Hive Table, etc.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(@Context HttpServletRequest request) {
        try {
            final String typeDefinition = Servlets.getRequestPayload(request);
            LOG.debug("creating type with definition {} ", typeDefinition);

            JSONObject typesJson = metadataService.createType(typeDefinition);
            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            final JSONArray typesJsonArray = typesJson.getJSONArray("types");

            List<Map<String, String>> typesAddedList = new ArrayList<>();
            for (int i = 0; i < typesJsonArray.length(); i++) {
                final String name = typesJsonArray.getString(i);
                final URI locationUri = ub.path(name).build();
                typesAddedList.add(
                        new HashMap<String, String>() {{
                            put(MetadataServiceClient.NAME, name);
                        }});
            }

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());
            response.put(MetadataServiceClient.URI_TYPES, typesAddedList);
            return Response.status(ClientResponse.Status.CREATED).entity(response).build();
        } catch (Exception e) {
            LOG.error("Unable to persist types", e);
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
    @Path("{typeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDefinition(@Context HttpServletRequest request,
                                  @PathParam("typeName") String typeName) {
        try {
            final String typeDefinition = metadataService.getTypeDefinition(typeName);

            JSONObject response = new JSONObject();
            response.put("typeName", typeName);
            response.put(MetadataServiceClient.DEFINITION, typeDefinition);
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
     * Gets the list of trait type names registered in the type system.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTypesByFilter(@Context HttpServletRequest request,
                                     @DefaultValue(TYPE_ALL) @QueryParam("type") String type) {
        try {
            List<String> result = null;
            if (TYPE_ALL.equals(type)) {
                result = metadataService.getTypeNamesList();
            } else {
                DataTypes.TypeCategory typeCategory = DataTypes.TypeCategory.valueOf(type);
                result = metadataService.getTypeNamesByCategory(typeCategory);
            }

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.RESULTS, new JSONArray(result));
            response.put(MetadataServiceClient.COUNT, result.size());
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (IllegalArgumentException ie) {
            LOG.error("Unsupported typeName while retrieving type list {}", type);
            throw new WebApplicationException(
                    Servlets.getErrorResponse("Unsupported type " + type, Response.Status.BAD_REQUEST));
        } catch (Exception e) {
            LOG.error("Unable to get types list", e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        }
    }
}
