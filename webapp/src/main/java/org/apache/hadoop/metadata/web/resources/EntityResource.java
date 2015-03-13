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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
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

    private static final String GUID = "GUID";
    private static final String TRAIT_NAME = "traitName";

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
            response.put(Servlets.REQUEST_ID, Servlets.getRequestId());
            response.put(GUID, guid);

            return Response.ok(response).build();
        } catch (MetadataException | IOException | IllegalArgumentException e) {
            LOG.error("Unable to persist instance for type {}", typeName, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to persist instance for type {}", typeName, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
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
        Preconditions.checkNotNull(guid, "Entity GUID cannot be null");

        try {
            LOG.debug("Fetching entity definition for guid={} ", guid);
            final String entityDefinition = metadataService.getEntityDefinition(guid);

            JSONObject response = new JSONObject();
            response.put(Servlets.REQUEST_ID, Servlets.getRequestId());
            response.put(GUID, guid);

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put("definition", entityDefinition);
                status = Response.Status.OK;
            }

            return Response.status(status).entity(response).build();

        } catch (MetadataException | IllegalArgumentException e) {
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
        Preconditions.checkNotNull(entityType, "Entity type cannot be null");
        try {
            LOG.debug("Fetching entity list for type={} ", entityType);
            final List<String> entityList = metadataService.getEntityList(entityType);

            JSONObject response = new JSONObject();
            response.put(Servlets.REQUEST_ID, Servlets.getRequestId());
            response.put("type", entityType);
            response.put("list", new JSONArray(entityList));

            return Response.ok(response).build();
        } catch (MetadataException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    // Trait management functions
    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     */
    @GET
    @Path("traits/list/{guid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTraitNames(@PathParam("guid") String guid) {
        Preconditions.checkNotNull(guid, "Entity GUID cannot be null");

        try {
            LOG.debug("Fetching trait names for entity={}", guid);
            final List<String> traitNames = metadataService.getTraitNames(guid);

            JSONObject response = new JSONObject();
            response.put(Servlets.REQUEST_ID, Servlets.getRequestId());
            response.put(GUID, guid);
            response.put("list", new JSONArray(traitNames));

            return Response.ok(response).build();
        } catch (MetadataException | IllegalArgumentException e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     */
    @POST
    @Path("traits/add/{guid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addTrait(@Context HttpServletRequest request,
                             @PathParam("guid") String guid) {
        Preconditions.checkNotNull(guid, "Entity GUID cannot be null");

        try {
            final String traitDefinition = Servlets.getRequestPayload(request);
            LOG.debug("Adding trait={} for entity={} ", traitDefinition, guid);
            metadataService.addTrait(guid, traitDefinition);

            JSONObject response = new JSONObject();
            response.put(Servlets.REQUEST_ID, Servlets.getRequestId());
            response.put(GUID, guid);
            response.put("traitInstance", traitDefinition);

            return Response.ok(response).build();
        } catch (MetadataException | IOException | IllegalArgumentException e) {
            LOG.error("Unable to add trait for entity={}", guid, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to add trait for entity={}", guid, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid      globally unique identifier for the entity
     * @param traitName name of the trait
     */
    @PUT
    @Path("traits/delete/{guid}/{traitName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteTrait(@Context HttpServletRequest request,
                                @PathParam("guid") String guid,
                                @PathParam(TRAIT_NAME) String traitName) {
        Preconditions.checkNotNull(guid, "Entity GUID cannot be null");
        Preconditions.checkNotNull(traitName, "Trait name cannot be null");

        LOG.debug("Deleting trait={} from entity={} ", traitName, guid);
        try {
            metadataService.deleteTrait(guid, traitName);

            JSONObject response = new JSONObject();
            response.put(Servlets.REQUEST_ID, Servlets.getRequestId());
            response.put(GUID, guid);
            response.put(TRAIT_NAME, traitName);

            return Response.ok(response).build();
        } catch (MetadataException | IllegalArgumentException e) {
            LOG.error("Unable to add trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to add trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
