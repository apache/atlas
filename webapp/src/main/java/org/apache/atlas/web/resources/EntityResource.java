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

package org.apache.atlas.web.resources;

import com.google.common.base.Preconditions;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
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
    private static final String TRAIT_NAME = "traitName";

    private final MetadataService metadataService;

    @Context
    UriInfo uriInfo;

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
     * Submits the entity definitions (instances).
     * The body contains the JSONArray of entity json. The service takes care of de-duping the entities based on any
     * unique attribute for the give type.
     */
    @POST
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response submit(@Context HttpServletRequest request) {
        try {
            String entities = Servlets.getRequestPayload(request);

            //Handle backward compatibility - if entities is not JSONArray, convert to JSONArray
            try {
                new JSONArray(entities);
            } catch (JSONException e) {
                final String finalEntities = entities;
                entities = new JSONArray() {{
                    put(finalEntities);
                }}.toString();
            }

            LOG.debug("submitting entities {} ", AtlasClient.toString(new JSONArray(entities)));

            final String guids = metadataService.createEntities(entities);

            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            URI locationURI = ub.path(guids).build();

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, new JSONArray(guids));
            response.put(AtlasClient.DEFINITION, new JSONObject(metadataService.getEntityDefinition(new JSONArray(guids).getString(0))));

            return Response.created(locationURI).entity(response).build();

        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a desrialization error ", ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Complete update of a set of entities - the values not specified will be replaced with null/removed
     * Adds/Updates given entities identified by its GUID or unique attribute
     * @return response payload as json
     */
    @PUT
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateEntities(@Context HttpServletRequest request) {
        try {
            final String entities = Servlets.getRequestPayload(request);
            LOG.debug("updating entities {} ", AtlasClient.toString(new JSONArray(entities)));

            final String guids = metadataService.updateEntities(entities);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, new JSONArray(guids));
            response.put(AtlasClient.DEFINITION, metadataService.getEntityDefinition(new JSONArray(guids).getString(0)));

            return Response.ok(response).build();
        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a desrialization error ", ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Adds/Updates given entity identified by its unique attribute( entityType, attributeName and value)
     * Updates support only partial update of an entity - Adds/updates any new values specified
     * Updates do not support removal of attribute values
     *
     * @param entityType the entity type
     * @param attribute the unique attribute used to identify the entity
     * @param value the unique attributes value
     * @param request The updated entity json
     * @return response payload as json
     * The body contains the JSONArray of entity json. The service takes care of de-duping the entities based on any
     * unique attribute for the give type.
     */
    @POST
    @Path("qualifiedName")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateByUniqueAttribute(@QueryParam("type") String entityType,
                                            @QueryParam("property") String attribute,
                                            @QueryParam("value") String value, @Context HttpServletRequest request) {
        try {
            String entities = Servlets.getRequestPayload(request);

            LOG.debug("Partially updating entity by unique attribute {} {} {} {} ", entityType, attribute, value, entities);

            Referenceable updatedEntity =
                InstanceSerialization.fromJsonReferenceable(entities, true);
            final String guid = metadataService.updateEntityByUniqueAttribute(entityType, attribute, value, updatedEntity);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Thread.currentThread().getName());
            response.put(AtlasClient.GUID, guid);
            return Response.ok(response).build();
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a desrialization error ", ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with type={} and qualifiedName={} does not exist", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to create/update entity {}" + entityType + ":" + attribute + "." + value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to update entity {}" + entityType + ":" + attribute + "." + value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Updates entity identified by its GUID
     * Support Partial update of an entity - Adds/updates any new values specified
     * Does not support removal of attribute values
     *
     * @param guid
     * @param request The updated entity json
     * @return
     */
    @POST
    @Path("{guid}")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateEntityByGuid(@PathParam("guid") String guid, @QueryParam("property") String attribute,
                                       @Context HttpServletRequest request) {
        if (StringUtils.isEmpty(attribute)) {
            return updateEntityPartialByGuid(guid, request);
        } else {
            return updateEntityAttributeByGuid(guid, attribute, request);
        }
    }

    private Response updateEntityPartialByGuid(String guid, HttpServletRequest request) {
        try {
            ParamChecker.notEmpty(guid, "Guid property cannot be null");
            final String entityJson = Servlets.getRequestPayload(request);
            LOG.debug("partially updating entity for guid {} : {} ", guid, entityJson);

            Referenceable updatedEntity =
                    InstanceSerialization.fromJsonReferenceable(entityJson, true);
            metadataService.updateEntityPartialByGuid(guid, updatedEntity);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Thread.currentThread().getName());
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to update entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to update entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Supports Partial updates
     * Adds/Updates given entity specified by its GUID
     * Supports updation of only simple primitive attributes like strings, ints, floats, enums, class references and
     * does not support updation of complex types like arrays, maps
     * @param guid entity id
     * @param property property to add
     * @postbody property's value
     * @return response payload as json
     */
    private Response updateEntityAttributeByGuid(String guid, String property, HttpServletRequest request) {
        try {
            Preconditions.checkNotNull(property, "Entity property cannot be null");
            String value = Servlets.getRequestPayload(request);
            Preconditions.checkNotNull(value, "Entity value cannot be null");

            metadataService.updateEntityAttributeByGuid(guid, property, value);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Thread.currentThread().getName());
            response.put(AtlasClient.GUID, guid);

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to add property {} to entity id {}", property, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to add property {} to entity id {}", property, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     */
    @GET
    @Path("{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntityDefinition(@PathParam("guid") String guid) {
        try {
            LOG.debug("Fetching entity definition for guid={} ", guid);
            ParamChecker.notEmpty(guid, "guid cannot be null");
            final String entityDefinition = metadataService.getEntityDefinition(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, guid);

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
                status = Response.Status.OK;
            } else {
                response.put(AtlasClient.ERROR,
                        Servlets.escapeJsonString(String.format("An entity with GUID={%s} does not exist", guid)));
            }

            return Response.status(status).entity(response).build();

        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Bad GUID={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get instance definition for GUID {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Gets the list of entities for a given entity type.
     *
     * @param entityType name of a type which is unique
     */
    public Response getEntityListByType(String entityType) {
        try {
            Preconditions.checkNotNull(entityType, "Entity type cannot be null");

            LOG.debug("Fetching entity list for type={} ", entityType);
            final List<String> entityList = metadataService.getEntityList(entityType);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPENAME, entityType);
            response.put(AtlasClient.RESULTS, new JSONArray(entityList));
            response.put(AtlasClient.COUNT, entityList.size());

            return Response.ok(response).build();
        } catch (NullPointerException e) {
            LOG.error("Entity type cannot be null", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    @GET
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntity(@QueryParam("type") String entityType,
                              @QueryParam("property") String attribute,
                              @QueryParam("value") String value) {
        if (StringUtils.isEmpty(attribute)) {
            //List API
            return getEntityListByType(entityType);
        } else {
            //Get entity by unique attribute
            return getEntityDefinitionByAttribute(entityType, attribute, value);
        }
    }

    /**
     * Fetch the complete definition of an entity given its qualified name.
     *
     * @param entityType
     * @param attribute
     * @param value
     */
    public Response getEntityDefinitionByAttribute(String entityType, String attribute, String value) {
        try {
            LOG.debug("Fetching entity definition for type={}, qualified name={}", entityType, value);
            ParamChecker.notEmpty(entityType, "Entity type cannot be null");
            ParamChecker.notEmpty(attribute, "attribute name cannot be null");
            ParamChecker.notEmpty(value, "attribute value cannot be null");

            final String entityDefinition = metadataService.getEntityDefinition(entityType, attribute, value);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
                status = Response.Status.OK;
            } else {
                response.put(AtlasClient.ERROR, Servlets.escapeJsonString(String.format("An entity with type={%s}, " +
                        "qualifiedName={%s} does not exist", entityType, value)));
            }

            return Response.status(status).entity(response).build();

        } catch (EntityNotFoundException e) {
            LOG.error("An entity with type={} and qualifiedName={} does not exist", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Bad type={}, qualifiedName={}", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get instance definition for type={}, qualifiedName={}", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
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
    @Path("{guid}/traits")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTraitNames(@PathParam("guid") String guid) {
        try {
            LOG.debug("Fetching trait names for entity={}", guid);
            final List<String> traitNames = metadataService.getTraitNames(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, guid);
            response.put(AtlasClient.RESULTS, new JSONArray(traitNames));
            response.put(AtlasClient.COUNT, traitNames.size());

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     */
    @POST
    @Path("{guid}/traits")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response addTrait(@Context HttpServletRequest request, @PathParam("guid") String guid) {
        try {
            final String traitDefinition = Servlets.getRequestPayload(request);
            LOG.debug("Adding trait={} for entity={} ", traitDefinition, guid);
            metadataService.addTrait(guid, traitDefinition);

            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            URI locationURI = ub.path(guid).build();

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, guid);

            return Response.created(locationURI).entity(response).build();
        } catch (EntityNotFoundException | TypeNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to add trait for entity={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to add trait for entity={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid      globally unique identifier for the entity
     * @param traitName name of the trait
     */
    @DELETE
    @Path("{guid}/traits/{traitName}")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteTrait(@Context HttpServletRequest request, @PathParam("guid") String guid,
            @PathParam(TRAIT_NAME) String traitName) {
        LOG.debug("Deleting trait={} from entity={} ", traitName, guid);
        try {
            metadataService.deleteTrait(guid, traitName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, guid);
            response.put(TRAIT_NAME, traitName);

            return Response.ok(response).build();
        } catch (EntityNotFoundException | TypeNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to delete trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to delete trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
