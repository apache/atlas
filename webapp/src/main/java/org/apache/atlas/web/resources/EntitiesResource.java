/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.resources;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.EntityExistsException;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;

@Path("entities")
@Singleton
public class EntitiesResource {
    private static final Logger LOG = LoggerFactory.getLogger(EntitiesResource.class);

    @Inject
    private MetadataService metadataService;

    @Context
    UriInfo uriInfo;

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
            final String entities = Servlets.getRequestPayload(request);
            LOG.debug("submitting entities {} ", AtlasClient.toString(new JSONArray(entities)));

            final String guids = metadataService.createEntities(entities);

            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            URI locationURI = ub.path(guids).build();

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.GUID, new JSONArray(guids));
            response.put(AtlasClient.DEFINITION, metadataService.getEntityDefinition(new JSONArray(guids).getString(0)));

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
     * Gets the list of entities for a given entity type.
     *
     * @param entityType name of a type which is unique
     */
    @GET
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntityListByType(@QueryParam("type") String entityType) {
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
}
