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

import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.BaseRequest;
import org.apache.atlas.catalog.CollectionRequest;
import org.apache.atlas.catalog.DefaultTypeSystem;
import org.apache.atlas.catalog.EntityResourceProvider;
import org.apache.atlas.catalog.EntityTagResourceProvider;
import org.apache.atlas.catalog.InstanceRequest;
import org.apache.atlas.catalog.Result;
import org.apache.atlas.catalog.exception.CatalogException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Service which handles API requests for v1 entity resources.
 */
@Path("v1/entities")
@Singleton
public class EntityService extends BaseService {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.EntityService");

    private final EntityResourceProvider entityResourceProvider;
    private final EntityTagResourceProvider entityTagResourceProvider;

    @Inject
    public EntityService(MetadataService metadataService, AtlasTypeDefStore typeDefStore) throws AtlasBaseException {
        DefaultTypeSystem typeSystem = new DefaultTypeSystem(metadataService, typeDefStore);
        entityResourceProvider = new EntityResourceProvider(typeSystem);
        entityTagResourceProvider = new EntityTagResourceProvider(typeSystem);
    }

    @GET
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntities(@Context HttpHeaders headers, @Context UriInfo ui) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.getEntities()");
            }

            String queryString = decode(getQueryString(ui));

            BaseRequest request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);
            Result result = getResources(entityResourceProvider, request);

            return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("{entityId}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntity(@Context HttpHeaders headers,
                              @Context UriInfo ui,
                              @PathParam("entityId") String entityId) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.getEntity(" + entityId + ")");
            }

            BaseRequest request = new InstanceRequest(Collections.<String, Object>singletonMap("id", entityId));
            Result result = getResource(entityResourceProvider, request);

            return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("{entityId}/tags/{tag}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntityTag(@Context HttpHeaders headers,
                                 @Context UriInfo ui,
                                 @PathParam("entityId") String entityId,
                                 @PathParam("tag") String tagName) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.getEntityTag(" + entityId + ", " + tagName + ")");
            }

            Map<String, Object> properties = new HashMap<>();
            properties.put("id", entityId);
            properties.put("name", tagName);
            Result result = getResource(entityTagResourceProvider, new InstanceRequest(properties));

            return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("{entityId}/tags")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntityTags(@Context HttpHeaders headers,
                                  @Context UriInfo ui,
                                  @PathParam("entityId") String entityGuid) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.getEntityTags(" + entityGuid + ")");
            }

            BaseRequest request = new CollectionRequest(Collections.<String, Object>singletonMap("id", entityGuid),
                    decode(getQueryString(ui)));
            Result result = getResources(entityTagResourceProvider, request);

            return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("{entityId}/tags/{tag}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response tagEntity(String body,
                              @Context HttpHeaders headers,
                              @Context UriInfo ui,
                              @PathParam("entityId") String entityId,
                              @PathParam("tag") String tagName) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.tagEntity(" + entityId + ", " + tagName + ")");
            }

            Map<String, Object> properties = new HashMap<>();
            properties.put("id", entityId);
            properties.put("name", tagName);
            createResource(entityTagResourceProvider, new InstanceRequest(properties));

            return Response.status(Response.Status.CREATED).entity(
                    new Results(ui.getRequestUri().toString(), 201)).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response tagEntities(String body,
                                @Context HttpHeaders headers,
                                @Context UriInfo ui) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.tagEntities()");
            }

            Map<String, Object> properties = parsePayload(body);

            if (properties.get("tags") == null || properties.size() != 1) {
                throw new CatalogException(
                        "Invalid Request, no 'tags' property specified. Creation of entity resource not supported.", 400);

            }
            String queryString = decode(getQueryString(ui));
            Collection<String> createResults = createResources(
                    entityTagResourceProvider, new CollectionRequest(properties, queryString));

            Collection<Results> result = new ArrayList<>();
            for (String relativeUrl : createResults) {
                result.add(new Results(ui.getBaseUri().toString() + relativeUrl, 201));
            }

            return Response.status(Response.Status.CREATED).entity(
                    new GenericEntity<Collection<Results>>(result) {
                    }).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DELETE
    @Path("{entityId}/tags/{tag}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteEntityTag(@Context HttpHeaders headers,
                                    @Context UriInfo ui,
                                    @PathParam("entityId") String entityId,
                                    @PathParam("tag") String tagName) throws CatalogException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityService.deleteEntityTag()");
            }

            Map<String, Object> properties = new HashMap<>();
            properties.put("id", entityId);
            properties.put("name", tagName);
            deleteResource(entityTagResourceProvider, new InstanceRequest(properties));

            return Response.status(Response.Status.OK).entity(
                    new Results(ui.getRequestUri().toString(), 200)).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}
