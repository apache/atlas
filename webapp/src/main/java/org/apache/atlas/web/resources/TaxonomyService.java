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

import org.apache.atlas.catalog.*;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.exception.CatalogException;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.web.util.Servlets;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service which handles API requests for taxonomy and term resources.
 */
@Path("v1/taxonomies")
@Singleton
public class TaxonomyService extends BaseService {

    private ResourceProvider taxonomyResourceProvider;
    private ResourceProvider termResourceProvider;

    @Inject
    public void setMetadataService(MetadataService metadataService) {
        DefaultTypeSystem typeSystem = new DefaultTypeSystem(metadataService);
        taxonomyResourceProvider = createTaxonomyResourceProvider(typeSystem);
        termResourceProvider = createTermResourceProvider(typeSystem);
    }

    @GET
    @Path("{taxonomyName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTaxonomy(@Context HttpHeaders headers,
                                @Context UriInfo ui,
                                @PathParam("taxonomyName") String taxonomyName) throws CatalogException {

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", taxonomyName);
        Result result = getResource(taxonomyResourceProvider, new InstanceRequest(properties));
        return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
    }

    @GET
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTaxonomies(@Context HttpHeaders headers, @Context UriInfo ui) throws CatalogException {
        String queryString = decode(getQueryString(ui));
        Request request = new CollectionRequest(Collections.<String, Object>emptyMap(), queryString);
        Result result = getResources(taxonomyResourceProvider, request);
        return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
    }

    @POST
    @Path("{taxonomyName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response createTaxonomy(String body,
                                   @Context HttpHeaders headers,
                                   @Context UriInfo ui,
                                   @PathParam("taxonomyName") String taxonomyName) throws CatalogException {

        Map<String, Object> properties = parsePayload(body);
        properties.put("name", taxonomyName);

        createResource(taxonomyResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.CREATED).entity(
                new Results(ui.getRequestUri().toString(), 201)).build();
    }

    @DELETE
    @Path("{taxonomyName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteTaxonomy(@Context HttpHeaders headers,
                                   @Context UriInfo ui,
                                   @PathParam("taxonomyName") String taxonomyName) throws CatalogException {

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", taxonomyName);

        deleteResource(taxonomyResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.OK).entity(
                new Results(ui.getRequestUri().toString(), 200)).build();
    }

    @GET
    @Path("{taxonomyName}/terms/{termName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTaxonomyTerm(@Context HttpHeaders headers,
                                    @Context UriInfo ui,
                                    @PathParam("taxonomyName") String taxonomyName,
                                    @PathParam("termName") String termName) throws CatalogException {

        TermPath termPath = new TermPath(taxonomyName, termName);
        Map<String, Object> properties = new HashMap<>();
        properties.put("termPath", termPath);
        Result result = getResource(termResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
    }

    @GET
    @Path("{taxonomyName}/terms")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTaxonomyTerms(@Context HttpHeaders headers,
                                     @Context UriInfo ui,
                                     @PathParam("taxonomyName") String taxonomyName) throws CatalogException {

        String queryString = decode(getQueryString(ui));
        TermPath termPath = new TermPath(taxonomyName, null);
        Request request = new CollectionRequest(
                Collections.<String, Object>singletonMap("termPath", termPath), queryString);
        Result result = getResources(termResourceProvider, request);

        return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
    }

    @GET
    @Path("{taxonomyName}/terms/{rootTerm}/{remainder:.*}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getSubTerms(@Context HttpHeaders headers,
                            @Context UriInfo ui,
                            @PathParam("taxonomyName") String taxonomyName,
                            @PathParam("rootTerm") String rootTerm,
                            @PathParam("remainder") String remainder) throws CatalogException {

        Result result;
        String termName = String.format("%s%s", rootTerm,
                remainder.replaceAll("/?terms/?([.]*)", "$1."));
        String queryString = decode(getQueryString(ui));
        TermPath termPath = new TermPath(taxonomyName, termName);

        Map<String, Object> properties = new HashMap<>();
        properties.put("termPath", termPath);

        List<PathSegment> pathSegments = ui.getPathSegments();
        int lastIndex = pathSegments.size() - 1;
        String lastSegment = pathSegments.get(lastIndex).getPath();
        if (lastSegment.equals("terms") || (lastSegment.isEmpty() && pathSegments.get(lastIndex - 1).getPath().equals("terms"))) {
            result = getResources(termResourceProvider, new CollectionRequest(properties, queryString));
        } else {
            result = getResource(termResourceProvider, new InstanceRequest(properties));
        }

        return Response.status(Response.Status.OK).entity(getSerializer().serialize(result, ui)).build();
    }

    @POST
    @Path("{taxonomyName}/terms/{termName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response createTerm(String body,
                               @Context HttpHeaders headers,
                               @Context UriInfo ui,
                               @PathParam("taxonomyName") String taxonomyName,
                               @PathParam("termName") String termName) throws CatalogException {

        Map<String, Object> properties = parsePayload(body);
        validateName(termName);
        properties.put("termPath", new TermPath(taxonomyName, termName));
        createResource(termResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.CREATED).entity(
                new Results(ui.getRequestUri().toString(), 201)).build();
    }

    @POST
    @Path("{taxonomyName}/terms/{termName}/{remainder:.*}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response createSubTerm(String body,
                                  @Context HttpHeaders headers,
                                  @Context UriInfo ui,
                                  @PathParam("taxonomyName") String taxonomyName,
                                  @PathParam("termName") String termName,
                                  @PathParam("remainder") String remainder) throws CatalogException {

        Map<String, Object> properties = parsePayload(body);
        String[] pathTokens = remainder.split("/");
        validateName(pathTokens[pathTokens.length -1]);
        properties.put("termPath", new TermPath(taxonomyName, String.format("%s%s", termName,
                remainder.replaceAll("/?terms/?([.]*)", "$1."))));
        createResource(termResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.CREATED).entity(
                new Results(ui.getRequestUri().toString(), 201)).build();
    }

    @DELETE
    @Path("{taxonomyName}/terms/{termName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteTerm(@Context HttpHeaders headers,
                               @Context UriInfo ui,
                               @PathParam("taxonomyName") String taxonomyName,
                               @PathParam("termName") String termName) throws CatalogException {

        Map<String, Object> properties = new HashMap<>();
        properties.put("termPath", new TermPath(taxonomyName, termName));
        deleteResource(termResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.OK).entity(
                new Results(ui.getRequestUri().toString(), 200)).build();
    }

    @DELETE
    @Path("{taxonomyName}/terms/{termName}/{remainder:.*}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteSubTerm(@Context HttpHeaders headers,
                                  @Context UriInfo ui,
                                  @PathParam("taxonomyName") String taxonomyName,
                                  @PathParam("termName") String termName,
                                  @PathParam("remainder") String remainder) throws CatalogException {

        Map<String, Object> properties = new HashMap<>();
        properties.put("termPath", new TermPath(taxonomyName, String.format("%s%s", termName,
                remainder.replaceAll("/?terms/?([.]*)", "$1."))));
        deleteResource(termResourceProvider, new InstanceRequest(properties));

        return Response.status(Response.Status.OK).entity(
                new Results(ui.getRequestUri().toString(), 200)).build();
    }

    protected ResourceProvider createTaxonomyResourceProvider(AtlasTypeSystem typeSystem) {
        return new TaxonomyResourceProvider(typeSystem);
    }

    protected ResourceProvider createTermResourceProvider(AtlasTypeSystem typeSystem) {
        return new TermResourceProvider(typeSystem);
    }

    private void validateName(String name) throws InvalidPayloadException {
        if (name.contains(".")) {
            throw new InvalidPayloadException("The \"name\" property may not contain the character '.'");
        }
    }
}
