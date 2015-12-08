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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.discovery.LineageService;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

/**
 * Jersey Resource for Hive Table Lineage.
 */
@Path("lineage/hive")
@Singleton
public class HiveLineageResource {

    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageResource.class);

    private final LineageService lineageService;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured LineageService.
     *
     * @param lineageService lineage service handle
     */
    @Inject
    public HiveLineageResource(LineageService lineageService) {
        this.lineageService = lineageService;
    }

    /**
     * Returns the inputs graph for a given entity.
     *
     * @param tableName table name
     */
    @GET
    @Path("table/{tableName}/inputs/graph")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response inputsGraph(@Context HttpServletRequest request, @PathParam("tableName") String tableName) {
        LOG.info("Fetching lineage inputs graph for tableName={}", tableName);

        try {
            ParamChecker.notEmpty(tableName, "table name cannot be null");
            final String jsonResult = lineageService.getInputsGraph(tableName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put("tableName", tableName);
            response.put(AtlasClient.RESULTS, new JSONObject(jsonResult));

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("table entity not found for {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get lineage inputs graph for table {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get lineage inputs graph for table {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Returns the outputs graph for a given entity.
     *
     * @param tableName table name
     */
    @GET
    @Path("table/{tableName}/outputs/graph")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response outputsGraph(@Context HttpServletRequest request, @PathParam("tableName") String tableName) {
        LOG.info("Fetching lineage outputs graph for tableName={}", tableName);

        try {
            ParamChecker.notEmpty(tableName, "table name cannot be null");
            final String jsonResult = lineageService.getOutputsGraph(tableName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put("tableName", tableName);
            response.put(AtlasClient.RESULTS, new JSONObject(jsonResult));

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("table entity not found for {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get lineage outputs graph for table {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get lineage outputs graph for table {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Return the schema for the given tableName.
     *
     * @param tableName table name
     */
    @GET
    @Path("table/{tableName}/schema")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response schema(@Context HttpServletRequest request, @PathParam("tableName") String tableName) {
        LOG.info("Fetching schema for tableName={}", tableName);

        try {
            ParamChecker.notEmpty(tableName, "table name cannot be null");
            final String jsonResult = lineageService.getSchema(tableName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put("tableName", tableName);
            response.put(AtlasClient.RESULTS, new JSONObject(jsonResult));

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("table entity not found for {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get schema for table {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get schema for table {}", tableName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
