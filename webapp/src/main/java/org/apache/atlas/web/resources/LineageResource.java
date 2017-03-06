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
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.discovery.LineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.SchemaNotFoundException;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.LineageUtils;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@Path("lineage")
@Singleton
@Deprecated
public class LineageResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataSetLineageResource.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.LineageResource");

    private final AtlasLineageService atlasLineageService;
    private final LineageService      lineageService;
    private final AtlasTypeRegistry   typeRegistry;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured LineageService.
     *
     * @param lineageService lineage service handle
     */
    @Inject
    public LineageResource(LineageService lineageService, AtlasLineageService atlasLineageService, AtlasTypeRegistry typeRegistry) {
        this.lineageService      = lineageService;
        this.atlasLineageService = atlasLineageService;
        this.typeRegistry        = typeRegistry;
    }

    /**
     * Returns input lineage graph for the given entity id.
     * @param guid dataset entity id
     * @return
     */
    @GET
    @Path("{guid}/inputs/graph")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response inputsGraph(@PathParam("guid") String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> LineageResource.inputsGraph({})", guid);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageResource.inputsGraph(" + guid + ")");
            }

            AtlasLineageInfo lineageInfo = atlasLineageService.getAtlasLineageInfo(guid, LineageDirection.INPUT, -1);
            final String result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, new JSONObject(result));

            return Response.ok(response).build();
        } catch (AtlasBaseException e) {
            LOG.error("Unable to get lineage inputs graph for entity guid={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get lineage inputs graph for entity guid={}", guid, e);
            throw e;
        } catch (JSONException e) {
            LOG.error("Unable to get lineage inputs graph for entity guid={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== LineageResource.inputsGraph({})", guid);
            }
        }
    }

    /**
     * Returns the outputs graph for a given entity id.
     *
     * @param guid dataset entity id
     */
    @GET
    @Path("{guid}/outputs/graph")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response outputsGraph(@PathParam("guid") String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> LineageResource.outputsGraph({})", guid);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageResource.outputsGraph(" + guid + ")");
            }

            AtlasLineageInfo lineageInfo = atlasLineageService.getAtlasLineageInfo(guid, LineageDirection.OUTPUT, -1);
            final String result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, new JSONObject(result));

            return Response.ok(response).build();
        } catch (AtlasBaseException e) {
            LOG.error("Unable to get lineage outputs graph for entity guid={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get lineage outputs graph for entity guid={}", guid, e);
            throw e;
        } catch (JSONException e) {
            LOG.error("Unable to get lineage outputs graph for entity guid={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== LineageResource.outputsGraph({})", guid);
            }
        }
    }

    /**
     * Returns the schema for the given dataset id.
     *
     * @param guid dataset entity id
     */
    @GET
    @Path("{guid}/schema")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response schema(@PathParam("guid") String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> LineageResource.schema({})", guid);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageResource.schema(" + guid + ")");
            }

            final String jsonResult = lineageService.getSchemaForEntity(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, new JSONObject(jsonResult));

            return Response.ok(response).build();
        } catch (SchemaNotFoundException e) {
            LOG.error("schema not found for {}", guid);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (EntityNotFoundException e) {
            LOG.error("table entity not found for {}", guid);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get schema for entity guid={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get schema for entity guid={}", guid, e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get schema for entity={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== LineageResource.schema({})", guid);
            }
        }
    }
}
