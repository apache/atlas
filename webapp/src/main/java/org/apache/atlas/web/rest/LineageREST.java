/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.rest;


import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * REST interface for an entity's lineage information
 */
@Path("v2/lineage")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class LineageREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.LineageREST");

    private final AtlasLineageService atlasLineageService;
    private static final String DEFAULT_DIRECTION = "BOTH";
    private static final String DEFAULT_DEPTH     = "3";

    @Context
    private HttpServletRequest httpServletRequest;

    @Inject
    public LineageREST(AtlasLineageService atlasLineageService) {
        this.atlasLineageService = atlasLineageService;
    }

    /**
     * Returns lineage info about entity.
     * @param guid - unique entity id
     * @param direction - input, output or both
     * @param depth - number of hops for lineage
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @GET
    @Path("/{guid}")
    public AtlasLineageInfo getLineageGraph(@PathParam("guid") String guid,
                                            @QueryParam("direction") @DefaultValue(DEFAULT_DIRECTION)  LineageDirection direction,
                                            @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getLineageGraph(" + guid + "," + direction +
                                                               "," + depth + ")");
            }

            return atlasLineageService.getAtlasLineageInfo(guid, direction, depth);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}