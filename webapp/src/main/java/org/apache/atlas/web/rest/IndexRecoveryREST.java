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
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.IndexRecoveryService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.DateTimeHelper;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;

@Path("v2/indexrecovery")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class IndexRecoveryREST {
    private static final Logger LOG      = LoggerFactory.getLogger(IndexRecoveryREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.IndexRecoveryREST");

    private final IndexRecoveryService indexRecoveryService;
    private final AtlasGraph           graph;

    @Inject
    IndexRecoveryREST(IndexRecoveryService indexRecoveryService, AtlasGraph graph) {
        this.indexRecoveryService = indexRecoveryService;
        this.graph                = graph;
    }
    /**
     * @return Future index recovery start time and previous recovery start time if applicable
     * @HTTP 200 If Index recovery data exists for the given entity
     * @HTTP 400 Bad query parameters
     */
    @GET
    @Timed
    public Map<String, String> getIndexRecoveryData() {

        AtlasPerfTracer     perf              = null;
        Long                startTime         = null;
        Long                prevTime          = null;
        Long                customStartTime   = null;
        Map<String, String> indexRecoveryData = new HashMap<>();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "IndexRecoveryREST.getIndexRecoveryData()");
            }

            AtlasVertex indexRecoveryVertex = indexRecoveryService.recoveryInfoManagement.findVertex();
            if (indexRecoveryVertex != null) {
                startTime        = indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
                prevTime         = indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class);
                customStartTime  = indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class);
            }

            indexRecoveryData.put(getPropertyKeyByRemovingPrefix(PROPERTY_KEY_INDEX_RECOVERY_START_TIME), startTime != null ? Instant.ofEpochMilli(startTime).toString() : "Not applicable");
            indexRecoveryData.put(getPropertyKeyByRemovingPrefix(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME), prevTime != null ? Instant.ofEpochMilli(prevTime).toString() : "Not applicable");
            indexRecoveryData.put(getPropertyKeyByRemovingPrefix(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME), customStartTime != null ? Instant.ofEpochMilli(customStartTime).toString() : "Not applicable");

        } finally {
            AtlasPerfTracer.log(perf);
        }

        return indexRecoveryData;
    }

    @POST
    @Path("/start")
    public void startCustomIndexRecovery(@QueryParam("startTime") @DateTimeFormat(pattern = DateTimeHelper.ISO8601_FORMAT)
                                                            final String startTime) throws AtlasBaseException, AtlasException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "IndexRecoveryREST.getIndexRecoveryData()");
            }

            AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_IMPORT), "to start dynamic index recovery by custom time");

            if (startTime == null) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Index Recovery requested without start time");
            }

            if (!indexRecoveryService.recoveryThread.isIndexBackendHealthy()) {
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Index recovery can not be started - Solr Health: Unhealthy");
            }

            long startTimeMilli = Instant.parse(startTime).toEpochMilli();

            indexRecoveryService.recoveryThread.stopMonitoringByUserRequest();

            indexRecoveryService.recoveryThread.startMonitoringByUserRequest(startTimeMilli);

            indexRecoveryService.recoveryInfoManagement.updateCustomStartTime(startTimeMilli);

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    public static String getPropertyKeyByRemovingPrefix(String propertyKey) {
        return StringUtils.removeStart(propertyKey, INDEX_RECOVERY_PREFIX);
    }
}