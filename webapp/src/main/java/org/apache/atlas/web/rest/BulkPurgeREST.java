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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.store.graph.v2.BulkPurgeService;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST endpoints for bulk purge operations.
 * Provides fast-path deletion for large numbers of entities belonging to a connection
 * or matching a qualifiedName prefix.
 */
@Path("bulk-purge")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class BulkPurgeREST {
    private static final Logger LOG      = LoggerFactory.getLogger(BulkPurgeREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.BulkPurgeREST");

    private final BulkPurgeService bulkPurgeService;

    @Inject
    public BulkPurgeREST(BulkPurgeService bulkPurgeService) {
        this.bulkPurgeService = bulkPurgeService;
    }

    /**
     * Submit a bulk purge request for all assets belonging to a connection.
     * The Connection entity itself is NOT deleted unless {@code deleteConnection} is true.
     *
     * @param connectionQualifiedName the qualifiedName of the connection whose assets to purge
     * @param deleteConnection        if true, delete the Connection entity after all child assets are purged (default: false)
     * @return requestId for tracking the purge progress
     */
    @POST
    @Path("/connection")
    public Response purgeByConnection(@QueryParam("connectionQualifiedName") String connectionQualifiedName,
                                      @QueryParam("deleteConnection") @DefaultValue("false") boolean deleteConnection,
                                      @QueryParam("workerCount") @DefaultValue("0") int workerCount) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BulkPurgeREST.purgeByConnection(" + connectionQualifiedName + ", deleteConnection=" + deleteConnection + ", workerCount=" + workerCount + ")");
            }

            if (StringUtils.isBlank(connectionQualifiedName)) {
                throw new AtlasBaseException("Query parameter 'connectionQualifiedName' is required");
            }

            validateWorkerCount(workerCount);

            // Authorization check
            AtlasAuthorizationUtils.verifyAccess(
                    new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE),
                    "Bulk purge by connection : " + connectionQualifiedName);

            String submittedBy = RequestContext.getCurrentUser();
            String requestId = bulkPurgeService.bulkPurgeByConnection(connectionQualifiedName, submittedBy, deleteConnection, workerCount);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("requestId", requestId);
            response.put("purgeKey", connectionQualifiedName);
            response.put("purgeMode", "CONNECTION");
            response.put("deleteConnection", deleteConnection);
            if (workerCount > 0) {
                response.put("workerCountOverride", workerCount);
            }
            response.put("message", "Bulk purge submitted successfully");

            LOG.info("BulkPurge: Connection purge submitted by {} for {}, deleteConnection={}, workerCount={}, requestId={}",
                    submittedBy, connectionQualifiedName, deleteConnection, workerCount, requestId);

            return Response.ok(response).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Submit a bulk purge request for all assets matching a qualifiedName prefix.
     *
     * @param prefix the qualifiedName prefix to match (minimum 10 characters)
     * @return requestId for tracking the purge progress
     */
    @POST
    @Path("/qualifiedName")
    public Response purgeByQualifiedName(@QueryParam("prefix") String prefix,
                                          @QueryParam("workerCount") @DefaultValue("0") int workerCount) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BulkPurgeREST.purgeByQualifiedName(" + prefix + ", workerCount=" + workerCount + ")");
            }

            if (StringUtils.isBlank(prefix)) {
                throw new AtlasBaseException("Query parameter 'prefix' is required");
            }

            if (prefix.length() < 10) {
                throw new AtlasBaseException("Query parameter 'prefix' must be at least 10 characters for safety");
            }

            validateWorkerCount(workerCount);

            // Authorization check
            AtlasAuthorizationUtils.verifyAccess(
                    new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE),
                    "Bulk purge by qualifiedName prefix: " + prefix);

            String submittedBy = RequestContext.getCurrentUser();
            String requestId = bulkPurgeService.bulkPurgeByQualifiedName(prefix, submittedBy, workerCount);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("requestId", requestId);
            response.put("purgeKey", prefix);
            response.put("purgeMode", "QUALIFIED_NAME_PREFIX");
            if (workerCount > 0) {
                response.put("workerCountOverride", workerCount);
            }
            response.put("message", "Bulk purge submitted successfully");

            LOG.info("BulkPurge: QualifiedName purge submitted by {} for prefix={}, workerCount={}, requestId={}",
                    submittedBy, prefix, workerCount, requestId);

            return Response.ok(response).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Check the status of a bulk purge request.
     *
     * @param requestId the requestId returned by the submit endpoint
     * @return current status including progress counts
     */
    @GET
    @Path("/status")
    public Response getStatus(@QueryParam("requestId") String requestId) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BulkPurgeREST.getStatus(" + requestId + ")");
            }

            if (StringUtils.isBlank(requestId)) {
                throw new AtlasBaseException("Query parameter 'requestId' is required");
            }

            // Authorization check
            AtlasAuthorizationUtils.verifyAccess(
                    new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE),
                    "Bulk purge status check: " + requestId);

            Map<String, Object> status = bulkPurgeService.getStatus(requestId);

            if (status == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Purge request not found: " + requestId))
                        .build();
            }

            return Response.ok(status).build();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Cancel an in-progress bulk purge.
     * The cancel is best-effort — some batches may complete after cancel is requested.
     *
     * @param requestId the requestId of the purge to cancel
     */
    @POST
    @Path("/cancel")
    public Response cancelPurge(@QueryParam("requestId") String requestId) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BulkPurgeREST.cancelPurge(" + requestId + ")");
            }

            if (StringUtils.isBlank(requestId)) {
                throw new AtlasBaseException("Query parameter 'requestId' is required");
            }

            // Authorization check
            AtlasAuthorizationUtils.verifyAccess(
                    new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE),
                    "Bulk purge cancel: " + requestId);

            boolean cancelled = bulkPurgeService.cancelPurge(requestId);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("requestId", requestId);

            if (cancelled) {
                response.put("message", "Cancel requested. The purge will stop after the current batch completes.");
                LOG.info("BulkPurge: Cancel requested for requestId={}", requestId);
                return Response.ok(response).build();
            } else {
                response.put("message", "No active purge found with requestId: " + requestId);
                return Response.status(Response.Status.NOT_FOUND).entity(response).build();
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void validateWorkerCount(int workerCount) throws AtlasBaseException {
        if (workerCount < 0) {
            throw new AtlasBaseException("workerCount must be non-negative");
        }
        int configuredMax = AtlasConfiguration.BULK_PURGE_WORKER_COUNT.getInt();
        if (workerCount > configuredMax) {
            throw new AtlasBaseException("workerCount " + workerCount + " exceeds configured maximum of " + configuredMax);
        }
    }
}
