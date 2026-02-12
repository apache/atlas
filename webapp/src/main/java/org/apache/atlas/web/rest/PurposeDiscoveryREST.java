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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.discovery.PurposeDiscoveryService;
import org.apache.atlas.discovery.PurposeDiscoveryServiceImpl;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * REST interface for discovering Purpose entities accessible to users.
 * <p>
 * This API provides an optimized way to fetch all Purpose entities that a user
 * has access to, based on their username and group memberships. It eliminates
 * the need for frontend aggregation of AuthPolicy entities.
 * </p>
 * <p>
 * Note: This class is NOT Spring-managed to avoid initialization order issues.
 * It looks up AtlasDiscoveryService from Spring context on first use.
 * Jersey discovers this class via @Path annotation.
 * </p>
 *
 * @see PurposeDiscoveryService
 */
@Path("purposes")
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class PurposeDiscoveryREST {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeDiscoveryREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.PurposeDiscoveryREST");

    private volatile PurposeDiscoveryService purposeDiscoveryService;

    @Context
    private HttpServletRequest httpServletRequest;

    @Context
    private ServletContext servletContext;

    private PurposeDiscoveryService getPurposeDiscoveryService() {
        if (purposeDiscoveryService == null) {
            synchronized (this) {
                if (purposeDiscoveryService == null) {
                    // Look up AtlasDiscoveryService from Spring context on first use
                    ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(servletContext);
                    AtlasDiscoveryService atlasDiscoveryService = ctx.getBean(AtlasDiscoveryService.class);
                    purposeDiscoveryService = new PurposeDiscoveryServiceImpl(atlasDiscoveryService);
                }
            }
        }
        return purposeDiscoveryService;
    }

    /**
     * Discover all Purpose entities accessible to a user.
     * <p>
     * This endpoint queries AuthPolicy entities with policyCategory="purpose" that match
     * the user's username or group memberships, then returns the unique Purpose entities
     * referenced by those policies.
     * </p>
     *
     * @param request Request containing username, groups, and optional pagination parameters
     * @return Response containing accessible Purpose entities with pagination info
     * @throws AtlasBaseException if discovery fails
     *
     * @HTTP 200 Success - returns PurposeUserResponse with purposes
     * @HTTP 400 Bad Request - invalid request parameters
     * @HTTP 401 Unauthorized - authentication required
     * @HTTP 500 Internal Server Error - server-side error
     */
    @POST
    @Path("/user")
    @Timed
    public PurposeUserResponse getUserPurposes(PurposeUserRequest request) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getUserPurposes");

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "PurposeDiscoveryREST.getUserPurposes()");
            }

            // Validate request
            if (request == null) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Request body is required");
            }

            try {
                request.validate();
            } catch (IllegalArgumentException e) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, e.getMessage());
            }

            // Authorization check: users can only query their own purposes
            String authenticatedUser = RequestContext.getCurrentUser();
            if (authenticatedUser == null) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Authentication required");
            }
            if (!authenticatedUser.equals(request.getUsername())) {
                LOG.warn("User '{}' attempted to query purposes for different user '{}'",
                        authenticatedUser, request.getUsername());
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS,
                        authenticatedUser, "Purpose discovery for other users");
            }

            LOG.info("getUserPurposes: username={}, groupCount={}, limit={}, offset={}",
                    request.getUsername(),
                    request.getGroups() != null ? request.getGroups().size() : 0,
                    request.getLimit(),
                    request.getOffset());

            // Discover purposes
            PurposeUserResponse response = getPurposeDiscoveryService().discoverPurposesForUser(request);

            LOG.info("getUserPurposes: username={}, found {} purposes (total: {})",
                    request.getUsername(),
                    response.getCount(),
                    response.getTotalCount());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }
}
