/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AuditSearchParams;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.plugin.util.KeycloakUserStore;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.plugin.util.ServicePolicies;
import org.apache.atlas.policytransformer.CachePolicyTransformerImpl;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.policytransformer.CachePolicyTransformerImpl.ATTR_SERVICE_LAST_SYNC;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;

/**
 * REST interface for CRUD operations on tasks
 */
@Path("auth")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class AuthREST {
    private static final Logger LOG      = LoggerFactory.getLogger(AuthREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AuthREST");

    private CachePolicyTransformerImpl policyTransformer;
    private ESBasedAuditRepository auditRepository;
    private AtlasEntityStore entityStore;

    @Inject
    public AuthREST(CachePolicyTransformerImpl policyTransformer,
                    ESBasedAuditRepository auditRepository, AtlasEntityStore entityStore) {
        this.entityStore = entityStore;
        this.auditRepository = auditRepository;
        this.policyTransformer = policyTransformer;
    }

    @GET
    @Path("download/roles/{serviceName}")
    public RangerRoles downloadRoles(@PathParam("serviceName") final String serviceName,
                                     @QueryParam("pluginId") String pluginId,
                                     @DefaultValue("0") @QueryParam("lastUpdatedTime") Long lastUpdatedTime,
                                     @Context HttpServletResponse response) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AuthREST.downloadRoles(serviceName="+serviceName+", pluginId="+pluginId+", lastUpdatedTime="+lastUpdatedTime+")");
            }

            KeycloakUserStore keycloakUserStore = new KeycloakUserStore(serviceName);
            RangerRoles roles = keycloakUserStore.loadRolesIfUpdated(lastUpdatedTime);

            if (roles == null) {
                response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            }

            return roles;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("download/users/{serviceName}")
    public RangerUserStore downloadUserStore(@PathParam("serviceName") final String serviceName,
                                             @QueryParam("pluginId") String pluginId,
                                             @DefaultValue("0") @QueryParam("lastUpdatedTime") Long lastUpdatedTime,
                                             @Context HttpServletResponse response) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AuthREST.downloadUserStore(serviceName="+serviceName+", pluginId="+pluginId+", lastUpdatedTime="+lastUpdatedTime+")");
            }

            KeycloakUserStore keycloakUserStore = new KeycloakUserStore(serviceName);
            RangerUserStore userStore = keycloakUserStore.loadUserStoreIfUpdated(lastUpdatedTime);

            if (userStore == null) {
                response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            }

            return userStore;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("download/policies/{serviceName}")
    public ServicePolicies downloadPolicies(@PathParam("serviceName") final String serviceName,
                                            @QueryParam("pluginId") String pluginId,
                                            @DefaultValue("0") @QueryParam("lastUpdatedTime") Long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AuthREST.downloadPolicies(serviceName="+serviceName+", pluginId="+pluginId+", lastUpdatedTime="+lastUpdatedTime+")");
            }

            if (!isPolicyUpdated(serviceName, lastUpdatedTime)) {
                return null;
            }

            ServicePolicies ret = policyTransformer.getPolicies(serviceName, pluginId, lastUpdatedTime);

            updateLastSync(serviceName);

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void updateLastSync(String serviceName) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("AuthRest.updateLastSync." + serviceName);

        try {
            if (policyTransformer.getService() != null) {
                AtlasEntity serviceEntity = new AtlasEntity(policyTransformer.getService());
                serviceEntity.setAttribute(ATTR_SERVICE_LAST_SYNC, System.currentTimeMillis());
                try {
                    entityStore.createOrUpdate(new AtlasEntityStream(serviceEntity), false);
                } catch (AtlasBaseException e) {
                    LOG.error("Failed to update authServicePolicyLastSync time: {}", e.getMessage());
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private boolean isPolicyUpdated(String serviceName, long lastUpdatedTime) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("AuthRest.isPolicyUpdated." + serviceName);

        List<String> entityUpdateToWatch = new ArrayList<>();
        entityUpdateToWatch.add(POLICY_ENTITY_TYPE);
        entityUpdateToWatch.add(PERSONA_ENTITY_TYPE);
        entityUpdateToWatch.add(PURPOSE_ENTITY_TYPE);

        AuditSearchParams parameters = new AuditSearchParams();
        Map<String, Object> dsl = getMap("size", 1);

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(getMap("terms", getMap("typeName", entityUpdateToWatch)));

        lastUpdatedTime = lastUpdatedTime == -1 ? 0 : lastUpdatedTime;
        mustClauseList.add(getMap("range", getMap("timestamp", getMap("gte", lastUpdatedTime))));

        dsl.put("query", getMap("bool", getMap("must", mustClauseList)));

        parameters.setDsl(dsl);

        try {
            EntityAuditSearchResult result = auditRepository.searchEvents(parameters.getQueryString());

            if (result == null || CollectionUtils.isEmpty(result.getEntityAudits())) {
                LOG.info("getPoliciesIfUpdated: Skipping as no update found");
                return false;
            }
        } catch (AtlasBaseException e) {
            LOG.error("ERROR in getPoliciesIfUpdated while fetching entity audits {}: ", e.getMessage());
            return true;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return true;
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
