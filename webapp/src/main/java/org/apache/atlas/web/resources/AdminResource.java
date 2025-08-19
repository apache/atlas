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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.discovery.SearchContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.impexp.MigrationStatus;
import org.apache.atlas.model.instance.AtlasCheckStateRequest;
import org.apache.atlas.model.instance.AtlasCheckStateResult;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.model.patches.AtlasPatch.AtlasPatches;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.repository.patches.AtlasPatchManager;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.services.MetricsService;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.SearchTracker;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.web.service.ActiveInstanceElectorService;
import org.apache.atlas.web.service.AtlasDebugMetricsSink;
import org.apache.atlas.web.service.AtlasHealthStatus;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.atlas.model.general.HealthStatus;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
import java.util.*;

import static org.apache.atlas.AtlasErrorCode.DEPRECATED_API;
import static org.apache.atlas.AtlasErrorCode.DISABLED_API;
import static org.apache.atlas.repository.Constants.STATUS;
import static org.apache.atlas.web.filters.AtlasCSRFPreventionFilter.CSRF_TOKEN;


/**
 * Jersey Resource for admin operations.
 */
@Path("admin")
@Singleton
@Service
public class AdminResource {
    private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AdminResource");

    private static final String isCSRF_ENABLED                 = "atlas.rest-csrf.enabled";
    private static final String BROWSER_USER_AGENT_PARAM       = "atlas.rest-csrf.browser-useragents-regex";
    private static final String CUSTOM_METHODS_TO_IGNORE_PARAM = "atlas.rest-csrf.methods-to-ignore";
    private static final String CUSTOM_HEADER_PARAM            = "atlas.rest-csrf.custom-header";
    private static final String isEntityUpdateAllowed          = "atlas.entity.update.allowed";
    private static final String isEntityCreateAllowed          = "atlas.entity.create.allowed";
    private static final String editableEntityTypes            = "atlas.ui.editable.entity.types";
    private static final String DEFAULT_EDITABLE_ENTITY_TYPES  = "hdfs_path";
    private static final String DEFAULT_UI_VERSION             = "atlas.ui.default.version";
    private static final String UI_VERSION_V2                  = "v2";
    private static final String UI_DATE_TIMEZONE_FORMAT_ENABLED = "atlas.ui.date.timezone.format.enabled";
    private static final String UI_DATE_FORMAT                 = "atlas.ui.date.format";
    private static final String UI_DATE_DEFAULT_FORMAT         = "MM/DD/YYYY hh:mm:ss A";
    private static final String OPERATION_STATUS               = "operationStatus";
    private static final List TIMEZONE_LIST                    = Arrays.asList(TimeZone.getAvailableIDs());

    @Inject
    private ActiveInstanceElectorService electorService;

    @Context
    private HttpServletRequest httpServletRequest;

    @Context
    private HttpServletResponse httpServletResponse;

    @Inject
    private AtlasHealthStatus atlasHealthStatus;

    private Response version;

    private static Configuration            atlasProperties;
    private final  ServiceState             serviceState;
    private final  MetricsService           metricsService;
    private final  SearchTracker            activeSearches;
    private final  AtlasTypeRegistry        typeRegistry;
    private final  ReentrantLock            importExportOperationLock;
    private final  TaskManagement           taskManagement;
    private final  AtlasEntityStore         entityStore;
    private final  AtlasPatchManager        patchManager;
    private final  String                   defaultUIVersion;
    private final  boolean                  isTimezoneFormatEnabled;
    private final  String                   uiDateFormat;
    private final  AtlasDebugMetricsSink    debugMetricsRESTSink;
    private final  boolean                  isDebugMetricsEnabled;
    private final  boolean                  isTasksEnabled;
    private final  boolean                  isOnDemandLineageEnabled;
    private final  int                      defaultLineageNodeCount;
    private final  MetricsRegistry          metricsRegistry;

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }
    }

    @Inject
    public AdminResource(ServiceState serviceState, MetricsService metricsService, AtlasTypeRegistry typeRegistry,
                         SearchTracker activeSearches, AtlasEntityStore entityStore, AtlasPatchManager patchManager,
                         TaskManagement taskManagement, AtlasDebugMetricsSink debugMetricsRESTSink, MetricsRegistry metricsRegistry) {
        this.serviceState              = serviceState;
        this.metricsService            = metricsService;
        this.activeSearches            = activeSearches;
        this.typeRegistry              = typeRegistry;
        this.entityStore               = entityStore;
        this.importExportOperationLock = new ReentrantLock();
        this.patchManager              = patchManager;
        this.taskManagement            = taskManagement;
        this.debugMetricsRESTSink      = debugMetricsRESTSink;
        this.metricsRegistry           = metricsRegistry;

        if (atlasProperties != null) {
            this.defaultUIVersion = atlasProperties.getString(DEFAULT_UI_VERSION, UI_VERSION_V2);
            this.isTimezoneFormatEnabled = atlasProperties.getBoolean(UI_DATE_TIMEZONE_FORMAT_ENABLED, true);
            this.uiDateFormat = atlasProperties.getString(UI_DATE_FORMAT, UI_DATE_DEFAULT_FORMAT);
            this.isDebugMetricsEnabled = AtlasConfiguration.DEBUG_METRICS_ENABLED.getBoolean();
            this.isTasksEnabled = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();
            this.isOnDemandLineageEnabled = AtlasConfiguration.LINEAGE_ON_DEMAND_ENABLED.getBoolean();
            this.defaultLineageNodeCount = AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getInt();
        } else {
            this.defaultUIVersion = UI_VERSION_V2;
            this.isTimezoneFormatEnabled = true;
            this.uiDateFormat = UI_DATE_DEFAULT_FORMAT;
            this.isDebugMetricsEnabled = false;
            this.isTasksEnabled = false;
            this.isOnDemandLineageEnabled = false;
            this.defaultLineageNodeCount = 3;
        }
    }

    /**
     * Fetches the thread stack dump for this application.
     *
     * @return json representing the thread stack dump.
     */
    @GET
    @Path("stack")
    @Produces(MediaType.TEXT_PLAIN)
    public String getThreadDump() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getThreadDump()");
        }

        ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();

        while (topThreadGroup.getParent() != null) {
            topThreadGroup = topThreadGroup.getParent();
        }
        Thread[] threads = new Thread[topThreadGroup.activeCount()];

        int nr = topThreadGroup.enumerate(threads);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nr; i++) {
            builder.append(threads[i].getName()).append("\nState: ").
                    append(threads[i].getState()).append("\n");
            String stackTrace = StringUtils.join(threads[i].getStackTrace(), "\n");
            builder.append(stackTrace);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getThreadDump()");
        }

        return builder.toString();
    }

    /**
     * Fetches the version for this application.
     *
     * @return json representing the version.
     */
    @GET
    @Path("version")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getVersion() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getVersion()");
        }

        if (version == null) {
            try {
                PropertiesConfiguration configProperties = new PropertiesConfiguration("atlas-buildinfo.properties");

                Map<String, Object> response = new HashMap<String, Object>();
                response.put("Version", configProperties.getString("build.version", "UNKNOWN"));
                response.put("Revision",configProperties.getString("vc.revision", "UNKNOWN"));
                response.put("Name", configProperties.getString("project.name", "apache-atlas"));
                response.put("Description", configProperties.getString("project.description",
                        "Metadata Management and Data Governance Platform over Hadoop"));

                // todo: add hadoop version?
                // response.put("Hadoop", VersionInfo.getVersion() + "-r" + VersionInfo.getRevision());
                version = Response.ok(AtlasJson.toV1Json(response)).build();
            } catch (ConfigurationException e) {
                throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getVersion()");
        }

        return version;
    }

    @GET
    @Path("status")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public Response getStatus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getStatus()");
        }

        Map<String, Object> responseData = new HashMap() {{
                put(STATUS, serviceState.getState().toString());
            }};


        Response response = Response.ok(AtlasJson.toV1Json(responseData)).build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getStatus()");
        }

        return response;
    }

    @GET
    @Path("session")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getUserProfile(@Context HttpServletRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getUserProfile()");
        }

        Response response;

        boolean isEntityUpdateAccessAllowed = false;
        boolean isEntityCreateAccessAllowed = false;
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String userName = null;
        Set<String> groups = new HashSet<>();
        if (auth != null) {
            userName = auth.getName();
            Collection<? extends GrantedAuthority> authorities = auth.getAuthorities();
            for (GrantedAuthority c : authorities) {
                groups.add(c.getAuthority());
            }

            isEntityUpdateAccessAllowed = AtlasAuthorizationUtils.isAccessAllowed(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE));
            isEntityCreateAccessAllowed = AtlasAuthorizationUtils.isAccessAllowed(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE));
        }

        Map<String, Object> responseData = new HashMap<>();

        responseData.put(isCSRF_ENABLED, AtlasCSRFPreventionFilter.isCSRF_ENABLED);
        responseData.put(BROWSER_USER_AGENT_PARAM, AtlasCSRFPreventionFilter.BROWSER_USER_AGENTS_DEFAULT);
        responseData.put(CUSTOM_METHODS_TO_IGNORE_PARAM, AtlasCSRFPreventionFilter.METHODS_TO_IGNORE_DEFAULT);
        responseData.put(CUSTOM_HEADER_PARAM, AtlasCSRFPreventionFilter.HEADER_DEFAULT);
        responseData.put(isEntityUpdateAllowed, isEntityUpdateAccessAllowed);
        responseData.put(isEntityCreateAllowed, isEntityCreateAccessAllowed);
        responseData.put(editableEntityTypes, getEditableEntityTypes(atlasProperties));
        responseData.put(DEFAULT_UI_VERSION, defaultUIVersion);
        responseData.put("userName", userName);
        responseData.put("groups", groups);
        responseData.put("timezones", TIMEZONE_LIST);
        responseData.put(UI_DATE_TIMEZONE_FORMAT_ENABLED, isTimezoneFormatEnabled);
        responseData.put(UI_DATE_FORMAT, uiDateFormat);
        responseData.put(AtlasConfiguration.DEBUG_METRICS_ENABLED.getPropertyName(), isDebugMetricsEnabled);
        responseData.put(AtlasConfiguration.TASKS_USE_ENABLED.getPropertyName(), isTasksEnabled);
        responseData.put(AtlasConfiguration.LINEAGE_ON_DEMAND_ENABLED.getPropertyName(), isOnDemandLineageEnabled);
        responseData.put(AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getPropertyName(), defaultLineageNodeCount);

        if (AtlasConfiguration.SESSION_TIMEOUT_SECS.getInt() != -1) {
            responseData.put(AtlasConfiguration.SESSION_TIMEOUT_SECS.getPropertyName(), AtlasConfiguration.SESSION_TIMEOUT_SECS.getInt());
        }

        String salt = (String) request.getSession().getAttribute(CSRF_TOKEN);
        if (StringUtils.isEmpty(salt)) {
            salt = RandomStringUtils.random(20, 0, 0, true, true, null, new SecureRandom());
            request.getSession().setAttribute(CSRF_TOKEN, salt);
        }

        responseData.put(CSRF_TOKEN, salt);

        response = Response.ok(AtlasJson.toV1Json(responseData)).build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getUserProfile()");
        }

        return response;
    }

    @GET
    @Path("health")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public Response healthCheck() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.healthCheck()");
        }

        Map<String, HealthStatus> result = new HashMap<>();

        AtlasGraph<Object, Object> graph = AtlasGraphProvider.getGraphInstance();

        boolean cassandraFailed = false;
        boolean elasticSearchFailed = false;
        List<String> failedServices = new ArrayList<>();
        try {
            List<HealthStatus> healthStatuses = atlasHealthStatus.getHealthStatuses();
            for (final HealthStatus healthStatus : healthStatuses) {
                result.put(healthStatus.name, healthStatus);
            }

            // Use lightweight Cassandra health check
            boolean cassandraHealthy = TagDAOCassandraImpl.getInstance().isHealthy();
            if (cassandraHealthy) {
                result.put("cassandra", new HealthStatus("cassandra", "ok", true, new Date().toString(), ""));
            } else {
                result.put("cassandra", new HealthStatus("cassandra", "error", false, new Date().toString(), "Cassandra health check failed"));
                cassandraFailed = true;
                failedServices.add("cassandra");
            }
        } catch (Exception e) {
            result.put("cassandra", new HealthStatus("cassandra", "error", false, new Date().toString(), e.toString()));
            cassandraFailed = true;
            failedServices.add("cassandra");
        }

        try {
            boolean isConnected = AtlasElasticsearchDatabase.getClient().ping(RequestOptions.DEFAULT);
            if (isConnected) {
                result.put("elasticsearch", new HealthStatus("elasticsearch", "ok", true, new Date().toString(), ""));
            } else {
                result.put("elasticsearch", new HealthStatus("elasticsearch", "error", false, new Date().toString(), "Elasticsearch ping failed"));
                elasticSearchFailed = true;
                failedServices.add("elasticsearch");
            }
        } catch (Exception e) {
            result.put("elasticsearch", new HealthStatus("elasticsearch", "error", false, new Date().toString(), e.toString()));
            elasticSearchFailed = true;
            failedServices.add("elasticsearch");
        }

        // Add failed services to MDC for logging/monitoring
        if (!failedServices.isEmpty()) {
            MDC.put("failedServices", String.join(",", failedServices));
            MDC.put("healthCheck", "health check failed");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.healthCheck()");
        }

        if (cassandraFailed || elasticSearchFailed || atlasHealthStatus.isAtleastOneComponentUnHealthy()) {
            return Response.status(500).entity(result).build();
        }

        return Response.status(200).entity(result).build();
    }

    @GET
    @Path("killtheleader")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response killTheLeader() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.killTheLeader()");
        }

        System.out.println(electorService);
        try{
            return Response.status(200).build();
        } finally {
            //do after actions
            electorService.quitElection();
        }
    }

    @GET
    @Path("isactive")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response isActive() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.isActive()");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.isActive()");
        }

        if (serviceState.getState().toString().equals(ServiceState.ServiceStateValue.ACTIVE.toString())) {
            return Response.ok().build();
        }

        return Response.serverError().build();
    }

    @GET
    @Path("metrics")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasMetrics getMetrics() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getMetrics()");
        }

        AtlasMetrics metrics = metricsService.getMetrics();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getMetrics()");
        }

        return metrics;
    }

    @GET
    @Path("metrics/prometheus")
    public void scrapMetrics(@Context HttpServletResponse httpServletResponse) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AdminResource.scrapMetrics()");
            }
            this.metricsRegistry.scrape(httpServletResponse.getWriter());
        } catch (IOException e) {
            //do nothing
            LOG.error("Failed to scrap metrics for prometheus");
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AdminResource.scrapMetrics()");
            }
        }
    }

    @GET
    @Path("pushMetricsToStatsd")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response pushMetricsToStatsd() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.pushMetricsToStatsd()");
        }

        metricsService.pushMetricsToStatsd();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.pushMetricsToStatsd()");
        }

        return Response.ok().build();
    }

    private void releaseExportImportLock() {
        importExportOperationLock.unlock();
    }

    @PUT
    @Path("/purge")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public EntityMutationResponse purgeByIds(Set<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            for (String guid : guids) {
                Servlets.validateQueryParamLength("guid", guid);
            }
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AdminResource.purgeByIds(" + guids  + ")");
            }

            EntityMutationResponse resp =  entityStore.purgeByIds(guids);

            return resp;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("/audit/{auditGuid}/details")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public List<AtlasEntityHeader> getAuditDetails(@PathParam("auditGuid") String auditGuid,
                                    @QueryParam("limit") @DefaultValue("10") int limit,
                                    @QueryParam("offset") @DefaultValue("0") int offset) throws AtlasBaseException {
        throw new AtlasBaseException(DEPRECATED_API, "/entity/auditSearch");
    }

    @GET
    @Path("activeSearches")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Set<String> getActiveSearches() {
        return activeSearches.getActiveSearches();
    }

    @DELETE
    @Path("activeSearches/{id}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public boolean terminateActiveSearch(@PathParam("id") String searchId) {
        SearchContext terminate = activeSearches.terminate(searchId);
        return null != terminate;
    }

    @POST
    @Path("checkstate")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    public AtlasCheckStateResult checkState(AtlasCheckStateRequest request) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "checkState(" + request + ")");
            }

            AtlasCheckStateResult ret = entityStore.checkState(request);

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("repairmeanings")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    public void repairmeanings(List<String> termGuid) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "repairmeanings(" + termGuid + ")");
            }

            entityStore.repairMeaningAttributeForTerms(termGuid);

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GET
    @Path("patches")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasPatches getAtlasPatches() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getAtlasPatches()");
        }

        AtlasPatches ret = patchManager.getAllPatches();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getAtlasPatches()");
        }

        return ret;
    }

    /*
     * This returns all tasks
     * Filtering support: Either filter by statusList or guids
     * @param guids filter tasks with specified list of guids, If not specified, return all tasks, will not have any effect if statusList is specified
     * @param statusList filter tasks with specified list of status, If not specified, return all tasks, will not have any effect if guids is specified
     * guids takes preference
     * */
    @GET
    @Path("/tasks")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public List<AtlasTask> getTaskStatus(@QueryParam("status") List<String> statusList, @QueryParam("guids") List<String> guids,
                                         @QueryParam("offset") @DefaultValue("0") int offset,
                                         @QueryParam("limit") @DefaultValue("20") int limit) throws AtlasBaseException {
        return CollectionUtils.isNotEmpty(guids) ? taskManagement.getByGuids(guids) : taskManagement.getAll(statusList, offset, limit);
    }

    /*
    * Retry failed/ in_progress tasks on demand (for a very special case of cassandra went down)
    * @Param taskGuids list of task guids to retry
    * */
    @POST
    @Path("/tasks/retry")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void retryFailedTasks(@QueryParam("guid") List<String> taskGuids) throws AtlasBaseException {
        //taskManagement.retryTasks(taskGuids);
        throw new AtlasBaseException(DISABLED_API, "META-2979: Limit tasks queue");
    }

    @DELETE
    @Path("/tasks")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public void deleteTask(@QueryParam("guids") List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            taskManagement.deleteByGuids(guids);
        }
    }

    @GET
    @Path("/debug/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Map getDebugMetrics() {
        return debugMetricsRESTSink.getMetrics();
    }

    @POST
    @Path("featureFlag")
    @Produces(MediaType.APPLICATION_JSON)
    public void setFeatureFlag(@QueryParam("key") String key, @QueryParam("value") String value) throws AtlasBaseException {
        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_FEATURE_FLAG_CUD), "featureFlag");
        FeatureFlagStore.setFlag(key, value);
    }

    @DELETE
    @Path("featureFlag/{flag}")
    @Produces(MediaType.APPLICATION_JSON)
    public void deleteFeatureFlag(@PathParam("flag") String key) throws AtlasBaseException {
        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_FEATURE_FLAG_CUD), "featureFlag");
        FeatureFlagStore.deleteFlag(key);
    }
    private String getEditableEntityTypes(Configuration config) {
        String ret = DEFAULT_EDITABLE_ENTITY_TYPES;

        if (config != null && config.containsKey(editableEntityTypes)) {
            Object value = config.getProperty(editableEntityTypes);

            if (value instanceof String) {
                ret = (String) value;
            } else if (value instanceof Collection) {
                StringBuilder sb = new StringBuilder();

                for (Object elem : ((Collection) value)) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }

                    sb.append(elem.toString());
                }

                ret = sb.toString();
            }
        }

        return ret;
    }

    private void acquireExportImportLock(String activity) throws AtlasBaseException {
        boolean alreadyLocked = importExportOperationLock.isLocked();
        if (alreadyLocked) {
            LOG.warn("Another export or import is currently in progress..aborting this " + activity, Thread.currentThread().getName());

            throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_IMPORT_EXPORT_LOCK);
        }

        importExportOperationLock.lock();
    }

}
