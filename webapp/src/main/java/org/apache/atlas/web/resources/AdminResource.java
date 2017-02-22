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

import com.google.inject.Inject;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetricsService;
import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.apache.atlas.authorize.simple.AtlasAuthorizationUtils;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.web.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.inject.Singleton;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.configuration.Configuration;

import static org.apache.atlas.repository.converters.AtlasInstanceConverter.toAtlasBaseException;


/**
 * Jersey Resource for admin operations.
 */
@Path("admin")
@Singleton
public class AdminResource {
    private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);

    @Context
    private HttpServletRequest httpServletRequest;

    @Context
    private HttpServletResponse httpServletResponse;

    private final ReentrantLock importExportOperationLock;

    private static final String isCSRF_ENABLED = "atlas.rest-csrf.enabled";
    private static final String BROWSER_USER_AGENT_PARAM = "atlas.rest-csrf.browser-useragents-regex";
    private static final String CUSTOM_METHODS_TO_IGNORE_PARAM = "atlas.rest-csrf.methods-to-ignore";
    private static final String CUSTOM_HEADER_PARAM = "atlas.rest-csrf.custom-header";
    private static final String isTaxonomyEnabled = "atlas.feature.taxonomy.enable";
    private static final String isEntityUpdateAllowed = "atlas.entity.update.allowed";
    private static final String isEntityCreateAllowed = "atlas.entity.create.allowed";
    private static final String editableEntityTypes = "atlas.ui.editable.entity.types";
    private static final String DEFAULT_EDITABLE_ENTITY_TYPES = "hdfs_path,hbase_table,hbase_column,hbase_column_family,kafka_topic";
    private Response version;

    private final ServiceState      serviceState;
    private final MetricsService    metricsService;
    private final AtlasTypeRegistry typeRegistry;
    private final AtlasTypeDefStore typesDefStore;
    private final AtlasEntityStore  entityStore;
    private static Configuration atlasProperties;

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }
    }

    @Inject
    public AdminResource(ServiceState serviceState, MetricsService metricsService,
                         AtlasTypeRegistry typeRegistry, AtlasTypeDefStore typeDefStore,
                         AtlasEntityStore entityStore) {
        this.serviceState               = serviceState;
        this.metricsService             = metricsService;
        this.typeRegistry               = typeRegistry;
        this.typesDefStore              = typeDefStore;
        this.entityStore                = entityStore;
        this.importExportOperationLock  = new ReentrantLock();
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

                JSONObject response = new JSONObject();
                response.put("Version", configProperties.getString("build.version", "UNKNOWN"));
                response.put("Name", configProperties.getString("project.name", "apache-atlas"));
                response.put("Description", configProperties.getString("project.description",
                        "Metadata Management and Data Governance Platform over Hadoop"));

                // todo: add hadoop version?
                // response.put("Hadoop", VersionInfo.getVersion() + "-r" + VersionInfo.getRevision());
                version = Response.ok(response).build();
            } catch (JSONException | ConfigurationException e) {
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
    public Response getStatus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getStatus()");
        }

        Response response;

        try {
            JSONObject responseData = new JSONObject();
            responseData.put(AtlasClient.STATUS, serviceState.getState().toString());
            response = Response.ok(responseData).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getStatus()");
        }

        return response;
    }

    @GET
    @Path("session")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getUserProfile() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getUserProfile()");
        }

        Response response;
        Boolean enableTaxonomy = false;
        try {
            if(atlasProperties != null) {
                enableTaxonomy = atlasProperties.getBoolean(isTaxonomyEnabled, false);
            }

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
                isEntityUpdateAccessAllowed = AtlasAuthorizationUtils.isAccessAllowed(AtlasResourceTypes.ENTITY,
                        AtlasActionTypes.UPDATE, userName, groups);
                isEntityCreateAccessAllowed = AtlasAuthorizationUtils.isAccessAllowed(AtlasResourceTypes.ENTITY,
                        AtlasActionTypes.CREATE, userName, groups);
            }

            JSONObject responseData = new JSONObject();

            responseData.put(isCSRF_ENABLED, AtlasCSRFPreventionFilter.isCSRF_ENABLED);
            responseData.put(BROWSER_USER_AGENT_PARAM, AtlasCSRFPreventionFilter.BROWSER_USER_AGENTS_DEFAULT);
            responseData.put(CUSTOM_METHODS_TO_IGNORE_PARAM, AtlasCSRFPreventionFilter.METHODS_TO_IGNORE_DEFAULT);
            responseData.put(CUSTOM_HEADER_PARAM, AtlasCSRFPreventionFilter.HEADER_DEFAULT);
            responseData.put(isTaxonomyEnabled, enableTaxonomy);
            responseData.put(isEntityUpdateAllowed, isEntityUpdateAccessAllowed);
            responseData.put(isEntityCreateAllowed, isEntityCreateAccessAllowed);
            responseData.put(editableEntityTypes, getEditableEntityTypes(atlasProperties));
            responseData.put("userName", userName);
            responseData.put("groups", groups);

            response = Response.ok(responseData).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getUserProfile()");
        }

        return response;
    }

    @GET
    @Path("metrics")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasMetrics getMetrics(@QueryParam("ignoreCache") boolean ignoreCache) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.getMetrics()");
        }

        AtlasMetrics metrics = metricsService.getMetrics(ignoreCache);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdminResource.getMetrics()");
        }

        return metrics;
    }

    private void releaseExportImportLock() {
        importExportOperationLock.unlock();
    }

    @POST
    @Path("/export")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    public Response export(AtlasExportRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.export()");
        }

        acquireExportImportLock("export");

        ZipSink exportSink = null;
        try {
            exportSink = new ZipSink();
            ExportService exportService = new ExportService(this.typeRegistry);

            AtlasExportResult result = exportService.run(exportSink, request, Servlets.getUserName(httpServletRequest),
                                                         Servlets.getHostName(httpServletRequest),
                                                         Servlets.getRequestIpAddress(httpServletRequest));

            exportSink.close();

            ServletOutputStream outStream = httpServletResponse.getOutputStream();
            exportSink.writeTo(outStream);

            httpServletResponse.setContentType("application/zip");
            httpServletResponse.setHeader("Content-Disposition",
                                          "attachment; filename=" + result.getClass().getSimpleName());

            outStream.flush();
            return Response.ok().build();
        } catch (IOException excp) {
            LOG.error("export() failed", excp);

            throw new AtlasBaseException(excp);
        } finally {
            releaseExportImportLock();

            if (exportSink != null) {
                exportSink.close();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AdminResource.export()");
            }
        }
    }

    @POST
    @Path("/import")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Consumes(Servlets.BINARY)
    public AtlasImportResult importData(byte[] bytes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.importData(bytes.length={})", bytes.length);
        }

        acquireExportImportLock("import");

        AtlasImportResult result;

        try {
            AtlasImportRequest   request       = new AtlasImportRequest(Servlets.getParameterMap(httpServletRequest));
            ByteArrayInputStream inputStream   = new ByteArrayInputStream(bytes);
            ImportService        importService = new ImportService(this.typesDefStore, this.entityStore);

            ZipSource zipSource = new ZipSource(inputStream);

            result = importService.run(zipSource, request, Servlets.getUserName(httpServletRequest),
                                       Servlets.getHostName(httpServletRequest),
                                       Servlets.getRequestIpAddress(httpServletRequest));
        } catch (Exception excp) {
            LOG.error("importData(binary) failed", excp);

            throw new AtlasBaseException(excp);
        } finally {
            releaseExportImportLock();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AdminResource.importData(binary)");
            }
        }

        return result;
    }

    @POST
    @Path("/importfile")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasImportResult importFile() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdminResource.importFile()");
        }

        acquireExportImportLock("importFile");

        AtlasImportResult result;

        try {
            AtlasImportRequest request       = new AtlasImportRequest(Servlets.getParameterMap(httpServletRequest));
            ImportService      importService = new ImportService(this.typesDefStore, this.entityStore);

            result = importService.run(request, Servlets.getUserName(httpServletRequest),
                                       Servlets.getHostName(httpServletRequest),
                                       Servlets.getRequestIpAddress(httpServletRequest));
        } catch (Exception excp) {
            LOG.error("importFile() failed", excp);

            throw new AtlasBaseException(excp);
        } finally {
            releaseExportImportLock();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AdminResource.importFile()");
            }
        }

        return result;
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
