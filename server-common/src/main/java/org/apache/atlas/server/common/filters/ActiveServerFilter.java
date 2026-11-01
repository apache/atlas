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

package org.apache.atlas.server.common.filters;

import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;

import java.io.IOException;

/**
 * Returns 503 while this node is still starting ({@code BECOMING_ACTIVE}) so load-balancers
 * can gate traffic until {@code AtlasActivationService} completes. In active-active peer mode
 * every node becomes ACTIVE; there is no redirect to another node and no passive state.
 */
public class ActiveServerFilter implements Filter {
    private static final Logger LOG                          = LoggerFactory.getLogger(ActiveServerFilter.class);
    private static final String MIGRATION_STATUS_STATIC_PAGE = "migration-status.html";

    private final String[] adminUriNotFiltered = {
            "/admin/export", "/admin/import", "/admin/importfile", "/admin/audits",
            "/admin/purge", "/admin/expimp/audit", "/admin/metrics", "/admin/server", "/admin/audit/", "admin/tasks",
            "/admin/debug/metrics", "/admin/audits/ageout", "admin/async/import", "admin/async/import/status"
    };
    private final String[] adminUriNotSupportedInMigration = {
            "/admin/export", "/admin/import", "/admin/importfile", "admin/async/import"
    };

    private final ServiceStateProvider serviceState;

    public ActiveServerFilter(ServiceStateProvider serviceState) {
        this.serviceState = serviceState;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("ActiveServerFilter initialized");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        if (isAdminURISupportedInCurrentState(servletRequest)) {
            LOG.debug("URL {} is supported in state {}. Passing request downstream.",
                    ((HttpServletRequest) servletRequest).getRequestURI(), serviceState.getStateName());
            filterChain.doFilter(servletRequest, servletResponse);
        } else if (serviceState.isActive()) {
            LOG.debug("Instance is active (state={}). Passing request downstream", serviceState.getStateName());
            filterChain.doFilter(servletRequest, servletResponse);
        } else if (serviceState.isInstanceInTransition()) {
            LOG.error("Instance in transition (state={}). Service may not be ready to return a result",
                    serviceState.getStateName());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } else if (serviceState.isInstanceInMigration()) {
            if (isRootURI(servletRequest)) {
                handleMigrationRedirect(servletRequest, servletResponse);
            }
            LOG.error("Instance in migration. Service may not be ready to return a result");
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } else {
            LOG.error("Instance not active (state={}). Returning SERVICE_UNAVAILABLE for request {}",
                    serviceState.getStateName(), ((HttpServletRequest) servletRequest).getRequestURI());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
    }

    @Override
    public void destroy() {
    }

    boolean isInstanceActive() {
        return serviceState.isActive();
    }

    private boolean isAdminURISupportedInCurrentState(ServletRequest servletRequest) {
        String   requestURI      = ((HttpServletRequest) servletRequest).getRequestURI();
        String[] uriNotSupported = serviceState.isInstanceInMigration()
                ? adminUriNotSupportedInMigration
                : adminUriNotFiltered;

        if (!requestURI.contains("/admin/")) {
            return false;
        }

        for (String s : uriNotSupported) {
            if (requestURI.contains(s)) {
                LOG.trace("URL not supported in current state: {}", requestURI);
                return false;
            }
        }
        return true;
    }

    private boolean isRootURI(ServletRequest servletRequest) {
        return ((HttpServletRequest) servletRequest).getRequestURI().equals("/");
    }

    private void handleMigrationRedirect(ServletRequest servletRequest, ServletResponse servletResponse)
            throws IOException {
        HttpServletResponse httpResponse     = (HttpServletResponse) servletResponse;
        HttpServletRequest  httpRequest      = (HttpServletRequest) servletRequest;
        String              redirectLocation = httpRequest.getRequestURL() + MIGRATION_STATUS_STATIC_PAGE;

        if (isUnsafeHttpMethod(httpRequest)) {
            httpResponse.setHeader(HttpHeaders.LOCATION, redirectLocation);
            httpResponse.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        } else {
            httpResponse.sendRedirect(redirectLocation);
        }
    }

    private boolean isUnsafeHttpMethod(HttpServletRequest httpRequest) {
        String method = httpRequest.getMethod();
        return HttpMethod.POST.equals(method) || HttpMethod.PUT.equals(method) || HttpMethod.DELETE.equals(method);
    }
}
