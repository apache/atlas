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

package org.apache.atlas.web.filters;

import org.apache.atlas.server.common.filters.ActiveServerFilter;
import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ActiveServerFilter} in active-active peer mode.
 *
 * <p>There is no redirect-to-active: the filter either passes the request
 * downstream (ACTIVE) or returns 503 (BECOMING_ACTIVE, MIGRATING).
 */
public class ActiveServerFilterTest {
    private static final String ADMIN_URI        = "/api/atlas/admin/types";
    private static final String SUPPORTED_URI    = "/api/atlas/v2/types/typedefs";
    private static final String EXPORT_ADMIN_URI = "/api/atlas/admin/export";

    @Mock private ServiceStateProvider serviceState;
    @Mock private HttpServletRequest  request;
    @Mock private HttpServletResponse response;
    @Mock private FilterChain         chain;

    private ActiveServerFilter filter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        filter = new ActiveServerFilter(serviceState);

        when(request.getMethod()).thenReturn("GET");
        when(serviceState.isInstanceInMigration()).thenReturn(false);
        when(serviceState.isInstanceInTransition()).thenReturn(false);
        when(serviceState.getStateName()).thenReturn("ACTIVE");
    }

    // -------------------------------------------------------------------------
    // ACTIVE node — all requests pass through
    // -------------------------------------------------------------------------

    @Test
    public void requestPassesThroughWhenNodeIsActive() throws Exception {
        when(serviceState.isActive()).thenReturn(true);
        when(request.getRequestURI()).thenReturn(SUPPORTED_URI);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(response, never()).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void adminRequestPassesThroughWhenNodeIsActive() throws Exception {
        when(serviceState.isActive()).thenReturn(true);
        when(request.getRequestURI()).thenReturn(ADMIN_URI);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    // -------------------------------------------------------------------------
    // BECOMING_ACTIVE (in transition) — 503 returned
    // -------------------------------------------------------------------------

    @Test
    public void returns503WhenNodeIsBecomingActive() throws Exception {
        when(serviceState.isActive()).thenReturn(false);
        when(serviceState.isInstanceInTransition()).thenReturn(true);
        when(request.getRequestURI()).thenReturn(SUPPORTED_URI);

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(chain, never()).doFilter(request, response);
    }

    // -------------------------------------------------------------------------
    // Admin URIs that are always supported (pass through regardless of state)
    // -------------------------------------------------------------------------

    @Test
    public void adminMetricsReturns503WhenNotActive() throws Exception {
        when(serviceState.isActive()).thenReturn(false);
        when(serviceState.isInstanceInTransition()).thenReturn(true);
        // /admin/metrics is in the always-supported list
        when(request.getRequestURI()).thenReturn("/api/atlas/admin/metrics");

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(chain, never()).doFilter(request, response);
    }

    @Test
    public void adminServerStatusReturns503WhenNotActive() throws Exception {
        when(serviceState.isActive()).thenReturn(false);
        when(serviceState.isInstanceInTransition()).thenReturn(true);
        when(request.getRequestURI()).thenReturn("/api/atlas/admin/server");

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(chain, never()).doFilter(request, response);
    }

    // -------------------------------------------------------------------------
    // Admin import/export URIs — NOT supported when not active
    // -------------------------------------------------------------------------

    @Test
    public void adminExportReturns503WhenNotActive() throws Exception {
        when(serviceState.isActive()).thenReturn(false);
        when(serviceState.isInstanceInTransition()).thenReturn(true);
        when(request.getRequestURI()).thenReturn(EXPORT_ADMIN_URI);

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(chain, never()).doFilter(request, response);
    }

    // -------------------------------------------------------------------------
    // Non-admin URIs when not active — 503
    // -------------------------------------------------------------------------

    @Test
    public void nonAdminUriReturns503WhenNeitherActiveNorTransition() throws Exception {
        when(serviceState.isActive()).thenReturn(false);
        when(serviceState.isInstanceInTransition()).thenReturn(false);
        when(serviceState.isInstanceInMigration()).thenReturn(false);
        when(request.getRequestURI()).thenReturn(SUPPORTED_URI);

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(chain, never()).doFilter(request, response);
    }

    // -------------------------------------------------------------------------
    // Filter lifecycle
    // -------------------------------------------------------------------------

    @Test
    public void initDoesNotThrow() throws Exception {
        filter.init(null);
    }

    @Test
    public void destroyDoesNotThrow() {
        filter.destroy();
    }
}
