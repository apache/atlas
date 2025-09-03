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

import org.apache.atlas.web.service.ActiveInstanceState;
import org.apache.atlas.web.service.ServiceState;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ActiveServerFilterTest {
    public static final String ACTIVE_SERVER_ADDRESS = "http://localhost:21000/";

    @Mock
    private ActiveInstanceState activeInstanceState;

    @Mock
    private HttpServletRequest servletRequest;

    @Mock
    private HttpServletResponse servletResponse;

    @Mock
    private FilterChain filterChain;

    @Mock
    private ServiceState serviceState;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldPassThroughRequestsIfActive() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);
    }

    @Test
    public void testShouldFailIfCannotRetrieveActiveServerAddress() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(null);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testShouldRedirectRequestToActiveServerAddress() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getRequestURI()).thenReturn("types");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "types");
    }

    @Test
    public void adminImportRequestsToPassiveServerShouldToActiveServerAddress() throws IOException, ServletException {
        String[] importExportUrls = {"api/admin/export", "api/admin/import", "api/admin/importfile", "api/admin/audits",
                "api/admin/purge", "api/admin/expimp/audit", "api/admin/metrics",
                "api/admin/server/dummy_name", "api/admin/audit/dummy_guid/details", "api/admin/tasks"};

        for (String partialUrl : importExportUrls) {
            when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
            when(servletRequest.getRequestURI()).thenReturn(partialUrl);

            ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

            when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
            when(servletRequest.getRequestURI()).thenReturn(partialUrl);
            when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

            activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

            verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + partialUrl);
        }
    }

    @Test
    public void testRedirectedRequestShouldContainQueryParameters() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        when(servletRequest.getRequestURI()).thenReturn("types");
        when(servletRequest.getQueryString()).thenReturn("query=TRAIT");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "types?query=TRAIT");
    }

    @Test
    public void testRedirectedRequestShouldContainEncodeQueryParameters() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/v2/search/basic");
        when(servletRequest.getQueryString()).thenReturn("limit=25&excludeDeletedEntities=true&spaceParam=firstpart secondpart&_=1500969656054&listParam=value1,value2");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "api/atlas/v2/search/basic?limit=25&excludeDeletedEntities=true&spaceParam=firstpart%20secondpart&_=1500969656054&listParam=value1,value2");
    }

    @Test
    public void testOriginalRequestShouldNotEncodeQueryParametersAgain() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/v2/search/basic");
        when(servletRequest.getQueryString()).thenReturn("limit=25&excludeDeletedEntities=true&spaceParam=firstpart%20secondpart&_=1500969656054&listParam=value1,value2");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "api/atlas/v2/search/basic?limit=25&excludeDeletedEntities=true&spaceParam=firstpart%20secondpart&_=1500969656054&listParam=value1,value2");
    }

    @Test
    public void testOriginalRequestShouldNotEncodePartiallyEncodedQueryParameters() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/v2/search/basic");
        when(servletRequest.getQueryString()).thenReturn("limit=25&excludeDeletedEntities=true&query=where name%3D%22ABC%22&_=1500969656054&listParam=value1,value2");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "api/atlas/v2/search/basic?limit=25&excludeDeletedEntities=true&query=where%20name=%22ABC%22&_=1500969656054&listParam=value1,value2");
    }

    @Test
    public void testShouldRedirectPOSTRequest() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.POST);
        when(servletRequest.getRequestURI()).thenReturn("types");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).setHeader("Location", ACTIVE_SERVER_ADDRESS + "types");
        verify(servletResponse).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
    }

    @Test
    public void testShouldRedirectPUTRequest() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.PUT);
        when(servletRequest.getRequestURI()).thenReturn("types");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).setHeader("Location", ACTIVE_SERVER_ADDRESS + "types");
        verify(servletResponse).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
    }

    @Test
    public void testShouldRedirectDELETERequest() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.DELETE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/entities/6ebb039f-eaa5-4b9c-ae44-799c7910545d/traits/test_tag_ha3");

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).setHeader("Location", ACTIVE_SERVER_ADDRESS + "api/atlas/entities/6ebb039f-eaa5-4b9c-ae44-799c7910545d/traits/test_tag_ha3");
        verify(servletResponse).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
    }

    @Test
    public void testShouldReturnServiceUnavailableIfStateBecomingActive() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.BECOMING_ACTIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testShouldNotRedirectAdminAPIs() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/admin/asmasn"); // any Admin URI is fine.

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);
        verifyZeroInteractions(activeInstanceState);
    }

    @Test
    public void testShouldHandleMigrationStateWithRootURI() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(serviceState.isInstanceInMigration()).thenReturn(true);
        when(servletRequest.getRequestURI()).thenReturn("/");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:21000/"));

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect("http://localhost:21000/migration-status.html");
    }

    @Test
    public void testShouldHandleMigrationStateWithRootURIForUnsafeMethod() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(serviceState.isInstanceInMigration()).thenReturn(true);
        when(servletRequest.getRequestURI()).thenReturn("/");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.POST);
        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:21000/"));

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).setHeader("Location", "http://localhost:21000/migration-status.html");
        verify(servletResponse).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
    }

    @Test
    public void testShouldHandleMigrationStateWithNonRootURI() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(serviceState.isInstanceInMigration()).thenReturn(true);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testShouldHandleEmptyRequestURI() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS);
    }

    @Test
    public void testShouldHandleNullQueryString() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("types");
        when(servletRequest.getQueryString()).thenReturn(null);
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "types");
    }

    @Test
    public void testShouldHandleEmptyQueryString() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("types");
        when(servletRequest.getQueryString()).thenReturn("");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        when(activeInstanceState.getActiveServerAddress()).thenReturn(ACTIVE_SERVER_ADDRESS);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendRedirect(ACTIVE_SERVER_ADDRESS + "types");
    }

    @Test
    public void testShouldHandleTransitionState() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(serviceState.isInstanceInTransition()).thenReturn(true);
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(servletResponse).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testShouldHandleFilterInitialization() throws ServletException {
        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        // Test init method
        activeServerFilter.init(null);

        // Test destroy method
        activeServerFilter.destroy();

        // No assertions needed as these methods don't have return values or side effects
    }

    @Test
    public void testIsInstanceActiveMethod() throws Exception {
        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        // Test with ACTIVE state
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        Method isInstanceActiveMethod = ActiveServerFilter.class.getDeclaredMethod("isInstanceActive");
        isInstanceActiveMethod.setAccessible(true);
        boolean result = (Boolean) isInstanceActiveMethod.invoke(activeServerFilter);

        assertTrue(result, "Should return true for ACTIVE state");

        // Test with PASSIVE state
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        result = (Boolean) isInstanceActiveMethod.invoke(activeServerFilter);

        assertFalse(result, "Should return false for PASSIVE state");
    }

    @Test
    public void testIsRootURIMethod() throws Exception {
        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);
        Method isRootURIMethod = ActiveServerFilter.class.getDeclaredMethod("isRootURI", ServletRequest.class);
        isRootURIMethod.setAccessible(true);

        // Test with root URI
        when(servletRequest.getRequestURI()).thenReturn("/");
        boolean result = (Boolean) isRootURIMethod.invoke(activeServerFilter, servletRequest);

        assertTrue(result, "Should return true for root URI");

        // Test with non-root URI
        when(servletRequest.getRequestURI()).thenReturn("api/atlas/types");
        result = (Boolean) isRootURIMethod.invoke(activeServerFilter, servletRequest);

        assertFalse(result, "Should return false for non-root URI");
    }

    @Test
    public void testIsUnsafeHttpMethodMethod() throws Exception {
        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);
        Method isUnsafeHttpMethodMethod = ActiveServerFilter.class.getDeclaredMethod("isUnsafeHttpMethod", HttpServletRequest.class);
        isUnsafeHttpMethodMethod.setAccessible(true);

        // Test with POST method
        when(servletRequest.getMethod()).thenReturn(HttpMethod.POST);
        boolean result = (Boolean) isUnsafeHttpMethodMethod.invoke(activeServerFilter, servletRequest);

        assertTrue(result, "Should return true for POST method");

        // Test with PUT method
        when(servletRequest.getMethod()).thenReturn(HttpMethod.PUT);
        result = (Boolean) isUnsafeHttpMethodMethod.invoke(activeServerFilter, servletRequest);

        assertTrue(result, "Should return true for PUT method");

        // Test with DELETE method
        when(servletRequest.getMethod()).thenReturn(HttpMethod.DELETE);
        result = (Boolean) isUnsafeHttpMethodMethod.invoke(activeServerFilter, servletRequest);

        assertTrue(result, "Should return true for DELETE method");

        // Test with GET method
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);
        result = (Boolean) isUnsafeHttpMethodMethod.invoke(activeServerFilter, servletRequest);

        assertFalse(result, "Should return false for GET method");
    }

    @Test
    public void testShouldHandleFilteredURIWhenInstanceIsPassive() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/admin/export");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);
    }

    @Test
    public void testShouldHandleFilteredURIWhenInstanceIsActive() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(servletRequest.getRequestURI()).thenReturn("api/admin/export");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);
    }

    @Test
    public void testShouldHandleFilteredURIWhenInstanceIsInTransition() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(serviceState.isInstanceInTransition()).thenReturn(true);
        when(servletRequest.getRequestURI()).thenReturn("api/admin/export");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);
    }

    @Test
    public void testShouldHandleFilteredURIWhenInstanceIsInMigration() throws IOException, ServletException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(serviceState.isInstanceInMigration()).thenReturn(true);
        when(servletRequest.getRequestURI()).thenReturn("api/admin/export");
        when(servletRequest.getMethod()).thenReturn(HttpMethod.GET);

        ActiveServerFilter activeServerFilter = new ActiveServerFilter(activeInstanceState, serviceState);

        activeServerFilter.doFilter(servletRequest, servletResponse, filterChain);
    }
}
