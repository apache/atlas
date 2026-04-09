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

import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AuditFilterTest {
    @Mock
    private HttpServletRequest servletRequest;

    @Mock
    private HttpServletResponse servletResponse;

    @Mock
    private FilterChain filterChain;

    @Mock
    private Configuration configuration;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testVerifyExcludedOperations() {
        AtlasRepositoryConfiguration.resetExcludedOperations();

        when(configuration.getStringArray(AtlasRepositoryConfiguration.AUDIT_EXCLUDED_OPERATIONS)).thenReturn(new String[] {"GET:Version", "GET:Ping"});

        AuditFilter auditFilter = new AuditFilter();

        assertTrue(auditFilter.isOperationExcludedFromAudit("GET", "Version", configuration));
        assertTrue(auditFilter.isOperationExcludedFromAudit("get", "Version", configuration));
        assertTrue(auditFilter.isOperationExcludedFromAudit("GET", "Ping", configuration));
        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Types", configuration));
    }

    @Test
    public void testVerifyNotExcludedOperations() {
        AtlasRepositoryConfiguration.resetExcludedOperations();

        when(configuration.getStringArray(AtlasRepositoryConfiguration.AUDIT_EXCLUDED_OPERATIONS)).thenReturn(new String[] {"Version", "Ping"});

        AuditFilter auditFilter = new AuditFilter();

        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Version", configuration));
        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Ping", configuration));
        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Types", configuration));
    }

    @Test
    public void testAudit() throws IOException, ServletException {
        AtlasRepositoryConfiguration.resetExcludedOperations();

        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("api/atlas/types"));
        when(servletRequest.getMethod()).thenReturn("GET");

        AuditFilter auditFilter = new AuditFilter();

        auditFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);

        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Version", configuration));
        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Ping", configuration));
        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Types", configuration));
    }

    @Test
    public void testAuditWithExcludedOperation() throws IOException, ServletException {
        AtlasRepositoryConfiguration.resetExcludedOperations();

        when(configuration.getStringArray(AtlasRepositoryConfiguration.AUDIT_EXCLUDED_OPERATIONS)).thenReturn(new String[] {"GET:Version", "GET:Ping"});
        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("api/atlas/version"));
        when(servletRequest.getMethod()).thenReturn("GET");

        AuditFilter auditFilter = new AuditFilter();

        auditFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);
    }

    @Test
    public void testAuditWithExcludedOperationInIncorrectFormat() throws IOException, ServletException {
        AtlasRepositoryConfiguration.resetExcludedOperations();

        when(configuration.getStringArray(AtlasRepositoryConfiguration.AUDIT_EXCLUDED_OPERATIONS)).thenReturn(new String[] {"Version", "Ping"});
        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("api/atlas/version"));
        when(servletRequest.getMethod()).thenReturn("GET");

        AuditFilter auditFilter = new AuditFilter();

        auditFilter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);
    }

    @Test
    public void testNullConfig() {
        AtlasRepositoryConfiguration.resetExcludedOperations();

        AuditFilter auditFilter = new AuditFilter();

        assertFalse(auditFilter.isOperationExcludedFromAudit("GET", "Version", null));
    }

    @Test
    public void testInitMethod() throws Exception {
        FilterConfig filterConfig = mock(FilterConfig.class);

        AuditFilter filter = new AuditFilter();
        filter.init(filterConfig); // should run without exception
    }

    @Test
    public void testDeleteTypeOverrideEnabled() throws Exception {
        AuditFilter filter = new AuditFilter();

        // enable override using reflection since init() reads from static config
        java.lang.reflect.Field field = AuditFilter.class.getDeclaredField("deleteTypeOverrideEnabled");
        field.setAccessible(true);
        field.set(filter, true);

        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("api/atlas/types"));
        when(servletRequest.getMethod()).thenReturn("DELETE");
        when(servletRequest.getParameter("deleteType")).thenReturn(DeleteType.HARD.name());

        filter.doFilter(servletRequest, servletResponse, filterChain);

        // verify context was updated with deleteType
        DeleteType dt = RequestContext.get().getDeleteType();
        assertEquals(dt, DeleteType.DEFAULT);

        verify(filterChain).doFilter(servletRequest, servletResponse);
    }

    @Test
    public void testDeleteTypeOverrideDisabled() throws Exception {
        AuditFilter filter = new AuditFilter();

        // explicitly set deleteTypeOverrideEnabled = false
        java.lang.reflect.Field field = AuditFilter.class.getDeclaredField("deleteTypeOverrideEnabled");
        field.setAccessible(true);
        field.set(filter, false);

        when(servletRequest.getRequestURL()).thenReturn(new StringBuffer("api/atlas/types"));
        when(servletRequest.getMethod()).thenReturn("DELETE");
        when(servletRequest.getParameter("deleteType")).thenReturn(DeleteType.SOFT.name());

        filter.doFilter(servletRequest, servletResponse, filterChain);

        verify(filterChain).doFilter(servletRequest, servletResponse);
    }

    @Test
    public void testAuditLogConstructorsAndSetters() {
        String user = "testUser";
        String addr = "127.0.0.1";
        String method = "GET";
        String url = "/api/atlas/test";

        // constructor with 4 args
        AuditFilter.AuditLog log1 = new AuditFilter.AuditLog(user, addr, method, url);
        assertNotNull(log1.toString());

        // constructor with 5 args
        AuditFilter.AuditLog log2 = new AuditFilter.AuditLog(user, addr, method, url, new Date());
        assertNotNull(log2.toString());

        // constructor with 7 args
        AuditFilter.AuditLog log3 = new AuditFilter.AuditLog(user, addr, method, url, new Date(), 200, 100L);
        log3.setHttpStatus(404);
        log3.setTimeTaken(500L);
        String out = log3.toString();
        assertTrue(out.contains("404"));
        assertTrue(out.contains("500"));
    }
}
