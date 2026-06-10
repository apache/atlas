/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.atlas.web.filters;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration2.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class AtlasHeaderPreAuthFilterTest {
    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @Mock
    private Configuration configuration;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        SecurityContextHolder.clearContext();
    }

    @AfterMethod
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void testDoFilterEnabledWithUsernameAndRoles() throws Exception {
        when(configuration.getBoolean(AtlasHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, false)).thenReturn(true);
        when(configuration.getString(AtlasHeaderPreAuthFilter.PROP_USERNAME_HEADER, ""))
                .thenReturn("x-user");
        when(configuration.getString(AtlasHeaderPreAuthFilter.PROP_ROLES_HEADER, ""))
                .thenReturn("x-roles");
        when(request.getHeader("x-user")).thenReturn("alice");
        when(request.getHeader("x-roles")).thenReturn("ROLE_ADMIN, ROLE_USER");

        try (MockedStatic<ApplicationProperties> appProps = org.mockito.Mockito.mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(configuration);

            AtlasHeaderPreAuthFilter filter = new AtlasHeaderPreAuthFilter();
            filter.doFilter(request, response, filterChain);
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(auth);
        assertTrue(auth instanceof AtlasAuthenticationToken);

        AtlasAuthenticationToken token = (AtlasAuthenticationToken) auth;
        UserDetails principal          = (UserDetails) token.getPrincipal();

        assertEquals(principal.getUsername(), "alice");
        assertEquals(token.getAuthorities().size(), 2);
        assertEquals(token.getAuthType(), AtlasAuthenticationToken.AUTH_TYPE_TRUSTED_PROXY);
        verify(filterChain).doFilter(eq(request), any(HttpServletResponse.class));
    }

    @Test
    public void testDoFilterEnabledWithoutUsernameDoesNotAuthenticate() throws IOException, ServletException {
        when(configuration.getBoolean(AtlasHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, false)).thenReturn(true);
        when(configuration.getString(AtlasHeaderPreAuthFilter.PROP_USERNAME_HEADER, ""))
                .thenReturn("x-user");
        when(configuration.getString(AtlasHeaderPreAuthFilter.PROP_ROLES_HEADER, ""))
                .thenReturn("x-roles");
        when(request.getHeader("x-user")).thenReturn("   ");

        try (MockedStatic<ApplicationProperties> appProps = org.mockito.Mockito.mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(configuration);

            AtlasHeaderPreAuthFilter filter = new AtlasHeaderPreAuthFilter();
            filter.doFilter(request, response, filterChain);
        }

        assertEquals(SecurityContextHolder.getContext().getAuthentication(), null);
        verify(filterChain).doFilter(eq(request), any(HttpServletResponse.class));
    }

    @Test
    public void testDoFilterEnabledKeepsExistingAuthentication() throws IOException, ServletException {
        Authentication existing = org.mockito.Mockito.mock(Authentication.class);
        when(existing.isAuthenticated()).thenReturn(true);
        SecurityContextHolder.getContext().setAuthentication(existing);

        when(configuration.getBoolean(AtlasHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, false)).thenReturn(true);
        when(configuration.getString(AtlasHeaderPreAuthFilter.PROP_USERNAME_HEADER, ""))
                .thenReturn("x-user");
        when(configuration.getString(AtlasHeaderPreAuthFilter.PROP_ROLES_HEADER, ""))
                .thenReturn("x-roles");
        when(request.getHeader("x-user")).thenReturn("alice");

        try (MockedStatic<ApplicationProperties> appProps = org.mockito.Mockito.mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(configuration);

            AtlasHeaderPreAuthFilter filter = new AtlasHeaderPreAuthFilter();
            filter.doFilter(request, response, filterChain);
        }

        assertSame(SecurityContextHolder.getContext().getAuthentication(), existing);
        verify(filterChain).doFilter(eq(request), any(HttpServletResponse.class));
    }
}
