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

import org.apache.commons.configuration2.Configuration;
import org.mockito.Mockito;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AtlasJwtAuthWrapperTest {
    @AfterMethod
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void testDoFilter_redirectsToLoginForBrowserWhenNotAuthenticated() throws Exception {
        SecurityContextHolder.clearContext();

        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.DEFAULT_BROWSER_USERAGENT)).thenReturn("Mozilla,Opera,Chrome");
        when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(null);

        AtlasJwtAuthWrapper wrapper = Mockito.spy(new AtlasJwtAuthWrapper(configuration));
        wrapper.initialize();

        AtlasJwtAuthFilter jwtFilter = mock(AtlasJwtAuthFilter.class);
        wrapper.atlasJwtAuthFilter = jwtFilter;

        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        when(req.getHeader("User-Agent")).thenReturn("Mozilla/5.0");
        when(req.getHeader("Authorization")).thenReturn("Bearer sometoken");
        doNothing().when(res).sendRedirect(anyString());

        wrapper.doFilter(req, res, chain);

        verify(jwtFilter, times(1)).doFilter(any(ServletRequest.class), any(ServletResponse.class), any(FilterChain.class));
        verify(res, atLeastOnce()).sendRedirect(anyString());
        verify(chain, times(1)).doFilter(req, res);
    }

    @Test
    public void testDoFilter_skipsJwtWhenAlreadyAuthenticated() throws Exception {
        User principal = new User("user", "", Collections.emptyList());
        UsernamePasswordAuthenticationToken authentication =
                new UsernamePasswordAuthenticationToken(principal, "", principal.getAuthorities());
        SecurityContextHolder.getContext().setAuthentication(authentication);

        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.DEFAULT_BROWSER_USERAGENT)).thenReturn("Mozilla,Opera,Chrome");
        when(configuration.getString(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT)).thenReturn(null);

        AtlasJwtAuthWrapper wrapper = Mockito.spy(new AtlasJwtAuthWrapper(configuration));
        wrapper.initialize();

        AtlasJwtAuthFilter jwtFilter = mock(AtlasJwtAuthFilter.class);
        wrapper.atlasJwtAuthFilter = jwtFilter;

        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        wrapper.doFilter(req, res, chain);

        verify(jwtFilter, never()).doFilter(any(ServletRequest.class), any(ServletResponse.class), any(FilterChain.class));
        verify(chain, times(1)).doFilter(req, res);
    }
}
