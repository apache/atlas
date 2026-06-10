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

import org.apache.atlas.authn.handler.AtlasAuth;
import org.mockito.Mockito;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasJwtAuthFilterTest {
    @AfterMethod
    public void cleanup() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void testInit_noopDoesNotThrow() throws Exception {
        AtlasJwtAuthFilter filter = new AtlasJwtAuthFilter();
        filter.init(mock(javax.servlet.FilterConfig.class));
    }

    @Test
    public void testDestroy_noopDoesNotThrow() {
        AtlasJwtAuthFilter filter = new AtlasJwtAuthFilter();
        filter.destroy();
    }

    @Test
    public void testDoFilter_setsAuthenticationWhenAuthenticateSucceeds() throws Exception {
        AtlasJwtAuthFilter filter = Mockito.spy(new AtlasJwtAuthFilter());

        HttpServletRequest req  = mock(HttpServletRequest.class);
        ServletResponse res     = mock(ServletResponse.class);
        FilterChain chain       = mock(FilterChain.class);

        AtlasAuth atlasAuth = new AtlasAuth("alice", AtlasAuth.AuthType.JWT_JWKS);

        doReturn(atlasAuth).when(filter).authenticate(any(HttpServletRequest.class));

        filter.doFilter(req, res, chain);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(auth);
        assertTrue(auth.getPrincipal() instanceof User);
        User user = (User) auth.getPrincipal();
        assertEquals(user.getUsername(), "alice");
        assertNotNull(auth.getAuthorities());
        assertFalse(auth.getAuthorities().isEmpty());
    }

    @Test
    public void testDoFilter_leavesAuthenticationNullWhenAuthenticateReturnsNull() throws Exception {
        AtlasJwtAuthFilter filter = Mockito.spy(new AtlasJwtAuthFilter());

        HttpServletRequest req = mock(HttpServletRequest.class);
        ServletResponse res    = mock(ServletResponse.class);
        FilterChain chain      = mock(FilterChain.class);

        doReturn(null).when(filter).authenticate(any(HttpServletRequest.class));

        filter.doFilter(req, res, chain);

        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }
}
