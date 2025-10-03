/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.filters;

import org.apache.atlas.web.resources.AdminResource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasKnoxSSOAuthenticationFilterTest {
    @Mock
    private FilterChain filterChain;

    @InjectMocks
    private AtlasKnoxSSOAuthenticationFilter filter;

    private MockHttpServletRequest request;
    private MockHttpServletResponse response;
    private MockHttpSession session;

    @InjectMocks
    AdminResource adminResource;

    @BeforeMethod
    public void testSetup() {
        MockitoAnnotations.openMocks(this);
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        session = new MockHttpSession();
        request.setSession(session);

        request.setRequestURI("/api/atlas/admin/checksso");
        request.addHeader("User-Agent", "Chrome");
    }

    @Test
    public void testDoFilter_withoutJwt_setsSsoDisabled() throws IOException, ServletException {
        request.setCookies();  // clear cookies for this test
        filter.doFilter(request, response, filterChain);

        assertNull(filter.getJWTFromCookie(request));

        verify(filterChain).doFilter(request, response);
    }

    @Test
    public void testDoFilter_withJwt_setsSsoEnabledTrue() throws IOException, ServletException {
        request.setCookies(new Cookie("hadoop-jwt", "dummy-jwt-token"));
        filter.doFilter(request, response, filterChain);

        assertNotNull(filter.getJWTFromCookie(request));
        assertNotNull(request.getCookies());
        assertTrue(Arrays.stream(request.getCookies())
                .anyMatch(cookie -> cookie.getValue().equals("dummy-jwt-token")));

        verify(filterChain).doFilter(request, response);
    }

    @Test
    public void test_CheckSSO_API_true() {
        request.setAttribute("ssoEnabled", true);
        String result = adminResource.checkSSO(request);
        assertEquals(result, "true");
    }

    @Test
    public void test_CheckSSO_API_false() {
        request.setAttribute("ssoEnabled", false);
        String result = adminResource.checkSSO(request);
        assertEquals(result, "false");
    }
}
