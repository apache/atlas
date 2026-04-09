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

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.AuthenticationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AtlasAuthenticationEntryPointTest {
    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private AuthenticationException authException;

    private AtlasAuthenticationEntryPoint entryPoint;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        entryPoint = new AtlasAuthenticationEntryPoint("/login.jsp");
    }

    @Test
    public void testCommence_AjaxRequest() throws IOException {
        // Arrange
        when(request.getHeader("X-Requested-With")).thenReturn("XMLHttpRequest");

        // Act
        entryPoint.commence(request, response, authException);

        // Assert
        verify(response).setHeader("X-Frame-Options", "DENY");
        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    public void testCommence_NormalRequest() throws IOException {
        // Arrange
        when(request.getHeader("X-Requested-With")).thenReturn(null);

        // Act
        entryPoint.commence(request, response, authException);

        // Assert
        verify(response).setHeader("X-Frame-Options", "DENY");
        verify(response).sendRedirect("/login.jsp");
        verify(response, never()).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }
}
