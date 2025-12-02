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

package org.apache.atlas.web.security;

import org.apache.atlas.AtlasConfiguration;
import org.json.simple.JSONObject;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AtlasAuthenticationSuccessHandlerTest {
    @Mock
    private HttpServletRequest mockHttpServletRequest;

    @Mock
    private HttpServletResponse mockHttpServletResponse;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private HttpSession mockHttpSession;

    @Mock
    private ServletContext mockServletContext;

    @Mock
    private PrintWriter mockPrintWriter;

    private AtlasAuthenticationSuccessHandler authenticationSuccessHandler;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        authenticationSuccessHandler = new AtlasAuthenticationSuccessHandler();

        // Setup common mock behavior
        when(mockHttpServletResponse.getWriter()).thenReturn(mockPrintWriter);
    }

    @Test
    public void testSetup_WithNegativeTimeout() throws Exception {
        int negativeTimeout = -1;

        try (MockedStatic<AtlasConfiguration> mockedConfig = mockStatic(AtlasConfiguration.class)) {
            // Mock AtlasConfiguration.SESSION_TIMEOUT_SECS
            AtlasConfiguration mockSessionTimeoutConfig = mock(AtlasConfiguration.class);
            when(mockSessionTimeoutConfig.getInt()).thenReturn(negativeTimeout);

            // Execute
            authenticationSuccessHandler.setup();

            // Verify using reflection
            Field sessionTimeoutField = AtlasAuthenticationSuccessHandler.class.getDeclaredField("sessionTimeout");
            sessionTimeoutField.setAccessible(true);
            int actualTimeout = (int) sessionTimeoutField.get(authenticationSuccessHandler);

            assertEquals(actualTimeout, negativeTimeout);
        }
    }

    @Test
    public void testOnAuthenticationSuccess_WithSession() throws IOException {
        // Setup
        String sessionId = "test-session-id";
        Object principal = "test-user";

        when(mockAuthentication.getPrincipal()).thenReturn(principal);
        when(mockHttpServletRequest.getSession()).thenReturn(mockHttpSession);
        when(mockHttpSession.getId()).thenReturn(sessionId);
        when(mockHttpServletRequest.getServletContext()).thenReturn(mockServletContext);

        // Set sessionTimeout to positive value
        setPrivateField(authenticationSuccessHandler, "sessionTimeout", 3600);

        // Execute
        authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);

        // Verify session attributes are set
        verify(mockHttpSession).setAttribute(AtlasAuthenticationSuccessHandler.LOCALLOGIN, "true");
        verify(mockServletContext).setAttribute(sessionId, AtlasAuthenticationSuccessHandler.LOCALLOGIN);
        verify(mockHttpSession).setMaxInactiveInterval(3600);

        // Verify response setup
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_OK);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());

        // Verify authentication principal is accessed
        verify(mockAuthentication).getPrincipal();
    }

    @Test
    public void testOnAuthenticationSuccess_WithSessionAndNegativeTimeout() throws IOException {
        // Setup
        String sessionId = "test-session-id";
        Object principal = "test-user";

        when(mockAuthentication.getPrincipal()).thenReturn(principal);
        when(mockHttpServletRequest.getSession()).thenReturn(mockHttpSession);
        when(mockHttpSession.getId()).thenReturn(sessionId);
        when(mockHttpServletRequest.getServletContext()).thenReturn(mockServletContext);

        // Set sessionTimeout to -1 (no timeout)
        setPrivateField(authenticationSuccessHandler, "sessionTimeout", -1);

        // Execute
        authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);

        // Verify session attributes are set
        verify(mockHttpSession).setAttribute(AtlasAuthenticationSuccessHandler.LOCALLOGIN, "true");
        verify(mockServletContext).setAttribute(sessionId, AtlasAuthenticationSuccessHandler.LOCALLOGIN);

        // Verify timeout is NOT set when sessionTimeout is -1
        verify(mockHttpSession, never()).setMaxInactiveInterval(anyInt());

        // Verify response setup
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_OK);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
    }

    @Test
    public void testOnAuthenticationSuccess_WithoutSession() throws IOException {
        // Setup
        Object principal = "test-user";

        when(mockAuthentication.getPrincipal()).thenReturn(principal);
        when(mockHttpServletRequest.getSession()).thenReturn(null);

        // Execute
        authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);

        // Verify no session operations are performed
        verify(mockHttpServletRequest).getSession();
        verify(mockHttpServletRequest, never()).getServletContext();

        // Verify response setup still happens
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_OK);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());

        // Verify authentication principal is accessed
        verify(mockAuthentication).getPrincipal();
    }

    @Test
    public void testOnAuthenticationSuccess_VerifyJSONContent() throws IOException {
        // Setup
        Object principal = "test-user";
        when(mockAuthentication.getPrincipal()).thenReturn(principal);
        when(mockHttpServletRequest.getSession()).thenReturn(null);

        StringWriter stringWriter = new StringWriter();
        PrintWriter realPrintWriter = new PrintWriter(stringWriter);
        when(mockHttpServletResponse.getWriter()).thenReturn(realPrintWriter);

        // Execute
        authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);

        // Verify JSON content
        realPrintWriter.flush();
        String writtenContent = stringWriter.toString();

        // Verify it contains the expected JSON structure
        assertTrue(writtenContent.contains("\"msgDesc\""));
        assertTrue(writtenContent.contains("\"Success\""));
        assertTrue(writtenContent.startsWith("{"));
        assertTrue(writtenContent.endsWith("}"));

        // Create expected JSON and compare
        JSONObject expectedJson = new JSONObject();
        expectedJson.put("msgDesc", "Success");
        assertEquals(writtenContent, expectedJson.toJSONString());
    }

    @Test
    public void testOnAuthenticationSuccess_VerifyResponseStatusValue() throws IOException {
        // Setup
        Object principal = "test-user";
        when(mockAuthentication.getPrincipal()).thenReturn(principal);
        when(mockHttpServletRequest.getSession()).thenReturn(null);

        // Execute
        authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);

        // Verify the exact status code (200)
        verify(mockHttpServletResponse).setStatus(200);
    }

    @Test
    public void testOnAuthenticationSuccess_WithIOException() throws IOException {
        // Setup
        Object principal = "test-user";
        when(mockAuthentication.getPrincipal()).thenReturn(principal);
        when(mockHttpServletRequest.getSession()).thenReturn(null);
        when(mockHttpServletResponse.getWriter()).thenThrow(new IOException("Writer error"));

        // Execute & Verify
        try {
            authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);
            fail("Expected IOException to be thrown");
        } catch (IOException e) {
            assertEquals(e.getMessage(), "Writer error");

            // Verify that response setup methods were still called before the exception
            verify(mockHttpServletResponse).setContentType("application/json");
            verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_OK);
            verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
            verify(mockHttpServletResponse).getWriter();
        }
    }

    @Test
    public void testOnAuthenticationSuccess_WithComplexPrincipal() throws IOException {
        // Setup with complex principal object
        Object complexPrincipal = new Object() {
            @Override
            public String toString() {
                return "Complex Principal Object";
            }
        };

        when(mockAuthentication.getPrincipal()).thenReturn(complexPrincipal);
        when(mockHttpServletRequest.getSession()).thenReturn(mockHttpSession);
        when(mockHttpSession.getId()).thenReturn("session-123");
        when(mockHttpServletRequest.getServletContext()).thenReturn(mockServletContext);
        setPrivateField(authenticationSuccessHandler, "sessionTimeout", 900);

        // Execute
        authenticationSuccessHandler.onAuthenticationSuccess(mockHttpServletRequest, mockHttpServletResponse, mockAuthentication);

        // Verify all operations are performed
        verify(mockAuthentication).getPrincipal();
        verify(mockHttpSession).setAttribute(AtlasAuthenticationSuccessHandler.LOCALLOGIN, "true");
        verify(mockServletContext).setAttribute("session-123", AtlasAuthenticationSuccessHandler.LOCALLOGIN);
        verify(mockHttpSession).setMaxInactiveInterval(900);
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_OK);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockPrintWriter).write(anyString());
    }

    @Test
    public void testImplementsAuthenticationSuccessHandler() {
        // Verify that the class properly implements the interface
        assertTrue(authenticationSuccessHandler instanceof AuthenticationSuccessHandler);
    }

    @Test
    public void testClassAnnotations() {
        // Verify that the class has the @Component annotation
        Component componentAnnotation = AtlasAuthenticationSuccessHandler.class.getAnnotation(Component.class);
        assertNotNull(componentAnnotation);
    }

    @Test
    public void testConstantValues() {
        // Verify the static constant value
        assertEquals(AtlasAuthenticationSuccessHandler.LOCALLOGIN, "locallogin");
    }

    @Test
    public void testDefaultSessionTimeout() throws Exception {
        // Create a new instance to test default value
        AtlasAuthenticationSuccessHandler newHandler = new AtlasAuthenticationSuccessHandler();

        // Verify default sessionTimeout value using reflection
        Field sessionTimeoutField = AtlasAuthenticationSuccessHandler.class.getDeclaredField("sessionTimeout");
        sessionTimeoutField.setAccessible(true);
        int defaultTimeout = (int) sessionTimeoutField.get(newHandler);

        assertEquals(defaultTimeout, 3600);
    }

    // Helper method for reflection
    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set private field", e);
        }
    }
}
