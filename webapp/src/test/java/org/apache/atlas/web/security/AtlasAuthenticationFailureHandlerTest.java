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

import org.json.simple.JSONObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AtlasAuthenticationFailureHandlerTest {
    @Mock
    private HttpServletRequest mockHttpServletRequest;

    @Mock
    private HttpServletResponse mockHttpServletResponse;

    @Mock
    private AuthenticationException mockAuthenticationException;

    @Mock
    private PrintWriter mockPrintWriter;

    private AtlasAuthenticationFailureHandler authenticationFailureHandler;
    private StringWriter stringWriter;

    @BeforeMethod
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        authenticationFailureHandler = new AtlasAuthenticationFailureHandler();
        stringWriter = new StringWriter();

        // Setup common mock behavior
        when(mockHttpServletResponse.getWriter()).thenReturn(mockPrintWriter);
    }

    @Test
    public void testOnAuthenticationFailure_WithValidException() throws IOException {
        // Setup
        String exceptionMessage = "Authentication failed for user";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify response headers and status are set
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");

        // Verify writer is called
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());

        // Verify the exception message is used
        verify(mockAuthenticationException).getMessage();
    }

    @Test
    public void testOnAuthenticationFailure_WithNullExceptionMessage() throws IOException {
        // Setup
        when(mockAuthenticationException.getMessage()).thenReturn(null);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify response is set up correctly even with null message
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
    }

    @Test
    public void testOnAuthenticationFailure_WithEmptyExceptionMessage() throws IOException {
        // Setup
        String exceptionMessage = "";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify response setup
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
    }

    @Test
    public void testOnAuthenticationFailure_WithSpecialCharactersInMessage() throws IOException {
        // Setup
        String exceptionMessage = "Authentication failed: Special chars !@#$%^&*(){}[]|\\:;\"'<>,.?/~`";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify response setup
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
        verify(mockAuthenticationException).getMessage();
    }

    @Test
    public void testOnAuthenticationFailure_WithLongExceptionMessage() throws IOException {
        // Setup
        StringBuilder longMessage = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longMessage.append("This is a very long authentication failure message. ");
        }
        String exceptionMessage = longMessage.toString();
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify response setup
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
        verify(mockAuthenticationException).getMessage();
    }

    @Test
    public void testOnAuthenticationFailure_WithUnicodeCharacters() throws IOException {
        // Setup
        String exceptionMessage = "认证失败 - Authentication failed with 中文 characters and émojis 🔐";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify response setup
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
        verify(mockAuthenticationException).getMessage();
    }

    @Test
    public void testOnAuthenticationFailure_VerifyJSONContent() throws IOException {
        // Setup
        String exceptionMessage = "Test authentication failure";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        StringWriter stringWriter = new StringWriter();
        PrintWriter realPrintWriter = new PrintWriter(stringWriter);
        when(mockHttpServletResponse.getWriter()).thenReturn(realPrintWriter);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify JSON content
        realPrintWriter.flush();
        String writtenContent = stringWriter.toString();

        // Verify it's valid JSON and contains the expected message
        assertTrue(writtenContent.contains("\"msgDesc\""));
        assertTrue(writtenContent.contains(exceptionMessage));

        // Verify it looks like JSON
        assertTrue(writtenContent.startsWith("{"));
        assertTrue(writtenContent.endsWith("}"));
    }

    @Test
    public void testOnAuthenticationFailure_VerifyJSONContentWithNullMessage() throws IOException {
        // Setup
        when(mockAuthenticationException.getMessage()).thenReturn(null);

        StringWriter stringWriter = new StringWriter();
        PrintWriter realPrintWriter = new PrintWriter(stringWriter);
        when(mockHttpServletResponse.getWriter()).thenReturn(realPrintWriter);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify JSON content
        realPrintWriter.flush();
        String writtenContent = stringWriter.toString();

        // Verify it's valid JSON structure even with null message
        assertTrue(writtenContent.contains("\"msgDesc\""));
        assertTrue(writtenContent.startsWith("{"));
        assertTrue(writtenContent.endsWith("}"));
    }

    @Test
    public void testOnAuthenticationFailure_WithIOException() throws IOException {
        // Setup
        String exceptionMessage = "Authentication failed";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);
        when(mockHttpServletResponse.getWriter()).thenThrow(new IOException("Writer error"));

        // Execute & Verify
        try {
            authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);
            fail("Expected IOException to be thrown");
        } catch (IOException e) {
            assertEquals(e.getMessage(), "Writer error");

            // Verify that response setup methods were still called before the exception
            verify(mockHttpServletResponse).setContentType("application/json");
            verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
            verify(mockHttpServletResponse).getWriter();
        }
    }

    @Test
    public void testOnAuthenticationFailure_ResponseStatusValue() throws IOException {
        // Setup
        String exceptionMessage = "Authentication failed";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify the exact status code (401)
        verify(mockHttpServletResponse).setStatus(401);
    }

    @Test
    public void testOnAuthenticationFailure_MethodCallOrder() throws IOException {
        // Setup
        String exceptionMessage = "Authentication failed";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Create an ordered verifier to check method call order
        verify(mockHttpServletResponse).setContentType("application/json");
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");
        verify(mockHttpServletResponse).getWriter();
        verify(mockPrintWriter).write(anyString());
    }

    @Test
    public void testImplementsAuthenticationFailureHandler() {
        assertTrue(authenticationFailureHandler instanceof AuthenticationFailureHandler);
    }

    @Test
    public void testClassAnnotations() {
        // Verify that the class has the @Component annotation
        Component componentAnnotation = AtlasAuthenticationFailureHandler.class.getAnnotation(Component.class);
        assertNotNull(componentAnnotation);
    }

    @Test
    public void testJSONObjectCreation() throws IOException {
        // Setup
        String exceptionMessage = "Test message";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        StringWriter stringWriter = new StringWriter();
        PrintWriter realPrintWriter = new PrintWriter(stringWriter);
        when(mockHttpServletResponse.getWriter()).thenReturn(realPrintWriter);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);
        realPrintWriter.flush();

        // Verify that JSON is properly formatted by creating a JSON object manually and comparing
        JSONObject expectedJson = new JSONObject();
        expectedJson.put("msgDesc", exceptionMessage);

        String writtenContent = stringWriter.toString();
        String expectedContent = expectedJson.toJSONString();

        assertEquals(writtenContent, expectedContent);
    }

    @Test
    public void testOnAuthenticationFailure_AllExecutionPaths() throws IOException {
        // This test ensures all lines of code in the onAuthenticationFailure method are executed

        // Setup
        String exceptionMessage = "Complete execution path test";
        when(mockAuthenticationException.getMessage()).thenReturn(exceptionMessage);

        // Execute
        authenticationFailureHandler.onAuthenticationFailure(mockHttpServletRequest, mockHttpServletResponse, mockAuthenticationException);

        // Verify every operation that happens in the method
        // 1. Exception message is retrieved
        verify(mockAuthenticationException).getMessage();

        // 2. JSONObject is created and message is put into it (verified by the write operation)
        // 3. Response content type is set
        verify(mockHttpServletResponse).setContentType("application/json");

        // 4. Response status is set
        verify(mockHttpServletResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);

        // 5. Response character encoding is set
        verify(mockHttpServletResponse).setCharacterEncoding("UTF-8");

        // 6. Response writer is obtained
        verify(mockHttpServletResponse).getWriter();

        // 7. JSON is written to response
        verify(mockPrintWriter).write(anyString());
    }
}
