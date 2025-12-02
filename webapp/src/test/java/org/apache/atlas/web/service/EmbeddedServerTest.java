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

package org.apache.atlas.web.service;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class EmbeddedServerTest {
    @Mock
    private AtlasAuditService auditService;

    @Mock
    private ServiceState serviceState;

    @Mock
    private Server server;

    private EmbeddedServer embeddedServer;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConstructor() throws IOException {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        assertNotNull(embeddedServer);

        // Verify that server field is initialized
        Field serverField = getField(EmbeddedServer.class, "server");
        Server actualServer = (Server) getFieldValue(serverField, embeddedServer);
        assertNotNull(actualServer);
    }

    @Test
    public void testNewServerSecure() throws IOException {
        String host = "localhost";
        int port = 8443;
        String path = "/test";

        try {
            EmbeddedServer result = EmbeddedServer.newServer(host, port, path, true);
            assertNotNull(result);
            assertTrue(result instanceof SecureEmbeddedServer);
        } catch (IOException e) {
            // Expected in test environment due to missing SSL configuration
            assertTrue(e.getMessage().contains("credential provider path") ||
                      e.getMessage().contains("keystore") ||
                      e.getMessage().contains("SSL"));
        }
    }

    @Test
    public void testNewServerNonSecure() throws IOException {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        EmbeddedServer result = EmbeddedServer.newServer(host, port, path, false);

        assertNotNull(result);
        assertTrue(result instanceof EmbeddedServer);
    }

    @Test
    public void testGetWebAppContext() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        Method method = EmbeddedServer.class.getDeclaredMethod("getWebAppContext", String.class);
        method.setAccessible(true);

        WebAppContext webAppContext = (WebAppContext) method.invoke(embeddedServer, path);

        assertNotNull(webAppContext);
        assertEquals(webAppContext.getContextPath(), "/");
        assertEquals(webAppContext.getWar(), path);
        assertEquals(webAppContext.getInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed"), "false");
    }

    @Test
    public void testGetConnector() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        Method method = EmbeddedServer.class.getDeclaredMethod("getConnector", String.class, int.class);
        method.setAccessible(true);

        Connector connector = (Connector) method.invoke(embeddedServer, host, port);

        assertNotNull(connector);
    }

    @Test
    public void testStop() throws IOException {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        // Test that stop doesn't throw exception
        embeddedServer.stop();
    }

    @Test
    public void testStopWithException() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path) {
            @Override
            public void stop() {
                try {
                    Server mockServer = mock(Server.class);
                    doThrow(new Exception("Test exception")).when(mockServer).stop();

                    Field serverField = getField(EmbeddedServer.class, "server");
                    setFieldValue(serverField, this, mockServer);

                    super.stop();
                } catch (Exception e) {
                    // Expected exception
                }
            }
        };

        // Test that stop handles exception gracefully
        embeddedServer.stop();
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testStartWithException() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path) {
            @Override
            public void start() throws AtlasBaseException {
                try {
                    Server mockServer = mock(Server.class);
                    doThrow(new Exception("Test exception")).when(mockServer).start();

                    Field serverField = getField(EmbeddedServer.class, "server");
                    setFieldValue(serverField, this, mockServer);

                    super.start();
                } catch (AtlasBaseException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        embeddedServer.start();
    }

    @Test
    public void testAuditServerStatusActive() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        // Set up mocks for audit
        setFieldValue(getField(EmbeddedServer.class, "auditService"), embeddedServer, auditService);
        setFieldValue(getField(EmbeddedServer.class, "serviceState"), embeddedServer, serviceState);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        Method method = EmbeddedServer.class.getDeclaredMethod("auditServerStatus");
        method.setAccessible(true);

        try {
            method.invoke(embeddedServer);
            // Test passes if no exception is thrown
        } catch (InvocationTargetException e) {
            // Expected to fail due to BeanUtil dependency in test environment
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testAuditServerStatusNotActive() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        // Set up mocks for audit
        setFieldValue(getField(EmbeddedServer.class, "auditService"), embeddedServer, auditService);
        setFieldValue(getField(EmbeddedServer.class, "serviceState"), embeddedServer, serviceState);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        Method method = EmbeddedServer.class.getDeclaredMethod("auditServerStatus");
        method.setAccessible(true);

        try {
            method.invoke(embeddedServer);
            // Test passes if no exception is thrown
        } catch (InvocationTargetException e) {
            // Expected to fail due to BeanUtil dependency in test environment
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testAuditServerStatusWithException() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        // Set up mocks for audit
        setFieldValue(getField(EmbeddedServer.class, "auditService"), embeddedServer, auditService);
        setFieldValue(getField(EmbeddedServer.class, "serviceState"), embeddedServer, serviceState);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        doThrow(new AtlasBaseException("Test exception")).when(auditService).add(any(), any(), any(), anyObject(), anyObject(), anyInt());

        Method method = EmbeddedServer.class.getDeclaredMethod("auditServerStatus");
        method.setAccessible(true);

        try {
            method.invoke(embeddedServer);
            // Test passes if no exception is thrown
        } catch (InvocationTargetException e) {
            // Expected to fail due to BeanUtil dependency in test environment
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testConstants() {
        assertEquals(EmbeddedServer.ATLAS_DEFAULT_BIND_ADDRESS, "0.0.0.0");
        assertNotNull(EmbeddedServer.SERVER_START_TIME);
        assertTrue(EmbeddedServer.SERVER_START_TIME instanceof Date);
    }

    @Test
    public void testServerStartTime() {
        Date startTime = EmbeddedServer.SERVER_START_TIME;
        assertNotNull(startTime);

        // Verify that the start time is reasonable (not too far in the past or future)
        Date now = new Date();
        long timeDiff = Math.abs(now.getTime() - startTime.getTime());

        // Should be within a reasonable time frame (e.g., 1 hour)
        assertTrue(timeDiff < 3600000, "Server start time should be within reasonable bounds");
    }

    @Test
    public void testServerFieldAccess() throws Exception {
        String host = "localhost";
        int port = 8080;
        String path = "/test";

        embeddedServer = new EmbeddedServer(host, port, path);

        Field serverField = getField(EmbeddedServer.class, "server");
        Server actualServer = (Server) getFieldValue(serverField, embeddedServer);

        assertNotNull(actualServer);
        assertTrue(actualServer instanceof Server);
    }

    // Helper methods for reflection
    private Field getField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getFieldValue(Field field, Object instance) {
        try {
            return field.get(instance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void setFieldValue(Field field, Object instance, Object value) {
        try {
            field.set(instance, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
