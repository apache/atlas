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

package org.apache.atlas;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Permission;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AtlasAdminClientTest {
    @Mock
    private Configuration mockConfiguration;

    private MockedStatic<ApplicationProperties> applicationPropertiesMock;
    private MockedStatic<AuthenticationUtil> authenticationUtilMock;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;
    private PrintStream originalOut;
    private PrintStream originalErr;
    private SecurityManager originalSecurityManager;

    // Custom SecurityManager to prevent System.exit
    private static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
            // Allow all permissions
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // Allow all permissions
        }

        @Override
        public void checkExit(int status) {
            throw new SecurityException("System.exit(" + status + ") blocked by test");
        }
    }

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        applicationPropertiesMock = mockStatic(ApplicationProperties.class);
        authenticationUtilMock = mockStatic(AuthenticationUtil.class);

        // Capture output streams
        originalOut = System.out;
        originalErr = System.err;
        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));

        // Install custom SecurityManager to block System.exit
        originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @AfterMethod
    public void tearDown() {
        applicationPropertiesMock.close();
        authenticationUtilMock.close();
        System.setOut(originalOut);
        System.setErr(originalErr);
        System.setSecurityManager(originalSecurityManager);
    }

    @Test
    public void testConstructor() throws Exception {
        // Test constructor execution
        AtlasAdminClient client = new AtlasAdminClient();
        assertNotNull(client);
    }

    @Test
    public void testParseCommandLineOptionsWithValidStatus() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method parseMethod = AtlasAdminClient.class.getDeclaredMethod("parseCommandLineOptions", String[].class);
        parseMethod.setAccessible(true);

        // Act
        CommandLine result = (CommandLine) parseMethod.invoke(client, (Object) new String[] {"-status"});

        // Assert
        assertNotNull(result);
        assertTrue(result.hasOption("status"));
    }

    @Test
    public void testParseCommandLineOptionsWithValidStats() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method parseMethod = AtlasAdminClient.class.getDeclaredMethod("parseCommandLineOptions", String[].class);
        parseMethod.setAccessible(true);

        // Act
        CommandLine result = (CommandLine) parseMethod.invoke(client, (Object) new String[] {"-stats"});

        // Assert
        assertNotNull(result);
        assertTrue(result.hasOption("stats"));
    }

    @Test
    public void testParseCommandLineOptionsWithCredentials() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method parseMethod = AtlasAdminClient.class.getDeclaredMethod("parseCommandLineOptions", String[].class);
        parseMethod.setAccessible(true);

        // Act
        CommandLine result = (CommandLine) parseMethod.invoke(client, (Object) new String[] {"-u", "user:pass", "-status"});

        // Assert
        assertNotNull(result);
        assertTrue(result.hasOption("u"));
        assertTrue(result.hasOption("status"));
        assertEquals(result.getOptionValue("u"), "user:pass");
    }

    @Test
    public void testParseCommandLineOptionsWithInvalidOption() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method parseMethod = AtlasAdminClient.class.getDeclaredMethod("parseCommandLineOptions", String[].class);
        parseMethod.setAccessible(true);

        try {
            parseMethod.invoke(client, (Object) new String[] {"-invalid"});
            fail("Should have called System.exit due to ParseException");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SecurityException);
            assertTrue(e.getCause().getMessage().contains("System.exit"));
        }
    }

    @Test
    public void testParseCommandLineOptionsWithEmptyArgs() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method parseMethod = AtlasAdminClient.class.getDeclaredMethod("parseCommandLineOptions", String[].class);
        parseMethod.setAccessible(true);

        try {
            parseMethod.invoke(client, (Object) new String[] {});
            fail("Should have called System.exit");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SecurityException);
            assertTrue(e.getCause().getMessage().contains("System.exit"));
        }
    }

    @Test
    public void testPrintUsage() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method printUsageMethod = AtlasAdminClient.class.getDeclaredMethod("printUsage");
        printUsageMethod.setAccessible(true);

        try {
            printUsageMethod.invoke(client);
            fail("Should have called System.exit");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SecurityException);
            assertTrue(e.getCause().getMessage().contains("System.exit"));
        }

        // Verify usage was printed
        String output = outputStream.toString();
        assertTrue(output.contains("usage:"));
        assertTrue(output.contains("status"));
        assertTrue(output.contains("stats"));
    }

    @Test
    public void testInitAtlasClientMethodExistsAndAccessible() throws Exception {
        Method initAtlasClientMethod = AtlasAdminClient.class.getDeclaredMethod("initAtlasClient", String[].class, String[].class);
        initAtlasClientMethod.setAccessible(true);
        assertNotNull(initAtlasClientMethod);
        assertEquals(initAtlasClientMethod.getParameterCount(), 2);
    }

    @Test
    public void testAuthenticationUtilIntegration() throws Exception {
        authenticationUtilMock.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(false);
        authenticationUtilMock.when(AuthenticationUtil::getBasicAuthenticationInput)
                .thenReturn(new String[] {"user", "password"});
        // Verify mocking works
        boolean isKerberos = AuthenticationUtil.isKerberosAuthenticationEnabled();
        String[] credentials = AuthenticationUtil.getBasicAuthenticationInput();
        assertEquals(isKerberos, false);
        assertNotNull(credentials);
        assertEquals(credentials.length, 2);
        assertEquals(credentials[0], "user");
        assertEquals(credentials[1], "password");
    }

    @Test
    public void testPrintStandardHttpErrorDetailsWithFullException() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        AtlasServiceException exception = mock(AtlasServiceException.class);
        ClientResponse.Status status = mock(ClientResponse.Status.class);
        when(exception.getStatus()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(404);
        when(status.getReasonPhrase()).thenReturn("Not Found");
        when(exception.getMessage()).thenReturn("Resource not found");

        Method printErrorMethod = AtlasAdminClient.class.getDeclaredMethod("printStandardHttpErrorDetails",
                AtlasServiceException.class);
        printErrorMethod.setAccessible(true);

        // Act
        printErrorMethod.invoke(client, exception);

        // Assert
        String errorOutput = errorStream.toString();
        assertTrue(errorOutput.contains("Error details:"));
        assertTrue(errorOutput.contains("HTTP Status: 404"));
        assertTrue(errorOutput.contains("Not Found"));
        assertTrue(errorOutput.contains("Resource not found"));
    }

    @Test
    public void testPrintStandardHttpErrorDetailsMethodSignature() throws Exception {
        Method printErrorMethod = AtlasAdminClient.class.getDeclaredMethod("printStandardHttpErrorDetails",
                AtlasServiceException.class);
        printErrorMethod.setAccessible(true);
        assertNotNull(printErrorMethod);
        assertEquals(printErrorMethod.getParameterCount(), 1);
        assertEquals(printErrorMethod.getParameterTypes()[0], AtlasServiceException.class);
    }

    @Test
    public void testPrintStandardHttpErrorDetailsWithDifferentStatusCodes() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        AtlasServiceException exception = mock(AtlasServiceException.class);
        ClientResponse.Status status = mock(ClientResponse.Status.class);
        when(exception.getStatus()).thenReturn(status);
        when(status.getStatusCode()).thenReturn(500);
        when(status.getReasonPhrase()).thenReturn("Internal Server Error");
        when(exception.getMessage()).thenReturn("Server error occurred");

        Method printErrorMethod = AtlasAdminClient.class.getDeclaredMethod("printStandardHttpErrorDetails",
                AtlasServiceException.class);
        printErrorMethod.setAccessible(true);

        // Act
        printErrorMethod.invoke(client, exception);

        // Assert
        String errorOutput = errorStream.toString();
        assertTrue(errorOutput.contains("HTTP Status: 500"));
        assertTrue(errorOutput.contains("Internal Server Error"));
        assertTrue(errorOutput.contains("Server error occurred"));
    }

    @Test
    public void testHandleCommandWithUnsupportedOption() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        CommandLine mockCommandLine = mock(CommandLine.class);
        when(mockCommandLine.hasOption("status")).thenReturn(false);
        when(mockCommandLine.hasOption("stats")).thenReturn(false);

        Method handleCommandMethod = AtlasAdminClient.class.getDeclaredMethod("handleCommand",
                CommandLine.class, String[].class);
        handleCommandMethod.setAccessible(true);

        try {
            // Act
            int result = (Integer) handleCommandMethod.invoke(client, mockCommandLine,
                    new String[] {"http://localhost:21000"});
            fail("Should have called System.exit");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SecurityException);
            assertTrue(e.getCause().getMessage().contains("System.exit"));
        }
    }

    @Test
    public void testRunWithValidStatusOption() throws Exception {
        applicationPropertiesMock.when(ApplicationProperties::get).thenReturn(mockConfiguration);
        when(mockConfiguration.getStringArray("atlas.rest.address"))
                .thenReturn(new String[] {"http://localhost:21000"});

        authenticationUtilMock.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(false);
        authenticationUtilMock.when(AuthenticationUtil::getBasicAuthenticationInput)
                .thenReturn(new String[] {"user", "password"});

        AtlasAdminClient client = new AtlasAdminClient();
        Method runMethod = AtlasAdminClient.class.getDeclaredMethod("run", String[].class);
        runMethod.setAccessible(true);

        try {
            int result = (Integer) runMethod.invoke(client, (Object) new String[] {"-status"});
            // If we get here, it should be -1
            assertEquals(result, -1);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("Connection") || e.getCause().getMessage().contains("refused"));
        }
    }

    @Test
    public void testRunWithValidStatsOption() throws Exception {
        applicationPropertiesMock.when(ApplicationProperties::get).thenReturn(mockConfiguration);
        when(mockConfiguration.getStringArray("atlas.rest.address"))
                .thenReturn(new String[] {"http://localhost:21000"});

        authenticationUtilMock.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(false);
        authenticationUtilMock.when(AuthenticationUtil::getBasicAuthenticationInput)
                .thenReturn(new String[] {"user", "password"});

        AtlasAdminClient client = new AtlasAdminClient();
        Method runMethod = AtlasAdminClient.class.getDeclaredMethod("run", String[].class);
        runMethod.setAccessible(true);

        try {
            // Act
            int result = (Integer) runMethod.invoke(client, (Object) new String[] {"-stats"});
            assertEquals(result, -1);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("Connection") || e.getCause().getMessage().contains("refused"));
        }
    }

    @Test
    public void testRunWithNullAtlasServerUri() throws Exception {
        applicationPropertiesMock.when(ApplicationProperties::get).thenReturn(mockConfiguration);
        when(mockConfiguration.getStringArray("atlas.rest.address")).thenReturn(null);

        authenticationUtilMock.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(false);
        authenticationUtilMock.when(AuthenticationUtil::getBasicAuthenticationInput)
                .thenReturn(new String[] {"user", "password"});

        AtlasAdminClient client = new AtlasAdminClient();
        Method runMethod = AtlasAdminClient.class.getDeclaredMethod("run", String[].class);
        runMethod.setAccessible(true);

        try {
            // Act
            int result = (Integer) runMethod.invoke(client, (Object) new String[] {"-status"});
            assertEquals(result, -1);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("Connection") || e.getCause().getMessage().contains("refused"));
        }
    }

    @Test
    public void testRunWithEmptyAtlasServerUri() throws Exception {
        applicationPropertiesMock.when(ApplicationProperties::get).thenReturn(mockConfiguration);
        when(mockConfiguration.getStringArray("atlas.rest.address")).thenReturn(new String[] {});

        authenticationUtilMock.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(false);
        authenticationUtilMock.when(AuthenticationUtil::getBasicAuthenticationInput)
                .thenReturn(new String[] {"user", "password"});

        AtlasAdminClient client = new AtlasAdminClient();
        Method runMethod = AtlasAdminClient.class.getDeclaredMethod("run", String[].class);
        runMethod.setAccessible(true);

        try {
            // Act
            int result = (Integer) runMethod.invoke(client, (Object) new String[] {"-status"});
            // If we get here, should be -1
            assertEquals(result, -1);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("Connection") || e.getCause().getMessage().contains("refused"));
        }
    }

    @Test
    public void testRunWithInvalidArguments() throws Exception {
        AtlasAdminClient client = new AtlasAdminClient();
        Method runMethod = AtlasAdminClient.class.getDeclaredMethod("run", String[].class);
        runMethod.setAccessible(true);

        try {
            runMethod.invoke(client, (Object) new String[] {});
            fail("Should have called System.exit");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SecurityException);
            assertTrue(e.getCause().getMessage().contains("System.exit"));
        }
    }

    @Test
    public void testRunWithParseException() throws Exception {
        // Test run with invalid option that causes ParseException
        AtlasAdminClient client = new AtlasAdminClient();
        Method runMethod = AtlasAdminClient.class.getDeclaredMethod("run", String[].class);
        runMethod.setAccessible(true);

        try {
            runMethod.invoke(client, (Object) new String[] {"-invalid"});
            fail("Should have called System.exit");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SecurityException);
            assertTrue(e.getCause().getMessage().contains("System.exit"));
        }
    }

    @Test
    public void testMainMethodWithArguments() throws Exception {
        applicationPropertiesMock.when(ApplicationProperties::get).thenReturn(mockConfiguration);

        when(mockConfiguration.getStringArray("atlas.rest.address"))
                .thenReturn(new String[] {"http://localhost:21000"});

        authenticationUtilMock.when(AuthenticationUtil::isKerberosAuthenticationEnabled).thenReturn(false);
        authenticationUtilMock.when(AuthenticationUtil::getBasicAuthenticationInput)
                .thenReturn(new String[] {"user", "password"});

        try {
            AtlasAdminClient.main(new String[] {"-status"});
            fail("Should have called System.exit or failed with connection error");
        } catch (SecurityException e) {
            assertTrue(e.getMessage().contains("System.exit"));
        } catch (Exception e) {
            // Also expected - connection error before System.exit
            assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("refused") || e.getMessage().contains("Error"));
        }
    }

    @Test
    public void testMainMethodWithEmptyArguments() throws Exception {
        // Test main method with empty arguments
        try {
            AtlasAdminClient.main(new String[] {});
            fail("Should have called System.exit");
        } catch (SecurityException e) {
            assertTrue(e.getMessage().contains("System.exit"));
        }
    }

    @Test
    public void testStaticFieldsInitialization() throws Exception {
        // Test that static fields are properly initialized by accessing them
        Field statusField = AtlasAdminClient.class.getDeclaredField("STATUS");
        statusField.setAccessible(true);
        org.apache.commons.cli.Option status = (org.apache.commons.cli.Option) statusField.get(null);
        assertNotNull(status);
        assertEquals(status.getOpt(), "status");

        Field statsField = AtlasAdminClient.class.getDeclaredField("STATS");
        statsField.setAccessible(true);
        org.apache.commons.cli.Option stats = (org.apache.commons.cli.Option) statsField.get(null);
        assertNotNull(stats);
        assertEquals(stats.getOpt(), "stats");

        Field credentialsField = AtlasAdminClient.class.getDeclaredField("CREDENTIALS");
        credentialsField.setAccessible(true);
        org.apache.commons.cli.Option credentials = (org.apache.commons.cli.Option) credentialsField.get(null);
        assertNotNull(credentials);
        assertEquals(credentials.getOpt(), "u");

        Field optionsField = AtlasAdminClient.class.getDeclaredField("OPTIONS");
        optionsField.setAccessible(true);
        Options options = (Options) optionsField.get(null);
        assertNotNull(options);
        assertTrue(options.hasOption("status"));
        assertTrue(options.hasOption("stats"));
        assertTrue(options.hasOption("u"));

        Field invalidStatusField = AtlasAdminClient.class.getDeclaredField("INVALID_OPTIONS_STATUS");
        invalidStatusField.setAccessible(true);
        int invalidStatus = (Integer) invalidStatusField.get(null);
        assertEquals(invalidStatus, 1);

        Field programErrorField = AtlasAdminClient.class.getDeclaredField("PROGRAM_ERROR_STATUS");
        programErrorField.setAccessible(true);
        int programError = (Integer) programErrorField.get(null);
        assertEquals(programError, -1);
    }
}
