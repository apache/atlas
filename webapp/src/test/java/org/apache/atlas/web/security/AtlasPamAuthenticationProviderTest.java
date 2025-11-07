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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.web.model.User;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.jaas.DefaultJaasAuthenticationProvider;
import org.springframework.security.authentication.jaas.memory.InMemoryConfiguration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.PostConstruct;
import javax.security.auth.login.AppConfigurationEntry;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AtlasPamAuthenticationProviderTest {
    @Mock
    private Configuration mockConfiguration;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private Authentication mockAuthenticatedResult;

    @Mock
    private DefaultJaasAuthenticationProvider mockJaasAuthenticationProvider;

    private AtlasPamAuthenticationProvider pamAuthenticationProvider;
    private Properties mockProperties;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        pamAuthenticationProvider = new AtlasPamAuthenticationProvider();
        mockProperties = new Properties();
    }

    @Test
    public void testSetup_WithValidConfiguration() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            // Setup
            setupMockConfiguration();
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(any())).thenReturn(mockProperties);

            try (MockedConstruction<AppConfigurationEntry> mockedAppConfigEntry =
                            mockConstruction(AppConfigurationEntry.class);
                    MockedConstruction<InMemoryConfiguration> mockedInMemoryConfig =
                            mockConstruction(InMemoryConfiguration.class);
                    MockedConstruction<UserAuthorityGranter> mockedAuthorityGranter =
                            mockConstruction(UserAuthorityGranter.class)) {
                // Execute
                pamAuthenticationProvider.setup();

                // Verify using reflection
                assertTrue((Boolean) getPrivateField(pamAuthenticationProvider, "groupsFromUGI"));
                Map<String, String> options = (Map<String, String>) getPrivateField(pamAuthenticationProvider, "options");
                assertFalse(options.isEmpty());

                // Verify construction calls
                assertEquals(mockedAppConfigEntry.constructed().size(), 1);
                assertEquals(mockedInMemoryConfig.constructed().size(), 1);
                assertEquals(mockedAuthorityGranter.constructed().size(), 1);
            }
        }
    }

    @Test
    public void testSetup_WithException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get())
                    .thenThrow(new RuntimeException("Configuration error"));

            // Execute - should not throw exception
            pamAuthenticationProvider.setup();

            // Verify default values remain
            assertFalse((Boolean) getPrivateField(pamAuthenticationProvider, "groupsFromUGI"));
        }
    }

    @Test
        public void testAuthenticate_Success() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");
        when(mockAuthenticatedResult.isAuthenticated()).thenReturn(true);

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Mock the jaasAuthenticationProvider to return authenticated result
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(mockAuthenticatedResult);

            // Execute
            Authentication result = pamAuthenticationProvider.authenticate(mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
            verify(mockJaasAuthenticationProvider).authenticate(any());
        }
    }

    @Test
        public void testAuthenticate_Failure() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");
        when(mockAuthenticatedResult.isAuthenticated()).thenReturn(false);

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Mock the jaasAuthenticationProvider to return unauthenticated result
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(mockAuthenticatedResult);

            // Execute & Verify
            try {
                pamAuthenticationProvider.authenticate(mockAuthentication);
                fail("Expected AtlasAuthenticationException");
            } catch (AtlasAuthenticationException e) {
                assertEquals(e.getMessage(), "PAM Authentication Failed");
            }
        }
    }

    @Test
        public void testAuthenticate_WithNullResult() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Mock the jaasAuthenticationProvider to return null
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(null);

            // Execute & Verify
            try {
                pamAuthenticationProvider.authenticate(mockAuthentication);
                fail("Expected AtlasAuthenticationException");
            } catch (AtlasAuthenticationException e) {
                assertEquals(e.getMessage(), "PAM Authentication Failed");
            }
        }
    }

    @Test
        public void testGetPamAuthentication_Success() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(mockAuthenticatedResult);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
            verify(mockJaasAuthenticationProvider).authenticate(any());
        }
    }

    @Test
    public void testGetPamAuthentication_WithGroupsFromUGI() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        setPrivateField(pamAuthenticationProvider, "groupsFromUGI", true);
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(mockAuthenticatedResult);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
            verify(mockJaasAuthenticationProvider).authenticate(any());
        }
    }

    @Test
    public void testGetPamAuthentication_WithoutGroupsFromUGI() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        setPrivateField(pamAuthenticationProvider, "groupsFromUGI", false);
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(mockAuthenticatedResult);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
            verify(mockJaasAuthenticationProvider).authenticate(any());
        }
    }

    @Test
    public void testGetPamAuthentication_WithNullUsername() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn(null);
        when(mockAuthentication.getCredentials()).thenReturn("password");

        // Execute using reflection
        Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to null username
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetPamAuthentication_WithEmptyUsername() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        // Execute using reflection
        Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to empty username
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetPamAuthentication_WithNullPassword() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn(null);

        // Execute using reflection
        Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to null password
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetPamAuthentication_WithEmptyPassword() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("");

        // Execute using reflection
        Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to empty password
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetPamAuthentication_WithWhitespaceCredentials() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("   ");
        when(mockAuthentication.getCredentials()).thenReturn("   ");

        // Execute using reflection
        Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to whitespace-only credentials
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetPamAuthentication_WithException() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class, (mock, context) -> {
                            throw new RuntimeException("User creation failed");
                        })) {
            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

            // Verify - should return original authentication due to exception
            assertEquals(result, mockAuthentication);
        }
    }

    @Test
    public void testSetPamProperties_Success() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                    MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            // Setup
            setupMockConfiguration();
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(any())).thenReturn(mockProperties);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("setPamProperties");
            method.setAccessible(true);
            method.invoke(pamAuthenticationProvider);

            // Verify
            assertTrue((Boolean) getPrivateField(pamAuthenticationProvider, "groupsFromUGI"));
            Map<String, String> options = (Map<String, String>) getPrivateField(pamAuthenticationProvider, "options");
            assertEquals(options.get("option1"), "value1");
            assertEquals(options.get("option2"), "value2");
            assertEquals(options.get("service"), "atlas-login"); // default service
        }
    }

    @Test
    public void testSetPamProperties_WithCustomService() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                    MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            // Setup with custom service
            setupMockConfiguration();
            mockProperties.setProperty("service", "custom-service");
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(any())).thenReturn(mockProperties);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("setPamProperties");
            method.setAccessible(true);
            method.invoke(pamAuthenticationProvider);

            // Verify
            Map<String, String> options = (Map<String, String>) getPrivateField(pamAuthenticationProvider, "options");
            assertEquals(options.get("service"), "custom-service"); // custom service, not default
        }
    }

    @Test
    public void testSetPamProperties_WithException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get())
                    .thenThrow(new RuntimeException("Configuration error"));

            // Execute using reflection - should not throw exception
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("setPamProperties");
            method.setAccessible(true);
            method.invoke(pamAuthenticationProvider);

            // Verify - fields should remain at default values
            assertFalse((Boolean) getPrivateField(pamAuthenticationProvider, "groupsFromUGI"));
            Map<String, String> options = (Map<String, String>) getPrivateField(pamAuthenticationProvider, "options");
            assertTrue(options.isEmpty());
        }
    }

    @Test
    public void testInit_Success() throws Exception {
        // Setup
        setupPamAuthenticationProvider();

        try (MockedConstruction<AppConfigurationEntry> mockedAppConfigEntry =
                        mockConstruction(AppConfigurationEntry.class);
                    MockedConstruction<InMemoryConfiguration> mockedInMemoryConfig =
                            mockConstruction(InMemoryConfiguration.class);
                    MockedConstruction<UserAuthorityGranter> mockedAuthorityGranter =
                            mockConstruction(UserAuthorityGranter.class)) {
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("init");
            method.setAccessible(true);
            method.invoke(pamAuthenticationProvider);

            // Verify
            assertEquals(mockedAppConfigEntry.constructed().size(), 1);
            assertEquals(mockedInMemoryConfig.constructed().size(), 1);
            assertEquals(mockedAuthorityGranter.constructed().size(), 1);

            verify(mockJaasAuthenticationProvider).setConfiguration(any());
            verify(mockJaasAuthenticationProvider).setAuthorityGranters(any());
            verify(mockJaasAuthenticationProvider).afterPropertiesSet();
        }
    }

    @Test
    public void testClassAnnotations() {
        // Verify @Component annotation
        Component componentAnnotation = AtlasPamAuthenticationProvider.class.getAnnotation(Component.class);
        assertNotNull(componentAnnotation);
    }

    @Test
    public void testSetupAnnotation() throws Exception {
        // Verify @PostConstruct annotation on setup method
        Method setupMethod = AtlasPamAuthenticationProvider.class.getDeclaredMethod("setup");
        PostConstruct postConstructAnnotation = setupMethod.getAnnotation(PostConstruct.class);
        assertNotNull(postConstructAnnotation);
    }

    @Test
    public void testExtendsAtlasAbstractAuthenticationProvider() {
        // Verify inheritance
        assertTrue(pamAuthenticationProvider instanceof AtlasAbstractAuthenticationProvider);

        // Test inherited supports() method
        assertTrue(pamAuthenticationProvider.supports(UsernamePasswordAuthenticationToken.class));
    }

    @Test
    public void testStaticFields() throws Exception {
        // Verify static final fields
        Field loginModuleNameField = AtlasPamAuthenticationProvider.class.getDeclaredField("loginModuleName");
        loginModuleNameField.setAccessible(true);
        assertEquals(loginModuleNameField.get(null), "org.apache.atlas.web.security.PamLoginModule");

        Field controlFlagField = AtlasPamAuthenticationProvider.class.getDeclaredField("controlFlag");
        controlFlagField.setAccessible(true);
        assertEquals(controlFlagField.get(null), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED);
    }

    @Test
    public void testDefaultFieldValues() throws Exception {
        // Create new instance to test default values
        AtlasPamAuthenticationProvider newProvider = new AtlasPamAuthenticationProvider();

        // Verify default values
        assertFalse((Boolean) getPrivateField(newProvider, "groupsFromUGI"));
        Map<String, String> options = (Map<String, String>) getPrivateField(newProvider, "options");
        assertNotNull(options);
        assertTrue(options.isEmpty());

        DefaultJaasAuthenticationProvider jaasProvider =
                (DefaultJaasAuthenticationProvider) getPrivateField(newProvider, "jaasAuthenticationProvider");
        assertNotNull(jaasProvider);
    }

    @Test
    public void testGetPamAuthentication_VerifyJaasProviderInteraction() throws Exception {
        // Setup
        setupPamAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        List<GrantedAuthority> authorities = new ArrayList<>();
        authorities.add(new SimpleGrantedAuthority("USER"));

        try (MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            setPrivateField(pamAuthenticationProvider, "jaasAuthenticationProvider", mockJaasAuthenticationProvider);
            when(mockJaasAuthenticationProvider.authenticate(any())).thenReturn(mockAuthenticatedResult);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("getPamAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(pamAuthenticationProvider, mockAuthentication);

            // Verify specific interactions
            assertEquals(result, mockAuthenticatedResult);
            verify(mockJaasAuthenticationProvider, times(1)).authenticate(any());

            // Verify construction of required objects
            assertEquals(mockedUser.constructed().size(), 1);
            assertEquals(mockedToken.constructed().size(), 1);
        }
    }

    @Test
    public void testSetPamProperties_EmptyPropertiesHandling() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                    MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            // Setup with empty properties
            Properties emptyProperties = new Properties();
            when(mockConfiguration.getBoolean("atlas.authentication.method.pam.ugi-groups", true)).thenReturn(false);
            when(mockConfiguration.subset("atlas.authentication.method.pam")).thenReturn(mockConfiguration);
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(any())).thenReturn(emptyProperties);

            // Execute using reflection
            Method method = AtlasPamAuthenticationProvider.class.getDeclaredMethod("setPamProperties");
            method.setAccessible(true);
            method.invoke(pamAuthenticationProvider);

            // Verify
            assertFalse((Boolean) getPrivateField(pamAuthenticationProvider, "groupsFromUGI"));
            Map<String, String> options = (Map<String, String>) getPrivateField(pamAuthenticationProvider, "options");
            assertEquals(options.get("service"), "atlas-login"); // default service should still be set
        }
    }

    // Helper methods
    private void setupMockConfiguration() {
        mockProperties.setProperty("option1", "value1");
        mockProperties.setProperty("option2", "value2");
        // Note: no "service" property, so default should be used

        when(mockConfiguration.getBoolean("atlas.authentication.method.pam.ugi-groups", true)).thenReturn(true);
        when(mockConfiguration.subset("atlas.authentication.method.pam")).thenReturn(mockConfiguration);
    }

    private void setupPamAuthenticationProvider() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        options.put("service", "atlas-login");

        setPrivateField(pamAuthenticationProvider, "options", options);
        setPrivateField(pamAuthenticationProvider, "groupsFromUGI", false);
    }

    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set private field: " + fieldName, e);
        }
    }

    private Object getPrivateField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get private field: " + fieldName, e);
        }
    }
}
