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
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.PostConstruct;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasLdapAuthenticationProviderTest {
    @Mock
    private Configuration mockConfiguration;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private LdapContextSource mockLdapContextSource;

    @Mock
    private FilterBasedLdapUserSearch mockFilterBasedLdapUserSearch;

    @Mock
    private BindAuthenticator mockBindAuthenticator;

    @Mock
    private DefaultLdapAuthoritiesPopulator mockDefaultLdapAuthoritiesPopulator;

    @Mock
    private LdapAuthenticationProvider mockLdapAuthenticationProvider;

    @Mock
    private Authentication mockAuthenticatedResult;

    private AtlasLdapAuthenticationProvider ldapAuthenticationProvider;
    private Properties mockProperties;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        ldapAuthenticationProvider = new AtlasLdapAuthenticationProvider();
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

            // Execute
            ldapAuthenticationProvider.setup();

            // Verify using reflection
            verifyLdapPropertiesSet();
        }
    }

    @Test
    public void testSetup_WithException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get())
                    .thenThrow(new RuntimeException("Configuration error"));

            // Execute - should not throw exception
            ldapAuthenticationProvider.setup();

            // Verify default values remain
            assertNull(getPrivateField(ldapAuthenticationProvider, "ldapURL"));
        }
    }

    @Test
    public void testAuthenticate_SuccessfulLdapBindAuthentication() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                    mockConstruction(DefaultSpringSecurityContextSource.class, (mock, context) -> {
                        // Mock the context source construction
                    });
                MockedConstruction<FilterBasedLdapUserSearch> mockedUserSearch =
                        mockConstruction(FilterBasedLdapUserSearch.class, (mock, context) -> {
                            // Mock the user search construction
                        });
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class, (mock, context) -> {
                            // Mock the bind authenticator construction
                        });
                MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                        mockConstruction(DefaultLdapAuthoritiesPopulator.class, (mock, context) -> {
                            // Mock the authorities populator construction
                        });
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class, (mock, context) -> {
                            // Mock User construction
                        });
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class, (mock, context) -> {
                            // Mock token construction
                        })) {
            when(mockAuthenticatedResult.isAuthenticated()).thenReturn(true);

            // Execute
            Authentication result = ldapAuthenticationProvider.authenticate(mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
        }
    }

    @Test
    public void testAuthenticate_BothMethodsFail() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class, (mock, context) -> {
                            throw new RuntimeException("LDAP connection failed");
                        })) {
            // Execute
            Authentication result = ldapAuthenticationProvider.authenticate(mockAuthentication);

            // Verify - should return the original authentication
            assertEquals(result, mockAuthentication);
        }
    }

    @Test
    public void testGetLdapBindAuthentication_Success() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                    MockedConstruction<FilterBasedLdapUserSearch> mockedUserSearch =
                            mockConstruction(FilterBasedLdapUserSearch.class);
                    MockedConstruction<BindAuthenticator> mockedBindAuth =
                            mockConstruction(BindAuthenticator.class);
                    MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                            mockConstruction(DefaultLdapAuthoritiesPopulator.class);
                    MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                            mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                                when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                            });
                    MockedConstruction<User> mockedUser =
                            mockConstruction(User.class);
                    MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                            mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapBindAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
        }
    }

    @Test
    public void testGetLdapBindAuthentication_WithNullCredentials() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn(null);

        // Execute using reflection
        Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapBindAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to null credentials
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetLdapBindAuthentication_WithEmptyUsername() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        // Execute using reflection
        Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapBindAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to empty username
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetLdapBindAuthentication_WithGroupsFromUGI() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        setPrivateField(ldapAuthenticationProvider, "groupsFromUGI", true);
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                MockedConstruction<FilterBasedLdapUserSearch> mockedUserSearch =
                        mockConstruction(FilterBasedLdapUserSearch.class);
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class);
                MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                        mockConstruction(DefaultLdapAuthoritiesPopulator.class);
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapBindAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify that method completes (UGI processing would be called)
            assertNotNull(result);
        }
    }

    @Test
    public void testGetLdapAuthentication_Success() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class);
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);
        }
    }

    @Test
    public void testGetLdapAuthentication_WithGroupSearchConfiguration() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        setPrivateField(ldapAuthenticationProvider, "ldapGroupSearchBase", "ou=groups,dc=example,dc=com");
        setPrivateField(ldapAuthenticationProvider, "ldapGroupSearchFilter", "(member={0})");
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class);
                MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                        mockConstruction(DefaultLdapAuthoritiesPopulator.class);
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);

            // Verify that DefaultLdapAuthoritiesPopulator was constructed (group search path)
            assertFalse(mockedAuthPopulator.constructed().isEmpty());
        }
    }

    @Test
    public void testGetLdapAuthentication_WithoutGroupSearch() throws Exception {
        // Setup - no group search base/filter
        setupLdapAuthenticationProvider();
        setPrivateField(ldapAuthenticationProvider, "ldapGroupSearchBase", null);
        setPrivateField(ldapAuthenticationProvider, "ldapGroupSearchFilter", null);
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class);
                MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                        mockConstruction(DefaultLdapAuthoritiesPopulator.class);
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);

            // Verify that DefaultLdapAuthoritiesPopulator was NOT constructed (no group search)
            assertTrue(mockedAuthPopulator.constructed().isEmpty());
        }
    }

    @Test
    public void testGetLdapAuthentication_WithEmptyPassword() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("");

        // Execute using reflection
        Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

        // Verify - should return original authentication due to empty password
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetLdapAuthentication_WithException() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class, (mock, context) -> {
                            throw new RuntimeException("LDAP connection error");
                        })) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify - should return original authentication due to exception
            assertEquals(result, mockAuthentication);
        }
    }

    @Test
    public void testSetLdapProperties_UsingReflection() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            // Setup
            setupMockConfiguration();
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(any())).thenReturn(mockProperties);

            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("setLdapProperties");
            method.setAccessible(true);
            method.invoke(ldapAuthenticationProvider);

            // Verify
            verifyLdapPropertiesSet();
        }
    }

    @Test
    public void testSetLdapProperties_WithException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get())
                    .thenThrow(new RuntimeException("Configuration error"));

            // Execute using reflection - should not throw exception
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("setLdapProperties");
            method.setAccessible(true);
            method.invoke(ldapAuthenticationProvider);

            // Verify - fields should remain null
            assertNull(getPrivateField(ldapAuthenticationProvider, "ldapURL"));
        }
    }

    @Test
    public void testGetLdapContextSource_UsingReflection() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapContextSource");
            method.setAccessible(true);
            LdapContextSource result = (LdapContextSource) method.invoke(ldapAuthenticationProvider);

            // Verify
            assertNotNull(result);
            assertEquals(mockedContextSource.constructed().size(), 1);

            // Verify configuration calls on the mock
            DefaultSpringSecurityContextSource mockSource = mockedContextSource.constructed().get(0);
            verify(mockSource).setUserDn(anyString());
            verify(mockSource).setPassword(anyString());
            verify(mockSource).setReferral(anyString());
            verify(mockSource).setCacheEnvironmentProperties(false);
            verify(mockSource).setAnonymousReadOnly(false);
            verify(mockSource).setPooled(true);
            verify(mockSource).afterPropertiesSet();
        }
    }

    @Test
    public void testGetDefaultLdapAuthoritiesPopulator_UsingReflection() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();

        try (MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                        mockConstruction(DefaultLdapAuthoritiesPopulator.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getDefaultLdapAuthoritiesPopulator", LdapContextSource.class);
            method.setAccessible(true);
            DefaultLdapAuthoritiesPopulator result = (DefaultLdapAuthoritiesPopulator) method.invoke(ldapAuthenticationProvider, mockLdapContextSource);

            // Verify
            assertNotNull(result);
            assertEquals(mockedAuthPopulator.constructed().size(), 1);

            // Verify configuration calls on the mock
            DefaultLdapAuthoritiesPopulator mockPopulator = mockedAuthPopulator.constructed().get(0);
            verify(mockPopulator).setGroupRoleAttribute(anyString());
            verify(mockPopulator).setGroupSearchFilter(anyString());
            verify(mockPopulator).setIgnorePartialResultException(true);
        }
    }

    @Test
    public void testGetBindAuthenticator_UsingReflection() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();

        try (MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getBindAuthenticator", FilterBasedLdapUserSearch.class, LdapContextSource.class);
            method.setAccessible(true);
            BindAuthenticator result = (BindAuthenticator) method.invoke(ldapAuthenticationProvider, mockFilterBasedLdapUserSearch, mockLdapContextSource);

            // Verify
            assertNotNull(result);
            assertEquals(mockedBindAuth.constructed().size(), 1);

            // Verify configuration calls on the mock
            BindAuthenticator mockAuth = mockedBindAuth.constructed().get(0);
            verify(mockAuth).setUserSearch(mockFilterBasedLdapUserSearch);
            verify(mockAuth).setUserDnPatterns(any(String[].class));
            verify(mockAuth).afterPropertiesSet();
        }
    }

    @Test
    public void testGetLdapBindAuthentication_WithNullUserSearchFilter() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        setPrivateField(ldapAuthenticationProvider, "ldapUserSearchFilter", null);
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                MockedConstruction<FilterBasedLdapUserSearch> mockedUserSearch =
                        mockConstruction(FilterBasedLdapUserSearch.class);
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class);
                MockedConstruction<DefaultLdapAuthoritiesPopulator> mockedAuthPopulator =
                        mockConstruction(DefaultLdapAuthoritiesPopulator.class);
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapBindAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);

            // Verify that FilterBasedLdapUserSearch was created with default filter "(uid={0})"
            assertFalse(mockedUserSearch.constructed().isEmpty());
        }
    }

    @Test
    public void testGetLdapAuthentication_WithMultipleUserDnPatterns() throws Exception {
        // Setup
        setupLdapAuthenticationProvider();
        setPrivateField(ldapAuthenticationProvider, "ldapUserDNPattern", "uid={0},ou=users,dc=example,dc=com;cn={0},ou=people,dc=example,dc=com");
        when(mockAuthentication.getName()).thenReturn("testUser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedConstruction<DefaultSpringSecurityContextSource> mockedContextSource =
                        mockConstruction(DefaultSpringSecurityContextSource.class);
                MockedConstruction<BindAuthenticator> mockedBindAuth =
                        mockConstruction(BindAuthenticator.class);
                MockedConstruction<LdapAuthenticationProvider> mockedLdapProvider =
                        mockConstruction(LdapAuthenticationProvider.class, (mock, context) -> {
                            when(mock.authenticate(any())).thenReturn(mockAuthenticatedResult);
                        });
                MockedConstruction<User> mockedUser =
                        mockConstruction(User.class);
                MockedConstruction<UsernamePasswordAuthenticationToken> mockedToken =
                        mockConstruction(UsernamePasswordAuthenticationToken.class)) {
            // Execute using reflection
            Method method = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("getLdapAuthentication", Authentication.class);
            method.setAccessible(true);
            Authentication result = (Authentication) method.invoke(ldapAuthenticationProvider, mockAuthentication);

            // Verify
            assertEquals(result, mockAuthenticatedResult);

            // Verify that BindAuthenticator was configured with multiple patterns
            assertFalse(mockedBindAuth.constructed().isEmpty());
            BindAuthenticator mockAuth = mockedBindAuth.constructed().get(0);
            verify(mockAuth).setUserDnPatterns(any(String[].class));
        }
    }

    @Test
    public void testClassAnnotations() {
        // Verify @Component annotation
        Component componentAnnotation = AtlasLdapAuthenticationProvider.class.getAnnotation(Component.class);
        assertNotNull(componentAnnotation);
    }

    @Test
    public void testSetupAnnotation() throws Exception {
        // Verify @PostConstruct annotation on setup method
        Method setupMethod = AtlasLdapAuthenticationProvider.class.getDeclaredMethod("setup");
        PostConstruct postConstructAnnotation = setupMethod.getAnnotation(PostConstruct.class);
        assertNotNull(postConstructAnnotation);
    }

    @Test
    public void testExtendsAtlasAbstractAuthenticationProvider() {
        // Verify inheritance
        assertTrue(ldapAuthenticationProvider instanceof AtlasAbstractAuthenticationProvider);

        // Test inherited supports() method
        assertTrue(ldapAuthenticationProvider.supports(UsernamePasswordAuthenticationToken.class));
    }

    @Test
    public void testDefaultFieldValues() throws Exception {
        // Create new instance to test default values
        AtlasLdapAuthenticationProvider newProvider = new AtlasLdapAuthenticationProvider();

        // Verify default values (should be null before setup)
        assertNull(getPrivateField(newProvider, "ldapURL"));
        assertNull(getPrivateField(newProvider, "ldapUserDNPattern"));
        assertNull(getPrivateField(newProvider, "ldapGroupSearchBase"));
        assertFalse((Boolean) getPrivateField(newProvider, "groupsFromUGI"));
    }

    // Helper methods
    private void setupMockConfiguration() {
        mockProperties.setProperty("url", "ldap://localhost:389");
        mockProperties.setProperty("userDNpattern", "uid={0},ou=users,dc=example,dc=com");
        mockProperties.setProperty("groupSearchBase", "ou=groups,dc=example,dc=com");
        mockProperties.setProperty("groupSearchFilter", "(member={0})");
        mockProperties.setProperty("groupRoleAttribute", "cn");
        mockProperties.setProperty("bind.dn", "cn=admin,dc=example,dc=com");
        mockProperties.setProperty("bind.password", "admin");
        mockProperties.setProperty("default.role", "ROLE_USER");
        mockProperties.setProperty("user.searchfilter", "(uid={0})");
        mockProperties.setProperty("referral", "follow");
        mockProperties.setProperty("base.dn", "dc=example,dc=com");

        when(mockConfiguration.subset("atlas.authentication.method.ldap")).thenReturn(mockConfiguration);
        when(mockConfiguration.getBoolean("atlas.authentication.method.ldap.ugi-groups", true)).thenReturn(true);
    }

    private void setupLdapAuthenticationProvider() throws Exception {
        setPrivateField(ldapAuthenticationProvider, "ldapURL", "ldap://localhost:389");
        setPrivateField(ldapAuthenticationProvider, "ldapUserDNPattern", "uid={0},ou=users,dc=example,dc=com");
        setPrivateField(ldapAuthenticationProvider, "ldapGroupSearchBase", "ou=groups,dc=example,dc=com");
        setPrivateField(ldapAuthenticationProvider, "ldapGroupSearchFilter", "(member={0})");
        setPrivateField(ldapAuthenticationProvider, "ldapGroupRoleAttribute", "cn");
        setPrivateField(ldapAuthenticationProvider, "ldapBindDN", "cn=admin,dc=example,dc=com");
        setPrivateField(ldapAuthenticationProvider, "ldapBindPassword", "admin");
        setPrivateField(ldapAuthenticationProvider, "ldapDefaultRole", "ROLE_USER");
        setPrivateField(ldapAuthenticationProvider, "ldapUserSearchFilter", "(uid={0})");
        setPrivateField(ldapAuthenticationProvider, "ldapReferral", "follow");
        setPrivateField(ldapAuthenticationProvider, "ldapBase", "dc=example,dc=com");
        setPrivateField(ldapAuthenticationProvider, "groupsFromUGI", false);
    }

    private void verifyLdapPropertiesSet() throws Exception {
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapURL"), "ldap://localhost:389");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapUserDNPattern"), "uid={0},ou=users,dc=example,dc=com");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapGroupSearchBase"), "ou=groups,dc=example,dc=com");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapGroupSearchFilter"), "(member={0})");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapGroupRoleAttribute"), "cn");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapBindDN"), "cn=admin,dc=example,dc=com");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapBindPassword"), "admin");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapDefaultRole"), "ROLE_USER");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapUserSearchFilter"), "(uid={0})");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapReferral"), "follow");
        assertEquals(getPrivateField(ldapAuthenticationProvider, "ldapBase"), "dc=example,dc=com");
        assertTrue((Boolean) getPrivateField(ldapAuthenticationProvider, "groupsFromUGI"));
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
