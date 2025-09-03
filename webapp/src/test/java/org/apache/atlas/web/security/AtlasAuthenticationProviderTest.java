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
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.context.annotation.Scope;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AtlasAuthenticationProviderTest {
    @Mock
    private AtlasLdapAuthenticationProvider mockLdapAuthenticationProvider;

    @Mock
    private AtlasFileAuthenticationProvider mockFileAuthenticationProvider;

    @Mock
    private AtlasADAuthenticationProvider mockAdAuthenticationProvider;

    @Mock
    private AtlasPamAuthenticationProvider mockPamAuthenticationProvider;

    @Mock
    private AtlasKeycloakAuthenticationProvider mockKeycloakAuthenticationProvider;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private Configuration mockConfiguration;

    private AtlasAuthenticationProvider authenticationProvider;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        authenticationProvider = new AtlasAuthenticationProvider(
                mockLdapAuthenticationProvider,
                mockFileAuthenticationProvider,
                mockAdAuthenticationProvider,
                mockPamAuthenticationProvider,
                mockKeycloakAuthenticationProvider);
    }

    @Test
    public void testConstructor() {
        // Verify constructor properly injects dependencies
        assertNotNull(authenticationProvider);

        // Use reflection to verify injected dependencies
        try {
            Field ldapField = AtlasAuthenticationProvider.class.getDeclaredField("ldapAuthenticationProvider");
            ldapField.setAccessible(true);
            assertEquals(ldapField.get(authenticationProvider), mockLdapAuthenticationProvider);

            Field fileField = AtlasAuthenticationProvider.class.getDeclaredField("fileAuthenticationProvider");
            fileField.setAccessible(true);
            assertEquals(fileField.get(authenticationProvider), mockFileAuthenticationProvider);

            Field adField = AtlasAuthenticationProvider.class.getDeclaredField("adAuthenticationProvider");
            adField.setAccessible(true);
            assertEquals(adField.get(authenticationProvider), mockAdAuthenticationProvider);

            Field pamField = AtlasAuthenticationProvider.class.getDeclaredField("pamAuthenticationProvider");
            pamField.setAccessible(true);
            assertEquals(pamField.get(authenticationProvider), mockPamAuthenticationProvider);

            Field keycloakField = AtlasAuthenticationProvider.class.getDeclaredField("atlasKeycloakAuthenticationProvider");
            keycloakField.setAccessible(true);
            assertEquals(keycloakField.get(authenticationProvider), mockKeycloakAuthenticationProvider);
        } catch (Exception e) {
            fail("Failed to verify constructor injection: " + e.getMessage());
        }
    }

    @Test
    public void testAuthenticate_SSOEnabled_Success() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", true);
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockAuthentication).isAuthenticated();
    }

    @Test
    public void testAuthenticate_SSOEnabled_NullAuthentication() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", true);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(null);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }
    }

    @Test
    public void testAuthenticate_SSOEnabled_NotAuthenticated() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", true);
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }
    }

    @Test
    public void testAuthenticate_LDAPType_Success() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "LDAP");
        when(mockLdapAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockLdapAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_ADType_Success() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "AD");
        when(mockAdAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockAdAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_ADType_Exception() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "AD");
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", false);

        when(mockAdAuthenticationProvider.authenticate(mockAuthentication))
                .thenThrow(new RuntimeException("AD connection failed"));
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }

        verify(mockAdAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_PAMEnabled_Success() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", true);
        when(mockPamAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockPamAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_PAMEnabled_Exception() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", true);
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", false);

        when(mockPamAuthenticationProvider.authenticate(mockAuthentication))
                .thenThrow(new RuntimeException("PAM authentication failed"));
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }

        verify(mockPamAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_KeycloakEnabled_Success() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "keycloakAuthenticationEnabled", true);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(true);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_KeycloakEnabled_Exception() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "keycloakAuthenticationEnabled", true);
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", false);

        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication))
                .thenThrow(new RuntimeException("Keycloak authentication failed"));
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }

        verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_FileAuthenticationFallback_Success() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "LDAP");
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", true);

        when(mockLdapAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(false).thenReturn(true);
        when(mockFileAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockLdapAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockFileAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_FileAuthenticationFallback_Failed() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "LDAP");
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", true);

        when(mockLdapAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(false, false);
        when(mockFileAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }

        verify(mockLdapAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockFileAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testAuthenticate_FileAuthenticationDisabled_AuthenticationNotNull() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "LDAP");
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", false);

        when(mockLdapAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }

        verify(mockLdapAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockFileAuthenticationProvider, never()).authenticate(any());
    }

    @Test
    public void testSupports_PAMEnabled() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", true);
        Class<?> authClass = Object.class;
        when(mockPamAuthenticationProvider.supports(authClass)).thenReturn(true);

        // Execute
        boolean result = authenticationProvider.supports(authClass);

        // Verify
        assertTrue(result);
        verify(mockPamAuthenticationProvider).supports(authClass);
    }

    @Test
    public void testSupports_LDAPType() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "LDAP");
        Class<?> authClass = Object.class;
        when(mockLdapAuthenticationProvider.supports(authClass)).thenReturn(true);

        // Execute
        boolean result = authenticationProvider.supports(authClass);

        // Verify
        assertTrue(result);
        verify(mockLdapAuthenticationProvider).supports(authClass);
    }

    @Test
    public void testSupports_ADType() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "AD");
        Class<?> authClass = Object.class;
        when(mockAdAuthenticationProvider.supports(authClass)).thenReturn(false);

        // Execute
        boolean result = authenticationProvider.supports(authClass);

        // Verify
        assertFalse(result);
        verify(mockAdAuthenticationProvider).supports(authClass);
    }

    @Test
    public void testSupports_KeycloakEnabled() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "keycloakAuthenticationEnabled", true);
        Class<?> authClass = Object.class;
        when(mockKeycloakAuthenticationProvider.supports(authClass)).thenReturn(true);

        // Execute
        boolean result = authenticationProvider.supports(authClass);

        // Verify
        assertTrue(result);
        verify(mockKeycloakAuthenticationProvider).supports(authClass);
    }

    @Test
    public void testSupports_DefaultBehavior() throws Exception {
        // Setup
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "keycloakAuthenticationEnabled", false);

        // Execute
        boolean result = authenticationProvider.supports(Object.class);

        // Verify - should call super.supports() which returns false for Object.class
        assertFalse(result);
    }

    @Test
    public void testIsSsoEnabled_False() {
        // Execute & Verify
        assertFalse(authenticationProvider.isSsoEnabled());
    }

    @Test
    public void testSetSsoEnabled_True() {
        // Execute
        authenticationProvider.setSsoEnabled(true);

        // Verify
        assertTrue(authenticationProvider.isSsoEnabled());
    }

    @Test
    public void testSetSsoEnabled_False() {
        // Setup
        authenticationProvider.setSsoEnabled(true);

        // Execute
        authenticationProvider.setSsoEnabled(false);

        // Verify
        assertFalse(authenticationProvider.isSsoEnabled());
    }

    @Test
    public void testSetAuthenticationMethod_AllEnabled() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.FILE_AUTH_METHOD, true)).thenReturn(true);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.PAM_AUTH_METHOD, false)).thenReturn(true);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(true);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.LDAP_AUTH_METHOD, false)).thenReturn(true);
            when(mockConfiguration.getString(AtlasAuthenticationProvider.LDAP_TYPE, "NONE")).thenReturn("LDAP");

            // Execute using reflection
            Method method = AtlasAuthenticationProvider.class.getDeclaredMethod("setAuthenticationMethod");
            method.setAccessible(true);
            method.invoke(authenticationProvider);

            // Verify
            assertTrue((Boolean) getPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled"));
            assertTrue((Boolean) getPrivateField(authenticationProvider, "pamAuthenticationEnabled"));
            assertTrue((Boolean) getPrivateField(authenticationProvider, "keycloakAuthenticationEnabled"));
            assertEquals(getPrivateField(authenticationProvider, "ldapType"), "LDAP");
        }
    }

    @Test
    public void testSetAuthenticationMethod_LDAPDisabled() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.FILE_AUTH_METHOD, true)).thenReturn(false);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.PAM_AUTH_METHOD, false)).thenReturn(false);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(false);
            when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.LDAP_AUTH_METHOD, false)).thenReturn(false);

            // Execute using reflection
            Method method = AtlasAuthenticationProvider.class.getDeclaredMethod("setAuthenticationMethod");
            method.setAccessible(true);
            method.invoke(authenticationProvider);

            // Verify
            assertFalse((Boolean) getPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled"));
            assertFalse((Boolean) getPrivateField(authenticationProvider, "pamAuthenticationEnabled"));
            assertFalse((Boolean) getPrivateField(authenticationProvider, "keycloakAuthenticationEnabled"));
            assertEquals(getPrivateField(authenticationProvider, "ldapType"), "NONE");
        }
    }

    @Test
    public void testSetAuthenticationMethod_WithException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get())
                    .thenThrow(new RuntimeException("Configuration error"));

            // Execute using reflection - should not throw exception
            Method method = AtlasAuthenticationProvider.class.getDeclaredMethod("setAuthenticationMethod");
            method.setAccessible(true);
            method.invoke(authenticationProvider);

            // Verify default values are preserved
            assertTrue((Boolean) getPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled"));
            assertFalse((Boolean) getPrivateField(authenticationProvider, "pamAuthenticationEnabled"));
            assertFalse((Boolean) getPrivateField(authenticationProvider, "keycloakAuthenticationEnabled"));
            assertEquals(getPrivateField(authenticationProvider, "ldapType"), "NONE");
        }
    }

    @Test
    public void testGetSSOAuthentication_UsingReflection() throws Exception {
        // Execute using reflection
        Method method = AtlasAuthenticationProvider.class.getDeclaredMethod("getSSOAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(authenticationProvider, mockAuthentication);

        // Verify
        assertEquals(result, mockAuthentication);
    }

    @Test
    public void testGetSSOAuthentication_WithNullAuthentication() throws Exception {
        // Execute using reflection
        Method method = AtlasAuthenticationProvider.class.getDeclaredMethod("getSSOAuthentication", Authentication.class);
        method.setAccessible(true);
        Authentication result = (Authentication) method.invoke(authenticationProvider, (Authentication) null);

        // Verify
        assertNull(result);
    }

    @Test
    public void testClassAnnotations() {
        Component componentAnnotation = AtlasAuthenticationProvider.class.getAnnotation(Component.class);
        assertNotNull(componentAnnotation);

        // Verify @Scope annotation
        Scope scopeAnnotation = AtlasAuthenticationProvider.class.getAnnotation(Scope.class);
        assertNotNull(scopeAnnotation);
        assertEquals(scopeAnnotation.value(), "prototype");
    }

    @Test
    public void testConstructorAnnotation() throws Exception {
        // Verify @Inject annotation on constructor
        Inject injectAnnotation = AtlasAuthenticationProvider.class.getConstructor(
                AtlasLdapAuthenticationProvider.class,
                AtlasFileAuthenticationProvider.class,
                AtlasADAuthenticationProvider.class,
                AtlasPamAuthenticationProvider.class,
                AtlasKeycloakAuthenticationProvider.class
        ).getAnnotation(Inject.class);
        assertNotNull(injectAnnotation);
    }

    @Test
    public void testSetAuthenticationMethodAnnotation() throws Exception {
        // Verify @PostConstruct annotation on setAuthenticationMethod
        Method method = AtlasAuthenticationProvider.class.getDeclaredMethod("setAuthenticationMethod");
        PostConstruct postConstructAnnotation = method.getAnnotation(PostConstruct.class);
        assertNotNull(postConstructAnnotation);
    }

    @Test
    public void testConstantValues() {
        // Verify constant values
        assertEquals(AtlasAuthenticationProvider.FILE_AUTH_METHOD, "atlas.authentication.method.file");
        assertEquals(AtlasAuthenticationProvider.LDAP_AUTH_METHOD, "atlas.authentication.method.ldap");
        assertEquals(AtlasAuthenticationProvider.LDAP_TYPE, "atlas.authentication.method.ldap.type");
        assertEquals(AtlasAuthenticationProvider.PAM_AUTH_METHOD, "atlas.authentication.method.pam");
        assertEquals(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, "atlas.authentication.method.keycloak");
    }

    @Test
    public void testDefaultFieldValues() throws Exception {
        AtlasAuthenticationProvider newProvider = new AtlasAuthenticationProvider(
                mockLdapAuthenticationProvider,
                mockFileAuthenticationProvider,
                mockAdAuthenticationProvider,
                mockPamAuthenticationProvider,
                mockKeycloakAuthenticationProvider);

        // Verify default values
        assertTrue((Boolean) getPrivateField(newProvider, "fileAuthenticationMethodEnabled"));
        assertFalse((Boolean) getPrivateField(newProvider, "pamAuthenticationEnabled"));
        assertFalse((Boolean) getPrivateField(newProvider, "keycloakAuthenticationEnabled"));
        assertEquals(getPrivateField(newProvider, "ldapType"), "NONE");
        assertFalse((Boolean) getPrivateField(newProvider, "ssoEnabled"));
    }

    @Test
    public void testAuthenticate_ComplexScenario_LDAPFailsFileSucceeds() throws Exception {
        // Setup complex scenario: LDAP throws exception, file auth succeeds
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "LDAP");
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", true);

        // LDAP provider throws exception
        when(mockLdapAuthenticationProvider.authenticate(mockAuthentication))
                .thenThrow(new RuntimeException("LDAP server unavailable"));

        // Authentication initially not authenticated, then becomes authenticated after file auth
        when(mockAuthentication.isAuthenticated()).thenReturn(false).thenReturn(true);
        when(mockFileAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockAuthentication);

        // Execute
        Authentication result = authenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockAuthentication);
        verify(mockLdapAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockFileAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockAuthentication, times(2)).isAuthenticated();
    }

    @Test
    public void testAuthenticate_AllMethodsDisabled() throws Exception {
        // Setup - all authentication methods disabled
        setPrivateField(authenticationProvider, "ssoEnabled", false);
        setPrivateField(authenticationProvider, "ldapType", "NONE");
        setPrivateField(authenticationProvider, "pamAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "keycloakAuthenticationEnabled", false);
        setPrivateField(authenticationProvider, "fileAuthenticationMethodEnabled", false);

        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute & Verify
        try {
            authenticationProvider.authenticate(mockAuthentication);
            fail("Expected AtlasAuthenticationException");
        } catch (AtlasAuthenticationException e) {
            assertEquals(e.getMessage(), "Authentication failed.");
        }

        // Verify no providers were called
        verify(mockLdapAuthenticationProvider, never()).authenticate(any());
        verify(mockAdAuthenticationProvider, never()).authenticate(any());
        verify(mockPamAuthenticationProvider, never()).authenticate(any());
        verify(mockKeycloakAuthenticationProvider, never()).authenticate(any());
        verify(mockFileAuthenticationProvider, never()).authenticate(any());
    }

    // Helper methods for reflection
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
