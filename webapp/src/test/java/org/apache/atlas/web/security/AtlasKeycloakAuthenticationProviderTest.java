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
import org.keycloak.KeycloakSecurityContext;
import org.keycloak.adapters.OidcKeycloakAccount;
import org.keycloak.adapters.springsecurity.authentication.KeycloakAuthenticationProvider;
import org.keycloak.adapters.springsecurity.token.KeycloakAuthenticationToken;
import org.keycloak.representations.AccessToken;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class AtlasKeycloakAuthenticationProviderTest {
    @Mock
    private Configuration mockConfiguration;

    @Mock
    private KeycloakAuthenticationProvider mockKeycloakAuthenticationProvider;

    @Mock
    private Authentication mockAuthentication;

    @Mock
    private Principal principal;

    @Mock
    private KeycloakAuthenticationToken mockKeycloakAuthenticationToken;

    @Mock
    private OidcKeycloakAccount mockKeycloakAccount;

    @Mock
    private KeycloakSecurityContext mockKeycloakSecurityContext;

    @Mock
    private AccessToken mockAccessToken;

    private AtlasKeycloakAuthenticationProvider keycloakAuthenticationProvider;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConstructor_WithDefaultConfiguration() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.getBoolean("atlas.authentication.method.keycloak.ugi-groups", true)).thenReturn(true);
            when(mockConfiguration.getString("atlas.authentication.method.keycloak.groups_claim")).thenReturn(null);

            // Execute
            keycloakAuthenticationProvider = new AtlasKeycloakAuthenticationProvider();

            // Verify using reflection
            assertTrue((Boolean) getPrivateField(keycloakAuthenticationProvider, "groupsFromUGI"));
            assertNull(getPrivateField(keycloakAuthenticationProvider, "groupsClaim"));
            assertNotNull(getPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider"));
        }
    }

    @Test
    public void testConstructor_WithCustomConfiguration() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.getBoolean("atlas.authentication.method.keycloak.ugi-groups", true)).thenReturn(false);
            when(mockConfiguration.getString("atlas.authentication.method.keycloak.groups_claim")).thenReturn("customGroups");

            // Execute
            keycloakAuthenticationProvider = new AtlasKeycloakAuthenticationProvider();

            // Verify using reflection
            assertFalse((Boolean) getPrivateField(keycloakAuthenticationProvider, "groupsFromUGI"));
            assertEquals(getPrivateField(keycloakAuthenticationProvider, "groupsClaim"), "customGroups");
            assertNotNull(getPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider"));
        }
    }

    @Test
    public void testConstructor_WithException() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            // Setup
            mockedAppProps.when(() -> ApplicationProperties.get())
                    .thenThrow(new RuntimeException("Configuration loading failed"));

            // Execute & Verify
            try {
                keycloakAuthenticationProvider = new AtlasKeycloakAuthenticationProvider();
                fail("Expected Exception to be thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Configuration loading failed") || e instanceof RuntimeException);
            }
        }
    }

    @Test
    public void testAuthenticate_WithGroupsFromUGI() throws Exception {
        // Setup provider
        setupProviderWithConfig(true, null);

        String username = "testUser";
        List<GrantedAuthority> ugiAuthorities = new ArrayList<>();
        ugiAuthorities.add(new SimpleGrantedAuthority("UGI_GROUP_1"));
        ugiAuthorities.add(new SimpleGrantedAuthority("UGI_GROUP_2"));

        when(mockAuthentication.getName()).thenReturn(username);
        when(mockKeycloakAuthenticationToken.getAccount()).thenReturn(mockKeycloakAccount);
        when(mockKeycloakAuthenticationToken.isInteractive()).thenReturn(true);

        // Mock KeycloakAccount.getPrincipal() to return non-null value
        when(principal.getName()).thenReturn(username);
        when(mockKeycloakAccount.getPrincipal()).thenReturn(principal);

        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);

        // Mock UGI authorities using MockedStatic
        try (MockedStatic<AtlasAbstractAuthenticationProvider> mockedAbstract = mockStatic(AtlasAbstractAuthenticationProvider.class)) {
            mockedAbstract.when(() -> AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(username))
                    .thenReturn(ugiAuthorities);

            // Execute
            Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

            // Verify
            assertNotNull(result);
            assertTrue(result instanceof KeycloakAuthenticationToken);
            KeycloakAuthenticationToken token = (KeycloakAuthenticationToken) result;

            verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
            verify(mockKeycloakAuthenticationToken).getAccount();
            verify(mockKeycloakAuthenticationToken).isInteractive();
        }
    }

    @Test
    public void testAuthenticate_WithGroupsClaim() throws Exception {
        // Setup provider
        String groupsClaimName = "atlas_groups";
        setupProviderWithConfig(false, groupsClaimName);

        List<String> claimGroups = new ArrayList<>();
        claimGroups.add("CLAIM_GROUP_1");
        claimGroups.add("CLAIM_GROUP_2");
        claimGroups.add("CLAIM_GROUP_3");

        Map<String, Object> claims = new HashMap<>();
        claims.put(groupsClaimName, claimGroups);

        when(mockKeycloakAuthenticationToken.getAccount()).thenReturn(mockKeycloakAccount);
        when(mockKeycloakAuthenticationToken.isInteractive()).thenReturn(false);
        when(mockKeycloakAccount.getKeycloakSecurityContext()).thenReturn(mockKeycloakSecurityContext);
        when(mockKeycloakSecurityContext.getToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getOtherClaims()).thenReturn(claims);

        // Mock KeycloakAccount.getPrincipal() to return non-null value
        when(principal.getName()).thenReturn("testUser");
        when(mockKeycloakAccount.getPrincipal()).thenReturn(principal);
        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);

        // Execute
        Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof KeycloakAuthenticationToken);
        KeycloakAuthenticationToken token = (KeycloakAuthenticationToken) result;
        assertEquals(token.getAuthorities().size(), 3);
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("CLAIM_GROUP_1")));
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("CLAIM_GROUP_2")));
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("CLAIM_GROUP_3")));

        verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockKeycloakAuthenticationToken, times(2)).getAccount();
        verify(mockKeycloakAccount).getKeycloakSecurityContext();
        verify(mockKeycloakSecurityContext).getToken();
        verify(mockAccessToken).getOtherClaims();
    }

    @Test
    public void testAuthenticate_WithGroupsClaimNotInClaims() throws Exception {
        // Setup provider
        String groupsClaimName = "atlas_groups";
        setupProviderWithConfig(false, groupsClaimName);

        Map<String, Object> claims = new HashMap<>();
        claims.put("other_claim", "other_value");
        // Note: atlas_groups claim is not present

        when(mockKeycloakAuthenticationToken.getAccount()).thenReturn(mockKeycloakAccount);
        when(mockKeycloakAccount.getKeycloakSecurityContext()).thenReturn(mockKeycloakSecurityContext);
        when(mockKeycloakSecurityContext.getToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getOtherClaims()).thenReturn(claims);

        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);

        // Execute
        Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

        // Verify - should return the original authentication token since claim is not present
        assertNotNull(result);
        assertEquals(result, mockKeycloakAuthenticationToken);

        verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
        verify(mockKeycloakAuthenticationToken).getAccount();
        verify(mockKeycloakAccount).getKeycloakSecurityContext();
        verify(mockKeycloakSecurityContext).getToken();
        verify(mockAccessToken).getOtherClaims();
    }

    @Test
    public void testAuthenticate_WithNullGroupsClaim() throws Exception {
        // Setup provider - groupsClaim is null
        setupProviderWithConfig(false, null);

        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);

        // Execute
        Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

        // Verify - should return the original authentication token since groupsClaim is null
        assertNotNull(result);
        assertEquals(result, mockKeycloakAuthenticationToken);

        verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
        // Should not access token claims since groupsClaim is null
        verify(mockKeycloakAuthenticationToken, never()).getAccount();
    }

    @Test
    public void testAuthenticate_WithEmptyClaimGroups() throws Exception {
        // Setup provider
        String groupsClaimName = "atlas_groups";
        setupProviderWithConfig(false, groupsClaimName);

        List<String> emptyClaimGroups = new ArrayList<>();

        Map<String, Object> claims = new HashMap<>();
        claims.put(groupsClaimName, emptyClaimGroups);

        when(mockKeycloakAuthenticationToken.getAccount()).thenReturn(mockKeycloakAccount);
        when(mockKeycloakAuthenticationToken.isInteractive()).thenReturn(true);
        when(mockKeycloakAccount.getKeycloakSecurityContext()).thenReturn(mockKeycloakSecurityContext);
        when(mockKeycloakSecurityContext.getToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getOtherClaims()).thenReturn(claims);

        // Mock KeycloakAccount.getPrincipal() to return non-null value
        when(principal.getName()).thenReturn("testUser");
        when(mockKeycloakAccount.getPrincipal()).thenReturn(principal);
        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);
        // Execute
        Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof KeycloakAuthenticationToken);
        KeycloakAuthenticationToken token = (KeycloakAuthenticationToken) result;
        assertEquals(token.getAuthorities().size(), 0);
    }

    @Test
    public void testAuthenticate_WithSingleClaimGroup() throws Exception {
        // Setup provider
        String groupsClaimName = "atlas_groups";
        setupProviderWithConfig(false, groupsClaimName);

        List<String> singleClaimGroup = new ArrayList<>();
        singleClaimGroup.add("SINGLE_GROUP");

        Map<String, Object> claims = new HashMap<>();
        claims.put(groupsClaimName, singleClaimGroup);

        when(mockKeycloakAuthenticationToken.getAccount()).thenReturn(mockKeycloakAccount);
        when(mockKeycloakAuthenticationToken.isInteractive()).thenReturn(false);
        when(mockKeycloakAccount.getKeycloakSecurityContext()).thenReturn(mockKeycloakSecurityContext);
        when(mockKeycloakSecurityContext.getToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getOtherClaims()).thenReturn(claims);

        // Mock KeycloakAccount.getPrincipal() to return non-null value
        when(principal.getName()).thenReturn("testuser");
        when(mockKeycloakAccount.getPrincipal()).thenReturn(principal);
        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);

        // Execute
        Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof KeycloakAuthenticationToken);
        KeycloakAuthenticationToken token = (KeycloakAuthenticationToken) result;
        assertEquals(token.getAuthorities().size(), 1);
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("SINGLE_GROUP")));
    }

    @Test
    public void testAuthenticate_KeycloakProviderException() throws Exception {
        // Setup provider
        setupProviderWithConfig(true, null);

        // Mock the keycloak provider to throw exception
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication))
                .thenThrow(new RuntimeException("Keycloak authentication failed"));

        // Execute & Verify
        try {
            keycloakAuthenticationProvider.authenticate(mockAuthentication);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "Keycloak authentication failed");
        }

        verify(mockKeycloakAuthenticationProvider).authenticate(mockAuthentication);
    }

    @Test
    public void testSupports() throws Exception {
        // Setup provider
        setupProviderWithConfig(true, null);

        Class<?> testClass = KeycloakAuthenticationToken.class;

        // Mock the keycloak provider supports method
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.supports(testClass)).thenReturn(true);

        // Execute
        boolean result = keycloakAuthenticationProvider.supports(testClass);

        // Verify
        assertTrue(result);
        verify(mockKeycloakAuthenticationProvider).supports(testClass);
    }

    @Test
    public void testSupports_False() throws Exception {
        // Setup provider
        setupProviderWithConfig(true, null);

        Class<?> testClass = Object.class;

        // Mock the keycloak provider supports method
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.supports(testClass)).thenReturn(false);

        // Execute
        boolean result = keycloakAuthenticationProvider.supports(testClass);

        // Verify
        assertFalse(result);
        verify(mockKeycloakAuthenticationProvider).supports(testClass);
    }

    @Test
    public void testSupports_WithNullClass() throws Exception {
        // Setup provider
        setupProviderWithConfig(true, null);

        // Mock the keycloak provider supports method
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.supports(null)).thenReturn(false);

        // Execute
        boolean result = keycloakAuthenticationProvider.supports(null);

        // Verify
        assertFalse(result);
        verify(mockKeycloakAuthenticationProvider).supports(null);
    }

    @Test
    public void testClassAnnotations() {
        // Verify @Component annotation
        Component componentAnnotation = AtlasKeycloakAuthenticationProvider.class.getAnnotation(Component.class);
        assertNotNull(componentAnnotation);
    }

    @Test
    public void testExtendsAtlasAbstractAuthenticationProvider() throws Exception {
        // Setup provider
        setupProviderWithConfig(true, null);

        // Verify inheritance
        assertTrue(keycloakAuthenticationProvider instanceof AtlasAbstractAuthenticationProvider);
    }

    @Test
    public void testAuthenticate_WithComplexClaimGroups() throws Exception {
        // Setup provider
        String groupsClaimName = "complex_groups";
        setupProviderWithConfig(false, groupsClaimName);

        List<String> complexClaimGroups = new ArrayList<>();
        complexClaimGroups.add("GROUP_WITH_SPACES");
        complexClaimGroups.add("group-with-dashes");
        complexClaimGroups.add("GROUP.WITH.DOTS");
        complexClaimGroups.add("group_with_underscores");

        Map<String, Object> claims = new HashMap<>();
        claims.put(groupsClaimName, complexClaimGroups);
        claims.put("other_claim1", "value1");
        claims.put("other_claim2", 12345);

        when(mockKeycloakAuthenticationToken.getAccount()).thenReturn(mockKeycloakAccount);
        when(mockKeycloakAuthenticationToken.isInteractive()).thenReturn(true);
        when(mockKeycloakAccount.getKeycloakSecurityContext()).thenReturn(mockKeycloakSecurityContext);
        when(mockKeycloakSecurityContext.getToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getOtherClaims()).thenReturn(claims);

        // Mock KeycloakAccount.getPrincipal() to return non-null value
        when(principal.getName()).thenReturn("testUser");
        when(mockKeycloakAccount.getPrincipal()).thenReturn(principal);
        // Mock the keycloak provider authentication
        setPrivateField(keycloakAuthenticationProvider, "keycloakAuthenticationProvider", mockKeycloakAuthenticationProvider);
        when(mockKeycloakAuthenticationProvider.authenticate(mockAuthentication)).thenReturn(mockKeycloakAuthenticationToken);

        // Execute
        Authentication result = keycloakAuthenticationProvider.authenticate(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof KeycloakAuthenticationToken);
        KeycloakAuthenticationToken token = (KeycloakAuthenticationToken) result;
        assertEquals(token.getAuthorities().size(), 4);
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("GROUP_WITH_SPACES")));
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("group-with-dashes")));
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("GROUP.WITH.DOTS")));
        assertTrue(token.getAuthorities().contains(new SimpleGrantedAuthority("group_with_underscores")));
    }

    @Test
    public void testConstructor_ConfigurationProperties() throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.getBoolean("atlas.authentication.method.keycloak.ugi-groups", true)).thenReturn(true);
            when(mockConfiguration.getString("atlas.authentication.method.keycloak.groups_claim")).thenReturn("custom_claim");

            // Execute
            keycloakAuthenticationProvider = new AtlasKeycloakAuthenticationProvider();

            // Verify configuration loading
            verify(mockConfiguration).getBoolean("atlas.authentication.method.keycloak.ugi-groups", true);
            verify(mockConfiguration).getString("atlas.authentication.method.keycloak.groups_claim");

            // Verify field values
            assertTrue((Boolean) getPrivateField(keycloakAuthenticationProvider, "groupsFromUGI"));
            assertEquals(getPrivateField(keycloakAuthenticationProvider, "groupsClaim"), "custom_claim");
        }
    }

    // Helper methods
    private void setupProviderWithConfig(boolean groupsFromUGI, String groupsClaim) throws Exception {
        try (MockedStatic<ApplicationProperties> mockedAppProps = mockStatic(ApplicationProperties.class)) {
            mockedAppProps.when(() -> ApplicationProperties.get()).thenReturn(mockConfiguration);
            when(mockConfiguration.getBoolean("atlas.authentication.method.keycloak.ugi-groups", true)).thenReturn(groupsFromUGI);
            when(mockConfiguration.getString("atlas.authentication.method.keycloak.groups_claim")).thenReturn(groupsClaim);

            keycloakAuthenticationProvider = new AtlasKeycloakAuthenticationProvider();
        }
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
