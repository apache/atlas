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

import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class AtlasAbstractAuthenticationProviderTest {
    @Mock
    private UserGroupInformation mockUgi;

    @Mock
    private Authentication mockAuthentication;

    private AtlasAbstractAuthenticationProvider authProvider;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        authProvider = new TestAtlasAbstractAuthenticationProvider();
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithValidUgiAndGroups() {
        String userName = "testuser";
        String[] groups = {"group1", "group2", "admin"};

        try (MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class)) {
            // Setup mocks
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser(userName)).thenReturn(mockUgi);
            mockedAuthUtil.when(() -> AuthenticationUtil.includeHadoopGroups()).thenReturn(false);
            when(mockUgi.getGroupNames()).thenReturn(groups);

            // Execute
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(userName);

            // Verify
            assertNotNull(authorities);
            assertEquals(authorities.size(), 3);
            assertTrue(authorities.stream().anyMatch(auth -> auth.getAuthority().equals("group1")));
            assertTrue(authorities.stream().anyMatch(auth -> auth.getAuthority().equals("group2")));
            assertTrue(authorities.stream().anyMatch(auth -> auth.getAuthority().equals("admin")));

            mockedUgi.verify(() -> UserGroupInformation.createRemoteUser(userName));
            verify(mockUgi).getGroupNames();
            mockedAuthUtil.verify(() -> AuthenticationUtil.includeHadoopGroups());
        }
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithNullUgi() {
        String userName = "testuser";

        try (MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class)) {
            // Setup mocks - UGI creation returns null
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser(userName)).thenReturn(null);
            mockedAuthUtil.when(() -> AuthenticationUtil.includeHadoopGroups()).thenReturn(false);

            // Execute
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(userName);

            assertNotNull(authorities);

            mockedUgi.verify(() -> UserGroupInformation.createRemoteUser(userName));
        }
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithEmptyUgiGroups() {
        String userName = "testuser";

        try (MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class)) {
            // Setup mocks - UGI with null/empty groups
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser(userName)).thenReturn(mockUgi);
            mockedAuthUtil.when(() -> AuthenticationUtil.includeHadoopGroups()).thenReturn(false);
            when(mockUgi.getGroupNames()).thenReturn(null); // No groups from UGI

            // Execute
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(userName);

            // Verify
            assertNotNull(authorities);

            verify(mockUgi).getGroupNames();
            mockedUgi.verify(() -> UserGroupInformation.createRemoteUser(userName));
        }
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithIncludeHadoopGroupsTrue() {
        String userName = "testuser";
        String[] ugiGroups = {"ugigroup1"};

        try (MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class)) {
            // Setup mocks
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser(userName)).thenReturn(mockUgi);
            mockedAuthUtil.when(() -> AuthenticationUtil.includeHadoopGroups()).thenReturn(true);
            when(mockUgi.getGroupNames()).thenReturn(ugiGroups);

            // Execute
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(userName);

            // Verify
            assertNotNull(authorities);
            assertTrue(authorities.size() >= 1);
            assertTrue(authorities.stream().anyMatch(auth -> auth.getAuthority().equals("ugigroup1")));

            verify(mockUgi).getGroupNames();
            mockedUgi.verify(() -> UserGroupInformation.createRemoteUser(userName));
        }
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithEmptyGroupArray() {
        String userName = "testuser";
        String[] groups = {}; // Empty array

        try (MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class)) {
            // Setup mocks
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser(userName)).thenReturn(mockUgi);
            mockedAuthUtil.when(() -> AuthenticationUtil.includeHadoopGroups()).thenReturn(false);
            when(mockUgi.getGroupNames()).thenReturn(groups); // Empty array

            // Execute
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(userName);

            // Verify
            assertNotNull(authorities);

            verify(mockUgi).getGroupNames();
            mockedUgi.verify(() -> UserGroupInformation.createRemoteUser(userName));
        }
    }

    @Test
    public void testSupports_WithUsernamePasswordAuthenticationToken() {
        // Execute
        boolean result = authProvider.supports(UsernamePasswordAuthenticationToken.class);

        // Verify
        assertTrue(result);
    }

    @Test
    public void testSupports_WithOtherAuthenticationType() {
        // Execute
        boolean result = authProvider.supports(Authentication.class);

        // Verify
        assertFalse(result);
    }

    @Test
    public void testSupports_WithNull() {
        try {
            // Execute
            boolean result = authProvider.supports(null);

            // Verify - should handle null gracefully or throw exception
            assertFalse(result);
        } catch (NullPointerException e) {
            // NPE is acceptable behavior for null input
            assertTrue(true);
        }
    }

    @Test
    public void testGetAuthenticationWithGrantedAuthority_WithAuthenticatedAuth() {
        // Setup
        String username = "testuser";
        String credentials = "password";

        when(mockAuthentication.isAuthenticated()).thenReturn(true);
        when(mockAuthentication.getName()).thenReturn(username);
        when(mockAuthentication.getCredentials()).thenReturn(credentials);
        when(mockAuthentication.getDetails()).thenReturn("details");

        // Execute
        Authentication result = authProvider.getAuthenticationWithGrantedAuthority(mockAuthentication);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof UsernamePasswordAuthenticationToken);
        assertEquals(result.getName(), username);
        assertEquals(result.getCredentials(), credentials);
        assertEquals(result.getDetails(), "details");
        assertNotNull(result.getAuthorities());
        assertEquals(result.getAuthorities().size(), 1);
        assertTrue(result.getAuthorities().stream()
                .anyMatch(auth -> auth.getAuthority().equals("DATA_SCIENTIST")));

        verify(mockAuthentication).isAuthenticated();
        verify(mockAuthentication, times(2)).getName(); // Called twice: once for getAuthorities, once for UserDetails
        verify(mockAuthentication, times(2)).getCredentials(); // Called twice: once for UserDetails, once for token
        verify(mockAuthentication).getDetails();
    }

    @Test
    public void testGetAuthenticationWithGrantedAuthority_WithNonAuthenticatedAuth() {
        // Setup
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute
        Authentication result = authProvider.getAuthenticationWithGrantedAuthority(mockAuthentication);

        // Verify
        assertSame(result, mockAuthentication);
        verify(mockAuthentication).isAuthenticated();
        verify(mockAuthentication, never()).getName();
        verify(mockAuthentication, never()).getCredentials();
    }

    @Test
    public void testGetAuthenticationWithGrantedAuthority_WithNullAuth() {
        // Execute
        Authentication result = authProvider.getAuthenticationWithGrantedAuthority(null);

        // Verify
        assertNull(result);
    }

    @Test
    public void testGetAuthenticationWithGrantedAuthorityFromUGI_WithAuthenticatedAuth() {
        String username = "testuser";
        String credentials = "password";
        String[] groups = {"group1", "admin"};

        when(mockAuthentication.isAuthenticated()).thenReturn(true);
        when(mockAuthentication.getName()).thenReturn(username);
        when(mockAuthentication.getCredentials()).thenReturn(credentials);
        when(mockAuthentication.getDetails()).thenReturn("details");

        try (MockedStatic<UserGroupInformation> mockedUgi = mockStatic(UserGroupInformation.class);
                MockedStatic<AuthenticationUtil> mockedAuthUtil = mockStatic(AuthenticationUtil.class)) {
            // Setup UGI mocks
            mockedUgi.when(() -> UserGroupInformation.createRemoteUser(username)).thenReturn(mockUgi);
            mockedAuthUtil.when(() -> AuthenticationUtil.includeHadoopGroups()).thenReturn(false);
            when(mockUgi.getGroupNames()).thenReturn(groups);

            // Execute
            Authentication result = authProvider.getAuthenticationWithGrantedAuthorityFromUGI(mockAuthentication);

            // Verify
            assertNotNull(result);
            assertTrue(result instanceof UsernamePasswordAuthenticationToken);
            assertEquals(result.getName(), username);
            assertEquals(result.getCredentials(), credentials);
            assertEquals(result.getDetails(), "details");
            assertNotNull(result.getAuthorities());
            assertEquals(result.getAuthorities().size(), 2);
            assertTrue(result.getAuthorities().stream()
                    .anyMatch(auth -> auth.getAuthority().equals("group1")));
            assertTrue(result.getAuthorities().stream()
                    .anyMatch(auth -> auth.getAuthority().equals("admin")));
        }
    }

    @Test
    public void testGetAuthenticationWithGrantedAuthorityFromUGI_WithNonAuthenticatedAuth() {
        // Setup
        when(mockAuthentication.isAuthenticated()).thenReturn(false);

        // Execute
        Authentication result = authProvider.getAuthenticationWithGrantedAuthorityFromUGI(mockAuthentication);

        // Verify
        assertSame(result, mockAuthentication);
        verify(mockAuthentication).isAuthenticated();
    }

    @Test
    public void testGetAuthenticationWithGrantedAuthorityFromUGI_WithNullAuth() {
        // Execute
        Authentication result = authProvider.getAuthenticationWithGrantedAuthorityFromUGI(null);

        // Verify
        assertNull(result);
    }

    @Test
    public void testGetAuthorities_UsingReflection() throws Exception {
        // Use reflection to test the protected getAuthorities method
        Method getAuthoritiesMethod = AtlasAbstractAuthenticationProvider.class
                .getDeclaredMethod("getAuthorities", String.class);
        getAuthoritiesMethod.setAccessible(true);

        // Execute
        @SuppressWarnings("unchecked")
        List<GrantedAuthority> authorities = (List<GrantedAuthority>) getAuthoritiesMethod
                .invoke(authProvider, "testuser");

        // Verify
        assertNotNull(authorities);
        assertEquals(authorities.size(), 1);
        assertEquals(authorities.get(0).getAuthority(), "DATA_SCIENTIST");
        assertTrue(authorities.get(0) instanceof SimpleGrantedAuthority);
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithNullUserName() {
        try {
            // Execute with null userName
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI(null);

            // Verify - should handle null gracefully
            assertNotNull(authorities);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Null user");
            assertTrue(true);
        }
    }

    @Test
    public void testGetAuthorities_WithNullUsername_UsingReflection() throws Exception {
        // Use reflection to test the protected getAuthorities method with null
        Method getAuthoritiesMethod = AtlasAbstractAuthenticationProvider.class
                .getDeclaredMethod("getAuthorities", String.class);
        getAuthoritiesMethod.setAccessible(true);

        // Execute
        @SuppressWarnings("unchecked")
        List<GrantedAuthority> authorities = (List<GrantedAuthority>) getAuthoritiesMethod
                .invoke(authProvider, (String) null);

        // Verify
        assertNotNull(authorities);
        assertEquals(authorities.size(), 1);
        assertEquals(authorities.get(0).getAuthority(), "DATA_SCIENTIST");
    }

    @Test
    public void testGetAuthoritiesFromUGI_WithEmptyUserName() {
        try {
            // Execute with empty userName
            List<GrantedAuthority> authorities = AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("");

            // Verify
            assertNotNull(authorities);
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is acceptable behavior for empty user
            assertTrue(e.getMessage().contains("user") || e.getMessage().contains("Null"));
            assertTrue(true);
        }
    }

    // Test implementation class to instantiate abstract class for testing
    private static class TestAtlasAbstractAuthenticationProvider extends AtlasAbstractAuthenticationProvider {
        @Override
        public Authentication authenticate(Authentication authentication) {
            // Mock implementation for testing
            return authentication;
        }
    }
}
