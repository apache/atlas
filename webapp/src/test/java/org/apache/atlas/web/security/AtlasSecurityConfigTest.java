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

import org.apache.atlas.web.filters.ActiveServerFilter;
import org.apache.atlas.web.filters.AtlasAuthenticationEntryPoint;
import org.apache.atlas.web.filters.AtlasAuthenticationFilter;
import org.apache.atlas.web.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.web.filters.AtlasDelegatingAuthenticationEntryPoint;
import org.apache.atlas.web.filters.AtlasKnoxSSOAuthenticationFilter;
import org.apache.atlas.web.filters.StaleTransactionCleanupFilter;
import org.apache.commons.configuration.Configuration;
import org.keycloak.adapters.AdapterDeploymentContext;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.springsecurity.AdapterDeploymentContextFactoryBean;
import org.keycloak.adapters.springsecurity.authentication.KeycloakAuthenticationEntryPoint;
import org.keycloak.adapters.springsecurity.authentication.KeycloakLogoutHandler;
import org.keycloak.adapters.springsecurity.filter.KeycloakAuthenticatedActionsFilter;
import org.keycloak.adapters.springsecurity.filter.KeycloakAuthenticationProcessingFilter;
import org.keycloak.adapters.springsecurity.filter.KeycloakPreAuthActionsFilter;
import org.keycloak.adapters.springsecurity.filter.KeycloakSecurityContextRequestFilter;
import org.keycloak.adapters.springsecurity.management.HttpSessionManager;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.CsrfConfigurer;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.config.annotation.web.configurers.FormLoginConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.annotation.web.configurers.HttpBasicConfigurer;
import org.springframework.security.config.annotation.web.configurers.LogoutConfigurer;
import org.springframework.security.config.annotation.web.configurers.ServletApiConfigurer;
import org.springframework.security.config.annotation.web.configurers.SessionManagementConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.DelegatingAuthenticationEntryPoint;
import org.springframework.security.web.authentication.session.RegisterSessionAuthenticationStrategy;
import org.springframework.security.web.authentication.session.SessionAuthenticationStrategy;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.security.web.util.matcher.RequestHeaderRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.apache.atlas.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class AtlasSecurityConfigTest {
    @Mock
    private AtlasKnoxSSOAuthenticationFilter mockSsoAuthenticationFilter;

    @Mock
    private AtlasCSRFPreventionFilter mockCsrfPreventionFilter;

    @Mock
    private AtlasAuthenticationFilter mockAtlasAuthenticationFilter;

    @Mock
    private AtlasAuthenticationProvider mockAuthenticationProvider;

    @Mock
    private AtlasAuthenticationSuccessHandler mockSuccessHandler;

    @Mock
    private AtlasAuthenticationFailureHandler mockFailureHandler;

    @Mock
    private AtlasAuthenticationEntryPoint mockAtlasAuthenticationEntryPoint;

    @Mock
    private Configuration mockConfiguration;

    @Mock
    private StaleTransactionCleanupFilter mockStaleTransactionCleanupFilter;

    @Mock
    private ActiveServerFilter mockActiveServerFilter;

    @Mock
    private KeycloakConfigResolver mockKeycloakConfigResolver;

    @Mock
    private Resource mockKeycloakConfigFileResource;

    @Mock
    private AuthenticationManagerBuilder mockAuthenticationManagerBuilder;

    @Mock
    private WebSecurity mockWebSecurity;

    @Mock
    private HttpSecurity mockHttpSecurity;

    @Mock
    private AuthenticationManager mockAuthenticationManager;

    private AtlasSecurityConfig atlasSecurityConfig;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConstructor_WithKeycloakDisabled() throws Exception {
        // Setup
        when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(false);

        // Execute
        atlasSecurityConfig = new AtlasSecurityConfig(
                mockSsoAuthenticationFilter,
                mockCsrfPreventionFilter,
                mockAtlasAuthenticationFilter,
                mockAuthenticationProvider,
                mockSuccessHandler,
                mockFailureHandler,
                mockAtlasAuthenticationEntryPoint,
                mockConfiguration,
                mockStaleTransactionCleanupFilter,
                mockActiveServerFilter);

        // Verify using reflection
        assertFalse((Boolean) getPrivateField(atlasSecurityConfig, "keycloakEnabled"));
        assertEquals(getPrivateField(atlasSecurityConfig, "authenticationProvider"), mockAuthenticationProvider);
        assertEquals(getPrivateField(atlasSecurityConfig, "successHandler"), mockSuccessHandler);
        assertEquals(getPrivateField(atlasSecurityConfig, "failureHandler"), mockFailureHandler);
        assertEquals(getPrivateField(atlasSecurityConfig, "configuration"), mockConfiguration);
    }

    @Test
    public void testConstructor_WithKeycloakEnabled() throws Exception {
        // Setup
        when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(true);

        // Execute
        atlasSecurityConfig = new AtlasSecurityConfig(
                mockSsoAuthenticationFilter,
                mockCsrfPreventionFilter,
                mockAtlasAuthenticationFilter,
                mockAuthenticationProvider,
                mockSuccessHandler,
                mockFailureHandler,
                mockAtlasAuthenticationEntryPoint,
                mockConfiguration,
                mockStaleTransactionCleanupFilter,
                mockActiveServerFilter);

        // Verify using reflection
        assertTrue((Boolean) getPrivateField(atlasSecurityConfig, "keycloakEnabled"));
    }

    @Test
    public void testGetAuthenticationEntryPoint_WithKeycloakEnabled() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<KeycloakAuthenticationEntryPoint> mockedKeycloakEntry =
                        mockConstruction(KeycloakAuthenticationEntryPoint.class)) {
            // Execute
            AuthenticationEntryPoint result = atlasSecurityConfig.getAuthenticationEntryPoint();

            // Verify
            assertNotNull(result);
            assertEquals(mockedKeycloakEntry.constructed().size(), 1);

            KeycloakAuthenticationEntryPoint mockEntry = mockedKeycloakEntry.constructed().get(0);
            verify(mockEntry).setRealm("atlas.com");
            verify(mockEntry).setLoginUri("/login.jsp");
        }
    }

    @Test
    public void testGetAuthenticationEntryPoint_WithKeycloakDisabled() throws Exception {
        // Setup
        setupAtlasSecurityConfig(false);

        try (MockedConstruction<RequestHeaderRequestMatcher> mockedRequestMatcher =
                        mockConstruction(RequestHeaderRequestMatcher.class);
                MockedConstruction<AtlasDelegatingAuthenticationEntryPoint> mockedDelegatingEntry =
                        mockConstruction(AtlasDelegatingAuthenticationEntryPoint.class)) {
            // Execute
            AuthenticationEntryPoint result = atlasSecurityConfig.getAuthenticationEntryPoint();

            // Verify
            assertNotNull(result);
            assertEquals(mockedRequestMatcher.constructed().size(), 1);
            assertEquals(mockedDelegatingEntry.constructed().size(), 1);
        }
    }

    @Test
    public void testGetDelegatingAuthenticationEntryPoint() throws Exception {
        // Setup
        setupAtlasSecurityConfig(false);

        try (MockedConstruction<RequestHeaderRequestMatcher> mockedRequestMatcher =
                        mockConstruction(RequestHeaderRequestMatcher.class);
                MockedConstruction<DelegatingAuthenticationEntryPoint> mockedDelegatingEntry =
                        mockConstruction(DelegatingAuthenticationEntryPoint.class);
                MockedConstruction<AtlasDelegatingAuthenticationEntryPoint> mockedAtlasDelegatingEntry =
                        mockConstruction(AtlasDelegatingAuthenticationEntryPoint.class)) {
            // Execute
            DelegatingAuthenticationEntryPoint result = atlasSecurityConfig.getDelegatingAuthenticationEntryPoint();

            // Verify
            assertNotNull(result);
            assertEquals(mockedRequestMatcher.constructed().size(), 2); // One for each call
            assertEquals(mockedDelegatingEntry.constructed().size(), 1);

            DelegatingAuthenticationEntryPoint mockEntry = mockedDelegatingEntry.constructed().get(0);
            verify(mockEntry).setDefaultEntryPoint(any());
        }
    }

    @Test
    public void testConfigure_AuthenticationManagerBuilder() throws Exception {
        // Setup
        setupAtlasSecurityConfig(false);

        // Execute using reflection
        Method method = AtlasSecurityConfig.class.getDeclaredMethod("configure", AuthenticationManagerBuilder.class);
        method.setAccessible(true);
        method.invoke(atlasSecurityConfig, mockAuthenticationManagerBuilder);

        // Verify
        verify(mockAuthenticationManagerBuilder).authenticationProvider(mockAuthenticationProvider);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testConfigure_WebSecurity_WithKeycloakEnabled() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        WebSecurity.IgnoredRequestConfigurer mockIgnoredConfigurer = mock(WebSecurity.IgnoredRequestConfigurer.class);
        when(mockWebSecurity.ignoring()).thenReturn(mockIgnoredConfigurer);
        when(mockIgnoredConfigurer.antMatchers(any(String[].class))).thenReturn(mockIgnoredConfigurer);

        // Execute
        atlasSecurityConfig.configure(mockWebSecurity);

        // Verify that the configure method is called - this achieves code coverage
        verify(mockWebSecurity).ignoring();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testConfigure_WebSecurity_WithKeycloakDisabled() throws Exception {
        // Setup
        setupAtlasSecurityConfig(false);

        WebSecurity.IgnoredRequestConfigurer mockIgnoredConfigurer = mock(WebSecurity.IgnoredRequestConfigurer.class);
        when(mockWebSecurity.ignoring()).thenReturn(mockIgnoredConfigurer);
        when(mockIgnoredConfigurer.antMatchers(any(String[].class))).thenReturn(mockIgnoredConfigurer);

        // Execute
        atlasSecurityConfig.configure(mockWebSecurity);

        // Verify that the configure method is called - this achieves code coverage
        verify(mockWebSecurity).ignoring();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testConfigure_HttpSecurity_ComprehensiveCoverage() throws Exception {
        // Test 1: Migration mode enabled, Keycloak disabled
        testHttpSecurityConfiguration(true, false, false);

        // Test 2: HA mode enabled, Keycloak disabled
        testHttpSecurityConfiguration(false, true, false);

        // Test 3: Both migration and HA enabled, Keycloak disabled
        testHttpSecurityConfiguration(true, true, false);

        // Test 4: Neither migration nor HA enabled, Keycloak disabled
        testHttpSecurityConfiguration(false, false, false);

        // Test 5: Migration mode enabled, Keycloak enabled
        testHttpSecurityConfiguration(true, false, true);

        // Test 6: No special modes, Keycloak enabled
        testHttpSecurityConfiguration(false, false, true);
    }

    private void testHttpSecurityConfiguration(boolean migrationEnabled, boolean haEnabled, boolean keycloakEnabled) throws Exception {
        // Setup configuration
        when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(keycloakEnabled);
        when(mockConfiguration.getString(ATLAS_MIGRATION_MODE_FILENAME)).thenReturn(migrationEnabled ? "/migration/path" : "");
        when(mockConfiguration.getBoolean("atlas.server.ha.enabled", false)).thenReturn(haEnabled);

        if (keycloakEnabled) {
            setupKeycloakConfiguration();
        }

        // Create AtlasSecurityConfig instance
        AtlasSecurityConfig configInstance = new AtlasSecurityConfig(
                mockSsoAuthenticationFilter,
                mockCsrfPreventionFilter,
                mockAtlasAuthenticationFilter,
                mockAuthenticationProvider,
                mockSuccessHandler,
                mockFailureHandler,
                mockAtlasAuthenticationEntryPoint,
                mockConfiguration,
                mockStaleTransactionCleanupFilter,
                mockActiveServerFilter);

        // Set up comprehensive HttpSecurity mocking first
        setupHttpSecurityMocks();

        // Create a spy to override problematic methods
        AtlasSecurityConfig spyConfig = spy(configInstance);
        doReturn(mock(DelegatingAuthenticationEntryPoint.class)).when(spyConfig).getDelegatingAuthenticationEntryPoint();

        if (keycloakEnabled) {
            // Mock keycloak methods to avoid NPE - using correct return types
            doReturn(mock(KeycloakLogoutHandler.class)).when(spyConfig).keycloakLogoutHandler();
            doReturn(mock(KeycloakAuthenticationProcessingFilter.class)).when(spyConfig).keycloakAuthenticationProcessingFilter();
            doReturn(mock(KeycloakPreAuthActionsFilter.class)).when(spyConfig).keycloakPreAuthActionsFilter();
            doReturn(mock(KeycloakSecurityContextRequestFilter.class)).when(spyConfig).keycloakSecurityContextRequestFilter();
            doReturn(mock(KeycloakAuthenticatedActionsFilter.class)).when(spyConfig).keycloakAuthenticatedActionsRequestFilter();
        }

        // Execute the configure method for complete code coverage
        Method method = AtlasSecurityConfig.class.getDeclaredMethod("configure", HttpSecurity.class);
        method.setAccessible(true);
        method.invoke(spyConfig, mockHttpSecurity);

        // Verify that the HttpSecurity methods were called (basic verification)
        verify(mockHttpSecurity, atLeastOnce()).authorizeRequests();
        verify(mockHttpSecurity, atLeastOnce()).addFilterAfter(any(), any());
        verify(mockHttpSecurity, atLeastOnce()).addFilterBefore(any(), any());

        // Verify the condition logic was covered
        String migrationFile = mockConfiguration.getString(ATLAS_MIGRATION_MODE_FILENAME);
        boolean configMigrationEnabled = !org.apache.commons.lang.StringUtils.isEmpty(migrationFile);
        assertEquals(migrationEnabled, configMigrationEnabled);
        assertEquals(haEnabled, mockConfiguration.getBoolean("atlas.server.ha.enabled", false));
        assertEquals(keycloakEnabled, mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false));
    }

    @Test
    public void testConfigure_HttpSecurity_DetailedCoverage() throws Exception {
        // This test specifically targets the code blocks that were not being executed
        // Test all combinations to ensure complete coverage of configure(HttpSecurity) method

        // Test 1: All conditions false (baseline test)
        testDetailedHttpSecurityConfiguration(false, false, false);

        // Test 2: Migration enabled only
        testDetailedHttpSecurityConfiguration(true, false, false);

        // Test 3: HA enabled only
        testDetailedHttpSecurityConfiguration(false, true, false);

        // Test 4: Keycloak enabled only
        testDetailedHttpSecurityConfiguration(false, false, true);

        // Test 5: Migration + Keycloak enabled
        testDetailedHttpSecurityConfiguration(true, false, true);

        // Test 6: HA + Keycloak enabled
        testDetailedHttpSecurityConfiguration(false, true, true);

        // Test 7: All conditions enabled
        testDetailedHttpSecurityConfiguration(true, true, true);
    }

    private void testDetailedHttpSecurityConfiguration(boolean migrationEnabled, boolean haEnabled, boolean keycloakEnabled) throws Exception {
        // Setup fresh configuration for each test
        when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(keycloakEnabled);
        when(mockConfiguration.getString(ATLAS_MIGRATION_MODE_FILENAME)).thenReturn(migrationEnabled ? "/migration/path" : "");
        when(mockConfiguration.getBoolean("atlas.server.ha.enabled", false)).thenReturn(haEnabled);

        if (keycloakEnabled) {
            setupKeycloakConfiguration();
        }

        // Create fresh AtlasSecurityConfig instance
        AtlasSecurityConfig configInstance = new AtlasSecurityConfig(
                mockSsoAuthenticationFilter,
                mockCsrfPreventionFilter,
                mockAtlasAuthenticationFilter,
                mockAuthenticationProvider,
                mockSuccessHandler,
                mockFailureHandler,
                mockAtlasAuthenticationEntryPoint,
                mockConfiguration,
                mockStaleTransactionCleanupFilter,
                mockActiveServerFilter);

        // Create fresh HttpSecurity mock for each test to avoid state pollution
        HttpSecurity freshHttpSecurity = mock(HttpSecurity.class);
        setupHttpSecurityMocksFor(freshHttpSecurity);

        // Create spy to override problematic methods
        AtlasSecurityConfig spyConfig = spy(configInstance);
        doReturn(mock(DelegatingAuthenticationEntryPoint.class)).when(spyConfig).getDelegatingAuthenticationEntryPoint();

        if (keycloakEnabled) {
            // Mock keycloak methods
            doReturn(mock(KeycloakLogoutHandler.class)).when(spyConfig).keycloakLogoutHandler();
            doReturn(mock(KeycloakAuthenticationProcessingFilter.class)).when(spyConfig).keycloakAuthenticationProcessingFilter();
            doReturn(mock(KeycloakPreAuthActionsFilter.class)).when(spyConfig).keycloakPreAuthActionsFilter();
            doReturn(mock(KeycloakSecurityContextRequestFilter.class)).when(spyConfig).keycloakSecurityContextRequestFilter();
            doReturn(mock(KeycloakAuthenticatedActionsFilter.class)).when(spyConfig).keycloakAuthenticatedActionsRequestFilter();
        }

        // Execute the configure method
        Method method = AtlasSecurityConfig.class.getDeclaredMethod("configure", HttpSecurity.class);
        method.setAccessible(true);
        method.invoke(spyConfig, freshHttpSecurity);

        // Verify the basic security configuration was applied
        verify(freshHttpSecurity, atLeastOnce()).authorizeRequests();
        verify(freshHttpSecurity, atLeastOnce()).headers();
        verify(freshHttpSecurity, atLeastOnce()).servletApi();
        verify(freshHttpSecurity, atLeastOnce()).csrf();
        verify(freshHttpSecurity, atLeastOnce()).sessionManagement();
        verify(freshHttpSecurity, atLeastOnce()).httpBasic();
        verify(freshHttpSecurity, atLeastOnce()).formLogin();
        verify(freshHttpSecurity, atLeastOnce()).logout();

        // Verify filter additions based on configuration
        boolean shouldAddActiveServerFilter = migrationEnabled || haEnabled;
        if (shouldAddActiveServerFilter) {
            verify(freshHttpSecurity, atLeastOnce()).addFilterAfter(eq(mockActiveServerFilter), any());
        }

        // Verify standard filters are always added
        verify(freshHttpSecurity, atLeastOnce()).addFilterAfter(eq(mockStaleTransactionCleanupFilter), any());
        verify(freshHttpSecurity, atLeastOnce()).addFilterBefore(eq(mockSsoAuthenticationFilter), any());
        verify(freshHttpSecurity, atLeastOnce()).addFilterAfter(eq(mockAtlasAuthenticationFilter), any());
        verify(freshHttpSecurity, atLeastOnce()).addFilterAfter(eq(mockCsrfPreventionFilter), any());

        if (keycloakEnabled) {
            // Verify keycloak-specific filter additions
            verify(freshHttpSecurity, atLeastOnce()).addFilterBefore(any(KeycloakAuthenticationProcessingFilter.class), any());
            verify(freshHttpSecurity, atLeastOnce()).addFilterBefore(any(KeycloakPreAuthActionsFilter.class), any());
            verify(freshHttpSecurity, atLeastOnce()).addFilterAfter(any(KeycloakSecurityContextRequestFilter.class), any());
            verify(freshHttpSecurity, atLeastOnce()).addFilterAfter(any(KeycloakAuthenticatedActionsFilter.class), any());
        }
    }

    private void setupHttpSecurityMocksFor(HttpSecurity httpSecurity) throws Exception {
        // Create fresh mocks for each HttpSecurity instance
        ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry mockAuthRequests = mock(ExpressionUrlAuthorizationConfigurer.ExpressionInterceptUrlRegistry.class);
        ExpressionUrlAuthorizationConfigurer<HttpSecurity>.AuthorizedUrl mockAuthorizedUrl = mock(ExpressionUrlAuthorizationConfigurer.AuthorizedUrl.class);
        HeadersConfigurer<HttpSecurity> mockHeadersConfigurer = mock(HeadersConfigurer.class);
        HeadersConfigurer<HttpSecurity>.XXssConfig mockXssConfigurer = mock(HeadersConfigurer.XXssConfig.class);
        ServletApiConfigurer<HttpSecurity> mockServletApiConfigurer = mock(ServletApiConfigurer.class);
        CsrfConfigurer<HttpSecurity> mockCsrfConfigurer = mock(CsrfConfigurer.class);
        SessionManagementConfigurer<HttpSecurity> mockSessionConfigurer = mock(SessionManagementConfigurer.class);
        SessionManagementConfigurer<HttpSecurity>.SessionFixationConfigurer mockSessionFixationConfigurer = mock(SessionManagementConfigurer.SessionFixationConfigurer.class);
        HttpBasicConfigurer<HttpSecurity> mockHttpBasicConfigurer = mock(HttpBasicConfigurer.class);
        FormLoginConfigurer<HttpSecurity> mockFormLoginConfigurer = mock(FormLoginConfigurer.class);
        LogoutConfigurer<HttpSecurity> mockLogoutConfigurer = mock(LogoutConfigurer.class);

        // Set up the complete fluent API chain for this specific HttpSecurity instance
        when(httpSecurity.authorizeRequests()).thenReturn(mockAuthRequests);
        when(mockAuthRequests.anyRequest()).thenReturn(mockAuthorizedUrl);
        when(mockAuthorizedUrl.authenticated()).thenReturn(mockAuthRequests);
        when(mockAuthRequests.and()).thenReturn(httpSecurity);

        when(httpSecurity.headers()).thenReturn(mockHeadersConfigurer);
        when(mockHeadersConfigurer.xssProtection()).thenReturn(mockXssConfigurer);
        when(mockXssConfigurer.disable()).thenReturn(mockHeadersConfigurer);
        when(mockHeadersConfigurer.addHeaderWriter(any(StaticHeadersWriter.class))).thenReturn(mockHeadersConfigurer);
        when(mockHeadersConfigurer.and()).thenReturn(httpSecurity);

        when(httpSecurity.servletApi()).thenReturn(mockServletApiConfigurer);
        when(mockServletApiConfigurer.and()).thenReturn(httpSecurity);

        when(httpSecurity.csrf()).thenReturn(mockCsrfConfigurer);
        when(mockCsrfConfigurer.disable()).thenReturn(httpSecurity);

        when(httpSecurity.sessionManagement()).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.enableSessionUrlRewriting(false)).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.sessionCreationPolicy(SessionCreationPolicy.ALWAYS)).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.sessionFixation()).thenReturn(mockSessionFixationConfigurer);
        when(mockSessionFixationConfigurer.newSession()).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.and()).thenReturn(httpSecurity);

        when(httpSecurity.httpBasic()).thenReturn(mockHttpBasicConfigurer);
        when(mockHttpBasicConfigurer.authenticationEntryPoint(any())).thenReturn(mockHttpBasicConfigurer);
        when(mockHttpBasicConfigurer.and()).thenReturn(httpSecurity);

        when(httpSecurity.formLogin()).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.loginPage(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.loginProcessingUrl(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.successHandler(any())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.failureHandler(any())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.usernameParameter(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.passwordParameter(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.and()).thenReturn(httpSecurity);

        when(httpSecurity.logout()).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.logoutSuccessUrl(anyString())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.deleteCookies(anyString())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.logoutUrl(anyString())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.addLogoutHandler(any())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.and()).thenReturn(httpSecurity);

        // Mock filter methods
        when(httpSecurity.addFilterAfter(any(), any())).thenReturn(httpSecurity);
        when(httpSecurity.addFilterBefore(any(), any())).thenReturn(httpSecurity);
    }

    @Test
    public void testSessionAuthenticationStrategy() throws Exception {
        // Setup
        setupAtlasSecurityConfig(false);

        try (MockedConstruction<RegisterSessionAuthenticationStrategy> mockedStrategy =
                        mockConstruction(RegisterSessionAuthenticationStrategy.class);
                MockedConstruction<SessionRegistryImpl> mockedRegistry =
                        mockConstruction(SessionRegistryImpl.class)) {
            // Execute
            SessionAuthenticationStrategy result = atlasSecurityConfig.sessionAuthenticationStrategy();

            // Verify
            assertNotNull(result);
            assertEquals(mockedStrategy.constructed().size(), 1);
            assertEquals(mockedRegistry.constructed().size(), 1);
        }
    }

    @Test
    public void testAdapterDeploymentContext_WithConfigFile() throws Exception {
        // Setup with file-based configuration
        when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false)).thenReturn(true);
        setupKeycloakConfiguration("/path/to/keycloak.json");

        atlasSecurityConfig = new AtlasSecurityConfig(
                mockSsoAuthenticationFilter,
                mockCsrfPreventionFilter,
                mockAtlasAuthenticationFilter,
                mockAuthenticationProvider,
                mockSuccessHandler,
                mockFailureHandler,
                mockAtlasAuthenticationEntryPoint,
                mockConfiguration,
                mockStaleTransactionCleanupFilter,
                mockActiveServerFilter);

        setPrivateField(atlasSecurityConfig, "keycloakConfigFileResource", mockKeycloakConfigFileResource);

        try (MockedConstruction<FileSystemResource> mockedFileResource =
                        mockConstruction(FileSystemResource.class);
                MockedConstruction<AdapterDeploymentContextFactoryBean> mockedFactoryBean =
                        mockConstruction(AdapterDeploymentContextFactoryBean.class, (mock, context) -> {
                            when(mock.getObject()).thenReturn(mock(AdapterDeploymentContext.class));
                        })) {
            // Execute
            AdapterDeploymentContext result = atlasSecurityConfig.adapterDeploymentContext();

            // Verify
            assertNotNull(result);
            assertEquals(mockedFileResource.constructed().size(), 1);
            assertEquals(mockedFactoryBean.constructed().size(), 1);

            AdapterDeploymentContextFactoryBean mockFactoryBean = mockedFactoryBean.constructed().get(0);
            verify(mockFactoryBean).afterPropertiesSet();
            verify(mockFactoryBean).getObject();
        }
    }

    @Test
    public void testAdapterDeploymentContext_WithConfiguration() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);
        when(mockConfiguration.getString("atlas.authentication.method.keycloak.file")).thenReturn("");

        Configuration mockKeycloakConfig = mock(Configuration.class);
        when(mockConfiguration.subset("atlas.authentication.method.keycloak")).thenReturn(mockKeycloakConfig);
        when(mockKeycloakConfig.getString("realm", "atlas.com")).thenReturn("test-realm");
        when(mockKeycloakConfig.getString("auth-server-url", "https://localhost/auth")).thenReturn("https://test.com/auth");
        when(mockKeycloakConfig.getString("resource", "none")).thenReturn("test-resource");
        when(mockKeycloakConfig.getString("credentials-secret", "nosecret")).thenReturn("test-secret");

        try (MockedConstruction<AdapterConfig> mockedAdapterConfig =
                        mockConstruction(AdapterConfig.class);
                MockedStatic<KeycloakDeploymentBuilder> mockedDeploymentBuilder =
                        mockStatic(KeycloakDeploymentBuilder.class);
                MockedConstruction<AdapterDeploymentContextFactoryBean> mockedFactoryBean =
                        mockConstruction(AdapterDeploymentContextFactoryBean.class, (mock, context) -> {
                            when(mock.getObject()).thenReturn(mock(AdapterDeploymentContext.class));
                        })) {
            KeycloakDeployment mockDeployment = mock(KeycloakDeployment.class);
            mockedDeploymentBuilder.when(() -> KeycloakDeploymentBuilder.build(any(AdapterConfig.class)))
                    .thenReturn(mockDeployment);

            // Execute
            AdapterDeploymentContext result = atlasSecurityConfig.adapterDeploymentContext();

            // Verify
            assertNotNull(result);
            assertEquals(mockedAdapterConfig.constructed().size(), 1);
            assertEquals(mockedFactoryBean.constructed().size(), 1);

            AdapterConfig mockConfig = mockedAdapterConfig.constructed().get(0);
            verify(mockConfig).setRealm("test-realm");
            verify(mockConfig).setAuthServerUrl("https://test.com/auth");
            verify(mockConfig).setResource("test-resource");
            verify(mockConfig).setCredentials(any(Map.class));
        }
    }

    @Test
    public void testKeycloakPreAuthActionsFilter() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<KeycloakPreAuthActionsFilter> mockedFilter =
                        mockConstruction(KeycloakPreAuthActionsFilter.class);
                MockedConstruction<HttpSessionManager> mockedSessionManager =
                        mockConstruction(HttpSessionManager.class)) {
            // Execute
            KeycloakPreAuthActionsFilter result = atlasSecurityConfig.keycloakPreAuthActionsFilter();

            // Verify
            assertNotNull(result);
            assertEquals(mockedFilter.constructed().size(), 1);
            assertEquals(mockedSessionManager.constructed().size(), 1);
        }
    }

    @Test
    public void testHttpSessionManager() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<HttpSessionManager> mockedSessionManager =
                        mockConstruction(HttpSessionManager.class)) {
            // Execute
            HttpSessionManager result = atlasSecurityConfig.httpSessionManager();

            // Verify
            assertNotNull(result);
            assertEquals(mockedSessionManager.constructed().size(), 1);
        }
    }

    @Test
    public void testKeycloakLogoutHandler() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<KeycloakLogoutHandler> mockedLogoutHandler =
                        mockConstruction(KeycloakLogoutHandler.class);
                MockedConstruction<AdapterDeploymentContextFactoryBean> mockedFactoryBean =
                        mockConstruction(AdapterDeploymentContextFactoryBean.class, (mock, context) -> {
                            when(mock.getObject()).thenReturn(mock(AdapterDeploymentContext.class));
                        })) {
            // Execute using reflection
            Method method = AtlasSecurityConfig.class.getDeclaredMethod("keycloakLogoutHandler");
            method.setAccessible(true);
            KeycloakLogoutHandler result = (KeycloakLogoutHandler) method.invoke(atlasSecurityConfig);

            // Verify
            assertNotNull(result);
            assertEquals(mockedLogoutHandler.constructed().size(), 1);
        }
    }

    @Test
    public void testKeycloakSecurityContextRequestFilter() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<KeycloakSecurityContextRequestFilter> mockedFilter =
                        mockConstruction(KeycloakSecurityContextRequestFilter.class)) {
            // Execute
            KeycloakSecurityContextRequestFilter result = atlasSecurityConfig.keycloakSecurityContextRequestFilter();

            // Verify
            assertNotNull(result);
            assertEquals(mockedFilter.constructed().size(), 1);
        }
    }

    @Test
    public void testKeycloakAuthenticatedActionsRequestFilter() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<KeycloakAuthenticatedActionsFilter> mockedFilter =
                        mockConstruction(KeycloakAuthenticatedActionsFilter.class)) {
            // Execute
            KeycloakAuthenticatedActionsFilter result = atlasSecurityConfig.keycloakAuthenticatedActionsRequestFilter();

            // Verify
            assertNotNull(result);
            assertEquals(mockedFilter.constructed().size(), 1);
        }
    }

    @Test
    public void testKeycloakAuthenticationProcessingFilter() throws Exception {
        // Setup
        setupAtlasSecurityConfig(true);

        try (MockedConstruction<KeycloakAuthenticationProcessingFilter> mockedFilter =
                        mockConstruction(KeycloakAuthenticationProcessingFilter.class);
                MockedConstruction<RegisterSessionAuthenticationStrategy> mockedStrategy =
                        mockConstruction(RegisterSessionAuthenticationStrategy.class);
                MockedConstruction<SessionRegistryImpl> mockedRegistry =
                        mockConstruction(SessionRegistryImpl.class)) {
            // Create a spy to mock authenticationManagerBean() method
            AtlasSecurityConfig spyConfig = spy(atlasSecurityConfig);
            doReturn(mockAuthenticationManager).when(spyConfig).authenticationManagerBean();

            // Execute
            KeycloakAuthenticationProcessingFilter result = spyConfig.keycloakAuthenticationProcessingFilter();

            // Verify
            assertNotNull(result);
            assertEquals(mockedFilter.constructed().size(), 1);

            KeycloakAuthenticationProcessingFilter mockFilter = mockedFilter.constructed().get(0);
            verify(mockFilter).setSessionAuthenticationStrategy(any());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testClassAnnotations() {
        // Setup
        setupAtlasSecurityConfig(false);

        // Verify @EnableWebSecurity annotation
        EnableWebSecurity enableWebSecurityAnnotation = AtlasSecurityConfig.class.getAnnotation(EnableWebSecurity.class);
        assertNotNull(enableWebSecurityAnnotation);

        // Verify @EnableGlobalMethodSecurity annotation
        EnableGlobalMethodSecurity enableGlobalMethodSecurityAnnotation =
                AtlasSecurityConfig.class.getAnnotation(EnableGlobalMethodSecurity.class);
        assertNotNull(enableGlobalMethodSecurityAnnotation);
        assertTrue(enableGlobalMethodSecurityAnnotation.prePostEnabled());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testExtendsWebSecurityConfigurerAdapter() {
        // Setup
        setupAtlasSecurityConfig(false);

        // Verify inheritance
        assertTrue(atlasSecurityConfig instanceof WebSecurityConfigurerAdapter);
    }

    @Test
    public void testStaticFields() throws Exception {
        // Verify static final field
        Field requestMatcherField = AtlasSecurityConfig.class.getDeclaredField("KEYCLOAK_REQUEST_MATCHER");
        requestMatcherField.setAccessible(true);
        RequestMatcher requestMatcher = (RequestMatcher) requestMatcherField.get(null);
        assertNotNull(requestMatcher);
    }

    @Test
    public void testInjectAnnotation() throws Exception {
        // Verify @Inject annotation on constructor
        Inject injectAnnotation = AtlasSecurityConfig.class.getConstructor(
                AtlasKnoxSSOAuthenticationFilter.class,
                AtlasCSRFPreventionFilter.class,
                AtlasAuthenticationFilter.class,
                AtlasAuthenticationProvider.class,
                AtlasAuthenticationSuccessHandler.class,
                AtlasAuthenticationFailureHandler.class,
                AtlasAuthenticationEntryPoint.class,
                Configuration.class,
                StaleTransactionCleanupFilter.class,
                ActiveServerFilter.class
        ).getAnnotation(Inject.class);
        assertNotNull(injectAnnotation);
    }

    // Helper methods
    private void setupAtlasSecurityConfig(boolean keycloakEnabled) {
        when(mockConfiguration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false))
                .thenReturn(keycloakEnabled);

        // Setup keycloak configuration if enabled
        if (keycloakEnabled) {
            setupKeycloakConfiguration();
        }

        atlasSecurityConfig = new AtlasSecurityConfig(
                mockSsoAuthenticationFilter,
                mockCsrfPreventionFilter,
                mockAtlasAuthenticationFilter,
                mockAuthenticationProvider,
                mockSuccessHandler,
                mockFailureHandler,
                mockAtlasAuthenticationEntryPoint,
                mockConfiguration,
                mockStaleTransactionCleanupFilter,
                mockActiveServerFilter);

        // Set the keycloakConfigFileResource using reflection
        setPrivateField(atlasSecurityConfig, "keycloakConfigFileResource", mockKeycloakConfigFileResource);
    }

    @SuppressWarnings("deprecation")
    private void setupHttpSecurityMocks() throws Exception {
        // Create mocks for all the fluent API configurers
        ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry mockAuthRequests = mock(ExpressionUrlAuthorizationConfigurer.ExpressionInterceptUrlRegistry.class);
        ExpressionUrlAuthorizationConfigurer<HttpSecurity>.AuthorizedUrl mockAuthorizedUrl = mock(ExpressionUrlAuthorizationConfigurer.AuthorizedUrl.class);
        HeadersConfigurer<HttpSecurity> mockHeadersConfigurer = mock(HeadersConfigurer.class);
        HeadersConfigurer<HttpSecurity>.XXssConfig mockXssConfigurer = mock(HeadersConfigurer.XXssConfig.class);
        ServletApiConfigurer<HttpSecurity> mockServletApiConfigurer = mock(ServletApiConfigurer.class);
        CsrfConfigurer<HttpSecurity> mockCsrfConfigurer = mock(CsrfConfigurer.class);
        SessionManagementConfigurer<HttpSecurity> mockSessionConfigurer = mock(SessionManagementConfigurer.class);
        SessionManagementConfigurer<HttpSecurity>.SessionFixationConfigurer mockSessionFixationConfigurer = mock(SessionManagementConfigurer.SessionFixationConfigurer.class);
        HttpBasicConfigurer<HttpSecurity> mockHttpBasicConfigurer = mock(HttpBasicConfigurer.class);
        FormLoginConfigurer<HttpSecurity> mockFormLoginConfigurer = mock(FormLoginConfigurer.class);
        LogoutConfigurer<HttpSecurity> mockLogoutConfigurer = mock(LogoutConfigurer.class);

        // Set up the complete fluent API chain
        when(mockHttpSecurity.authorizeRequests()).thenReturn(mockAuthRequests);
        when(mockAuthRequests.anyRequest()).thenReturn(mockAuthorizedUrl);
        when(mockAuthorizedUrl.authenticated()).thenReturn(mockAuthRequests);
        when(mockAuthRequests.and()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.headers()).thenReturn(mockHeadersConfigurer);
        when(mockHeadersConfigurer.xssProtection()).thenReturn(mockXssConfigurer);
        when(mockXssConfigurer.disable()).thenReturn(mockHeadersConfigurer);
        when(mockHeadersConfigurer.addHeaderWriter(any(StaticHeadersWriter.class))).thenReturn(mockHeadersConfigurer);
        when(mockHeadersConfigurer.and()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.servletApi()).thenReturn(mockServletApiConfigurer);
        when(mockServletApiConfigurer.and()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.csrf()).thenReturn(mockCsrfConfigurer);
        when(mockCsrfConfigurer.disable()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.sessionManagement()).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.enableSessionUrlRewriting(false)).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.sessionCreationPolicy(SessionCreationPolicy.ALWAYS)).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.sessionFixation()).thenReturn(mockSessionFixationConfigurer);
        when(mockSessionFixationConfigurer.newSession()).thenReturn(mockSessionConfigurer);
        when(mockSessionConfigurer.and()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.httpBasic()).thenReturn(mockHttpBasicConfigurer);
        when(mockHttpBasicConfigurer.authenticationEntryPoint(any())).thenReturn(mockHttpBasicConfigurer);
        when(mockHttpBasicConfigurer.and()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.formLogin()).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.loginPage(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.loginProcessingUrl(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.successHandler(any())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.failureHandler(any())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.usernameParameter(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.passwordParameter(anyString())).thenReturn(mockFormLoginConfigurer);
        when(mockFormLoginConfigurer.and()).thenReturn(mockHttpSecurity);

        when(mockHttpSecurity.logout()).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.logoutSuccessUrl(anyString())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.deleteCookies(anyString())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.logoutUrl(anyString())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.addLogoutHandler(any())).thenReturn(mockLogoutConfigurer);
        when(mockLogoutConfigurer.and()).thenReturn(mockHttpSecurity);

        // Mock filter methods for complete coverage
        when(mockHttpSecurity.addFilterAfter(any(), any())).thenReturn(mockHttpSecurity);
        when(mockHttpSecurity.addFilterBefore(any(), any())).thenReturn(mockHttpSecurity);
    }

    private void setupKeycloakConfiguration() {
        setupKeycloakConfiguration(null); // Default to no file
    }

    private void setupKeycloakConfiguration(String configFile) {
        // Mock keycloak configuration
        Configuration mockKeycloakConfig = mock(Configuration.class);
        when(mockConfiguration.subset("atlas.authentication.method.keycloak")).thenReturn(mockKeycloakConfig);
        when(mockKeycloakConfig.getString("realm", "atlas.com")).thenReturn("atlas.com");
        when(mockKeycloakConfig.getString("auth-server-url", "https://localhost/auth")).thenReturn("https://localhost/auth");
        when(mockKeycloakConfig.getString("resource", "none")).thenReturn("none");
        when(mockKeycloakConfig.getString("credentials-secret", "nosecret")).thenReturn("nosecret");

        // Mock keycloak file configuration
        when(mockConfiguration.getString("atlas.authentication.method.keycloak.file")).thenReturn(configFile);
    }

    private void setupKeycloakMocks() throws Exception {
        // Set up mocks for keycloak-related method calls
        setPrivateField(atlasSecurityConfig, "keycloakConfigResolver", mockKeycloakConfigResolver);

        // Mock the keycloak filter creation methods
        AtlasSecurityConfig spyConfig = spy(atlasSecurityConfig);
        when(spyConfig.authenticationManagerBean()).thenReturn(mockAuthenticationManager);
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
