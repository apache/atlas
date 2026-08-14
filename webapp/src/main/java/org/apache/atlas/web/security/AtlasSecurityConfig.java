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
package org.apache.atlas.web.security;

import org.apache.atlas.server.common.filters.ActiveServerFilter;
import org.apache.atlas.server.common.filters.AtlasAuthenticationEntryPoint;
import org.apache.atlas.server.common.filters.AtlasAuthenticationFilter;
import org.apache.atlas.server.common.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.server.common.filters.AtlasKnoxSSOAuthenticationFilter;
import org.apache.atlas.server.common.security.AtlasAuthenticationFailureHandler;
import org.apache.atlas.server.common.security.AtlasAuthenticationProvider;
import org.apache.atlas.server.common.security.AtlasAuthenticationSuccessHandler;
import org.apache.atlas.web.filters.AtlasHeaderPreAuthFilter;
import org.apache.atlas.web.filters.AtlasJwtAuthWrapper;
import org.apache.commons.configuration2.Configuration;
import org.keycloak.adapters.AdapterDeploymentContext;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.springsecurity.AdapterDeploymentContextFactoryBean;
import org.keycloak.adapters.springsecurity.KeycloakConfiguration;
import org.keycloak.adapters.springsecurity.authentication.KeycloakAuthenticationEntryPoint;
import org.keycloak.adapters.springsecurity.authentication.KeycloakLogoutHandler;
import org.keycloak.adapters.springsecurity.filter.KeycloakAuthenticatedActionsFilter;
import org.keycloak.adapters.springsecurity.filter.KeycloakAuthenticationProcessingFilter;
import org.keycloak.adapters.springsecurity.filter.KeycloakPreAuthActionsFilter;
import org.keycloak.adapters.springsecurity.filter.KeycloakSecurityContextRequestFilter;
import org.keycloak.adapters.springsecurity.filter.QueryParamPresenceRequestMatcher;
import org.keycloak.adapters.springsecurity.management.HttpSessionManager;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.logout.LogoutFilter;
import org.springframework.security.web.authentication.session.RegisterSessionAuthenticationStrategy;
import org.springframework.security.web.authentication.session.SessionAuthenticationStrategy;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestHeaderRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

import javax.inject.Inject;
import javax.servlet.Filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Security configuration for the Atlas main webapp.
 *
 * Why this class was created
 * Before this commit, {@code server-common/AtlasSecurityConfig} was a concrete
 * {@code @EnableWebSecurity} class. That caused Spring to register TWO security
 * configurations when the webapp started — the server-common one (found on
 * classpath) and any module-specific one. This bean conflict prevented the
 * Kerberos authentication filter chain from initializing correctly.
 *
 * This class was created as the SINGLE {@code @EnableWebSecurity} authority
 * for the webapp, extending the now-abstract server-common base. Spring Security
 * sees only this concrete class and builds one correct filter chain.
 *
 * What is webapp-specific in this class (not in base)
 *   {@code staleTransactionCleanupFilter} — cleans up open JanusGraph transactions.
 *   Only exists in webapp's Spring context; rest-notification-webapp has no
 *   JanusGraph transactions. Was previously in server-common's constructor
 *   causing {@code NoSuchBeanDefinitionException} at rest-notification startup.
 */
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@KeycloakConfiguration
public class AtlasSecurityConfig extends org.apache.atlas.server.common.security.AtlasSecurityConfig {
    public static final RequestMatcher KEYCLOAK_REQUEST_MATCHER = new OrRequestMatcher(new AntPathRequestMatcher("/login.jsp"), new RequestHeaderRequestMatcher("Authorization"), new QueryParamPresenceRequestMatcher("access_token"));

    private final boolean                      keycloakEnabled;
    private final ObjectProvider<Filter>       staleTransactionCleanupFilterProvider;
    private final AtlasHeaderPreAuthFilter     headerPreAuthFilter;
    private final AtlasJwtAuthWrapper          atlasJwtAuthWrapper;

    @Value("${keycloak.configurationFile:WEB-INF/keycloak.json}")
    private Resource keycloakConfigFileResource;

    @Autowired(required = false)
    private KeycloakConfigResolver keycloakConfigResolver;

    @Inject
    public AtlasSecurityConfig(ObjectProvider<AtlasKnoxSSOAuthenticationFilter> ssoAuthenticationFilterProvider,
                               ObjectProvider<AtlasCSRFPreventionFilter> csrfPreventionFilterProvider,
                               ObjectProvider<AtlasAuthenticationFilter> atlasAuthenticationFilterProvider,
                               AtlasAuthenticationProvider authenticationProvider,
                               AtlasAuthenticationSuccessHandler successHandler,
                               AtlasAuthenticationFailureHandler failureHandler,
                               AtlasAuthenticationEntryPoint atlasAuthenticationEntryPoint,
                               Configuration configuration,
                               ObjectProvider<ActiveServerFilter> activeServerFilterProvider,
                               @Qualifier("staleTransactionCleanupFilter") ObjectProvider<Filter> staleTransactionCleanupFilterProvider,
                               AtlasHeaderPreAuthFilter headerPreAuthFilter,
                               AtlasJwtAuthWrapper atlasJwtAuthWrapper) {
        super(ssoAuthenticationFilterProvider, csrfPreventionFilterProvider, atlasAuthenticationFilterProvider,
                authenticationProvider, successHandler, failureHandler, atlasAuthenticationEntryPoint, configuration,
                activeServerFilterProvider);
        this.staleTransactionCleanupFilterProvider = staleTransactionCleanupFilterProvider;
        this.headerPreAuthFilter                    = headerPreAuthFilter;
        this.atlasJwtAuthWrapper                    = atlasJwtAuthWrapper;
        this.keycloakEnabled = configuration.getBoolean(AtlasAuthenticationProvider.KEYCLOAK_AUTH_METHOD, false);
    }

    @Override
    public AuthenticationEntryPoint getAuthenticationEntryPoint() throws Exception {
        if (keycloakEnabled) {
            KeycloakAuthenticationEntryPoint keycloakAuthenticationEntryPoint = new KeycloakAuthenticationEntryPoint(adapterDeploymentContext());

            keycloakAuthenticationEntryPoint.setRealm("atlas.com");
            keycloakAuthenticationEntryPoint.setLoginUri("/login.jsp");
            return keycloakAuthenticationEntryPoint;
        }
        return super.getAuthenticationEntryPoint();
    }

    @Override
    public void configure(WebSecurity web) {
        List<String> matchers = new ArrayList<>(Arrays.asList("/css/**", "/n/css/**",
                "/img/**", "/n/img/**",
                "/libs/**", "/n/libs/**",
                "/js/**", "/n/js/**",
                "/ieerror.html", "/migration-status.html",
                "/api/atlas/admin/status",
                "/api/atlas/admin/prometheus",
                "/apidocs/**"));

        if (!keycloakEnabled) {
            matchers.add("/login.jsp");
        }

        web.ignoring().antMatchers(matchers.toArray(new String[0]));
    }

    @Override
    protected void configure(HttpSecurity httpSecurity) throws Exception {
        configureCommonHttpSecurity(httpSecurity);
        addWebUiFormLogin(httpSecurity);
        addHaAndMigrationGuards(httpSecurity);

        // Header auth
        httpSecurity.addFilterBefore(headerPreAuthFilter, BasicAuthenticationFilter.class);

        // Knox SSO — after header pre-auth
        AtlasKnoxSSOAuthenticationFilter ssoAuthenticationFilter = ssoAuthenticationFilterProvider.getIfAvailable();
        if (ssoAuthenticationFilter != null) {
            httpSecurity.addFilterAfter(ssoAuthenticationFilter, AtlasHeaderPreAuthFilter.class);
        }

        // JWT wrapper
        httpSecurity.addFilterAfter(atlasJwtAuthWrapper, AtlasKnoxSSOAuthenticationFilter.class);

        // Stale transaction cleanup (webapp-only filter)
        Filter staleTransactionCleanupFilter = staleTransactionCleanupFilterProvider.getIfAvailable();
        if (staleTransactionCleanupFilter != null) {
            httpSecurity.addFilterAfter(staleTransactionCleanupFilter, BasicAuthenticationFilter.class);
        }

        // Atlas auth filter
        AtlasAuthenticationFilter atlasAuthenticationFilter = atlasAuthenticationFilterProvider.getIfAvailable();
        if (atlasAuthenticationFilter != null) {
            httpSecurity.addFilterAfter(atlasAuthenticationFilter, SecurityContextHolderAwareRequestFilter.class);
        }

        // CSRF filter
        AtlasCSRFPreventionFilter csrfPreventionFilter = csrfPreventionFilterProvider.getIfAvailable();
        if (csrfPreventionFilter != null && atlasAuthenticationFilter != null) {
            httpSecurity.addFilterAfter(csrfPreventionFilter, AtlasAuthenticationFilter.class);
        }

        if (keycloakEnabled) {
            httpSecurity.logout().addLogoutHandler(keycloakLogoutHandler()).and()
                    .addFilterBefore(keycloakAuthenticationProcessingFilter(), BasicAuthenticationFilter.class)
                    .addFilterBefore(keycloakPreAuthActionsFilter(), LogoutFilter.class)
                    .addFilterAfter(keycloakSecurityContextRequestFilter(), SecurityContextHolderAwareRequestFilter.class)
                    .addFilterAfter(keycloakAuthenticatedActionsRequestFilter(), KeycloakSecurityContextRequestFilter.class);
        }
    }

    @Bean
    protected SessionAuthenticationStrategy sessionAuthenticationStrategy() {
        return new RegisterSessionAuthenticationStrategy(new SessionRegistryImpl());
    }

    @Bean
    protected AdapterDeploymentContext adapterDeploymentContext() throws Exception {
        AdapterDeploymentContextFactoryBean factoryBean;
        String                              fileName = configuration.getString("atlas.authentication.method.keycloak.file");

        if (fileName != null && !fileName.isEmpty()) {
            keycloakConfigFileResource = new FileSystemResource(fileName);
            factoryBean                = new AdapterDeploymentContextFactoryBean(keycloakConfigFileResource);
        } else {
            Configuration conf = configuration.subset("atlas.authentication.method.keycloak");
            AdapterConfig cfg  = new AdapterConfig();

            cfg.setRealm(conf.getString("realm", "atlas.com"));
            cfg.setAuthServerUrl(conf.getString("auth-server-url", "https://localhost/auth"));
            cfg.setResource(conf.getString("resource", "none"));

            Map<String, Object> credentials = new HashMap<>();

            credentials.put("secret", conf.getString("credentials-secret", "nosecret"));

            cfg.setCredentials(credentials);

            KeycloakDeployment dep = KeycloakDeploymentBuilder.build(cfg);

            factoryBean = new AdapterDeploymentContextFactoryBean(request -> dep);
        }

        factoryBean.afterPropertiesSet();

        return factoryBean.getObject();
    }

    @Bean
    protected KeycloakPreAuthActionsFilter keycloakPreAuthActionsFilter() {
        return new KeycloakPreAuthActionsFilter(httpSessionManager());
    }

    @Bean
    protected HttpSessionManager httpSessionManager() {
        return new HttpSessionManager();
    }

    protected KeycloakLogoutHandler keycloakLogoutHandler() throws Exception {
        return new KeycloakLogoutHandler(adapterDeploymentContext());
    }

    @Bean
    protected KeycloakSecurityContextRequestFilter keycloakSecurityContextRequestFilter() {
        return new KeycloakSecurityContextRequestFilter();
    }

    @Bean
    protected KeycloakAuthenticatedActionsFilter keycloakAuthenticatedActionsRequestFilter() {
        return new KeycloakAuthenticatedActionsFilter();
    }

    @Bean
    protected KeycloakAuthenticationProcessingFilter keycloakAuthenticationProcessingFilter() throws Exception {
        KeycloakAuthenticationProcessingFilter filter = new KeycloakAuthenticationProcessingFilter(authenticationManagerBean(), KEYCLOAK_REQUEST_MATCHER);

        filter.setSessionAuthenticationStrategy(sessionAuthenticationStrategy());

        return filter;
    }
}
