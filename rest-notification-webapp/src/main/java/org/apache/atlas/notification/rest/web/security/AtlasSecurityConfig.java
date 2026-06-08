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
package org.apache.atlas.notification.rest.web.security;

import org.apache.atlas.server.common.security.AtlasAuthenticationFailureHandler;
import org.apache.atlas.server.common.security.AtlasAuthenticationProvider;
import org.apache.atlas.server.common.security.AtlasAuthenticationSuccessHandler;
import org.apache.commons.configuration.Configuration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import javax.inject.Inject;

/**
 * Spring Security configuration for the Atlas rest-notification-webapp.
 *
 * Why this class was created
 * Before this commit, {@code server-common/AtlasSecurityConfig} was a concrete
 * {@code @EnableWebSecurity} class. It also injected
 * {@code @Qualifier("staleTransactionCleanupFilter")} — a bean that only exists
 * in the webapp Spring context. When rest-notification-webapp started, Spring
 * threw {@code NoSuchBeanDefinitionException} because no such bean was registered,
 * preventing the Kerberos authentication filter chain from initializing.
 *
 * This class was created to be the SINGLE {@code @EnableWebSecurity} authority
 * for rest-notification-webapp — completely separate from webapp's security config.
 *
 * What is intentionally ABSENT here (compared to webapp)
 *  NO {@code staleTransactionCleanupFilter} — rest-notification makes no direct
 *  JanusGraph writes and has no such bean in its context
 *  NO form login ({@code addWebUiFormLogin}) — rest-notification is a headless
 *  REST server with no browser UI, login page, or interactive sessions.
 */
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class AtlasSecurityConfig extends org.apache.atlas.server.common.security.AtlasSecurityConfig {
    @Inject
    public AtlasSecurityConfig(ObjectProvider<org.apache.atlas.server.common.filters.AtlasKnoxSSOAuthenticationFilter> ssoAuthenticationFilterProvider,
                               ObjectProvider<org.apache.atlas.server.common.filters.AtlasCSRFPreventionFilter> csrfPreventionFilterProvider,
                               ObjectProvider<org.apache.atlas.server.common.filters.AtlasAuthenticationFilter> atlasAuthenticationFilterProvider,
                               AtlasAuthenticationProvider authenticationProvider,
                               AtlasAuthenticationSuccessHandler successHandler,
                               AtlasAuthenticationFailureHandler failureHandler,
                               org.apache.atlas.server.common.filters.AtlasAuthenticationEntryPoint atlasAuthenticationEntryPoint,
                               Configuration configuration,
                               ObjectProvider<org.apache.atlas.server.common.filters.ActiveServerFilter> activeServerFilterProvider) {
        super(ssoAuthenticationFilterProvider, csrfPreventionFilterProvider, atlasAuthenticationFilterProvider,
                authenticationProvider, successHandler, failureHandler, atlasAuthenticationEntryPoint, configuration,
                activeServerFilterProvider);
    }

    @Override
    public void configure(WebSecurity web) {
        web.ignoring()
                .antMatchers("/login.jsp",
                        "/css/**",
                        "/n/css/**",
                        "/img/**",
                        "/n/img/**",
                        "/libs/**",
                        "/n/libs/**",
                        "/js/**",
                        "/n/js/**",
                        "/ieerror.html",
                        "/migration-status.html",
                        "/rest/api/atlas/admin/status");
    }

    @Override
    protected void configure(HttpSecurity httpSecurity) throws Exception {
        configureCommonHttpSecurity(httpSecurity);
        addHaAndMigrationGuards(httpSecurity);
        addCommonAuthFilters(httpSecurity);
    }
}
