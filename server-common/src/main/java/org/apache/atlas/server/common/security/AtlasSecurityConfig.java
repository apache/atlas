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
package org.apache.atlas.server.common.security;

import org.apache.atlas.server.common.filters.ActiveServerFilter;
import org.apache.atlas.server.common.filters.AtlasAuthenticationEntryPoint;
import org.apache.atlas.server.common.filters.AtlasAuthenticationFilter;
import org.apache.atlas.server.common.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.server.common.filters.AtlasDelegatingAuthenticationEntryPoint;
import org.apache.atlas.server.common.filters.AtlasKnoxSSOAuthenticationFilter;
import org.apache.atlas.server.common.filters.HeadersUtil;
import org.apache.atlas.server.common.filters.spi.ActiveInstanceStateProvider;
import org.apache.atlas.server.common.filters.spi.AtlasAuthenticationProviderBridge;
import org.apache.atlas.server.common.filters.spi.ServiceStateProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.DelegatingAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;
import org.springframework.security.web.util.matcher.RequestHeaderRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

import javax.inject.Inject;

import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.atlas.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;

/**
 * Abstract base class for Atlas Spring Security configuration, shared between
 * {@code webapp} and {@code rest-notification-webapp}.
 *
 * Why this class is abstract
 * This class was previously a concrete {@code @EnableWebSecurity} class.
 * Having {@code @EnableWebSecurity} on a class in {@code server-common} caused
 * Spring to find TWO competing security configurations on the classpath when
 * either webapp started — this concrete base class AND the module-specific
 * subclass (if it existed) — causing a bean registration conflict that prevented
 * the Kerberos filter chain from assembling correctly.
 *
 * Making this class {@code abstract} ensures Spring Security NEVER tries to
 * instantiate it directly. Only the concrete subclasses in each module carry
 * the {@code @EnableWebSecurity} annotation and are registered as the security
 * configuration bean.
 */
public abstract class AtlasSecurityConfig extends WebSecurityConfigurerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasSecurityConfig.class);

    protected final AtlasAuthenticationProvider                     authenticationProvider;
    protected final AtlasAuthenticationSuccessHandler               successHandler;
    protected final AtlasAuthenticationFailureHandler               failureHandler;
    protected final ObjectProvider<AtlasKnoxSSOAuthenticationFilter> ssoAuthenticationFilterProvider;
    protected final ObjectProvider<AtlasAuthenticationFilter>         atlasAuthenticationFilterProvider;
    protected final ObjectProvider<AtlasCSRFPreventionFilter>         csrfPreventionFilterProvider;
    protected final AtlasAuthenticationEntryPoint                   atlasAuthenticationEntryPoint;
    protected final Configuration                                   configuration;
    protected final ObjectProvider<ActiveServerFilter>             activeServerFilterProvider;

    @Inject
    public AtlasSecurityConfig(ObjectProvider<AtlasKnoxSSOAuthenticationFilter> ssoAuthenticationFilterProvider,
                               ObjectProvider<AtlasCSRFPreventionFilter> csrfPreventionFilterProvider,
                               ObjectProvider<AtlasAuthenticationFilter> atlasAuthenticationFilterProvider,
                               AtlasAuthenticationProvider authenticationProvider,
                               AtlasAuthenticationSuccessHandler successHandler,
                               AtlasAuthenticationFailureHandler failureHandler,
                               AtlasAuthenticationEntryPoint atlasAuthenticationEntryPoint,
                               Configuration configuration,
                               ObjectProvider<ActiveServerFilter> activeServerFilterProvider) {
        this.ssoAuthenticationFilterProvider       = ssoAuthenticationFilterProvider;
        this.csrfPreventionFilterProvider          = csrfPreventionFilterProvider;
        this.atlasAuthenticationFilterProvider     = atlasAuthenticationFilterProvider;
        this.authenticationProvider                = authenticationProvider;
        this.successHandler                        = successHandler;
        this.failureHandler                        = failureHandler;
        this.atlasAuthenticationEntryPoint         = atlasAuthenticationEntryPoint;
        this.configuration                         = configuration;
        this.activeServerFilterProvider            = activeServerFilterProvider;
    }

    public AuthenticationEntryPoint getAuthenticationEntryPoint() throws Exception {
        LinkedHashMap<RequestMatcher, AuthenticationEntryPoint> entryPointMap = new LinkedHashMap<>();
        entryPointMap.put(new RequestHeaderRequestMatcher(HeadersUtil.USER_AGENT_KEY, HeadersUtil.USER_AGENT_VALUE), atlasAuthenticationEntryPoint);
        return new AtlasDelegatingAuthenticationEntryPoint(entryPointMap);
    }

    public DelegatingAuthenticationEntryPoint getDelegatingAuthenticationEntryPoint() throws Exception {
        LinkedHashMap<RequestMatcher, AuthenticationEntryPoint> entryPointMap = new LinkedHashMap<>();
        entryPointMap.put(new RequestHeaderRequestMatcher("User-Agent", "Mozilla"), atlasAuthenticationEntryPoint);
        DelegatingAuthenticationEntryPoint entryPoint = new DelegatingAuthenticationEntryPoint(entryPointMap);
        entryPoint.setDefaultEntryPoint(getAuthenticationEntryPoint());
        return entryPoint;
    }

    @Inject
    protected void configure(AuthenticationManagerBuilder authenticationManagerBuilder) {
        authenticationManagerBuilder.authenticationProvider(authenticationProvider);
    }

    protected void configureCommonHttpSecurity(HttpSecurity httpSecurity) throws Exception {
        httpSecurity
                .authorizeRequests().anyRequest().authenticated()
                .and()
                .headers()
                .addHeaderWriter(new StaticHeadersWriter(HeadersUtil.CONTENT_SEC_POLICY_KEY, HeadersUtil.getHeaderMap(HeadersUtil.CONTENT_SEC_POLICY_KEY)))
                .addHeaderWriter(new StaticHeadersWriter(HeadersUtil.SERVER_KEY, HeadersUtil.getHeaderMap(HeadersUtil.SERVER_KEY)))
                .and()
                .servletApi()
                .and()
                .csrf().disable()
                .sessionManagement()
                .enableSessionUrlRewriting(false)
                .sessionCreationPolicy(SessionCreationPolicy.ALWAYS)
                .sessionFixation()
                .newSession();

        httpSecurity
                .httpBasic()
                .authenticationEntryPoint(getDelegatingAuthenticationEntryPoint());
    }

    protected void addWebUiFormLogin(HttpSecurity httpSecurity) throws Exception {
        httpSecurity
                .formLogin()
                .loginPage("/login.jsp")
                .loginProcessingUrl("/j_spring_security_check")
                .successHandler(successHandler)
                .failureHandler(failureHandler)
                .usernameParameter("j_username")
                .passwordParameter("j_password")
                .and()
                .logout()
                .logoutSuccessUrl("/login.jsp")
                .deleteCookies("ATLASSESSIONID")
                .logoutUrl("/logout.html");
    }

    protected void addHaAndMigrationGuards(HttpSecurity httpSecurity) throws Exception {
        boolean configMigrationEnabled = !StringUtils.isEmpty(configuration.getString(ATLAS_MIGRATION_MODE_FILENAME));
        if (configuration.getBoolean("atlas.server.ha.enabled", false) || configMigrationEnabled) {
            if (configMigrationEnabled) {
                LOG.info("Atlas is in Migration Mode, enabling ActiveServerFilter");
            } else {
                LOG.info("Atlas is in HA Mode, enabling ActiveServerFilter");
            }
            ActiveServerFilter activeServerFilter = activeServerFilterProvider.getIfAvailable();
            if (activeServerFilter != null) {
                httpSecurity.addFilterAfter(activeServerFilter, BasicAuthenticationFilter.class);
            }
        }
    }

    protected void addCommonAuthFilters(HttpSecurity httpSecurity) throws Exception {
        AtlasKnoxSSOAuthenticationFilter ssoAuthenticationFilter = ssoAuthenticationFilterProvider.getIfAvailable();
        if (ssoAuthenticationFilter != null) {
            httpSecurity.addFilterBefore(ssoAuthenticationFilter, BasicAuthenticationFilter.class);
        }

        AtlasAuthenticationFilter atlasAuthenticationFilter = atlasAuthenticationFilterProvider.getIfAvailable();
        if (atlasAuthenticationFilter != null) {
            httpSecurity.addFilterAfter(atlasAuthenticationFilter, SecurityContextHolderAwareRequestFilter.class);
        }

        AtlasCSRFPreventionFilter csrfPreventionFilter = csrfPreventionFilterProvider.getIfAvailable();
        if (csrfPreventionFilter != null && atlasAuthenticationFilter != null) {
            httpSecurity.addFilterAfter(csrfPreventionFilter, AtlasAuthenticationFilter.class);
        }
    }

    @Bean
    public AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge(AtlasAuthenticationProvider authenticationProvider) {
        return new AtlasAuthenticationProviderBridge() {
            @Override
            public List<GrantedAuthority> getAuthoritiesFromUGI(String userName) {
                return AtlasAuthenticationProvider.getAuthoritiesFromUGI(userName);
            }

            @Override
            public void setSsoEnabled(boolean enabled) {
                authenticationProvider.setSsoEnabled(enabled);
            }

            @Override
            public Authentication authenticate(Authentication authentication) {
                return authenticationProvider.authenticate(authentication);
            }
        };
    }

    @Bean
    public ActiveServerFilter activeServerFilter(ActiveInstanceStateProvider activeInstanceStateProvider,
                                                       ServiceStateProvider serviceStateProvider) {
        return new ActiveServerFilter(activeInstanceStateProvider, serviceStateProvider);
    }

    @Bean
    public AtlasAuthenticationFilter atlasAuthenticationFilter(AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge) {
        return new AtlasAuthenticationFilter(atlasAuthenticationProviderBridge);
    }

    @Bean
    public AtlasKnoxSSOAuthenticationFilter atlasKnoxSSOAuthenticationFilter(AtlasAuthenticationProviderBridge atlasAuthenticationProviderBridge) {
        return new AtlasKnoxSSOAuthenticationFilter(atlasAuthenticationProviderBridge);
    }

    @Bean
    public AtlasCSRFPreventionFilter atlasCSRFPreventionFilter() {
        return new AtlasCSRFPreventionFilter();
    }
}
