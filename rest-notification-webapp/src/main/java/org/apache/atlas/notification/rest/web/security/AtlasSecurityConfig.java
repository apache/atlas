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

import org.apache.atlas.notification.rest.web.filters.ActiveServerFilter;
import org.apache.atlas.notification.rest.web.filters.AtlasAuthenticationEntryPoint;
import org.apache.atlas.notification.rest.web.filters.AtlasAuthenticationFilter;
import org.apache.atlas.notification.rest.web.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.notification.rest.web.filters.AtlasDelegatingAuthenticationEntryPoint;
import org.apache.atlas.notification.rest.web.filters.AtlasKnoxSSOAuthenticationFilter;
import org.apache.atlas.notification.rest.web.filters.HeadersUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.DelegatingAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;
import org.springframework.security.web.util.matcher.RequestHeaderRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

import javax.inject.Inject;
import java.util.LinkedHashMap;

@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class AtlasSecurityConfig extends WebSecurityConfigurerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasSecurityConfig.class);

    private final AtlasAuthenticationProvider       authenticationProvider;
    private final AtlasAuthenticationSuccessHandler successHandler;
    private final AtlasAuthenticationFailureHandler failureHandler;
    private final AtlasKnoxSSOAuthenticationFilter  ssoAuthenticationFilter;
    private final AtlasAuthenticationFilter         atlasAuthenticationFilter;
    private final AtlasCSRFPreventionFilter         csrfPreventionFilter;
    private final AtlasAuthenticationEntryPoint     atlasAuthenticationEntryPoint;

    // Our own Atlas filters need to be registered as well
    private final Configuration      configuration;
    private final ActiveServerFilter activeServerFilter;

    @Inject
    public AtlasSecurityConfig(AtlasKnoxSSOAuthenticationFilter ssoAuthenticationFilter,
                               AtlasCSRFPreventionFilter atlasCSRFPreventionFilter,
                               AtlasAuthenticationFilter atlasAuthenticationFilter,
                               AtlasAuthenticationProvider authenticationProvider,
                               AtlasAuthenticationSuccessHandler successHandler,
                               AtlasAuthenticationFailureHandler failureHandler,
                               AtlasAuthenticationEntryPoint atlasAuthenticationEntryPoint,
                               Configuration configuration,
                               ActiveServerFilter activeServerFilter)

    {
        this.ssoAuthenticationFilter = ssoAuthenticationFilter;
        this.csrfPreventionFilter = atlasCSRFPreventionFilter;
        this.atlasAuthenticationFilter = atlasAuthenticationFilter;
        this.authenticationProvider = authenticationProvider;
        this.successHandler = successHandler;
        this.failureHandler = failureHandler;
        this.atlasAuthenticationEntryPoint = atlasAuthenticationEntryPoint;
        this.configuration = configuration;
        this.activeServerFilter = activeServerFilter;
    }

    public AuthenticationEntryPoint getAuthenticationEntryPoint() {
        LinkedHashMap<RequestMatcher, AuthenticationEntryPoint> entryPointMap = new LinkedHashMap<>();
        entryPointMap.put(new RequestHeaderRequestMatcher(HeadersUtil.USER_AGENT_KEY, HeadersUtil.USER_AGENT_VALUE), atlasAuthenticationEntryPoint);
        AtlasDelegatingAuthenticationEntryPoint basicAuthenticationEntryPoint = new AtlasDelegatingAuthenticationEntryPoint(entryPointMap);
        return basicAuthenticationEntryPoint;
    }

    public DelegatingAuthenticationEntryPoint getDelegatingAuthenticationEntryPoint() {
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

    @Override
    public void configure(WebSecurity web) throws Exception {
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
                        "/api/atlas/admin/status");
    }

    protected void configure(HttpSecurity httpSecurity) throws Exception {

        //@formatter:off
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
                .newSession()
                .and()
                .httpBasic()
                .authenticationEntryPoint(getDelegatingAuthenticationEntryPoint());


        if (configuration.getBoolean("atlas.server.ha.enabled", false)) {
            LOG.info("Atlas is in HA Mode, enabling ActiveServerFilter");
            httpSecurity.addFilterAfter(activeServerFilter, BasicAuthenticationFilter.class);
        }
        httpSecurity
                .addFilterBefore(ssoAuthenticationFilter, BasicAuthenticationFilter.class)
                .addFilterAfter(atlasAuthenticationFilter, SecurityContextHolderAwareRequestFilter.class)
                .addFilterAfter(csrfPreventionFilter, AtlasAuthenticationFilter.class);
    }
}
