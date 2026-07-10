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
package org.apache.atlas.web.filters;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Lazy
@Component
public class AtlasJwtAuthWrapper extends GenericFilterBean {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJwtAuthWrapper.class);

    private String[] browserUserAgents = new String[] {""};

    private final Configuration configuration;

    @Lazy
    @Autowired
    AtlasJwtAuthFilter atlasJwtAuthFilter;

    @Inject
    public AtlasJwtAuthWrapper(Configuration configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public void initialize() {
        String defaultUserAgent = configuration.getString(AtlasKnoxSSOAuthenticationFilter.DEFAULT_BROWSER_USERAGENT);
        String userAgent        = configuration.getString(AtlasKnoxSSOAuthenticationFilter.BROWSER_USERAGENT);

        if (StringUtils.isBlank(userAgent) && StringUtils.isNotBlank(defaultUserAgent)) {
            userAgent = defaultUserAgent;
        }

        if (StringUtils.isNotBlank(userAgent)) {
            browserUserAgents = userAgent.split(",");
        }
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        LOG.debug("===>>> AtlasJwtAuthWrapper.doFilter({}, {}, {})", servletRequest, servletResponse, filterChain);

        Configuration configuration = null;
        try {
            configuration = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
        boolean isProxyEnabled      = configuration.getBoolean("atlas.authentication.method.trustedproxy", false);
        boolean useJwtAuthMechanism = servletRequest != null && !isRequestAuthenticated() && AtlasJwtAuthFilter.canAuthenticateRequest(servletRequest);
        boolean ssoEnabled          = configuration.getBoolean("atlas.sso.knox.enabled", false);

        if (!ssoEnabled && useJwtAuthMechanism && !isProxyEnabled) {
            atlasJwtAuthFilter.doFilter(servletRequest, servletResponse, filterChain);

            if (!isRequestAuthenticated()) {
                String userAgent = ((HttpServletRequest) servletRequest).getHeader("User-Agent");
                if (isBrowserAgent(userAgent)) {
                    LOG.debug("Redirecting to login page as request does not have valid JWT auth details.");
                    ((HttpServletResponse) servletResponse).sendRedirect("/login.jsp");
                }
            }
        } else {
            LOG.debug("<<<=== AtlasJwtAuthWrapper.doFilter() - Skipping JWT auth.");
        }
        filterChain.doFilter(servletRequest, servletResponse);

        LOG.debug("<<<=== AtlasJwtAuthWrapper.doFilter()");
    }

    protected boolean isBrowserAgent(String userAgent) {
        boolean isBrowser = false;

        if (browserUserAgents.length > 0 && StringUtils.isNotBlank(userAgent)) {
            for (String ua : browserUserAgents) {
                if (userAgent.toLowerCase().startsWith(ua.toLowerCase())) {
                    isBrowser = true;
                    break;
                }
            }
        }

        return isBrowser;
    }

    private boolean isRequestAuthenticated() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth != null && auth.isAuthenticated();
    }
}
