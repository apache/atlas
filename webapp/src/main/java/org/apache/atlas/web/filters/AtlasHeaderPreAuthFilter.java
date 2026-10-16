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
import org.apache.atlas.server.common.filters.AtlasResponseRequestWrapper;
import org.apache.atlas.server.common.filters.HeadersUtil;
import org.apache.atlas.server.common.model.User;
import org.apache.atlas.utils.SpiffeIdUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Component
public class AtlasHeaderPreAuthFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasHeaderPreAuthFilter.class);

    public static final String PROP_HEADER_AUTH_ENABLED = "atlas.authentication.method.header.enabled";
    public static final String PROP_USERNAME_HEADER     = "atlas.authentication.method.header.username";
    public static final String PROP_ROLES_HEADER        = "atlas.authentication.method.header.roles";
    public static final String PROP_REQUEST_ID_HEADER   = "atlas.authentication.method.header.request-id";
    public static final String PROP_SPIFFE_HEADER       = "atlas.authentication.method.header.spiffe";
    public static final String REQUEST_ID_ATTRIBUTE     = "atlas.request.id";

    private Configuration configuration;
    private boolean       headerAuthEnabled;
    private String        userNameHeaderName;
    private String        rolesHeaderName;
    private List<String>  spiffeHeaderNames;

    @Inject
    public AtlasHeaderPreAuthFilter() {
        loadConfiguration();
    }

    private void loadConfiguration() {
        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.error("Error loading application properties for header pre-auth", e);
            configuration = null;
        }
        if (configuration == null) {
            headerAuthEnabled  = false;
            userNameHeaderName = null;
            rolesHeaderName    = null;
            spiffeHeaderNames  = Collections.emptyList();
            return;
        }
        headerAuthEnabled  = configuration.getBoolean(PROP_HEADER_AUTH_ENABLED, false);
        userNameHeaderName = StringUtils.trimToNull(configuration.getString(PROP_USERNAME_HEADER, ""));
        rolesHeaderName    = StringUtils.trimToNull(configuration.getString(PROP_ROLES_HEADER, ""));
        spiffeHeaderNames  = SpiffeIdUtil.parseHeaderNames(configuration.getStringArray(PROP_SPIFFE_HEADER));

        if (headerAuthEnabled && userNameHeaderName == null && spiffeHeaderNames.isEmpty()) {
            LOG.warn("Disabling header-based authentication, as neither {} nor {} is set",
                    PROP_USERNAME_HEADER, PROP_SPIFFE_HEADER);
            headerAuthEnabled = false;
        }
    }

    /**
     * Resolves the principal from trusted headers. The username header (user identity) takes
     * precedence; when it is absent, configured SPIFFE headers (service identity) are consulted and
     * the trailing service-account segment of the SPIFFE ID becomes the principal.
     */
    private String resolvePrincipal(HttpServletRequest httpRequest) {
        String username = userNameHeaderName != null
                ? StringUtils.trimToNull(httpRequest.getHeader(userNameHeaderName))
                : null;

        if (username != null) {
            return username;
        }

        for (String spiffeHeaderName : spiffeHeaderNames) {
            String spiffeId       = StringUtils.trimToNull(httpRequest.getHeader(spiffeHeaderName));
            String serviceAccount = SpiffeIdUtil.extractServiceAccount(spiffeId);

            if (serviceAccount != null) {
                LOG.debug("Resolved service-account '{}' from SPIFFE header '{}'", serviceAccount, spiffeHeaderName);

                return serviceAccount;
            } else if (spiffeId != null) {
                LOG.warn("SPIFFE header '{}' value is not a well-formed SPIFFE ID", spiffeHeaderName);
            }
        }

        return null;
    }

    private List<GrantedAuthority> getAuthoritiesFromRolesHeader(String rolesHeader) {
        List<GrantedAuthority> ret = new ArrayList<>();
        if (StringUtils.isBlank(rolesHeader)) {
            return ret;
        }

        for (String role : rolesHeader.split(",")) {
            String trimmed = StringUtils.trimToNull(role);
            if (trimmed != null) {
                ret.add(new org.springframework.security.core.authority.SimpleGrantedAuthority(trimmed));
            }
        }

        return ret;
    }

    @Override
    public void init(FilterConfig filterConfig) {
        loadConfiguration();
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest  httpRequest  = (HttpServletRequest) servletRequest;
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        AtlasResponseRequestWrapper responseWrapper = new AtlasResponseRequestWrapper(httpResponse);
        HeadersUtil.setSecurityHeaders(responseWrapper);

        try {
            if (headerAuthEnabled) {
                Authentication pre = SecurityContextHolder.getContext().getAuthentication();

                if (pre == null || !pre.isAuthenticated()) {
                    String username = resolvePrincipal(httpRequest);

                    if (username != null) {
                        List<GrantedAuthority> grantedAuths =
                                getAuthoritiesFromRolesHeader(httpRequest.getHeader(rolesHeaderName));

                        UserDetails principal = new User(username, "", grantedAuths);

                        AtlasAuthenticationToken token =
                                new AtlasAuthenticationToken(principal, grantedAuths,
                                        AtlasAuthenticationToken.AUTH_TYPE_TRUSTED_PROXY);

                        token.setDetails(new WebAuthenticationDetails(httpRequest));

                        SecurityContextHolder.getContext().setAuthentication(token);
                    }
                }
            }

            Authentication auth      = SecurityContextHolder.getContext().getAuthentication();
            String         requestId = getRequestId(auth, httpRequest);
            httpRequest.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);

            chain.doFilter(servletRequest, responseWrapper);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServletException e) {
            throw new RuntimeException(e);
        }
    }

    private String getRequestId(Authentication auth, HttpServletRequest request) {
        String requestIdHeaderName = configuration != null ? configuration.getString(PROP_REQUEST_ID_HEADER) : null;

        if (requestIdHeaderName != null &&
                auth instanceof AtlasAuthenticationToken &&
                ((AtlasAuthenticationToken) auth).getAuthType() == AtlasAuthenticationToken.AUTH_TYPE_TRUSTED_PROXY) {
            String requestId = StringUtils.trimToNull(request.getHeader(requestIdHeaderName));
            if (requestId != null) {
                return requestId;
            }
        }

        return UUID.randomUUID().toString();
    }

    @Override
    public void destroy() {
    }
}
