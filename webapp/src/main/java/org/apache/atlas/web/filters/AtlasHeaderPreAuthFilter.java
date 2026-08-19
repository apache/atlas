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
import org.apache.atlas.web.model.User;
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
import java.util.List;
import java.util.UUID;

@Component
public class AtlasHeaderPreAuthFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasHeaderPreAuthFilter.class);

    private static final String REQUEST_ID_ATTRIBUTE = "atlas.request.id";

    private AtlasHeaderAuthConfiguration headerAuthConfiguration;

    @Inject
    public AtlasHeaderPreAuthFilter() {
        loadConfiguration();
    }

    private void loadConfiguration() {
        try {
            headerAuthConfiguration = AtlasHeaderAuthConfiguration.load(ApplicationProperties.get());
        } catch (Exception e) {
            LOG.error("Error loading application properties for header pre-auth", e);
            headerAuthConfiguration = AtlasHeaderAuthConfiguration.load(null);
        }
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
            if (headerAuthConfiguration.isEnabled()) {
                Authentication pre = SecurityContextHolder.getContext().getAuthentication();

                if (pre == null || !pre.isAuthenticated()) {
                    String username = headerAuthConfiguration.readHeaderValue(httpRequest, AtlasHeaderAuthConfiguration.KEY_USERNAME);

                    if (username != null) {
                        String rolesHeaderValue = headerAuthConfiguration.readHeaderValue(httpRequest, AtlasHeaderAuthConfiguration.KEY_ROLES);
                        List<GrantedAuthority> grantedAuths = getAuthoritiesFromRolesHeader(rolesHeaderValue);

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
        if (auth instanceof AtlasAuthenticationToken &&
                ((AtlasAuthenticationToken) auth).getAuthType() == AtlasAuthenticationToken.AUTH_TYPE_TRUSTED_PROXY) {
            String requestId = headerAuthConfiguration.readHeaderValue(request, AtlasHeaderAuthConfiguration.KEY_REQUEST_ID);

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
