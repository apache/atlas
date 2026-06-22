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
import org.apache.atlas.authn.handler.AtlasAuth;
import org.apache.atlas.authn.handler.jwt.AtlasDefaultJwtAuthHandler;
import org.apache.atlas.authn.handler.jwt.AtlasJwtAuthHandler;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Lazy
@Component
public class AtlasJwtAuthFilter extends AtlasDefaultJwtAuthHandler implements Filter {
    private static final Logger LOG                = LoggerFactory.getLogger(AtlasJwtAuthFilter.class);
    private static final String DEFAULT_ATLAS_ROLE = "ROLE_USER";

    @PostConstruct
    public void initialize() {
        LOG.debug("===>>> AtlasJwtAuthFilter.initialize()");

        try {
            Configuration config = ApplicationProperties.get();
            if (StringUtils.isEmpty(config.getString(AtlasJwtAuthHandler.KEY_PROVIDER_URL))) {
                config.setProperty(AtlasJwtAuthHandler.KEY_PROVIDER_URL,
                        config.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL));
            }
            config.setProperty(AtlasJwtAuthHandler.KEY_JWT_PUBLIC_KEY,
                    config.getString(AtlasKnoxSSOAuthenticationFilter.JWT_PUBLIC_KEY, ""));
            config.setProperty(AtlasJwtAuthHandler.KEY_JWT_COOKIE_NAME,
                    config.getString(AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME,
                            AtlasKnoxSSOAuthenticationFilter.JWT_COOKIE_NAME_DEFAULT));
            config.setProperty(AtlasJwtAuthHandler.KEY_JWT_AUDIENCES,
                    config.getString(AtlasKnoxSSOAuthenticationFilter.JWT_AUDIENCES, ""));

            super.initialize(config);
        } catch (Exception e) {
            LOG.error("Failed to initialize Atlas JWT Auth Filter.", e);
        }

        LOG.debug("<<<=== AtlasJwtAuthFilter.initialize()");
    }

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        LOG.debug("===>>> AtlasJwtAuthFilter.doFilter()");

        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        AtlasAuth atlasAuth                   = authenticate(httpServletRequest);

        if (atlasAuth != null) {
            final List<GrantedAuthority> grantedAuths = Arrays.asList(new SimpleGrantedAuthority(DEFAULT_ATLAS_ROLE));
            final UserDetails principal               = new User(atlasAuth.getUserName(), "", grantedAuths);
            final Authentication finalAuthentication  = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
            final WebAuthenticationDetails webDetails = new WebAuthenticationDetails(httpServletRequest);
            ((AbstractAuthenticationToken) finalAuthentication).setDetails(webDetails);
            SecurityContextHolder.getContext().setAuthentication(finalAuthentication);
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null) {
            LOG.debug("<<<=== AtlasJwtAuthFilter.doFilter() - user=[{}], isUserAuthenticated=[{}]",
                    auth.getPrincipal(), auth.isAuthenticated());
        } else {
            LOG.warn("<<<=== AtlasJwtAuthFilter.doFilter() - Failed to authenticate request using Atlas JWT authentication framework.");
        }
    }

    @Override
    public void destroy() {
    }
}
