/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.filters;

import com.google.common.base.Strings;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasAuthorizerFactory;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Component
public class AtlasAuthorizationFilter extends GenericFilterBean {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationFilter.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private AtlasAuthorizer authorizer = null;

    private final String BASE_URL = "/" + AtlasClient.BASE_URI;

    public AtlasAuthorizationFilter() {
        if (isDebugEnabled) {
            LOG.debug("==> AtlasAuthorizationFilter() -- " + "Now initializing the Apache Atlas Authorizer!!!");
        }

        try {
            authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
            if (authorizer != null) {
                authorizer.init();
            } else {
                LOG.warn("AtlasAuthorizer not initialized properly, please check the application logs and add proper configurations.");
            }
        } catch (AtlasAuthorizationException e) {
            LOG.error("Unable to obtain AtlasAuthorizer. ", e);
        }

    }

    @Override
    public void destroy() {
        if (isDebugEnabled) {
            LOG.debug("==> AtlasAuthorizationFilter destroy");
        }
        if (authorizer != null) {
            authorizer.cleanUp();
        }
        super.destroy();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException,
        ServletException {
        if (isDebugEnabled) {
            LOG.debug("==> AuthorizationFilter.doFilter");
        }

        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) res;
        AtlasResponseRequestWrapper responseWrapper = new AtlasResponseRequestWrapper(response);
        responseWrapper.setHeader("X-Frame-Options", "DENY");

        String pathInfo = request.getServletPath();
        if (!Strings.isNullOrEmpty(pathInfo) && (pathInfo.startsWith(BASE_URL) || BASE_URL.startsWith(pathInfo))) {
            if (isDebugEnabled) {
                LOG.debug("{} is a valid REST API request!!!", pathInfo);
            }

            String userName = null;
            Set<String> groups = new HashSet<>();

            Authentication auth = SecurityContextHolder.getContext().getAuthentication();

            if (auth != null) {
                userName = auth.getName();
                Collection<? extends GrantedAuthority> authorities = auth.getAuthorities();
                for (GrantedAuthority c : authorities) {
                    groups.add(c.getAuthority());
                }
            } else {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Cannot obtain Security Context");
                }
                throw new ServletException("Cannot obtain Security Context");
            }

            AtlasAccessRequest atlasRequest = new AtlasAccessRequest(request, userName, groups);
            if (isDebugEnabled) {
                LOG.debug("============================\nUserName :: {}\nGroups :: {}\nURL :: {}\nAction :: {}\nrequest.getServletPath() :: {}\n============================\n", atlasRequest.getUser(), atlasRequest.getUserGroups(), request.getRequestURL(), atlasRequest.getAction(), pathInfo);
            }

            boolean accessAllowed = false;

            Set<AtlasResourceTypes> atlasResourceTypes = atlasRequest.getResourceTypes();
            if (atlasResourceTypes.size() == 1 && atlasResourceTypes.contains(AtlasResourceTypes.UNKNOWN)) {
                // Allowing access to unprotected resource types
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Allowing access to unprotected resource types {}", atlasResourceTypes);
                }
                accessAllowed = true;
            } else {

                try {
                    if (authorizer != null) {
                        accessAllowed = authorizer.isAccessAllowed(atlasRequest);
                    }
                } catch (AtlasAuthorizationException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Access Restricted. Could not process the request :: {}", e);
                    }
                }
                if (isDebugEnabled) {
                    LOG.debug("Authorizer result :: {}", accessAllowed);
                }
            }

            if (accessAllowed) {
                if (isDebugEnabled) {
                    LOG.debug("Access is allowed so forwarding the request!!!");
                }
                chain.doFilter(req, res);
            } else {
                JSONObject json = new JSONObject();
                json.put("AuthorizationError", "You are not authorized for " + atlasRequest.getAction().name() + " on "
                    + atlasResourceTypes + " : " + atlasRequest.getResource());

                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);

                response.sendError(HttpServletResponse.SC_FORBIDDEN, json.toString());
                if (isDebugEnabled) {
                    LOG.debug("You are not authorized for {} on {} : {}\nReturning 403 since the access is blocked update!!!!", atlasRequest.getAction().name(), atlasResourceTypes, atlasRequest.getResource());
                }
            }

        } else {
            if (isDebugEnabled) {
                LOG.debug("Ignoring request {}", pathInfo);
            }
            chain.doFilter(req, res);
        }
    }

}