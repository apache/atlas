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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.apache.atlas.authorize.SimpleAtlasAuthorizer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;
import static org.apache.atlas.authorize.AtlasAuthorizationUtils.*;

import com.google.common.base.Strings;

public class AtlasAuthorizationFilter extends GenericFilterBean {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationFilter.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private AtlasAuthorizer authorizer = SimpleAtlasAuthorizer.getInstance();

    private final String BASE_URL = "/" + AtlasClient.BASE_URI;

    public AtlasAuthorizationFilter() {
        if (isDebugEnabled) {
            LOG.debug("<== AtlasAuthorizationFilter() -- " + "Now initializing the Apache Atlas Authorizer!!!");
        }
        authorizer.init();
    }

    @Override
    public void destroy() {
        if (isDebugEnabled) {
            LOG.debug("<== AtlasAuthorizationFilter destroy");
        }
        authorizer.cleanUp();
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
        String pathInfo = request.getServletPath();
        if (pathInfo.startsWith(BASE_URL)) {
            if (isDebugEnabled) {
                LOG.debug(pathInfo + " is a valid REST API request!!!");
            }

            AtlasActionTypes action = getAtlasAction(request.getMethod());
            String userName = null;
            List<String> groups = new ArrayList<String>();
            StringBuilder sb = new StringBuilder();

            Authentication auth = SecurityContextHolder.getContext().getAuthentication();

            if (auth != null) {
                userName = String.valueOf(auth.getPrincipal());
                Collection<? extends GrantedAuthority> authorities = auth.getAuthorities();
                for (GrantedAuthority c : authorities) {
                    groups.add(c.getAuthority());
                }
                sb.append("============================\n");
                sb.append("UserName ==>> " + userName + "\nGroups ==>> " + groups);
            } else {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Cannot obtain Security Context : " + auth);
                }
                throw new ServletException("Cannot obtain Security Context : " + auth);
            }

            sb.append("\n" + "URL :: " + request.getRequestURL() + " Action :: " + action);
            sb.append("\nrequest.getServletPath() :: " + pathInfo);
            sb.append("\n============================\n");

            if (isDebugEnabled) {
                LOG.debug(sb.toString());
            }
            sb = null;
            List<AtlasResourceTypes> atlasResourceType = getAtlasResourceType(pathInfo);
            String resource = getAtlasResource(request, action);
            AtlasAccessRequest atlasRequest =
                new AtlasAccessRequest(atlasResourceType, resource, action, userName, groups);
            boolean accessAllowed = false;
            try {
                accessAllowed = authorizer.isAccessAllowed(atlasRequest);
            } catch (AtlasAuthorizationException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Access Restricted. Could not process the request due to : " + e);
                }
            }
            if (isDebugEnabled) {
                LOG.debug("Authorizer result :: " + accessAllowed);
            }
            if (accessAllowed) {
                if (isDebugEnabled) {
                    LOG.debug("Access is allowed so forwarding the request!!!");
                }
                chain.doFilter(req, res);
            } else {
                JSONObject json = new JSONObject();
                json.put("AuthorizationError", "Sorry you are not authorized for " + action.name() + " on "
                    + atlasResourceType + " : " + resource);
                HttpServletResponse response = (HttpServletResponse) res;
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);

                response.sendError(HttpServletResponse.SC_FORBIDDEN, json.toString());
                if (isDebugEnabled) {
                    LOG.debug("Sorry you are not authorized for " + action.name() + " on " + atlasResourceType + " : "
                        + resource);
                    LOG.debug("Returning 403 since the access is blocked update!!!!");
                }
                return;
            }

        } else {
            if (isDebugEnabled) {
                LOG.debug("Ignoring request " + pathInfo);
            }
            chain.doFilter(req, res);
        }
    }

}