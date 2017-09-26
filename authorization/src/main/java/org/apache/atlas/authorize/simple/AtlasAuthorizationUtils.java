/**
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

package org.apache.atlas.authorize.simple;

import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasAuthorizerFactory;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class AtlasAuthorizationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private static final String BASE_URL = "/api/atlas/";

    public static String getApi(String contextPath) {
        if (isDebugEnabled) {
            LOG.debug("==> getApi({})", contextPath);
        }

        if(contextPath == null){
            contextPath = "";
        }

        if (contextPath.startsWith(BASE_URL)) {
            contextPath = contextPath.substring(BASE_URL.length());
        } else {
            // strip of leading '/'
            if (contextPath.startsWith("/")) {
                contextPath = contextPath.substring(1);
            }
        }
        String[] split = contextPath.split("/", 3);

        String api = split[0];
        if (Pattern.matches("v\\d", api)) {
            api = split[1];
        }

        if (isDebugEnabled) {
            LOG.debug("<== getApi({}): {}", contextPath, api);
        }

        return api;
    }

    public static AtlasActionTypes getAtlasAction(String method) {
        AtlasActionTypes action = null;

        switch (method.toUpperCase()) {
            case "POST":
                action = AtlasActionTypes.CREATE;
                break;
            case "GET":
                action = AtlasActionTypes.READ;
                break;
            case "PUT":
                action = AtlasActionTypes.UPDATE;
                break;
            case "DELETE":
                action = AtlasActionTypes.DELETE;
                break;
            default:
                if (isDebugEnabled) {
                    LOG.debug("getAtlasAction(): Invalid HTTP method '{}", method);
                }
                break;
        }

        if (isDebugEnabled) {
            LOG.debug("<== AtlasAuthorizationFilter getAtlasAction HTTP Method {} mapped to AtlasAction : {}",
                    method, action);
        }
        return action;
    }

    /**
     * @param contextPath
     * @return set of AtlasResourceTypes types api mapped with AtlasResourceTypes.TYPE eg :- /api/atlas/types/*
     *
     * gremlin discovery,admin,graph apis are mapped with AtlasResourceTypes.OPERATION eg :-/api/atlas/admin/*
     * /api/atlas/discovery/search/gremlin /api/atlas/graph/*
     *
     * entities,lineage and discovery apis are mapped with AtlasResourceTypes.ENTITY eg :- /api/atlas/lineage/hive/table/*
     * /api/atlas/entities/{guid}* /api/atlas/discovery/*
     *
     * taxonomy API are also mapped to AtlasResourceTypes.TAXONOMY & AtlasResourceTypes.ENTITY and its terms APIs have
     * added AtlasResourceTypes.TERM associations.
     *
     * unprotected types are mapped with AtlasResourceTypes.UNKNOWN, access to these are allowed.
     */
    public static Set<AtlasResourceTypes> getAtlasResourceType(String contextPath) {
        Set<AtlasResourceTypes> resourceTypes = new HashSet<>();
        if (isDebugEnabled) {
            LOG.debug("==> getAtlasResourceType  for {}", contextPath);
        }
        String api = getApi(contextPath);
        if (api.startsWith("types")) {
            resourceTypes.add(AtlasResourceTypes.TYPE);
        } else if (api.startsWith("admin") && (contextPath.contains("/session") || contextPath.contains("/version"))) {
            resourceTypes.add(AtlasResourceTypes.UNKNOWN);
        } else if ((api.startsWith("discovery") && contextPath.contains("/gremlin")) || api.startsWith("admin")
                || api.startsWith("graph")) {
            resourceTypes.add(AtlasResourceTypes.OPERATION);
        } else if (api.startsWith("entities") || api.startsWith("lineage") ||
                api.startsWith("discovery") || api.startsWith("entity") || api.startsWith("search")) {
            resourceTypes.add(AtlasResourceTypes.ENTITY);
        } else if (api.startsWith("taxonomies")) {
            resourceTypes.add(AtlasResourceTypes.TAXONOMY);
            // taxonomies are modeled as entities
            resourceTypes.add(AtlasResourceTypes.ENTITY);
            if (contextPath.contains("/terms")) {
                resourceTypes.add(AtlasResourceTypes.TERM);
            }
        } else {
            LOG.error("Unable to find Atlas Resource corresponding to : {}\nSetting {}"
                    , api, AtlasResourceTypes.UNKNOWN.name());
            resourceTypes.add(AtlasResourceTypes.UNKNOWN);
        }

        if (isDebugEnabled) {
            LOG.debug("<== Returning AtlasResources {} for api {}", resourceTypes, api);
        }
        return resourceTypes;
    }

    public static boolean isAccessAllowed(AtlasResourceTypes resourcetype, AtlasActionTypes actionType, String userName, Set<String> groups, HttpServletRequest request) {
        AtlasAuthorizer authorizer = null;
        boolean isaccessAllowed = false;

        Set<AtlasResourceTypes> resourceTypes = new HashSet<>();
        resourceTypes.add(resourcetype);
        AtlasAccessRequest atlasRequest = new AtlasAccessRequest(resourceTypes, "*", actionType, userName, groups, AtlasAuthorizationUtils.getRequestIpAddress(request));
        try {
            authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
            if (authorizer != null) {
                isaccessAllowed = authorizer.isAccessAllowed(atlasRequest);
            }
        } catch (AtlasAuthorizationException e) {
            LOG.error("Unable to obtain AtlasAuthorizer. ", e);
        }

        return isaccessAllowed;
    }

    public static String getRequestIpAddress(HttpServletRequest httpServletRequest) {
        try {
            InetAddress inetAddr = InetAddress.getByName(httpServletRequest.getRemoteAddr());

            String ip = inetAddr.getHostAddress();

            return ip;
        } catch (UnknownHostException ex) {
            LOG.error("Error occured when retrieving IP address", ex);
            return "";
        }
    }
}
