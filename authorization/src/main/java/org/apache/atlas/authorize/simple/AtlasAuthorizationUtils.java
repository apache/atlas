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

import java.util.HashSet;
import java.util.Set;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlasAuthorizationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private static final String BASE_URL = "/" + AtlasClient.BASE_URI;

    public static String getApi(String contextPath) {
        if (isDebugEnabled) {
            LOG.debug("==> getApi from " + contextPath);
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
        if (split.length > 1) {
            return (!api.equals("v1")) ? api : String.format("v1/%s", split[1]);
        } else {
            return api;
        }
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
                    LOG.debug("getAtlasAction(): Invalid HTTP method '" + method + "'");
                }
                break;
        }

        if (isDebugEnabled) {
            LOG.debug("<== AtlasAuthorizationFilter getAtlasAction HTTP Method " + method + " mapped to AtlasAction : "
                + action);
        }
        return action;
    }

    /**
     * @param contextPath
     * @return set of AtlasResourceTypes types api mapped with AtlasResourceTypes.TYPE eg :- /api/atlas/types/*
     *
     *         gremlin discovery,admin,graph apis are mapped with AtlasResourceTypes.OPERATION eg :-/api/atlas/admin/*
     *         /api/atlas/discovery/search/gremlin /api/atlas/graph/*
     *
     *         entities,lineage and discovery apis are mapped with AtlasResourceTypes.ENTITY eg :- /api/atlas/lineage/hive/table/*
     *         /api/atlas/entities/{guid}* /api/atlas/discovery/*
     * 
     *         unprotected types are mapped with AtlasResourceTypes.UNKNOWN, access to these are allowed.
     */
    public static Set<AtlasResourceTypes> getAtlasResourceType(String contextPath) {
        Set<AtlasResourceTypes> resourceTypes = new HashSet<AtlasResourceTypes>();
        if (isDebugEnabled) {
            LOG.debug("==> getAtlasResourceType  for " + contextPath);
        }
        String api = getApi(contextPath);
        if (api.startsWith("types")) {
            resourceTypes.add(AtlasResourceTypes.TYPE);
        } else if ((api.startsWith("discovery") && contextPath.contains("/gremlin")) || api.startsWith("admin")
            || api.startsWith("graph")) {
            resourceTypes.add(AtlasResourceTypes.OPERATION);
        } else if (api.startsWith("entities") || api.startsWith("lineage") || api.startsWith("discovery")) {
            resourceTypes.add(AtlasResourceTypes.ENTITY);
        } else if (api.startsWith("v1/taxonomies")) {
            resourceTypes.add(AtlasResourceTypes.TAXONOMY);
            // taxonomies are modeled as entities
            resourceTypes.add(AtlasResourceTypes.ENTITY);
            if (contextPath.contains("/terms")) {
                resourceTypes.add(AtlasResourceTypes.TERM);
            }
        } else if (api.startsWith("v1/entities")) {
            resourceTypes.add(AtlasResourceTypes.ENTITY);
        } else {
            LOG.error("Unable to find Atlas Resource corresponding to : " + api + "\nSetting "
                + AtlasResourceTypes.UNKNOWN.name());
            resourceTypes.add(AtlasResourceTypes.UNKNOWN);
        }

        if (isDebugEnabled) {
            LOG.debug("<== Returning AtlasResources " + resourceTypes + " for api " + api);
        }
        return resourceTypes;
    }
}
