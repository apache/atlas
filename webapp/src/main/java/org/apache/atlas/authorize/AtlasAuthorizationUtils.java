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

package org.apache.atlas.authorize;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.atlas.AtlasClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class AtlasAuthorizationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private static final String BASE_URL = "/" + AtlasClient.BASE_URI;

    public static String parse(String fullPath, String subPath) {
        String api = null;
        if (!Strings.isNullOrEmpty(fullPath)) {
            api = fullPath.substring(subPath.length(), fullPath.length());

        }
        if (isDebugEnabled) {
            LOG.debug("Extracted " + api + " from path : " + fullPath);
        }
        return api;
    }

    public static String getApi(String u) {
        if (isDebugEnabled) {
            LOG.debug("getApi <=== from " + u);
        }
        if (u.startsWith(BASE_URL)) {
            u = parse(u, BASE_URL);
        } else {
            // strip of leading '/'
            u = u.substring(1);
        }
        String[] split = u.split("/");
        String api = split[0];
        return (! api.equals("v1")) ? api : String.format("v1/%s", split[1]);
    }

    public static AtlasActionTypes getAtlasAction(String method) {
        AtlasActionTypes action = null;

        switch (method.toUpperCase()) {
            case "POST":
                action = AtlasActionTypes.WRITE;
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
                    LOG.debug("Invalid HTTP method in request : " + method + " this is serious!!!");
                }
                break;
        }

        if (isDebugEnabled) {
            LOG.debug("==> AtlasAuthorizationFilter getAtlasAction HTTP Method " + method + " mapped to AtlasAction : "
                + action);
        }
        return action;
    }

    public static List<AtlasResourceTypes> getAtlasResourceType(String contextPath) throws ServletException {
        List<AtlasResourceTypes> resourceTypes = new ArrayList<AtlasResourceTypes>();
        if (isDebugEnabled) {
            LOG.debug("getAtlasResourceType <=== for " + contextPath);
        }
        String api = getApi(contextPath);

        if (api.startsWith("types")) {
            resourceTypes.add(AtlasResourceTypes.TYPE);
        } else if ((api.startsWith("discovery") && contextPath.contains("gremlin")) || api.startsWith("admin")
            || api.startsWith("graph")) {
            resourceTypes.add(AtlasResourceTypes.OPERATION);
        } else if ((api.startsWith("entities") && contextPath.contains("traits")) || api.startsWith("discovery")) {
            resourceTypes.add(AtlasResourceTypes.ENTITY);
            resourceTypes.add(AtlasResourceTypes.TYPE);
        } else if (api.startsWith("entities") || api.startsWith("lineage")) {
            resourceTypes.add(AtlasResourceTypes.ENTITY);
        } else if (api.startsWith("v1/taxonomies")) {
            resourceTypes.add(AtlasResourceTypes.TAXONOMY);
            // taxonomies are modeled as entities
            resourceTypes.add(AtlasResourceTypes.ENTITY);
            if (contextPath.contains("terms")) {
                resourceTypes.add(AtlasResourceTypes.TERM);
                // terms are modeled as traits
                resourceTypes.add(AtlasResourceTypes.TYPE);
            }
        } else if (api.startsWith("v1/entities")) {
            resourceTypes.add(AtlasResourceTypes.ENTITY);
            if (contextPath.contains("tags")) {
                // tags are modeled as traits
                resourceTypes.add(AtlasResourceTypes.TYPE);
            }
        } else {
            LOG.error("Unable to find Atlas Resource corresponding to : " + api);
            throw new ServletException("Unable to find Atlas Resource corresponding to : " + api);
        }

        if (isDebugEnabled) {
            LOG.debug("Returning AtlasResources " + resourceTypes + " for api " + api);
        }
        return resourceTypes;
    }

    /*
     * This implementation will be changed for Resource level Authorization.
     */
    public static String getAtlasResource(HttpServletRequest requeset, AtlasActionTypes action) {
        if (isDebugEnabled) {
            LOG.debug("getAtlasResource <=== "
                + "This implementation will be changed for Resource level Authorization.");
        }
        return "*";
    }

}
