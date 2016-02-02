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

package org.apache.atlas.web.util;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility functions for dealing with servlets.
 */
public final class Servlets {

    private static final Logger LOG = LoggerFactory.getLogger(Servlets.class);

    private Servlets() {
        /* singleton */
    }

    public static final String JSON_MEDIA_TYPE = MediaType.APPLICATION_JSON + "; charset=UTF-8";

    /**
     * Returns the user of the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the user
     */
    public static String getUserFromRequest(HttpServletRequest httpRequest) {
        String user = httpRequest.getRemoteUser();
        if (!StringUtils.isEmpty(user)) {
            return user;
        }

        user = httpRequest.getParameter("user.name"); // available in query-param
        if (!StringUtils.isEmpty(user)) {
            return user;
        }

        user = httpRequest.getHeader("Remote-User"); // backwards-compatibility
        if (!StringUtils.isEmpty(user)) {
            return user;
        }

        return null;
    }

    /**
     * Returns the URI of the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the URI, including the query string
     */
    public static String getRequestURI(HttpServletRequest httpRequest) {
        final StringBuilder url = new StringBuilder(100).append(httpRequest.getRequestURI());
        if (httpRequest.getQueryString() != null) {
            url.append('?').append(httpRequest.getQueryString());
        }

        return url.toString();
    }

    /**
     * Returns the full URL of the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the full URL, including the query string
     */
    public static String getRequestURL(HttpServletRequest httpRequest) {
        final StringBuilder url = new StringBuilder(100).append(httpRequest.getRequestURL());
        if (httpRequest.getQueryString() != null) {
            url.append('?').append(httpRequest.getQueryString());
        }

        return url.toString();
    }

    public static Response getErrorResponse(Throwable e, Response.Status status) {
        String message = e.getMessage() == null ? "Failed with " + e.getClass().getName() : e.getMessage();
        Response response = getErrorResponse(message, status);
        JSONObject responseJson = (JSONObject) response.getEntity();
        try {
            responseJson.put(AtlasClient.STACKTRACE, printStackTrace(e));
        } catch (JSONException e1) {
            LOG.warn("Could not construct error Json rensponse", e1);
        }
        return response;
    }

    private static String printStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static Response getErrorResponse(String message, Response.Status status) {
        JSONObject errorJson = new JSONObject();
        Object errorEntity = escapeJsonString(message);
        try {
            errorJson.put(AtlasClient.ERROR, errorEntity);
            errorEntity = errorJson;
        } catch (JSONException jsonE) {
            LOG.warn("Could not construct error Json rensponse", jsonE);
        }
        return Response.status(status).entity(errorEntity).type(JSON_MEDIA_TYPE).build();
    }

    public static String getRequestPayload(HttpServletRequest request) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(request.getInputStream(), writer);
        return writer.toString();
    }

    public static String getRequestId() {
        return Thread.currentThread().getName();
    }

    public static String escapeJsonString(String inputStr) {
        ParamChecker.notNull(inputStr, "Input String cannot be null");
        return StringEscapeUtils.escapeJson(inputStr);
    }
}
