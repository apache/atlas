/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.servlets;

import org.apache.atlas.web.filters.AtlasResponseRequestWrapper;
import org.apache.atlas.web.filters.HeadersUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AtlasHttpServlet extends HttpServlet {
    public static final Logger LOG = LoggerFactory.getLogger(AtlasHttpServlet.class);

    public static final String TEXT_HTML = "text/html";
    public static final String ALLOW     = "ALLOW";

    protected void includeResponse(HttpServletRequest request, HttpServletResponse response, String template) {
        try {
            response.setContentType(TEXT_HTML);
            AtlasResponseRequestWrapper responseWrapper = new AtlasResponseRequestWrapper(response);
            HeadersUtil.setSecurityHeaders(responseWrapper);

            getServletContext()
                    .getRequestDispatcher(template)
                    .include(request, response);
        } catch (Exception e) {
            LOG.error("Failed to include template [{}] in AtlasHttpServlet", template, e);
        }
    }
}
