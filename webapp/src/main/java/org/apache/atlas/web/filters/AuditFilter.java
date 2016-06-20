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

package org.apache.atlas.web.filters;

import com.google.inject.Singleton;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.RequestContext;
import org.apache.atlas.web.util.DateTimeHelper;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * This records audit information as part of the filter after processing the request
 * and also introduces a UUID into request and response for tracing requests in logs.
 */
@Singleton
public class AuditFilter implements Filter {

    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("AUDIT");
    private static final Logger LOG = LoggerFactory.getLogger(AuditFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("AuditFilter initialization started");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
    throws IOException, ServletException {
        final String requestTimeISO9601 = DateTimeHelper.formatDateUTC(new Date());
        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final String requestId = UUID.randomUUID().toString();
        final Thread currentThread = Thread.currentThread();
        final String oldName = currentThread.getName();
        String user = getUserFromRequest(httpRequest);

        try {
            currentThread.setName(formatName(oldName, requestId));
            RequestContext requestContext = RequestContext.createContext();
            requestContext.setUser(user);
            recordAudit(httpRequest, requestTimeISO9601, user);
            filterChain.doFilter(request, response);
        } finally {
            // put the request id into the response so users can trace logs for this request
            ((HttpServletResponse) response).setHeader(AtlasClient.REQUEST_ID, requestId);
            currentThread.setName(oldName);
            RequestContext.clear();
        }
    }

    private String formatName(String oldName, String requestId) {
        return oldName + " - " + requestId;
    }

    private void recordAudit(HttpServletRequest httpRequest, String whenISO9601, String who) {
        final String fromHost = httpRequest.getRemoteHost();
        final String fromAddress = httpRequest.getRemoteAddr();
        final String whatRequest = httpRequest.getMethod();
        final String whatURL = Servlets.getRequestURL(httpRequest);
        final String whatAddrs = httpRequest.getLocalAddr();

        LOG.info("Audit: {}/{} performed request {} {} ({}) at time {}", who, fromAddress, whatRequest, whatURL,
                whatAddrs, whenISO9601);
        audit(who, fromAddress, whatRequest, fromHost, whatURL, whatAddrs, whenISO9601);
    }

    private String getUserFromRequest(HttpServletRequest httpRequest) {
        // look for the user in the request
        final String userFromRequest = Servlets.getUserFromRequest(httpRequest);
        return userFromRequest == null ? "UNKNOWN" : userFromRequest;
    }

    public static void audit(String who, String fromAddress, String whatRequest, String fromHost, String whatURL, String whatAddrs,
            String whenISO9601) {
        AUDIT_LOG.info("Audit: {}/{}-{} performed request {} {} ({}) at time {}", who, fromAddress, fromHost, whatRequest, whatURL,
                whatAddrs, whenISO9601);
    }

    @Override
    public void destroy() {
        // do nothing
    }
}