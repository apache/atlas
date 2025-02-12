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

import org.apache.atlas.*;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.atlas.web.util.DateTimeHelper;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

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
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import static java.util.Optional.ofNullable;
import static org.apache.atlas.AtlasConfiguration.*;
import static org.apache.commons.lang.StringUtils.EMPTY;
/**
 * This records audit information as part of the filter after processing the request
 * and also introduces a UUID into request and response for tracing requests in logs.
 */
@Component
public class AuditFilter implements Filter {
    private static final Logger LOG       = LoggerFactory.getLogger(AuditFilter.class);
    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("AUDIT");
    public static final String TRACE_ID   = "trace_id";
    public static final String X_ATLAN_REQUEST_ID = "X-Atlan-Request-Id";
    public static final String X_ATLAN_CLIENT_ORIGIN = "X-Atlan-Client-Origin";
    private boolean deleteTypeOverrideEnabled                = false;
    private boolean createShellEntityForNonExistingReference = false;

    @Inject
    private MetricsRegistry metricsRegistry;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("AuditFilter initialization started");

        deleteTypeOverrideEnabled                = REST_API_ENABLE_DELETE_TYPE_OVERRIDE.getBoolean();
        createShellEntityForNonExistingReference = REST_API_CREATE_SHELL_ENTITY_FOR_NON_EXISTING_REF.getBoolean();
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
        LOG.info("REST_API_ENABLE_DELETE_TYPE_OVERRIDE={}", deleteTypeOverrideEnabled);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
    throws IOException, ServletException {
        final long                startTime          = System.currentTimeMillis();
        final Date                requestTime         = new Date();
        final HttpServletRequest  httpRequest        = (HttpServletRequest) request;
        final HttpServletResponse httpResponse       = (HttpServletResponse) response;
        final String              internalRequestId          = UUID.randomUUID().toString();
        final Thread              currentThread      = Thread.currentThread();
        final String              oldName            = currentThread.getName();
        final String              user               = AtlasAuthorizationUtils.getCurrentUserName();
        final Set<String>         userGroups         = AtlasAuthorizationUtils.getCurrentUserGroups();
        final String              deleteType         = httpRequest.getParameter("deleteType");
        final boolean             skipFailedEntities = Boolean.parseBoolean(httpRequest.getParameter("skipFailedEntities"));

        try {
            currentThread.setName(formatName(oldName, internalRequestId));

            RequestContext.clear();
            RequestContext requestContext = RequestContext.get();
            requestContext.setUri(MetricUtils.matchCanonicalPattern(httpRequest.getRequestURI()).orElse(EMPTY));
            requestContext.setTraceId(internalRequestId);
            requestContext.setUser(user, userGroups);
            requestContext.setClientIPAddress(AtlasAuthorizationUtils.getRequestIpAddress(httpRequest));
            requestContext.setCreateShellEntityForNonExistingReference(createShellEntityForNonExistingReference);
            requestContext.setForwardedAddresses(AtlasAuthorizationUtils.getForwardedAddressesFromRequest(httpRequest));
            requestContext.setSkipFailedEntities(skipFailedEntities);
            requestContext.setClientOrigin(httpRequest.getHeader(X_ATLAN_CLIENT_ORIGIN));
            requestContext.setMetricRegistry(metricsRegistry);
            MDC.put(TRACE_ID, internalRequestId);
            MDC.put(X_ATLAN_CLIENT_ORIGIN, ofNullable(httpRequest.getHeader(X_ATLAN_CLIENT_ORIGIN)).orElse(EMPTY));
            MDC.put(X_ATLAN_REQUEST_ID, ofNullable(httpRequest.getHeader(X_ATLAN_REQUEST_ID)).orElse(EMPTY));
            if (StringUtils.isNotEmpty(deleteType)) {
                if (deleteTypeOverrideEnabled) {
                    if(DeleteType.PURGE.name().equals(deleteType)) {
                        requestContext.setPurgeRequested(true);
                    }
                    requestContext.setDeleteType(DeleteType.from(deleteType));
                } else {
                    LOG.warn("Override of deleteType is not enabled. Ignoring parameter deleteType={}, in request from user={}", deleteType, user);
                }
            }

            HeadersUtil.setRequestContextHeaders((HttpServletRequest)request);

            filterChain.doFilter(request, response);
        } finally {
            long timeTaken = System.currentTimeMillis() - startTime;

            recordAudit(httpRequest, requestTime, user, httpResponse.getStatus(), timeTaken);

            // put the request id into the response so users can trace logs for this request
            httpResponse.setHeader(TRACE_ID, internalRequestId);
            httpResponse.setHeader(X_ATLAN_REQUEST_ID, MDC.get(X_ATLAN_REQUEST_ID));
            currentThread.setName(oldName);
            RequestContext.clear();
            MDC.clear();
        }
    }

    private String formatName(String oldName, String requestId) {
        return oldName + " - " + requestId;
    }

    private void recordAudit(HttpServletRequest httpRequest, Date when, String who, int httpStatus, long timeTaken) {
        final String fromAddress = httpRequest.getRemoteAddr();
        final String whatRequest = httpRequest.getMethod();
        final String whatURL     = Servlets.getRequestURL(httpRequest);
        final String whatUrlPath = httpRequest.getRequestURL().toString(); //url path without query string

        if (!isOperationExcludedFromAudit(whatRequest, whatUrlPath.toLowerCase(), null)) {
            audit(new AuditLog(who, fromAddress, whatRequest, whatURL, when, httpStatus, timeTaken));
        } else {
            if(LOG.isDebugEnabled()) {
                LOG.debug(" Skipping Audit for {} ", whatURL);
            }
        }
    }

    public static void audit(AuditLog auditLog) {
        if (AUDIT_LOG.isInfoEnabled() && auditLog != null) {
            MDC.put("requestTime", DateTimeHelper.formatDateUTC(auditLog.requestTime));
            MDC.put("user", auditLog.userName);
            MDC.put("from", auditLog.fromAddress);
            MDC.put("requestMethod", auditLog.requestMethod);
            MDC.put("requestUrl", auditLog.requestUrl);
            MDC.put("httpStatus", String.valueOf(auditLog.httpStatus));
            MDC.put("timeTaken", String.valueOf(auditLog.timeTaken));
            AUDIT_LOG.info("ATLAS_AUDIT - {} {} {} {}", auditLog.requestMethod, auditLog.requestUrl, auditLog.httpStatus, auditLog.timeTaken);
        }
    }

    boolean isOperationExcludedFromAudit(String requestHttpMethod, String requestOperation, Configuration config) {
       try {
        return AtlasRepositoryConfiguration.isExcludedFromAudit(config, requestHttpMethod, requestOperation);
    } catch (AtlasException e) {
        return false;
    }
    }

    @Override
    public void destroy() {
        // do nothing
    }

    public static class AuditLog {
        private static final char FIELD_SEP = '|';

        private final String userName;
        private final String fromAddress;
        private final String requestMethod;
        private final String requestUrl;
        private final Date   requestTime;
        private       int    httpStatus;
        private       long   timeTaken;

        public AuditLog(String userName, String fromAddress, String requestMethod, String requestUrl) {
            this(userName, fromAddress, requestMethod, requestUrl, new Date());
        }

        public AuditLog(String userName, String fromAddress, String requestMethod, String requestUrl, Date requestTime) {
            this(userName, fromAddress, requestMethod, requestUrl, requestTime, HttpServletResponse.SC_OK, 0);
        }

        public AuditLog(String userName, String fromAddress, String requestMethod, String requestUrl, Date requestTime, int httpStatus, long timeTaken) {
            this.userName      = userName;
            this.fromAddress   = fromAddress;
            this.requestMethod = requestMethod;
            this.requestUrl    = requestUrl;
            this.requestTime   = requestTime;
            this.httpStatus    = httpStatus;
            this.timeTaken     = timeTaken;
        }

        public void setHttpStatus(int httpStatus) { this.httpStatus = httpStatus; }

        public void setTimeTaken(long timeTaken) { this.timeTaken = timeTaken; }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(DateTimeHelper.formatDateUTC(requestTime))
              .append(FIELD_SEP).append(userName)
              .append(FIELD_SEP).append(fromAddress)
              .append(FIELD_SEP).append(requestMethod)
              .append(FIELD_SEP).append(requestUrl)
              .append(FIELD_SEP).append(httpStatus)
              .append(FIELD_SEP).append(timeTaken);

            return sb.toString();
        }
    }
}