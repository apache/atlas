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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * This enforces authentication as part of the filter before processing the request.
 * todo: Subclass of {@link org.apache.hadoop.security.authentication.server.AuthenticationFilter}.
 */
@Singleton
public class AtlasAuthenticationFilter extends AuthenticationFilter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthenticationFilter.class);
    static final String PREFIX = "atlas.http.authentication";

    /**
     * An options servlet is used to authenticate users. OPTIONS method is used for triggering authentication
     * before invoking the actual resource.
     */
    private HttpServlet optionsServlet;

    /**
     * Initialize the filter.
     *
     * @param filterConfig filter configuration.
     * @throws ServletException thrown if the filter could not be initialized.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("AtlasAuthenticationFilter initialization started");
        super.init(filterConfig);

        optionsServlet = new HttpServlet() {};
        optionsServlet.init();
    }

    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
        Configuration configuration;
        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            throw new ServletException(e);
        }

        // transfer atlas-application.properties config items starting with defined prefix
        Configuration subConfiguration = ApplicationProperties.getSubsetConfiguration(configuration, PREFIX);
        Properties config = ConfigurationConverter.getProperties(subConfiguration);

        config.put(AuthenticationFilter.COOKIE_PATH, "/");

        // add any config passed in as init parameters
        Enumeration<String> enumeration = filterConfig.getInitParameterNames();
        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();
            config.put(name, filterConfig.getInitParameter(name));
        }

        //Resolve _HOST into bind address
        String bindAddress = configuration.getString(SecurityProperties.BIND_ADDRESS);
        if (bindAddress == null) {
            LOG.info("No host name configured.  Defaulting to local host name.");
            try {
                bindAddress = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new ServletException("Unable to obtain host name", e);
            }
        }
        String principal = config.getProperty(KerberosAuthenticationHandler.PRINCIPAL);
        if (principal != null) {
            try {
                principal = SecurityUtil.getServerPrincipal(principal, bindAddress);
            } catch (IOException ex) {
                throw new RuntimeException("Could not resolve Kerberos principal name: " + ex.toString(), ex);
            }
            config.put(KerberosAuthenticationHandler.PRINCIPAL, principal);
        }

        LOG.info("AuthenticationFilterConfig: {}", config);

        return config;
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response,
                         final FilterChain filterChain) throws IOException, ServletException {

        FilterChain filterChainWrapper = new FilterChain() {

            @Override
            public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
                    throws IOException, ServletException {
                HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;

                if (httpRequest.getMethod().equals("OPTIONS")) { // option request meant only for authentication
                    optionsServlet.service(request, response);
                } else {
                    final String user = Servlets.getUserFromRequest(httpRequest);
                    if (StringUtils.isEmpty(user)) {
                        ((HttpServletResponse) response).sendError(Response.Status.BAD_REQUEST.getStatusCode(),
                                "Param user.name can't be empty");
                    } else {
                        try {
                            NDC.push(user + ":" + httpRequest.getMethod() + httpRequest.getRequestURI());
                            RequestContext requestContext = RequestContext.get();
                            requestContext.setUser(user);
                            LOG.info("Request from authenticated user: {}, URL={}", user,
                                    Servlets.getRequestURI(httpRequest));

                            filterChain.doFilter(servletRequest, servletResponse);
                        } finally {
                            NDC.pop();
                        }
                    }
                }
            }
        };

        super.doFilter(request, response, filterChainWrapper);
    }

    @Override
    public void destroy() {
        if (optionsServlet != null) {
            optionsServlet.destroy();
        }

        super.destroy();
    }
}
