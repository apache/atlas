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
import org.apache.atlas.security.SecurityProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

/**
 * This enforces authentication as part of the filter before processing the request.
 * todo: Subclass of {@link org.apache.hadoop.security.authentication.server.AuthenticationFilter}.
 */
@Singleton
public class AtlasAuthenticationFilter extends AuthenticationFilter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthenticationFilter.class);
    static final String PREFIX = "atlas.http.authentication";

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

}
