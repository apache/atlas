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
package org.apache.atlas.web.filters;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class HeadersUtil {
    public static final Logger LOG = LoggerFactory.getLogger(HeadersUtil.class);

    public static final String X_FRAME_OPTIONS_KEY                 = "X-Frame-Options";
    public static final String X_CONTENT_TYPE_OPTIONS_KEY          = "X-Content-Type-Options";
    public static final String X_XSS_PROTECTION_KEY                = "X-XSS-Protection";
    public static final String STRICT_TRANSPORT_SEC_KEY            = "Strict-Transport-Security";
    public static final String CONTENT_SEC_POLICY_KEY              = "Content-Security-Policy";
    public static final String X_FRAME_OPTIONS_VAL                 = "DENY";
    public static final String X_CONTENT_TYPE_OPTIONS_VAL          = "nosniff";
    public static final String X_XSS_PROTECTION_VAL                = "1; mode=block";
    public static final String STRICT_TRANSPORT_SEC_VAL            = "max-age=31536000; includeSubDomains";
    public static final String CONTENT_SEC_POLICY_VAL              = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: data:; connect-src 'self'; img-src 'self' blob: data:; style-src 'self' 'unsafe-inline';font-src 'self' data:";
    public static final String SERVER_KEY                          = "Server";
    public static final String USER_AGENT_KEY                      = "User-Agent";
    public static final String USER_AGENT_VALUE                    = "Mozilla";
    public static final String X_REQUESTED_WITH_KEY                = "X-REQUESTED-WITH";
    public static final String X_REQUESTED_WITH_VALUE              = "XMLHttpRequest";
    public static final int    SC_AUTHENTICATION_TIMEOUT           = 419;
    public static final String CONFIG_PREFIX_HTTP_RESPONSE_HEADER  = "atlas.headers";

    private static final Map<String, String> HEADER_MAP = new HashMap<>();

    private HeadersUtil() {
        // to block instantiation
    }

    public static String getHeaderMap(String header) {
        return HEADER_MAP.get(header);
    }

    public static Map<String, String> getAllHeaders() {
        return new HashMap<>(HEADER_MAP);
    }

    public static void setHeaderMapAttributes(AtlasResponseRequestWrapper responseWrapper, String headerKey) {
        responseWrapper.setHeader(headerKey, HEADER_MAP.get(headerKey));
    }

    public static void setSecurityHeaders(AtlasResponseRequestWrapper responseWrapper) {
        HEADER_MAP.forEach((key, value) -> responseWrapper.setHeader(key, value));
    }

    @VisibleForTesting
    public static void initializeHttpResponseHeaders(Properties configuredHeaders) {
        HEADER_MAP.clear();

        HEADER_MAP.put(X_FRAME_OPTIONS_KEY, X_FRAME_OPTIONS_VAL);
        HEADER_MAP.put(X_CONTENT_TYPE_OPTIONS_KEY, X_CONTENT_TYPE_OPTIONS_VAL);
        HEADER_MAP.put(X_XSS_PROTECTION_KEY, X_XSS_PROTECTION_VAL);
        HEADER_MAP.put(STRICT_TRANSPORT_SEC_KEY, STRICT_TRANSPORT_SEC_VAL);
        HEADER_MAP.put(CONTENT_SEC_POLICY_KEY, CONTENT_SEC_POLICY_VAL);
        HEADER_MAP.put(SERVER_KEY, AtlasConfiguration.HTTP_HEADER_SERVER_VALUE.getString());

        if (configuredHeaders != null) {
            configuredHeaders.stringPropertyNames().forEach(name -> HEADER_MAP.put(name, configuredHeaders.getProperty(name)));
        }
    }

    static {
        Properties configuredHeaders = null;

        try {
            Configuration baseConfig   = ApplicationProperties.get();
            Configuration headerConfig = ApplicationProperties.getSubsetConfiguration(baseConfig, CONFIG_PREFIX_HTTP_RESPONSE_HEADER);

            configuredHeaders = Optional.ofNullable(headerConfig)
                    .map(ConfigurationConverter::getProperties)
                    .orElseGet(Properties::new);
        } catch (Exception e) {
            LOG.info("Failed to load custom headers: {}", e.getMessage());
        }

        initializeHttpResponseHeaders(configuredHeaders);
    }
}
