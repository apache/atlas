/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.atlas.web.filters;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class AtlasHeaderAuthConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasHeaderAuthConfiguration.class);

    public static final String PROP_HEADER_AUTH_ENABLED  = "atlas.authentication.header.enabled";
    public static final String PROP_METHOD_HEADER_PREFIX = "atlas.authentication.method.header";

    public static final String KEY_USERNAME   = "username";
    public static final String KEY_ROLES      = "roles";
    public static final String KEY_REQUEST_ID = "requestid";

    private final boolean             enabled;
    private final Map<String, String> headerNamesByKey;

    private AtlasHeaderAuthConfiguration(boolean enabled, Map<String, String> headerNamesByKey) {
        this.enabled          = enabled;
        this.headerNamesByKey = headerNamesByKey;
    }

    public static AtlasHeaderAuthConfiguration load(Configuration configuration) {
        if (configuration == null || !configuration.getBoolean(PROP_HEADER_AUTH_ENABLED, false)) {
            return disabled();
        }

        Map<String, String> headerNamesByKey = new HashMap<>();

        applyMethodHeaderMappings(configuration.subset(PROP_METHOD_HEADER_PREFIX), headerNamesByKey);

        return new AtlasHeaderAuthConfiguration(true, Collections.unmodifiableMap(headerNamesByKey));
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Set<String> getKeys() {
        return headerNamesByKey.keySet();
    }

    public String getHeaderName(String key) {
        return headerNamesByKey.get(key);
    }

    public String readHeaderValue(HttpServletRequest request, String key) {
        String headerName = getHeaderName(key);

        if (headerName == null || request == null) {
            return null;
        }

        return StringUtils.trimToNull(request.getHeader(headerName));
    }

    private static AtlasHeaderAuthConfiguration disabled() {
        return new AtlasHeaderAuthConfiguration(false, Collections.emptyMap());
    }

    private static void applyMethodHeaderMappings(Configuration methodHeaders, Map<String, String> headerNamesByKey) {
        if (methodHeaders == null) {
            return;
        }

        Iterator<String> keys = methodHeaders.getKeys();

        while (keys.hasNext()) {
            String propertyKey  = keys.next();
            String logicalKey   = normalizeLogicalKey(propertyKey);
            String httpHeaderName = StringUtils.trimToNull(methodHeaders.getString(propertyKey));

            if (logicalKey == null) {
                LOG.warn("Ignoring unrecognized header property key '{}'", propertyKey);
                continue;
            }

            if (httpHeaderName == null) {
                headerNamesByKey.remove(logicalKey);
                continue;
            }

            headerNamesByKey.put(logicalKey, httpHeaderName);
        }
    }

    private static String normalizeLogicalKey(String propertyKey) {
        if (StringUtils.isBlank(propertyKey)) {
            return null;
        }

        String normalized = propertyKey.trim().toLowerCase(Locale.ROOT).replace('-', '_');

        switch (normalized) {
            case KEY_USERNAME:
            case "user":
                return KEY_USERNAME;

            case KEY_ROLES:
            case "role":
            case "groups":
                return KEY_ROLES;

            case KEY_REQUEST_ID:
            case "request_id":
                return KEY_REQUEST_ID;

            default:
                return null;
        }
    }
}
