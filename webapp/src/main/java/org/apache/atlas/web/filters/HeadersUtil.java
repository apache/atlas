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

import org.apache.atlas.AtlasConfiguration;

import java.util.HashMap;
import java.util.Map;

public class HeadersUtil {
    public static final String X_FRAME_OPTIONS_KEY        = "X-Frame-Options";
    public static final String X_CONTENT_TYPE_OPTIONS_KEY = "X-Content-Type-Options";
    public static final String X_XSS_PROTECTION_KEY       = "X-XSS-Protection";
    public static final String STRICT_TRANSPORT_SEC_KEY   = "Strict-Transport-Security";
    public static final String CONTENT_SEC_POLICY_KEY     = "Content-Security-Policy";
    public static final String X_FRAME_OPTIONS_VAL        = "DENY";
    public static final String X_CONTENT_TYPE_OPTIONS_VAL = "nosniff";
    public static final String X_XSS_PROTECTION_VAL       = "1; mode=block";
    public static final String STRICT_TRANSPORT_SEC_VAL   = "max-age=31536000; includeSubDomains";
    public static final String CONTENT_SEC_POLICY_VAL     = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: data:; connect-src 'self'; img-src 'self' blob: data:; style-src 'self' 'unsafe-inline';font-src 'self' data:";
    public static final String SERVER_KEY                 = "Server";
    public static final String USER_AGENT_KEY             = "User-Agent";
    public static final String USER_AGENT_VALUE           = "Mozilla";
    public static final String X_REQUESTED_WITH_KEY       = "X-REQUESTED-WITH";
    public static final String X_REQUESTED_WITH_VALUE     = "XMLHttpRequest";
    public static final int    SC_AUTHENTICATION_TIMEOUT  = 419;

    private static final Map<String, String> HEADER_MAP = new HashMap<>();

    private HeadersUtil() {
        // to block instantiation
    }

    public static String getHeaderMap(String header) {
        return HEADER_MAP.get(header);
    }

    public static void setHeaderMapAttributes(AtlasResponseRequestWrapper responseWrapper, String headerKey) {
        responseWrapper.setHeader(headerKey, HEADER_MAP.get(headerKey));
    }

    public static void setSecurityHeaders(AtlasResponseRequestWrapper responseWrapper) {
        for (Map.Entry<String, String> entry : HEADER_MAP.entrySet()) {
            responseWrapper.setHeader(entry.getKey(), entry.getValue());
        }
    }

    static {
        HEADER_MAP.put(X_FRAME_OPTIONS_KEY, X_FRAME_OPTIONS_VAL);
        HEADER_MAP.put(X_CONTENT_TYPE_OPTIONS_KEY, X_CONTENT_TYPE_OPTIONS_VAL);
        HEADER_MAP.put(X_XSS_PROTECTION_KEY, X_XSS_PROTECTION_VAL);
        HEADER_MAP.put(STRICT_TRANSPORT_SEC_KEY, STRICT_TRANSPORT_SEC_VAL);
        HEADER_MAP.put(CONTENT_SEC_POLICY_KEY, CONTENT_SEC_POLICY_VAL);
        HEADER_MAP.put(SERVER_KEY, AtlasConfiguration.HTTP_HEADER_SERVER_VALUE.getString());
    }
}
