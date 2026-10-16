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
package org.apache.atlas.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper for validating a SPIFFE ID and extracting the service-account identity from it.
 *
 * <p>A SPIFFE ID is expected in the exact form {@code spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>};
 * for service-to-service authentication the receiving server trusts the workload
 * identity carried in a header (e.g. {@code x-awc-source-workload-id}) and uses
 * the trailing service-account segment as the authenticated principal.
 */
public final class SpiffeIdUtil {
    private static final String HEADER_NAMES_SEP = ",";

    /**
     * Matches {@code spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>} using the character
     * sets from the SPIFFE-ID specification:
     * <ul>
     *   <li>trust-domain: lowercase letters, digits, {@code .}, {@code -}, {@code _} (the spec requires
     *       the trust domain to be lowercase);
     *   <li>namespace and service-account: ASCII letters (any case), digits, {@code .}, {@code -}, {@code _}
     *       (SPIFFE path segments are case-sensitive).
     * </ul>
     * All segments must be non-empty. This is an allow-list of safe characters, so whitespace, control
     * characters and {@code /} are excluded, keeping the extracted principal well-formed. Per the spec,
     * the namespace and service-account path segments must not be the relative modifiers {@code .} or
     * {@code ..} (enforced by the leading negative look-aheads). The service-account is exposed via the
     * named group {@code sa}.
     */
    private static final Pattern SPIFFE_ID_PATTERN = Pattern.compile(
            "^spiffe://[a-z0-9._-]+/ns/(?!\\.{1,2}/)[A-Za-z0-9._-]+/sa/(?<sa>(?!\\.{1,2}$)[A-Za-z0-9._-]+)$");

    private SpiffeIdUtil() {
        // to block instantiation
    }

    /**
     * Parses a configured, comma-separated list of SPIFFE header names into an ordered list of
     * trimmed, non-empty names. Supports a single header name as a list of length one.
     *
     * @param configValue the raw config value (e.g. {@code x-awc-source-workload-id})
     * @return an ordered, immutable list of header names; empty when the input is blank
     */
    public static List<String> parseHeaderNames(String configValue) {
        if (StringUtils.isBlank(configValue)) {
            return Collections.emptyList();
        }

        List<String> ret = new ArrayList<>();

        for (String name : configValue.split(HEADER_NAMES_SEP)) {
            String trimmed = StringUtils.trimToNull(name);

            if (trimmed != null) {
                ret.add(trimmed);
            }
        }

        return Collections.unmodifiableList(ret);
    }

    /**
     * Validates that the given value is a well-formed SPIFFE ID in the exact form
     * {@code spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>} with non-empty segments.
     *
     * @param spiffeId the raw SPIFFE ID value from the request header
     * @return {@code true} if the value matches the expected SPIFFE ID format; {@code false} otherwise
     */
    public static boolean isValidSpiffeId(String spiffeId) {
        return matchSpiffeId(spiffeId) != null;
    }

    /**
     * Extracts the service-account name from a SPIFFE ID such as
     * {@code spiffe://my-cluster/ns/service-namespace/sa/service-sa}. The value must match the exact
     * SPIFFE ID format (see {@link #isValidSpiffeId(String)}); otherwise {@code null} is returned.
     *
     * @param spiffeId the raw SPIFFE ID value from the request header
     * @return the service-account segment (e.g. {@code service-sa}), or
     *         {@code null} if the input is blank or not a well-formed SPIFFE ID
     */
    public static String extractServiceAccount(String spiffeId) {
        Matcher matcher = matchSpiffeId(spiffeId);

        return matcher != null ? matcher.group("sa") : null;
    }

    private static Matcher matchSpiffeId(String spiffeId) {
        if (StringUtils.isBlank(spiffeId)) {
            return null;
        }

        Matcher matcher = SPIFFE_ID_PATTERN.matcher(spiffeId.trim());

        return matcher.matches() ? matcher : null;
    }
}
