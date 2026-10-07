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
package org.apache.atlas.semantic;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Builds the {@link SemanticHttpClient} for the OpenSearch cluster behind the JanusGraph index backend, from the
 * same {@code atlas.graph.index.search.*} settings as JanusGraph's RestClientSetup (hosts, TLS, basic auth,
 * timeouts), so semantic search needs no separate OpenSearch connection config.
 */
final class OpenSearchClientFactory {
    static final String HOSTNAME_CONF = "atlas.graph.index.search.hostname";
    private static final String PORT_CONF = "atlas.graph.index.search.port";

    private static final String OS_PREFIX                         = "atlas.graph.index.search.opensearch.";
    private static final String SSL_ENABLED_CONF                  = OS_PREFIX + "ssl.enabled";
    private static final String SSL_TRUSTSTORE_LOCATION_CONF      = OS_PREFIX + "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD_CONF      = OS_PREFIX + "ssl.truststore.password";
    private static final String SSL_KEYSTORE_LOCATION_CONF        = OS_PREFIX + "ssl.keystore.location";
    private static final String SSL_KEYSTORE_PASSWORD_CONF        = OS_PREFIX + "ssl.keystore.storepassword";
    private static final String SSL_KEY_PASSWORD_CONF             = OS_PREFIX + "ssl.keystore.keypassword";
    private static final String SSL_DISABLE_HOSTNAME_VERIFY_CONF  = OS_PREFIX + "ssl.disable-hostname-verification";
    private static final String SSL_ALLOW_SELF_SIGNED_CONF        = OS_PREFIX + "ssl.allow-self-signed-certificates";
    private static final String HTTP_AUTH_TYPE_CONF               = OS_PREFIX + "http.auth.type";
    private static final String HTTP_AUTH_USERNAME_CONF           = OS_PREFIX + "http.auth.basic.username";
    private static final String HTTP_AUTH_PASSWORD_CONF           = OS_PREFIX + "http.auth.basic.password";
    private static final String CONNECT_TIMEOUT_CONF              = OS_PREFIX + "connect-timeout";
    private static final String SOCKET_TIMEOUT_CONF               = OS_PREFIX + "socket-timeout";

    // defaults match JanusGraph OpenSearchIndex
    private static final int DEFAULT_PORT               = 9200;
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 1000;
    private static final int DEFAULT_SOCKET_TIMEOUT_MS  = 30000;

    private OpenSearchClientFactory() {
    }

    static SemanticHttpClient create(Configuration config) throws SemanticSearchException {
        boolean ssl            = config.getBoolean(SSL_ENABLED_CONF, false);
        int     connectTimeout = config.getInt(CONNECT_TIMEOUT_CONF, DEFAULT_CONNECT_TIMEOUT_MS);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setConnectionRequestTimeout(connectTimeout)
                .setSocketTimeout(config.getInt(SOCKET_TIMEOUT_CONF, DEFAULT_SOCKET_TIMEOUT_MS))
                .build();

        return new SemanticHttpClient(
                parseHosts(config.getStringArray(HOSTNAME_CONF), config.getInt(PORT_CONF, DEFAULT_PORT), ssl ? "https" : "http"),
                requestConfig,
                authHeaders(config),
                ssl ? buildSslSocketFactory(config) : null);
    }

    static List<HttpHost> parseHosts(String[] hostnames, int defaultPort, String scheme) throws SemanticSearchException {
        List<HttpHost> ret = new ArrayList<>();

        if (hostnames != null) {
            for (String entry : hostnames) {
                for (String host : StringUtils.split(StringUtils.defaultString(entry), ',')) {
                    String trimmed = host.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }

                    // same "host" or "host:port" format as JanusGraph's RestClientSetup, which has no IPv6 support either
                    String[] parts = trimmed.split(":");
                    if (parts.length > 2 || parts[0].trim().isEmpty()) {
                        throw new SemanticSearchException(HOSTNAME_CONF + ": invalid entry '" + trimmed
                                + "', expected host or host:port (IPv6 addresses are not supported, use a host name)");
                    }

                    ret.add(new HttpHost(parts[0].trim(), parts.length == 2 ? parsePort(trimmed, parts[1].trim()) : defaultPort, scheme));
                }
            }
        }

        if (ret.isEmpty()) {
            throw new SemanticSearchException(HOSTNAME_CONF + " must be set for semantic search");
        }

        return Collections.unmodifiableList(ret);
    }

    private static int parsePort(String entry, String port) throws SemanticSearchException {
        try {
            int ret = Integer.parseInt(port);
            if (ret >= 1 && ret <= 65535) {
                return ret;
            }
        } catch (NumberFormatException e) {
            // reported below
        }

        throw new SemanticSearchException(HOSTNAME_CONF + ": invalid port in '" + entry + "', expected 1-65535");
    }

    private static List<Header> authHeaders(Configuration config) throws SemanticSearchException {
        String authType = config.getString(HTTP_AUTH_TYPE_CONF, "NONE");

        if ("NONE".equalsIgnoreCase(authType)) {
            return Collections.emptyList();
        }

        if (!"BASIC".equalsIgnoreCase(authType)) {
            throw new SemanticSearchException(HTTP_AUTH_TYPE_CONF + "=" + authType + " is not supported by semantic search (use NONE or BASIC)");
        }

        String credentials = config.getString(HTTP_AUTH_USERNAME_CONF, "") + ":" + config.getString(HTTP_AUTH_PASSWORD_CONF, "");
        String encoded     = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        return Collections.singletonList(new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded));
    }

    private static SSLConnectionSocketFactory buildSslSocketFactory(Configuration config) throws SemanticSearchException {
        String  trustStore      = config.getString(SSL_TRUSTSTORE_LOCATION_CONF, "");
        String  keyStore        = config.getString(SSL_KEYSTORE_LOCATION_CONF, "");
        String  keyStorePass    = config.getString(SSL_KEYSTORE_PASSWORD_CONF, "");
        String  keyPass         = StringUtils.defaultIfEmpty(config.getString(SSL_KEY_PASSWORD_CONF), keyStorePass);
        boolean allowSelfSigned = config.getBoolean(SSL_ALLOW_SELF_SIGNED_CONF, false);
        boolean noHostnameCheck = config.getBoolean(SSL_DISABLE_HOSTNAME_VERIFY_CONF, false);

        try {
            SSLContextBuilder sslContext = new SSLContextBuilder();
            TrustSelfSignedStrategy trustStrategy = allowSelfSigned ? TrustSelfSignedStrategy.INSTANCE : null;

            if (StringUtils.isNotEmpty(trustStore)) {
                sslContext.loadTrustMaterial(new File(trustStore), config.getString(SSL_TRUSTSTORE_PASSWORD_CONF, "").toCharArray(), trustStrategy);
            } else {
                sslContext.loadTrustMaterial(trustStrategy);
            }

            if (StringUtils.isNotEmpty(keyStore)) {
                sslContext.loadKeyMaterial(new File(keyStore), keyStorePass.toCharArray(), keyPass.toCharArray());
            }

            return noHostnameCheck
                    ? new SSLConnectionSocketFactory(sslContext.build(), NoopHostnameVerifier.INSTANCE)
                    : new SSLConnectionSocketFactory(sslContext.build());
        } catch (Exception e) {
            throw new SemanticSearchException("Failed to configure OpenSearch TLS from " + OS_PREFIX + "ssl.*", e);
        }
    }
}
