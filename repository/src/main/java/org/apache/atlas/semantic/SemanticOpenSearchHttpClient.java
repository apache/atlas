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
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP client for the OpenSearch cluster behind the JanusGraph index backend. Reads the same
 * {@code atlas.graph.index.search.*} settings as JanusGraph's RestClientSetup (hosts, TLS, basic auth,
 * timeouts), so semantic search needs no separate OpenSearch connection config.
 */
final class SemanticOpenSearchHttpClient implements Closeable {
    static final String HOSTNAME_CONF = "atlas.graph.index.search.hostname";
    static final String PORT_CONF     = "atlas.graph.index.search.port";

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
    private static final int MAX_CONNECTIONS            = 50;

    private final List<HttpHost>      hosts;
    private final CloseableHttpClient httpClient;
    private final AtomicInteger       nextHost = new AtomicInteger();

    SemanticOpenSearchHttpClient(Configuration config) throws SemanticSearchException {
        boolean ssl = config.getBoolean(SSL_ENABLED_CONF, false);

        this.hosts      = parseHosts(config.getStringArray(HOSTNAME_CONF), config.getInt(PORT_CONF, DEFAULT_PORT), ssl ? "https" : "http");
        this.httpClient = buildHttpClient(config, ssl);
    }

    /**
     * One attempt: returns the body of a 2xx response, otherwise throws (retryable for transient failures).
     * Callers decide whether to retry, with {@link SemanticRetry}.
     */
    String sendForBody(HttpUriRequest request) throws SemanticSearchException {
        Response response = send(request);
        if (!isSuccess(response.statusCode)) {
            throw httpFailure(response);
        }
        return response.body;
    }

    /**
     * One attempt: connection failures become retryable exceptions, HTTP error statuses are returned.
     */
    Response send(HttpUriRequest request) throws SemanticSearchException {
        try {
            return execute(request);
        } catch (IOException e) {
            throw new SemanticSearchException("HTTP request failed", e, true);
        }
    }

    static boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    static SemanticSearchException httpFailure(Response response) {
        String message = "HTTP request failed with status " + response.statusCode;
        if (StringUtils.isNotEmpty(response.body)) {
            message += ": " + response.body;
        }
        return new SemanticSearchException(message, isTransientHttpStatus(response.statusCode));
    }

    private static boolean isTransientHttpStatus(int status) {
        return status == 409 || status == 429 || status == 502 || status == 503 || status == 504;
    }

    /**
     * Executes a request with a path-only URI; on connection failure, fails over to the next configured host.
     */
    private Response execute(HttpUriRequest request) throws IOException {
        int         start = Math.floorMod(nextHost.getAndIncrement(), hosts.size());
        IOException last  = null;

        for (int i = 0; i < hosts.size(); i++) {
            HttpHost host = hosts.get((start + i) % hosts.size());

            try (CloseableHttpResponse response = httpClient.execute(host, request)) {
                String body = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "";

                return new Response(response.getStatusLine().getStatusCode(), body);
            } catch (IOException e) {
                last = e;
            }
        }

        throw last;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
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

    private static CloseableHttpClient buildHttpClient(Configuration config, boolean ssl) throws SemanticSearchException {
        HttpClientBuilder builder = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectTimeout(config.getInt(CONNECT_TIMEOUT_CONF, DEFAULT_CONNECT_TIMEOUT_MS))
                        .setConnectionRequestTimeout(config.getInt(CONNECT_TIMEOUT_CONF, DEFAULT_CONNECT_TIMEOUT_MS))
                        .setSocketTimeout(config.getInt(SOCKET_TIMEOUT_CONF, DEFAULT_SOCKET_TIMEOUT_MS))
                        .build())
                .setMaxConnTotal(MAX_CONNECTIONS)
                .setMaxConnPerRoute(MAX_CONNECTIONS);

        String authType = config.getString(HTTP_AUTH_TYPE_CONF, "NONE");
        if ("BASIC".equalsIgnoreCase(authType)) {
            String credentials = config.getString(HTTP_AUTH_USERNAME_CONF, "") + ":" + config.getString(HTTP_AUTH_PASSWORD_CONF, "");
            String encoded     = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

            builder.setDefaultHeaders(Collections.singletonList(new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded)));
        } else if (!"NONE".equalsIgnoreCase(authType)) {
            throw new SemanticSearchException(HTTP_AUTH_TYPE_CONF + "=" + authType + " is not supported by semantic search (use NONE or BASIC)");
        }

        if (ssl) {
            builder.setSSLSocketFactory(buildSslSocketFactory(config));
        }

        return builder.build();
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

    static final class Response {
        final int    statusCode;
        final String body;

        Response(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body       = body;
        }
    }
}
