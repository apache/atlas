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

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP client over a list of hosts: requests use path-only URIs and are sent round-robin, moving to the next host
 * when a connection can't be established.
 */
final class SemanticHttpClient implements Closeable {
    private static final int MAX_CONNECTIONS = 50;

    private final List<HttpHost>      hosts;
    private final RequestConfig       requestConfig;
    private final CloseableHttpClient httpClient;
    private final AtomicInteger       nextHost = new AtomicInteger();

    /**
     * @param hosts            at least one host
     * @param defaultHeaders   sent with every request (e.g. Authorization)
     * @param sslSocketFactory null for plain HTTP
     */
    SemanticHttpClient(List<HttpHost> hosts, RequestConfig requestConfig, List<Header> defaultHeaders,
                       SSLConnectionSocketFactory sslSocketFactory) {
        HttpClientBuilder builder = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setDefaultHeaders(defaultHeaders)
                .setMaxConnTotal(MAX_CONNECTIONS)
                .setMaxConnPerRoute(MAX_CONNECTIONS);

        if (sslSocketFactory != null) {
            builder.setSSLSocketFactory(sslSocketFactory);
        }

        this.hosts         = hosts;
        this.requestConfig = requestConfig;
        this.httpClient    = builder.build();
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
     * Same as {@link #sendForBody(HttpUriRequest)}, with its own socket (read) timeout.
     */
    String sendForBody(HttpRequestBase request, int socketTimeoutMs) throws SemanticSearchException {
        request.setConfig(RequestConfig.copy(requestConfig).setSocketTimeout(socketTimeoutMs).build());

        return sendForBody(request);
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
        return status == 429 || status == 502 || status == 503 || status == 504;
    }

    /**
     * A read timeout is not retried on another host: the request may already be running there.
     */
    private Response execute(HttpUriRequest request) throws IOException {
        int         start = Math.floorMod(nextHost.getAndIncrement(), hosts.size());
        IOException last  = null;

        for (int i = 0; i < hosts.size(); i++) {
            HttpHost host = hosts.get((start + i) % hosts.size());

            try (CloseableHttpResponse response = httpClient.execute(host, request)) {
                String body = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "";

                return new Response(response.getStatusLine().getStatusCode(), body);
            } catch (ConnectException | ConnectTimeoutException | UnknownHostException e) {
                last = e;
            }
        }

        throw last;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
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
