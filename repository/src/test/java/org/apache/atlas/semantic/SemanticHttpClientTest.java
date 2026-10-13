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

import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class SemanticHttpClientTest {
    private HttpServer server;
    private int        status;

    @BeforeMethod
    public void startServer() throws IOException {
        status = 200;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    @AfterMethod
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void isSuccessOnlyFor2xx() {
        assertTrue(SemanticHttpClient.isSuccess(200));
        assertTrue(SemanticHttpClient.isSuccess(201));
        assertFalse(SemanticHttpClient.isSuccess(199));
        assertFalse(SemanticHttpClient.isSuccess(300));
        assertFalse(SemanticHttpClient.isSuccess(404));
    }

    @Test
    public void httpFailureIsRetryableOnlyForTransientStatuses() {
        for (int transientStatus : new int[] {429, 502, 503, 504}) {
            assertTrue(SemanticHttpClient.httpFailure(new SemanticHttpClient.Response(transientStatus, "")).isRetryable(), "status " + transientStatus);
        }
        for (int permanentStatus : new int[] {400, 401, 404, 500}) {
            assertFalse(SemanticHttpClient.httpFailure(new SemanticHttpClient.Response(permanentStatus, "")).isRetryable(), "status " + permanentStatus);
        }
    }

    @Test
    public void httpFailureIncludesTheResponseBody() {
        SemanticSearchException e = SemanticHttpClient.httpFailure(new SemanticHttpClient.Response(400, "bad query"));

        assertEquals(e.getMessage(), "HTTP request failed with status 400: bad query");
    }

    @Test
    public void sendForBodyMovesToTheNextHostWhenAHostIsDown() throws Exception {
        try (SemanticHttpClient client = client(Arrays.asList(closedHost(), serverHost()))) {
            assertEquals(client.sendForBody(new HttpGet("/")), "ok");
            assertEquals(client.sendForBody(new HttpGet("/")), "ok");
        }
    }

    @Test
    public void sendForBodyFailsRetryablyWhenAllHostsAreDown() throws Exception {
        try (SemanticHttpClient client = client(Collections.singletonList(closedHost()))) {
            SemanticSearchException e = expectThrows(SemanticSearchException.class, () -> client.sendForBody(new HttpGet("/")));

            assertTrue(e.isRetryable());
        }
    }

    @Test
    public void sendForBodyThrowsOnErrorStatus() throws Exception {
        status = 503;

        try (SemanticHttpClient client = client(Collections.singletonList(serverHost()))) {
            SemanticSearchException e = expectThrows(SemanticSearchException.class, () -> client.sendForBody(new HttpGet("/")));

            assertTrue(e.isRetryable());
            assertEquals(e.getMessage(), "HTTP request failed with status 503: ok");
        }
    }

    private static SemanticHttpClient client(List<HttpHost> hosts) {
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(1000).setSocketTimeout(2000).build();

        return new SemanticHttpClient(hosts, requestConfig, Collections.emptyList(), null);
    }

    private HttpHost serverHost() {
        return new HttpHost("127.0.0.1", server.getAddress().getPort());
    }

    private static HttpHost closedHost() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return new HttpHost("127.0.0.1", socket.getLocalPort());
        }
    }
}
