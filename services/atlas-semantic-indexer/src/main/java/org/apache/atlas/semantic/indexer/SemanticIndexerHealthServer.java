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
package org.apache.atlas.semantic.indexer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

/**
 * Minimal HTTP health endpoint for the standalone semantic indexer process.
 */
public final class SemanticIndexerHealthServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticIndexerHealthServer.class);

    private final HttpServer        server;
    private final BooleanSupplier     running;
    private final long                startTimeMs = System.currentTimeMillis();
    private final int                 port;
    private final String              path;

    public SemanticIndexerHealthServer(int port, String path, BooleanSupplier running) throws IOException {
        this.port    = port;
        this.path    = path.startsWith("/") ? path : "/" + path;
        this.running = running;
        this.server  = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext(this.path, this::handleHealth);
        this.server.setExecutor(null);
    }

    public void start() {
        server.start();
        LOG.info("Health endpoint listening on http://0.0.0.0:{}{}", port, path);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        boolean up   = running.getAsBoolean();
        int     code = up ? 200 : 503;
        long uptimeSeconds = (System.currentTimeMillis() - startTimeMs) / 1000L;
        String body = String.format(
                "{\"status\":\"%s\",\"service\":\"atlas-semantic-indexer\",\"uptimeSeconds\":%d}",
                up ? "UP" : "DOWN",
                uptimeSeconds);

        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, bytes.length);

        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
