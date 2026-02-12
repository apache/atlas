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
package org.apache.atlas.web.integration;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lightweight Jetty server for in-process integration testing.
 *
 * Differs from production {@link org.apache.atlas.web.service.EmbeddedServer} in two ways:
 * <ul>
 *   <li>Sets {@code parentLoaderPriority=true} so Jetty uses the Maven test classpath
 *       instead of scanning WEB-INF/lib (no WAR packaging needed)</li>
 *   <li>Does NOT call {@code server.join()} so it doesn't block the test thread</li>
 * </ul>
 */
public class InProcessAtlasServer {

    private static final Logger LOG = LoggerFactory.getLogger(InProcessAtlasServer.class);

    private final Server server;
    private final int port;

    public InProcessAtlasServer(int port, String webappPath) {
        this.port = port;
        this.server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setHost("localhost");
        connector.setPort(port);
        server.addConnector(connector);

        WebAppContext context = new WebAppContext(webappPath, "/");
        context.setParentLoaderPriority(true);
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        context.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

        server.setHandler(context);
        server.setStopTimeout(10_000); // 10 second graceful shutdown limit
    }

    public void start() throws Exception {
        LOG.info("Starting in-process Atlas server on port {}", port);
        server.start();
        LOG.info("In-process Atlas server started on port {}", port);
    }

    public void stop() throws Exception {
        LOG.info("Stopping in-process Atlas server");
        Thread stopThread = new Thread(() -> {
            try {
                server.stop();
            } catch (Exception e) {
                LOG.warn("Error during server stop", e);
            }
        }, "atlas-server-stop");
        stopThread.start();
        try {
            stopThread.join(30_000); // Wait max 30 seconds for shutdown
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while stopping server", e);
        }
        if (stopThread.isAlive()) {
            LOG.warn("Server stop timed out after 30s, interrupting");
            stopThread.interrupt();
        }
        LOG.info("In-process Atlas server stopped");
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return server.isRunning();
    }
}
