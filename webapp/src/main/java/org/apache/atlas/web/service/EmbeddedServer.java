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

package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class embeds a Jetty server and a connector.
 */
public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);

    private static final int DEFAULT_BUFFER_SIZE = 16192;

    protected final Server server = new Server();

    public EmbeddedServer(int port, String path) throws IOException {
        Connector connector = getConnector(port);
        server.addConnector(connector);

        WebAppContext application = getWebAppContext(path);
        server.setHandler(application);
    }

    protected WebAppContext getWebAppContext(String path) {
        WebAppContext application = new WebAppContext(path, "/");
        application.setClassLoader(Thread.currentThread().getContextClassLoader());
        return application;
    }

    public static EmbeddedServer newServer(int port, String path, boolean secure) throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(port, path);
        } else {
            return new EmbeddedServer(port, path);
        }
    }

    protected Connector getConnector(int port) throws IOException {

        HttpConfiguration http_config = new HttpConfiguration();
        // this is to enable large header sizes when Kerberos is enabled with AD
        final int bufferSize = getBufferSize();
        http_config.setResponseHeaderSize(bufferSize);
        http_config.setRequestHeaderSize(bufferSize);

        ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(http_config));
        connector.setPort(port);
        connector.setHost("0.0.0.0");
        server.addConnector(connector);
        return connector;
    }

    protected Integer getBufferSize() {
        try {
            Configuration configuration = ApplicationProperties.get();
            return configuration.getInt("atlas.jetty.request.buffer.size", DEFAULT_BUFFER_SIZE);
        } catch (Exception e) {
            // do nothing
        }

        return DEFAULT_BUFFER_SIZE;
    }

    public void start() throws Exception {
        server.start();
        server.join();
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            LOG.warn("Error during shutdown", e);
        }
    }
}
