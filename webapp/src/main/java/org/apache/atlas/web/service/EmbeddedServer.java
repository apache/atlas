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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.IOException;

/**
 * This class embeds a Jetty server and a connector.
 */
public class EmbeddedServer {
    private static final int DEFAULT_BUFFER_SIZE = 16192;

    protected final Server server = new Server();

    public EmbeddedServer(int port, String path) throws IOException {
        Connector connector = getConnector(port);
        server.addConnector(connector);

        WebAppContext application = new WebAppContext(path, "/");
        server.setHandler(application);
    }

    public static EmbeddedServer newServer(int port, String path, boolean secure) throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(port, path);
        } else {
            return new EmbeddedServer(port, path);
        }
    }

    protected Connector getConnector(int port) throws IOException {
        Connector connector = new SocketConnector();
        connector.setPort(port);
        connector.setHost("0.0.0.0");

        // this is to enable large header sizes when Kerberos is enabled with AD
        final Integer bufferSize = getBufferSize();
        connector.setHeaderBufferSize(bufferSize);
        connector.setRequestBufferSize(bufferSize);

        return connector;
    }

    private Integer getBufferSize() {
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration("application.properties");
            return configuration.getInt("atlas.jetty.request.buffer.size", DEFAULT_BUFFER_SIZE);
        } catch (ConfigurationException e) {
            // do nothing
        }

        return DEFAULT_BUFFER_SIZE;
    }

    public void start() throws Exception {
        server.start();
        server.join();
    }

    public void stop() throws Exception {
        server.stop();
    }
}
