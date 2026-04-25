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
package org.apache.atlas.server.common.service;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class embeds a Jetty server and a connector.
 */

public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);

    public static final String ATLAS_DEFAULT_BIND_ADDRESS = "0.0.0.0";
    public static final String REST_DEFAULT_BIND_ADDRESS  = "0.0.0.0";
    public static final String DEFAULT_BIND_ADDRESS       = ATLAS_DEFAULT_BIND_ADDRESS;
    public static final Date   SERVER_START_TIME          = new Date();

    public final Server server;

    public EmbeddedServer(String host, int port, String path) throws IOException {
        int                           queueSize     = AtlasConfiguration.WEBSERVER_QUEUE_SIZE.getInt();
        LinkedBlockingQueue<Runnable> queue         = new LinkedBlockingQueue<>(queueSize);
        int                           minThreads    = AtlasConfiguration.WEBSERVER_MIN_THREADS.getInt();
        int                           maxThreads    = AtlasConfiguration.WEBSERVER_MAX_THREADS.getInt();
        long                          keepAliveTime = AtlasConfiguration.WEBSERVER_KEEPALIVE_SECONDS.getLong();
        ThreadPoolExecutor            executor      = new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, queue);
        ExecutorThreadPool            pool          = new ExecutorThreadPool(executor, minThreads);

        server = new Server(pool);

        Connector connector = getConnector(host, port);

        server.addConnector(connector);

        WebAppContext application = getWebAppContext(path);

        server.setHandler(application);
    }

    public static EmbeddedServer newServer(String host, int port, String path, boolean secure) throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(host, port, path);
        }

        return new EmbeddedServer(host, port, path);
    }

    public void start() throws AtlasBaseException {
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.EMBEDDED_SERVER_START, e);
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            LOG.warn("Error during shutdown", e);
        }
    }

    protected WebAppContext getWebAppContext(String path) {
        WebAppContext application = new WebAppContext(path, "/");

        application.setClassLoader(Thread.currentThread().getContextClassLoader());
        // Disable directory listing
        application.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

        return application;
    }

    protected Connector getConnector(String host, int port) throws IOException {
        HttpConfiguration httpConfig = new HttpConfiguration();

        // this is to enable large header sizes when Kerberos is enabled with AD
        final int bufferSize = AtlasConfiguration.WEBSERVER_REQUEST_BUFFER_SIZE.getInt();

        httpConfig.setResponseHeaderSize(bufferSize);
        httpConfig.setRequestHeaderSize(bufferSize);
        httpConfig.setSendServerVersion(false);

        ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));

        connector.setPort(port);
        connector.setHost(host);

        return connector;
    }
}
