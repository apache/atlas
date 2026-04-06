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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.jetty.JettyConnectionMetrics;
import io.micrometer.core.instrument.binder.jetty.JettyServerThreadPoolMetrics;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.util.BeanUtil;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
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
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * This class embeds a Jetty server and a connector.
 */
public class EmbeddedServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);

    public static final String ATLAS_DEFAULT_BIND_ADDRESS = "0.0.0.0";

    protected final Server server;

    public EmbeddedServer(String host, int port, String path) throws IOException {
        int                           queueSize       = AtlasConfiguration.WEBSERVER_QUEUE_SIZE.getInt();
        LinkedBlockingQueue<Runnable> queue           = new LinkedBlockingQueue<>(queueSize);
        int                           minThreads      = AtlasConfiguration.WEBSERVER_MIN_THREADS.getInt();
        int                           maxThreads      = AtlasConfiguration.WEBSERVER_MAX_THREADS.getInt();
        int                           reservedThreads = AtlasConfiguration.WEBSERVER_RESERVED_THREADS.getInt();
        long                          keepAliveTime   = AtlasConfiguration.WEBSERVER_KEEPALIVE_SECONDS.getLong();
        ThreadPoolExecutor            executor        = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, queue);
        executor.allowCoreThreadTimeOut(true);
        ExecutorThreadPool            pool            = new ExecutorThreadPool(executor, reservedThreads);

        server = new Server(pool);

        Connector connector = getConnector(host, port);
        connector.addBean(new JettyConnectionMetrics(getMeterRegistry()));
        new JettyServerThreadPoolMetrics(pool, Collections.emptyList()).bindTo(getMeterRegistry());
        registerQueueMetrics(queue, queueSize, executor);
        server.addConnector(connector);

        WebAppContext application = getWebAppContext(path);
        server.setHandler(application);

        LOG.info("Jetty configured: minThreads={}, maxThreads={}, reservedThreads={}, queueSize={}, keepAlive={}s, idleTimeout={}ms",
                minThreads, maxThreads, reservedThreads, queueSize, keepAliveTime,
                AtlasConfiguration.WEBSERVER_IDLE_TIMEOUT_MS.getLong());
    }

    private void registerQueueMetrics(LinkedBlockingQueue<Runnable> queue, int queueCapacity, ThreadPoolExecutor executor) {
        Gauge.builder("jetty.threads.queue.size", queue, LinkedBlockingQueue::size)
                .description("Number of requests queued waiting for a thread")
                .register(getMeterRegistry());

        Gauge.builder("jetty.threads.queue.capacity", () -> queueCapacity)
                .description("Maximum queue capacity")
                .register(getMeterRegistry());

        Gauge.builder("jetty.threads.queue.utilization", queue, q -> {
            int size = q.size();
            return queueCapacity > 0 ? (double) size / queueCapacity : 0;
        }).description("Queue utilization ratio (0.0 to 1.0)")
                .register(getMeterRegistry());

        Gauge.builder("jetty.threads.active", executor, ThreadPoolExecutor::getActiveCount)
                .description("Number of threads actively executing tasks")
                .register(getMeterRegistry());

        Gauge.builder("jetty.threads.pool.size", executor, ThreadPoolExecutor::getPoolSize)
                .description("Current number of threads in the pool")
                .register(getMeterRegistry());

        Gauge.builder("jetty.threads.completed.total", executor, e -> (double) e.getCompletedTaskCount())
                .description("Total number of tasks that have completed execution")
                .register(getMeterRegistry());
    }

    protected WebAppContext getWebAppContext(String path) {
        WebAppContext application = new WebAppContext(path, "/");
        application.setClassLoader(Thread.currentThread().getContextClassLoader());
        // Disable directory listing
        application.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
        return application;
    }

    public static EmbeddedServer newServer(String host, int port, String path, boolean secure)
            throws IOException {
        if (secure) {
            return new SecureEmbeddedServer(host, port, path);
        } else {
            return new EmbeddedServer(host, port, path);
        }
    }

    protected Connector getConnector(String host, int port) throws IOException {
        HttpConfiguration http_config = new HttpConfiguration();
        // this is to enable large header sizes when Kerberos is enabled with AD
        final int bufferSize = AtlasConfiguration.WEBSERVER_REQUEST_BUFFER_SIZE.getInt();;
        http_config.setResponseHeaderSize(bufferSize);
        http_config.setRequestHeaderSize(bufferSize);
        http_config.setSendServerVersion(false);

        ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(http_config));
        connector.setPort(port);
        connector.setHost(host);
        connector.setIdleTimeout(AtlasConfiguration.WEBSERVER_IDLE_TIMEOUT_MS.getLong());
        return connector;
    }

    public void start() throws AtlasBaseException {
        try {
            server.start();

            server.join();
        } catch(Exception e) {
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
}
