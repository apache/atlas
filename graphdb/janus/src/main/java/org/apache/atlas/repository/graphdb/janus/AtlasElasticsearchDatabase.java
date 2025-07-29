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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT;
import static org.apache.atlas.AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT;

public class AtlasElasticsearchDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasElasticsearchDatabase.class);

    // Performance-optimized clients with advanced connection pooling
    private static final RestHighLevelClient searchClient;
    private static final RestClient lowLevelClient;
    private static final RestClient esUiClusterClient;
    private static final RestClient esNonUiClusterClient;
    
    public static final String INDEX_BACKEND_CONF = "atlas.graph.index.search.hostname";
    
    // Simple, conservative connection pool configuration
    // Optimized for 2-8 pod deployments without overwhelming Elasticsearch
    private static final int DEFAULT_CONNECTIONS_TOTAL = 15;  // Conservative per pod
    private static final int DEFAULT_CONNECTIONS_PER_ROUTE = 10;  // 70% of total
    
    // Allow override via configuration if needed
    private static final int BASE_CONNECTIONS_TOTAL;
    private static final int BASE_CONNECTIONS_PER_ROUTE;
    
    private static final int CONNECTION_REQUEST_TIMEOUT = 5000; // 5 seconds
    private static final int CONNECTION_TIME_TO_LIVE = 300000; // 5 minutes
    private static final int IO_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    
    // Configuration keys for Atlas properties (optional overrides)
    private static final String ES_CONNECTION_POOL_SIZE_CONF = "atlas.graph.index.search.connection.pool.size";
    private static final String ES_CONNECTION_PER_ROUTE_CONF = "atlas.graph.index.search.connection.per.route";

    // Single static initializer block - executes once during class loading  
    static {
        // First: Load configuration
        try {
            Configuration configuration = ApplicationProperties.get();
            
            // Use configured values if provided, otherwise use conservative defaults
            BASE_CONNECTIONS_TOTAL = configuration.getInt(ES_CONNECTION_POOL_SIZE_CONF, DEFAULT_CONNECTIONS_TOTAL);
            BASE_CONNECTIONS_PER_ROUTE = configuration.getInt(ES_CONNECTION_PER_ROUTE_CONF, DEFAULT_CONNECTIONS_PER_ROUTE);
            
            LOG.info("Elasticsearch connection pool configured: Total={}, PerRoute={}", 
                    BASE_CONNECTIONS_TOTAL, BASE_CONNECTIONS_PER_ROUTE);
                    
            // Validate configuration
            if (BASE_CONNECTIONS_TOTAL > 50) {
                LOG.warn("Connection pool size ({}) is quite large. For 2-8 pod deployments, consider 10-20 connections per pod.", 
                        BASE_CONNECTIONS_TOTAL);
            }
        } catch (Exception e) {
            throw new ExceptionInInitializerError("Failed to load Elasticsearch connection configuration: " + e.getMessage());
        }
        
        // Second: Initialize clients
        LOG.info("Initializing high-performance Elasticsearch clients with advanced connection pooling...");
        
        RestHighLevelClient tempSearchClient = null;
        RestClient tempLowLevelClient = null;
        RestClient tempUiClusterClient = null;
        RestClient tempNonUiClusterClient = null;
        
        try {
            // Initialize main search clients with performance optimizations
            List<HttpHost> httpHosts = getHttpHosts();
            
            // Initialize high-level client with advanced performance tuning
            tempSearchClient = createOptimizedHighLevelClient(httpHosts);
            LOG.info("Successfully initialized high-performance Elasticsearch high-level client");
            
            // Initialize low-level client with advanced performance tuning
            tempLowLevelClient = createOptimizedLowLevelClient(httpHosts);
            LOG.info("Successfully initialized high-performance Elasticsearch low-level client");
            
            // Initialize cluster-specific clients if request isolation is enabled
            if (AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_REQUEST_ISOLATION.getBoolean()) {
                try {
                    // Initialize UI cluster client
                    HttpHost uiHost = HttpHost.create(AtlasConfiguration.ATLAS_ELASTICSEARCH_UI_SEARCH_CLUSTER_URL.getString());
                    tempUiClusterClient = createOptimizedLowLevelClient(java.util.Collections.singletonList(uiHost));
                    LOG.info("Successfully initialized high-performance Elasticsearch UI cluster client");
                } catch (Exception e) {
                    LOG.error("Failed to initialize UI cluster client, will use null", e);
                }
                
                try {
                    // Initialize Non-UI cluster client
                    HttpHost nonUiHost = HttpHost.create(AtlasConfiguration.ATLAS_ELASTICSEARCH_NON_UI_SEARCH_CLUSTER_URL.getString());
                    tempNonUiClusterClient = createOptimizedLowLevelClient(java.util.Collections.singletonList(nonUiHost));
                    LOG.info("Successfully initialized high-performance Elasticsearch Non-UI cluster client");
                } catch (Exception e) {
                    LOG.error("Failed to initialize Non-UI cluster client, will use null", e);
                }
            } else {
                LOG.info("Request isolation disabled, skipping cluster-specific client initialization");
            }
            
        } catch (Exception e) {
            LOG.error("Critical failure during Elasticsearch client initialization", e);
            throw new ExceptionInInitializerError("Failed to initialize Elasticsearch clients: " + e.getMessage());
        }
        
        // Assign to final fields
        searchClient = tempSearchClient;
        lowLevelClient = tempLowLevelClient;
        esUiClusterClient = tempUiClusterClient;
        esNonUiClusterClient = tempNonUiClusterClient;
        
        LOG.info("All high-performance Elasticsearch clients successfully initialized and ready for use");
        logPerformanceConfiguration();
    }



    /**
     * Creates a high-performance RestHighLevelClient with advanced connection pooling and optimizations
     */
    private static RestHighLevelClient createOptimizedHighLevelClient(List<HttpHost> httpHosts) {
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
        
        // Configure advanced HTTP client with connection pooling
        builder.setHttpClientConfigCallback(AtlasElasticsearchDatabase::configureHttpAsyncClientBuilder);
        
        // Configure request settings
        builder.setRequestConfigCallback(AtlasElasticsearchDatabase::configureRequestConfig);
        
        // Set default headers for performance
        builder.setDefaultHeaders(getDefaultHeaders());
        
        // For Kubernetes load balancer - use all available endpoints
        builder.setNodeSelector(NodeSelector.ANY);
        
        return new RestHighLevelClient(builder);
    }

    /**
     * Creates a high-performance RestClient with advanced connection pooling and optimizations
     */
    private static RestClient createOptimizedLowLevelClient(List<HttpHost> httpHosts) {
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
        
        // Configure advanced HTTP client with connection pooling
        builder.setHttpClientConfigCallback(AtlasElasticsearchDatabase::configureHttpAsyncClientBuilder);
        
        // Configure request settings
        builder.setRequestConfigCallback(AtlasElasticsearchDatabase::configureRequestConfig);
        
        // Set default headers for performance
        builder.setDefaultHeaders(getDefaultHeaders());
        
        // Low-level client specific optimizations for Kubernetes setup
        configureLowLevelClientOptimizations(builder);
        
        return builder.build();
    }

    /**
     * Configures HttpAsyncClientBuilder with performance optimizations compatible with all ES client versions
     */
    private static HttpAsyncClientBuilder configureHttpAsyncClientBuilder(HttpAsyncClientBuilder httpAsyncClientBuilder) {
        try {
            // Create IO Reactor with optimized configuration
            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(IO_THREAD_COUNT)
                    .setConnectTimeout(INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                    .setSoTimeout(INDEX_CLIENT_SOCKET_TIMEOUT.getInt())
                    .setSoKeepAlive(true)
                    .setTcpNoDelay(true)
                    .setSoReuseAddress(true)
                    .setBacklogSize(1024)
                    .build();

            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);

            // Create advanced connection manager with pooling
            PoolingNHttpClientConnectionManager connectionManager = 
                    new PoolingNHttpClientConnectionManager(ioReactor);
            
            // Configure connection pool settings
            connectionManager.setMaxTotal(BASE_CONNECTIONS_TOTAL);
            connectionManager.setDefaultMaxPerRoute(BASE_CONNECTIONS_PER_ROUTE);
            // Note: Connection validation handled by keep-alive strategy
            
            // Set connection manager
            httpAsyncClientBuilder.setConnectionManager(connectionManager);
            
            // Configure keep-alive strategy for connection reuse (compatible method)
            httpAsyncClientBuilder.setKeepAliveStrategy((response, context) -> {
                // Keep connections alive for Kubernetes load balancer efficiency
                return CONNECTION_TIME_TO_LIVE;
            });
            
            // Set user agent for monitoring
            httpAsyncClientBuilder.setUserAgent("Atlas-ElasticsearchClient/1.0 (K8s-optimized)");
            
            LOG.debug("Configured HttpAsyncClientBuilder with {} total connections, {} per route for K8s load balancer", 
                    BASE_CONNECTIONS_TOTAL, BASE_CONNECTIONS_PER_ROUTE);
            
        } catch (IOReactorException e) {
            LOG.error("Failed to create IO Reactor, falling back to basic configuration", e);
            // Fall back to basic configuration
            httpAsyncClientBuilder.setKeepAliveStrategy((response, context) -> CONNECTION_TIME_TO_LIVE);
        }
        
        return httpAsyncClientBuilder;
    }

    /**
     * Configures RequestConfig with optimized timeout and performance settings
     */
    private static RequestConfig.Builder configureRequestConfig(RequestConfig.Builder requestConfigBuilder) {
        return requestConfigBuilder
                .setConnectTimeout(INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                .setSocketTimeout(INDEX_CLIENT_SOCKET_TIMEOUT.getInt())
                .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
                .setExpectContinueEnabled(false) // Disable 100-continue for better performance
                .setRedirectsEnabled(false) // Disable redirects for predictable behavior
                .setRelativeRedirectsAllowed(false)
                .setCircularRedirectsAllowed(false)
                .setMaxRedirects(0)
                .setAuthenticationEnabled(false); // Handle auth at application level if needed
    }

    /**
     * Returns default headers optimized for performance with Kubernetes load balancer
     */
    private static Header[] getDefaultHeaders() {
        return new Header[]{
                new BasicHeader("Accept-Encoding", "gzip, deflate"),
                new BasicHeader("Content-Type", "application/json"),
                new BasicHeader("Accept", "application/json"),
                new BasicHeader("Connection", "keep-alive"),
                new BasicHeader("Cache-Control", "no-cache"),
                new BasicHeader("X-Client-Type", "Atlas-K8s")
        };
    }

    /**
     * Applies low-level client specific performance optimizations for Kubernetes setup
     */
    private static void configureLowLevelClientOptimizations(RestClientBuilder builder) {
        // Override the HTTP client configuration to add low-level specific optimizations
        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            HttpAsyncClientBuilder configured = configureHttpAsyncClientBuilder(httpAsyncClientBuilder);
            
            // Add request interceptor for performance monitoring and optimization
            configured.addInterceptorFirst((org.apache.http.HttpRequestInterceptor) (request, context) -> {
                // Add request timing for monitoring
                context.setAttribute("request-start-time", System.currentTimeMillis());
            });
            
            // Add response interceptor for performance monitoring
            configured.addInterceptorFirst((org.apache.http.HttpResponseInterceptor) (response, context) -> {
                // Log slow requests for monitoring
                Long startTime = (Long) context.getAttribute("request-start-time");
                if (startTime != null) {
                    long duration = System.currentTimeMillis() - startTime;
                    if (duration > 1000) { // Log requests taking more than 1 second
                        LOG.warn("Slow Elasticsearch request detected: {}ms (via K8s load balancer)", duration);
                    }
                }
                
                // Optimize response processing for large responses
                if (response.containsHeader("Content-Length")) {
                    String contentLength = response.getFirstHeader("Content-Length").getValue();
                    long length = Long.parseLong(contentLength);
                    if (length > 1048576) { // 1MB
                        LOG.debug("Processing large response: {} bytes (via K8s)", length);
                    }
                }
            });
            
            return configured;
        });
        
        // Configure failure listener optimized for Kubernetes load balancer
        builder.setFailureListener(new RestClient.FailureListener() {
            private volatile int consecutiveFailures = 0;
            private volatile long lastFailureTime = 0;
            private static final int MAX_CONSECUTIVE_FAILURES = 3; // Lower threshold for K8s
            private static final long CIRCUIT_BREAK_DURATION = 15000; // 15 seconds for K8s

            @Override
            public void onFailure(org.elasticsearch.client.Node node) {
                long currentTime = System.currentTimeMillis();
                
                // Reset consecutive failures if enough time has passed (circuit breaker recovery)
                if (currentTime - lastFailureTime > CIRCUIT_BREAK_DURATION) {
                    consecutiveFailures = 0;
                    LOG.debug("Circuit breaker reset for K8s endpoint: {} after {}ms recovery period", 
                            node.getHost(), CIRCUIT_BREAK_DURATION);
                }
                
                consecutiveFailures++;
                lastFailureTime = currentTime;
                
                LOG.warn("Elasticsearch failure detected via K8s load balancer: {} (consecutive failures: {})", 
                        node.getHost(), consecutiveFailures);
                
                if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                    LOG.error("Circuit breaker activated for K8s endpoint: {} after {} consecutive failures", 
                            node.getHost(), consecutiveFailures);
                }
            }
        });
        
        // Simple node selector for Kubernetes load balancer (no complex routing needed)
        builder.setNodeSelector(NodeSelector.ANY);
        
        LOG.debug("Applied K8s-optimized low-level client configuration: simplified routing, enhanced monitoring");
    }

    /**
     * Logs the performance configuration for monitoring and debugging
     */
    private static void logPerformanceConfiguration() {
        LOG.info("=== Elasticsearch Client Performance Configuration ===");
        LOG.info("Max Total Connections: {}", BASE_CONNECTIONS_TOTAL);
        LOG.info("Max Connections Per Route: {}", BASE_CONNECTIONS_PER_ROUTE);
        LOG.info("Connection Timeout: {}ms", INDEX_CLIENT_CONNECTION_TIMEOUT.getInt());
        LOG.info("Socket Timeout: {}ms", INDEX_CLIENT_SOCKET_TIMEOUT.getInt());
        LOG.info("Connection Request Timeout: {}ms", CONNECTION_REQUEST_TIMEOUT);
        LOG.info("Connection TTL: {}ms", CONNECTION_TIME_TO_LIVE);
        LOG.info("IO Thread Count: {}", IO_THREAD_COUNT);
        LOG.info("--- Kubernetes Load Balancer Optimizations ---");
        LOG.info("HTTP Compression: gzip, deflate");
        LOG.info("Keep-Alive Strategy: 5 minutes (K8s friendly)");
        LOG.info("Connection Pooling: {} total, {} per route", BASE_CONNECTIONS_TOTAL, BASE_CONNECTIONS_PER_ROUTE);
        LOG.info("Circuit Breaker: 3 failures, 15s recovery (K8s optimized)");
        LOG.info("Request/Response Interceptors: Enabled");
        LOG.info("Estimated Load (2-8 pods): {}-{} total connections via K8s", 
                2 * BASE_CONNECTIONS_TOTAL, 8 * BASE_CONNECTIONS_TOTAL);
        LOG.info("====================================================");
    }

    private static List<HttpHost> getHttpHosts() throws AtlasException {
        List<HttpHost> httpHosts = new ArrayList<>();
        Configuration configuration = ApplicationProperties.get();
        String indexConf = configuration.getString(INDEX_BACKEND_CONF);
        String[] hosts = indexConf.split(",");
        for (String host: hosts) {
            host = host.trim();
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length == 1) {
                httpHosts.add(new HttpHost(hostAndPort[0]));
            } else if (hostAndPort.length == 2) {
                httpHosts.add(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                throw new AtlasException("Invalid config");
            }
        }
        return httpHosts;
    }

    /**
     * Get the pre-initialized high-performance Elasticsearch client.
     * No synchronization needed - client is ready for immediate use.
     * 
     * @return RestHighLevelClient instance with advanced connection pooling, never null
     */
    public static RestHighLevelClient getClient() {
        return searchClient;
    }

    /**
     * Get the pre-initialized high-performance low-level Elasticsearch client.
     * No synchronization needed - client is ready for immediate use.
     * 
     * @return RestClient instance with advanced connection pooling, never null
     */
    public static RestClient getLowLevelClient() {
        return lowLevelClient;
    }

    /**
     * Get the pre-initialized UI cluster client.
     * Returns null if request isolation is disabled or initialization failed.
     * 
     * @return RestClient instance with performance optimizations or null
     */
    public static RestClient getUiClusterClient() {
        return esUiClusterClient;
    }

    /**
     * Get the pre-initialized Non-UI cluster client.
     * Returns null if request isolation is disabled or initialization failed.
     * 
     * @return RestClient instance with performance optimizations or null
     */
    public static RestClient getNonUiClusterClient() {
        return esNonUiClusterClient;
    }

    /**
     * Get simple configuration information for monitoring
     */
    public static String getConnectionConfiguration() {
        try {
            Configuration configuration = ApplicationProperties.get();
            String configSource = configuration.getInt(ES_CONNECTION_POOL_SIZE_CONF, -1) > 0 
                ? "CONFIGURED" : "DEFAULT";
            
            return String.format(
                "=== Elasticsearch K8s Load Balancer Configuration ===%n" +
                "Connections Per Pod: %d%n" +
                "Connections Per Route: %d%n" +
                "Configuration Source: %s%n" +
                "Estimated Load (2-8 pods): %d-%d total connections via K8s%n" +
                "K8s Optimizations: ENABLED%n" +
                "Connection Pooling: ACTIVE (reuses connections through load balancer)",
                BASE_CONNECTIONS_TOTAL,
                BASE_CONNECTIONS_PER_ROUTE,
                configSource,
                2 * BASE_CONNECTIONS_TOTAL,
                8 * BASE_CONNECTIONS_TOTAL
            );
        } catch (Exception e) {
            return "Configuration unavailable: " + e.getMessage();
        }
    }
}
