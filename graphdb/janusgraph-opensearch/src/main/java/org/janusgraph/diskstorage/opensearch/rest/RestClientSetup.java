// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.opensearch.rest;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.opensearch.client.RestClientBuilder.RequestConfigCallback;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.opensearch.OpenSearchClient;
import org.janusgraph.diskstorage.opensearch.OpenSearchIndex;
import org.janusgraph.diskstorage.opensearch.rest.util.BasicAuthHttpClientConfigCallback;
import org.janusgraph.diskstorage.opensearch.rest.util.ConnectionKeepAliveConfigCallback;
import org.janusgraph.diskstorage.opensearch.rest.util.HttpAuthTypes;
import org.janusgraph.diskstorage.opensearch.rest.util.RestClientAuthenticator;
import org.janusgraph.diskstorage.opensearch.rest.util.SSLConfigurationCallback;
import org.janusgraph.diskstorage.opensearch.rest.util.SSLConfigurationCallback.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_PORT;

/**
 * Create an instance of OpenSearch REST {@link org.opensearch.client.RestClient} from a JanusGraph
 * {@link org.janusgraph.diskstorage.configuration.Configuration}.
 */
public class RestClientSetup {

    private static final Logger log = LoggerFactory.getLogger(RestClientSetup.class);

    public OpenSearchClient connect(Configuration config) throws IOException {
        log.debug("Configuring RestClient");

        final List<HttpHost> hosts = new ArrayList<>();
        final int defaultPort = config.has(INDEX_PORT) ? config.get(INDEX_PORT) : OpenSearchIndex.HOST_PORT_DEFAULT;
        final String httpScheme = config.get(OpenSearchIndex.SSL_ENABLED) ? "https" : "http";
        for (String host : config.get(INDEX_HOSTS)) {
            String[] hostStringParts = host.split(":");
            String hostname = hostStringParts[0];
            int hostPort = defaultPort;
            if (hostStringParts.length == 2) hostPort = Integer.parseInt(hostStringParts[1]);
            log.debug("Configured remote host: {} : {}", hostname, hostPort);
            hosts.add(new HttpHost(httpScheme, hostname, hostPort));
        }

        final RestClient rc = getRestClient(hosts.toArray(new HttpHost[hosts.size()]), config);

        final int scrollKeepAlive = config.get(OpenSearchIndex.OS_SCROLL_KEEP_ALIVE);
        Preconditions.checkArgument(scrollKeepAlive >= 1, "Scroll keep-alive should be greater than or equal to 1");
        final boolean useMappingTypesForOS2 = config.get(OpenSearchIndex.USE_MAPPING_TYPES);
        int retryLimit = config.getOrDefault(OpenSearchIndex.RETRY_LIMIT);
        long retryInitialWaitMs = config.getOrDefault(OpenSearchIndex.RETRY_INITIAL_WAIT);
        long retryMaxWaitMs = config.getOrDefault(OpenSearchIndex.RETRY_MAX_WAIT);
        Set<Integer> errorCodesToRetry = Arrays.stream(config.getOrDefault(OpenSearchIndex.RETRY_ERROR_CODES))
            .mapToInt(Integer::parseInt).boxed().collect(Collectors.toSet());
        int bulkChunkLimitBytes = config.getOrDefault(OpenSearchIndex.BULK_CHUNK_SIZE_LIMIT_BYTES);
        final RestOpenSearchClient client = getOpenSearchClient(rc, scrollKeepAlive, useMappingTypesForOS2,
            retryLimit, errorCodesToRetry, retryInitialWaitMs, retryMaxWaitMs, bulkChunkLimitBytes);
        if (config.has(OpenSearchIndex.BULK_REFRESH)) {
            client.setBulkRefresh(config.get(OpenSearchIndex.BULK_REFRESH));
        }

        Integer retryOnConflict = config.has(OpenSearchIndex.RETRY_ON_CONFLICT) ? config.get(OpenSearchIndex.RETRY_ON_CONFLICT) : null;
        client.setRetryOnConflict(retryOnConflict);

        return client;
    }

    protected RestClient getRestClient(HttpHost[] hosts, Configuration config) {
        final RestClientBuilder restClientBuilder = getRestClientBuilder(hosts);

        final HttpClientConfigCallback httpClientConfigCallback = getHttpClientConfigCallback(config);
        if (httpClientConfigCallback != null) {
            restClientBuilder.setHttpClientConfigCallback(httpClientConfigCallback);
        }

        final RequestConfigCallback requestConfigCallback = getRequestConfigCallback(config);
        if (requestConfigCallback != null) {
            restClientBuilder.setRequestConfigCallback(requestConfigCallback);
        }

        return restClientBuilder.build();
    }

    protected RestClientBuilder getRestClientBuilder(HttpHost[] hosts) {
        return RestClient.builder(hosts);
    }

    protected RestOpenSearchClient getOpenSearchClient(RestClient rc, int scrollKeepAlive, boolean useMappingTypesForOS2,
                                                             int retryAttemptLimit, Set<Integer> retryOnErrorCodes, long retryInitialWaitMs,
                                                             long retryMaxWaitMs, int bulkChunkSerializedLimit) {
        return new RestOpenSearchClient(rc, scrollKeepAlive, useMappingTypesForOS2, retryAttemptLimit, retryOnErrorCodes,
            retryInitialWaitMs, retryMaxWaitMs, bulkChunkSerializedLimit);
    }

    /**
     * <p>
     * Returns the callback for customizing the {@link RequestConfig} or null if no
     * customization is needed.
     * </p>
     * <p>
     * See {@link RestClientBuilder#setRequestConfigCallback(RequestConfigCallback)} for more details.
     * </p>
     *
     * @param config
     *            ES index configuration
     * @return callback or null if the request customization is not needed
     */
    protected RequestConfigCallback getRequestConfigCallback(Configuration config) {

        final List<RequestConfigCallback> callbackList = new LinkedList<>();

        final Integer connectTimeout = config.get(OpenSearchIndex.CONNECT_TIMEOUT);
        final Integer socketTimeout = config.get(OpenSearchIndex.SOCKET_TIMEOUT);

        callbackList.add((requestConfigBuilder) ->
            requestConfigBuilder
                .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout))
                .setResponseTimeout(Timeout.ofMilliseconds(socketTimeout)));

        // will execute the chain of individual callbacks
        return requestConfigBuilder -> {
            for(RequestConfigCallback cb: callbackList) {
                cb.customizeRequestConfig(requestConfigBuilder);
            }
            return requestConfigBuilder;
        };
    }

    /**
     * <p>
     * Returns the callback for customizing {@link CloseableHttpAsyncClient} or null if no
     * customization is needed.
     * </p>
     * <p>
     * See {@link RestClientBuilder#setHttpClientConfigCallback(HttpClientConfigCallback)} for more details.
     * </p>
     *
     * @param config
     *            ES index configuration
     * @return callback or null if the client customization is not needed
     */
    protected HttpClientConfigCallback getHttpClientConfigCallback(Configuration config) {

        final List<HttpClientConfigCallback> callbackList = new LinkedList<>();

        final HttpAuthTypes authType = ConfigOption.getEnumValue(config.get(OpenSearchIndex.OS_HTTP_AUTH_TYPE),
                HttpAuthTypes.class);
        log.debug("Configuring HTTP(S) authentication type {}", authType);

        switch (authType) {
        case BASIC:
            callbackList.add(new BasicAuthHttpClientConfigCallback(
                    config.has(OpenSearchIndex.OS_HTTP_AUTH_REALM) ? config.get(OpenSearchIndex.OS_HTTP_AUTH_REALM) : "",
                    config.get(OpenSearchIndex.OS_HTTP_AUTH_USERNAME),
                    config.get(OpenSearchIndex.OS_HTTP_AUTH_PASSWORD)));
            break;
        case CUSTOM:
            callbackList.add(getCustomAuthenticator(
                    config.get(OpenSearchIndex.OS_HTTP_AUTHENTICATOR_CLASS),
                    config.get(OpenSearchIndex.OS_HTTP_AUTHENTICATOR_ARGS)));
            break;
        case NONE:
            break;
        default:
            // not expected
            throw new IllegalArgumentException("Authentication type \"" + authType + "\" is not implemented");
        }

        if (config.has(OpenSearchIndex.CLIENT_KEEP_ALIVE )) {
            callbackList.add(new ConnectionKeepAliveConfigCallback(config.get(OpenSearchIndex.CLIENT_KEEP_ALIVE)));
        }

        if (config.get(OpenSearchIndex.SSL_ENABLED)) {
            // Custom SSL configuration
            final Builder sslConfCBBuilder = getSSLConfigurationCallbackBuilder();
            boolean configureSSL = false;
            if (config.has(OpenSearchIndex.SSL_TRUSTSTORE_LOCATION)) {
                sslConfCBBuilder.withTrustStore(config.get(OpenSearchIndex.SSL_TRUSTSTORE_LOCATION),
                        config.get(OpenSearchIndex.SSL_TRUSTSTORE_PASSWORD));
                configureSSL = true;
            }
            if (config.has(OpenSearchIndex.SSL_KEYSTORE_LOCATION)) {
                final String keystorePassword = config.get(OpenSearchIndex.SSL_KEYSTORE_PASSWORD);
                sslConfCBBuilder.withKeyStore(config.get(OpenSearchIndex.SSL_KEYSTORE_LOCATION),
                        keystorePassword,
                        config.has(OpenSearchIndex.SSL_KEY_PASSWORD) ? config.get(OpenSearchIndex.SSL_KEY_PASSWORD) : keystorePassword);
                configureSSL = true;
            }

            if (config.has(OpenSearchIndex.SSL_DISABLE_HOSTNAME_VERIFICATION) &&
                config.get(OpenSearchIndex.SSL_DISABLE_HOSTNAME_VERIFICATION)) {
                log.warn("SSL hostname verification is disabled, OpenSearch HTTPS connections may not be secure");
                sslConfCBBuilder.disableHostNameVerification();
                configureSSL = true;
            }

            if (config.has(OpenSearchIndex.SSL_ALLOW_SELF_SIGNED_CERTIFICATES) &&
                config.get(OpenSearchIndex.SSL_ALLOW_SELF_SIGNED_CERTIFICATES)) {
                log.warn("Self-signed SSL certificate support is enabled, OpenSearch HTTPS connections may not be secure");
                sslConfCBBuilder.allowSelfSignedCertificates();
                configureSSL = true;
            }

            if (configureSSL) {
                callbackList.add(sslConfCBBuilder.build());
            }
        }

        if (callbackList.isEmpty()) {
            return null;
        }

        // will execute the chain of individual callbacks
        return httpClientBuilder -> {
            for(HttpClientConfigCallback cb: callbackList) {
                cb.customizeHttpClient(httpClientBuilder);
            }
            return httpClientBuilder;
        };
    }

    protected SSLConfigurationCallback.Builder getSSLConfigurationCallbackBuilder() {
        return SSLConfigurationCallback.Builder.create();
    }

    protected RestClientAuthenticator getCustomAuthenticator(String authClassName, String[] authClassConstructorArgList) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(authClassName),
                "Custom authenticator class name cannot be empty");
        Preconditions.checkNotNull(authClassConstructorArgList,
                "Custom authenticator class constructor argument list cannot be null");

        final RestClientAuthenticator authenticator;

        try {
            final Class<?> c = Class.forName(authClassName);
            Preconditions.checkArgument(RestClientAuthenticator.class.isAssignableFrom(c),
                "Authenticator class %s must be a subclass of %s", authClassName, RestClientAuthenticator.class.getName());
            @SuppressWarnings("unchecked")
            final Constructor<RestClientAuthenticator> ctr = ((Class<RestClientAuthenticator>)c).getConstructor(String[].class);
            authenticator = ctr.newInstance((Object)authClassConstructorArgList);
        } catch (Exception e) {
            log.error("Unable to instantiate the custom authenticator {} with constructor arguments \"{}\"",
                    authClassName, authClassConstructorArgList, e);
            throw new RuntimeException("Unable to instantiate the custom authenticator", e);
        }

        try {
            authenticator.init();
        } catch (IOException e) {
            log.error("Unable to initialize the custom authenticator {} with constructor arguments \"{}\"",
                    authClassName, authClassConstructorArgList, e);
            throw new RuntimeException("Unable to initialize the custom authenticator", e);
        }

        return authenticator;
    }
}
