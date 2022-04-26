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
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AtlasElasticsearchDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasElasticsearchDatabase.class);

    private static volatile RestHighLevelClient searchClient;
    private static volatile RestClient lowLevelClient;
    public static final String INDEX_BACKEND_CONF = "atlas.graph.index.search.hostname";

    public static List<HttpHost> getHttpHosts() throws AtlasException {
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

    public static RestHighLevelClient getClient() {
        if (searchClient == null) {
            synchronized (AtlasElasticsearchDatabase.class) {
                if (searchClient == null) {
                    try {
                        List<HttpHost> httpHosts = getHttpHosts();

                        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]))
                                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(900000)
                                        .setSocketTimeout(900000));
                        searchClient =
                                new RestHighLevelClient(restClientBuilder);
                    } catch (AtlasException e) {
                        LOG.error("Failed to initialize high level client for ES");
                    }
                }
            }
        }
        return searchClient;
    }

    public static RestClient getLowLevelClient() {
        if (lowLevelClient == null) {
            synchronized (AtlasElasticsearchDatabase.class) {
                if (lowLevelClient == null) {
                    try {
                        List<HttpHost> httpHosts = getHttpHosts();

                        RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
                        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setKeepAliveStrategy(((httpResponse, httpContext) -> 3600000)));
                        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                                .setConnectTimeout(900000)
                                .setSocketTimeout(900000));

                        lowLevelClient = builder.build();
                    } catch (AtlasException e) {
                        LOG.error("Failed to initialize low level rest client for ES");
                    }
                }
            }
        }
        return lowLevelClient;
    }
}
