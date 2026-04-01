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
package org.apache.atlas.repository.graphdb.elasticsearch;

import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;

public class AtlasElasticsearchIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasElasticsearchIndexClient.class);

    private static final long   HEALTH_CHECK_LOG_FREQUENCY_MS = 60000;
    private static long         prevHealthCheckTime;

    private final Configuration configuration;

    public AtlasElasticsearchIndexClient(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean isHealthy() {
        boolean isHealthy   = false;
        long    currentTime = System.currentTimeMillis();

        try {
            isHealthy = isElasticsearchHealthy();
        } catch (Exception exception) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Error: isHealthy", exception);
            }
        }

        if (!isHealthy && (prevHealthCheckTime == 0 || currentTime - prevHealthCheckTime > HEALTH_CHECK_LOG_FREQUENCY_MS)) {
            LOG.info("Index Health: Unhealthy!");
            prevHealthCheckTime = currentTime;
        }

        return isHealthy;
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> indexFieldName2SearchWeightMap) {
    }

    @Override
    public Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext) {
        return Collections.EMPTY_MAP;
    }

    @Override
    public void applySuggestionFields(String collectionName, List<String> suggestionProperties) {
        LOG.info("Applied suggestion fields request handler for collection {}.", collectionName);
    }

    @Override
    public List<String> getSuggestions(String prefixString, String indexFieldName) {
        return Collections.EMPTY_LIST;
    }

    private boolean isElasticsearchHealthy() throws ElasticsearchException, IOException {
        RestHighLevelClient client = AtlasElasticsearchDatabase.getClient();
        ClusterHealthRequest request = new ClusterHealthRequest(Constants.INDEX_PREFIX + Constants.VERTEX_INDEX);
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        RestStatus restStatus = response.status();
        if (restStatus.toString().equals(ELASTICSEARCH_REST_STATUS_OK)){
            ClusterHealthStatus status = response.getStatus();
            if (status.toString().equals(ELASTICSEARCH_CLUSTER_STATUS_GREEN) || status.toString().equals(ELASTICSEARCH_CLUSTER_STATUS_YELLOW)) {
                return true;
            }
        } else {
            LOG.error("isElasticsearchHealthy => ES health check request timed out!");
        }
        return false;
    }
}
