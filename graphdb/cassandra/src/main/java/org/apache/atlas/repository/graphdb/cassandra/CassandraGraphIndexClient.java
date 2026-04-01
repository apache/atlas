package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.util.*;

public class CassandraGraphIndexClient implements AtlasGraphIndexClient {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphIndexClient.class);

    private final CassandraGraph graph;

    public CassandraGraphIndexClient(CassandraGraph graph) {
        this.graph = graph;
    }

    @Override
    public Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext) {
        // TODO: implement via ES REST client
        return Collections.emptyMap();
    }

    @Override
    public List<String> getSuggestions(String prefixString, String indexFieldName) {
        // TODO: implement via ES REST client
        return Collections.emptyList();
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> indexFieldName2SearchWeightMap) {
        LOG.debug("applySearchWeight for collection: {}", collectionName);
    }

    @Override
    public void applySuggestionFields(String collectionName, List<String> suggestionProperties) {
        LOG.debug("applySuggestionFields for collection: {}", collectionName);
    }

    @Override
    public boolean isHealthy() {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            Response response = client.performRequest(new Request("GET", "/_cluster/health"));
            return response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            LOG.warn("ES health check failed: {}", e.getMessage());
            return false;
        }
    }
}
