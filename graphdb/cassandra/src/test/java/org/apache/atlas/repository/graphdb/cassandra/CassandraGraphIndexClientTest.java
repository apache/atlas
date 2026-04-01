package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraGraphIndexClient.
 * Covers: aggregated metrics, suggestions, search weight, suggestion fields, health check.
 */
public class CassandraGraphIndexClientTest {

    private CassandraGraph mockGraph;
    private CassandraGraphIndexClient client;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
        client = new CassandraGraphIndexClient(mockGraph);
    }

    @Test
    public void testGetAggregatedMetricsReturnsEmpty() {
        Map<String, List<AtlasAggregationEntry>> result = client.getAggregatedMetrics(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetSuggestionsReturnsEmpty() {
        List<String> result = client.getSuggestions("prefix", "fieldName");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testApplySearchWeightNoOp() {
        Map<String, Integer> weightMap = new HashMap<>();
        weightMap.put("name", 10);
        client.applySearchWeight("vertex_index", weightMap); // should not throw
    }

    @Test
    public void testApplySuggestionFieldsNoOp() {
        client.applySuggestionFields("vertex_index", Arrays.asList("name", "description")); // should not throw
    }

    @Test
    public void testIsHealthy() {
        assertTrue(client.isHealthy());
    }
}
