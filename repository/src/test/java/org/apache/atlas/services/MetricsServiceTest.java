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
package org.apache.atlas.services;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlin3QueryProvider;
import org.apache.commons.configuration.Configuration;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MetricsServiceTest {
    private Configuration             mockConfig       = mock(Configuration.class);
    private Configuration             mockConfig1      = mock(Configuration.class);
    private AtlasTypeRegistry         mockTypeRegistry = mock(AtlasTypeRegistry.class);
    private AtlasGraph                mockGraph        = mock(AtlasGraph.class);
    private AtlasGraph                mockGraph1       = mock(AtlasGraph.class);
    private MetricsService            metricsService;
    private MetricsService            metricsService1;

    private List<Map> activeEntityCountList = new ArrayList<>();
    private List<Map> deletedEntityCountList = new ArrayList<>();

    @BeforeClass
    public void init() throws Exception {
        try {
            Map<String, Object> activeEntityCount = new HashMap<>();
            activeEntityCount.put("a", 1);
            activeEntityCount.put("b", 2);
            activeEntityCount.put("d", 5);
            activeEntityCount.put("e", 10);
            activeEntityCount.put("f", 15);
            activeEntityCountList.add(activeEntityCount);

            Map<String, Object> deletedEntityCount = new HashMap<>();
            deletedEntityCount.put("b", 5);
            deletedEntityCountList.add(deletedEntityCount);

            when(mockConfig.getInt(eq(MetricsService.METRIC_QUERY_GREMLIN_TYPES_BATCH_SIZE), anyInt())).thenReturn(25);
            when(mockConfig.getInt(eq(MetricsService.METRIC_QUERY_CACHE_TTL), anyInt())).thenReturn(900);
            when(mockConfig1.getInt(eq(MetricsService.METRIC_QUERY_GREMLIN_TYPES_BATCH_SIZE), anyInt())).thenReturn(2);

            when(mockTypeRegistry.getAllTypeNames()).thenReturn(Arrays.asList("a", "b", "c", "d", "e", "f"));
            when(mockTypeRegistry.getAllEntityDefNames()).thenReturn(Arrays.asList("a", "b", "c"));
            when(mockTypeRegistry.getAllClassificationDefNames()).thenReturn(Arrays.asList("d", "e", "f"));

            setupMockGraph();

            metricsService = new MetricsService(mockConfig, mockGraph, mockTypeRegistry, new AtlasGremlin3QueryProvider());
            metricsService1 = new MetricsService(mockConfig1, mockGraph1, mockTypeRegistry, new AtlasGremlin3QueryProvider());
        }
        catch(Exception e) {
            throw new SkipException("MetricsServicesTest: init failed!", e);
        }
    }

    @AfterClass
    public void cleanup() throws Exception {
        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    private void setupMockGraph() throws AtlasBaseException {
        if (mockGraph == null) mockGraph = mock(AtlasGraph.class);
        if (mockGraph1 == null) mockGraph1 = mock(AtlasGraph.class);
        when(mockGraph.executeGremlinScript(anyString(), eq(false))).thenAnswer(invocationOnMock -> {
            if (((String)invocationOnMock.getArguments()[0]).contains("ACTIVE")) {
                return activeEntityCountList;
            } else {
                return deletedEntityCountList;
            }
        });
        when(mockGraph1.executeGremlinScript(anyString(), eq(false))).thenAnswer(invocationOnMock -> {
            if (((String)invocationOnMock.getArguments()[0]).contains("ACTIVE")) {
                return activeEntityCountList;
            } else {
                return deletedEntityCountList;
            }
        });
    }

    @Test
    public void testGetMetrics() throws InterruptedException, AtlasBaseException {
        assertNotNull(metricsService);
        AtlasMetrics metrics = metricsService.getMetrics(false);
        assertNotNull(metrics);
        Map activeMetrics = (Map) metrics.getMetric("entity", "entityActive");
        assertNotNull(activeMetrics);
        assertEquals(activeMetrics.get("a"), 1);
        assertEquals(activeMetrics.get("b"), 2);

        Map deletedMetrics = (Map) metrics.getMetric("entity", "entityDeleted");
        assertEquals(deletedMetrics.get("b"), 5);

        Number unusedTypeCount = metrics.getNumericMetric("general", "typeUnusedCount");
        assertEquals(unusedTypeCount, 1);

        Number cCount = metrics.getNumericMetric("general", "entityCount");
        assertEquals(cCount, 8);

        Number aTags = (Number) metrics.getMetric("general", "tagCount");
        assertEquals(aTags, 3);

        Map taggedEntityMetric = (Map) metrics.getMetric("tag", "tagEntities");
        assertNotNull(taggedEntityMetric);
        assertEquals(taggedEntityMetric.get("d"), 5);
        assertEquals(taggedEntityMetric.get("e"), 10);
        assertEquals(taggedEntityMetric.get("f"), 15);

        // 2 calls for entity types and 2 calls for classification types
        verify(mockGraph, times(4)).executeGremlinScript(anyString(), anyBoolean());

        // Test batched calls
        metricsService1.getMetrics(false);
        // 3 classifications, 3 entity types & batch size = 2 and 2 calls per batch, total batches = 4, total calls = 8
        // 2 for entity and 2 for classification
        verify(mockGraph1, times(8)).executeGremlinScript(anyString(), anyBoolean());

        // Subsequent call within the cache timeout window
        metricsService.getMetrics(false);
        verifyZeroInteractions(mockGraph);

        // Now test the cache refresh
        Thread.sleep(6000);
        metricsService.getMetrics(true);
        verify(mockGraph, atLeastOnce()).executeGremlinScript(anyString(), anyBoolean());
    }
}
