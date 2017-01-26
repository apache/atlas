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
package org.apache.atlas.services;

import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MetricsServiceTest {
    private Configuration mockConfig = mock(Configuration.class);
    private AtlasTypeRegistry mockTypeRegistry = mock(AtlasTypeRegistry.class);
    private AtlasGraph mockGraph = mock(AtlasGraph.class);
    private MetricsService metricsService;

    private List<Map> mockMapList = new ArrayList<>();
    private Number mockCount = 10;

    @BeforeClass
    public void init() throws ScriptException {
        Map<String, Object> aMockMap = new HashMap<>();
        Map<String, Object> bMockMap = new HashMap<>();
        Map<String, Object> cMockMap = new HashMap<>();
        aMockMap.put("key", "a");
        aMockMap.put("value", 1);

        bMockMap.put("key", "b");
        bMockMap.put("value", 2);

        cMockMap.put("key", "c");
        cMockMap.put("value", 3);
        mockMapList.add(aMockMap);
        mockMapList.add(bMockMap);
        mockMapList.add(cMockMap);

        when(mockConfig.getInt(anyString(), anyInt())).thenReturn(5);
        when(mockConfig.getString(anyString(), anyString()))
                // we have seven count queries so stubbing 7 counts
                .thenReturn("dummyTestQuery.count()")
                .thenReturn("dummyTestQuery.count()")
                .thenReturn("dummyTestQuery.count()")
                .thenReturn("dummyTestQuery.count()")
                .thenReturn("dummyTestQuery.count()")
                .thenReturn("dummyTestQuery.count()")
                .thenReturn("dummyTestQuery.count()")
                // The last query is a map
                .thenReturn("dummyTestQuery");
        assertEquals(mockConfig.getInt("test", 1), 5);
        when(mockTypeRegistry.getAllEntityDefNames()).thenReturn(Arrays.asList("a", "b", "c"));
        setupMockGraph();
        metricsService = new MetricsService(mockConfig, mockGraph, mockTypeRegistry);
    }

    private void setupMockGraph() throws ScriptException {
        if (mockGraph == null) mockGraph = mock(AtlasGraph.class);
        when(mockGraph.executeGremlinScript(anyString(), eq(false))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (((String)invocationOnMock.getArguments()[0]).contains("count()")) {
                    return mockCount;
                } else {
                    return mockMapList;
                }
            }
        });
    }

    @Test
    public void testGetMetrics() throws InterruptedException, ScriptException {
        assertNotNull(metricsService);
        AtlasMetrics metrics = metricsService.getMetrics();
        assertNotNull(metrics);
        Number aCount = metrics.getMetric("entity", "a");
        assertNotNull(aCount);
        assertEquals(aCount, 10);

        Number bCount = metrics.getMetric("entity", "b");
        assertNotNull(bCount);
        assertEquals(bCount, 10);

        Number cCount = metrics.getMetric("entity", "c");
        assertNotNull(cCount);
        assertEquals(cCount, 10);

        Number aTags = metrics.getMetric("tag", "a");
        assertNotNull(aTags);
        assertEquals(aTags, 1);

        Number bTags = metrics.getMetric("tag", "b");
        assertNotNull(bTags);
        assertEquals(bTags, 2);

        Number cTags = metrics.getMetric("tag", "c");
        assertNotNull(cTags);
        assertEquals(cTags, 3);

        verify(mockGraph, atLeastOnce()).executeGremlinScript(anyString(), anyBoolean());

        // Subsequent call within the cache timeout window
        metricsService.getMetrics();
        verifyZeroInteractions(mockGraph);

        // Now test the cache refresh
        Thread.sleep(6000);
        metricsService.getMetrics();
        verify(mockGraph, atLeastOnce()).executeGremlinScript(anyString(), anyBoolean());
    }
}