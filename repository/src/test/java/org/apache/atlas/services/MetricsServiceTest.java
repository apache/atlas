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