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
package org.apache.atlas.model.discovery;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestAtlasQuickSearchResult {
    private AtlasQuickSearchResult quickSearchResult;

    @BeforeMethod
    public void setUp() {
        quickSearchResult = new AtlasQuickSearchResult();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasQuickSearchResult result = new AtlasQuickSearchResult();

        assertNull(result.getSearchResults());
        assertNull(result.getAggregationMetrics());
    }

    @Test
    public void testParameterizedConstructor() {
        AtlasSearchResult searchResults = new AtlasSearchResult();
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        AtlasQuickSearchResult result = new AtlasQuickSearchResult(searchResults, aggregationMetrics);

        assertSame(result.getSearchResults(), searchResults);
        assertSame(result.getAggregationMetrics(), aggregationMetrics);
    }

    @Test
    public void testParameterizedConstructorWithNullValues() {
        AtlasQuickSearchResult result = new AtlasQuickSearchResult(null, null);

        assertNull(result.getSearchResults());
        assertNull(result.getAggregationMetrics());
    }

    @Test
    public void testSearchResultsGetterSetter() {
        AtlasSearchResult searchResults = new AtlasSearchResult();

        quickSearchResult.setSearchResults(searchResults);
        assertSame(quickSearchResult.getSearchResults(), searchResults);

        quickSearchResult.setSearchResults(null);
        assertNull(quickSearchResult.getSearchResults());
    }

    @Test
    public void testAggregationMetricsGetterSetter() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        quickSearchResult.setAggregationMetrics(aggregationMetrics);
        assertSame(quickSearchResult.getAggregationMetrics(), aggregationMetrics);

        quickSearchResult.setAggregationMetrics(null);
        assertNull(quickSearchResult.getAggregationMetrics());
    }

    @Test
    public void testSettersReturnVoid() {
        AtlasSearchResult searchResults = new AtlasSearchResult();
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        quickSearchResult.setSearchResults(searchResults);
        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertNotNull(quickSearchResult);
    }

    @Test
    public void testCompleteQuickSearchResult() {
        AtlasSearchResult searchResults = new AtlasSearchResult();
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        List<AtlasAggregationEntry> typeMetrics = new ArrayList<>();
        AtlasAggregationEntry typeEntry = new AtlasAggregationEntry();
        typeEntry.setCount(10L);
        typeMetrics.add(typeEntry);

        aggregationMetrics.put("entityType", typeMetrics);

        quickSearchResult.setSearchResults(searchResults);
        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertSame(quickSearchResult.getSearchResults(), searchResults);
        assertSame(quickSearchResult.getAggregationMetrics(), aggregationMetrics);
    }

    @Test
    public void testEmptyAggregationMetrics() {
        Map<String, List<AtlasAggregationEntry>> emptyMetrics = new HashMap<>();

        quickSearchResult.setAggregationMetrics(emptyMetrics);

        assertNotNull(quickSearchResult.getAggregationMetrics());
        assertEquals(quickSearchResult.getAggregationMetrics().size(), 0);
        assertEquals(quickSearchResult.getAggregationMetrics().isEmpty(), true);
    }

    @Test
    public void testMultipleAggregationMetrics() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        List<AtlasAggregationEntry> typeMetrics = new ArrayList<>();
        AtlasAggregationEntry dbEntry = new AtlasAggregationEntry();
        dbEntry.setCount(5L);
        typeMetrics.add(dbEntry);

        AtlasAggregationEntry tableEntry = new AtlasAggregationEntry();
        tableEntry.setCount(15L);
        typeMetrics.add(tableEntry);

        List<AtlasAggregationEntry> classificationMetrics = new ArrayList<>();
        AtlasAggregationEntry piiEntry = new AtlasAggregationEntry();
        piiEntry.setCount(3L);
        classificationMetrics.add(piiEntry);

        aggregationMetrics.put("entityType", typeMetrics);
        aggregationMetrics.put("classification", classificationMetrics);

        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertEquals(quickSearchResult.getAggregationMetrics().size(), 2);
        assertEquals(quickSearchResult.getAggregationMetrics().get("entityType").size(), 2);
        assertEquals(quickSearchResult.getAggregationMetrics().get("classification").size(), 1);
    }

    @Test
    public void testAggregationMetricsWithEmptyLists() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        aggregationMetrics.put("entityType", new ArrayList<>());
        aggregationMetrics.put("classification", new ArrayList<>());

        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertNotNull(quickSearchResult.getAggregationMetrics());
        assertEquals(quickSearchResult.getAggregationMetrics().size(), 2);
        assertEquals(quickSearchResult.getAggregationMetrics().get("entityType").size(), 0);
        assertEquals(quickSearchResult.getAggregationMetrics().get("classification").size(), 0);
    }

    @Test
    public void testAggregationMetricsWithNullLists() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        aggregationMetrics.put("entityType", null);
        aggregationMetrics.put("classification", null);

        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertNotNull(quickSearchResult.getAggregationMetrics());
        assertEquals(quickSearchResult.getAggregationMetrics().size(), 2);
        assertNull(quickSearchResult.getAggregationMetrics().get("entityType"));
        assertNull(quickSearchResult.getAggregationMetrics().get("classification"));
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(quickSearchResult);
    }

    @Test
    public void testObjectState() {
        AtlasSearchResult searchResults = new AtlasSearchResult();
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        // Initially null
        assertNull(quickSearchResult.getSearchResults());
        assertNull(quickSearchResult.getAggregationMetrics());

        // Set values
        quickSearchResult.setSearchResults(searchResults);
        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        // Verify state
        assertNotNull(quickSearchResult.getSearchResults());
        assertNotNull(quickSearchResult.getAggregationMetrics());
        assertSame(quickSearchResult.getSearchResults(), searchResults);
        assertSame(quickSearchResult.getAggregationMetrics(), aggregationMetrics);

        // Reset to null
        quickSearchResult.setSearchResults(null);
        quickSearchResult.setAggregationMetrics(null);

        // Verify null state
        assertNull(quickSearchResult.getSearchResults());
        assertNull(quickSearchResult.getAggregationMetrics());
    }

    @Test
    public void testSearchResultsIndependence() {
        AtlasSearchResult searchResults = new AtlasSearchResult();

        quickSearchResult.setSearchResults(searchResults);

        assertNotNull(quickSearchResult.getSearchResults());
        assertNull(quickSearchResult.getAggregationMetrics());
    }

    @Test
    public void testAggregationMetricsIndependence() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertNotNull(quickSearchResult.getAggregationMetrics());
        assertNull(quickSearchResult.getSearchResults());
    }

    @Test
    public void testParameterizedConstructorAssignment() {
        AtlasSearchResult searchResults1 = new AtlasSearchResult();
        AtlasSearchResult searchResults2 = new AtlasSearchResult();
        Map<String, List<AtlasAggregationEntry>> metrics1 = new HashMap<>();

        AtlasQuickSearchResult result = new AtlasQuickSearchResult(searchResults1, metrics1);

        // Verify correct references
        assertSame(result.getSearchResults(), searchResults1);
        assertSame(result.getAggregationMetrics(), metrics1);

        // Verify they're not pointing to different objects
        assertNotNull(result.getSearchResults());
        assertNotNull(result.getAggregationMetrics());

        if (searchResults1 != searchResults2) {
            assertEquals(result.getSearchResults() == searchResults2, false);
        }
    }

    @Test
    public void testMutableAggregationMetrics() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();
        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        // Modify the map after setting
        List<AtlasAggregationEntry> entries = new ArrayList<>();
        AtlasAggregationEntry entry = new AtlasAggregationEntry();
        entries.add(entry);

        aggregationMetrics.put("test", entries);

        assertEquals(quickSearchResult.getAggregationMetrics().size(), 1);
        assertNotNull(quickSearchResult.getAggregationMetrics().get("test"));
        assertEquals(quickSearchResult.getAggregationMetrics().get("test").size(), 1);
    }

    @Test
    public void testHashCodeConsistency() {
        int hashCode1 = quickSearchResult.hashCode();
        int hashCode2 = quickSearchResult.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(quickSearchResult.equals(quickSearchResult));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(quickSearchResult.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(quickSearchResult.equals("not a quick search result"));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        AtlasQuickSearchResult result1 = new AtlasQuickSearchResult();
        AtlasQuickSearchResult result2 = new AtlasQuickSearchResult();

        result1.setSearchResults(new AtlasSearchResult());
        result2.setSearchResults(new AtlasSearchResult());

        assertFalse(result1.equals(result2));
    }

    @Test
    public void testToString() {
        AtlasSearchResult searchResults = new AtlasSearchResult();
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        quickSearchResult.setSearchResults(searchResults);
        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        String result = quickSearchResult.toString();

        assertNotNull(result);
    }

    @Test
    public void testComplexAggregationMetricsScenario() {
        Map<String, List<AtlasAggregationEntry>> aggregationMetrics = new HashMap<>();

        List<AtlasAggregationEntry> entityTypes = new ArrayList<>();
        entityTypes.add(new AtlasAggregationEntry("Table", 100L));
        entityTypes.add(new AtlasAggregationEntry("Database", 10L));

        List<AtlasAggregationEntry> classifications = new ArrayList<>();
        classifications.add(new AtlasAggregationEntry("PII", 50L));
        classifications.add(new AtlasAggregationEntry("Confidential", 25L));

        aggregationMetrics.put("entityType", entityTypes);
        aggregationMetrics.put("classification", classifications);

        quickSearchResult.setAggregationMetrics(aggregationMetrics);

        assertEquals(quickSearchResult.getAggregationMetrics().size(), 2);
        assertEquals(quickSearchResult.getAggregationMetrics().get("entityType").size(), 2);
        assertEquals(quickSearchResult.getAggregationMetrics().get("classification").size(), 2);

        AtlasAggregationEntry tableEntry = quickSearchResult.getAggregationMetrics().get("entityType").get(0);
        assertEquals(tableEntry.getName(), "Table");
        assertEquals(tableEntry.getCount(), 100L);
    }
}
