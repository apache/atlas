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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AtlasGraphUtilsV2.findByGuids() batch lookup method.
 */
class AtlasGraphUtilsV2BatchLookupTest {

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test configuration", e);
        }
    }

    @Mock
    private AtlasGraph mockGraph;

    @Mock
    private AtlasGraphQuery mockQuery;

    @Mock
    private AtlasVertex mockVertex1;

    @Mock
    private AtlasVertex mockVertex2;

    @Mock
    private AtlasVertex mockVertex3;

    private MockedStatic<GraphTransactionInterceptor> mockedInterceptor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get();

        // Mock the static cache methods to return null (cache miss)
        mockedInterceptor = mockStatic(GraphTransactionInterceptor.class);
        mockedInterceptor.when(() -> GraphTransactionInterceptor.getVertexFromCache(anyString()))
                .thenReturn(null);
        mockedInterceptor.when(() -> GraphTransactionInterceptor.addToVertexCache(anyString(), any(AtlasVertex.class)))
                .then(invocation -> null);
    }

    @AfterEach
    void tearDown() {
        if (mockedInterceptor != null) {
            mockedInterceptor.close();
        }
    }

    @Test
    void testFindByGuids_emptyInput() {
        // Test with null
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, null);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Test with empty collection
        result = AtlasGraphUtilsV2.findByGuids(mockGraph, Collections.emptyList());
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Test with collection of nulls
        result = AtlasGraphUtilsV2.findByGuids(mockGraph, Arrays.asList(null, null));
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Verify no graph query was made
        verifyNoInteractions(mockGraph);
    }

    @Test
    void testFindByGuids_deduplicatesInput() {
        // Setup
        String guid1 = "guid-1";
        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenReturn(Collections.singletonList(mockVertex1));
        when(mockVertex1.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid1);

        // Execute with duplicates
        List<String> guidsWithDuplicates = Arrays.asList(guid1, guid1, guid1);
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, guidsWithDuplicates);

        // Verify
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(mockVertex1, result.get(guid1));

        // Verify the query was called with deduplicated set (only one guid)
        verify(mockQuery).in(eq(Constants.GUID_PROPERTY_KEY), argThat(collection ->
            collection instanceof Collection && ((Collection<?>) collection).size() == 1));
    }

    @Test
    void testFindByGuids_missingGuids() {
        // Setup - query returns empty
        String guid1 = "guid-1";
        String guid2 = "guid-2";
        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenReturn(Collections.emptyList());

        // Execute
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, Arrays.asList(guid1, guid2));

        // Verify - missing guids should be absent from result map
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertNull(result.get(guid1));
        assertNull(result.get(guid2));
    }

    @Test
    void testFindByGuids_mixedExistingAndMissing() {
        // Setup - only guid1 and guid3 exist
        String guid1 = "guid-1";
        String guid2 = "guid-missing";
        String guid3 = "guid-3";

        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenReturn(Arrays.asList(mockVertex1, mockVertex3));
        when(mockVertex1.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid1);
        when(mockVertex3.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid3);

        // Execute
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, Arrays.asList(guid1, guid2, guid3));

        // Verify
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(mockVertex1, result.get(guid1));
        assertNull(result.get(guid2)); // Missing guid should be absent
        assertEquals(mockVertex3, result.get(guid3));
    }

    @Test
    void testFindByGuids_allExisting() {
        // Setup
        String guid1 = "guid-1";
        String guid2 = "guid-2";
        String guid3 = "guid-3";

        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenReturn(Arrays.asList(mockVertex1, mockVertex2, mockVertex3));
        when(mockVertex1.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid1);
        when(mockVertex2.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid2);
        when(mockVertex3.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid3);

        // Execute
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, Arrays.asList(guid1, guid2, guid3));

        // Verify
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals(mockVertex1, result.get(guid1));
        assertEquals(mockVertex2, result.get(guid2));
        assertEquals(mockVertex3, result.get(guid3));
    }

    @Test
    void testFindByGuids_queryException() {
        // Setup - query throws exception
        String guid1 = "guid-1";
        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenThrow(new RuntimeException("Graph query failed"));

        // Execute - should throw exception for caller to handle fallback
        assertThrows(RuntimeException.class, () ->
            AtlasGraphUtilsV2.findByGuids(mockGraph, Collections.singletonList(guid1)));
    }

    @Test
    void testFindByGuids_handlesNullVertex() {
        // Setup - query returns list with null vertex
        String guid1 = "guid-1";
        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenReturn(Arrays.asList(null, mockVertex1));
        when(mockVertex1.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(guid1);

        // Execute - should skip null vertices
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, Arrays.asList(guid1, "guid-2"));

        // Verify
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(mockVertex1, result.get(guid1));
    }

    @Test
    void testFindByGuids_handlesVertexWithNullGuid() {
        // Setup - vertex returns null guid
        when(mockGraph.query()).thenReturn(mockQuery);
        when(mockQuery.in(eq(Constants.GUID_PROPERTY_KEY), any(Collection.class))).thenReturn(mockQuery);
        when(mockQuery.vertices()).thenReturn(Collections.singletonList(mockVertex1));
        when(mockVertex1.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn(null);

        // Execute - should skip vertex with null guid
        Map<String, AtlasVertex> result = AtlasGraphUtilsV2.findByGuids(mockGraph, Collections.singletonList("guid-1"));

        // Verify
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
