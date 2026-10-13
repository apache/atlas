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
package org.apache.atlas.semantic.indexer;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SemanticNotificationGuidExpanderTest {
    private MockedStatic<AtlasGraphUtilsV2> graphUtils;
    private MockedStatic<GraphHelper>       graphHelper;

    @BeforeMethod
    public void setUp() {
        graphUtils  = mockStatic(AtlasGraphUtilsV2.class);
        graphHelper = mockStatic(GraphHelper.class);

        graphHelper.when(() -> GraphHelper.getActiveTermAssignmentEdges(any(), any(), anyInt())).thenCallRealMethod();
    }

    @AfterMethod
    public void tearDown() {
        graphHelper.close();
        graphUtils.close();
    }

    @Test
    public void expandForIndexingMapsGlossaryTermGuidToAssignedEntities() {
        AtlasVertex      termVertex   = mock(AtlasVertex.class);
        AtlasVertex      entityVertex = mock(AtlasVertex.class);
        AtlasEdge        edge         = mock(AtlasEdge.class);
        AtlasVertexQuery query        = mock(AtlasVertexQuery.class);

        graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid("term-guid")).thenReturn(termVertex);
        when(termVertex.query()).thenReturn(query);
        when(query.direction(AtlasEdgeDirection.OUT)).thenReturn(query);
        when(query.label(TERM_ASSIGNMENT_LABEL)).thenReturn(query);
        when(query.has(STATE_PROPERTY_KEY, "ACTIVE")).thenReturn(query);
        when(query.edges(anyInt())).thenReturn(Collections.singletonList(edge));
        when(edge.getInVertex()).thenReturn(entityVertex);
        graphHelper.when(() -> GraphHelper.getGuid(entityVertex)).thenReturn("entity-guid");

        Set<String> expanded = SemanticNotificationGuidExpander.expandForIndexing(Map.of("term-guid", "AtlasGlossaryTerm"));

        assertEquals(expanded, Set.of("entity-guid"));
    }

    @Test
    public void expandForIndexingCapsGlossaryTermFanOut() throws Exception {
        AtlasVertex      termVertex = mock(AtlasVertex.class);
        AtlasVertexQuery query      = mock(AtlasVertexQuery.class);
        List<AtlasEdge>  edges      = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            AtlasEdge   edge         = mock(AtlasEdge.class);
            AtlasVertex entityVertex = mock(AtlasVertex.class);
            String      entityGuid   = "entity-" + i;

            when(edge.getInVertex()).thenReturn(entityVertex);
            graphHelper.when(() -> GraphHelper.getGuid(entityVertex)).thenReturn(entityGuid);
            edges.add(edge);
        }

        graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid("term-guid")).thenReturn(termVertex);
        when(termVertex.query()).thenReturn(query);
        when(query.direction(AtlasEdgeDirection.OUT)).thenReturn(query);
        when(query.label(TERM_ASSIGNMENT_LABEL)).thenReturn(query);
        when(query.has(STATE_PROPERTY_KEY, "ACTIVE")).thenReturn(query);
        when(query.edges(3)).thenReturn(edges);

        ApplicationProperties.get().setProperty(AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getPropertyName(), 2);
        try {
            Set<String> expanded = SemanticNotificationGuidExpander.expandForIndexing(Map.of("term-guid", "AtlasGlossaryTerm"));

            assertEquals(expanded, Set.of("entity-0", "entity-1"));
        } finally {
            ApplicationProperties.get().clearProperty(AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getPropertyName());
        }
    }

    @Test
    public void expandForIndexingSkipsGlossaryTermFanOutWhenDisabled() throws Exception {
        ApplicationProperties.get().setProperty(AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getPropertyName(), 0);
        try {
            assertTrue(SemanticNotificationGuidExpander.expandForIndexing(Map.of("term-guid", "AtlasGlossaryTerm")).isEmpty());
            graphUtils.verify(() -> AtlasGraphUtilsV2.findByGuid(any()), never());
        } finally {
            ApplicationProperties.get().clearProperty(AtlasConfiguration.SEMANTIC_INDEXER_MAX_TERM_ENTITIES.getPropertyName());
        }
    }

    @Test
    public void expandForIndexingPassesOtherGuidsOnWithoutReadingTheGraph() {
        Map<String, String> guidTypes = new LinkedHashMap<>();
        guidTypes.put("table-guid", "hive_table");
        guidTypes.put("glossary-guid", "AtlasGlossary");
        guidTypes.put("untyped-guid", null);

        Set<String> expanded = SemanticNotificationGuidExpander.expandForIndexing(guidTypes);

        assertEquals(expanded, Set.of("table-guid", "glossary-guid", "untyped-guid"));
        graphUtils.verify(() -> AtlasGraphUtilsV2.findByGuid(any()), never());
    }
}
