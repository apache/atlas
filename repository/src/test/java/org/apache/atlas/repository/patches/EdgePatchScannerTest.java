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
package org.apache.atlas.repository.patches;

import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class EdgePatchScannerTest {
    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasGraphQuery graphQuery;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private WorkItemManager workItemManager;

    @Mock
    private AtlasRelationshipType relationshipType;

    @Mock
    private AtlasEdge edge1;

    @Mock
    private AtlasEdge edge2;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSubmitRelationshipEdgesWithEntityTypePaginated() {
        when(typeRegistry.getAllRelationshipTypes()).thenReturn(Collections.singletonList(relationshipType));
        when(relationshipType.getTypeName()).thenReturn("hive_table_storage_desc");
        when(graph.query()).thenReturn(graphQuery);
        when(graphQuery.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("hive_table_storage_desc"))).thenReturn(graphQuery);
        when(graphQuery.edges(eq(0), anyInt())).thenReturn(Collections.nCopies(EdgePatchProcessor.BATCH_SIZE, edge1));
        when(graphQuery.edges(eq(EdgePatchProcessor.BATCH_SIZE), anyInt())).thenReturn(Arrays.asList(edge1, edge2));
        when(edge1.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn("hive_table_storage_desc");
        when(edge2.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn("hive_table_storage_desc");
        when(edge1.getId()).thenReturn("e1");
        when(edge2.getId()).thenReturn("e2");

        int count = EdgePatchScanner.submitRelationshipEdgesWithEntityType(graph, typeRegistry, workItemManager);

        assertEquals(count, EdgePatchProcessor.BATCH_SIZE + 2);
        verify(graphQuery, times(1)).edges(0, EdgePatchProcessor.BATCH_SIZE);
        verify(graphQuery, times(1)).edges(EdgePatchProcessor.BATCH_SIZE, EdgePatchProcessor.BATCH_SIZE);
    }
}
