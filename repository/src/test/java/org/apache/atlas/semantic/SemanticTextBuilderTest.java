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
package org.apache.atlas.semantic;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SemanticTextBuilderTest {
    private MockedStatic<AtlasGraphUtilsV2> graphUtils;
    private MockedStatic<GraphHelper>       graphHelper;
    private SemanticTextBuilder             builder;

    @BeforeMethod
    public void setUp() {
        graphUtils   = mockStatic(AtlasGraphUtilsV2.class);
        graphHelper  = mockStatic(GraphHelper.class);
        builder      = new SemanticTextBuilder();

        graphHelper.when(() -> GraphHelper.getActiveTermAssignmentEdges(any(), any(), anyInt())).thenCallRealMethod();
    }

    @AfterMethod
    public void tearDown() {
        graphHelper.close();
        graphUtils.close();
    }

    @Test
    public void buildTextIncludesTypeNameClassificationsLabelsAndCustomAttributes() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getPropertyKeys()).thenReturn(Collections.emptyList());
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn("demo_car");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, CLASSIFICATION_TEXT_KEY, String.class))
                .thenReturn("PII");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, LABELS_PROPERTY_KEY, String.class))
                .thenReturn("fleet");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, CUSTOM_ATTRIBUTES_PROPERTY_KEY, String.class))
                .thenReturn("region=eu");

        assertEquals(builder.buildText(vertex), "demo_car PII fleet region=eu");
    }

    @Test
    public void buildTextEmbedsOnlyKnownAttributesAndSkipsNonStringValues() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getPropertyKeys()).thenReturn((Collection) new LinkedHashSet<>(Arrays.asList(
                "__state", "Asset.owner", "hive_table.tableType", "hive_table.createTime", "__comment", "hive_table.comment")));
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn("hive_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(vertex), any(), eq(String.class)))
                .thenReturn(null);
        when(vertex.getProperty("Asset.owner", Object.class)).thenReturn("etl_user");
        when(vertex.getProperty("hive_table.tableType", Object.class)).thenReturn("MANAGED_TABLE");
        when(vertex.getProperty("Asset.displayName", Object.class)).thenReturn(42L);
        when(vertex.getProperty("__comment", Object.class)).thenReturn("internal");
        when(vertex.getProperty("hive_table.comment", Object.class)).thenReturn("raw orders");

        assertEquals(builder.buildText(vertex), "hive_table raw orders");
    }

    @Test
    public void buildTextReturnsEmptyForNullVertex() {
        assertEquals(builder.buildText(null), "");
    }

    @Test
    public void buildTextIncludesGlossaryTermDisplayNamesFromAssignmentEdges() {
        AtlasVertex entityVertex = mock(AtlasVertex.class);
        AtlasVertex termVertex   = mock(AtlasVertex.class);
        AtlasEdge   edge         = mock(AtlasEdge.class);
        AtlasVertexQuery query   = mock(AtlasVertexQuery.class);

        when(entityVertex.getPropertyKeys()).thenReturn(Collections.emptyList());
        when(entityVertex.query()).thenReturn(query);
        when(query.direction(AtlasEdgeDirection.IN)).thenReturn(query);
        when(query.label(TERM_ASSIGNMENT_LABEL)).thenReturn(query);
        when(query.has(STATE_PROPERTY_KEY, "ACTIVE")).thenReturn(query);
        when(query.edges(100)).thenReturn(Collections.singletonList(edge));
        when(edge.getOutVertex()).thenReturn(termVertex);

        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(entityVertex)).thenReturn("demo_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(entityVertex), any(), eq(String.class)))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(termVertex, "AtlasGlossaryTerm.name", String.class))
                .thenReturn("Customer Data");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(termVertex, "AtlasGlossaryTerm.abbreviation", String.class))
                .thenReturn("CD");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(termVertex, "AtlasGlossaryTerm.description", String.class))
                .thenReturn(null);

        String text = builder.buildText(entityVertex);

        assertTrue(text.contains("demo_table"));
        assertTrue(text.contains("Customer Data"));
        assertTrue(text.contains("CD"));
    }

    @Test
    public void buildTextDoesNotFollowTermsLinkedToAssignedTerms() {
        AtlasVertex      entityVertex   = mock(AtlasVertex.class);
        AtlasVertex      assignedTerm   = mock(AtlasVertex.class);
        AtlasEdge        assignmentEdge = mock(AtlasEdge.class);
        AtlasVertexQuery entityQuery    = mock(AtlasVertexQuery.class);

        when(entityVertex.getPropertyKeys()).thenReturn(Collections.emptyList());
        when(entityVertex.query()).thenReturn(entityQuery);
        when(entityQuery.direction(AtlasEdgeDirection.IN)).thenReturn(entityQuery);
        when(entityQuery.label(TERM_ASSIGNMENT_LABEL)).thenReturn(entityQuery);
        when(entityQuery.has(STATE_PROPERTY_KEY, "ACTIVE")).thenReturn(entityQuery);
        when(entityQuery.edges(anyInt())).thenReturn(Collections.singletonList(assignmentEdge));
        when(assignmentEdge.getOutVertex()).thenReturn(assignedTerm);

        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(entityVertex)).thenReturn("demo_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(entityVertex), any(), eq(String.class)))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(assignedTerm, "AtlasGlossaryTerm.name", String.class))
                .thenReturn("Customer Lifetime Value");

        String text = builder.buildText(entityVertex);

        assertTrue(text.contains("Customer Lifetime Value"));
        verify(assignedTerm, never()).query();
    }

    @Test
    public void buildTextUsesClassificationTextWithoutReadingClassificationVertices() {
        AtlasVertex entityVertex = mock(AtlasVertex.class);

        when(entityVertex.getPropertyKeys()).thenReturn(Collections.emptyList());
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(entityVertex)).thenReturn("demo_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(entityVertex), any(), eq(String.class)))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(entityVertex, CLASSIFICATION_TEXT_KEY, String.class))
                .thenReturn("Certified gold");

        String text = builder.buildText(entityVertex);

        assertTrue(text.contains("Certified gold"));
        graphHelper.verify(() -> GraphHelper.getAllClassificationEdges(any()), never());
    }

    @Test
    public void buildTextOrdersKnownAttributesAndSkipsUniqueShadowCopies() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getPropertyKeys()).thenReturn((Collection) new LinkedHashSet<>(Arrays.asList(
                "hive_table.comment", "Referenceable.__u_qualifiedName", "Referenceable.qualifiedName",
                "Asset.userDescription", "Asset.description", "Asset.name")));
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn("hive_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(vertex), any(), eq(String.class)))
                .thenReturn(null);
        when(vertex.getProperty("Asset.name", Object.class)).thenReturn("orders");
        when(vertex.getProperty("Asset.description", Object.class)).thenReturn("all orders");
        when(vertex.getProperty("Asset.userDescription", Object.class)).thenReturn("one row per order");
        when(vertex.getProperty("Referenceable.qualifiedName", Object.class)).thenReturn("db.orders@cl1");
        when(vertex.getProperty("Referenceable.__u_qualifiedName", Object.class)).thenReturn("db.orders@cl1");
        when(vertex.getProperty("hive_table.comment", Object.class)).thenReturn("raw");

        assertEquals(builder.buildText(vertex), "hive_table orders all orders one row per order raw db.orders@cl1");
    }

    @Test
    public void buildTextEmbedsAttributesOfTypesThatDoNotExtendAsset() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getPropertyKeys()).thenReturn((Collection) new LinkedHashSet<>(Arrays.asList(
                "my_type.description", "my_type.__u_name", "my_type.name", "my_type.username", "__typeName")));
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn("my_type");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(vertex), any(), eq(String.class)))
                .thenReturn(null);
        when(vertex.getProperty("my_type.name", Object.class)).thenReturn("sensor-7");
        when(vertex.getProperty("my_type.__u_name", Object.class)).thenReturn("sensor-7");
        when(vertex.getProperty("my_type.username", Object.class)).thenReturn("bob");
        when(vertex.getProperty("my_type.description", Object.class)).thenReturn("roof temperature");

        assertEquals(builder.buildText(vertex), "my_type sensor-7 roof temperature");
    }
}
