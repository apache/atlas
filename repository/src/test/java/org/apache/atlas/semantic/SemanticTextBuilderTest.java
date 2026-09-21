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
import java.util.List;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TERM_ASSIGNMENT_LABEL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
    }

    @AfterMethod
    public void tearDown() {
        graphHelper.close();
        graphUtils.close();
    }

    @Test
    public void buildTextIncludesTypeNameKnownInternalFieldsAndUserAttributes() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        Set<String> keys   = new LinkedHashSet<>(Arrays.asList(
                CLASSIFICATION_TEXT_KEY,
                LABELS_PROPERTY_KEY,
                CUSTOM_ATTRIBUTES_PROPERTY_KEY,
                "make",
                "model",
                "__guid"));

        when(vertex.getPropertyKeys()).thenReturn((Collection) keys);
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn("demo_car");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, CLASSIFICATION_TEXT_KEY, String.class))
                .thenReturn("PII");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, LABELS_PROPERTY_KEY, String.class))
                .thenReturn("fleet");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, CUSTOM_ATTRIBUTES_PROPERTY_KEY, String.class))
                .thenReturn("region=eu");
        when(vertex.getProperty("make", String.class)).thenReturn("Toyota");
        when(vertex.getProperty("model", String.class)).thenReturn("Camry");
        graphHelper.when(() -> GraphHelper.getAllClassificationEdges(vertex)).thenReturn(Collections.emptyList());

        String text = builder.buildText(vertex);

        assertTrue(text.contains("demo_car"));
        assertTrue(text.contains("PII"));
        assertTrue(text.contains("fleet"));
        assertTrue(text.contains("region=eu"));
        assertTrue(text.contains("Toyota"));
        assertTrue(text.contains("Camry"));
    }

    @Test
    public void buildTextSkipsInternalUnderscorePropertiesExceptHandledOnes() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getPropertyKeys()).thenReturn((Collection) Set.of("__state", "color"));
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(vertex)).thenReturn("demo_car");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(vertex), any(), eq(String.class)))
                .thenReturn(null);
        when(vertex.getProperty("__state", String.class)).thenReturn("ACTIVE");
        when(vertex.getProperty("color", String.class)).thenReturn("red");
        graphHelper.when(() -> GraphHelper.getAllClassificationEdges(vertex)).thenReturn(Collections.emptyList());

        String text = builder.buildText(vertex);

        assertEquals(text, "demo_car red");
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
        when(query.edges()).thenReturn(Collections.singletonList(edge));
        when(edge.getOutVertex()).thenReturn(termVertex);
        AtlasVertexQuery synonymQuery = mock(AtlasVertexQuery.class);
        AtlasVertexQuery seeAlsoQuery = mock(AtlasVertexQuery.class);
        when(termVertex.query()).thenReturn(query);
        when(query.direction(AtlasEdgeDirection.BOTH)).thenReturn(query);
        when(query.label("r:AtlasGlossarySynonym")).thenReturn(synonymQuery);
        when(query.label("r:AtlasGlossaryRelatedTerm")).thenReturn(seeAlsoQuery);
        when(synonymQuery.edges()).thenReturn(Collections.emptyList());
        when(seeAlsoQuery.edges()).thenReturn(Collections.emptyList());

        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(entityVertex)).thenReturn("demo_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(entityVertex), any(), eq(String.class)))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(termVertex, "AtlasGlossaryTerm.name", String.class))
                .thenReturn("Customer Data");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(termVertex, "AtlasGlossaryTerm.abbreviation", String.class))
                .thenReturn("CD");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(termVertex, "AtlasGlossaryTerm.description", String.class))
                .thenReturn(null);
        graphHelper.when(() -> GraphHelper.getAllClassificationEdges(entityVertex)).thenReturn(Collections.emptyList());

        String text = builder.buildText(entityVertex);

        assertTrue(text.contains("demo_table"));
        assertTrue(text.contains("Customer Data"));
        assertTrue(text.contains("CD"));
    }

    @Test
    public void buildTextIncludesSynonymAndSeeAlsoTermNames() {
        AtlasVertex entityVertex   = mock(AtlasVertex.class);
        AtlasVertex assignedTerm   = mock(AtlasVertex.class);
        AtlasVertex synonymTerm    = mock(AtlasVertex.class);
        AtlasVertex seeAlsoTerm    = mock(AtlasVertex.class);
        AtlasEdge   assignmentEdge = mock(AtlasEdge.class);
        AtlasEdge   synonymEdge    = mock(AtlasEdge.class);
        AtlasEdge   seeAlsoEdge    = mock(AtlasEdge.class);
        AtlasVertexQuery entityQuery  = mock(AtlasVertexQuery.class);
        AtlasVertexQuery termQuery    = mock(AtlasVertexQuery.class);
        AtlasVertexQuery synonymQuery = mock(AtlasVertexQuery.class);
        AtlasVertexQuery seeAlsoQuery = mock(AtlasVertexQuery.class);

        when(entityVertex.getPropertyKeys()).thenReturn(Collections.emptyList());
        when(entityVertex.query()).thenReturn(entityQuery);
        when(entityQuery.direction(AtlasEdgeDirection.IN)).thenReturn(entityQuery);
        when(entityQuery.label(TERM_ASSIGNMENT_LABEL)).thenReturn(entityQuery);
        when(entityQuery.edges()).thenReturn(Collections.singletonList(assignmentEdge));
        when(assignmentEdge.getOutVertex()).thenReturn(assignedTerm);

        when(assignedTerm.query()).thenReturn(termQuery);
        when(termQuery.direction(AtlasEdgeDirection.BOTH)).thenReturn(termQuery);
        when(termQuery.label("r:AtlasGlossarySynonym")).thenReturn(synonymQuery);
        when(termQuery.label("r:AtlasGlossaryRelatedTerm")).thenReturn(seeAlsoQuery);
        when(synonymQuery.edges()).thenReturn(Collections.singletonList(synonymEdge));
        when(seeAlsoQuery.edges()).thenReturn(Collections.singletonList(seeAlsoEdge));
        when(synonymEdge.getOutVertex()).thenReturn(assignedTerm);
        when(synonymEdge.getInVertex()).thenReturn(synonymTerm);
        when(seeAlsoEdge.getOutVertex()).thenReturn(assignedTerm);
        when(seeAlsoEdge.getInVertex()).thenReturn(seeAlsoTerm);

        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(entityVertex)).thenReturn("demo_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(entityVertex), any(), eq(String.class)))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(assignedTerm, "AtlasGlossaryTerm.name", String.class))
                .thenReturn("Customer Lifetime Value");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(assignedTerm, "AtlasGlossaryTerm.abbreviation", String.class))
                .thenReturn("CLV");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(assignedTerm, "AtlasGlossaryTerm.description", String.class))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(synonymTerm, "AtlasGlossaryTerm.name", String.class))
                .thenReturn("LTV");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(synonymTerm, "AtlasGlossaryTerm.abbreviation", String.class))
                .thenReturn(null);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(seeAlsoTerm, "AtlasGlossaryTerm.name", String.class))
                .thenReturn("Attrition");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(seeAlsoTerm, "AtlasGlossaryTerm.abbreviation", String.class))
                .thenReturn(null);
        graphHelper.when(() -> GraphHelper.getAllClassificationEdges(entityVertex)).thenReturn(Collections.emptyList());

        String text = builder.buildText(entityVertex);

        assertTrue(text.contains("Customer Lifetime Value"));
        assertTrue(text.contains("CLV"));
        assertTrue(text.contains("LTV"));
        assertTrue(text.contains("Attrition"));
    }

    @Test
    public void buildTextIncludesClassificationVertexAttributes() {
        AtlasVertex entityVertex          = mock(AtlasVertex.class);
        AtlasVertex classificationVertex    = mock(AtlasVertex.class);
        AtlasEdge   classificationEdge    = mock(AtlasEdge.class);

        when(entityVertex.getPropertyKeys()).thenReturn(Collections.emptyList());
        when(classificationVertex.getPropertyKeys()).thenReturn((Collection) Set.of("Certified.level"));
        when(classificationEdge.getInVertex()).thenReturn(classificationVertex);
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(entityVertex)).thenReturn("demo_table");
        graphUtils.when(() -> AtlasGraphUtilsV2.getTypeName(classificationVertex)).thenReturn("Certified");
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(eq(entityVertex), any(), eq(String.class)))
                .thenReturn(null);
        when(classificationVertex.getProperty("Certified.level", String.class)).thenReturn("gold");
        graphHelper.when(() -> GraphHelper.getAllClassificationEdges(entityVertex))
                .thenReturn(List.of(classificationEdge));

        String text = builder.buildText(entityVertex);

        assertTrue(text.contains("Certified"));
        assertTrue(text.contains("gold"));
    }
}
