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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.semantic.SemanticEntityEmbedder.IndexStats;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SemanticEntityEmbedderTest {
    private MockedStatic<AtlasGraphUtilsV2> graphUtils;
    private SemanticTextBuilder             textBuilder;
    private SemanticVectorStore             semanticStore;
    private SemanticEntityEmbedder          embedder;

    @BeforeMethod
    public void setUp() {
        graphUtils    = mockStatic(AtlasGraphUtilsV2.class);
        textBuilder   = mock(SemanticTextBuilder.class);
        semanticStore = mock(SemanticVectorStore.class);
        embedder      = new SemanticEntityEmbedder(textBuilder, semanticStore);
    }

    @AfterMethod
    public void tearDown() {
        graphUtils.close();
    }

    @Test
    public void isEmbeddableEntityTypeRejectsGlossaryAndInternalTypes() {
        assertFalse(SemanticEntityEmbedder.isEmbeddableEntityType("AtlasGlossaryTerm"));
        assertFalse(SemanticEntityEmbedder.isEmbeddableEntityType("AtlasGlossary"));
        assertFalse(SemanticEntityEmbedder.isEmbeddableEntityType("__AtlasAuditEntry"));
        assertFalse(SemanticEntityEmbedder.isEmbeddableEntityType(""));
        assertTrue(SemanticEntityEmbedder.isEmbeddableEntityType("DataSet"));
    }

    @Test
    public void embedStoresTheTextOfActiveEmbeddableEntities() throws Exception {
        entity("g1", "DataSet", AtlasEntity.Status.ACTIVE, "orders");

        IndexStats stats = embedder.embed(Collections.singletonList("g1"));

        assertStats(stats, 1, 0, 0);
        verify(semanticStore).updateEmbedding("g1", "orders");
    }

    @Test
    public void embedSkipsMissingInactiveNonEmbeddableAndTextlessEntities() throws Exception {
        entity("deleted", "DataSet", AtlasEntity.Status.DELETED, "orders");
        entity("term", "AtlasGlossaryTerm", AtlasEntity.Status.ACTIVE, "customer");
        entity("internal", "__AtlasAuditEntry", AtlasEntity.Status.ACTIVE, "audit");
        entity("textless", "DataSet", AtlasEntity.Status.ACTIVE, " ");

        IndexStats stats = embedder.embed(Arrays.asList("missing", "deleted", "term", "internal", "textless", ""));

        assertStats(stats, 0, 6, 0);
        verify(semanticStore, never()).updateEmbedding(anyString(), anyString());
    }

    @Test
    public void embedCountsStoreAndGraphErrorsAsFailures() throws Exception {
        entity("store-error", "DataSet", AtlasEntity.Status.ACTIVE, "orders");
        entity("ok", "DataSet", AtlasEntity.Status.ACTIVE, "customers");
        doThrow(new SemanticSearchException("HTTP request failed", true)).when(semanticStore).updateEmbedding("store-error", "orders");
        graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid("graph-error")).thenThrow(new IllegalStateException("storage down"));

        IndexStats stats = embedder.embed(Arrays.asList("store-error", "graph-error", "ok"));

        assertStats(stats, 1, 0, 2);
        assertTrue(stats.hasFailures());
    }

    private void entity(String guid, String typeName, AtlasEntity.Status state, String text) {
        AtlasVertex vertex = mock(AtlasVertex.class);
        graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(guid)).thenReturn(vertex);
        graphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(vertex, TYPE_NAME_PROPERTY_KEY, String.class)).thenReturn(typeName);
        graphUtils.when(() -> AtlasGraphUtilsV2.getState(vertex)).thenReturn(state);
        when(textBuilder.buildText(vertex)).thenReturn(text);
    }

    private static void assertStats(IndexStats stats, int indexed, int skipped, int failed) {
        assertEquals(stats.getIndexed(), indexed, "indexed");
        assertEquals(stats.getSkipped(), skipped, "skipped");
        assertEquals(stats.getFailed(), failed, "failed");
    }
}
