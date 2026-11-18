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
package org.apache.atlas.tools;

import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.semantic.SemanticEntityEmbedder;
import org.apache.atlas.semantic.SemanticEntityEmbedder.IndexStats;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class SemanticRepairTest {
    @Test
    public void dryRunCountsCandidatesWithoutEmbedding() {
        SemanticEntityEmbedder embedder = mock(SemanticEntityEmbedder.class);
        AtlasGraph             graph    = mock(AtlasGraph.class);
        SemanticRepair         repair   = new SemanticRepair(embedder, graph, true);

        repair.repairPage(Arrays.asList("g1", "g2"));
        repair.repairPage(Collections.singletonList("g3"));

        assertEquals(repair.matched, 3);
        assertEquals(repair.stats.getIndexed(), 0);
        verifyNoInteractions(embedder, graph);
    }

    @Test
    public void repairPageAddsUpStatsAndRollsBackEachPage() {
        SemanticEntityEmbedder embedder = mock(SemanticEntityEmbedder.class);
        AtlasGraph             graph    = mock(AtlasGraph.class);
        SemanticRepair         repair   = new SemanticRepair(embedder, graph, false);
        when(embedder.embed(any())).thenReturn(new IndexStats(1, 1, 0), new IndexStats(0, 0, 1));

        repair.repairPage(Arrays.asList("g1", "g2"));
        repair.repairPage(Collections.singletonList("g3"));

        assertEquals(repair.matched, 3);
        assertEquals(repair.stats.getIndexed(), 1);
        assertEquals(repair.stats.getSkipped(), 1);
        assertEquals(repair.stats.getFailed(), 1);
        verify(graph, times(2)).rollback();
    }

    @Test
    public void repairPageRollsBackWhenEmbeddingThrows() {
        SemanticEntityEmbedder embedder = mock(SemanticEntityEmbedder.class);
        AtlasGraph             graph    = mock(AtlasGraph.class);
        SemanticRepair         repair   = new SemanticRepair(embedder, graph, false);
        when(embedder.embed(any())).thenThrow(new IllegalStateException("graph closed"));

        expectThrows(IllegalStateException.class, () -> repair.repairPage(Collections.singletonList("g1")));

        verify(graph).rollback();
    }
}
