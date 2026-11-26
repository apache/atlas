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
package org.apache.atlas.repository.patches;

import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class EdgePatchScannerTest {
    @Test
    public void testQueryUsesEdgeIndexFields() {
        String prefix = AtlasGraphUtilsV2.getIndexSearchPrefix();

        assertEquals(EdgePatchScanner.EDGES_WITHOUT_RELATIONSHIP_TYPE_QUERY, prefix + "\"_r__guid\":* AND -" + prefix + "\"_r__typeName\":*");
    }

    @Test
    public void testAllEdgesSubmittedOnceWhenUpdatesSucceed() {
        FakeEdgeIndex index = new FakeEdgeIndex(2500, Collections.emptySet());

        long unpatched = EdgePatchScanner.submitEdgesWithoutRelationshipType(index.graph, index.processor, index.manager, 300, 1000);

        assertEquals(unpatched, 0);
        assertEquals(index.produced.size(), 2500);
        assertEquals(new HashSet<>(index.produced).size(), 2500);
        assertTrue(index.remaining.isEmpty());
        verify(index.graph, never()).getEdges();
        verify(index.graph, never()).query();
    }

    @Test
    public void testFailedEdgesAreSkippedAndReported() {
        Set<String>   failing = new HashSet<>(Arrays.asList("e00005", "e01200"));
        FakeEdgeIndex index   = new FakeEdgeIndex(2000, failing);

        long unpatched = EdgePatchScanner.submitEdgesWithoutRelationshipType(index.graph, index.processor, index.manager, 300, 700);

        assertEquals(unpatched, 2);
        assertEquals(index.remaining, Arrays.asList("e00005", "e01200"));

        Map<String, Integer> submitCounts = index.submitCounts();

        assertEquals(submitCounts.size(), 2000);

        for (Map.Entry<String, Integer> entry : submitCounts.entrySet()) {
            // failing edges are retried once in the final pass; others are submitted exactly once
            assertEquals((int) entry.getValue(), failing.contains(entry.getKey()) ? 2 : 1, entry.getKey());
        }
    }

    @Test
    public void testNoEdges() {
        FakeEdgeIndex index = new FakeEdgeIndex(0, Collections.emptySet());

        long unpatched = EdgePatchScanner.submitEdgesWithoutRelationshipType(index.graph, index.processor, index.manager, 300, 1000);

        assertEquals(unpatched, 0);
        assertTrue(index.produced.isEmpty());
        verify(index.manager, never()).checkProduce(any());
    }

    @Test
    public void testFailsWhenUpdatesNeverBecomeVisible() {
        FakeEdgeIndex index = new FakeEdgeIndex(10, Collections.emptySet());

        index.applyUpdates = false;

        expectThrows(IllegalStateException.class, () -> EdgePatchScanner.submitEdgesWithoutRelationshipType(index.graph, index.processor, index.manager, 300, 1000, 1));
    }

    /**
     * Simulates the edge index: the result holds edges not yet patched, in index order. Draining the manager
     * "processes" submitted edges: successful ones leave the result, failing ones stay in place.
     */
    private static class FakeEdgeIndex {
        final AtlasGraph         graph     = mock(AtlasGraph.class);
        final EdgePatchProcessor processor = mock(EdgePatchProcessor.class);
        final WorkItemManager    manager   = mock(WorkItemManager.class);
        final List<String>       remaining = new ArrayList<>();
        final List<String>       produced  = new ArrayList<>();
        final List<String>       pending   = new ArrayList<>();
        final AtomicLong         updated   = new AtomicLong();
        final AtomicLong         failed    = new AtomicLong();
        boolean                  applyUpdates = true;

        FakeEdgeIndex(int edgeCount, Set<String> failing) {
            for (int i = 0; i < edgeCount; i++) {
                remaining.add(String.format("e%05d", i));
            }

            when(processor.getUpdatedCount()).thenAnswer(inv -> updated.get());
            when(processor.getFailedCount()).thenAnswer(inv -> failed.get());

            when(graph.indexQuery(eq(Constants.EDGE_INDEX), anyString())).thenAnswer(inv -> {
                AtlasIndexQuery query = mock(AtlasIndexQuery.class);

                when(query.edges(anyInt(), anyInt())).thenAnswer(q -> page(q.getArgument(0), q.getArgument(1)));

                return query;
            });

            doAnswer(inv -> {
                String edgeId = inv.getArgument(0);

                produced.add(edgeId);
                pending.add(edgeId);

                return null;
            }).when(manager).checkProduce(any());

            doAnswer(inv -> {
                for (String edgeId : pending) {
                    if (failing.contains(edgeId)) {
                        failed.incrementAndGet();
                    } else {
                        updated.incrementAndGet();

                        if (applyUpdates) {
                            remaining.remove(edgeId);
                        }
                    }
                }

                pending.clear();

                return null;
            }).when(manager).drain();
        }

        Map<String, Integer> submitCounts() {
            Map<String, Integer> ret = new HashMap<>();

            for (String edgeId : produced) {
                ret.merge(edgeId, 1, Integer::sum);
            }

            return ret;
        }

        private Iterator<AtlasIndexQuery.Result> page(int offset, int limit) {
            List<AtlasIndexQuery.Result> ret = new ArrayList<>();

            for (int i = offset; i < Math.min(remaining.size(), offset + limit); i++) {
                AtlasIndexQuery.Result result = mock(AtlasIndexQuery.Result.class);
                AtlasEdge              edge   = mock(AtlasEdge.class);

                when(edge.getId()).thenReturn(remaining.get(i));
                when(result.getEdge()).thenReturn(edge);

                ret.add(result);
            }

            return ret.iterator();
        }
    }
}
