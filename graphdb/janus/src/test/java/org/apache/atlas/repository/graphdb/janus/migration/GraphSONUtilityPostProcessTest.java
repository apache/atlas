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

package org.apache.atlas.repository.graphdb.janus.migration;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

public class GraphSONUtilityPostProcessTest extends BaseUtils {
    final String HIVE_COLUMNS_PROPERTY = "hive_table.columns";
    final String edgeId1 = "816u-35tc-ao0l-47so";
    final String edgeId2 = "82rq-35tc-ao0l-2glc";

    final String edgeId1x = "816u-35tc-ao0l-xxxx";
    final String edgeId2x = "82rq-35tc-ao0l-xxxx";

    private TinkerGraph tg;
    private MappedElementCache cache = new MappedElementCache();
    private Vertex tableV;

    @Test
    public void noRefNoUpdate() throws IOException {
        tg = TinkerGraph.open();
        graphSONUtility = new GraphSONUtility(emptyRelationshipCache);

        addEdge(tg, cache);

        tableV = fetchTableVertex(tg);
        assertNotNull(tableV);

        assertListProperty(HIVE_COLUMNS_PROPERTY, edgeId1, edgeId2, tableV);

        graphSONUtility.replaceReferencedEdgeIdForList(tg, cache, tableV, HIVE_COLUMNS_PROPERTY);
        assertListProperty(HIVE_COLUMNS_PROPERTY, edgeId1, edgeId2, tableV);
    }

    @Test(dependsOnMethods = "noRefNoUpdate")
    public void refFoundVertexUpdated() throws IOException {

        cache.lruEdgeCache.put(edgeId1, edgeId1x);
        cache.lruEdgeCache.put(edgeId2, edgeId2x);

        graphSONUtility.replaceReferencedEdgeIdForList(tg, cache, tableV, HIVE_COLUMNS_PROPERTY);
        assertListProperty(HIVE_COLUMNS_PROPERTY, edgeId1x, edgeId2x, tableV);
    }

    @Test(dependsOnMethods = "refFoundVertexUpdated")
    public void updateUsingPostProcessConsumer() throws IOException {
        MappedElementCache cache = new MappedElementCache();
        BlockingQueue<Object> bc = new BlockingArrayQueue<>();
        PostProcessManager.Consumer consumer = new PostProcessManager.Consumer(bc, tg, graphSONUtility,
                new String[] {HIVE_COLUMNS_PROPERTY}, cache, 5);

        cache.lruEdgeCache.put(edgeId1x, edgeId1);
        cache.lruEdgeCache.put(edgeId2x, edgeId2);
        consumer.processItem(tableV.id());

        assertListProperty(HIVE_COLUMNS_PROPERTY, edgeId1, edgeId2, tableV);
    }

    private void assertListProperty(String HIVE_COLUMNS_PROPERTY, String edgeId1, String edgeId2, Vertex tableV) {
        assertTrue(tableV.property(HIVE_COLUMNS_PROPERTY).isPresent());
        List list = (List) tableV.property(HIVE_COLUMNS_PROPERTY).value();

        assertEquals(list.size(), 2);
        assertEquals(list.get(0), edgeId1);
        assertEquals(list.get(1), edgeId2);
    }
}
