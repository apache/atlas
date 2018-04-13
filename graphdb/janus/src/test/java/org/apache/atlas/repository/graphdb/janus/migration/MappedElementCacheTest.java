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

package org.apache.atlas.repository.graphdb.janus.migration;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.*;

public class MappedElementCacheTest extends BaseUtils {

    @Test(dataProvider = "col1")
    public void vertexFetch(JsonNode node) {
        MappedElementCache cache = new MappedElementCache();
        TinkerGraph tg = TinkerGraph.open();

        addVertex(tg, node);

        Vertex vx = cache.getMappedVertex(tg, 98336);
        assertNotNull(vx);
        assertEquals(cache.lruVertexCache.size(), 1);
        assertEquals(cache.lruEdgeCache.size(), 0);
    }

    @Test
    public void edgeFetch() throws IOException {
        MappedElementCache cache = new MappedElementCache();
        TinkerGraph tg = TinkerGraph.open();

        addEdge(tg, cache);

        assertEquals(cache.lruVertexCache.size(), 2);
        assertEquals(cache.lruEdgeCache.size(), 0);
    }


    @Test
    public void nonExistentVertexReturnsNull() {
        TinkerGraph tg = TinkerGraph.open();
        MappedElementCache cache = new MappedElementCache();

        assertNull(cache.fetchVertex(tg, 1111));
        assertNull(cache.fetchEdge(tg, "abcd"));
    }

    @DataProvider(name = "col1")
    public Object[][] getCol1(ITestContext context) throws IOException {
        return getJsonNodeFromFile("col-legacy.json");
    }
}
