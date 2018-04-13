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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.repository.Constants.EDGE_ID_IN_IMPORT_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_ID_IN_IMPORT_KEY;
import static org.testng.Assert.*;

public class GraphSONUtilityTest extends BaseUtils {

    @Test(dataProvider = "col1")
    public void idFetch(JsonNode node) {
        Object o = GraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokensTP2._ID));

        assertNotNull(o);
        assertEquals((int) o, 98336);
    }

    @Test(dataProvider = "col1")
    public void verifyReadProperties(JsonNode node) {
        Map<String, Object> props = GraphSONUtility.readProperties(node);

        assertEquals(props.get("__superTypeNames").getClass(), ArrayList.class);
        assertEquals(props.get("Asset.name").getClass(), String.class);
        assertEquals(props.get("hive_column.position").getClass(), Integer.class);
        assertEquals(props.get("__timestamp").getClass(), Long.class);

        assertNotNull(props);
    }

    @Test(dataProvider = "col1")
    public void dataNodeReadAndVertexAddedToGraph(JsonNode entityNode) throws IOException {
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(emptyRelationshipCache);
        Map<String, Object> map = gu.vertexFromJson(tg, entityNode);

        assertNull(map);
        assertEquals((long) tg.traversal().V().count().next(), 1L);

        Vertex v = tg.vertices().next();
        assertTrue(v.property(VERTEX_ID_IN_IMPORT_KEY).isPresent());
    }

    @Test(dataProvider = "dbType")
    public void typeNodeReadAndVertexNotAddedToGraph(JsonNode entityNode) throws IOException {
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(emptyRelationshipCache);
        gu.vertexFromJson(tg, entityNode);

        Assert.assertEquals((long) tg.traversal().V().count().next(), 0L);
    }

    @Test
    public void edgeReadAndAddedToGraph() throws IOException {
        TinkerGraph tg = TinkerGraph.open();
        GraphSONUtility gu = new GraphSONUtility(emptyRelationshipCache);
        Map<String, Object> m = null;

        m = gu.vertexFromJson(tg, (JsonNode) (getDBV(null)[0][0]));
        assertNull(m);

        m = gu.vertexFromJson(tg, (JsonNode) (getTableV(null))[0][0]);
        assertNull(m);

        m = gu.edgeFromJson(tg, new MappedElementCache(), (JsonNode) getEdge(null)[0][0]);
        assertNull(m);

        Assert.assertEquals((long) tg.traversal().V().count().next(), 2L);
        Assert.assertEquals((long) tg.traversal().E().count().next(), 1L);

        Edge e = tg.edges().next();
        assertTrue(e.property(EDGE_ID_IN_IMPORT_KEY).isPresent());
    }
}
