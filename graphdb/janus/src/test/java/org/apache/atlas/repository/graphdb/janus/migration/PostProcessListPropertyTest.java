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

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.janus.migration.postproc.PostProcessListProperty;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.atlas.repository.Constants.ATTRIBUTE_INDEX_PROPERTY_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class PostProcessListPropertyTest extends BaseUtils {
    private static final String HIVE_TABLE_TYPE       = "hive_table";
    private static final String HIVE_COLUMNS_PROPERTY = "hive_table.columns";
    private static final String COL_1_EDGE_ID         = "816u-35tc-ao0l-47so";
    private static final String COL_2_EDGE_ID         = "82rq-35tc-ao0l-2glc";

    @Test
    public void noRefNoUpdate() {
        TestSetup ts = new TestSetup();

        ts.getPostProcessListProperty().process(ts.getTable(), HIVE_TABLE_TYPE, HIVE_COLUMNS_PROPERTY);
        ts.assertIncomplete();
    }

    @Test
    public void refFoundVertexUpdated() {
        TestSetup ts = new TestSetup();

        assertNotNull(ts.getTable());

        ts.getPostProcessListProperty().process(ts.getTable(), HIVE_TABLE_TYPE, HIVE_COLUMNS_PROPERTY);
        ts.assertComplete();
    }

    @Test
    public void updateUsingPostProcessConsumer() {
        TestSetup                   ts       = new TestSetup();
        BlockingQueue<Object>       bc       = new BlockingArrayQueue<>();
        PostProcessManager.Consumer consumer = new PostProcessManager.Consumer(bc, ts.getGraph(), getTypePropertyMap("hive_table", HIVE_COLUMNS_PROPERTY, "ARRAY"), 5);
        Vertex                      tableV   = fetchTableVertex(ts.getGraph());

        consumer.processItem(tableV.id());

        ts.assertComplete();
    }

    private class TestSetup {
        private final PostProcessListProperty postProcessListProperty;
        private final TinkerGraph             tg;
        private final MappedElementCache      cache;
        private final Vertex                  tableV;

        public TestSetup() {
            postProcessListProperty = new PostProcessListProperty();
            tg                      = TinkerGraph.open();
            cache                   = new MappedElementCache();

            addEdge(tg, cache);

            tableV = fetchTableVertex(tg);

            assertSetup();
        }

        public PostProcessListProperty getPostProcessListProperty() {
            return postProcessListProperty;
        }

        public TinkerGraph getGraph() {
            return tg;
        }

        public MappedElementCache getCache() {
            return cache;
        }

        public Vertex getTable() {
            return tableV;
        }

        public void assertSetup() {
            assertTrue(tableV.property(HIVE_COLUMNS_PROPERTY).isPresent());

            List<?> list = (List<?>) tableV.property(HIVE_COLUMNS_PROPERTY).value();

            assertEquals(list.size(), 2);
            assertEquals(list.get(0), COL_1_EDGE_ID);
            assertEquals(list.get(1), COL_2_EDGE_ID);
        }

        public String getEdgeLabel(String property) {
            return Constants.INTERNAL_PROPERTY_KEY_PREFIX + property;
        }

        private void assertIncomplete() {
            assertPropertyRemoved(HIVE_COLUMNS_PROPERTY, tableV);

            Iterator<Edge> edges = tableV.edges(Direction.OUT, getEdgeLabel(HIVE_COLUMNS_PROPERTY));

            while (edges.hasNext()) {
                Edge e = edges.next();

                assertFalse(e.property(ATTRIBUTE_INDEX_PROPERTY_KEY).isPresent());
            }
        }

        private void assertComplete() {
            assertPropertyRemoved(HIVE_COLUMNS_PROPERTY, tableV);
        }

        private void assertPropertyRemoved(String property, Vertex tableV) {
            assertFalse(tableV.property(property).isPresent());
        }
    }
}
