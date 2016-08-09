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

package org.apache.atlas.repository.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import org.apache.atlas.RepositoryMetadataModule;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphHelperTest {
    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @DataProvider(name = "encodeDecodeTestData")
    private Object[][] createTestData() {
        return new Object[][]{
                {"hivedb$", "hivedb_d"},
                {"hivedb", "hivedb"},
                {"{hivedb}", "_ohivedb_c"},
                {"%hivedb}", "_phivedb_c"},
                {"\"hivedb\"", "_qhivedb_q"},
                {"\"$%{}", "_q_d_p_o_c"},
                {"", ""},
                {"  ", "  "},
                {"\n\r", "\n\r"},
                {null, null}
        };
    }

    @Test(dataProvider = "encodeDecodeTestData")
    public void testEncodeDecode(String str, String expectedEncodedStr) throws Exception {
        String encodedStr = GraphHelper.encodePropertyKey(str);
        assertEquals(encodedStr, expectedEncodedStr);

        String decodedStr = GraphHelper.decodePropertyKey(encodedStr);
        assertEquals(decodedStr, str);
    }

    @Test
    public void testGetOutgoingEdgesByLabel() throws Exception {
        TitanGraph graph = graphProvider.get();
        TitanVertex v1 = graph.addVertex();
        TitanVertex v2 = graph.addVertex();

        v1.addEdge("l1", v2);
        v1.addEdge("l2", v2);

        Iterator<Edge> iterator = GraphHelper.getOutGoingEdgesByLabel(v1, "l1");
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertNull(iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }
}
