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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphHelper.VertexInfo;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

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

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    private TypeSystem typeSystem;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(graphProvider);

        TestUtils.defineDeptEmployeeTypes(typeSystem);
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetCompositeGuidsAndVertices() throws Exception {
        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(typeSystem);
        List<String> createdGuids = repositoryService.createEntities(hrDept);
        String deptGuid = null;
        Set<String> expectedGuids = new HashSet<>();

        for (String guid : createdGuids) {
            ITypedReferenceableInstance entityDefinition = repositoryService.getEntityDefinition(guid);
            expectedGuids.add(guid);
            if (entityDefinition.getId().getTypeName().equals(TestUtils.DEPARTMENT_TYPE)) {
                deptGuid = guid;
            }
        }
        Vertex deptVertex = GraphHelper.getInstance().getVertexForGUID(deptGuid);
        Set<VertexInfo> compositeVertices = GraphHelper.getInstance().getCompositeVertices(deptVertex);
        HashMap<String, VertexInfo> verticesByGuid = new HashMap<>();
        for (VertexInfo vertexInfo: compositeVertices) {
            verticesByGuid.put(vertexInfo.getGuid(), vertexInfo);
        }

        // Verify compositeVertices has entries for all expected guids.
        Assert.assertEquals(compositeVertices.size(), expectedGuids.size());
        for (String expectedGuid : expectedGuids) {
            Assert.assertTrue(verticesByGuid.containsKey(expectedGuid));
        }
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

        Iterator<Edge> iterator = GraphHelper.getInstance().getOutGoingEdgesByLabel(v1, "l1");
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertNull(iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }
}
