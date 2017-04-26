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

import org.apache.atlas.AtlasException;
import org.apache.atlas.TestOnlyModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphHelper.VertexInfo;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.*;

@Guice(modules = TestOnlyModule.class)
public class GraphHelperTest {


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
    private MetadataService metadataService;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    private TypeSystem typeSystem;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        new GraphBackedSearchIndexer(typeRegistry);
        TypesDef typesDef = TestUtils.defineHiveTypes();
        try {
            metadataService.getTypeDefinition(TestUtils.TABLE_TYPE);
        } catch (TypeNotFoundException e) {
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }
        TestUtils.defineDeptEmployeeTypes(typeSystem);
    }

    @AfterClass
    public void tearDown() {
        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testGetInstancesByUniqueAttributes() throws Exception {

        GraphHelper helper = GraphHelper.getInstance();
        List<ITypedReferenceableInstance> instances =  new ArrayList<>();
        List<String> guids = new ArrayList<>();
        TypeSystem ts = TypeSystem.getInstance();
        ClassType dbType = ts.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);

        for(int i = 0; i < 10; i++) {
            Referenceable db = TestUtils.createDBEntity();
            String guid = createInstance(db);
            ITypedReferenceableInstance instance = convert(db, dbType);
            instances.add(instance);
            guids.add(guid);
        }

        //lookup vertices via getVertexForInstanceByUniqueAttributes
        List<AtlasVertex> vertices = helper.getVerticesForInstancesByUniqueAttribute(dbType, instances);
        assertEquals(instances.size(), vertices.size());
        //assert vertex matches the vertex we get through getVertexForGUID
        for(int i = 0; i < instances.size(); i++) {
            String guid = guids.get(i);
            AtlasVertex foundVertex = vertices.get(i);
            AtlasVertex expectedVertex = helper.getVertexForGUID(guid);
            assertEquals(foundVertex, expectedVertex);
        }
    }
    @Test
    public void testGetVerticesForGUIDSWithDuplicates() throws Exception {
        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(TypeSystem.getInstance());
        List<String> result = repositoryService.createEntities(hrDept).getCreatedEntities();
        String guid = result.get(0);
        Map<String, AtlasVertex> verticesForGUIDs = GraphHelper.getInstance().getVerticesForGUIDs(Arrays.asList(guid, guid));
        Assert.assertEquals(verticesForGUIDs.size(), 1);
        Assert.assertTrue(verticesForGUIDs.containsKey(guid));
    }
    @Test
    public void testGetCompositeGuidsAndVertices() throws Exception {
        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(typeSystem);
        List<String> createdGuids = repositoryService.createEntities(hrDept).getCreatedEntities();
        String deptGuid = null;
        Set<String> expectedGuids = new HashSet<>();

        for (String guid : createdGuids) {
            ITypedReferenceableInstance entityDefinition = repositoryService.getEntityDefinition(guid);
            expectedGuids.add(guid);
            if (entityDefinition.getId().getTypeName().equals(TestUtils.DEPARTMENT_TYPE)) {
                deptGuid = guid;
            }
        }
        AtlasVertex deptVertex = GraphHelper.getInstance().getVertexForGUID(deptGuid);
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
        AtlasGraph graph = TestUtils.getGraph();
        AtlasVertex v1 = graph.addVertex();
        AtlasVertex v2 = graph.addVertex();
        graph.addEdge(v1, v2, "l1");
        graph.addEdge(v1, v2, "l2");

        Iterator<AtlasEdge> iterator = GraphHelper.getInstance().getOutGoingEdgesByLabel(v1, "l1");
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertNull(iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }

    private ITypedReferenceableInstance convert(Referenceable instance, ClassType type) throws AtlasException {

        return type.convert(instance, Multiplicity.REQUIRED);
    }

    private String createInstance(Referenceable entity) throws Exception {
        TestUtils.resetRequestContext();

        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        List<String> guids = metadataService.createEntities(entitiesJson.toString()).getCreatedEntities();
        if (guids != null && guids.size() > 0) {
            return guids.get(guids.size() - 1);
        }
        return null;
    }
}
