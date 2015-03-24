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

package org.apache.hadoop.metadata.discovery;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.TestUtils;
import org.apache.hadoop.metadata.discovery.graph.GraphBackedDiscoveryService;
import org.apache.hadoop.metadata.query.HiveTitanSample;
import org.apache.hadoop.metadata.query.QueryTestsUtils;
import org.apache.hadoop.metadata.repository.graph.GraphBackedMetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphHelper;
import org.apache.hadoop.metadata.repository.graph.GraphProvider;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedDiscoveryServiceTest {

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    @BeforeClass
    public void setUp() throws Exception {
        TypeSystem typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        QueryTestsUtils.setupTypes();
        setupSampleData();

        TestUtils.defineDeptEmployeeTypes(typeSystem);

        Referenceable hrDept = TestUtils.createDeptEg1(typeSystem);
        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        repositoryService.createEntity(hrDept2);
    }

    private void setupSampleData() throws ScriptException {
        TitanGraph titanGraph = graphProvider.get();

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("g", titanGraph);

        String hiveGraphFile = FileUtils.getTempDirectory().getPath()
                + File.separator + System.nanoTime() + ".gson";
        System.out.println("hiveGraphFile = " + hiveGraphFile);
        HiveTitanSample.writeGson(hiveGraphFile);
        bindings.put("hiveGraphFile", hiveGraphFile);

        engine.eval("g.loadGraphSON(hiveGraphFile)", bindings);
        titanGraph.commit();

        System.out.println("*******************Graph Dump****************************");
        for (Vertex vertex : titanGraph.getVertices()) {
            System.out.println(GraphHelper.vertexString(vertex));
        }

        for (Edge edge : titanGraph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }
        System.out.println("*******************Graph Dump****************************");
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
    }

    @Test
    public void testSearchByDSL() throws Exception {
        String dslQuery = "from Department";

        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        Assert.assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        Assert.assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        Assert.assertNotNull(typeName);
        Assert.assertEquals(typeName, "Department");

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertNotNull(rows);
        Assert.assertEquals(rows.length(), 1);
    }

    @Test(expectedExceptions = Throwable.class)
    public void testSearchByDSLBadQuery() throws Exception {
        String dslQuery = "from blah";

        discoveryService.searchByDSL(dslQuery);
        Assert.fail();
    }

    @Test
    public void testRawSearch1() throws Exception {
        // Query for all Vertices in Graph
        Object r = discoveryService.searchByGremlin("g.V.toList()");
        System.out.println("search result = " + r);

        // Query for all Vertices of a Type
        r = discoveryService.searchByGremlin("g.V.filter{it.typeName == 'Department'}.toList()");
        System.out.println("search result = " + r);

        // Property Query: list all Person names
        r = discoveryService
                .searchByGremlin("g.V.filter{it.typeName == 'Person'}.'Person.name'.toList()");
        System.out.println("search result = " + r);
    }

    @DataProvider(name = "dslQueriesProvider")
    private Object[][] createDSLQueries() {
        return new String[][] {
                {"from DB"},
                {"DB"},
                {"from Table"},
                {"Table"},
                {"DB, Table"},
                /*{"DB as db1 Table where db1.name = \"Reporting\""},*/
                {"DB name = \"Reporting\""},
                {"Column where Column isa PII"},
                {"Table is Dimension"},
                {"View is Dimension"},
                /*{"Column where Column isa PII select Column.name"},*/
                {"Column select Column.name"},
                {"from Table select Table.name"},
        };
    }

    @Test (dataProvider = "dslQueriesProvider")
    public void testSearchByDSLQueries(String dslQuery) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        Assert.assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        Assert.assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        Assert.assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertNotNull(rows);
        Assert.assertTrue(rows.length() > 0);
    }

    @Test
    public void testSearchByDSLQuery() throws Exception {
        String dslQuery = "Column as PII";
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        Assert.assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        Assert.assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        Assert.assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertNotNull(rows);
        Assert.assertTrue(rows.length() > 0);

        for (int index = 0; index < rows.length(); index++) {
            JSONObject row = rows.getJSONObject(index);
            String type = row.getString("$typeName$");
            Assert.assertEquals(type, "Column");

            String name = row.getString("name");
            Assert.assertNotEquals(name, "null");
        }
    }
}