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

package org.apache.atlas.discovery;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.query.HiveTitanSample;
import org.apache.atlas.query.QueryTestsUtils;
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.io.FileUtils;
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

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;

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

        String hiveGraphFile = FileUtils.getTempDirectory().getPath() + File.separator + System.nanoTime() + ".gson";
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
        r = discoveryService.searchByGremlin("g.V.filter{it.typeName == 'Person'}.'Person.name'.toList()");
        System.out.println("search result = " + r);
    }

    @DataProvider(name = "dslQueriesProvider")
    private Object[][] createDSLQueries() {
        return new String[][]{
                {"from DB"}, {"DB"}, {"DB where DB.name=\"Reporting\""}, {"DB DB.name = \"Reporting\""},
                {"DB where DB.name=\"Reporting\" select name, owner"}, {"DB has name"}, {"DB, Table"},
                {"DB is JdbcAccess"},
            /*
            {"DB, LoadProcess has name"},
            {"DB as db1, Table where db1.name = \"Reporting\""},
            {"DB where DB.name=\"Reporting\" and DB.createTime < " + System.currentTimeMillis()},
            */
                {"from Table"}, {"Table"}, {"Table is Dimension"}, {"Column where Column isa PII"},
                {"View is Dimension"},
            /*{"Column where Column isa PII select Column.name"},*/
                {"Column select Column.name"}, {"Column select name"}, {"Column where Column.name=\"customer_id\""},
                {"from Table select Table.name"}, {"DB where (name = \"Reporting\")"},
                {"DB where (name = \"Reporting\") select name as _col_0, owner as _col_1"},
                {"DB where DB is JdbcAccess"}, {"DB where DB has name"}, {"DB Table"}, {"DB where DB has name"},
                {"DB as db1 Table where (db1.name = \"Reporting\")"},
                {"DB where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 "},
                {"Table where (name = \"sales_fact\" and created > \"2014-01-01\" ) select name as _col_0, created as _col_1 "},
                {"Table where (name = \"sales_fact\" and created > \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, created as _col_1 "},
            /*
            todo: does not work
            {"DB where (name = \"Reporting\") and ((createTime + 1) > 0)"},
            {"DB as db1 Table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") select db1.name
            as dbName, tab.name as tabName"},
            {"DB as db1 Table as tab where ((db1.createTime + 1) > 0) or (db1.name = \"Reporting\") select db1.name
            as dbName, tab.name as tabName"},
            {"DB as db1 Table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner
             select db1.name as dbName, tab.name as tabName"},
            {"DB as db1 Table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner
             select db1.name as dbName, tab.name as tabName"},
            */
                // trait searches
                {"Dimension"},
            /*{"Fact"}, - todo: does not work*/
                {"JdbcAccess"}, {"ETL"}, {"Metric"}, {"PII"},
                // Lineage
                {"Table LoadProcess outputTable"}, {"Table loop (LoadProcess outputTable)"},
                {"Table as _loop0 loop (LoadProcess outputTable) withPath"},
                {"Table as src loop (LoadProcess outputTable) as dest select src.name as srcTable, dest.name as "
                        + "destTable withPath"},
                {"Table as t, sd, Column as c where t.name=\"sales_fact\" select c.name as colName, c.dataType as "
                        + "colType"},
                {"Table where name='sales_fact', db where name='Reporting'"}};
    }

    @Test(dataProvider = "dslQueriesProvider")
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
        Assert.assertTrue(rows.length() >= 0); // some queries may not have any results
        System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows");
    }

    @DataProvider(name = "invalidDslQueriesProvider")
    private Object[][] createInvalidDSLQueries() {
        return new String[][]{{"from Unknown"}, {"Unknown"}, {"Unknown is Blah"},};
    }

    @Test(dataProvider = "invalidDslQueriesProvider", expectedExceptions = DiscoveryException.class)
    public void testSearchByDSLInvalidQueries(String dslQuery) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        discoveryService.searchByDSL(dslQuery);
        Assert.fail();
    }

    @Test
    public void testSearchForTypeInheritance() throws Exception {
        createTypesWithMultiLevelInheritance();
        createInstances();

        String dslQuery = "from D where a = 1";
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        Assert.assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        System.out.println("results = " + results);
    }

    /*
     * Type Hierarchy is:
     *   A(a)
     *   B(b) extends A
     *   C(c) extends B
     *   D(d) extends C
     */
    private void createTypesWithMultiLevelInheritance() throws Exception {
        HierarchicalTypeDefinition A = createClassTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE));

        HierarchicalTypeDefinition B =
                createClassTypeDef("B", ImmutableList.of("A"), createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));

        HierarchicalTypeDefinition C =
                createClassTypeDef("C", ImmutableList.of("B"), createOptionalAttrDef("c", DataTypes.BYTE_TYPE));

        HierarchicalTypeDefinition D =
                createClassTypeDef("D", ImmutableList.of("C"), createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        TypeSystem.getInstance().defineClassTypes(A, B, C, D);
    }

    private void createInstances() throws Exception {
        Referenceable instance = new Referenceable("D");
        instance.set("d", 1);
        instance.set("c", 1);
        instance.set("b", true);
        instance.set("a", 1);

        ClassType deptType = TypeSystem.getInstance().getDataType(ClassType.class, "D");
        ITypedReferenceableInstance typedInstance = deptType.convert(instance, Multiplicity.REQUIRED);

        repositoryService.createEntity(typedInstance);
    }
}