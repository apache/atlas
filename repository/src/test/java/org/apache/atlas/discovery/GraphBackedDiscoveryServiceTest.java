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
import org.apache.atlas.BaseHiveRepositoryTest;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedDiscoveryServiceTest extends BaseHiveRepositoryTest {

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private MetadataRepository repositoryService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        TypeSystem typeSystem = TypeSystem.getInstance();
        TestUtils.defineDeptEmployeeTypes(typeSystem);

        Referenceable hrDept = TestUtils.createDeptEg1(typeSystem);
        ClassType deptType = typeSystem.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        repositoryService.createEntities(hrDept2);
    }

    @AfterClass
    public void tearDown() throws Exception {
        super.tearDown();
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
        return new Object[][]{
                {"from hive_db", 2},
                {"hive_db", 2},
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db hive_db.name = \"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 2},
                {"hive_db, hive_table", 6},
                {"View is JdbcAccess", 2},
                {"hive_db as db1, hive_table where db1.name = \"Reporting\"", 0}, //Not working - ATLAS-145
                // - Final working query -> discoveryService.searchByGremlin("L:{_var_0 = [] as Set;g.V().has(\"__typeName\", \"hive_db\").fill(_var_0);g.V().has(\"__superTypeNames\", \"hive_db\").fill(_var_0);_var_0._().as(\"db1\").in(\"__hive_table.db\").back(\"db1\").and(_().has(\"hive_db.name\", T.eq, \"Reporting\")).toList()}")
                /*
                {"hive_db, hive_process has name"}, //Invalid query
                {"hive_db where hive_db.name=\"Reporting\" and hive_db.createTime < " + System.currentTimeMillis()}
                */
                {"from hive_table", 6},
                {"hive_table", 6},
                {"hive_table isa Dimension", 3},
                {"hive_column where hive_column isa PII", 6},
                {"View is Dimension" , 2},
//                {"hive_column where hive_column isa PII select hive_column.name", 6}, //Not working - ATLAS-175
                {"hive_column select hive_column.name", 27},
                {"hive_column select name", 27},
                {"hive_column where hive_column.name=\"customer_id\"", 4},
                {"from hive_table select hive_table.name", 6},
                {"hive_db where (name = \"Reporting\")", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where hive_db is JdbcAccess", 0}, //Not supposed to work
                {"hive_db hive_table", 6},
                {"hive_db where hive_db has name", 2},
                {"hive_db as db1 hive_table where (db1.name = \"Reporting\")", 0}, //Not working -> ATLAS-145
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 ", 1},

            /*
            todo: does not work - ATLAS-146
            {"hive_db where (name = \"Reporting\") and ((createTime + 1) > 0)"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") select db1.name
            as dbName, tab.name as tabName"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) or (db1.name = \"Reporting\") select db1.name
            as dbName, tab.name as tabName"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner
             select db1.name as dbName, tab.name as tabName"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner
             select db1.name as dbName, tab.name as tabName"},
            */
                // trait searches
                {"Dimension", 5},
                {"JdbcAccess", 2},
                {"ETL", 2},
                {"Metric", 5},
                {"PII", 6},

                /* Lineage queries are fired through ClosureQuery and are tested through HiveLineageJerseyResourceIt in webapp module.
                   Commenting out the below queries since DSL to Gremlin parsing/translation fails with lineage queries when there are array types
                   used within loop expressions which is the case with DataSet.inputs and outputs.`
                  // Lineage
                  {"Table LoadProcess outputTable"}, {"Table loop (LoadProcess outputTable)"},
                  {"Table as _loop0 loop (LoadProcess outputTable) withPath"},
                  {"Table as src loop (LoadProcess outputTable) as dest select src.name as srcTable, dest.name as "
                                        + "destTable withPath"},
                 */
//                {"hive_table as t, sd, hive_column as c where t.name=\"sales_fact\" select c.name as colName, c.dataType as "
//                        + "colType", 0}, //Not working - ATLAS-145 and ATLAS-166

                {"hive_table where name='sales_fact', db where name='Sales'", 1},
                {"hive_table where name='sales_fact', db where name='Reporting'", 0},
                {"hive_partition as p where values = ['2015-01-01']", 1},
//              {"StorageDesc select cols", 6} //Not working since loading of lists needs to be fixed yet
        };
    }

    @Test(dataProvider = "dslQueriesProvider")
    public void  testSearchByDSLQueries(String dslQuery, Integer expectedNumRows) throws Exception {
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
        Assert.assertEquals(rows.length(), expectedNumRows.intValue()); // some queries may not have any results
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

        repositoryService.createEntities(typedInstance);
    }
}