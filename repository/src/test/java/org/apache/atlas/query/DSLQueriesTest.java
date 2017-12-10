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
package org.apache.atlas.query;

import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.runner.LocalSolrRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class DSLQueriesTest extends BasicTestSetup {
    @Inject
    private EntityDiscoveryService discoveryService;

    @BeforeClass
    public void setup() throws Exception {
        LocalSolrRunner.start();
        setupTestData();
    }

    @AfterClass
    public void teardown() throws Exception {
        LocalSolrRunner.stop();
    }

    @DataProvider(name = "comparisonQueriesProvider")
    private Object[][] createComparisonQueries() {
        //create queries the exercise the comparison logic for
        //all of the different supported data types
        return new Object[][] {
                {"Person where (birthday < \"1950-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday > \"1975-01-01T02:35:58.440Z\" )", 2},
                {"Person where (birthday >= \"1975-01-01T02:35:58.440Z\" )", 2},
                {"Person where (birthday <= \"1950-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday = \"1975-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday != \"1975-01-01T02:35:58.440Z\" )", 4},

                {"Person where (hasPets = true)", 2},
                {"Person where (hasPets = false)", 2},
                {"Person where (hasPets != false)", 2},
                {"Person where (hasPets != true)", 2},

                {"Person where (numberOfCars > 0)", 2},
                {"Person where (numberOfCars > 1)", 1},
                {"Person where (numberOfCars >= 1)", 2},
                {"Person where (numberOfCars < 2)", 3},
                {"Person where (numberOfCars <= 2)", 4},
                {"Person where (numberOfCars = 2)", 1},
                {"Person where (numberOfCars != 2)", 3},

                {"Person where (houseNumber > 0)", 2},
                {"Person where (houseNumber > 17)", 1},
                {"Person where (houseNumber >= 17)", 2},
                {"Person where (houseNumber < 153)", 3},
                {"Person where (houseNumber <= 153)", 4},
                {"Person where (houseNumber =  17)", 1},
                {"Person where (houseNumber != 17)", 3},

                {"Person where (carMileage > 0)", 2},
                {"Person where (carMileage > 13)", 1},
                {"Person where (carMileage >= 13)", 2},
                {"Person where (carMileage < 13364)", 3},
                {"Person where (carMileage <= 13364)", 4},
                {"Person where (carMileage =  13)", 1},
                {"Person where (carMileage != 13)", 3},

                {"Person where (shares > 0)", 2},
                {"Person where (shares > 13)", 2},
                {"Person where (shares >= 16000)", 1},
                {"Person where (shares < 13364)", 2},
                {"Person where (shares <= 15000)", 3},
                {"Person where (shares =  15000)", 1},
                {"Person where (shares != 1)", 4},

                {"Person where (salary > 0)", 2},
                {"Person where (salary > 100000)", 2},
                {"Person where (salary >= 200000)", 1},
                {"Person where (salary < 13364)", 2},
                {"Person where (salary <= 150000)", 3},
                {"Person where (salary =  12334)", 0},
                {"Person where (salary != 12344)", 4},

                {"Person where (age > 36)", 1},
                {"Person where (age > 49)", 1},
                {"Person where (age >= 49)", 1},
                {"Person where (age < 50)", 3},
                {"Person where (age <= 35)", 2},
                {"Person where (age =  35)", 0},
                {"Person where (age != 35)", 4}
        };
    }

    @Test(dataProvider = "comparisonQueriesProvider")
    public void testComparisonQueries(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), expected);
    }

    @DataProvider(name = "dslQueriesProvider")
    private Object[][] createDSLQueries() {
        return new Object[][]{
                {"hive_db as inst where inst.name=\"Reporting\" select inst as id, inst.name", 1},
                {"from hive_db as h select h as id", 3},
                {"from hive_db", 3},
                {"hive_db", 3},
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db hive_db.name = \"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 3},
                {"hive_db, hive_table", 10},
                {"View is JdbcAccess", 2},
                {"hive_db as db1, hive_table where db1.name = \"Reporting\"", 0}, //Not working - ATLAS-145
                // - Final working query -> discoveryService.searchByGremlin("L:{_var_0 = [] as Set;g.V().has(\"__typeName\", \"hive_db\").fill(_var_0);g.V().has(\"__superTypeNames\", \"hive_db\").fill(_var_0);_var_0._().as(\"db1\").in(\"__hive_table.db\").back(\"db1\").and(_().has(\"hive_db.name\", T.eq, \"Reporting\")).toList()}")
                /*
                {"hive_db, hive_process has name"}, //Invalid query
                {"hive_db where hive_db.name=\"Reporting\" and hive_db.createTime < " + System.currentTimeMillis()}
                */
                {"from hive_table", 10},
                {"hive_table", 10},
                {"hive_table isa Dimension", 3},
                {"hive_column where hive_column isa PII", 8},
                {"View is Dimension" , 2},
//                {"hive_column where hive_column isa PII select hive_column.name", 6}, //Not working - ATLAS-175
                {"hive_column select hive_column.name", 37},
                {"hive_column select name",37},
                {"hive_column where hive_column.name=\"customer_id\"", 6},
                {"from hive_table select hive_table.name", 10},
                {"hive_db where (name = \"Reporting\")", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where hive_db is JdbcAccess", 0}, //Not supposed to work
                {"hive_db hive_table", 10},
                {"hive_db where hive_db has name", 3},
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
                {"ETL", 5},
                {"Metric", 9},
                {"PII", 8},
                {"`Log Data`", 4},
                // Not sure what the expected rows should be, but since we didn't assign or do anything with the created
                // I assume it'll be zero
                {"`isa`", 0},

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

                //check supertypeNames
                {"DataSet where name='sales_fact'", 1},
                {"Asset where name='sales_fact'", 1}
        };
    }

    @Test(dataProvider = "dslQueriesProvider")
    public void testBasicDSL(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), expected);
    }


    @DataProvider(name = "dslExplicitLimitQueriesProvider")
    private Object[][] createDSLQueriesWithExplicitLimit() {
        return new Object[][]{
                {"hive_column", 37, 40, 0},//with higher limit all rows returned
                {"hive_column limit 10", 10, 50, 0},//lower limit in query
                {"hive_column select hive_column.name limit 10", 5, 5, 0},//lower limit in query param
                {"hive_column select hive_column.name withPath", 20, 20, 0},//limit only in params
                //with offset, only remaining rows returned
                {"hive_column select hive_column.name limit 40 withPath", 17, 40, 20},
                //with higher offset, no rows returned
                {"hive_column select hive_column.name limit 40 withPath", 0, 40, 40},
                //offset used from query
                {"hive_column select hive_column.name limit 40 offset 10", 27, 40, 0},
                //offsets in query and parameter added up
                {"hive_column select hive_column.name limit 40 offset 10", 17, 40, 10},
                //works with where clause
                {"hive_db where name = 'Reporting' limit 10 offset 0", 1, 40, 0},
                //works with joins
                {"hive_db, hive_table where db.name = 'Reporting' limit 10", 1, 1, 0},
                {"hive_column limit 25", 5, 10, 20},    //last page should return records limited by limit in query
                {"hive_column limit 25", 0, 10, 30},    //offset > limit returns 0 rows
        };
    }

    @Test(dataProvider = "dslExplicitLimitQueriesProvider")
    public void testExplicitDSL(String query, int expected, int limit, int offset) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, limit, offset);
        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), expected);
    }

    @DataProvider(name = "dslLimitQueriesProvider")
    private Object[][] createDSLQueriesWithLimit() {
        return new Object[][]{
                {"hive_column  limit 10 ", 10},
                {"hive_column select hive_column.name limit 10 ", 10},
                {"hive_column select hive_column.name  withPath", 37},
                {"hive_column select hive_column.name limit 10 withPath", 10},

                {"from hive_db", 3},
                {"from hive_db limit 2", 2},
                {"from hive_db limit 2 offset 0", 2},
                {"from hive_db limit 2 offset 1", 2},
                {"from hive_db limit 3 offset 1", 2},
                {"hive_db", 3},
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" or hive_db.name=\"Sales\" or hive_db.name=\"Logging\" limit 1 offset 1", 1},
                {"hive_db where hive_db.name=\"Reporting\" or hive_db.name=\"Sales\" or hive_db.name=\"Logging\" limit 1 offset 2", 1},
                {"hive_db where hive_db.name=\"Reporting\" or hive_db.name=\"Sales\" or hive_db.name=\"Logging\" limit 2 offset 1", 2},
                {"hive_db where hive_db.name=\"Reporting\" limit 10 ", 1},
                {"hive_db hive_db.name = \"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 3},
                {"hive_db has name limit 2 offset 0", 2},
                {"hive_db has name limit 2 offset 1", 2},
                {"hive_db has name limit 10 offset 1", 2},
                {"hive_db has name limit 10 offset 0", 3},
                {"hive_db, hive_table", 10},
                {"hive_db, hive_table limit 5", 5},
                {"hive_db, hive_table limit 5 offset 0", 5},
                {"hive_db, hive_table limit 5 offset 5", 5},

                {"View is JdbcAccess", 2},
                {"View is JdbcAccess limit 1", 1},
                {"View is JdbcAccess limit 2 offset 1", 1},
                {"hive_db as db1, hive_table where db1.name = \"Reporting\"", 0}, //Not working - ATLAS-145


                {"from hive_table", 10},
                {"from hive_table limit 5", 5},
                {"from hive_table limit 5 offset 5", 5},

                {"hive_table", 10},
                {"hive_table limit 5", 5},
                {"hive_table limit 5 offset 5", 5},

                {"hive_table isa Dimension", 3},
                {"hive_table isa Dimension limit 2", 2},
                {"hive_table isa Dimension limit 2 offset 0", 2},
                {"hive_table isa Dimension limit 2 offset 1", 2},
                {"hive_table isa Dimension limit 3 offset 1", 2},

                {"hive_column where hive_column isa PII", 8},
                {"hive_column where hive_column isa PII limit 5", 5},
                {"hive_column where hive_column isa PII limit 5 offset 1", 5},
                {"hive_column where hive_column isa PII limit 5 offset 5", 3},


                {"View is Dimension" , 2},
                {"View is Dimension limit 1" , 1},
                {"View is Dimension limit 1 offset 1" , 1},
                {"View is Dimension limit 10 offset 1" , 1},

                {"hive_column select hive_column.name", 37},
                {"hive_column select hive_column.name limit 5", 5},
                {"hive_column select hive_column.name limit 5 offset 36", 1},

                {"hive_column select name", 37},
                {"hive_column select name limit 5", 5},
                {"hive_column select name limit 5 offset 36 ", 1},

                {"hive_column where hive_column.name=\"customer_id\"", 6},
                {"hive_column where hive_column.name=\"customer_id\" limit 2", 2},
                {"hive_column where hive_column.name=\"customer_id\" limit 2 offset 1", 2},
                {"hive_column where hive_column.name=\"customer_id\" limit 10 offset 3", 3},

                {"from hive_table select hive_table.name", 10},
                {"from hive_table select hive_table.name limit 5", 5},
                {"from hive_table select hive_table.name limit 5 offset 5", 5},

                {"hive_db where (name = \"Reporting\")", 1},
                {"hive_db where (name = \"Reporting\") limit 10", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1 limit 10", 1},
                {"hive_db where hive_db is JdbcAccess", 0}, //Not supposed to work
                {"hive_db hive_table", 10},
                {"hive_db hive_table limit 5", 5},
                {"hive_db hive_table limit 5 offset 5", 5},
                {"hive_db where hive_db has name", 3},
                {"hive_db where hive_db has name limit 5", 3},
                {"hive_db where hive_db has name limit 2 offset 0", 2},
                {"hive_db where hive_db has name limit 2 offset 1", 2},

                {"hive_db as db1 hive_table where (db1.name = \"Reporting\")", 0}, //Not working -> ATLAS-145
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 ", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 limit 10", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 limit 10 offset 1", 0},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 limit 10 offset 0", 1},

                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 offset 0", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 offset 5", 0},

                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10 offset 0", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10 offset 1", 0},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 0 offset 1", 0},

                // trait searches
                {"Dimension", 5},
                {"Dimension limit 2", 2},
                {"Dimension limit 2 offset 1", 2},
                {"Dimension limit 5 offset 4", 1},

                {"JdbcAccess", 2},
                {"JdbcAccess limit 5 offset 0", 2},
                {"JdbcAccess limit 2 offset 1", 1},
                {"JdbcAccess limit 1", 1},

                {"ETL", 5},
                {"ETL limit 2", 2},
                {"ETL limit 1", 1},
                {"ETL limit 1 offset 0", 1},
                {"ETL limit 2 offset 1", 2},

                {"Metric", 9},
                {"Metric limit 10", 9},
                {"Metric limit 2", 2},
                {"Metric limit 10 offset 1", 8},



                {"PII", 8},
                {"PII limit 10", 8},
                {"PII limit 2", 2},
                {"PII limit 10 offset 1", 7},

                {"`Log Data`", 4},
                {"`Log Data` limit 3", 3},
                {"`Log Data` limit 10 offset 2", 2},


                {"hive_table where name='sales_fact', db where name='Sales'", 1},
                {"hive_table where name='sales_fact', db where name='Sales' limit 10", 1},
                {"hive_table where name='sales_fact', db where name='Sales' limit 10 offset 1", 0},
                {"hive_table where name='sales_fact', db where name='Reporting'", 0},
                {"hive_table where name='sales_fact', db where name='Reporting' limit 10", 0},
                {"hive_table where name='sales_fact', db where name='Reporting' limit 10 offset 1", 0},
                {"hive_partition as p where values = ['2015-01-01']", 1},
                {"hive_partition as p where values = ['2015-01-01'] limit 10", 1},
                {"hive_partition as p where values = ['2015-01-01'] limit 10 offset 1", 0},

        };
    }

    @Test(dataProvider = "dslLimitQueriesProvider")
    public void testDSLLimitQueries(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), expected);
    }



    @DataProvider(name = "dslOrderByQueriesProvider")
    private Object[][] createDSLQueriesWithOrderBy() {
        return new Object[][]{
                //test with alias
                // {"from hive_db select hive_db.name as 'o' orderby o limit 3", 3, "name", isAscending},
                {"from hive_db as h orderby h.owner limit 3", 3, "owner", true},
                {"hive_column as c select c.name orderby hive_column.name ", 37, "c.name", true},
                {"hive_column as c select c.name orderby hive_column.name limit 5", 5, "c.name", true},
                {"hive_column as c select c.name orderby hive_column.name desc limit 5", 5, "c.name", false},

                {"from hive_db orderby hive_db.owner limit 3", 3, "owner", true},
                {"hive_column select hive_column.name orderby hive_column.name ", 37, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name limit 5", 5, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name desc limit 5", 5, "hive_column.name", false},

                {"from hive_db orderby owner limit 3", 3, "owner", true},
                {"hive_column select hive_column.name orderby name ", 37, "hive_column.name", true},
                {"hive_column select hive_column.name orderby name limit 5", 5, "hive_column.name", true},
                {"hive_column select hive_column.name orderby name desc limit 5", 5, "hive_column.name", false},

                //Not working, the problem is in server code not figuring out how to sort. not sure if it is valid use case.
//               {"hive_db  hive_table  orderby 'hive_db.owner'", 10, "owner", isAscending},
//               {"hive_db hive_table orderby 'hive_db.owner' limit 5", 5, "owner", isAscending},
//               {"hive_db hive_table orderby 'hive_db.owner' limit 5 offset 5", 3, "owner", isAscending},

                {"hive_db select hive_db.description orderby hive_db.description limit 10 withPath", 3, "hive_db.description", true},
                {"hive_db select hive_db.description orderby hive_db.description desc limit 10 withPath", 3, "hive_db.description", false},

                {"hive_column select hive_column.name orderby hive_column.name limit 10 withPath", 10, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name asc limit 10 withPath", 10, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name desc limit 10 withPath", 10, "hive_column.name", false},
                {"from hive_db orderby hive_db.owner limit 3", 3, "owner", true},
                {"hive_db where hive_db.name=\"Reporting\" orderby 'owner'", 1, "owner", true},

                {"hive_db where hive_db.name=\"Reporting\" orderby hive_db.owner limit 10 ", 1, "owner", true},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner orderby hive_db.name ", 1, "name", true},
                {"hive_db has name orderby hive_db.owner limit 10 offset 0", 3, "owner", true},

                {"from hive_table select hive_table.owner orderby hive_table.owner", 10, "hive_table.owner", true},
                {"from hive_table select hive_table.owner  orderby hive_table.owner limit 8", 8, "hive_table.owner", true},

                {"hive_table orderby hive_table.name", 10, "name", true},

                {"hive_table orderby hive_table.owner", 10, "owner", true},
                {"hive_table orderby hive_table.owner limit 8", 8, "owner", true},
                {"hive_table orderby hive_table.owner limit 8 offset 0", 8, "owner", true},
                {"hive_table orderby hive_table.owner desc limit 8 offset 0", 8, "owner", false},

                //Not working because of existing bug Atlas-175
//                   {"hive_table isa Dimension orderby hive_table.owner", 3, "hive_table.owner", isAscending},//order not working
//                   {"hive_table isa Dimension orderby hive_table.owner limit 3", 3, "hive_table.owner", isAscending},
//                   {"hive_table isa Dimension orderby hive_table.owner limit 3 offset 0", 3, "hive_table.owner", isAscending},
//                   {"hive_table isa Dimension orderby hive_table.owner desc limit 3 offset 0", 3, "hive_table.owner", !isAscending},
//
//                   {"hive_column where hive_column isa PII orderby hive_column.name", 6, "hive_column.name", isAscending},
//                   {"hive_column where hive_column isa PII orderby hive_column.name limit 5", 5, "hive_column.name", isAscending},
//                   {"hive_column where hive_column isa PII orderby hive_column.name limit 5 offset 1", 5, "hive_column.name", isAscending},
//                   {"hive_column where hive_column isa PII orderby hive_column.name desc limit 5 offset 1", 5, "hive_column.name", !isAscending},

                {"hive_column select hive_column.name orderby hive_column.name ", 37, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name limit 5", 5, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name desc limit 5", 5, "hive_column.name", false},

                {"hive_column select hive_column.name orderby hive_column.name limit 5 offset 28", 5, "hive_column.name", true},

                {"hive_column select name orderby hive_column.name", 37, "name", true},
                {"hive_column select name orderby hive_column.name limit 5", 5, "name", true},
                {"hive_column select name orderby hive_column.name desc", 37, "name", false},

                {"hive_column where hive_column.name=\"customer_id\" orderby hive_column.name", 6, "name", true},
                {"hive_column where hive_column.name=\"customer_id\" orderby hive_column.name limit 2", 2, "name", true},
                {"hive_column where hive_column.name=\"customer_id\" orderby hive_column.name limit 2 offset 1", 2, "name", true},

                {"from hive_table select owner orderby hive_table.owner",10, "owner", true},
                {"from hive_table select owner orderby hive_table.owner limit 5", 5, "owner", true},
                {"from hive_table select owner orderby hive_table.owner desc limit 5", 5, "owner", false},
                {"from hive_table select owner orderby hive_table.owner limit 5 offset 5", 5, "owner", true},

                {"hive_db where (name = \"Reporting\") orderby hive_db.name", 1, "name", true},
                {"hive_db where (name = \"Reporting\") orderby hive_db.name limit 10", 1, "name", true},
                {"hive_db where hive_db has name orderby hive_db.owner", 3, "owner", true},
                {"hive_db where hive_db has name orderby hive_db.owner limit 5", 3, "owner", true},
                {"hive_db where hive_db has name orderby hive_db.owner limit 2 offset 0", 2, "owner", true},
                {"hive_db where hive_db has name orderby hive_db.owner limit 2 offset 1", 2, "owner", true},


                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1'", 1, "_col_1", true},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1' limit 10", 1, "_col_1", true},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1' limit 10 offset 1", 0, "_col_1", true},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1' limit 10 offset 0", 1, "_col_1", true},

                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' ", 1, "_col_1", true},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' limit 10 ", 1, "_col_1", true},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' limit 10 offset 0", 1, "_col_1", true},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' limit 10 offset 5", 0, "_col_1", true},

                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' ", 1, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 10 offset 0", 1, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 10 offset 1", 0, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 10", 1, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 0 offset 1", 0, "_col_0", true},

                {"hive_column select hive_column.name orderby hive_column.name limit 10 withPath", 10, "hive_column.name", true},
                {"hive_column select hive_column.name orderby hive_column.name limit 10 withPath", 10, "hive_column.name", true},
                {"hive_table orderby 'hive_table.owner_notdefined'", 10, null, true},
        };
    }

    @Test(dataProvider = "dslOrderByQueriesProvider")
    public void testOrderByDSL(String query, int expected, String orderBy, boolean ascending) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), expected);
        // TODO: Implement order checking here
    }

    @DataProvider(name = "dslLikeQueriesProvider")
    private Object[][] createDslLikeQueries() {
        return new Object[][]{
                {"hive_table where name like \"sa?es*\"", 3},
                {"hive_db where name like \"R*\"", 1},
                {"hive_db where hive_db.name like \"R???rt?*\" or hive_db.name like \"S?l?s\" or hive_db.name like\"Log*\"", 3},
                {"hive_db where hive_db.name like \"R???rt?*\" and hive_db.name like \"S?l?s\" and hive_db.name like\"Log*\"", 0},
                {"hive_table where name like 'sales*', db where name like 'Sa?es'", 1},
                {"hive_table where name like 'sales*' and db.name like 'Sa?es'", 1},
                {"hive_table where db.name like \"Sa*\"", 4},
                {"hive_table where db.name like \"Sa*\" and name like \"*dim\"", 3},
        };
    }

    @Test(dataProvider = "comparisonQueriesProvider")
    public void testLikeQueries(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertNotNull(searchResult.getEntities());
        assertEquals(searchResult.getEntities().size(), expected);
    }



    // TODO: Implement FieldValidator with new Data types
//    @DataProvider(name = "dslGroupByQueriesProvider")
//    private Object[][] createDSLGroupByQueries() {
//        return new Object[][]{
//                { "from Person as p, mentor as m groupby(m.name) select m.name, count()",
//                        new FieldValueValidator().withFieldNames("m.name", "count()").withExpectedValues("Max", 1)
//                                .withExpectedValues("Julius", 1) },
//
//                // This variant of this query is currently failing.  See OMS-335 for details.
//                { "from Person as p, mentor groupby(mentor.name) select mentor.name, count()",
//                        new FieldValueValidator().withFieldNames("mentor.name", "count()").withExpectedValues("Max", 1)
//                                .withExpectedValues("Julius", 1) },
//
//                { "from Person, mentor groupby(mentor.name) select mentor.name, count()",
//                        new FieldValueValidator().withFieldNames("mentor.name", "count()").withExpectedValues("Max", 1)
//                                .withExpectedValues("Julius", 1) },
//
//                { "from Person, mentor as m groupby(m.name) select m.name, count()",
//                        new FieldValueValidator().withFieldNames("m.name", "count()").withExpectedValues("Max", 1)
//                                .withExpectedValues("Julius", 1) },
//
//                { "from Person groupby (isOrganDonor) select count()",
//                        new FieldValueValidator().withFieldNames("count()").withExpectedValues(2)
//                                .withExpectedValues(2) },
//                { "from Person groupby (isOrganDonor) select Person.isOrganDonor, count()",
//                        new FieldValueValidator().withFieldNames("Person.isOrganDonor", "count()")
//                                                 .withExpectedValues(true, 2).withExpectedValues(false, 2) },
//
//                { "from Person groupby (isOrganDonor) select Person.isOrganDonor as 'organDonor', count() as 'count', max(Person.age) as 'max', min(Person.age) as 'min'",
//                        new FieldValueValidator().withFieldNames("organDonor", "max", "min", "count")
//                                                 .withExpectedValues(true, 50, 36, 2).withExpectedValues(false, 0, 0, 2) },
//
//                { "from hive_db groupby (owner, name) select count() ", new FieldValueValidator()
//                                                                                .withFieldNames("count()").withExpectedValues(1).withExpectedValues(1).withExpectedValues(1) },
//
//                { "from hive_db groupby (owner, name) select hive_db.owner, hive_db.name, count() ",
//                        new FieldValueValidator().withFieldNames("hive_db.owner", "hive_db.name", "count()")
//                                                 .withExpectedValues("Jane BI", "Reporting", 1)
//                                                 .withExpectedValues("Tim ETL", "Logging", 1)
//                                .withExpectedValues("John ETL", "Sales", 1) },
//
//                { "from hive_db groupby (owner) select count() ",
//                        new FieldValueValidator().withFieldNames("count()").withExpectedValues(1).withExpectedValues(1)
//                                .withExpectedValues(1) },
//
//                { "from hive_db groupby (owner) select hive_db.owner, count() ",
//                        new FieldValueValidator().withFieldNames("hive_db.owner", "count()")
//                                                 .withExpectedValues("Jane BI", 1).withExpectedValues("Tim ETL", 1)
//                                .withExpectedValues("John ETL", 1) },
//
//                { "from hive_db groupby (owner) select hive_db.owner, max(hive_db.name) ",
//                        new FieldValueValidator().withFieldNames("hive_db.owner", "max(hive_db.name)")
//                                                 .withExpectedValues("Tim ETL", "Logging").withExpectedValues("Jane BI", "Reporting")
//                                .withExpectedValues("John ETL", "Sales") },
//
//                { "from hive_db groupby (owner) select max(hive_db.name) ",
//                        new FieldValueValidator().withFieldNames("max(hive_db.name)").withExpectedValues("Logging")
//                                                 .withExpectedValues("Reporting").withExpectedValues("Sales") },
//
//                { "from hive_db groupby (owner) select owner, hive_db.name, min(hive_db.name)  ",
//                        new FieldValueValidator().withFieldNames("owner", "hive_db.name", "min(hive_db.name)")
//                                                 .withExpectedValues("Tim ETL", "Logging", "Logging")
//                                                 .withExpectedValues("Jane BI", "Reporting", "Reporting")
//                                .withExpectedValues("John ETL", "Sales", "Sales") },
//
//                { "from hive_db groupby (owner) select owner, min(hive_db.name)  ",
//                        new FieldValueValidator().withFieldNames("owner", "min(hive_db.name)")
//                                                 .withExpectedValues("Tim ETL", "Logging").withExpectedValues("Jane BI", "Reporting")
//                                .withExpectedValues("John ETL", "Sales") },
//
//                { "from hive_db groupby (owner) select min(name)  ",
//                        new FieldValueValidator().withFieldNames("min(name)")
//                                                 .withExpectedValues("Reporting").withExpectedValues("Logging")
//                                .withExpectedValues("Sales") },
//
//                { "from hive_db groupby (owner) select min('name') ",
//                        new FieldValueValidator().withFieldNames("min(\"name\")").withExpectedValues("name")
//                                                 .withExpectedValues("name").withExpectedValues("name") }, //finding the minimum of a constant literal expression...
//
//                { "from hive_db groupby (owner) select name ",
//                        new FieldValueValidator().withFieldNames("name").withExpectedValues("Reporting")
//                                                 .withExpectedValues("Sales").withExpectedValues("Logging") },
//
//                //implied group by
//                { "from hive_db select count() ",
//                        new FieldValueValidator().withFieldNames("count()").withExpectedValues(3) },
//                //implied group by
//                { "from Person select count() as 'count', max(Person.age) as 'max', min(Person.age) as 'min'",
//                        new FieldValueValidator().withFieldNames("max", "min", "count").withExpectedValues(50, 0, 4) },
//                //Sum
//                { "from Person groupby (isOrganDonor) select count() as 'count', sum(Person.age) as 'sum'",
//                        new FieldValueValidator().withFieldNames("count", "sum").withExpectedValues(2, 0)
//                                .withExpectedValues(2, 86) },
//                { "from Person groupby (isOrganDonor) select Person.isOrganDonor as 'organDonor', count() as 'count', sum(Person.age) as 'sum'",
//                        new FieldValueValidator().withFieldNames("organDonor", "count", "sum").withExpectedValues(false, 2, 0)
//                                .withExpectedValues(true, 2, 86) },
//                { "from Person select count() as 'count', sum(Person.age) as 'sum'",
//                        new FieldValueValidator().withFieldNames("count", "sum").withExpectedValues(4, 86) },
//                // tests to ensure that group by works with order by and limit
//                { "from hive_db groupby (owner) select min(name) orderby name limit 2 ",
//                        new FieldValueValidator().withFieldNames("min(name)")
//                                                 .withExpectedValues("Logging").withExpectedValues("Reporting")
//                },
//
//                { "from hive_db groupby (owner) select min(name) orderby name desc limit 2 ",
//                        new FieldValueValidator().withFieldNames("min(name)")
//                                                 .withExpectedValues("Reporting").withExpectedValues("Sales")
//                },
//        };
//    }
//
//    @DataProvider(name = "dslObjectQueriesReturnIdProvider")
//    private Object[][] createDSLObjectIdQueries() {
//        return new Object[][] { {
//                "from hive_db as h select h as id",
//                new FieldValueValidator().withFieldNames("id")
//                                         .withExpectedValues(idType).withExpectedValues(idType)
//                        .withExpectedValues(idType) }
//        };
//    }

}
