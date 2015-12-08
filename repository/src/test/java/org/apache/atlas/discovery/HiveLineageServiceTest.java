/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.discovery;

import org.apache.atlas.BaseHiveRepositoryTest;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

/**
 * Unit tests for Hive LineageService.
 */
@Guice(modules = RepositoryMetadataModule.class)
public class HiveLineageServiceTest extends BaseHiveRepositoryTest {

    @Inject
    private DiscoveryService discoveryService;

    @Inject
    private HiveLineageService hiveLineageService;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterClass
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @DataProvider(name = "dslQueriesProvider")
    private Object[][] createDSLQueries() {
        return new String[][]{
            // joins
            {"hive_table where name=\"sales_fact\", columns"},
            {"hive_table where name=\"sales_fact\", columns select name, dataType, comment"},
            {"hive_table where name=\"sales_fact\", columns as c select c.name, c.dataType, c.comment"},
            //            {"hive_db as db where (db.name=\"Reporting\"), hive_table as table select db.name,
            // table.name"},
            {"from hive_db"}, {"hive_db"}, {"hive_db where hive_db.name=\"Reporting\""},
            {"hive_db hive_db.name = \"Reporting\""},
            {"hive_db where hive_db.name=\"Reporting\" select name, owner"}, {"hive_db has name"},
            //            {"hive_db, hive_table"},
            //            {"hive_db, hive_process has name"},
            //            {"hive_db as db1, hive_table where db1.name = \"Reporting\""},
            //            {"hive_db where hive_db.name=\"Reporting\" and hive_db.createTime < " + System
            // .currentTimeMillis()},
            {"from hive_table"}, {"hive_table"}, {"hive_table is Dimension"},
            {"hive_column where hive_column isa PII"},
            //            {"hive_column where hive_column isa PII select hive_column.name"},
            {"hive_column select hive_column.name"}, {"hive_column select name"},
            {"hive_column where hive_column.name=\"customer_id\""}, {"from hive_table select hive_table.name"},
            {"hive_db where (name = \"Reporting\")"},
            {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1"},
            {"hive_db where hive_db has name"},
            //            {"hive_db hive_table"},
            {"hive_db where hive_db has name"},
            //            {"hive_db as db1 hive_table where (db1.name = \"Reporting\")"},
            {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 "},
            //            {"hive_db where (name = \"Reporting\") and ((createTime + 1) > 0)"},
            //            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name =
            // \"Reporting\") select db1.name as dbName, tab.name as tabName"},
            //            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) or (db1.name =
            // \"Reporting\") select db1.name as dbName, tab.name as tabName"},
            //            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name =
            // \"Reporting\") or db1 has owner select db1.name as dbName, tab.name as tabName"},
            //            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name =
            // \"Reporting\") or db1 has owner select db1.name as dbName, tab.name as tabName"},
            // trait searches
            {"Dimension"}, {"Fact"}, {"ETL"}, {"Metric"}, {"PII"},};
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

    @Test
    public void testGetInputs() throws Exception {
        JSONObject results = new JSONObject(hiveLineageService.getInputs("sales_fact_monthly_mv"));
        Assert.assertNotNull(results);
        System.out.println("inputs = " + results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertTrue(rows.length() > 0);

        final JSONObject row = rows.getJSONObject(0);
        JSONArray paths = row.getJSONArray("path");
        Assert.assertTrue(paths.length() > 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetInputsTableNameNull() throws Exception {
        hiveLineageService.getInputs(null);
        Assert.fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetInputsTableNameEmpty() throws Exception {
        hiveLineageService.getInputs("");
        Assert.fail();
    }

    @Test(expectedExceptions = EntityNotFoundException.class)
    public void testGetInputsBadTableName() throws Exception {
        hiveLineageService.getInputs("blah");
        Assert.fail();
    }

    @Test
    public void testGetInputsGraph() throws Exception {
        JSONObject results = new JSONObject(hiveLineageService.getInputsGraph("sales_fact_monthly_mv"));
        Assert.assertNotNull(results);
        System.out.println("inputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        Assert.assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 4);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test
    public void testGetOutputs() throws Exception {
        JSONObject results = new JSONObject(hiveLineageService.getOutputs("sales_fact"));
        Assert.assertNotNull(results);
        System.out.println("outputs = " + results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertTrue(rows.length() > 0);

        final JSONObject row = rows.getJSONObject(0);
        JSONArray paths = row.getJSONArray("path");
        Assert.assertTrue(paths.length() > 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetOututsTableNameNull() throws Exception {
        hiveLineageService.getOutputs(null);
        Assert.fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetOutputsTableNameEmpty() throws Exception {
        hiveLineageService.getOutputs("");
        Assert.fail();
    }

    @Test(expectedExceptions = EntityNotFoundException.class)
    public void testGetOutputsBadTableName() throws Exception {
        hiveLineageService.getOutputs("blah");
        Assert.fail();
    }

    @Test
    public void testGetOutputsGraph() throws Exception {
        JSONObject results = new JSONObject(hiveLineageService.getOutputsGraph("sales_fact"));
        Assert.assertNotNull(results);
        System.out.println("outputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        Assert.assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 3);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @DataProvider(name = "tableNamesProvider")
    private Object[][] tableNames() {
        return new String[][]{{"sales_fact", "4"}, {"time_dim", "3"}, {"sales_fact_daily_mv", "4"},
            {"sales_fact_monthly_mv", "4"}};
    }

    @Test(dataProvider = "tableNamesProvider")
    public void testGetSchema(String tableName, String expected) throws Exception {
        JSONObject results = new JSONObject(hiveLineageService.getSchema(tableName));
        Assert.assertNotNull(results);
        System.out.println("columns = " + results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertEquals(rows.length(), Integer.parseInt(expected));

        for (int index = 0; index < rows.length(); index++) {
            final JSONObject row = rows.getJSONObject(index);
            Assert.assertNotNull(row.getString("name"));
            Assert.assertNotNull(row.getString("comment"));
            Assert.assertNotNull(row.getString("dataType"));
            Assert.assertEquals(row.getString("$typeName$"), "hive_column");
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetSchemaTableNameNull() throws Exception {
        hiveLineageService.getSchema(null);
        Assert.fail();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetSchemaTableNameEmpty() throws Exception {
        hiveLineageService.getSchema("");
        Assert.fail();
    }

    @Test(expectedExceptions = EntityNotFoundException.class)
    public void testGetSchemaBadTableName() throws Exception {
        hiveLineageService.getSchema("blah");
        Assert.fail();
    }
}
