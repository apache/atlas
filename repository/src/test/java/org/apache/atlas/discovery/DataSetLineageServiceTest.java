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

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.BaseRepositoryTest;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtils;
import org.apache.atlas.model.legacy.EntityResult;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.collections.ArrayStack;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for Hive LineageService.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class DataSetLineageServiceTest extends BaseRepositoryTest {

    @Inject
    private DiscoveryService discoveryService;

    @Inject
    private DataSetLineageService lineageService;

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

    @Test(enabled = false)
    public void testSearchByDSLQueries(String dslQuery) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery, new QueryParams(100, 0));
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        Assert.assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        assertNotNull(rows);
        Assert.assertTrue(rows.length() >= 0); // some queries may not have any results
        System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows");
    }

    @Test(enabled = false)
    public void testGetInputsGraphInvalidArguments(final String tableName, String expectedException) throws Exception {
        testInvalidArguments(expectedException, new Invoker() {
            @Override
            void run() throws AtlasException {
                lineageService.getInputsGraph(tableName);
            }
        });
    }

    @Test(enabled = false)
    public void testGetInputsGraphForEntityInvalidArguments(final String tableName, String expectedException)
            throws Exception {
        testInvalidArguments(expectedException, new Invoker() {
            @Override
            void run() throws AtlasException {
                lineageService.getInputsGraph(tableName);
            }
        });
    }

    @Test(enabled = false)
    public void testGetInputsGraph() throws Exception {
        JSONObject results = getInputsGraph("sales_fact_monthly_mv");
        assertNotNull(results);
        System.out.println("inputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 4);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test(enabled = false)
    public void testCircularLineage() throws Exception{
        JSONObject results = getInputsGraph("table2");
        assertNotNull(results);
        System.out.println("inputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 2);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test(enabled = false)
    public void testGetInputsGraphForEntity() throws Exception {
        ITypedReferenceableInstance entity =
                repository.getEntityDefinition(HIVE_TABLE_TYPE, "name", "sales_fact_monthly_mv");

        JSONObject results = new JSONObject(lineageService.getInputsGraphForEntity(entity.getId()._getId()));
        assertNotNull(results);
        System.out.println("inputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 4);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test(enabled = false)
    public void testGetOutputsGraphInvalidArguments(final String tableName, String expectedException) throws Exception {
        testInvalidArguments(expectedException, new Invoker() {
            @Override
            void run() throws AtlasException {
                lineageService.getOutputsGraph(tableName);
            }
        });
    }

    @Test(enabled = false)
    public void testGetOutputsGraphForEntityInvalidArguments(final String tableId, String expectedException)
            throws Exception {
        testInvalidArguments(expectedException, new Invoker() {
            @Override
            void run() throws AtlasException {
                lineageService.getOutputsGraphForEntity(tableId);
            }
        });
    }

    @Test(enabled = false)
    public void testGetOutputsGraph() throws Exception {
        JSONObject results = getOutputsGraph("sales_fact");
        assertNotNull(results);
        System.out.println("outputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 3);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test(enabled = false)
    public void testGetOutputsGraphForEntity() throws Exception {
        ITypedReferenceableInstance entity =
                repository.getEntityDefinition(HIVE_TABLE_TYPE, "name", "sales_fact");

        JSONObject results = new JSONObject(lineageService.getOutputsGraphForEntity(entity.getId()._getId()));
        assertNotNull(results);
        System.out.println("outputs graph = " + results);

        JSONObject values = results.getJSONObject("values");
        assertNotNull(values);

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

    @Test(enabled = false)
    public void testGetSchema(String tableName, String expected) throws Exception {
        JSONObject results = getSchema(tableName);
        assertNotNull(results);
        System.out.println("columns = " + results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertEquals(rows.length(), Integer.parseInt(expected));

        for (int index = 0; index < rows.length(); index++) {
            assertColumn(rows.getJSONObject(index));
        }
    }

    @Test(enabled = false)
    public void testGetSchemaForEntity(String tableName, String expected) throws Exception {
        ITypedReferenceableInstance entity =
                repository.getEntityDefinition(HIVE_TABLE_TYPE, "name", tableName);

        JSONObject results = new JSONObject(lineageService.getSchemaForEntity(entity.getId()._getId()));
        assertNotNull(results);
        System.out.println("columns = " + results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertEquals(rows.length(), Integer.parseInt(expected));

        for (int index = 0; index < rows.length(); index++) {
            assertColumn(rows.getJSONObject(index));
        }
    }

    private void assertColumn(JSONObject jsonObject) throws JSONException {
        assertNotNull(jsonObject.getString("name"));
        assertNotNull(jsonObject.getString("comment"));
        assertNotNull(jsonObject.getString("dataType"));
        Assert.assertEquals(jsonObject.getString("$typeName$"), "hive_column");
    }

    @Test(enabled = false)
    public void testGetSchemaForDBEntity() throws Exception {
        String dbId = getEntityId(DATASET_SUBTYPE, "name", "dataSetSubTypeInst1");
        JSONObject results = new JSONObject(lineageService.getSchemaForEntity(dbId));
    }

    @DataProvider(name = "invalidArgumentsProvider")
    private Object[][] arguments() {
        return new String[][]{{null, IllegalArgumentException.class.getName()},
                {"", IllegalArgumentException.class.getName()},
                {"blah", EntityNotFoundException.class.getName()}};
    }

    abstract class Invoker {
        abstract void run() throws AtlasException;
    }

    public void testInvalidArguments(String expectedException, Invoker invoker) throws Exception {
        try {
            invoker.run();
            fail("Expected " + expectedException);
        } catch(Exception e) {
            assertEquals(e.getClass().getName(), expectedException);
        }
    }

    @Test(enabled = false)
    public void testGetSchemaInvalidArguments(final String tableName, String expectedException) throws Exception {
        testInvalidArguments(expectedException, new Invoker() {
            @Override
            void run() throws AtlasException {
                lineageService.getSchema(tableName);
            }
        });
    }

    @Test(enabled = false)
    public void testGetSchemaForEntityInvalidArguments(final String entityId, String expectedException) throws Exception {
        testInvalidArguments(expectedException, new Invoker() {
            @Override
            void run() throws AtlasException {
                lineageService.getSchemaForEntity(entityId);
            }
        });
    }

    private JSONObject getSchema(String tableName) throws Exception {
        return new JSONObject(lineageService.getSchema("qualified:" + tableName));
    }

    private JSONObject getInputsGraph(String tableName) throws Exception {
        return new JSONObject(lineageService.getInputsGraph("qualified:" + tableName));
    }

    private JSONObject getOutputsGraph(String tableName) throws Exception {
        return new JSONObject(lineageService.getOutputsGraph("qualified:" + tableName));
    }

    @Test(enabled = false)
    public void testLineageWithDelete() throws Exception {
        String tableName = "table" + random();
        createTable(tableName, 3, true);
        String tableId = getEntityId(HIVE_TABLE_TYPE, "name", tableName);

        JSONObject results = getSchema(tableName);
        assertEquals(results.getJSONArray("rows").length(), 3);

        results = getInputsGraph(tableName);
        Struct resultInstance = InstanceSerialization.fromJsonStruct(results.toString(), true);
        Map<String, Struct> vertices = (Map) resultInstance.get("vertices");
        assertEquals(vertices.size(), 2);
        Struct vertex = vertices.get(tableId);
        assertEquals(((Struct) vertex.get("vertexId")).get("state"), Id.EntityState.ACTIVE.name());

        results = getOutputsGraph(tableName);
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 2);

        results = new JSONObject(lineageService.getSchemaForEntity(tableId));
        assertEquals(results.getJSONArray("rows").length(), 3);

        results = new JSONObject(lineageService.getInputsGraphForEntity(tableId));
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 2);

        results = new JSONObject(lineageService.getOutputsGraphForEntity(tableId));
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 2);

        //Delete the entity. Lineage for entity returns the same results as before.
        //Lineage for table name throws EntityNotFoundException
        EntityResult deleteResult = repository.deleteEntities(Arrays.asList(tableId));
        assertTrue(deleteResult.getDeletedEntities().contains(tableId));

        results = new JSONObject(lineageService.getSchemaForEntity(tableId));
        assertEquals(results.getJSONArray("rows").length(), 3);

        results = new JSONObject(lineageService.getInputsGraphForEntity(tableId));
        resultInstance = InstanceSerialization.fromJsonStruct(results.toString(), true);
        vertices = (Map) resultInstance.get("vertices");
        assertEquals(vertices.size(), 2);
        vertex = vertices.get(tableId);
        assertEquals(((Struct) vertex.get("vertexId")).get("state"), Id.EntityState.DELETED.name());

        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 2);

        results = new JSONObject(lineageService.getOutputsGraphForEntity(tableId));
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 2);

        try {
            getSchema(tableName);
            fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        try {
            getInputsGraph(tableName);
            fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        try {
            getOutputsGraph(tableName);
            fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        //Create table again should show new lineage
        createTable(tableName, 2, false);
        results = getSchema(tableName);
        assertEquals(results.getJSONArray("rows").length(), 2);

        results = getOutputsGraph(tableName);
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 0);

        results = getInputsGraph(tableName);
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 0);

        tableId = getEntityId(HIVE_TABLE_TYPE, "name", tableName);

        results = new JSONObject(lineageService.getSchemaForEntity(tableId));
        assertEquals(results.getJSONArray("rows").length(), 2);

        results = new JSONObject(lineageService.getInputsGraphForEntity(tableId));
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 0);

        results = new JSONObject(lineageService.getOutputsGraphForEntity(tableId));
        assertEquals(results.getJSONObject("values").getJSONObject("vertices").length(), 0);
    }

    private void createTable(String tableName, int numCols, boolean createLineage) throws Exception {
        String dbId = getEntityId(DATABASE_TYPE, "name", "Sales");
        Id salesDB = new Id(dbId, 0, DATABASE_TYPE);

        //Create the entity again and schema should return the new schema
        List<Referenceable> columns = new ArrayStack();
        for (int i = 0; i < numCols; i++) {
            columns.add(column("col" + random(), "int", "column descr"));
        }

        Referenceable sd =
                storageDescriptor("hdfs://host:8000/apps/warehouse/sales", "TextInputFormat", "TextOutputFormat", true,
                        ImmutableList.of(column("time_id", "int", "time id")));

        Id table = table(tableName, "test table", salesDB, sd, "fetl", "External", columns);
        if (createLineage) {
            Id inTable = table("table" + random(), "test table", salesDB, sd, "fetl", "External", columns);
            Id outTable = table("table" + random(), "test table", salesDB, sd, "fetl", "External", columns);
            loadProcess("process" + random(), "hive query for monthly summary", "Tim ETL", ImmutableList.of(inTable),
                    ImmutableList.of(table), "create table as select ", "plan", "id", "graph", "ETL");
            loadProcess("process" + random(), "hive query for monthly summary", "Tim ETL", ImmutableList.of(table),
                    ImmutableList.of(outTable), "create table as select ", "plan", "id", "graph", "ETL");
        }
    }

    private String random() {
        return TestUtils.randomString(5);
    }

    private String getEntityId(String typeName, String attributeName, String attributeValue) throws Exception {
        return repository.getEntityDefinition(typeName, attributeName, attributeValue).getId()._getId();
    }
}
