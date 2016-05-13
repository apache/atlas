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

package org.apache.atlas.web.resources;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Hive Lineage Integration Tests.
 */
public class HiveLineageJerseyResourceIT extends BaseResourceIT {

    private static final String BASE_URI = "api/atlas/lineage/hive/table/";
    private String salesFactTable;
    private String salesMonthlyTable;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypeDefinitions();
        setupInstances();
    }

    @Test
    public void testInputsGraph() throws Exception {
        WebResource resource = service.path(BASE_URI).path(salesMonthlyTable).path("inputs").path("graph");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        System.out.println("inputs graph = " + responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        JSONObject results = response.getJSONObject(AtlasClient.RESULTS);
        Assert.assertNotNull(results);

        JSONObject values = results.getJSONObject("values");
        Assert.assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 4);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test
    public void testOutputsGraph() throws Exception {
        WebResource resource = service.path(BASE_URI).path(salesFactTable).path("outputs").path("graph");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        System.out.println("outputs graph= " + responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        JSONObject results = response.getJSONObject(AtlasClient.RESULTS);
        Assert.assertNotNull(results);

        JSONObject values = results.getJSONObject("values");
        Assert.assertNotNull(values);

        final JSONObject vertices = values.getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 3);

        final JSONObject edges = values.getJSONObject("edges");
        Assert.assertEquals(edges.length(), 4);
    }

    @Test
    public void testSchema() throws Exception {
        WebResource resource = service.path(BASE_URI).path(salesFactTable).path("schema");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        System.out.println("schema = " + responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        JSONObject results = response.getJSONObject(AtlasClient.RESULTS);
        Assert.assertNotNull(results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertEquals(rows.length(), 4);

        for (int index = 0; index < rows.length(); index++) {
            final JSONObject row = rows.getJSONObject(index);
            Assert.assertNotNull(row.getString("name"));
            Assert.assertNotNull(row.getString("comment"));
            Assert.assertNotNull(row.getString("dataType"));
            Assert.assertEquals(row.getString("$typeName$"), "hive_column");
        }
    }

    @Test
    public void testSchemaForEmptyTable() throws Exception {
        WebResource resource = service.path(BASE_URI).path("").path("schema");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testSchemaForInvalidTable() throws Exception {
        WebResource resource = service.path(BASE_URI).path("blah").path("schema");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    private void setupInstances() throws Exception {
        Id salesDB = database("Sales" + randomString(), "Sales Database", "John ETL",
                "hdfs://host:8000/apps/warehouse/sales");

        List<Referenceable> salesFactColumns = ImmutableList
                .of(column("time_id", "int", "time id"), column("product_id", "int", "product id"),
                        column("customer_id", "int", "customer id", "pii"),
                        column("sales", "double", "product id", "Metric"));

        salesFactTable = "sales_fact" + randomString();
        Id salesFact = table(salesFactTable, "sales fact table", salesDB, "Joe", "MANAGED", salesFactColumns, "Fact");

        List<Referenceable> timeDimColumns = ImmutableList
                .of(column("time_id", "int", "time id"), column("dayOfYear", "int", "day Of Year"),
                        column("weekDay", "int", "week Day"));

        Id timeDim =
                table("time_dim" + randomString(), "time dimension table", salesDB, "John Doe", "EXTERNAL",
                        timeDimColumns, "Dimension");

        Id reportingDB =
                database("Reporting" + randomString(), "reporting database", "Jane BI",
                        "hdfs://host:8000/apps/warehouse/reporting");

        Id salesFactDaily =
                table("sales_fact_daily_mv" + randomString(), "sales fact daily materialized view", reportingDB,
                        "Joe BI", "MANAGED", salesFactColumns, "Metric");

        String procName = "loadSalesDaily" + randomString();
        loadProcess(procName, "John ETL", ImmutableList.of(salesFact, timeDim),
                ImmutableList.of(salesFactDaily), "create table as select ", "plan", "id", "graph", "ETL");

        salesMonthlyTable = "sales_fact_monthly_mv" + randomString();
        Id salesFactMonthly =
                table(salesMonthlyTable, "sales fact monthly materialized view", reportingDB, "Jane BI",
                        "MANAGED", salesFactColumns, "Metric");

        loadProcess("loadSalesMonthly" + randomString(), "John ETL", ImmutableList.of(salesFactDaily),
                ImmutableList.of(salesFactMonthly), "create table as select ", "plan", "id", "graph", "ETL");
    }

    Id database(String name, String description, String owner, String locationUri, String... traitNames)
    throws Exception {
        Referenceable referenceable = new Referenceable(DATABASE_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("owner", owner);
        referenceable.set("locationUri", locationUri);
        referenceable.set("createTime", System.currentTimeMillis());

        return createInstance(referenceable);
    }

    Referenceable column(String name, String dataType, String comment, String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(COLUMN_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("dataType", dataType);
        referenceable.set("comment", comment);

        return referenceable;
    }

    Id table(String name, String description, Id dbId, String owner, String tableType, List<Referenceable> columns,
            String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(HIVE_TABLE_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("owner", owner);
        referenceable.set("tableType", tableType);
        referenceable.set("createTime", System.currentTimeMillis());
        referenceable.set("lastAccessTime", System.currentTimeMillis());
        referenceable.set("retention", System.currentTimeMillis());

        referenceable.set("db", dbId);
        referenceable.set("columns", columns);

        return createInstance(referenceable);
    }

    Id loadProcess(String name, String user, List<Id> inputTables, List<Id> outputTables, String queryText,
            String queryPlan, String queryId, String queryGraph, String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(HIVE_PROCESS_TYPE, traitNames);
        referenceable.set(AtlasClient.NAME, name);
        referenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
        referenceable.set("user", user);
        referenceable.set("startTime", System.currentTimeMillis());
        referenceable.set("endTime", System.currentTimeMillis() + 10000);

        referenceable.set("inputs", inputTables);
        referenceable.set("outputs", outputTables);

        referenceable.set("queryText", queryText);
        referenceable.set("queryPlan", queryPlan);
        referenceable.set("queryId", queryId);
        referenceable.set("queryGraph", queryGraph);

        return createInstance(referenceable);
    }
}
