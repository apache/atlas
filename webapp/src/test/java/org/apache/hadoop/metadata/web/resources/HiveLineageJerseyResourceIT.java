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

package org.apache.hadoop.metadata.web.resources;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.persistence.Id;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeUtils;
import org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Hive Lineage Integration Tests.
 */
public class HiveLineageJerseyResourceIT extends BaseResourceIT {

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        setUpTypes();
        setupInstances();
    }

    @Test
    public void testInputs() throws Exception {
        WebResource resource = service
                .path("api/metadata/lineage/hive/inputs")
                .path("sales_fact_monthly_mv");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        System.out.println("inputs = " + responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));

        JSONObject results = response.getJSONObject(MetadataServiceClient.RESULTS);
        Assert.assertNotNull(results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertTrue(rows.length() > 0);

        final JSONObject row = rows.getJSONObject(0);
        JSONArray paths = row.getJSONArray("path");
        Assert.assertTrue(paths.length() > 0);
    }

    @Test
    public void testOutputs() throws Exception {
        WebResource resource = service
                .path("api/metadata/lineage/hive/outputs")
                .path("sales_fact");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        System.out.println("outputs = " + responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));

        JSONObject results = response.getJSONObject(MetadataServiceClient.RESULTS);
        Assert.assertNotNull(results);

        JSONArray rows = results.getJSONArray("rows");
        Assert.assertTrue(rows.length() > 0);

        final JSONObject row = rows.getJSONObject(0);
        JSONArray paths = row.getJSONArray("path");
        Assert.assertTrue(paths.length() > 0);
    }

    @Test
    public void testSchema() throws Exception {
        WebResource resource = service
                .path("api/metadata/lineage/hive/schema")
                .path("sales_fact");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        System.out.println("schema = " + responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(MetadataServiceClient.REQUEST_ID));

        JSONObject results = response.getJSONObject(MetadataServiceClient.RESULTS);
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

    private void setUpTypes() throws Exception {
        TypesDef typesDef = createTypeDefinitions();
        createType(typesDef);
    }

    private static final String DATABASE_TYPE = "hive_db";
    private static final String HIVE_TABLE_TYPE = "hive_table";
    private static final String COLUMN_TYPE = "hive_column";
    private static final String HIVE_PROCESS_TYPE = "hive_process";

    private TypesDef createTypeDefinitions() {
        HierarchicalTypeDefinition<ClassType> dbClsDef
                = TypesUtil.createClassTypeDef(DATABASE_TYPE, null,
                attrDef("name", DataTypes.STRING_TYPE),
                attrDef("description", DataTypes.STRING_TYPE),
                attrDef("locationUri", DataTypes.STRING_TYPE),
                attrDef("owner", DataTypes.STRING_TYPE),
                attrDef("createTime", DataTypes.INT_TYPE)
        );

        HierarchicalTypeDefinition<ClassType> columnClsDef =
                TypesUtil.createClassTypeDef(COLUMN_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("dataType", DataTypes.STRING_TYPE),
                        attrDef("comment", DataTypes.STRING_TYPE)
                );

        HierarchicalTypeDefinition<ClassType> tblClsDef =
                TypesUtil.createClassTypeDef(HIVE_TABLE_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("description", DataTypes.STRING_TYPE),
                        attrDef("owner", DataTypes.STRING_TYPE),
                        attrDef("createTime", DataTypes.INT_TYPE),
                        attrDef("lastAccessTime", DataTypes.INT_TYPE),
                        attrDef("tableType", DataTypes.STRING_TYPE),
                        attrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition("db", DATABASE_TYPE,
                                Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("columns",
                                DataTypes.arrayTypeName(COLUMN_TYPE),
                                Multiplicity.COLLECTION, true, null)
                );

        HierarchicalTypeDefinition<ClassType> loadProcessClsDef =
                TypesUtil.createClassTypeDef(HIVE_PROCESS_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("userName", DataTypes.STRING_TYPE),
                        attrDef("startTime", DataTypes.INT_TYPE),
                        attrDef("endTime", DataTypes.INT_TYPE),
                        new AttributeDefinition("inputTables",
                                DataTypes.arrayTypeName(HIVE_TABLE_TYPE),
                                Multiplicity.COLLECTION, false, null),
                        new AttributeDefinition("outputTables",
                                DataTypes.arrayTypeName(HIVE_TABLE_TYPE),
                                Multiplicity.COLLECTION, false, null),
                        attrDef("queryText", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryPlan", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryId", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryGraph", DataTypes.STRING_TYPE, Multiplicity.REQUIRED)
                );

        HierarchicalTypeDefinition<TraitType> dimTraitDef =
                TypesUtil.createTraitTypeDef("Dimension", null);

        HierarchicalTypeDefinition<TraitType> factTraitDef =
                TypesUtil.createTraitTypeDef("Fact", null);

        HierarchicalTypeDefinition<TraitType> metricTraitDef =
                TypesUtil.createTraitTypeDef("Metric", null);

        HierarchicalTypeDefinition<TraitType> etlTraitDef =
                TypesUtil.createTraitTypeDef("ETL", null);


        HierarchicalTypeDefinition<TraitType> piiTraitDef =
                TypesUtil.createTraitTypeDef("PII", null);

        return TypeUtils.getTypesDef(
                ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(dimTraitDef, factTraitDef, metricTraitDef, etlTraitDef, piiTraitDef),
                ImmutableList.of(dbClsDef, columnClsDef, tblClsDef, loadProcessClsDef)
        );
    }

    AttributeDefinition attrDef(String name, IDataType dT) {
        return attrDef(name, dT, Multiplicity.OPTIONAL, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m) {
        return attrDef(name, dT, m, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT,
                                Multiplicity m, boolean isComposite, String reverseAttributeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dT);
        return new AttributeDefinition(name, dT.getName(), m, isComposite, reverseAttributeName);
    }

    private void setupInstances() throws Exception {
        Id salesDB = database(
                "Sales", "Sales Database", "John ETL", "hdfs://host:8000/apps/warehouse/sales");

        List<Referenceable> salesFactColumns = ImmutableList.of(
                column("time_id", "int", "time id"),
                column("product_id", "int", "product id"),
                column("customer_id", "int", "customer id", "PII"),
                column("sales", "double", "product id", "Metric")
        );

        Id salesFact = table("sales_fact", "sales fact table",
                salesDB, "Joe", "Managed", salesFactColumns, "Fact");

        List<Referenceable> timeDimColumns = ImmutableList.of(
                column("time_id", "int", "time id"),
                column("dayOfYear", "int", "day Of Year"),
                column("weekDay", "int", "week Day")
        );

        Id timeDim = table("time_dim", "time dimension table",
                salesDB, "John Doe", "External", timeDimColumns, "Dimension");

        Id reportingDB = database("Reporting", "reporting database", "Jane BI",
                "hdfs://host:8000/apps/warehouse/reporting");

        Id salesFactDaily = table("sales_fact_daily_mv",
                "sales fact daily materialized view",
                reportingDB, "Joe BI", "Managed", salesFactColumns, "Metric");

        loadProcess("loadSalesDaily", "John ETL",
                ImmutableList.of(salesFact, timeDim), ImmutableList.of(salesFactDaily),
                "create table as select ", "plan", "id", "graph",
                "ETL");

        Id salesFactMonthly = table("sales_fact_monthly_mv",
                "sales fact monthly materialized view",
                reportingDB, "Jane BI", "Managed", salesFactColumns, "Metric");

        loadProcess("loadSalesMonthly", "John ETL",
                ImmutableList.of(salesFactDaily), ImmutableList.of(salesFactMonthly),
                "create table as select ", "plan", "id", "graph",
                "ETL");
    }

    Id database(String name, String description,
                String owner, String locationUri,
                String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(DATABASE_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("owner", owner);
        referenceable.set("locationUri", locationUri);
        referenceable.set("createTime", System.currentTimeMillis());

        return createInstance(referenceable);
    }

    Referenceable column(String name, String dataType, String comment,
                         String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(COLUMN_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("dataType", dataType);
        referenceable.set("comment", comment);

        return referenceable;
    }

    Id table(String name, String description, Id dbId,
             String owner, String tableType,
             List<Referenceable> columns,
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

    Id loadProcess(String name, String user,
                   List<Id> inputTables,
                   List<Id> outputTables,
                   String queryText, String queryPlan,
                   String queryId, String queryGraph,
                   String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(HIVE_PROCESS_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("user", user);
        referenceable.set("startTime", System.currentTimeMillis());
        referenceable.set("endTime", System.currentTimeMillis() + 10000);

        referenceable.set("inputTables", inputTables);
        referenceable.set("outputTables", outputTables);

        referenceable.set("queryText", queryText);
        referenceable.set("queryPlan", queryPlan);
        referenceable.set("queryId", queryId);
        referenceable.set("queryGraph", queryGraph);

        return createInstance(referenceable);
    }
}