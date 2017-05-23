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

package org.apache.atlas.web.integration;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.AtlasBaseClient.APIInfo;

/**
 * Entity Lineage v2 Integration Tests.
 */
public class EntityLineageJerseyResourceIT extends DataSetLineageJerseyResourceIT {
    private static final String BASE_URI = "api/atlas/v2/lineage";
    private static final APIInfo LINEAGE_V2_API = new APIInfo(BASE_URI, "GET", Response.Status.OK);
    private static final String INPUT_DIRECTION = "INPUT";
    private static final String OUTPUT_DIRECTION = "OUTPUT";
    private static final String BOTH_DIRECTION = "BOTH";
    private static final String DIRECTION_PARAM = "direction";
    private static final String DEPTH_PARAM = "depth";

    private String salesFactTable;
    private String salesMonthlyTable;
    private String salesDBName;
    Gson gson = new Gson();

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypeDefinitionsV1();
        setupInstances();
    }

    @Test
    public void testInputLineageInfo() throws Exception {
        String tableId = atlasClientV1.getEntity(HIVE_TABLE_TYPE,
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, salesMonthlyTable).getId()._getId();

        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(DIRECTION_PARAM, INPUT_DIRECTION);
        queryParams.add(DEPTH_PARAM, "5");
        JSONObject response = atlasClientV1.callAPI(LINEAGE_V2_API, JSONObject.class, queryParams, tableId);
        Assert.assertNotNull(response);
        System.out.println("input lineage info = " + response
        );

        AtlasLineageInfo inputLineageInfo = gson.fromJson(response.toString(), AtlasLineageInfo.class);

        Map<String, AtlasEntityHeader> entities = inputLineageInfo.getGuidEntityMap();
        Assert.assertNotNull(entities);

        Set<AtlasLineageInfo.LineageRelation> relations = inputLineageInfo.getRelations();
        Assert.assertNotNull(relations);

        Assert.assertEquals(entities.size(), 6);
        Assert.assertEquals(relations.size(), 5);
        Assert.assertEquals(inputLineageInfo.getLineageDirection(), AtlasLineageInfo.LineageDirection.INPUT);
        Assert.assertEquals(inputLineageInfo.getLineageDepth(), 5);
        Assert.assertEquals(inputLineageInfo.getBaseEntityGuid(), tableId);
    }

    @Test
    public void testOutputLineageInfo() throws Exception {
        String tableId = atlasClientV1.getEntity(HIVE_TABLE_TYPE,
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, salesFactTable).getId()._getId();

        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(DIRECTION_PARAM, OUTPUT_DIRECTION);
        queryParams.add(DEPTH_PARAM, "5");
        JSONObject response = atlasClientV1.callAPI(LINEAGE_V2_API, JSONObject.class, queryParams, tableId);

        Assert.assertNotNull(response);
        System.out.println("output lineage info = " + response);

        AtlasLineageInfo outputLineageInfo = gson.fromJson(response.toString(), AtlasLineageInfo.class);

        Map<String, AtlasEntityHeader> entities = outputLineageInfo.getGuidEntityMap();
        Assert.assertNotNull(entities);

        Set<AtlasLineageInfo.LineageRelation> relations = outputLineageInfo.getRelations();
        Assert.assertNotNull(relations);

        Assert.assertEquals(entities.size(), 5);
        Assert.assertEquals(relations.size(), 4);
        Assert.assertEquals(outputLineageInfo.getLineageDirection(), AtlasLineageInfo.LineageDirection.OUTPUT);
        Assert.assertEquals(outputLineageInfo.getLineageDepth(), 5);
        Assert.assertEquals(outputLineageInfo.getBaseEntityGuid(), tableId);
    }

    @Test
    public void testLineageInfo() throws Exception {
        String tableId = atlasClientV1.getEntity(HIVE_TABLE_TYPE,
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, salesMonthlyTable).getId()._getId();

        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(DIRECTION_PARAM, BOTH_DIRECTION);
        queryParams.add(DEPTH_PARAM, "5");
        JSONObject response = atlasClientV1.callAPI(LINEAGE_V2_API, JSONObject.class, queryParams, tableId);

        Assert.assertNotNull(response);
        System.out.println("both lineage info = " + response);

        AtlasLineageInfo bothLineageInfo = gson.fromJson(response.toString(), AtlasLineageInfo.class);

        Map<String, AtlasEntityHeader> entities = bothLineageInfo.getGuidEntityMap();
        Assert.assertNotNull(entities);

        Set<AtlasLineageInfo.LineageRelation> relations = bothLineageInfo.getRelations();
        Assert.assertNotNull(relations);

        Assert.assertEquals(entities.size(), 6);
        Assert.assertEquals(relations.size(), 5);
        Assert.assertEquals(bothLineageInfo.getLineageDirection(), AtlasLineageInfo.LineageDirection.BOTH);
        Assert.assertEquals(bothLineageInfo.getLineageDepth(), 5);
        Assert.assertEquals(bothLineageInfo.getBaseEntityGuid(), tableId);
    }

    private void setupInstances() throws Exception {
        salesDBName = "Sales" + randomString();
        Id salesDB = database(salesDBName, "Sales Database", "John ETL", "hdfs://host:8000/apps/warehouse/sales");

        List<Referenceable> salesFactColumns = ImmutableList
                .of(column("time_id", "int", "time id"), column("product_id", "int", "product id"),
                        column("customer_id", "int", "customer id"),
                        column("sales", "double", "product id"));

        salesFactTable = "sales_fact" + randomString();
        Id salesFact = table(salesFactTable, "sales fact table", salesDB, "Joe", "MANAGED", salesFactColumns);

        List<Referenceable> timeDimColumns = ImmutableList
                .of(column("time_id", "int", "time id"), column("dayOfYear", "int", "day Of Year"),
                        column("weekDay", "int", "week Day"));

        Id timeDim =
                table("time_dim" + randomString(), "time dimension table", salesDB, "John Doe", "EXTERNAL",
                        timeDimColumns);

        Id reportingDB =
                database("Reporting" + randomString(), "reporting database", "Jane BI",
                        "hdfs://host:8000/apps/warehouse/reporting");

        Id salesFactDaily =
                table("sales_fact_daily_mv" + randomString(), "sales fact daily materialized view", reportingDB,
                        "Joe BI", "MANAGED", salesFactColumns);

        loadProcess("loadSalesDaily" + randomString(), "John ETL", ImmutableList.of(salesFact, timeDim),
                ImmutableList.of(salesFactDaily), "create table as select ", "plan", "id", "graph");

        salesMonthlyTable = "sales_fact_monthly_mv" + randomString();
        Id salesFactMonthly =
                table(salesMonthlyTable, "sales fact monthly materialized view", reportingDB, "Jane BI",
                        "MANAGED", salesFactColumns);

        loadProcess("loadSalesMonthly" + randomString(), "John ETL", ImmutableList.of(salesFactDaily),
                ImmutableList.of(salesFactMonthly), "create table as select ", "plan", "id", "graph");
    }
}
