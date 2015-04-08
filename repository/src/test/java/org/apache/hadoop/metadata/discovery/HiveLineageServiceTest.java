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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.discovery.graph.GraphBackedDiscoveryService;
import org.apache.hadoop.metadata.repository.graph.GraphHelper;
import org.apache.hadoop.metadata.repository.graph.GraphProvider;
import org.apache.hadoop.metadata.services.DefaultMetadataService;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization;
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
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.apache.hadoop.metadata.typesystem.types.TypeUtils;
import org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for Hive LineageService.
 */
@Guice(modules = RepositoryMetadataModule.class)
public class HiveLineageServiceTest {

    @Inject
    private DefaultMetadataService metadataService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    @Inject
    private HiveLineageService hiveLineageService;

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @BeforeClass
    public void setUp() throws Exception {
        TypeSystem.getInstance().reset();

        setUpTypes();
        setupInstances();

        // dumpGraph();
    }

    private void dumpGraph() throws Exception {
        TitanGraph titanGraph = graphProvider.get();
        File tempFile = File.createTempFile("graph", ".gson");
        System.out.println("tempFile.getPath() = " + tempFile.getPath());
        GraphSONWriter.outputGraph(titanGraph, tempFile.getPath());

        System.out.println("Vertices:");
        for (Vertex vertex : titanGraph.getVertices()) {
            System.out.println(GraphHelper.vertexString(vertex));
        }

        System.out.println("Edges:");
        for (Edge edge : titanGraph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }
    }

    @DataProvider(name = "dslQueriesProvider")
    private Object[][] createDSLQueries() {
        return new String[][] {
            // joins
            {"hive_table where name=\"sales_fact\", columns"},
            {"hive_table where name=\"sales_fact\", columns select name, dataType, comment"},
            {"hive_table where name=\"sales_fact\", columns as c select c.name, c.dataType, c.comment"},
//            {"hive_db as db where (db.name=\"Reporting\"), hive_table as table select db.name, table.name"},
            {"from hive_db"},
            {"hive_db"},
            {"hive_db where hive_db.name=\"Reporting\""},
            {"hive_db hive_db.name = \"Reporting\""},
            {"hive_db where hive_db.name=\"Reporting\" select name, owner"},
            {"hive_db has name"},
//            {"hive_db, hive_table"},
//            {"hive_db, hive_process has name"},
//            {"hive_db as db1, hive_table where db1.name = \"Reporting\""},
//            {"hive_db where hive_db.name=\"Reporting\" and hive_db.createTime < " + System.currentTimeMillis()},
            {"from hive_table"},
            {"hive_table"},
            {"hive_table is Dimension"},
            {"hive_column where hive_column isa PII"},
//            {"hive_column where hive_column isa PII select hive_column.name"},
            {"hive_column select hive_column.name"},
            {"hive_column select name"},
            {"hive_column where hive_column.name=\"customer_id\""},
            {"from hive_table select hive_table.name"},
            {"hive_db where (name = \"Reporting\")"},
            {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1"},
            {"hive_db where hive_db has name"},
//            {"hive_db hive_table"},
            {"hive_db where hive_db has name"},
//            {"hive_db as db1 hive_table where (db1.name = \"Reporting\")"},
            {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 "},
//            {"hive_db where (name = \"Reporting\") and ((createTime + 1) > 0)"},
//            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") select db1.name as dbName, tab.name as tabName"},
//            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) or (db1.name = \"Reporting\") select db1.name as dbName, tab.name as tabName"},
//            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner select db1.name as dbName, tab.name as tabName"},
//            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner select db1.name as dbName, tab.name as tabName"},
            // trait searches
            {"Dimension"},
            {"Fact"},
            {"ETL"},
            {"Metric"},
            {"PII"},
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

    private void setUpTypes() throws Exception {
        TypesDef typesDef = createTypeDefinitions();
        String typesAsJSON = TypesSerialization.toJson(typesDef);
        metadataService.createType(typesAsJSON);
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

        ArrayList<Referenceable> salesFactColumns = new ArrayList<>();
        salesFactColumns.add(column("time_id", "int", "time id"));
        salesFactColumns.add(column("product_id", "int", "product id"));
        salesFactColumns.add(column("customer_id", "int", "customer id", "PII"));
        salesFactColumns.add(column("sales", "double", "product id", "Metric"));

        Id salesFact = table("sales_fact", "sales fact table",
                salesDB, "Joe", "Managed", salesFactColumns, "Fact");

        ArrayList<Referenceable> timeDimColumns = new ArrayList<>();
        timeDimColumns.add(column("time_id", "int", "time id"));
        timeDimColumns.add(column("dayOfYear", "int", "day Of Year"));
        timeDimColumns.add(column("weekDay", "int", "week Day"));

        Id timeDim = table("time_dim", "time dimension table",
                salesDB, "John Doe", "External", timeDimColumns, "Dimension");

        Id reportingDB = database("Reporting", "reporting database", "Jane BI",
                "hdfs://host:8000/apps/warehouse/reporting");

        Id salesFactDaily = table("sales_fact_daily_mv",
                "sales fact daily materialized view",
                reportingDB, "Joe BI", "Managed", salesFactColumns, "Metric");

        Id loadSalesFactDaily = loadProcess("loadSalesDaily", "John ETL",
                ImmutableList.of(salesFact, timeDim), ImmutableList.of(salesFactDaily),
                "create table as select ", "plan", "id", "graph",
                "ETL");
        System.out.println("added loadSalesFactDaily = " + loadSalesFactDaily);

        Id salesFactMonthly = table("sales_fact_monthly_mv",
                "sales fact monthly materialized view",
                reportingDB, "Jane BI", "Managed", salesFactColumns, "Metric");

        Id loadSalesFactMonthly = loadProcess("loadSalesMonthly", "John ETL",
                ImmutableList.of(salesFactDaily), ImmutableList.of(salesFactMonthly),
                "create table as select ", "plan", "id", "graph",
                "ETL");
        System.out.println("added loadSalesFactMonthly = " + loadSalesFactMonthly);
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

    private Id createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        System.out.println("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        System.out.println("Submitting new entity= " + entityJSON);
        String guid = metadataService.createEntity(entityJSON);
        System.out.println("created instance for type " + typeName + ", guid: " + guid);

        // return the reference to created instance with guid
        return new Id(guid, 0, referenceable.getTypeName());
    }
}
