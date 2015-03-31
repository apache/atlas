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

package org.apache.hadoop.metadata.examples;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization;
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

import java.util.ArrayList;
import java.util.List;

/**
 * A driver that sets up sample types and data for testing purposes.
 * Please take a look at QueryDSL in docs for the Meta Model.
 * todo - move this to examples module. Fix failing collections.
 */
public class QuickStart {

    public static void main(String[] args) throws Exception {
        String baseUrl = getServerUrl(args);
        QuickStart quickStart = new QuickStart(baseUrl);

        // Shows how to create types in DGI for your meta model
        quickStart.createTypes();

        // Shows how to create entities (instances) for the added types in DGI
        quickStart.createEntities();

        // Shows some search queries using DSL based on types
        quickStart.search();
    }

    static String getServerUrl(String[] args) {
        String baseUrl = "http://localhost:21000";
        if (args.length > 0) {
            baseUrl = args[0];
        }

        return baseUrl;
    }

    private static final String DATABASE_TYPE = "DB";
    private static final String COLUMN_TYPE = "Column";
    private static final String TABLE_TYPE = "Table";
    private static final String VIEW_TYPE = "View";
    private static final String LOAD_PROCESS_TYPE = "LoadProcess";
    private static final String STORAGE_DESC_TYPE = "StorageDesc";

    private static final String[] TYPES = {
        DATABASE_TYPE, TABLE_TYPE, STORAGE_DESC_TYPE, COLUMN_TYPE, LOAD_PROCESS_TYPE, VIEW_TYPE,
        "JdbcAccess", "ETL", "Metric", "PII", "Fact", "Dimension"
    };

    private final MetadataServiceClient metadataServiceClient;

    QuickStart(String baseUrl) {
        metadataServiceClient = new MetadataServiceClient(baseUrl);
    }

    void createTypes() throws Exception {
        TypesDef typesDef = createTypeDefinitions();

        String typesAsJSON = TypesSerialization.toJson(typesDef);
        metadataServiceClient.createType(typesAsJSON);

        // verify types created
        verifyTypesCreated();
    }

    TypesDef createTypeDefinitions() throws Exception {
        HierarchicalTypeDefinition<ClassType> dbClsDef
                = TypesUtil.createClassTypeDef(DATABASE_TYPE, null,
                attrDef("name", DataTypes.STRING_TYPE),
                attrDef("description", DataTypes.STRING_TYPE),
                attrDef("locationUri", DataTypes.STRING_TYPE),
                attrDef("owner", DataTypes.STRING_TYPE),
                attrDef("createTime", DataTypes.INT_TYPE)
        );

        HierarchicalTypeDefinition<ClassType> storageDescClsDef =
                TypesUtil.createClassTypeDef(STORAGE_DESC_TYPE, null,
                        attrDef("location", DataTypes.STRING_TYPE),
                        attrDef("inputFormat", DataTypes.STRING_TYPE),
                        attrDef("outputFormat", DataTypes.STRING_TYPE),
                        attrDef("compressed", DataTypes.STRING_TYPE,
                                Multiplicity.REQUIRED, false, null)
                );

        HierarchicalTypeDefinition<ClassType> columnClsDef =
                TypesUtil.createClassTypeDef(COLUMN_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("dataType", DataTypes.STRING_TYPE),
                        attrDef("comment", DataTypes.STRING_TYPE),
                        new AttributeDefinition("sd", STORAGE_DESC_TYPE,
                                Multiplicity.REQUIRED, false, null)
//                        new AttributeDefinition("table", DataTypes.STRING_TYPE.getName(),
//                                 Multiplicity.REQUIRED, false, null)
                );

        HierarchicalTypeDefinition<ClassType> tblClsDef =
                TypesUtil.createClassTypeDef(TABLE_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("description", DataTypes.STRING_TYPE),
                        new AttributeDefinition("db", DATABASE_TYPE,
                                Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("sd", STORAGE_DESC_TYPE,
                                Multiplicity.REQUIRED, false, null),
                        attrDef("owner", DataTypes.STRING_TYPE),
                        attrDef("createTime", DataTypes.INT_TYPE),
                        attrDef("lastAccessTime", DataTypes.INT_TYPE),
                        attrDef("retention", DataTypes.INT_TYPE),
                        attrDef("viewOriginalText", DataTypes.STRING_TYPE),
                        attrDef("viewExpandedText", DataTypes.STRING_TYPE),
                        attrDef("tableType", DataTypes.STRING_TYPE),
                        attrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        // todo - fix this post serialization support for collections
                        new AttributeDefinition("columns",
                                DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                                Multiplicity.COLLECTION, false, null)
//                        new AttributeDefinition("columns", DataTypes.arrayTypeName(COLUMN_TYPE),
//                                Multiplicity.COLLECTION, true, null)
                );

        HierarchicalTypeDefinition<ClassType> loadProcessClsDef =
                TypesUtil.createClassTypeDef(LOAD_PROCESS_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("userName", DataTypes.STRING_TYPE),
                        attrDef("startTime", DataTypes.INT_TYPE),
                        attrDef("endTime", DataTypes.INT_TYPE),
                        // todo - fix this post serialization support for collections
//                        new AttributeDefinition("inputTables", DataTypes.arrayTypeName(TABLE_TYPE),
//                                Multiplicity.COLLECTION, false, null),
//                        new AttributeDefinition("outputTable", TABLE_TYPE,
//                                Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("inputTables", DataTypes.STRING_TYPE.getName(),
                                Multiplicity.COLLECTION, false, null),
                        new AttributeDefinition("outputTable", DataTypes.STRING_TYPE.getName(),
                                Multiplicity.REQUIRED, false, null),
                        attrDef("queryText", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryPlan", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryId", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryGraph", DataTypes.STRING_TYPE, Multiplicity.REQUIRED)
                );

        HierarchicalTypeDefinition<ClassType> viewClsDef =
                TypesUtil.createClassTypeDef(VIEW_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        new AttributeDefinition("db", DATABASE_TYPE,
                                Multiplicity.REQUIRED, false, null),
                        // todo - fix this post serialization support for collections
//                        new AttributeDefinition("inputTables", TABLE_TYPE, Multiplicity.COLLECTION,
//                                false, null)
                        new AttributeDefinition("inputTables", DataTypes.STRING_TYPE.getName(),
                                Multiplicity.COLLECTION, false, null)
                );

        HierarchicalTypeDefinition<TraitType> dimTraitDef =
                TypesUtil.createTraitTypeDef("Dimension", null);

        HierarchicalTypeDefinition<TraitType> factTraitDef =
                TypesUtil.createTraitTypeDef("Fact", null);

        HierarchicalTypeDefinition<TraitType> piiTraitDef =
                TypesUtil.createTraitTypeDef("PII", null);

        HierarchicalTypeDefinition<TraitType> metricTraitDef =
                TypesUtil.createTraitTypeDef("Metric", null);

        HierarchicalTypeDefinition<TraitType> etlTraitDef =
                TypesUtil.createTraitTypeDef("ETL", null);

        HierarchicalTypeDefinition<TraitType> jdbcTraitDef =
                TypesUtil.createTraitTypeDef("JdbcAccess", null);

        return TypeUtils.getTypesDef(
                ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(dimTraitDef, factTraitDef,
                        piiTraitDef, metricTraitDef, etlTraitDef, jdbcTraitDef),
                ImmutableList.of(dbClsDef, storageDescClsDef, columnClsDef,
                        tblClsDef, loadProcessClsDef, viewClsDef)
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

    void createEntities() throws Exception {
        Referenceable salesDB = database(
                "Sales", "Sales Database", "John ETL", "hdfs://host:8000/apps/warehouse/sales");

        Referenceable sd = storageDescriptor("hdfs://host:8000/apps/warehouse/sales",
                "TextInputFormat", "TextOutputFormat", true);


        ArrayList<Referenceable> salesFactColumns = new ArrayList<>();
        Referenceable column = column("time_id", "int", "time id", sd);
        salesFactColumns.add(column);
        column = column("product_id", "int", "product id", sd);
        salesFactColumns.add(column);
        column = column("customer_id", "int", "customer id", sd, "PII");
        salesFactColumns.add(column);
        column = column("sales", "double", "product id", sd, "Metric");
        salesFactColumns.add(column);

        Referenceable salesFact = table("sales_fact", "sales fact table",
                salesDB, sd, "Joe", "Managed", salesFactColumns, "Fact");

        ArrayList<Referenceable> productDimColumns = new ArrayList<>();
        column = column("product_id", "int", "product id", sd);
        productDimColumns.add(column);
        column = column("product_name", "string", "product name", sd);
        productDimColumns.add(column);
        column = column("brand_name", "int", "brand name", sd);
        productDimColumns.add(column);

        Referenceable productDim = table("product_dim", "product dimension table",
                salesDB, sd, "John Doe", "Managed", productDimColumns, "Dimension");

        ArrayList<Referenceable> timeDimColumns = new ArrayList<>();
        column = column("time_id", "int", "time id", sd);
        timeDimColumns.add(column);
        column = column("dayOfYear", "int", "day Of Year", sd);
        timeDimColumns.add(column);
        column = column("weekDay", "int", "week Day", sd);
        timeDimColumns.add(column);

        Referenceable timeDim = table("time_dim", "time dimension table",
                salesDB, sd, "John Doe", "External", timeDimColumns, "Dimension");


        ArrayList<Referenceable> customerDimColumns = new ArrayList<>();
        column = column("customer_id", "int", "customer id", sd, "PII");
        customerDimColumns.add(column);
        column = column("name", "string", "customer name", sd, "PII");
        customerDimColumns.add(column);
        column = column("address", "string", "customer address", sd, "PII");
        customerDimColumns.add(column);

        Referenceable customerDim = table("customer_dim", "customer dimension table",
                salesDB, sd, "fetl", "External", customerDimColumns, "Dimension");


        Referenceable reportingDB = database("Reporting", "reporting database", "Jane BI",
                "hdfs://host:8000/apps/warehouse/reporting");

        Referenceable salesFactDaily = table("sales_fact_daily_mv",
                "sales fact daily materialized view", reportingDB, sd,
                "Joe BI", "Managed", salesFactColumns, "Metric");

        Referenceable loadSalesFactDaily = loadProcess("loadSalesDaily", "John ETL",
                ImmutableList.of(salesFact, timeDim), salesFactDaily,
                "create table as select ", "plan", "id", "graph",
                "ETL");
        System.out.println("added loadSalesFactDaily = " + loadSalesFactDaily);

        Referenceable productDimView = view("product_dim_view", reportingDB,
                ImmutableList.of(productDim), "Dimension", "JdbcAccess");
        System.out.println("added productDimView = " + productDimView);

        Referenceable customerDimView = view("customer_dim_view", reportingDB,
                ImmutableList.of(customerDim), "Dimension", "JdbcAccess");
        System.out.println("added customerDimView = " + customerDimView);

        Referenceable salesFactMonthly = table("sales_fact_monthly_mv",
                "sales fact monthly materialized view",
                reportingDB, sd, "Jane BI", "Managed", salesFactColumns, "Metric");

        Referenceable loadSalesFactMonthly = loadProcess("loadSalesMonthly", "John ETL",
                ImmutableList.of(salesFactDaily), salesFactMonthly,
                "create table as select ", "plan", "id", "graph",
                "ETL");
        System.out.println("added loadSalesFactMonthly = " + loadSalesFactMonthly);
    }

    private Referenceable createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        System.out.println("Submitting new entity= " + entityJSON);
        JSONObject jsonObject = metadataServiceClient.createEntity(entityJSON);
        String guid = jsonObject.getString(MetadataServiceClient.RESULTS);
        System.out.println("created instance for type " + typeName + ", guid: " + guid);

        // return the reference to created instance with guid
        return new Referenceable(guid, referenceable.getTypeName(), referenceable.getValuesMap());
    }

    Referenceable database(String name, String description,
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

    Referenceable storageDescriptor(String location, String inputFormat,
                                    String outputFormat,
                                    boolean compressed) throws Exception {
        Referenceable referenceable = new Referenceable(STORAGE_DESC_TYPE);
        referenceable.set("location", location);
        referenceable.set("inputFormat", inputFormat);
        referenceable.set("outputFormat", outputFormat);
        referenceable.set("compressed", compressed);

        return createInstance(referenceable);
    }

    Referenceable column(String name, String dataType,
                         String comment, Referenceable sd,
                         String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(COLUMN_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("dataType", dataType);
        referenceable.set("comment", comment);
        referenceable.set("sd", sd);

        return createInstance(referenceable);
    }

    Referenceable table(String name, String description,
                        Referenceable db, Referenceable sd,
                        String owner, String tableType,
                        List<Referenceable> columns,
                        String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(TABLE_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("owner", owner);
        referenceable.set("tableType", tableType);
        referenceable.set("createTime", System.currentTimeMillis());
        referenceable.set("lastAccessTime", System.currentTimeMillis());
        referenceable.set("retention", System.currentTimeMillis());
        referenceable.set("db", db);
        referenceable.set("sd", sd);

        // todo - fix this post serialization support for collections
        // referenceable.set("columns", columns);
        ArrayList<String> columnNames = new ArrayList<>(columns.size());
        for (Referenceable column : columns) {
            columnNames.add(String.valueOf(column.get("name")));
        }
        referenceable.set("columns", columnNames);

        return createInstance(referenceable);
    }

    Referenceable loadProcess(String name, String user,
                              List<Referenceable> inputTables,
                              Referenceable outputTable,
                              String queryText, String queryPlan,
                              String queryId, String queryGraph,
                              String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(LOAD_PROCESS_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("user", user);
        referenceable.set("startTime", System.currentTimeMillis());
        referenceable.set("endTime", System.currentTimeMillis() + 10000);

        // todo - fix this post serialization support for collections
        /*
        referenceable.set("inputTables", inputTables);
        referenceable.set("outputTable", outputTable);
        */
        ArrayList<String> inputTableNames = new ArrayList<>(inputTables.size());
        for (Referenceable inputTable : inputTables) {
            inputTableNames.add(String.valueOf(inputTable.get("name")));
        }
        referenceable.set("inputTables", inputTableNames);
        referenceable.set("outputTable", outputTable.get("name"));

        referenceable.set("queryText", queryText);
        referenceable.set("queryPlan", queryPlan);
        referenceable.set("queryId", queryId);
        referenceable.set("queryGraph", queryGraph);

        return createInstance(referenceable);
    }

    Referenceable view(String name, Referenceable db,
                       List<Referenceable> inputTables,
                       String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(VIEW_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("db", db);

        // todo - fix this post serialization support for collections
        // referenceable.set("inputTables", inputTables);
        ArrayList<String> inputTableNames = new ArrayList<>(inputTables.size());
        for (Referenceable inputTable : inputTables) {
            inputTableNames.add(String.valueOf(inputTable.get("name")));
        }
        referenceable.set("inputTables", inputTableNames);

        return createInstance(referenceable);
    }

    private void verifyTypesCreated() throws Exception {
        List<String> types = metadataServiceClient.listTypes();
        for (String type : TYPES) {
            assert types.contains(type);
        }
    }

    private String[] getDSLQueries() {
        return new String[]{
            "from DB",
            "DB",
            "DB where name=\"Reporting\"",
            "DB where DB.name=\"Reporting\"",
            "DB name = \"Reporting\"",
            "DB DB.name = \"Reporting\"",
            "DB where name=\"Reporting\" select name, owner",
            "DB where DB.name=\"Reporting\" select name, owner",
            "DB has name",
            "DB where DB has name",
            "DB, Table",
            "DB is JdbcAccess",
            /*
            "DB, LoadProcess has name",
            "DB as db1, Table where db1.name = \"Reporting\"",
            "DB where DB.name=\"Reporting\" and DB.createTime < " + System.currentTimeMillis()},
            */
            "from Table",
            "Table",
            "Table is Dimension",
            "Column where Column isa PII",
            "View is Dimension",
            /*"Column where Column isa PII select Column.name",*/
            "Column select Column.name",
            "Column select name",
            "Column where Column.name=\"customer_id\"",
            "from Table select Table.name",
            "DB where (name = \"Reporting\")",
            "DB where (name = \"Reporting\") select name as _col_0, owner as _col_1",
            "DB where DB is JdbcAccess",
            "DB where DB has name",
            "DB Table",
            "DB where DB has name",
            "DB as db1 Table where (db1.name = \"Reporting\")",
            "DB where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 ",
            /*
            todo: does not work
            "DB where (name = \"Reporting\") and ((createTime + 1) > 0)",
            "DB as db1 Table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") select db1.name as dbName, tab.name as tabName",
            "DB as db1 Table as tab where ((db1.createTime + 1) > 0) or (db1.name = \"Reporting\") select db1.name as dbName, tab.name as tabName",
            "DB as db1 Table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner select db1.name as dbName, tab.name as tabName",
            "DB as db1 Table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner select db1.name as dbName, tab.name as tabName",
            */
            // trait searches
            "Dimension",
            /*"Fact", - todo: does not work*/
            "JdbcAccess",
            "ETL",
            "Metric",
            "PII",
            /*
            // Lineage - todo - fix this, its not working
            "Table LoadProcess outputTable",
            "Table loop (LoadProcess outputTable)",
            "Table as _loop0 loop (LoadProcess outputTable) withPath",
            "Table as src loop (LoadProcess outputTable) as dest select src.name as srcTable, dest.name as destTable withPath",
            */
        };
    }

    private void search() throws Exception {
        for (String dslQuery : getDSLQueries()) {
            JSONObject response = metadataServiceClient.searchEntity(dslQuery);
            JSONObject results = response.getJSONObject(MetadataServiceClient.RESULTS);
            if (!results.isNull("rows")) {
                JSONArray rows = results.getJSONArray("rows");
                System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows");
            } else {
                System.out.println("query [" + dslQuery + "] failed, results:" + results.toString());
            }
        }
    }
}
