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
package org.apache.atlas;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.annotations.Guice;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *  Base Class to set up hive types and instances for tests
 */
@Guice(modules = RepositoryMetadataModule.class)
public class BaseHiveRepositoryTest {

    @Inject
    protected MetadataService metadataService;

    @Inject
    protected MetadataRepository repository;

    @Inject
    protected GraphProvider<TitanGraph> graphProvider;

    protected void setUp() throws Exception {
        setUpTypes();
        new GraphBackedSearchIndexer(graphProvider);
        setupInstances();
        TestUtils.dumpGraph(graphProvider.get());
    }

    protected void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            graphProvider.get().shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
    private static final String STORAGE_DESC_TYPE = "StorageDesc";
    private static final String VIEW_TYPE = "View";
    private static final String PARTITION_TYPE = "hive_partition";

    TypesDef createTypeDefinitions() {
        HierarchicalTypeDefinition<ClassType> dbClsDef = TypesUtil
            .createClassTypeDef(DATABASE_TYPE, null, attrDef("name", DataTypes.STRING_TYPE),
                attrDef("description", DataTypes.STRING_TYPE), attrDef("locationUri", DataTypes.STRING_TYPE),
                attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.LONG_TYPE));

        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
            .createClassTypeDef(COLUMN_TYPE, null, attrDef("name", DataTypes.STRING_TYPE),
                attrDef("dataType", DataTypes.STRING_TYPE), attrDef("comment", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> storageDescClsDef = TypesUtil
            .createClassTypeDef(STORAGE_DESC_TYPE, null,
                new AttributeDefinition("cols", String.format("array<%s>", COLUMN_TYPE),
                    Multiplicity.COLLECTION, false, null),
                attrDef("location", DataTypes.STRING_TYPE),
                attrDef("inputFormat", DataTypes.STRING_TYPE), attrDef("outputFormat", DataTypes.STRING_TYPE),
                attrDef("compressed", DataTypes.STRING_TYPE, Multiplicity.REQUIRED, false, null));


        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
            .createClassTypeDef(HIVE_TABLE_TYPE, ImmutableList.of("DataSet"),
                attrDef("owner", DataTypes.STRING_TYPE),
                attrDef("createTime", DataTypes.DATE_TYPE),
                attrDef("lastAccessTime", DataTypes.LONG_TYPE), attrDef("tableType", DataTypes.STRING_TYPE),
                attrDef("temporary", DataTypes.BOOLEAN_TYPE),
                new AttributeDefinition("db", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                // todo - uncomment this, something is broken
                new AttributeDefinition("sd", STORAGE_DESC_TYPE,
                                                Multiplicity.REQUIRED, true, null),
                new AttributeDefinition("columns", DataTypes.arrayTypeName(COLUMN_TYPE),
                    Multiplicity.COLLECTION, true, null));

        HierarchicalTypeDefinition<ClassType> loadProcessClsDef = TypesUtil
            .createClassTypeDef(HIVE_PROCESS_TYPE, ImmutableList.of("Process"),
                attrDef("userName", DataTypes.STRING_TYPE), attrDef("startTime", DataTypes.LONG_TYPE),
                attrDef("endTime", DataTypes.LONG_TYPE),
                attrDef("queryText", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                attrDef("queryPlan", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                attrDef("queryId", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                attrDef("queryGraph", DataTypes.STRING_TYPE, Multiplicity.REQUIRED));

        HierarchicalTypeDefinition<ClassType> viewClsDef = TypesUtil
            .createClassTypeDef(VIEW_TYPE, null, attrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("db", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("inputTables", DataTypes.arrayTypeName(HIVE_TABLE_TYPE),
                    Multiplicity.COLLECTION, false, null));

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
            new AttributeDefinition("values", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                Multiplicity.OPTIONAL, false, null),
            new AttributeDefinition("table", HIVE_TABLE_TYPE, Multiplicity.REQUIRED, false, null),
            };
        HierarchicalTypeDefinition<ClassType> partClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, PARTITION_TYPE, null,
                attributeDefinitions);

        HierarchicalTypeDefinition<TraitType> dimTraitDef = TypesUtil.createTraitTypeDef("Dimension", null);

        HierarchicalTypeDefinition<TraitType> factTraitDef = TypesUtil.createTraitTypeDef("Fact", null);

        HierarchicalTypeDefinition<TraitType> metricTraitDef = TypesUtil.createTraitTypeDef("Metric", null);

        HierarchicalTypeDefinition<TraitType> etlTraitDef = TypesUtil.createTraitTypeDef("ETL", null);

        HierarchicalTypeDefinition<TraitType> piiTraitDef = TypesUtil.createTraitTypeDef("PII", null);

        HierarchicalTypeDefinition<TraitType> jdbcTraitDef = TypesUtil.createTraitTypeDef("JdbcAccess", null);

        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
            ImmutableList.of(dimTraitDef, factTraitDef, piiTraitDef, metricTraitDef, etlTraitDef, jdbcTraitDef),
            ImmutableList.of(dbClsDef, storageDescClsDef, columnClsDef, tblClsDef, loadProcessClsDef, viewClsDef, partClsDef));
    }

    AttributeDefinition attrDef(String name, IDataType dT) {
        return attrDef(name, dT, Multiplicity.OPTIONAL, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m) {
        return attrDef(name, dT, m, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m, boolean isComposite,
        String reverseAttributeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dT);
        return new AttributeDefinition(name, dT.getName(), m, isComposite, reverseAttributeName);
    }

    private void setupInstances() throws Exception {
        Id salesDB = database("Sales", "Sales Database", "John ETL", "hdfs://host:8000/apps/warehouse/sales");

        Referenceable sd =
            storageDescriptor("hdfs://host:8000/apps/warehouse/sales", "TextInputFormat", "TextOutputFormat", true, ImmutableList.of(
                column("time_id", "int", "time id")));

        List<Referenceable> salesFactColumns = ImmutableList
            .of(column("time_id", "int", "time id"),
                column("product_id", "int", "product id"),
                column("customer_id", "int", "customer id", "PII"),
                column("sales", "double", "product id", "Metric"));

        Id salesFact = table("sales_fact", "sales fact table", salesDB, sd, "Joe", "Managed", salesFactColumns, "Fact");

        List<Referenceable> timeDimColumns = ImmutableList
            .of(column("time_id", "int", "time id"),
                column("dayOfYear", "int", "day Of Year"),
                column("weekDay", "int", "week Day"));

        Id timeDim = table("time_dim", "time dimension table", salesDB, sd, "John Doe", "External", timeDimColumns,
            "Dimension");

        Id reportingDB =
            database("Reporting", "reporting database", "Jane BI", "hdfs://host:8000/apps/warehouse/reporting");

        Id salesFactDaily =
            table("sales_fact_daily_mv", "sales fact daily materialized view", reportingDB, sd, "Joe BI", "Managed",
                salesFactColumns, "Metric");

        loadProcess("loadSalesDaily", "hive query for daily summary", "John ETL", ImmutableList.of(salesFact, timeDim),
            ImmutableList.of(salesFactDaily), "create table as select ", "plan", "id", "graph", "ETL");

        List<Referenceable> productDimColumns = ImmutableList
            .of(column("product_id", "int", "product id"),
                column("product_name", "string", "product name"),
                column("brand_name", "int", "brand name"));

        Id productDim =
            table("product_dim", "product dimension table", salesDB, sd, "John Doe", "Managed", productDimColumns,
                "Dimension");

        view("product_dim_view", reportingDB, ImmutableList.of(productDim), "Dimension", "JdbcAccess");

        List<Referenceable> customerDimColumns = ImmutableList.of(
            column("customer_id", "int", "customer id", "PII"),
            column("name", "string", "customer name", "PII"),
            column("address", "string", "customer address", "PII"));

        Id customerDim =
            table("customer_dim", "customer dimension table", salesDB, sd, "fetl", "External", customerDimColumns,
                "Dimension");

        view("customer_dim_view", reportingDB, ImmutableList.of(customerDim), "Dimension", "JdbcAccess");

        Id salesFactMonthly =
            table("sales_fact_monthly_mv", "sales fact monthly materialized view", reportingDB, sd, "Jane BI",
                "Managed", salesFactColumns, "Metric");

        loadProcess("loadSalesMonthly", "hive query for monthly summary", "John ETL", ImmutableList.of(salesFactDaily),
            ImmutableList.of(salesFactMonthly), "create table as select ", "plan", "id", "graph", "ETL");

        partition(new ArrayList() {{ add("2015-01-01"); }}, salesFactDaily);
    }

    Id database(String name, String description, String owner, String locationUri, String... traitNames)
        throws Exception {
        Referenceable referenceable = new Referenceable(DATABASE_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("owner", owner);
        referenceable.set("locationUri", locationUri);
        referenceable.set("createTime", System.currentTimeMillis());

        ClassType clsType = TypeSystem.getInstance().getDataType(ClassType.class, DATABASE_TYPE);
        return createInstance(referenceable, clsType);
    }

    Referenceable storageDescriptor(String location, String inputFormat, String outputFormat, boolean compressed, List<Referenceable> columns)
        throws Exception {
        Referenceable referenceable = new Referenceable(STORAGE_DESC_TYPE);
        referenceable.set("location", location);
        referenceable.set("inputFormat", inputFormat);
        referenceable.set("outputFormat", outputFormat);
        referenceable.set("compressed", compressed);
        referenceable.set("cols", columns);

        return referenceable;
    }

    Referenceable column(String name, String dataType, String comment, String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(COLUMN_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("dataType", dataType);
        referenceable.set("comment", comment);

        return referenceable;
    }

    Id table(String name, String description, Id dbId, Referenceable sd, String owner, String tableType,
        List<Referenceable> columns, String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(HIVE_TABLE_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("owner", owner);
        referenceable.set("tableType", tableType);
        referenceable.set("temporary", false);
        referenceable.set("createTime", new Date(System.currentTimeMillis()));
        referenceable.set("lastAccessTime", System.currentTimeMillis());
        referenceable.set("retention", System.currentTimeMillis());

        referenceable.set("db", dbId);
        // todo - uncomment this, something is broken
        referenceable.set("sd", sd);
        referenceable.set("columns", columns);

        ClassType clsType = TypeSystem.getInstance().getDataType(ClassType.class, HIVE_TABLE_TYPE);
        return createInstance(referenceable, clsType);
    }

    Id loadProcess(String name, String description, String user, List<Id> inputTables, List<Id> outputTables,
        String queryText, String queryPlan, String queryId, String queryGraph, String... traitNames)
        throws Exception {
        Referenceable referenceable = new Referenceable(HIVE_PROCESS_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("description", description);
        referenceable.set("user", user);
        referenceable.set("startTime", System.currentTimeMillis());
        referenceable.set("endTime", System.currentTimeMillis() + 10000);

        referenceable.set("inputs", inputTables);
        referenceable.set("outputs", outputTables);

        referenceable.set("queryText", queryText);
        referenceable.set("queryPlan", queryPlan);
        referenceable.set("queryId", queryId);
        referenceable.set("queryGraph", queryGraph);

        ClassType clsType = TypeSystem.getInstance().getDataType(ClassType.class, HIVE_PROCESS_TYPE);
        return createInstance(referenceable, clsType);
    }

    Id view(String name, Id dbId, List<Id> inputTables, String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(VIEW_TYPE, traitNames);
        referenceable.set("name", name);
        referenceable.set("db", dbId);

        referenceable.set("inputTables", inputTables);
        ClassType clsType = TypeSystem.getInstance().getDataType(ClassType.class, VIEW_TYPE);
        return createInstance(referenceable, clsType);
    }

    Id partition(List<String> values, Id table, String... traitNames) throws Exception {
        Referenceable referenceable = new Referenceable(PARTITION_TYPE, traitNames);
        referenceable.set("values", values);
        referenceable.set("table", table);
        ClassType clsType = TypeSystem.getInstance().getDataType(ClassType.class, PARTITION_TYPE);
        return createInstance(referenceable, clsType);
    }
    private Id createInstance(Referenceable referenceable, ClassType clsType) throws Exception {
//        String entityJSON = InstanceSerialization.toJson(referenceable, true);


        ITypedReferenceableInstance typedInstance = clsType.convert(referenceable, Multiplicity.REQUIRED);
        String guid = repository.createEntities(typedInstance)[0];

        // return the reference to created instance with guid
        return new Id(guid, 0, referenceable.getTypeName());
    }
}
