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

package org.apache.atlas.examples;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AttributeSearchResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;

import javax.ws.rs.core.MultivaluedMap;
import java.util.*;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF;

/**
 * A driver that sets up sample types and entities using v2 types and entity model for testing purposes.
 */
public class QuickStartV2 {
    public static final String ATLAS_REST_ADDRESS          = "atlas.rest.address";

    public static final String SALES_DB                    = "Sales";
    public static final String REPORTING_DB                = "Reporting";
    public static final String LOGGING_DB                  = "Logging";

    public static final String SALES_FACT_TABLE            = "sales_fact";
    public static final String PRODUCT_DIM_TABLE           = "product_dim";
    public static final String CUSTOMER_DIM_TABLE          = "customer_dim";
    public static final String TIME_DIM_TABLE              = "time_dim";
    public static final String SALES_FACT_DAILY_MV_TABLE   = "sales_fact_daily_mv";
    public static final String SALES_FACT_MONTHLY_MV_TABLE = "sales_fact_monthly_mv";
    public static final String LOG_FACT_DAILY_MV_TABLE     = "log_fact_daily_mv";
    public static final String LOG_FACT_MONTHLY_MV_TABLE   = "logging_fact_monthly_mv";

    public static final String TIME_ID_COLUMN              = "time_id";
    public static final String PRODUCT_ID_COLUMN           = "product_id";
    public static final String CUSTOMER_ID_COLUMN          = "customer_id";
    public static final String APP_ID_COLUMN               = "app_id";
    public static final String MACHINE_ID_COLUMN           = "machine_id";
    public static final String PRODUCT_NAME_COLUMN         = "product_name";
    public static final String BRAND_NAME_COLUMN           = "brand_name";
    public static final String NAME_COLUMN                 = "name";
    public static final String SALES_COLUMN                = "sales";
    public static final String LOG_COLUMN                  = "log";
    public static final String ADDRESS_COLUMN              = "address";
    public static final String DAY_OF_YEAR_COLUMN          = "dayOfYear";
    public static final String WEEKDAY_COLUMN              = "weekDay";

    public static final String DIMENSION_CLASSIFICATION    = "Dimension";
    public static final String FACT_CLASSIFICATION         = "Fact";
    public static final String PII_CLASSIFICATION          = "PII";
    public static final String METRIC_CLASSIFICATION       = "Metric";
    public static final String ETL_CLASSIFICATION          = "ETL";
    public static final String JDBC_CLASSIFICATION         = "JdbcAccess";
    public static final String LOGDATA_CLASSIFICATION      = "Log Data";

    public static final String LOAD_SALES_DAILY_PROCESS    = "loadSalesDaily";
    public static final String LOAD_SALES_MONTHLY_PROCESS  = "loadSalesMonthly";
    public static final String LOAD_LOGS_MONTHLY_PROCESS   = "loadLogsMonthly";

    public static final String PRODUCT_DIM_VIEW            = "product_dim_view";
    public static final String CUSTOMER_DIM_VIEW           = "customer_dim_view";

    public static final String DATABASE_TYPE               = "DB";
    public static final String COLUMN_TYPE                 = "Column";
    public static final String TABLE_TYPE                  = "Table";
    public static final String VIEW_TYPE                   = "View";
    public static final String LOAD_PROCESS_TYPE           = "LoadProcess";
    public static final String STORAGE_DESC_TYPE           = "StorageDesc";

    public static final String[] TYPES = { DATABASE_TYPE, TABLE_TYPE, STORAGE_DESC_TYPE, COLUMN_TYPE, LOAD_PROCESS_TYPE,
                                           VIEW_TYPE, JDBC_CLASSIFICATION, ETL_CLASSIFICATION, METRIC_CLASSIFICATION,
                                           PII_CLASSIFICATION, FACT_CLASSIFICATION, DIMENSION_CLASSIFICATION, LOGDATA_CLASSIFICATION };

    public static void main(String[] args) throws Exception {
        String[] basicAuthUsernamePassword = null;

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
        }

        runQuickstart(args, basicAuthUsernamePassword);
    }

    @VisibleForTesting
    static void runQuickstart(String[] args, String[] basicAuthUsernamePassword) throws Exception {
        String[] urls = getServerUrl(args);

        QuickStartV2 quickStartV2 = null;
        try {
            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                quickStartV2 = new QuickStartV2(urls, basicAuthUsernamePassword);
            } else {
                quickStartV2 = new QuickStartV2(urls);
            }

            // Shows how to create v2 types in Atlas for your meta model
            quickStartV2.createTypes();

            // Shows how to create v2 entities (instances) for the added types in Atlas
            quickStartV2.createEntities();

            // Shows some search queries using DSL based on types
            quickStartV2.search();

            // Shows some lineage information on entity
            quickStartV2.lineage();
        } finally {
            if (quickStartV2!= null) {
                quickStartV2.closeConnection();
            }
        }
        
    }

    static String[] getServerUrl(String[] args) throws AtlasException {
        if (args.length > 0) {
            return args[0].split(",");
        }

        Configuration configuration = ApplicationProperties.get();
        String[] urls = configuration.getStringArray(ATLAS_REST_ADDRESS);

        if (ArrayUtils.isEmpty(urls)) {
            System.out.println("org.apache.atlas.examples.QuickStartV2 <Atlas REST address <http/https>://<atlas-fqdn>:<atlas-port> like http://localhost:21000>");
            System.exit(-1);
        }

        return urls;
    }

    private final AtlasClientV2 atlasClientV2;

    QuickStartV2(String[] urls, String[] basicAuthUsernamePassword) {
        atlasClientV2 = new AtlasClientV2(urls,basicAuthUsernamePassword);
    }

    QuickStartV2(String[] urls) throws AtlasException {
        atlasClientV2 = new AtlasClientV2(urls);
    }


    void createTypes() throws Exception {
        AtlasTypesDef atlasTypesDef = createTypeDefinitions();

        System.out.println("\nCreating sample types: ");
        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);

        verifyTypesCreated();
    }

    AtlasTypesDef createTypeDefinitions() throws Exception {
        AtlasEntityDef dbType   = AtlasTypeUtil.createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE, "1.0", null,
                                  AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("description", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("locationUri", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("owner", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("createTime", "long"));

        AtlasEntityDef sdType   = AtlasTypeUtil.createClassTypeDef(STORAGE_DESC_TYPE, STORAGE_DESC_TYPE, "1.0", null,
                                  AtlasTypeUtil.createOptionalAttrDefWithConstraint("table", TABLE_TYPE, CONSTRAINT_TYPE_INVERSE_REF,
                                          new HashMap<String, Object>() {{ put(CONSTRAINT_PARAM_ATTRIBUTE, "sd"); }}),
                                  AtlasTypeUtil.createOptionalAttrDef("location", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("inputFormat", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("outputFormat", "string"),
                                  AtlasTypeUtil.createRequiredAttrDef("compressed", "boolean"));

        AtlasEntityDef colType  = AtlasTypeUtil.createClassTypeDef(COLUMN_TYPE, COLUMN_TYPE, "1.0", null,
                                  AtlasTypeUtil.createOptionalAttrDef("name", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("dataType", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("comment", "string"),
                                  AtlasTypeUtil.createOptionalAttrDefWithConstraint("table", TABLE_TYPE, CONSTRAINT_TYPE_INVERSE_REF,
                                          new HashMap<String, Object>() {{ put(CONSTRAINT_PARAM_ATTRIBUTE, "columns"); }}));

        colType.setOptions(new HashMap<String, String>() {{ put("schemaAttributes", "[\"name\", \"description\", \"owner\", \"type\", \"comment\", \"position\"]"); }});

        AtlasEntityDef tblType  = AtlasTypeUtil.createClassTypeDef(TABLE_TYPE, TABLE_TYPE, "1.0", Collections.singleton("DataSet"),
                                  AtlasTypeUtil.createRequiredAttrDef("db", DATABASE_TYPE),
                                  AtlasTypeUtil.createRequiredAttrDefWithConstraint("sd", STORAGE_DESC_TYPE, CONSTRAINT_TYPE_OWNED_REF, null),
                                  AtlasTypeUtil.createOptionalAttrDef("owner", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("createTime", "long"),
                                  AtlasTypeUtil.createOptionalAttrDef("lastAccessTime", "long"),
                                  AtlasTypeUtil.createOptionalAttrDef("retention", "long"),
                                  AtlasTypeUtil.createOptionalAttrDef("viewOriginalText", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("viewExpandedText", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("tableType", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("temporary", "boolean"),
                                  AtlasTypeUtil.createRequiredListAttrDefWithConstraint("columns", AtlasBaseTypeDef.getArrayTypeName(COLUMN_TYPE),
                                          CONSTRAINT_TYPE_OWNED_REF, null));

        tblType.setOptions(new HashMap<String, String>() {{ put("schemaElementsAttribute", "columns"); }});

        AtlasEntityDef procType = AtlasTypeUtil.createClassTypeDef(LOAD_PROCESS_TYPE, LOAD_PROCESS_TYPE, "1.0", Collections.singleton("Process"),
                                  AtlasTypeUtil.createOptionalAttrDef("userName", "string"),
                                  AtlasTypeUtil.createOptionalAttrDef("startTime", "long"),
                                  AtlasTypeUtil.createOptionalAttrDef("endTime", "long"),
                                  AtlasTypeUtil.createRequiredAttrDef("queryText", "string"),
                                  AtlasTypeUtil.createRequiredAttrDef("queryPlan", "string"),
                                  AtlasTypeUtil.createRequiredAttrDef("queryId", "string"),
                                  AtlasTypeUtil.createRequiredAttrDef("queryGraph", "string"));

        AtlasEntityDef viewType = AtlasTypeUtil.createClassTypeDef(VIEW_TYPE, VIEW_TYPE, "1.0", Collections.singleton("DataSet"),
                                  AtlasTypeUtil.createRequiredAttrDef("db", DATABASE_TYPE),
                                  AtlasTypeUtil.createOptionalListAttrDef("inputTables", AtlasBaseTypeDef.getArrayTypeName(TABLE_TYPE)));

        AtlasClassificationDef dimClassifDef    = AtlasTypeUtil.createTraitTypeDef(DIMENSION_CLASSIFICATION,  "Dimension Classification", "1.0", Collections.<String>emptySet());
        AtlasClassificationDef factClassifDef   = AtlasTypeUtil.createTraitTypeDef(FACT_CLASSIFICATION, "Fact Classification", "1.0", Collections.<String>emptySet());
        AtlasClassificationDef piiClassifDef    = AtlasTypeUtil.createTraitTypeDef(PII_CLASSIFICATION, "PII Classification", "1.0", Collections.<String>emptySet());
        AtlasClassificationDef metricClassifDef = AtlasTypeUtil.createTraitTypeDef(METRIC_CLASSIFICATION, "Metric Classification", "1.0", Collections.<String>emptySet());
        AtlasClassificationDef etlClassifDef    = AtlasTypeUtil.createTraitTypeDef(ETL_CLASSIFICATION, "ETL Classification", "1.0", Collections.<String>emptySet());
        AtlasClassificationDef jdbcClassifDef   = AtlasTypeUtil.createTraitTypeDef(JDBC_CLASSIFICATION, "JdbcAccess Classification", "1.0", Collections.<String>emptySet());
        AtlasClassificationDef logClassifDef    = AtlasTypeUtil.createTraitTypeDef(LOGDATA_CLASSIFICATION, "LogData Classification", "1.0", Collections.<String>emptySet());

        return AtlasTypeUtil.getTypesDef(Collections.<AtlasEnumDef>emptyList(),
                                         Collections.<AtlasStructDef>emptyList(),
                                         Arrays.asList(dimClassifDef, factClassifDef, piiClassifDef, metricClassifDef, etlClassifDef, jdbcClassifDef, logClassifDef),
                                         Arrays.asList(dbType, sdType, colType, tblType, procType, viewType));
    }

    void createEntities() throws Exception {
        System.out.println("\nCreating sample entities: ");

        // Database entities
        AtlasEntity salesDB     = createDatabase(SALES_DB, "sales database", "John ETL", "hdfs://host:8000/apps/warehouse/sales");
        AtlasEntity reportingDB = createDatabase(REPORTING_DB, "reporting database", "Jane BI", "hdfs://host:8000/apps/warehouse/reporting");
        AtlasEntity logDB       = createDatabase(LOGGING_DB, "logging database", "Tim ETL", "hdfs://host:8000/apps/warehouse/logging");

        // Storage Descriptor entities
        AtlasEntity storageDesc = createStorageDescriptor("hdfs://host:8000/apps/warehouse/sales", "TextInputFormat", "TextOutputFormat", true);

        // Column entities
        List<AtlasEntity> salesFactColumns   = Arrays.asList(createColumn(TIME_ID_COLUMN, "int", "time id"),
                                                                createColumn(PRODUCT_ID_COLUMN, "int", "product id"),
                                                                createColumn(CUSTOMER_ID_COLUMN, "int", "customer id", PII_CLASSIFICATION),
                                                                createColumn(SALES_COLUMN, "double", "product id", METRIC_CLASSIFICATION));

        List<AtlasEntity> logFactColumns     = Arrays.asList(createColumn(TIME_ID_COLUMN, "int", "time id"),
                                                                createColumn(APP_ID_COLUMN, "int", "app id"),
                                                                createColumn(MACHINE_ID_COLUMN, "int", "machine id"),
                                                                createColumn(LOG_COLUMN, "string", "log data", LOGDATA_CLASSIFICATION));

        List<AtlasEntity> productDimColumns  = Arrays.asList(createColumn(PRODUCT_ID_COLUMN, "int", "product id"),
                                                                createColumn(PRODUCT_NAME_COLUMN, "string", "product name"),
                                                                createColumn(BRAND_NAME_COLUMN, "int", "brand name"));

        List<AtlasEntity> timeDimColumns     = Arrays.asList(createColumn(TIME_ID_COLUMN, "int", "time id"),
                                                                createColumn(DAY_OF_YEAR_COLUMN, "int", "day Of Year"),
                                                                createColumn(WEEKDAY_COLUMN, "int", "week Day"));

        List<AtlasEntity> customerDimColumns = Arrays.asList(createColumn(CUSTOMER_ID_COLUMN, "int", "customer id", PII_CLASSIFICATION),
                                                                createColumn(NAME_COLUMN, "string", "customer name", PII_CLASSIFICATION),
                                                                createColumn(ADDRESS_COLUMN, "string", "customer address", PII_CLASSIFICATION));

        // Table entities
        AtlasEntity salesFact          = createTable(SALES_FACT_TABLE, "sales fact table", salesDB, storageDesc,
                                                     "Joe", "Managed", salesFactColumns, FACT_CLASSIFICATION);
        AtlasEntity productDim         = createTable(PRODUCT_DIM_TABLE, "product dimension table", salesDB, storageDesc,
                                                     "John Doe", "Managed", productDimColumns, DIMENSION_CLASSIFICATION);
        AtlasEntity customerDim        = createTable(CUSTOMER_DIM_TABLE, "customer dimension table", salesDB, storageDesc,
                                                     "fetl", "External", customerDimColumns, DIMENSION_CLASSIFICATION);
        AtlasEntity timeDim            = createTable(TIME_DIM_TABLE, "time dimension table", salesDB, storageDesc,
                                                     "John Doe", "External", timeDimColumns, DIMENSION_CLASSIFICATION);
        AtlasEntity loggingFactDaily   = createTable(LOG_FACT_DAILY_MV_TABLE, "log fact daily materialized view", logDB,
                                                     storageDesc, "Tim ETL", "Managed", logFactColumns, LOGDATA_CLASSIFICATION);
        AtlasEntity loggingFactMonthly = createTable(LOG_FACT_MONTHLY_MV_TABLE, "logging fact monthly materialized view", logDB,
                                                     storageDesc, "Tim ETL", "Managed", logFactColumns, LOGDATA_CLASSIFICATION);
        AtlasEntity salesFactDaily     = createTable(SALES_FACT_DAILY_MV_TABLE, "sales fact daily materialized view", reportingDB,
                                                     storageDesc, "Joe BI", "Managed", salesFactColumns, METRIC_CLASSIFICATION);
        AtlasEntity salesFactMonthly   = createTable(SALES_FACT_MONTHLY_MV_TABLE, "sales fact monthly materialized view", reportingDB,
                                                     storageDesc, "Jane BI", "Managed", salesFactColumns, METRIC_CLASSIFICATION);

        // View entities
        createView(PRODUCT_DIM_VIEW, reportingDB, Collections.singletonList(productDim), DIMENSION_CLASSIFICATION, JDBC_CLASSIFICATION);
        createView(CUSTOMER_DIM_VIEW, reportingDB, Collections.singletonList(customerDim), DIMENSION_CLASSIFICATION, JDBC_CLASSIFICATION);

        // Process entities
        createProcess(LOAD_SALES_DAILY_PROCESS, "hive query for daily summary", "John ETL",
                      Arrays.asList(salesFact, timeDim),
                      Collections.singletonList(salesFactDaily),
                      "create table as select ", "plan", "id", "graph", ETL_CLASSIFICATION);

        createProcess(LOAD_SALES_MONTHLY_PROCESS, "hive query for monthly summary", "John ETL",
                      Collections.singletonList(salesFactDaily),
                      Collections.singletonList(salesFactMonthly),
                      "create table as select ", "plan", "id", "graph", ETL_CLASSIFICATION);

        createProcess(LOAD_LOGS_MONTHLY_PROCESS, "hive query for monthly summary", "Tim ETL",
                      Collections.singletonList(loggingFactDaily),
                      Collections.singletonList(loggingFactMonthly),
                      "create table as select ", "plan", "id", "graph", ETL_CLASSIFICATION);
    }

    private AtlasEntity createInstance(AtlasEntity entity, String[] traitNames) throws Exception {
        AtlasEntity ret = null;
        EntityMutationResponse  response = atlasClientV2.createEntity(new AtlasEntityWithExtInfo(entity));
        List<AtlasEntityHeader> entities = response.getEntitiesByOperation(EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(entities)) {
            AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid());
            ret = getByGuidResponse.getEntity();
            System.out.println("Created entity of type [" + ret.getTypeName() + "], guid: " + ret.getGuid());
        }

        return ret;
    }

    AtlasEntity createDatabase(String name, String description, String owner, String locationUri, String... traitNames)
            throws Exception {
        AtlasEntity entity = new AtlasEntity(DATABASE_TYPE);

        entity.setClassifications(toAtlasClassifications(traitNames));
        entity.setAttribute("name", name);
        entity.setAttribute("description", description);
        entity.setAttribute("owner", owner);
        entity.setAttribute("locationuri", locationUri);
        entity.setAttribute("createTime", System.currentTimeMillis());

        return createInstance(entity, traitNames);
    }

    private List<AtlasClassification> toAtlasClassifications(String[] traitNames) {
        List<AtlasClassification> ret    = new ArrayList<>();
        List<String>              traits = Arrays.asList(traitNames);

        if (CollectionUtils.isNotEmpty(traits)) {
            for (String trait : traits) {
                ret.add(new AtlasClassification(trait));
            }
        }

        return ret;
    }

    AtlasEntity createStorageDescriptor(String location, String inputFormat, String outputFormat, boolean compressed)
            throws Exception {
        AtlasEntity entity = new AtlasEntity(STORAGE_DESC_TYPE);

        entity.setAttribute("location", location);
        entity.setAttribute("inputFormat", inputFormat);
        entity.setAttribute("outputFormat", outputFormat);
        entity.setAttribute("compressed", compressed);

        return createInstance(entity, null);
    }

    AtlasEntity createColumn(String name, String dataType, String comment, String... traitNames) throws Exception {

        AtlasEntity entity = new AtlasEntity(COLUMN_TYPE);
        entity.setClassifications(toAtlasClassifications(traitNames));
        entity.setAttribute("name", name);
        entity.setAttribute("dataType", dataType);
        entity.setAttribute("comment", comment);

        return createInstance(entity, traitNames);
    }

    AtlasEntity createTable(String name, String description, AtlasEntity db, AtlasEntity sd, String owner, String tableType,
                            List<AtlasEntity> columns, String... traitNames) throws Exception {
        AtlasEntity entity = new AtlasEntity(TABLE_TYPE);

        entity.setClassifications(toAtlasClassifications(traitNames));
        entity.setAttribute("name", name);
        entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
        entity.setAttribute("description", description);
        entity.setAttribute("owner", owner);
        entity.setAttribute("tableType", tableType);
        entity.setAttribute("createTime", System.currentTimeMillis());
        entity.setAttribute("lastAccessTime", System.currentTimeMillis());
        entity.setAttribute("retention", System.currentTimeMillis());
        entity.setAttribute("db", AtlasTypeUtil.getAtlasObjectId(db));
        entity.setAttribute("sd", AtlasTypeUtil.getAtlasObjectId(sd));
        entity.setAttribute("columns", AtlasTypeUtil.toObjectIds(columns));

        return createInstance(entity, traitNames);
    }

    AtlasEntity createProcess(String name, String description, String user, List<AtlasEntity> inputs, List<AtlasEntity> outputs,
            String queryText, String queryPlan, String queryId, String queryGraph, String... traitNames) throws Exception {
        AtlasEntity entity = new AtlasEntity(LOAD_PROCESS_TYPE);

        entity.setClassifications(toAtlasClassifications(traitNames));
        entity.setAttribute(AtlasClient.NAME, name);
        entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
        entity.setAttribute("description", description);
        entity.setAttribute("inputs", inputs);
        entity.setAttribute("outputs", outputs);
        entity.setAttribute("user", user);
        entity.setAttribute("startTime", System.currentTimeMillis());
        entity.setAttribute("endTime", System.currentTimeMillis() + 10000);
        entity.setAttribute("queryText", queryText);
        entity.setAttribute("queryPlan", queryPlan);
        entity.setAttribute("queryId", queryId);
        entity.setAttribute("queryGraph", queryGraph);

        return createInstance(entity, traitNames);
    }

    AtlasEntity createView(String name, AtlasEntity db, List<AtlasEntity> inputTables, String... traitNames) throws Exception {
        AtlasEntity entity = new AtlasEntity(VIEW_TYPE);

        entity.setClassifications(toAtlasClassifications(traitNames));
        entity.setAttribute("name", name);
        entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
        entity.setAttribute("db", db);
        entity.setAttribute("inputTables", inputTables);

        return createInstance(entity, traitNames);
    }

    private void verifyTypesCreated() throws Exception {
        MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();

        for (String typeName : TYPES) {
            searchParams.clear();
            searchParams.add(SearchFilter.PARAM_NAME, typeName);
            SearchFilter searchFilter = new SearchFilter(searchParams);
            AtlasTypesDef searchDefs = atlasClientV2.getAllTypeDefs(searchFilter);

            assert (!searchDefs.isEmpty());
            System.out.println("Created type [" + typeName + "]");
        }
    }

    private String[] getDSLQueries() {
        return new String[]{
                "from DB",
                "DB",
                "DB where name=%22Reporting%22",
                "DB where name=%22encode_db_name%22",
                "Table where name=%2522sales_fact%2522",
                "DB where name=\"Reporting\"",
                "DB where DB.name=\"Reporting\"",
                "DB name = \"Reporting\"",
                "DB DB.name = \"Reporting\"",
                "DB where name=\"Reporting\" select name, owner",
                "DB where DB.name=\"Reporting\" select name, owner",
                "DB has name",
                "DB where DB has name",
//--TODO: Fix   "DB, Table",    // Table, db; Table db works
                "DB is JdbcAccess",
                "from Table",
                "Table",
                "Table is Dimension",
                "Column where Column isa PII",
                "View is Dimension",
                "Column select Column.name",
                "Column select name",
                "Column where Column.name=\"customer_id\"",
                "from Table select Table.name",
                "DB where (name = \"Reporting\")",
//--TODO: Fix   "DB where (name = \"Reporting\") select name as _col_0, owner as _col_1",
                "DB where DB is JdbcAccess",
                "DB where DB has name",
//--TODO: Fix   "DB Table",
                "DB as db1 Table where (db1.name = \"Reporting\")",
//--TODO: Fix   "DB where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 ", // N
                DIMENSION_CLASSIFICATION,
                JDBC_CLASSIFICATION,
                ETL_CLASSIFICATION,
                METRIC_CLASSIFICATION,
                PII_CLASSIFICATION,
                "`Log Data`",
                "Table where name=\"sales_fact\", columns",
                "Table where name=\"sales_fact\", columns as column select column.name, column.dataType, column.comment",
                "from DataSet",
                "from Process" };
    }

    private void search() throws Exception {
        System.out.println("\nSample DSL Queries: ");

        for (String dslQuery : getDSLQueries()) {
            try {
                AtlasSearchResult results = atlasClientV2.dslSearchWithParams(dslQuery, 10, 0);

                if (results != null) {
                    List<AtlasEntityHeader> entitiesResult = results.getEntities();
                    List<AtlasFullTextResult> fullTextResults = results.getFullTextResult();
                    AttributeSearchResult attribResult = results.getAttributes();

                    if (CollectionUtils.isNotEmpty(entitiesResult)) {
                        System.out.println("query [" + dslQuery + "] returned [" + entitiesResult.size() + "] rows.");
                    } else if (CollectionUtils.isNotEmpty(fullTextResults)) {
                        System.out.println("query [" + dslQuery + "] returned [" + fullTextResults.size() + "] rows.");
                    } else if (attribResult != null) {
                        System.out.println("query [" + dslQuery + "] returned [" + attribResult.getValues().size() + "] rows.");
                    } else {
                        System.out.println("query [" + dslQuery + "] returned [ 0 ] rows.");
                    }
                } else {
                    System.out.println("query [" + dslQuery + "] failed, results:" + results);
                }
            } catch (Exception e) {
                System.out.println("query [" + dslQuery + "] execution failed!");
            }
        }
    }

    private void lineage() throws AtlasServiceException {
        System.out.println("\nSample Lineage Info: ");

        AtlasLineageInfo lineageInfo = atlasClientV2.getLineageInfo(getTableId(SALES_FACT_DAILY_MV_TABLE), LineageDirection.BOTH, 0);
        Set<LineageRelation> relations = lineageInfo.getRelations();
        Map<String, AtlasEntityHeader> guidEntityMap = lineageInfo.getGuidEntityMap();

        for (LineageRelation relation : relations) {
            AtlasEntityHeader fromEntity = guidEntityMap.get(relation.getFromEntityId());
            AtlasEntityHeader toEntity   = guidEntityMap.get(relation.getToEntityId());

            System.out.println(fromEntity.getDisplayText() + "(" + fromEntity.getTypeName() + ") -> " +
                               toEntity.getDisplayText()   + "(" + toEntity.getTypeName() + ")");
        }
    }

    private String getTableId(String tableName) throws AtlasServiceException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);

        AtlasEntity tableEntity = atlasClientV2.getEntityByAttribute(TABLE_TYPE, attributes).getEntity();
        return tableEntity.getGuid();
    }

    private void closeConnection() {
        if (atlasClientV2 != null) {
            atlasClientV2.close();
        }
    }
}
