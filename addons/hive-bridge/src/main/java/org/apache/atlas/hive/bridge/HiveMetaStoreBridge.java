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

package org.apache.atlas.hive.bridge;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.hook.events.BaseHiveEvent;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.atlas.utils.HdfsNameServiceResolver;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.hive.hook.events.BaseHiveEvent.*;

/**
 * A Bridge Utility that imports metadata from the Hive Meta Store
 * and registers them in Atlas.
 */

public class HiveMetaStoreBridge {
    private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreBridge.class);

    public static final String CONF_PREFIX                     = "atlas.hook.hive.";
    public static final String HIVE_CLUSTER_NAME               = "atlas.cluster.name";
    public static final String HDFS_PATH_CONVERT_TO_LOWER_CASE = CONF_PREFIX + "hdfs_path.convert_to_lowercase";
    public static final String DEFAULT_CLUSTER_NAME            = "primary";
    public static final String TEMP_TABLE_PREFIX               = "_temp-";
    public static final String ATLAS_ENDPOINT                  = "atlas.rest.address";
    public static final String SEP                             = ":".intern();
    public static final String HDFS_PATH                       = "hdfs_path";

    private static final int    EXIT_CODE_SUCCESS = 0;
    private static final int    EXIT_CODE_FAILED  = 1;
    private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";

    private final String                  clusterName;
    private final Hive                    hiveClient;
    private final AtlasClientV2           atlasClientV2;
    private final boolean                 convertHdfsPathToLowerCase;


    public static void main(String[] args) {
        int exitCode = EXIT_CODE_FAILED;
        AtlasClientV2 atlasClientV2 = null;

        try {
            Options options = new Options();
            options.addOption("d", "database", true, "Database name");
            options.addOption("t", "table", true, "Table name");
            options.addOption("f", "filename", true, "Filename");
            options.addOption("failOnError", false, "failOnError");

            CommandLine   cmd              = new BasicParser().parse(options, args);
            boolean       failOnError      = cmd.hasOption("failOnError");
            String        databaseToImport = cmd.getOptionValue("d");
            String        tableToImport    = cmd.getOptionValue("t");
            String        fileToImport     = cmd.getOptionValue("f");
            Configuration atlasConf        = ApplicationProperties.get();
            String[]      atlasEndpoint    = atlasConf.getStringArray(ATLAS_ENDPOINT);

            if (atlasEndpoint == null || atlasEndpoint.length == 0) {
                atlasEndpoint = new String[] { DEFAULT_ATLAS_URL };
            }


            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();

                atlasClientV2 = new AtlasClientV2(atlasEndpoint, basicAuthUsernamePassword);
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), atlasEndpoint);
            }

            HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(atlasConf, new HiveConf(), atlasClientV2);

            if (StringUtils.isNotEmpty(fileToImport)) {
                File f = new File(fileToImport);

                if (f.exists() && f.canRead()) {
                    BufferedReader br   = new BufferedReader(new FileReader(f));
                    String         line = null;

                    while((line = br.readLine()) != null) {
                        String val[] = line.split(":");

                        if (ArrayUtils.isNotEmpty(val)) {
                            databaseToImport = val[0];

                            if (val.length > 1) {
                                tableToImport = val[1];
                            } else {
                                tableToImport = "";
                            }

                            hiveMetaStoreBridge.importHiveMetadata(databaseToImport, tableToImport, failOnError);
                        }
                    }

                    exitCode = EXIT_CODE_SUCCESS;
                } else {
                    LOG.error("Failed to read the input file: " + fileToImport);
                }
            } else {
                hiveMetaStoreBridge.importHiveMetadata(databaseToImport, tableToImport, failOnError);
            }

            exitCode = EXIT_CODE_SUCCESS;
        } catch(ParseException e) {
            LOG.error("Failed to parse arguments. Error: ", e.getMessage());
            printUsage();
        } catch(Exception e) {
            LOG.error("Import failed", e);
        } finally {
            if( atlasClientV2 !=null) {
                atlasClientV2.close();
            }
        }

        System.exit(exitCode);
    }

    private static void printUsage() {
        System.out.println();
        System.out.println();
        System.out.println("Usage 1: import-hive.sh [-d <database> OR --database <database>] "  );
        System.out.println("    Imports specified database and its tables ...");
        System.out.println();
        System.out.println("Usage 2: import-hive.sh [-d <database> OR --database <database>] [-t <table> OR --table <table>]");
        System.out.println("    Imports specified table within that database ...");
        System.out.println();
        System.out.println("Usage 3: import-hive.sh");
        System.out.println("    Imports all databases and tables...");
        System.out.println();
        System.out.println("Usage 4: import-hive.sh -f <filename>");
        System.out.println("  Imports all databases and tables in the file...");
        System.out.println("    Format:");
        System.out.println("    database1:tbl1");
        System.out.println("    database1:tbl2");
        System.out.println("    database2:tbl2");
        System.out.println();
    }

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf {@link HiveConf} for Hive component in the cluster
     */
    public HiveMetaStoreBridge(Configuration atlasProperties, HiveConf hiveConf, AtlasClientV2 atlasClientV2) throws Exception {
        this(atlasProperties.getString(HIVE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME), Hive.get(hiveConf), atlasClientV2, atlasProperties.getBoolean(HDFS_PATH_CONVERT_TO_LOWER_CASE, false));
    }

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf {@link HiveConf} for Hive component in the cluster
     */
    public HiveMetaStoreBridge(Configuration atlasProperties, HiveConf hiveConf) throws Exception {
        this(atlasProperties, hiveConf, null);
    }

    HiveMetaStoreBridge(String clusterName, Hive hiveClient, AtlasClientV2 atlasClientV2) {
        this(clusterName, hiveClient, atlasClientV2, true);
    }

    HiveMetaStoreBridge(String clusterName, Hive hiveClient, AtlasClientV2 atlasClientV2, boolean convertHdfsPathToLowerCase) {
        this.clusterName                = clusterName;
        this.hiveClient                 = hiveClient;
        this.atlasClientV2              = atlasClientV2;
        this.convertHdfsPathToLowerCase = convertHdfsPathToLowerCase;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Hive getHiveClient() {
        return hiveClient;
    }

    public boolean isConvertHdfsPathToLowerCase() {
        return convertHdfsPathToLowerCase;
    }


    @VisibleForTesting
    public void importHiveMetadata(String databaseToImport, String tableToImport, boolean failOnError) throws Exception {
        LOG.info("Importing Hive metadata");

        importDatabases(failOnError, databaseToImport, tableToImport);
    }

    private void importDatabases(boolean failOnError, String databaseToImport, String tableToImport) throws Exception {
        List<String> databaseNames = null;

        if (StringUtils.isEmpty(databaseToImport) && StringUtils.isEmpty(tableToImport)) {
            //when both database and table to import are empty, import all
            databaseNames = hiveClient.getAllDatabases();
        } else if (StringUtils.isEmpty(databaseToImport) && StringUtils.isNotEmpty(tableToImport)) {
            //when database is empty and table is not, then check table has database name in it and import that db and table
            if (isTableWithDatabaseName(tableToImport)) {
                String val[] = tableToImport.split("\\.");
                if (val.length > 1) {
                    databaseToImport = val[0];
                    tableToImport = val[1];
                }
                databaseNames = hiveClient.getDatabasesByPattern(databaseToImport);
            } else {
                databaseNames = hiveClient.getAllDatabases();
            }
        } else {
            //when database to import has some value then, import that db and all table under it.
            databaseNames = hiveClient.getDatabasesByPattern(databaseToImport);
        }

        if(!CollectionUtils.isEmpty(databaseNames)) {
            LOG.info("Found {} databases", databaseNames.size());

            for (String databaseName : databaseNames) {
                AtlasEntityWithExtInfo dbEntity = registerDatabase(databaseName);

                if (dbEntity != null) {
                    importTables(dbEntity.getEntity(), databaseName, tableToImport, failOnError);
                }
            }
        } else {
            LOG.info("No database found");
        }
    }

    /**
     * Imports all tables for the given db
     * @param dbEntity
     * @param databaseName
     * @param failOnError
     * @throws Exception
     */
    private int importTables(AtlasEntity dbEntity, String databaseName, String tblName, final boolean failOnError) throws Exception {
        int tablesImported = 0;

        final List<String> tableNames;

        if (StringUtils.isEmpty(tblName)) {
            tableNames = hiveClient.getAllTables(databaseName);
        } else {
            tableNames = hiveClient.getTablesByPattern(databaseName, tblName);
        }

        if(!CollectionUtils.isEmpty(tableNames)) {
            LOG.info("Found {} tables to import in database {}", tableNames.size(), databaseName);

            try {
                for (String tableName : tableNames) {
                    int imported = importTable(dbEntity, databaseName, tableName, failOnError);

                    tablesImported += imported;
                }
            } finally {
                if (tablesImported == tableNames.size()) {
                    LOG.info("Successfully imported {} tables from database {}", tablesImported, databaseName);
                } else {
                    LOG.error("Imported {} of {} tables from database {}. Please check logs for errors during import", tablesImported, tableNames.size(), databaseName);
                }
            }
        } else {
            LOG.info("No tables to import in database {}", databaseName);
        }

        return tablesImported;
    }

    @VisibleForTesting
    public int importTable(AtlasEntity dbEntity, String databaseName, String tableName, final boolean failOnError) throws Exception {
        try {
            Table                  table       = hiveClient.getTable(databaseName, tableName);
            AtlasEntityWithExtInfo tableEntity = registerTable(dbEntity, table);

            if (table.getTableType() == TableType.EXTERNAL_TABLE) {
                String                 processQualifiedName = getTableProcessQualifiedName(clusterName, table);
                AtlasEntityWithExtInfo processEntity        = findProcessEntity(processQualifiedName);

                if (processEntity == null) {
                    String      tableLocation = isConvertHdfsPathToLowerCase() ? lower(table.getDataLocation().toString()) : table.getDataLocation().toString();
                    String      query         = getCreateTableString(table, tableLocation);
                    AtlasEntity pathInst      = toHdfsPathEntity(tableLocation);
                    AtlasEntity tableInst     = tableEntity.getEntity();
                    AtlasEntity processInst   = new AtlasEntity(HiveDataTypes.HIVE_PROCESS.getName());
                    long        now           = System.currentTimeMillis();

                    processInst.setAttribute(ATTRIBUTE_QUALIFIED_NAME, processQualifiedName);
                    processInst.setAttribute(ATTRIBUTE_NAME, query);
                    processInst.setAttribute(ATTRIBUTE_CLUSTER_NAME, clusterName);
                    processInst.setAttribute(ATTRIBUTE_INPUTS, Collections.singletonList(BaseHiveEvent.getObjectId(pathInst)));
                    processInst.setAttribute(ATTRIBUTE_OUTPUTS, Collections.singletonList(BaseHiveEvent.getObjectId(tableInst)));
                    processInst.setAttribute(ATTRIBUTE_USER_NAME, table.getOwner());
                    processInst.setAttribute(ATTRIBUTE_START_TIME, now);
                    processInst.setAttribute(ATTRIBUTE_END_TIME, now);
                    processInst.setAttribute(ATTRIBUTE_OPERATION_TYPE, "CREATETABLE");
                    processInst.setAttribute(ATTRIBUTE_QUERY_TEXT, query);
                    processInst.setAttribute(ATTRIBUTE_QUERY_ID, query);
                    processInst.setAttribute(ATTRIBUTE_QUERY_PLAN, "{}");
                    processInst.setAttribute(ATTRIBUTE_RECENT_QUERIES, Collections.singletonList(query));

                    AtlasEntitiesWithExtInfo createTableProcess = new AtlasEntitiesWithExtInfo();

                    createTableProcess.addEntity(processInst);
                    createTableProcess.addEntity(pathInst);

                    registerInstances(createTableProcess);
                } else {
                    LOG.info("Process {} is already registered", processQualifiedName);
                }
            }

            return 1;
        } catch (Exception e) {
            LOG.error("Import failed for hive_table {}", tableName, e);

            if (failOnError) {
                throw e;
            }

            return 0;
        }
    }

    /**
     * Checks if db is already registered, else creates and registers db entity
     * @param databaseName
     * @return
     * @throws Exception
     */
    private AtlasEntityWithExtInfo registerDatabase(String databaseName) throws Exception {
        AtlasEntityWithExtInfo ret = null;
        Database               db  = hiveClient.getDatabase(databaseName);

        if (db != null) {
            ret = findDatabase(clusterName, databaseName);

            if (ret == null) {
                ret = registerInstance(new AtlasEntityWithExtInfo(toDbEntity(db)));
            } else {
                LOG.info("Database {} is already registered - id={}. Updating it.", databaseName, ret.getEntity().getGuid());

                ret.setEntity(toDbEntity(db, ret.getEntity()));

                updateInstance(ret);
            }
        }

        return ret;
    }

    private AtlasEntityWithExtInfo registerTable(AtlasEntity dbEntity, Table table) throws AtlasHookException {
        try {
            AtlasEntityWithExtInfo ret;
            AtlasEntityWithExtInfo tableEntity = findTableEntity(table);

            if (tableEntity == null) {
                tableEntity = toTableEntity(dbEntity, table);

                ret = registerInstance(tableEntity);
            } else {
                LOG.info("Table {}.{} is already registered with id {}. Updating entity.", table.getDbName(), table.getTableName(), tableEntity.getEntity().getGuid());

                ret = toTableEntity(dbEntity, table, tableEntity);

                updateInstance(ret);
            }

            return ret;
        } catch (Exception e) {
            throw new AtlasHookException("HiveMetaStoreBridge.registerTable() failed.", e);
        }
    }

    /**
     * Registers an entity in atlas
     * @param entity
     * @return
     * @throws Exception
     */
    private AtlasEntityWithExtInfo registerInstance(AtlasEntityWithExtInfo entity) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("creating {} entity: {}", entity.getEntity().getTypeName(), entity);
        }

        AtlasEntityWithExtInfo  ret             = null;
        EntityMutationResponse  response        = atlasClientV2.createEntity(entity);
        List<AtlasEntityHeader> createdEntities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntityHeader createdEntity : createdEntities) {
                if (ret == null) {
                    ret = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                    LOG.info("Created {} entity: name={}, guid={}", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
                } else if (ret.getEntity(createdEntity.getGuid()) == null) {
                    AtlasEntityWithExtInfo newEntity = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                    ret.addReferredEntity(newEntity.getEntity());

                    if (MapUtils.isNotEmpty(newEntity.getReferredEntities())) {
                        for (Map.Entry<String, AtlasEntity> entry : newEntity.getReferredEntities().entrySet()) {
                            ret.addReferredEntity(entry.getKey(), entry.getValue());
                        }
                    }

                    LOG.info("Created {} entity: name={}, guid={}", newEntity.getEntity().getTypeName(), newEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), newEntity.getEntity().getGuid());
                }
            }
        }

        clearRelationshipAttributes(ret);

        return ret;
    }

    /**
     * Registers an entity in atlas
     * @param entities
     * @return
     * @throws Exception
     */
    private AtlasEntitiesWithExtInfo registerInstances(AtlasEntitiesWithExtInfo entities) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("creating {} entities: {}", entities.getEntities().size(), entities);
        }

        AtlasEntitiesWithExtInfo ret = null;
        EntityMutationResponse   response        = atlasClientV2.createEntities(entities);
        List<AtlasEntityHeader>  createdEntities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            ret = new AtlasEntitiesWithExtInfo();

            for (AtlasEntityHeader createdEntity : createdEntities) {
                AtlasEntityWithExtInfo entity = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                ret.addEntity(entity.getEntity());

                if (MapUtils.isNotEmpty(entity.getReferredEntities())) {
                    for (Map.Entry<String, AtlasEntity> entry : entity.getReferredEntities().entrySet()) {
                        ret.addReferredEntity(entry.getKey(), entry.getValue());
                    }
                }

                LOG.info("Created {} entity: name={}, guid={}", entity.getEntity().getTypeName(), entity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), entity.getEntity().getGuid());
            }
        }

        clearRelationshipAttributes(ret);

        return ret;
    }

    private void updateInstance(AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updating {} entity: {}", entity.getEntity().getTypeName(), entity);
        }

        atlasClientV2.updateEntity(entity);

        LOG.info("Updated {} entity: name={}, guid={}", entity.getEntity().getTypeName(), entity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), entity.getEntity().getGuid());
    }

    /**
     * Create a Hive Database entity
     * @param hiveDB The Hive {@link Database} object from which to map properties
     * @return new Hive Database AtlasEntity
     * @throws HiveException
     */
    private AtlasEntity toDbEntity(Database hiveDB) throws HiveException {
        return toDbEntity(hiveDB, null);
    }

    private AtlasEntity toDbEntity(Database hiveDB, AtlasEntity dbEntity) {
        if (dbEntity == null) {
            dbEntity = new AtlasEntity(HiveDataTypes.HIVE_DB.getName());
        }

        String dbName = hiveDB.getName().toLowerCase();

        dbEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getDBQualifiedName(clusterName, dbName));
        dbEntity.setAttribute(ATTRIBUTE_NAME, dbName);
        dbEntity.setAttribute(ATTRIBUTE_DESCRIPTION, hiveDB.getDescription());
        dbEntity.setAttribute(ATTRIBUTE_OWNER, hiveDB.getOwnerName());

        dbEntity.setAttribute(ATTRIBUTE_CLUSTER_NAME, clusterName);
        dbEntity.setAttribute(ATTRIBUTE_LOCATION, HdfsNameServiceResolver.getPathWithNameServiceID(hiveDB.getLocationUri()));
        dbEntity.setAttribute(ATTRIBUTE_PARAMETERS, hiveDB.getParameters());

        if (hiveDB.getOwnerType() != null) {
            dbEntity.setAttribute(ATTRIBUTE_OWNER_TYPE, OWNER_TYPE_TO_ENUM_VALUE.get(hiveDB.getOwnerType().getValue()));
        }

        return dbEntity;
    }
    /**
     * Create a new table instance in Atlas
     * @param  database AtlasEntity for Hive  {@link AtlasEntity} to which this table belongs
     * @param hiveTable reference to the Hive {@link Table} from which to map properties
     * @return Newly created Hive AtlasEntity
     * @throws Exception
     */
    private AtlasEntityWithExtInfo toTableEntity(AtlasEntity database, Table hiveTable) throws AtlasHookException {
        return toTableEntity(database, hiveTable, null);
    }

    private AtlasEntityWithExtInfo toTableEntity(AtlasEntity database, final Table hiveTable, AtlasEntityWithExtInfo table) throws AtlasHookException {
        if (table == null) {
            table = new AtlasEntityWithExtInfo(new AtlasEntity(HiveDataTypes.HIVE_TABLE.getName()));
        }

        AtlasEntity tableEntity        = table.getEntity();
        String      tableQualifiedName = getTableQualifiedName(clusterName, hiveTable);
        long        createTime         = BaseHiveEvent.getTableCreateTime(hiveTable);
        long        lastAccessTime     = hiveTable.getLastAccessTime() > 0 ? hiveTable.getLastAccessTime() : createTime;

        tableEntity.setAttribute(ATTRIBUTE_DB, BaseHiveEvent.getObjectId(database));
        tableEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tableQualifiedName);
        tableEntity.setAttribute(ATTRIBUTE_NAME, hiveTable.getTableName().toLowerCase());
        tableEntity.setAttribute(ATTRIBUTE_OWNER, hiveTable.getOwner());

        tableEntity.setAttribute(ATTRIBUTE_CREATE_TIME, createTime);
        tableEntity.setAttribute(ATTRIBUTE_LAST_ACCESS_TIME, lastAccessTime);
        tableEntity.setAttribute(ATTRIBUTE_RETENTION, hiveTable.getRetention());
        tableEntity.setAttribute(ATTRIBUTE_PARAMETERS, hiveTable.getParameters());
        tableEntity.setAttribute(ATTRIBUTE_COMMENT, hiveTable.getParameters().get(ATTRIBUTE_COMMENT));
        tableEntity.setAttribute(ATTRIBUTE_TABLE_TYPE, hiveTable.getTableType().name());
        tableEntity.setAttribute(ATTRIBUTE_TEMPORARY, hiveTable.isTemporary());

        if (hiveTable.getViewOriginalText() != null) {
            tableEntity.setAttribute(ATTRIBUTE_VIEW_ORIGINAL_TEXT, hiveTable.getViewOriginalText());
        }

        if (hiveTable.getViewExpandedText() != null) {
            tableEntity.setAttribute(ATTRIBUTE_VIEW_EXPANDED_TEXT, hiveTable.getViewExpandedText());
        }

        AtlasEntity       sdEntity = toStroageDescEntity(hiveTable.getSd(), tableQualifiedName, getStorageDescQFName(tableQualifiedName), BaseHiveEvent.getObjectId(tableEntity));
        List<AtlasEntity> partKeys = toColumns(hiveTable.getPartitionKeys(), tableEntity);
        List<AtlasEntity> columns  = toColumns(hiveTable.getCols(), tableEntity);

        tableEntity.setAttribute(ATTRIBUTE_STORAGEDESC, BaseHiveEvent.getObjectId(sdEntity));
        tableEntity.setAttribute(ATTRIBUTE_PARTITION_KEYS, BaseHiveEvent.getObjectIds(partKeys));
        tableEntity.setAttribute(ATTRIBUTE_COLUMNS, BaseHiveEvent.getObjectIds(columns));

        table.addReferredEntity(database);
        table.addReferredEntity(sdEntity);

        if (partKeys != null) {
            for (AtlasEntity partKey : partKeys) {
                table.addReferredEntity(partKey);
            }
        }

        if (columns != null) {
            for (AtlasEntity column : columns) {
                table.addReferredEntity(column);
            }
        }

        table.setEntity(tableEntity);

        return table;
    }

    private AtlasEntity toStroageDescEntity(StorageDescriptor storageDesc, String tableQualifiedName, String sdQualifiedName, AtlasObjectId tableId ) throws AtlasHookException {
        AtlasEntity ret = new AtlasEntity(HiveDataTypes.HIVE_STORAGEDESC.getName());

        ret.setAttribute(ATTRIBUTE_TABLE, tableId);
        ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, sdQualifiedName);
        ret.setAttribute(ATTRIBUTE_PARAMETERS, storageDesc.getParameters());
        ret.setAttribute(ATTRIBUTE_LOCATION, HdfsNameServiceResolver.getPathWithNameServiceID(storageDesc.getLocation()));
        ret.setAttribute(ATTRIBUTE_INPUT_FORMAT, storageDesc.getInputFormat());
        ret.setAttribute(ATTRIBUTE_OUTPUT_FORMAT, storageDesc.getOutputFormat());
        ret.setAttribute(ATTRIBUTE_COMPRESSED, storageDesc.isCompressed());
        ret.setAttribute(ATTRIBUTE_NUM_BUCKETS, storageDesc.getNumBuckets());
        ret.setAttribute(ATTRIBUTE_STORED_AS_SUB_DIRECTORIES, storageDesc.isStoredAsSubDirectories());

        if (storageDesc.getBucketCols().size() > 0) {
            ret.setAttribute(ATTRIBUTE_BUCKET_COLS, storageDesc.getBucketCols());
        }

        if (storageDesc.getSerdeInfo() != null) {
            SerDeInfo serdeInfo = storageDesc.getSerdeInfo();

            LOG.debug("serdeInfo = {}", serdeInfo);
            // SkewedInfo skewedInfo = storageDesc.getSkewedInfo();

            AtlasStruct serdeInfoStruct = new AtlasStruct(HiveDataTypes.HIVE_SERDE.getName());

            serdeInfoStruct.setAttribute(ATTRIBUTE_NAME, serdeInfo.getName());
            serdeInfoStruct.setAttribute(ATTRIBUTE_SERIALIZATION_LIB, serdeInfo.getSerializationLib());
            serdeInfoStruct.setAttribute(ATTRIBUTE_PARAMETERS, serdeInfo.getParameters());

            ret.setAttribute(ATTRIBUTE_SERDE_INFO, serdeInfoStruct);
        }

        if (CollectionUtils.isNotEmpty(storageDesc.getSortCols())) {
            List<AtlasStruct> sortColsStruct = new ArrayList<>();

            for (Order sortcol : storageDesc.getSortCols()) {
                String hiveOrderName = HiveDataTypes.HIVE_ORDER.getName();
                AtlasStruct colStruct = new AtlasStruct(hiveOrderName);
                colStruct.setAttribute("col", sortcol.getCol());
                colStruct.setAttribute("order", sortcol.getOrder());

                sortColsStruct.add(colStruct);
            }

            ret.setAttribute(ATTRIBUTE_SORT_COLS, sortColsStruct);
        }

        return ret;
    }

    private List<AtlasEntity> toColumns(List<FieldSchema> schemaList, AtlasEntity table) throws AtlasHookException {
        List<AtlasEntity> ret = new ArrayList<>();

        int columnPosition = 0;
        for (FieldSchema fs : schemaList) {
            LOG.debug("Processing field {}", fs);

            AtlasEntity column = new AtlasEntity(HiveDataTypes.HIVE_COLUMN.getName());

            column.setAttribute(ATTRIBUTE_TABLE, BaseHiveEvent.getObjectId(table));
            column.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getColumnQualifiedName((String) table.getAttribute(ATTRIBUTE_QUALIFIED_NAME), fs.getName()));
            column.setAttribute(ATTRIBUTE_NAME, fs.getName());
            column.setAttribute(ATTRIBUTE_OWNER, table.getAttribute(ATTRIBUTE_OWNER));
            column.setAttribute(ATTRIBUTE_COL_TYPE, fs.getType());
            column.setAttribute(ATTRIBUTE_COL_POSITION, columnPosition++);
            column.setAttribute(ATTRIBUTE_COMMENT, fs.getComment());

            ret.add(column);
        }
        return ret;
    }

    private AtlasEntity toHdfsPathEntity(String pathUri) {
        AtlasEntity ret           = new AtlasEntity(HDFS_PATH);
        String      nameServiceID = HdfsNameServiceResolver.getNameServiceIDForPath(pathUri);
        Path        path          = new Path(pathUri);

        ret.setAttribute(ATTRIBUTE_NAME, Path.getPathWithoutSchemeAndAuthority(path).toString());
        ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, clusterName);

        if (StringUtils.isNotEmpty(nameServiceID)) {
            // Name service resolution is successful, now get updated HDFS path where the host port info is replaced by resolved name service
            String updatedHdfsPath = HdfsNameServiceResolver.getPathWithNameServiceID(pathUri);

            ret.setAttribute(ATTRIBUTE_PATH, updatedHdfsPath);
            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getHdfsPathQualifiedName(updatedHdfsPath));
            ret.setAttribute(ATTRIBUTE_NAMESERVICE_ID, nameServiceID);
        } else {
            ret.setAttribute(ATTRIBUTE_PATH, pathUri);

            // Only append clusterName for the HDFS path
            if (pathUri.startsWith(HdfsNameServiceResolver.HDFS_SCHEME)) {
                ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getHdfsPathQualifiedName(pathUri));
            } else {
                ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathUri);
            }
        }

        return ret;
    }

    /**
     * Gets the atlas entity for the database
     * @param databaseName  database Name
     * @param clusterName    cluster name
     * @return AtlasEntity for database if exists, else null
     * @throws Exception
     */
    private AtlasEntityWithExtInfo findDatabase(String clusterName, String databaseName) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Searching Atlas for database {}", databaseName);
        }

        String typeName = HiveDataTypes.HIVE_DB.getName();

        return findEntity(typeName, getDBQualifiedName(clusterName, databaseName));
    }

    /**
     * Gets Atlas Entity for the table
     *
     * @param hiveTable
     * @return table entity from Atlas  if exists, else null
     * @throws Exception
     */
    private AtlasEntityWithExtInfo findTableEntity(Table hiveTable)  throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Searching Atlas for table {}.{}", hiveTable.getDbName(), hiveTable.getTableName());
        }

        String typeName         = HiveDataTypes.HIVE_TABLE.getName();
        String tblQualifiedName = getTableQualifiedName(getClusterName(), hiveTable.getDbName(), hiveTable.getTableName());

        return findEntity(typeName, tblQualifiedName);
    }

    private AtlasEntityWithExtInfo findProcessEntity(String qualifiedName) throws Exception{
        if (LOG.isDebugEnabled()) {
            LOG.debug("Searching Atlas for process {}", qualifiedName);
        }

        String typeName = HiveDataTypes.HIVE_PROCESS.getName();

        return findEntity(typeName, qualifiedName);
    }

    private AtlasEntityWithExtInfo findEntity(final String typeName, final String qualifiedName) throws AtlasServiceException {
        AtlasEntityWithExtInfo ret = null;

        try {
            ret = atlasClientV2.getEntityByAttribute(typeName, Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName));
        } catch (AtlasServiceException e) {
            if(e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }

            throw e;
        }

        clearRelationshipAttributes(ret);

        return ret;
    }

    private String getCreateTableString(Table table, String location){
        String            colString = "";
        List<FieldSchema> colList   = table.getAllCols();

        if (colList != null) {
            for (FieldSchema col : colList) {
                colString += col.getName() + " " + col.getType() + ",";
            }

            if (colList.size() > 0) {
                colString = colString.substring(0, colString.length() - 1);
                colString = "(" + colString + ")";
            }
        }

        String query = "create external table " + table.getTableName() +  colString + " location '" + location + "'";

        return query;
    }

    private String lower(String str) {
        if (StringUtils.isEmpty(str)) {
            return "";
        }

        return str.toLowerCase().trim();
    }


    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param table hive table for which the qualified name is needed
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    private static String getTableQualifiedName(String clusterName, Table table) {
        return getTableQualifiedName(clusterName, table.getDbName(), table.getTableName(), table.isTemporary());
    }

    private String getHdfsPathQualifiedName(String hdfsPath) {
        return String.format("%s@%s", hdfsPath, clusterName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Database instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param dbName Name of the Hive database
     * @return Unique qualified name to identify the Database instance in Atlas.
     */
    public static String getDBQualifiedName(String clusterName, String dbName) {
        return String.format("%s@%s", dbName.toLowerCase(), clusterName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param dbName Name of the Hive database to which the Table belongs
     * @param tableName Name of the Hive table
     * @param isTemporaryTable is this a temporary table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String clusterName, String dbName, String tableName, boolean isTemporaryTable) {
        String tableTempName = tableName;

        if (isTemporaryTable) {
            if (SessionState.get() != null && SessionState.get().getSessionId() != null) {
                tableTempName = tableName + TEMP_TABLE_PREFIX + SessionState.get().getSessionId();
            } else {
                tableTempName = tableName + TEMP_TABLE_PREFIX + RandomStringUtils.random(10);
            }
        }

        return String.format("%s.%s@%s", dbName.toLowerCase(), tableTempName.toLowerCase(), clusterName);
    }

    public static String getTableProcessQualifiedName(String clusterName, Table table) {
        String tableQualifiedName = getTableQualifiedName(clusterName, table);
        long   createdTime        = getTableCreatedTime(table);

        return tableQualifiedName + SEP + createdTime;
    }


    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param dbName Name of the Hive database to which the Table belongs
     * @param tableName Name of the Hive table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String clusterName, String dbName, String tableName) {
        return getTableQualifiedName(clusterName, dbName, tableName, false);
    }
    public static String getStorageDescQFName(String tableQualifiedName) {
        return tableQualifiedName + "_storage";
    }

    public static String getColumnQualifiedName(final String tableQualifiedName, final String colName) {
        final String[] parts       = tableQualifiedName.split("@");
        final String   tableName   = parts[0];
        final String   clusterName = parts[1];

        return String.format("%s.%s@%s", tableName, colName.toLowerCase(), clusterName);
    }

    public static long getTableCreatedTime(Table table) {
        return table.getTTable().getCreateTime() * MILLIS_CONVERT_FACTOR;
    }

    private void clearRelationshipAttributes(AtlasEntitiesWithExtInfo entities) {
        if (entities != null) {
            if (entities.getEntities() != null) {
                for (AtlasEntity entity : entities.getEntities()) {
                    clearRelationshipAttributes(entity);;
                }
            }

            if (entities.getReferredEntities() != null) {
                clearRelationshipAttributes(entities.getReferredEntities().values());
            }
        }
    }

    private void clearRelationshipAttributes(AtlasEntityWithExtInfo entity) {
        if (entity != null) {
            clearRelationshipAttributes(entity.getEntity());

            if (entity.getReferredEntities() != null) {
                clearRelationshipAttributes(entity.getReferredEntities().values());
            }
        }
    }

    private void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
        if (entities != null) {
            for (AtlasEntity entity : entities) {
                clearRelationshipAttributes(entity);
            }
        }
    }

    private void clearRelationshipAttributes(AtlasEntity entity) {
        if (entity != null && entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().clear();
        }
    }

    private boolean isTableWithDatabaseName(String tableName) {
        boolean ret = false;
        if (tableName.contains(".")) {
            ret = true;
        }
        return ret;
    }
}
