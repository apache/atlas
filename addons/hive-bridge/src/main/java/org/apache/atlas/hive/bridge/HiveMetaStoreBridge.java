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
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.hook.HiveHook;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A Bridge Utility that imports metadata from the Hive Meta Store
 * and registers them in Atlas.
 */
public class HiveMetaStoreBridge {
    private static final String DEFAULT_DGI_URL = "http://localhost:21000/";
    public static final String HIVE_CLUSTER_NAME = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "primary";
    public static final String DESCRIPTION_ATTR = "description";

    public static final String TEMP_TABLE_PREFIX = "_temp-";

    private final String clusterName;
    public static final long MILLIS_CONVERT_FACTOR = 1000;

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

    public static final String COMMENT = "comment";
    public static final String PARAMETERS = "parameters";
    public static final String COLUMNS = "columns";
    public static final String POSITION = "position";
    public static final String PART_COLS = "partitionKeys";
    public static final String TABLE_ALIAS_LIST = "aliases";
    public static final String STORAGE_NUM_BUCKETS = "numBuckets";
    public static final String STORAGE_IS_STORED_AS_SUB_DIRS = "storedAsSubDirectories";
    public static final String TABLE = "table";
    public static final String DB = "db";
    public static final String STORAGE_DESC = "sd";
    public static final String STORAGE_DESC_INPUT_FMT = "inputFormat";
    public static final String STORAGE_DESC_OUTPUT_FMT = "outputFormat";
    public static final String LOCATION = "location";
    public static final String TABLE_TYPE_ATTR = "tableType";
    public static final String CREATE_TIME = "createTime";
    public static final String LAST_ACCESS_TIME = "lastAccessTime";
    public static final String HDFS_PATH = "hdfs_path";

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreBridge.class);

    public final Hive hiveClient;
    private AtlasClient atlasClient = null;

    HiveMetaStoreBridge(String clusterName, Hive hiveClient, AtlasClient atlasClient) {
        this.clusterName = clusterName;
        this.hiveClient = hiveClient;
        this.atlasClient = atlasClient;
    }

    public String getClusterName() {
        return clusterName;
    }

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf {@link HiveConf} for Hive component in the cluster
     */
    public HiveMetaStoreBridge(Configuration atlasProperties, HiveConf hiveConf) throws Exception {
        this(atlasProperties, hiveConf, null);
    }

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf {@link HiveConf} for Hive component in the cluster
     */
    public HiveMetaStoreBridge(Configuration atlasProperties, HiveConf hiveConf, AtlasClient atlasClient) throws Exception {
        this(atlasProperties.getString(HIVE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME), Hive.get(hiveConf), atlasClient);
    }

    AtlasClient getAtlasClient() {
        return atlasClient;
    }

    void importHiveMetadata(boolean failOnError) throws Exception {
        LOG.info("Importing hive metadata");
        importDatabases(failOnError);
    }

    private void importDatabases(boolean failOnError) throws Exception {
        List<String> databases = hiveClient.getAllDatabases();
        for (String databaseName : databases) {
            Referenceable dbReference = registerDatabase(databaseName);

            if (dbReference != null) {
                importTables(dbReference, databaseName, failOnError);
            }
        }
    }

    /**
     * Create a Hive Database entity
     * @param hiveDB The Hive {@link Database} object from which to map properties
     * @return new Hive Database entity
     * @throws HiveException
     */
    public Referenceable createDBInstance(Database hiveDB) throws HiveException {
        return createOrUpdateDBInstance(hiveDB, null);
    }

    /**
     * Checks if db is already registered, else creates and registers db entity
     * @param databaseName
     * @return
     * @throws Exception
     */
    private Referenceable registerDatabase(String databaseName) throws Exception {
        Referenceable dbRef = getDatabaseReference(clusterName, databaseName);
        Database db = hiveClient.getDatabase(databaseName);

        if (db != null) {
            if (dbRef == null) {
                dbRef = createDBInstance(db);
                dbRef = registerInstance(dbRef);
            } else {
                LOG.info("Database {} is already registered with id {}. Updating it.", databaseName, dbRef.getId().id);
                dbRef = createOrUpdateDBInstance(db, dbRef);
                updateInstance(dbRef);
            }
        }
        return dbRef;
    }

    private Referenceable createOrUpdateDBInstance(Database hiveDB, Referenceable dbRef) {
        LOG.info("Importing objects from databaseName : {}", hiveDB.getName());

        if (dbRef == null) {
            dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
        }
        String dbName = hiveDB.getName().toLowerCase();
        dbRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getDBQualifiedName(clusterName, dbName));
        dbRef.set(AtlasClient.NAME, dbName);
        dbRef.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        dbRef.set(DESCRIPTION_ATTR, hiveDB.getDescription());
        dbRef.set(LOCATION, hiveDB.getLocationUri());
        dbRef.set(PARAMETERS, hiveDB.getParameters());
        dbRef.set(AtlasClient.OWNER, hiveDB.getOwnerName());
        if (hiveDB.getOwnerType() != null) {
            dbRef.set("ownerType", hiveDB.getOwnerType().getValue());
        }
        return dbRef;
    }

    /**
     * Registers an entity in atlas
     * @param referenceable
     * @return
     * @throws Exception
     */
    private Referenceable registerInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        LOG.debug("creating instance of type {}", typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOG.debug("Submitting new entity {} = {}", referenceable.getTypeName(), entityJSON);
        List<String> guids = getAtlasClient().createEntity(entityJSON);
        LOG.debug("created instance for type {}, guid: {}", typeName, guids);

        return new Referenceable(guids.get(guids.size() - 1), referenceable.getTypeName(), null);
    }

    /**
     * Gets reference to the atlas entity for the database
     * @param databaseName  database Name
     * @param clusterName    cluster name
     * @return Reference for database if exists, else null
     * @throws Exception
     */
    private Referenceable getDatabaseReference(String clusterName, String databaseName) throws Exception {
        LOG.debug("Getting reference for database {}", databaseName);
        String typeName = HiveDataTypes.HIVE_DB.getName();

        return getEntityReference(typeName, getDBQualifiedName(clusterName, databaseName));
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

    private String getCreateTableString(Table table, String location){
        String colString = "";
        List<FieldSchema> colList = table.getAllCols();
        if ( colList != null) {
            for (FieldSchema col : colList) {
                colString += col.getName() + " " + col.getType() + ",";
            }
            if (colList.size() > 0) {
                colString = colString.substring(0, colString.length() - 1);
                colString = "(" + colString + ")";
            }
        }
        String query = "create external table " + table.getTableName() +  colString +
                " location '" + location + "'";
        return query;
    }

    /**
     * Imports all tables for the given db
     * @param databaseReferenceable
     * @param databaseName
     * @param failOnError
     * @throws Exception
     */
    private int importTables(Referenceable databaseReferenceable, String databaseName, final boolean failOnError) throws Exception {
        int tablesImported = 0;
        List<String> hiveTables = hiveClient.getAllTables(databaseName);
        LOG.info("Importing tables {} for db {}", hiveTables.toString(), databaseName);
        for (String tableName : hiveTables) {
            int imported = importTable(databaseReferenceable, databaseName, tableName, failOnError);
            tablesImported += imported;
        }

        if (tablesImported == hiveTables.size()) {
            LOG.info("Successfully imported all {} tables from {} ", tablesImported, databaseName);
        } else {
            LOG.error("Able to import {} tables out of {} tables from {}. Please check logs for import errors", tablesImported, hiveTables.size(), databaseName);
        }

        return tablesImported;
    }

    @VisibleForTesting
    public int importTable(Referenceable databaseReferenceable, String databaseName, String tableName, final boolean failOnError) throws Exception {
        try {
            Table table = hiveClient.getTable(databaseName, tableName);
            Referenceable tableReferenceable = registerTable(databaseReferenceable, table);
            if (table.getTableType() == TableType.EXTERNAL_TABLE) {
                String tableQualifiedName = getTableProcessQualifiedName(clusterName, table);
                Referenceable process = getProcessReference(tableQualifiedName);
                if (process == null) {
                    LOG.info("Attempting to register create table process for {}", tableQualifiedName);
                    Referenceable lineageProcess = new Referenceable(HiveDataTypes.HIVE_PROCESS.getName());
                    ArrayList<Referenceable> sourceList = new ArrayList<>();
                    ArrayList<Referenceable> targetList = new ArrayList<>();
                    String tableLocation = table.getDataLocation().toString();
                    Referenceable path = fillHDFSDataSet(tableLocation);
                    String query = getCreateTableString(table, tableLocation);
                    sourceList.add(path);
                    targetList.add(tableReferenceable);
                    lineageProcess.set("inputs", sourceList);
                    lineageProcess.set("outputs", targetList);
                    lineageProcess.set("userName", table.getOwner());
                    lineageProcess.set("startTime", new Date(System.currentTimeMillis()));
                    lineageProcess.set("endTime", new Date(System.currentTimeMillis()));
                    lineageProcess.set("operationType", "CREATETABLE");
                    lineageProcess.set("queryText", query);
                    lineageProcess.set("queryId", query);
                    lineageProcess.set("queryPlan", "{}");
                    lineageProcess.set("clusterName", clusterName);
                    List<String> recentQueries = new ArrayList<>(1);
                    recentQueries.add(query);
                    lineageProcess.set("recentQueries", recentQueries);
                    String processQualifiedName = getTableProcessQualifiedName(clusterName, table);
                    lineageProcess.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processQualifiedName);
                    lineageProcess.set(AtlasClient.NAME, query);
                    registerInstance(lineageProcess);
                } else {
                    LOG.info("Process {} is already registered", process.toString());
                }
            }
            return 1;
        } catch (Exception e) {
            LOG.error("Import failed for hive_table {} ", tableName, e);
            if (failOnError) {
                throw e;
            }
            return 0;
        }
    }

    /**
     * Gets reference for the table
     *
     * @param hiveTable
     * @return table reference if exists, else null
     * @throws Exception
     */
    private Referenceable getTableReference(Table hiveTable)  throws Exception {
        LOG.debug("Getting reference for table {}.{}", hiveTable.getDbName(), hiveTable.getTableName());

        String typeName = HiveDataTypes.HIVE_TABLE.getName();
        String tblQualifiedName = getTableQualifiedName(getClusterName(), hiveTable.getDbName(), hiveTable.getTableName());
        return getEntityReference(typeName, tblQualifiedName);
    }

    private Referenceable getEntityReference(final String typeName, final String tblQualifiedName) throws AtlasServiceException {
        AtlasClient dgiClient = getAtlasClient();
        try {
            return dgiClient.getEntity(typeName, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tblQualifiedName);
        } catch (AtlasServiceException e) {
            if(e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }
            throw e;
        }
    }

    private Referenceable getProcessReference(String qualifiedName) throws Exception{
        LOG.debug("Getting reference for process {}", qualifiedName);
        String typeName = HiveDataTypes.HIVE_PROCESS.getName();
        return getEntityReference(typeName, qualifiedName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param dbName Name of the Hive database to which the Table belongs
     * @param tableName Name of the Hive table
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



    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param table hive table for which the qualified name is needed
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String clusterName, Table table) {
        return getTableQualifiedName(clusterName, table.getDbName(), table.getTableName(), table.isTemporary());
    }

    public static String getTableProcessQualifiedName(String clusterName, Table table) {
        String tableQualifiedName = getTableQualifiedName(clusterName, table);
        Date createdTime = getTableCreatedTime(table);
        return tableQualifiedName + HiveHook.SEP + createdTime.getTime();
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

    /**
     * Create a new table instance in Atlas
     * @param dbReference reference to a created Hive database {@link Referenceable} to which this table belongs
     * @param hiveTable reference to the Hive {@link Table} from which to map properties
     * @return Newly created Hive reference
     * @throws Exception
     */
    public Referenceable createTableInstance(Referenceable dbReference, Table hiveTable)
            throws AtlasHookException {
        return createOrUpdateTableInstance(dbReference, null, hiveTable);
    }

    public static Date getTableCreatedTime(Table table) {
        return new Date(table.getTTable().getCreateTime() * MILLIS_CONVERT_FACTOR);
    }

    private Referenceable createOrUpdateTableInstance(Referenceable dbReference, Referenceable tableReference,
                                                      final Table hiveTable) throws AtlasHookException {
        LOG.info("Importing objects from {}.{}", hiveTable.getDbName(), hiveTable.getTableName());

        if (tableReference == null) {
            tableReference = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        }

        String tableQualifiedName = getTableQualifiedName(clusterName, hiveTable);
        tableReference.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableQualifiedName);
        tableReference.set(AtlasClient.NAME, hiveTable.getTableName().toLowerCase());
        tableReference.set(AtlasClient.OWNER, hiveTable.getOwner());

        Date createDate = new Date();
        if (hiveTable.getTTable() != null){
            try {
                createDate = getTableCreatedTime(hiveTable);
                LOG.debug("Setting create time to {} ", createDate);
                tableReference.set(CREATE_TIME, createDate);
            } catch(Exception ne) {
                LOG.error("Error while setting createTime for the table {} ", hiveTable.getCompleteName(), ne);
            }
        }

        Date lastAccessTime = createDate;
        if ( hiveTable.getLastAccessTime() > 0) {
            lastAccessTime = new Date(hiveTable.getLastAccessTime() * MILLIS_CONVERT_FACTOR);
        }
        tableReference.set(LAST_ACCESS_TIME, lastAccessTime);
        tableReference.set("retention", hiveTable.getRetention());

        tableReference.set(COMMENT, hiveTable.getParameters().get(COMMENT));

        // add reference to the database
        tableReference.set(DB, dbReference);

        // add reference to the StorageDescriptor
        Referenceable sdReferenceable = fillStorageDesc(hiveTable.getSd(), tableQualifiedName, getStorageDescQFName(tableQualifiedName), tableReference.getId());
        tableReference.set(STORAGE_DESC, sdReferenceable);

        tableReference.set(PARAMETERS, hiveTable.getParameters());

        if (hiveTable.getViewOriginalText() != null) {
            tableReference.set("viewOriginalText", hiveTable.getViewOriginalText());
        }

        if (hiveTable.getViewExpandedText() != null) {
            tableReference.set("viewExpandedText", hiveTable.getViewExpandedText());
        }

        tableReference.set(TABLE_TYPE_ATTR, hiveTable.getTableType().name());
        tableReference.set("temporary", hiveTable.isTemporary());

        // add reference to the Partition Keys
        List<Referenceable> partKeys = getColumns(hiveTable.getPartitionKeys(), tableReference);
        tableReference.set("partitionKeys", partKeys);

        tableReference.set(COLUMNS, getColumns(hiveTable.getCols(), tableReference));

        return tableReference;
    }

    public static String getStorageDescQFName(String entityQualifiedName) {
        return entityQualifiedName + "_storage";
    }

    private Referenceable registerTable(Referenceable dbReference, Table table) throws AtlasHookException {
        try {
            String dbName = table.getDbName();
            String tableName = table.getTableName();
            LOG.info("Attempting to register table [{}]", tableName);
            Referenceable tableReference = getTableReference(table);
            LOG.info("Found result {}", tableReference);
            if (tableReference == null) {
                tableReference = createTableInstance(dbReference, table);
                tableReference = registerInstance(tableReference);
            } else {
                LOG.info("Table {}.{} is already registered with id {}. Updating entity.", dbName, tableName,
                        tableReference.getId().id);
                tableReference = createOrUpdateTableInstance(dbReference, tableReference, table);
                updateInstance(tableReference);
            }
            return tableReference;
        } catch (Exception e) {
            throw new AtlasHookException("HiveMetaStoreBridge.getStorageDescQFName() failed.", e);
        }
    }

    private void updateInstance(Referenceable referenceable) throws AtlasServiceException {
        String typeName = referenceable.getTypeName();
        LOG.debug("updating instance of type {}", typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOG.debug("Updating entity {} = {}", referenceable.getTypeName(), entityJSON);

        atlasClient.updateEntity(referenceable.getId().id, referenceable);
    }

    public Referenceable fillStorageDesc(StorageDescriptor storageDesc, String tableQualifiedName,
        String sdQualifiedName, Id tableId) throws AtlasHookException {
        LOG.debug("Filling storage descriptor information for {}", storageDesc);

        Referenceable sdReferenceable = new Referenceable(HiveDataTypes.HIVE_STORAGEDESC.getName());
        sdReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sdQualifiedName);

        SerDeInfo serdeInfo = storageDesc.getSerdeInfo();
        LOG.debug("serdeInfo = {}", serdeInfo);
        // SkewedInfo skewedInfo = storageDesc.getSkewedInfo();

        String serdeInfoName = HiveDataTypes.HIVE_SERDE.getName();
        Struct serdeInfoStruct = new Struct(serdeInfoName);

        serdeInfoStruct.set(AtlasClient.NAME, serdeInfo.getName());
        serdeInfoStruct.set("serializationLib", serdeInfo.getSerializationLib());
        serdeInfoStruct.set(PARAMETERS, serdeInfo.getParameters());

        sdReferenceable.set("serdeInfo", serdeInfoStruct);
        sdReferenceable.set(STORAGE_NUM_BUCKETS, storageDesc.getNumBuckets());
        sdReferenceable
                .set(STORAGE_IS_STORED_AS_SUB_DIRS, storageDesc.isStoredAsSubDirectories());

        List<Struct> sortColsStruct = new ArrayList<>();
        for (Order sortcol : storageDesc.getSortCols()) {
            String hiveOrderName = HiveDataTypes.HIVE_ORDER.getName();
            Struct colStruct = new Struct(hiveOrderName);
            colStruct.set("col", sortcol.getCol());
            colStruct.set("order", sortcol.getOrder());

            sortColsStruct.add(colStruct);
        }
        if (sortColsStruct.size() > 0) {
            sdReferenceable.set("sortCols", sortColsStruct);
        }

        sdReferenceable.set(LOCATION, storageDesc.getLocation());
        sdReferenceable.set("inputFormat", storageDesc.getInputFormat());
        sdReferenceable.set("outputFormat", storageDesc.getOutputFormat());
        sdReferenceable.set("compressed", storageDesc.isCompressed());

        if (storageDesc.getBucketCols().size() > 0) {
            sdReferenceable.set("bucketCols", storageDesc.getBucketCols());
        }

        sdReferenceable.set(PARAMETERS, storageDesc.getParameters());
        sdReferenceable.set("storedAsSubDirectories", storageDesc.isStoredAsSubDirectories());
        sdReferenceable.set(TABLE, tableId);

        return sdReferenceable;
    }

    public Referenceable fillHDFSDataSet(String pathUri) {
        Referenceable ref = new Referenceable(HDFS_PATH);
        ref.set("path", pathUri);
        Path path = new Path(pathUri);
        ref.set(AtlasClient.NAME, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());
        ref.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pathUri);
        return ref;
    }

    public static String getColumnQualifiedName(final String tableQualifiedName, final String colName) {
        final String[] parts = tableQualifiedName.split("@");
        final String tableName = parts[0];
        final String clusterName = parts[1];
        return String.format("%s.%s@%s", tableName, colName.toLowerCase(), clusterName);
    }

    public List<Referenceable> getColumns(List<FieldSchema> schemaList, Referenceable tableReference) throws AtlasHookException {
        List<Referenceable> colList = new ArrayList<>();
        int columnPosition = 0;
        for (FieldSchema fs : schemaList) {
            LOG.debug("Processing field {}", fs);
            Referenceable colReferenceable = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
            colReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                    getColumnQualifiedName((String) tableReference.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), fs.getName()));
            colReferenceable.set(AtlasClient.NAME, fs.getName());
            colReferenceable.set(AtlasClient.OWNER, tableReference.get(AtlasClient.OWNER));
            colReferenceable.set("type", fs.getType());
            colReferenceable.set(POSITION, columnPosition++);
            colReferenceable.set(COMMENT, fs.getComment());
            colReferenceable.set(TABLE, tableReference.getId());


            colList.add(colReferenceable);
        }
        return colList;
    }


    public static void main(String[] args) throws AtlasHookException {
        try {
        Configuration atlasConf = ApplicationProperties.get();
        String[] atlasEndpoint = atlasConf.getStringArray(ATLAS_ENDPOINT);
        if (atlasEndpoint == null || atlasEndpoint.length == 0){
            atlasEndpoint = new String[] { DEFAULT_DGI_URL };
        }
        AtlasClient atlasClient;

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
            atlasClient = new AtlasClient(atlasEndpoint, basicAuthUsernamePassword);
        } else {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            atlasClient = new AtlasClient(ugi, ugi.getShortUserName(), atlasEndpoint);
        }

        Options options = new Options();
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse( options, args);

        boolean failOnError = false;
        if (cmd.hasOption("failOnError")) {
            failOnError = true;
        }

        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(atlasConf, new HiveConf(), atlasClient);
        hiveMetaStoreBridge.importHiveMetadata(failOnError);
        }
        catch(Exception e) {
            throw new AtlasHookException("HiveMetaStoreBridge.main() failed.", e);
        }
    }
}
