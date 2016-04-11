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

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.fs.model.FSDataModel;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    public static final String TABLE_TYPE_ATTR = "tableType";
    public static final String SEARCH_ENTRY_GUID_ATTR = "__guid";
    public static final String LAST_ACCESS_TIME_ATTR = "lastAccessTime";
    private final String clusterName;

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

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
    public HiveMetaStoreBridge(HiveConf hiveConf) throws Exception {
        this(hiveConf.get(HIVE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME), Hive.get(hiveConf), null);
    }

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf {@link HiveConf} for Hive component in the cluster
     */
    public HiveMetaStoreBridge(HiveConf hiveConf, AtlasClient atlasClient) throws Exception {
        this(hiveConf.get(HIVE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME), Hive.get(hiveConf), atlasClient);
    }

    AtlasClient getAtlasClient() {
        return atlasClient;
    }

    void importHiveMetadata() throws Exception {
        LOG.info("Importing hive metadata");
        importDatabases();
    }

    private void importDatabases() throws Exception {
        List<String> databases = hiveClient.getAllDatabases();
        for (String databaseName : databases) {
            Referenceable dbReference = registerDatabase(databaseName);

            importTables(dbReference, databaseName);
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
        if (dbRef == null) {
            dbRef = createDBInstance(db);
            dbRef = registerInstance(dbRef);
        } else {
            LOG.info("Database {} is already registered with id {}. Updating it.", databaseName, dbRef.getId().id);
            dbRef = createOrUpdateDBInstance(db, dbRef);
            updateInstance(dbRef);
        }
        return dbRef;
    }

    private Referenceable createOrUpdateDBInstance(Database hiveDB, Referenceable dbRef) {
        LOG.info("Importing objects from databaseName : " + hiveDB.getName());

        if (dbRef == null) {
            dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
        }
        String dbName = hiveDB.getName().toLowerCase();
        dbRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getDBQualifiedName(clusterName, dbName));
        dbRef.set(HiveDataModelGenerator.NAME, dbName);
        dbRef.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        dbRef.set(DESCRIPTION_ATTR, hiveDB.getDescription());
        dbRef.set("locationUri", hiveDB.getLocationUri());
        dbRef.set(HiveDataModelGenerator.PARAMETERS, hiveDB.getParameters());
        dbRef.set(HiveDataModelGenerator.OWNER, hiveDB.getOwnerName());
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
        LOG.debug("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOG.debug("Submitting new entity {} = {}", referenceable.getTypeName(), entityJSON);
        JSONArray guids = getAtlasClient().createEntity(entityJSON);
        LOG.debug("created instance for type " + typeName + ", guid: " + guids);

        return new Referenceable(guids.getString(0), referenceable.getTypeName(), null);
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

        String dslQuery = getDatabaseDSLQuery(clusterName, databaseName, typeName);
        return getEntityReferenceFromDSL(typeName, dslQuery);
    }

    static String getDatabaseDSLQuery(String clusterName, String databaseName, String typeName) {
        return String.format("%s where %s = '%s' and %s = '%s'", typeName, HiveDataModelGenerator.NAME,
                databaseName.toLowerCase(), AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
    }

    private Referenceable getEntityReferenceFromDSL(String typeName, String dslQuery) throws Exception {
        AtlasClient dgiClient = getAtlasClient();
        JSONArray results = dgiClient.searchByDSL(dslQuery);
        if (results.length() == 0) {
            return null;
        } else {
            String guid;
            JSONObject row = results.getJSONObject(0);
            if (row.has("$id$")) {
                guid = row.getJSONObject("$id$").getString("id");
            } else {
                guid = row.getJSONObject("_col_0").getString("id");
            }
            return new Referenceable(guid, typeName, null);
        }
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
     * Imports all tables for the given db
     * @param databaseName
     * @param databaseReferenceable
     * @throws Exception
     */
    private void importTables(Referenceable databaseReferenceable, String databaseName) throws Exception {
        List<String> hiveTables = hiveClient.getAllTables(databaseName);

        for (String tableName : hiveTables) {
            Table table = hiveClient.getTable(databaseName, tableName);
            Referenceable tableReferenceable = registerTable(databaseReferenceable, table);
        }
    }

    /**
     * Gets reference for the table
     *
     * @param dbName database name
     * @param tableName table name
     * @return table reference if exists, else null
     * @throws Exception
     */
    private Referenceable getTableReference(String dbName, String tableName) throws Exception {
        LOG.debug("Getting reference for table {}.{}", dbName, tableName);

        String typeName = HiveDataTypes.HIVE_TABLE.getName();
        String dslQuery = getTableDSLQuery(getClusterName(), dbName, tableName, typeName);
        return getEntityReferenceFromDSL(typeName, dslQuery);
    }

    static String getTableDSLQuery(String clusterName, String dbName, String tableName, String typeName) {
        String entityName = getTableQualifiedName(clusterName, dbName, tableName);
        return String.format("%s as t where name = '%s'", typeName, entityName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hive component belongs
     * @param dbName Name of the Hive database to which the Table belongs
     * @param tableName Name of the Hive table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String clusterName, String dbName, String tableName) {
        return String.format("%s.%s@%s", dbName.toLowerCase(), tableName.toLowerCase(), clusterName);
    }

    /**
     * Create a new table instance in Atlas
     * @param dbReference reference to a created Hive database {@link Referenceable} to which this table belongs
     * @param hiveTable reference to the Hive {@link Table} from which to map properties
     * @return Newly created Hive reference
     * @throws Exception
     */
    public Referenceable createTableInstance(Referenceable dbReference, Table hiveTable)
            throws Exception {
        return createOrUpdateTableInstance(dbReference, null, hiveTable);
    }

    private Referenceable createOrUpdateTableInstance(Referenceable dbReference, Referenceable tableReference,
                                                      final Table hiveTable) throws Exception {
        LOG.info("Importing objects from {}.{}", hiveTable.getDbName(), hiveTable.getTableName());

        if (tableReference == null) {
            tableReference = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        }
        String tableQualifiedName = getTableQualifiedName(clusterName, hiveTable.getDbName(), hiveTable.getTableName());
        tableReference.set(HiveDataModelGenerator.NAME, tableQualifiedName);
        tableReference.set(HiveDataModelGenerator.TABLE_NAME, hiveTable.getTableName().toLowerCase());
        tableReference.set("owner", hiveTable.getOwner());

        tableReference.set("createTime", hiveTable.getMetadata().getProperty(hive_metastoreConstants.DDL_TIME));
        tableReference.set("lastAccessTime", hiveTable.getLastAccessTime());
        tableReference.set("retention", hiveTable.getRetention());

        tableReference.set(HiveDataModelGenerator.COMMENT, hiveTable.getParameters().get(HiveDataModelGenerator.COMMENT));

        // add reference to the database
        tableReference.set(HiveDataModelGenerator.DB, dbReference);

        tableReference.set(HiveDataModelGenerator.COLUMNS, getColumns(hiveTable.getCols(), tableQualifiedName));

        // add reference to the StorageDescriptor
        Referenceable sdReferenceable = fillStorageDesc(hiveTable.getSd(), tableQualifiedName, getStorageDescQFName(tableQualifiedName));
        tableReference.set("sd", sdReferenceable);

        // add reference to the Partition Keys
        List<Referenceable> partKeys = getColumns(hiveTable.getPartitionKeys(), tableQualifiedName);
        tableReference.set("partitionKeys", partKeys);

        tableReference.set(HiveDataModelGenerator.PARAMETERS, hiveTable.getParameters());

        if (hiveTable.getViewOriginalText() != null) {
            tableReference.set("viewOriginalText", hiveTable.getViewOriginalText());
        }

        if (hiveTable.getViewExpandedText() != null) {
            tableReference.set("viewExpandedText", hiveTable.getViewExpandedText());
        }

        tableReference.set(TABLE_TYPE_ATTR, hiveTable.getTableType().name());
        tableReference.set("temporary", hiveTable.isTemporary());

        return tableReference;
    }

    private String getStorageDescQFName(String entityQualifiedName) {
        return entityQualifiedName + "_storage";
    }

    private Referenceable registerTable(Referenceable dbReference, Table table) throws Exception {
        String dbName = table.getDbName();
        String tableName = table.getTableName();
        LOG.info("Attempting to register table [" + tableName + "]");
        Referenceable tableReference = getTableReference(dbName, tableName);
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
    }

    private void updateInstance(Referenceable referenceable) throws AtlasServiceException {
        String typeName = referenceable.getTypeName();
        LOG.debug("updating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOG.debug("Updating entity {} = {}", referenceable.getTypeName(), entityJSON);

        atlasClient.updateEntity(referenceable.getId().id, referenceable);
    }


    private Referenceable getEntityReferenceFromGremlin(String typeName, String gremlinQuery)
    throws AtlasServiceException, JSONException {
        AtlasClient client = getAtlasClient();
        JSONArray results = client.searchByGremlin(gremlinQuery);
        if (results.length() == 0) {
            return null;
        }
        String guid = results.getJSONObject(0).getString(SEARCH_ENTRY_GUID_ATTR);
        return new Referenceable(guid, typeName, null);
    }

    private Referenceable getSDForTable(String dbName, String tableName) throws Exception {
        Referenceable tableRef = getTableReference(dbName, tableName);
        if (tableRef == null) {
            throw new IllegalArgumentException("Table " + dbName + "." + tableName + " doesn't exist");
        }

        AtlasClient dgiClient = getAtlasClient();
        Referenceable tableInstance = dgiClient.getEntity(tableRef.getId().id);
        Referenceable sd = (Referenceable) tableInstance.get("sd");
        return new Referenceable(sd.getId().id, sd.getTypeName(), null);
    }

    public Referenceable fillStorageDesc(StorageDescriptor storageDesc, String tableQualifiedName,
        String sdQualifiedName) throws Exception {
        LOG.debug("Filling storage descriptor information for " + storageDesc);

        Referenceable sdReferenceable = new Referenceable(HiveDataTypes.HIVE_STORAGEDESC.getName());
        sdReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sdQualifiedName);

        SerDeInfo serdeInfo = storageDesc.getSerdeInfo();
        LOG.debug("serdeInfo = " + serdeInfo);
        // SkewedInfo skewedInfo = storageDesc.getSkewedInfo();

        String serdeInfoName = HiveDataTypes.HIVE_SERDE.getName();
        Struct serdeInfoStruct = new Struct(serdeInfoName);

        serdeInfoStruct.set(HiveDataModelGenerator.NAME, serdeInfo.getName());
        serdeInfoStruct.set("serializationLib", serdeInfo.getSerializationLib());
        serdeInfoStruct.set(HiveDataModelGenerator.PARAMETERS, serdeInfo.getParameters());

        sdReferenceable.set("serdeInfo", serdeInfoStruct);
        sdReferenceable.set(HiveDataModelGenerator.STORAGE_NUM_BUCKETS, storageDesc.getNumBuckets());
        sdReferenceable
                .set(HiveDataModelGenerator.STORAGE_IS_STORED_AS_SUB_DIRS, storageDesc.isStoredAsSubDirectories());

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

        sdReferenceable.set("location", storageDesc.getLocation());
        sdReferenceable.set("inputFormat", storageDesc.getInputFormat());
        sdReferenceable.set("outputFormat", storageDesc.getOutputFormat());
        sdReferenceable.set("compressed", storageDesc.isCompressed());

        if (storageDesc.getBucketCols().size() > 0) {
            sdReferenceable.set("bucketCols", storageDesc.getBucketCols());
        }

        sdReferenceable.set(HiveDataModelGenerator.PARAMETERS, storageDesc.getParameters());
        sdReferenceable.set("storedAsSubDirectories", storageDesc.isStoredAsSubDirectories());

        return sdReferenceable;
    }

    public Referenceable fillHDFSDataSet(String pathUri) {
        Referenceable ref = new Referenceable(FSDataTypes.HDFS_PATH().toString());
        ref.set("path", pathUri);
//        Path path = new Path(pathUri);
//        ref.set("name", path.getName());
        //TODO - Fix after ATLAS-542 to shorter Name
        ref.set("name", pathUri);
        ref.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pathUri);
        return ref;
    }

    public static String getColumnQualifiedName(final String tableQualifiedName, final String colName) {
        final String[] parts = tableQualifiedName.split("@");
        final String tableName = parts[0];
        final String clusterName = parts[1];
        return String.format("%s.%s@%s", tableName, colName, clusterName);
    }

    public List<Referenceable> getColumns(List<FieldSchema> schemaList, String tableQualifiedName) throws Exception {
        List<Referenceable> colList = new ArrayList<>();
        for (FieldSchema fs : schemaList) {
            LOG.debug("Processing field " + fs);
            Referenceable colReferenceable = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
            colReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                    getColumnQualifiedName(tableQualifiedName, fs.getName()));
            colReferenceable.set(HiveDataModelGenerator.NAME, fs.getName());
            colReferenceable.set("type", fs.getType());
            colReferenceable.set(HiveDataModelGenerator.COMMENT, fs.getComment());

            colList.add(colReferenceable);
        }
        return colList;
    }

    /**
     * Register the Hive DataModel in Atlas, if not already defined.
     *
     * The method checks for the presence of the type {@link HiveDataTypes#HIVE_PROCESS} with the Atlas server.
     * If this type is defined, then we assume the Hive DataModel is registered.
     * @throws Exception
     */
    public synchronized void registerHiveDataModel() throws Exception {
        HiveDataModelGenerator dataModelGenerator = new HiveDataModelGenerator();
        AtlasClient dgiClient = getAtlasClient();

        try {
            dgiClient.getType(FSDataTypes.HDFS_PATH().toString());
            LOG.info("HDFS data model is already registered!");
        } catch(AtlasServiceException ase) {
            if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                //Trigger val definition
                FSDataModel.main(null);

                final String hdfsModelJson = TypesSerialization.toJson(FSDataModel.typesDef());
                //Expected in case types do not exist
                LOG.info("Registering HDFS data model : " + hdfsModelJson);
                dgiClient.createType(hdfsModelJson);
            }
        }

        try {
            dgiClient.getType(HiveDataTypes.HIVE_PROCESS.getName());
            LOG.info("Hive data model is already registered!");
        } catch(AtlasServiceException ase) {
            if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                //Expected in case types do not exist
                LOG.info("Registering Hive data model");
                dgiClient.createType(dataModelGenerator.getModelAsJson());
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Configuration atlasConf = ApplicationProperties.get();
        String atlasEndpoint = atlasConf.getString(ATLAS_ENDPOINT, DEFAULT_DGI_URL);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        AtlasClient atlasClient = new AtlasClient(atlasEndpoint, ugi, ugi.getShortUserName());

        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(new HiveConf(), atlasClient);
        hiveMetaStoreBridge.registerHiveDataModel();
        hiveMetaStoreBridge.importHiveMetadata();
    }
}
