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

package org.apache.hadoop.metadata.hive.bridge;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.hive.model.HiveDataModelGenerator;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.Struct;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization;
import org.apache.hadoop.metadata.typesystem.persistence.Id;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A Bridge Utility that imports metadata from the Hive Meta Store
 * and registers then in DGI.
 */
public class HiveMetaStoreBridge {
    private static final String DEFAULT_DGI_URL = "http://localhost:21000/";
    public static final String HIVE_CLUSTER_NAME = "hive.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "primary";
    private final String clusterName;

    public static final String DGI_URL_PROPERTY = "hive.hook.dgi.url";

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreBridge.class);

    private final Hive hiveClient;
    private final MetadataServiceClient metadataServiceClient;

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf
     */
    public HiveMetaStoreBridge(HiveConf hiveConf) throws Exception {
        clusterName = hiveConf.get(HIVE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
        hiveClient = Hive.get(hiveConf);
        metadataServiceClient = new MetadataServiceClient(hiveConf.get(DGI_URL_PROPERTY, DEFAULT_DGI_URL));
    }

    public MetadataServiceClient getMetadataServiceClient() {
        return metadataServiceClient;
    }

    public void importHiveMetadata() throws Exception {
        LOG.info("Importing hive metadata");
        importDatabases();
    }

    private void importDatabases() throws Exception {
        List<String> databases = hiveClient.getAllDatabases();
        for (String databaseName : databases) {
            Referenceable dbReference = registerDatabase(databaseName);

            importTables(databaseName, dbReference);
        }
    }

    /**
     * Gets reference for the database
     *
     *
     * @param databaseName
     * @param clusterName    cluster name
     * @return Reference for database if exists, else null
     * @throws Exception
     */
    private Referenceable getDatabaseReference(String databaseName, String clusterName) throws Exception {
        LOG.debug("Getting reference for database {}", databaseName);
        String typeName = HiveDataTypes.HIVE_DB.getName();
        MetadataServiceClient dgiClient = getMetadataServiceClient();

        String dslQuery = String.format("%s where name = '%s' and clusterName = '%s'",
                HiveDataTypes.HIVE_DB.getName(), databaseName, clusterName);
        JSONArray results = dgiClient.searchByDSL(dslQuery);
        if (results.length() == 0) {
            return null;
        } else {
            String guid = getGuidFromDSLResponse(results.getJSONObject(0));
            return new Referenceable(guid, typeName, null);
        }
    }

    public Referenceable registerDatabase(String databaseName) throws Exception {
        Referenceable dbRef = getDatabaseReference(databaseName, clusterName);
        if (dbRef == null) {
            LOG.info("Importing objects from databaseName : " + databaseName);
            Database hiveDB = hiveClient.getDatabase(databaseName);

            dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
            dbRef.set("name", hiveDB.getName());
            dbRef.set("clusterName", clusterName);
            dbRef.set("description", hiveDB.getDescription());
            dbRef.set("locationUri", hiveDB.getLocationUri());
            dbRef.set("parameters", hiveDB.getParameters());
            dbRef.set("ownerName", hiveDB.getOwnerName());
            if (hiveDB.getOwnerType() != null) {
                dbRef.set("ownerType", hiveDB.getOwnerType().getValue());
            }

            dbRef = createInstance(dbRef);
        } else {
            LOG.info("Database {} is already registered with id {}", databaseName, dbRef.getId().id);
        }
        return dbRef;
    }

    public Referenceable createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        LOG.debug("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOG.debug("Submitting new entity {} = {}", referenceable.getTypeName(), entityJSON);
        JSONObject jsonObject = metadataServiceClient.createEntity(entityJSON);
        String guid = jsonObject.getString(MetadataServiceClient.GUID);
        LOG.debug("created instance for type " + typeName + ", guid: " + guid);

        return new Referenceable(guid, referenceable.getTypeName(), null);
    }

    private void importTables(String databaseName, Referenceable databaseReferenceable) throws Exception {
        List<String> hiveTables = hiveClient.getAllTables(databaseName);

        for (String tableName : hiveTables) {
            Referenceable tableReferenceable = registerTable(databaseReferenceable, databaseName, tableName);

            // Import Partitions
            Referenceable sdReferenceable = getSDForTable(databaseName, tableName);
            importPartitions(databaseName, tableName, databaseReferenceable, tableReferenceable, sdReferenceable);

            // Import Indexes
            importIndexes(databaseName, tableName, databaseReferenceable, tableReferenceable);
        }
    }

    /**
     * Gets reference for the table
     *
     * @param dbName
     * @param tableName table name
     * @return table reference if exists, else null
     * @throws Exception
     */
    private Referenceable getTableReference(String dbName, String tableName) throws Exception {
        LOG.debug("Getting reference for table {}.{}", dbName, tableName);

        String typeName = HiveDataTypes.HIVE_TABLE.getName();
        MetadataServiceClient dgiClient = getMetadataServiceClient();

        String query = String.format("%s where name = '%s', dbName where name = '%s' and clusterName = '%s'",
                HiveDataTypes.HIVE_TABLE.getName(), tableName, dbName, clusterName);
        JSONArray results = dgiClient.searchByDSL(query);
        if (results.length() == 0) {
            return null;
        } else {
            //There should be just one instance with the given name
            String guid = getGuidFromDSLResponse(results.getJSONObject(0));
            LOG.debug("Got reference for table {}.{} = {}", dbName, tableName, guid);
            return new Referenceable(guid, typeName, null);
        }
    }

    private String getGuidFromDSLResponse(JSONObject jsonObject) throws JSONException {
        return jsonObject.getJSONObject("$id$").getString("id");
    }

    private Referenceable getSDForTable(String dbName, String tableName) throws Exception {
        Referenceable tableRef = getTableReference(dbName, tableName);
        if (tableRef == null) {
            throw new IllegalArgumentException("Table " + dbName + "." + tableName + " doesn't exist");
        }

        MetadataServiceClient dgiClient = getMetadataServiceClient();
        Referenceable tableInstance = dgiClient.getEntity(tableRef.getId().id);
        Id sdId = (Id) tableInstance.get("sd");
        return new Referenceable(sdId.id, sdId.getTypeName(), null);
    }

    public Referenceable registerTable(String dbName, String tableName) throws Exception {
        Referenceable dbReferenceable = registerDatabase(dbName);
        return registerTable(dbReferenceable, dbName, tableName);
    }

    public Referenceable registerTable(Referenceable dbReference, String dbName, String tableName) throws Exception {
        LOG.info("Attempting to register table [" + tableName + "]");
        Referenceable tableRef = getTableReference(dbName, tableName);
        if (tableRef == null) {
            LOG.info("Importing objects from " + dbName + "." + tableName);

            Table hiveTable = hiveClient.getTable(dbName, tableName);

            tableRef = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
            tableRef.set("name", hiveTable.getTableName());
            tableRef.set("owner", hiveTable.getOwner());
            //todo fix
            tableRef.set("createTime", hiveTable.getLastAccessTime());
            tableRef.set("lastAccessTime", hiveTable.getLastAccessTime());
            tableRef.set("retention", hiveTable.getRetention());

            // add reference to the database
            tableRef.set("dbName", dbReference);

            // add reference to the StorageDescriptor
            StorageDescriptor storageDesc = hiveTable.getSd();
            Referenceable sdReferenceable = fillStorageDescStruct(storageDesc);
            tableRef.set("sd", sdReferenceable);

            // add reference to the Partition Keys
            List<Referenceable> partKeys = new ArrayList<>();
            Referenceable colRef;
            if (hiveTable.getPartitionKeys().size() > 0) {
                for (FieldSchema fs : hiveTable.getPartitionKeys()) {
                    colRef = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
                    colRef.set("name", fs.getName());
                    colRef.set("type", fs.getType());
                    colRef.set("comment", fs.getComment());
                    Referenceable colRefTyped = createInstance(colRef);
                    partKeys.add(colRefTyped);
                }

                tableRef.set("partitionKeys", partKeys);
            }

            tableRef.set("parameters", hiveTable.getParameters());

            if (hiveTable.getViewOriginalText() != null) {
                tableRef.set("viewOriginalText", hiveTable.getViewOriginalText());
            }

            if (hiveTable.getViewExpandedText() != null) {
                tableRef.set("viewExpandedText", hiveTable.getViewExpandedText());
            }

            tableRef.set("tableType", hiveTable.getTableType());
            tableRef.set("temporary", hiveTable.isTemporary());

            List<Referenceable> colList = getColumns(hiveTable.getAllCols());
            tableRef.set("columns", colList);

            tableRef = createInstance(tableRef);
        } else {
            LOG.info("Table {}.{} is already registered with id {}", dbName, tableName, tableRef.getId().id);
        }
        return tableRef;
    }

    private void importPartitions(String db, String tableName,
                                  Referenceable dbReferenceable,
                                  Referenceable tableReferenceable,
                                  Referenceable sdReferenceable) throws Exception {
        Set<Partition> tableParts = hiveClient.getAllPartitionsOf(new Table(Table.getEmptyTable(db, tableName)));

        if (tableParts.size() > 0) {
            for (Partition hivePart : tableParts) {
                importPartition(hivePart, dbReferenceable, tableReferenceable, sdReferenceable);
            }
        }
    }

    //todo should be idempotent
    private Referenceable importPartition(Partition hivePart,
                                          Referenceable dbReferenceable,
                                          Referenceable tableReferenceable,
                                          Referenceable sdReferenceable) throws Exception {
        LOG.info("Importing partition for {}.{} with values {}", dbReferenceable, tableReferenceable,
                StringUtils.join(hivePart.getValues(), ","));
        Referenceable partRef = new Referenceable(HiveDataTypes.HIVE_PARTITION.getName());
        partRef.set("values", hivePart.getValues());

        partRef.set("dbName", dbReferenceable);
        partRef.set("tableName", tableReferenceable);

        //todo fix
        partRef.set("createTime", hivePart.getLastAccessTime());
        partRef.set("lastAccessTime", hivePart.getLastAccessTime());

        // sdStruct = fillStorageDescStruct(hivePart.getSd());
        // Instead of creating copies of the sdstruct for partitions we are reusing existing
        // ones will fix to identify partitions with differing schema.
        partRef.set("sd", sdReferenceable);

        partRef.set("parameters", hivePart.getParameters());

        return createInstance(partRef);
    }

    private void importIndexes(String db, String table,
                               Referenceable dbReferenceable,
                               Referenceable tableReferenceable) throws Exception {
        List<Index> indexes = hiveClient.getIndexes(db, table, Short.MAX_VALUE);
        if (indexes.size() > 0) {
            for (Index index : indexes) {
                importIndex(index, dbReferenceable, tableReferenceable);
            }
        }
    }

    //todo should be idempotent
    private void importIndex(Index index,
                             Referenceable dbReferenceable,
                             Referenceable tableReferenceable) throws Exception {
        LOG.info("Importing index {} for {}.{}", index.getIndexName(), dbReferenceable, tableReferenceable);
        Referenceable indexRef = new Referenceable(HiveDataTypes.HIVE_INDEX.getName());

        indexRef.set("indexName", index.getIndexName());
        indexRef.set("indexHandlerClass", index.getIndexHandlerClass());

        indexRef.set("dbName", dbReferenceable);

        indexRef.set("createTime", index.getCreateTime());
        indexRef.set("lastAccessTime", index.getLastAccessTime());
        indexRef.set("origTableName", index.getOrigTableName());
        indexRef.set("indexTableName", index.getIndexTableName());

        Referenceable sdReferenceable = fillStorageDescStruct(index.getSd());
        indexRef.set("sd", sdReferenceable);

        indexRef.set("parameters", index.getParameters());

        tableReferenceable.set("deferredRebuild", index.isDeferredRebuild());

        createInstance(indexRef);
    }

    private Referenceable fillStorageDescStruct(StorageDescriptor storageDesc) throws Exception {
        LOG.debug("Filling storage descriptor information for " + storageDesc);

        Referenceable sdReferenceable = new Referenceable(HiveDataTypes.HIVE_STORAGEDESC.getName());

        SerDeInfo serdeInfo = storageDesc.getSerdeInfo();
        LOG.debug("serdeInfo = " + serdeInfo);
        // SkewedInfo skewedInfo = storageDesc.getSkewedInfo();

        String serdeInfoName = HiveDataTypes.HIVE_SERDE.getName();
        Struct serdeInfoStruct = new Struct(serdeInfoName);

        serdeInfoStruct.set("name", serdeInfo.getName());
        serdeInfoStruct.set("serializationLib", serdeInfo.getSerializationLib());
        serdeInfoStruct.set("parameters", serdeInfo.getParameters());

        sdReferenceable.set("serdeInfo", serdeInfoStruct);

        // Will need to revisit this after we fix typesystem.
        /*
        LOG.info("skewedInfo = " + skewedInfo);
        String skewedInfoName = HiveDataTypes.HIVE_SKEWEDINFO.name();
        Struct skewedInfoStruct = new Struct(skewedInfoName);
        if (skewedInfo.getSkewedColNames().size() > 0) {
            skewedInfoStruct.set("skewedColNames", skewedInfo.getSkewedColNames());
            skewedInfoStruct.set("skewedColValues", skewedInfo.getSkewedColValues());
            skewedInfoStruct.set("skewedColValueLocationMaps",
                    skewedInfo.getSkewedColValueLocationMaps());
            StructType skewedInfotype = (StructType) hiveTypeSystem.getDataType(skewedInfoName);
            ITypedStruct skewedInfoStructTyped =
                    skewedInfotype.convert(skewedInfoStruct, Multiplicity.OPTIONAL);
            sdStruct.set("skewedInfo", skewedInfoStructTyped);
        }
        */

        List<Referenceable> fieldsList = getColumns(storageDesc.getCols());
        sdReferenceable.set("cols", fieldsList);

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

        sdReferenceable.set("parameters", storageDesc.getParameters());
        sdReferenceable.set("storedAsSubDirectories", storageDesc.isStoredAsSubDirectories());

        return createInstance(sdReferenceable);
    }

    private List<Referenceable> getColumns(List<FieldSchema> schemaList) throws Exception
    {
        List<Referenceable> colList = new ArrayList<>();
        for (FieldSchema fs : schemaList) {
            LOG.debug("Processing field " + fs);
            Referenceable colReferenceable = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
            colReferenceable.set("name", fs.getName());
            colReferenceable.set("type", fs.getType());
            colReferenceable.set("comment", fs.getComment());

            colList.add(createInstance(colReferenceable));
        }
        return colList;
    }

    public synchronized void registerHiveDataModel() throws Exception {
        HiveDataModelGenerator dataModelGenerator = new HiveDataModelGenerator();
        MetadataServiceClient dgiClient = getMetadataServiceClient();

        //Register hive data model if its not already registered
        if (dgiClient.getType(HiveDataTypes.HIVE_PROCESS.getName()) == null ) {
            LOG.info("Registering Hive data model");
            dgiClient.createType(dataModelGenerator.getModelAsJson());
        } else {
            LOG.info("Hive data model is already registered!");
        }
    }

    public static void main(String[] argv) throws Exception {
        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(new HiveConf());
        hiveMetaStoreBridge.registerHiveDataModel();
        hiveMetaStoreBridge.importHiveMetadata();
    }
}
