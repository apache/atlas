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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.Struct;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A Bridge Utility that imports metadata from the Hive Meta Store
 * and registers then in DGI.
 */
public class HiveMetaStoreBridge {
    public static class Pair<S, T> {
        public S first;
        public T second;

        public Pair(S first, T second) {
            this.first = first;
            this.second = second;
        }

        public static <S, T> Pair of(S first, T second) {
            return new Pair(first, second);
        }
    }

    public static final String DGI_URL_PROPERTY = "hive.dgi.url";

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreBridge.class);

    private final HiveMetaStoreClient hiveMetaStoreClient;
    private final MetadataServiceClient metadataServiceClient;

    /**
     * Construct a HiveMetaStoreBridge.
     * @param hiveConf
     */
    public HiveMetaStoreBridge(HiveConf hiveConf) throws Exception {
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metadataServiceClient = new MetadataServiceClient(hiveConf.get(DGI_URL_PROPERTY));
    }

    public MetadataServiceClient getMetadataServiceClient() {
        return metadataServiceClient;
    }

    public void importHiveMetadata() throws Exception {
        LOG.info("Importing hive metadata");
        importDatabases();
    }

    private void importDatabases() throws Exception {
        List<String> databases = hiveMetaStoreClient.getAllDatabases();
        for (String databaseName : databases) {
            Referenceable dbReference = registerDatabase(databaseName);

            importTables(databaseName, dbReference);
        }
    }

    public Referenceable registerDatabase(String databaseName) throws Exception {
        LOG.info("Importing objects from databaseName : " + databaseName);

        Database hiveDB = hiveMetaStoreClient.getDatabase(databaseName);

        Referenceable dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
        dbRef.set("name", hiveDB.getName());
        dbRef.set("description", hiveDB.getDescription());
        dbRef.set("locationUri", hiveDB.getLocationUri());
        dbRef.set("parameters", hiveDB.getParameters());
        dbRef.set("ownerName", hiveDB.getOwnerName());
        dbRef.set("ownerType", hiveDB.getOwnerType().getValue());

        return createInstance(dbRef);
    }

    public Referenceable createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        LOG.debug("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOG.debug("Submitting new entity= " + entityJSON);
        JSONObject jsonObject = metadataServiceClient.createEntity(entityJSON);
        String guid = jsonObject.getString(MetadataServiceClient.RESULTS);
        LOG.debug("created instance for type " + typeName + ", guid: " + guid);

        return new Referenceable(guid, referenceable.getTypeName(), null);
    }

    private void importTables(String databaseName, Referenceable databaseReferenceable) throws Exception {
        List<String> hiveTables = hiveMetaStoreClient.getAllTables(databaseName);

        for (String tableName : hiveTables) {
            Pair<Referenceable, Referenceable> tableReferenceable = registerTable(databaseReferenceable, databaseName, tableName);

            // Import Partitions
            importPartitions(databaseName, tableName, databaseReferenceable, tableReferenceable.first, tableReferenceable.second);

            // Import Indexes
            importIndexes(databaseName, tableName, databaseReferenceable, tableReferenceable.first);
        }
    }

    public Pair<Referenceable, Referenceable> registerTable(Referenceable dbReference, String dbName, String tableName) throws Exception {
        LOG.info("Importing objects from " + dbName + "." + tableName);

        Table hiveTable = hiveMetaStoreClient.getTable(dbName, tableName);

        Referenceable tableRef = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        tableRef.set("tableName", hiveTable.getTableName());
        tableRef.set("owner", hiveTable.getOwner());
        tableRef.set("createTime", hiveTable.getCreateTime());
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
        if (hiveTable.getPartitionKeysSize() > 0) {
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

        if (hiveTable.isSetViewOriginalText()) {
            tableRef.set("viewOriginalText", hiveTable.getViewOriginalText());
        }

        if (hiveTable.isSetViewExpandedText()) {
            tableRef.set("viewExpandedText", hiveTable.getViewExpandedText());
        }

        tableRef.set("tableType", hiveTable.getTableType());
        tableRef.set("temporary", hiveTable.isTemporary());

        Referenceable tableReferenceable = createInstance(tableRef);
        return Pair.of(tableReferenceable, sdReferenceable);
    }

    private void importPartitions(String db, String table,
                                  Referenceable dbReferenceable,
                                  Referenceable tableReferenceable,
                                  Referenceable sdReferenceable) throws Exception {
        List<Partition> tableParts = hiveMetaStoreClient.listPartitions(
                db, table, Short.MAX_VALUE);

        if (tableParts.size() > 0) {
            for (Partition hivePart : tableParts) {
                importPartition(hivePart, dbReferenceable, tableReferenceable, sdReferenceable);
            }
        }
    }

    private Referenceable importPartition(Partition hivePart,
                                          Referenceable dbReferenceable,
                                          Referenceable tableReferenceable,
                                          Referenceable sdReferenceable) throws Exception {
        Referenceable partRef = new Referenceable(HiveDataTypes.HIVE_PARTITION.getName());
        partRef.set("values", hivePart.getValues());

        partRef.set("dbName", dbReferenceable);
        partRef.set("tableName", tableReferenceable);

        partRef.set("createTime", hivePart.getCreateTime());
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
        List<Index> indexes = hiveMetaStoreClient.listIndexes(db, table, Short.MAX_VALUE);
        if (indexes.size() > 0) {
            for (Index index : indexes) {
                importIndex(index, dbReferenceable, tableReferenceable);
            }
        }
    }

    private void importIndex(Index index,
                             Referenceable dbReferenceable,
                             Referenceable tableReferenceable) throws Exception {
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

        List<Referenceable> fieldsList = new ArrayList<>();
        Referenceable colReferenceable;
        for (FieldSchema fs : storageDesc.getCols()) {
            LOG.debug("Processing field " + fs);
            colReferenceable = new Referenceable(HiveDataTypes.HIVE_COLUMN.getName());
            colReferenceable.set("name", fs.getName());
            colReferenceable.set("type", fs.getType());
            colReferenceable.set("comment", fs.getComment());

            fieldsList.add(createInstance(colReferenceable));
        }
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

    public static void main(String[] argv) throws Exception {
        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(new HiveConf());
        hiveMetaStoreBridge.importHiveMetadata();
    }
}
