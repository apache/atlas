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

package org.apache.hadoop.metadata.hivetypes;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.ITypedStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.storage.IRepository;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HiveImporter {

    private final HiveMetaStoreClient hiveMetastoreClient;

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveImporter.class);
    private IRepository repository;
    private MetadataRepository graphRepository;
    private HiveTypeSystem hiveTypeSystem;

    private List<Id> dbInstances;
    private List<Id> tableInstances;
    private List<Id> partitionInstances;
    private List<Id> columnInstances;

    public HiveImporter(MetadataRepository repo, HiveTypeSystem hts, HiveMetaStoreClient hmc) throws RepositoryException {
        this(hts, hmc);

        if (repo == null) {
            LOG.error("repository is null");
            throw new RuntimeException("repository is null");
        }

        this.graphRepository = repo;
    }

    public HiveImporter(IRepository repo, HiveTypeSystem hts, HiveMetaStoreClient hmc) throws RepositoryException {
        this(hts, hmc);

        if (repo == null) {
            LOG.error("repository is null");
            throw new RuntimeException("repository is null");
        }

        repository = repo;

        repository.defineTypes(hts.getHierarchicalTypeDefinitions());
    }

    private HiveImporter(HiveTypeSystem hts, HiveMetaStoreClient hmc) {
        this.hiveMetastoreClient = hmc;
        this.hiveTypeSystem = hts;
        dbInstances = new ArrayList<>();
        tableInstances = new ArrayList<>();
        partitionInstances = new ArrayList<>();
        columnInstances = new ArrayList<>();
    }

    public List<Id> getDBInstances() {
        return dbInstances;
    }

    public List<Id> getTableInstances() {
        return tableInstances;
    }

    public List<Id> getPartitionInstances() {
        return partitionInstances;
    }

    public List<Id> getColumnInstances() {
        return columnInstances;
    }

    public void importHiveMetadata() throws MetadataException {

        LOG.info("Importing hive metadata");
        try {
            List<String> dbs = hiveMetastoreClient.getAllDatabases();
            for (String db : dbs) {
                importDatabase(db);
            }
        } catch (MetaException me) {
            throw new MetadataException(me);
        }
    }

    private Referenceable createInstance(Referenceable ref)
            throws MetadataException {
        if (repository != null) {
            return (Referenceable) repository.create(ref);
        } else {
            String typeName = ref.getTypeName();
            IDataType dataType = hiveTypeSystem.getDataType(typeName);
            LOG.debug("creating instance of type " + typeName + " dataType " + dataType);
            ITypedReferenceableInstance instance =
                    (ITypedReferenceableInstance) dataType.convert(ref, Multiplicity.OPTIONAL);
            String guid = graphRepository.createEntity(instance, typeName);
            System.out.println("creating instance of type " + typeName + " dataType " + dataType
                    + ", guid: " + guid);

            return new Referenceable(guid, ref.getTypeName(), ref.getValuesMap());
        }
    }

    private void importDatabase(String db) throws MetadataException {
        try {
            LOG.info("Importing objects from database : " + db);

            Database hiveDB = hiveMetastoreClient.getDatabase(db);
            Referenceable dbRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_DB.name());
            dbRef.set("name", hiveDB.getName());
            dbRef.set("description", hiveDB.getDescription());
            dbRef.set("locationUri", hiveDB.getLocationUri());
            dbRef.set("parameters", hiveDB.getParameters());
            dbRef.set("ownerName", hiveDB.getOwnerName());
            dbRef.set("ownerType", hiveDB.getOwnerType().getValue());
            Referenceable dbRefTyped = createInstance(dbRef);
            dbInstances.add(dbRefTyped.getId());
            importTables(db, dbRefTyped);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void importTables(String db, Referenceable dbRefTyped) throws MetadataException {
        try {
            List<String> hiveTables = hiveMetastoreClient.getAllTables(db);

            for (String table : hiveTables) {
                LOG.info("Importing objects from " + db + "." + table);

                Table hiveTable = hiveMetastoreClient.getTable(db, table);

                Referenceable tableRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_TABLE.name());
                tableRef.set("dbName", dbRefTyped);
                tableRef.set("tableName", hiveTable.getTableName());
                tableRef.set("owner", hiveTable.getOwner());
                tableRef.set("createTime", hiveTable.getCreateTime());
                tableRef.set("lastAccessTime", hiveTable.getLastAccessTime());
                tableRef.set("retention", hiveTable.getRetention());

                StorageDescriptor storageDesc = hiveTable.getSd();
                ITypedStruct sdStruct = fillStorageDescStruct(storageDesc);
                tableRef.set("sd", sdStruct);
                tableRef.set("columns", sdStruct.get("cols"));
                List<Referenceable> partKeys = new ArrayList<>();
                Referenceable colRef;
                if (hiveTable.getPartitionKeysSize() > 0) {
                    for (FieldSchema fs : hiveTable.getPartitionKeys()) {
                        colRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_COLUMN.name());
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

                Referenceable tableRefTyped = createInstance(tableRef);
                tableInstances.add(tableRefTyped.getId());


                List<Partition> tableParts = hiveMetastoreClient.listPartitions(db, table, Short.MAX_VALUE);

                if (tableParts.size() > 0) {
                    for (Partition hivePart : tableParts) {
                        Referenceable partRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_PARTITION.name());
                        partRef.set("values", hivePart.getValues());
                        partRef.set("dbName", dbRefTyped);
                        partRef.set("tableName", tableRefTyped);
                        partRef.set("createTime", hivePart.getCreateTime());
                        partRef.set("lastAccessTime", hivePart.getLastAccessTime());
                        sdStruct = fillStorageDescStruct(hivePart.getSd());
                        partRef.set("sd", sdStruct);
                        partRef.set("columns", sdStruct.get("cols"));
                        partRef.set("parameters", hivePart.getParameters());
                        Referenceable partRefTyped = createInstance(partRef);
                        partitionInstances.add(partRefTyped.getId());
                    }
                }
            }
        } catch (Exception te) {
            throw new MetadataException(te);
        }

    }

    private ITypedStruct fillStorageDescStruct(StorageDescriptor storageDesc) throws Exception {
        String storageDescName = HiveTypeSystem.DefinedTypes.HIVE_STORAGEDESC.name();

        SerDeInfo serdeInfo = storageDesc.getSerdeInfo();
        // SkewedInfo skewedInfo = storageDesc.getSkewedInfo();

        Struct sdStruct = new Struct(storageDescName);

        LOG.debug("Filling storage descriptor information for " + storageDesc);

        String serdeInfoName = HiveTypeSystem.DefinedTypes.HIVE_SERDE.name();
        Struct serdeInfoStruct = new Struct(serdeInfoName);

        serdeInfoStruct.set("name", serdeInfo.getName());
        serdeInfoStruct.set("serializationLib", serdeInfo.getSerializationLib());
        serdeInfoStruct.set("parameters", serdeInfo.getParameters());

        LOG.debug("serdeInfo = " + serdeInfo);

        StructType serdeInfotype =  (StructType) hiveTypeSystem.getDataType(serdeInfoName);
        ITypedStruct serdeInfoStructTyped =
                 serdeInfotype.convert(serdeInfoStruct, Multiplicity.OPTIONAL);

        sdStruct.set("serdeInfo", serdeInfoStructTyped);


        // Will need to revisit this after we fix typesystem.

        //LOG.info("skewedInfo = " + skewedInfo);
        //String skewedInfoName = HiveTypeSystem.DefinedTypes.HIVE_SKEWEDINFO.name();
        //Struct skewedInfoStruct = new Struct(skewedInfoName);
        //if (skewedInfo.getSkewedColNames().size() > 0) {
        //    skewedInfoStruct.set("skewedColNames", skewedInfo.getSkewedColNames());
        //    skewedInfoStruct.set("skewedColValues", skewedInfo.getSkewedColValues());
        //    skewedInfoStruct.set("skewedColValueLocationMaps", skewedInfo.getSkewedColValueLocationMaps());
        //    StructType skewedInfotype = (StructType) hiveTypeSystem.getDataType(skewedInfoName);
        //    ITypedStruct skewedInfoStructTyped =
        //            skewedInfotype.convert(skewedInfoStruct, Multiplicity.OPTIONAL);
        //    sdStruct.set("skewedInfo", skewedInfoStructTyped);
        //}



        List<Referenceable> fieldsList = new ArrayList<>();
        Referenceable colRef;
        for (FieldSchema fs : storageDesc.getCols()) {
            LOG.debug("Processing field " + fs);
            colRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_COLUMN.name());
            colRef.set("name", fs.getName());
            colRef.set("type", fs.getType());
            colRef.set("comment", fs.getComment());
            Referenceable colRefTyped = createInstance(colRef);
            fieldsList.add(colRefTyped);
            columnInstances.add(colRefTyped.getId());
        }
        sdStruct.set("cols", fieldsList);

        List<ITypedStruct> sortColsStruct = new ArrayList<>();

        for (Order sortcol : storageDesc.getSortCols()) {
            String hiveOrderName = HiveTypeSystem.DefinedTypes.HIVE_ORDER.name();
            Struct colStruct = new Struct(hiveOrderName);
            colStruct.set("col", sortcol.getCol());
            colStruct.set("order", sortcol.getOrder());
            StructType sortColType = (StructType) hiveTypeSystem.getDataType(hiveOrderName);
            ITypedStruct sortColTyped =
                    sortColType.convert(colStruct, Multiplicity.OPTIONAL);
            sortColsStruct.add(sortColTyped);

        }
        sdStruct.set("location", storageDesc.getLocation());
        sdStruct.set("inputFormat", storageDesc.getInputFormat());
        sdStruct.set("outputFormat", storageDesc.getOutputFormat());
        sdStruct.set("compressed", storageDesc.isCompressed());
        if (storageDesc.getBucketCols().size() > 0) {
            sdStruct.set("bucketCols", storageDesc.getBucketCols());
        }
        if (sortColsStruct.size() > 0) {
            sdStruct.set("sortCols", sortColsStruct);
        }
        sdStruct.set("parameters", storageDesc.getParameters());
        sdStruct.set("storedAsSubDirectories", storageDesc.isStoredAsSubDirectories());
        StructType storageDesctype = (StructType) hiveTypeSystem.getDataType(storageDescName);
        return storageDesctype.convert(sdStruct, Multiplicity.OPTIONAL);
    }
}
