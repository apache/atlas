/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.ITypedStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.Struct;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.IRepository;
import org.apache.hadoop.metadata.typesystem.persistence.Id;
import org.apache.hadoop.metadata.repository.RepositoryException;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * todo - this needs to be removed.
 */
@Deprecated
public class HiveImporter {

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveImporter.class);
    private final HiveMetaStoreClient hiveMetastoreClient;
    private IRepository repository;
    private MetadataRepository graphRepository;
    private HiveTypeSystem hiveTypeSystem;

    private List<Id> dbInstances;
    private List<Id> tableInstances;
    private List<Id> partitionInstances;
    private List<Id> indexInstances;
    private List<Id> columnInstances;
    private List<Id> processInstances;


    public HiveImporter(MetadataRepository repo, HiveTypeSystem hts, HiveMetaStoreClient hmc)
    throws RepositoryException {
        this(hts, hmc);

        if (repo == null) {
            LOG.error("repository is null");
            throw new RuntimeException("repository is null");
        }

        this.graphRepository = repo;

    }


    public HiveImporter(IRepository repo, HiveTypeSystem hts, HiveMetaStoreClient hmc)
    throws RepositoryException {
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
        indexInstances = new ArrayList<>();
        columnInstances = new ArrayList<>();
        processInstances = new ArrayList<>();

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

    public List<Id> getIndexInstances() {
        return indexInstances;
    }

    public List<Id> getProcessInstances() {
        return processInstances;
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

    public void importHiveRTInfo(String stmt) throws MetadataException {

    }

    private boolean usingMemRepository() {
        return this.graphRepository == null;
    }

    private InstancePair createInstance(Referenceable ref)
    throws MetadataException {
        if (usingMemRepository()) {
            return new InstancePair(repository.create(ref), null);
        } else {
            String typeName = ref.getTypeName();
            IDataType dataType = hiveTypeSystem.getDataType(typeName);
            LOG.debug("creating instance of type " + typeName + " dataType " + dataType);
            ITypedReferenceableInstance instance =
                    (ITypedReferenceableInstance) dataType.convert(ref, Multiplicity.OPTIONAL);
            String guid = graphRepository.createEntity(instance);
            System.out.println("creating instance of type " + typeName + " dataType " + dataType
                    + ", guid: " + guid);

            return new InstancePair(null,
                    new Referenceable(guid, ref.getTypeName(), ref.getValuesMap()));
        }
    }

    private void setReferenceInstanceAttribute(Referenceable ref, String attr,
                                               InstancePair instance) {
        if (usingMemRepository()) {
            ref.set(attr, instance.left());
        } else {
            ref.set(attr, instance.right());
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
            InstancePair dbRefTyped = createInstance(dbRef);
            if (usingMemRepository()) {
                dbInstances.add(dbRefTyped.left().getId());
            }
            importTables(db, dbRefTyped);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void importTables(String db, InstancePair dbRefTyped) throws MetadataException {
        try {
            List<String> hiveTables = hiveMetastoreClient.getAllTables(db);

            for (String table : hiveTables) {
                importTable(db, table, dbRefTyped);
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void importTable(String db, String table, InstancePair dbRefTyped)
    throws MetadataException {
        try {
            LOG.info("Importing objects from " + db + "." + table);

            Table hiveTable = hiveMetastoreClient.getTable(db, table);

            Referenceable tableRef = new Referenceable(
                    HiveTypeSystem.DefinedTypes.HIVE_TABLE.name());
            setReferenceInstanceAttribute(tableRef, "dbName", dbRefTyped);
            tableRef.set("tableName", hiveTable.getTableName());
            tableRef.set("owner", hiveTable.getOwner());
            tableRef.set("createTime", hiveTable.getCreateTime());
            tableRef.set("lastAccessTime", hiveTable.getLastAccessTime());
            tableRef.set("retention", hiveTable.getRetention());

            StorageDescriptor storageDesc = hiveTable.getSd();
            InstancePair sdRefTyped = fillStorageDescStruct(storageDesc);
            setReferenceInstanceAttribute(tableRef, "sd", sdRefTyped);
            List<InstancePair> partKeys = new ArrayList<>();
            Referenceable colRef;
            if (hiveTable.getPartitionKeysSize() > 0) {
                for (FieldSchema fs : hiveTable.getPartitionKeys()) {
                    colRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_COLUMN.name());
                    colRef.set("name", fs.getName());
                    colRef.set("type", fs.getType());
                    colRef.set("comment", fs.getComment());
                    InstancePair colRefTyped = createInstance(colRef);
                    partKeys.add(colRefTyped);
                }
                if (usingMemRepository()) {
                    List<ITypedReferenceableInstance> keys = new ArrayList<>();
                    for (InstancePair ip : partKeys) {
                        keys.add(ip.left());
                    }
                    tableRef.set("partitionKeys", keys);
                } else {
                    List<Referenceable> keys = new ArrayList<>();
                    for (InstancePair ip : partKeys) {
                        keys.add(ip.right());
                    }
                    tableRef.set("partitionKeys", keys);
                }
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

            InstancePair tableRefTyped = createInstance(tableRef);
            if (usingMemRepository()) {
                tableInstances.add(tableRefTyped.left().getId());
            }

            importPartitions(db, table, dbRefTyped, tableRefTyped, sdRefTyped);
            List<Index> indexes = hiveMetastoreClient.listIndexes(db, table, Short.MAX_VALUE);

            if (indexes.size() > 0) {
                for (Index index : indexes) {
                    importIndexes(db, table, dbRefTyped, tableRef);
                }
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }


    }

    private void importPartitions(String db, String table, InstancePair dbRefTyped,
                                  InstancePair tableRefTyped, InstancePair sdRefTyped)
    throws MetadataException {
        try {
            List<Partition> tableParts = hiveMetastoreClient
                    .listPartitions(db, table, Short.MAX_VALUE);
            if (tableParts.size() > 0) {
                for (Partition hivePart : tableParts) {
                    importPartition(hivePart, dbRefTyped, tableRefTyped, sdRefTyped);
                }
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void importPartition(Partition hivePart,
                                 InstancePair dbRefTyped, InstancePair tableRefTyped,
                                 InstancePair sdRefTyped)
    throws MetadataException {
        try {
            Referenceable partRef = new Referenceable(
                    HiveTypeSystem.DefinedTypes.HIVE_PARTITION.name());
            partRef.set("values", hivePart.getValues());
            setReferenceInstanceAttribute(partRef, "dbName", dbRefTyped);
            setReferenceInstanceAttribute(partRef, "tableName", tableRefTyped);
            partRef.set("createTime", hivePart.getCreateTime());
            partRef.set("lastAccessTime", hivePart.getLastAccessTime());
            //sdStruct = fillStorageDescStruct(hivePart.getSd());
            // Instead of creating copies of the sdstruct for partitions we are reusing existing
            // ones
            // will fix to identify partitions with differing schema.
            setReferenceInstanceAttribute(partRef, "sd", sdRefTyped);
            partRef.set("parameters", hivePart.getParameters());
            InstancePair partRefTyped = createInstance(partRef);
            if (usingMemRepository()) {
                partitionInstances.add(partRefTyped.left().getId());
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void importIndexes(String db, String table, InstancePair dbRefTyped,
                               Referenceable tableRef)
    throws MetadataException {
        try {
            List<Index> indexes = hiveMetastoreClient.listIndexes(db, table, Short.MAX_VALUE);
            if (indexes.size() > 0) {
                for (Index index : indexes) {
                    importIndex(index, dbRefTyped, tableRef);
                }
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void importIndex(Index index,
                             InstancePair dbRefTyped, Referenceable tableRef)
    throws MetadataException {
        try {
            Referenceable indexRef = new Referenceable(
                    HiveTypeSystem.DefinedTypes.HIVE_INDEX.name());
            indexRef.set("indexName", index.getIndexName());
            indexRef.set("indexHandlerClass", index.getIndexHandlerClass());
            setReferenceInstanceAttribute(indexRef, "dbName", dbRefTyped);
            indexRef.set("createTime", index.getCreateTime());
            indexRef.set("lastAccessTime", index.getLastAccessTime());
            indexRef.set("origTableName", index.getOrigTableName());
            indexRef.set("indexTableName", index.getIndexTableName());
            InstancePair sdRefTyped = fillStorageDescStruct(index.getSd());
            setReferenceInstanceAttribute(indexRef, "sd", sdRefTyped);
            indexRef.set("parameters", index.getParameters());
            tableRef.set("deferredRebuild", index.isDeferredRebuild());
            InstancePair indexRefTyped = createInstance(indexRef);
            if (usingMemRepository()) {
                indexInstances.add(indexRefTyped.left().getId());
            }

        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private InstancePair fillStorageDescStruct(StorageDescriptor storageDesc) throws Exception {
        Referenceable sdRef = new Referenceable(
                HiveTypeSystem.DefinedTypes.HIVE_STORAGEDESC.name());

        SerDeInfo serdeInfo = storageDesc.getSerdeInfo();
        // SkewedInfo skewedInfo = storageDesc.getSkewedInfo();


        LOG.debug("Filling storage descriptor information for " + storageDesc);

        String serdeInfoName = HiveTypeSystem.DefinedTypes.HIVE_SERDE.name();
        Struct serdeInfoStruct = new Struct(serdeInfoName);

        serdeInfoStruct.set("name", serdeInfo.getName());
        serdeInfoStruct.set("serializationLib", serdeInfo.getSerializationLib());
        serdeInfoStruct.set("parameters", serdeInfo.getParameters());

        LOG.debug("serdeInfo = " + serdeInfo);

        StructType serdeInfotype = (StructType) hiveTypeSystem.getDataType(serdeInfoName);
        ITypedStruct serdeInfoStructTyped =
                serdeInfotype.convert(serdeInfoStruct, Multiplicity.OPTIONAL);

        sdRef.set("serdeInfo", serdeInfoStructTyped);


        // Will need to revisit this after we fix typesystem.

        //LOG.info("skewedInfo = " + skewedInfo);
        //String skewedInfoName = HiveTypeSystem.DefinedTypes.HIVE_SKEWEDINFO.name();
        //Struct skewedInfoStruct = new Struct(skewedInfoName);
        //if (skewedInfo.getSkewedColNames().size() > 0) {
        //    skewedInfoStruct.set("skewedColNames", skewedInfo.getSkewedColNames());
        //    skewedInfoStruct.set("skewedColValues", skewedInfo.getSkewedColValues());
        //    skewedInfoStruct.set("skewedColValueLocationMaps", skewedInfo
        // .getSkewedColValueLocationMaps());
        //    StructType skewedInfotype = (StructType) hiveTypeSystem.getDataType(skewedInfoName);
        //    ITypedStruct skewedInfoStructTyped =
        //            skewedInfotype.convert(skewedInfoStruct, Multiplicity.OPTIONAL);
        //    sdStruct.set("skewedInfo", skewedInfoStructTyped);
        //}


        List<InstancePair> fieldsList = new ArrayList<>();

        Referenceable colRef;
        for (FieldSchema fs : storageDesc.getCols()) {
            LOG.debug("Processing field " + fs);
            colRef = new Referenceable(HiveTypeSystem.DefinedTypes.HIVE_COLUMN.name());
            colRef.set("name", fs.getName());
            colRef.set("type", fs.getType());
            colRef.set("comment", fs.getComment());
            InstancePair colRefTyped = createInstance(colRef);
            fieldsList.add(colRefTyped);
            if (usingMemRepository()) {
                columnInstances.add(colRefTyped.left().getId());
            }
        }
        if (usingMemRepository()) {
            List<ITypedReferenceableInstance> flds = new ArrayList<>();
            for (InstancePair ip : fieldsList) {
                flds.add(ip.left());
            }
            sdRef.set("cols", flds);
        } else {
            List<Referenceable> flds = new ArrayList<>();
            for (InstancePair ip : fieldsList) {
                flds.add(ip.right());
            }
            sdRef.set("cols", flds);
        }

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
        sdRef.set("location", storageDesc.getLocation());
        sdRef.set("inputFormat", storageDesc.getInputFormat());
        sdRef.set("outputFormat", storageDesc.getOutputFormat());
        sdRef.set("compressed", storageDesc.isCompressed());
        if (storageDesc.getBucketCols().size() > 0) {
            sdRef.set("bucketCols", storageDesc.getBucketCols());
        }
        if (sortColsStruct.size() > 0) {
            sdRef.set("sortCols", sortColsStruct);
        }
        sdRef.set("parameters", storageDesc.getParameters());
        sdRef.set("storedAsSubDirectories", storageDesc.isStoredAsSubDirectories());
        InstancePair sdRefTyped = createInstance(sdRef);

        return sdRefTyped;
    }

    private class Pair<L, R> {
        final L left;
        final R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public L left() {
            return this.left;
        }

        public R right() {
            return this.right;
        }
    }

    private class InstancePair extends Pair<ITypedReferenceableInstance, Referenceable> {
        public InstancePair(ITypedReferenceableInstance left, Referenceable right) {
            super(left, right);
        }
    }
}
