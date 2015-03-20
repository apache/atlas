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

package org.apache.hadoop.metadata.hive.model;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.json.TypesSerialization;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.EnumValue;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility that generates hive data model for both metastore entities and DDL/DML queries.
 */
public class HiveDataModelGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDataModelGenerator.class);

    private static final DataTypes.MapType STRING_MAP_TYPE =
            new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE);

    private final Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions;
    private final Map<String, EnumTypeDefinition> enumTypeDefinitionMap;
    private final Map<String, StructTypeDefinition> structTypeDefinitionMap;

    public HiveDataModelGenerator() {
        classTypeDefinitions = new HashMap<>();
        enumTypeDefinitionMap = new HashMap<>();
        structTypeDefinitionMap = new HashMap<>();
    }

    public void createDataModel() throws MetadataException {
        LOG.info("Generating the Hive Data Model....");

        // enums
        createHiveObjectTypeEnum();
        createHivePrincipalTypeEnum();
        createFunctionTypeEnum();
        createResourceTypeEnum();

        // structs
        createSerDeStruct();
        //createSkewedInfoStruct();
        createOrderStruct();
        createResourceUriStruct();
        createStorageDescClass();

        // classes
        createDBClass();
        createTypeClass();
        createColumnClass();
        createPartitionClass();
        createTableClass();
        createIndexClass();
        createFunctionClass();
        createRoleClass();

        // DDL/DML Process
        createProcessClass();
    }

    public TypesDef getTypesDef() {
        return TypeUtils.getTypesDef(
                getEnumTypeDefinitions(),
                getStructTypeDefinitions(),
                getTraitTypeDefinitions(),
                getClassTypeDefinitions()
        );
    }

    public String getDataModelAsJSON() {
        return TypesSerialization.toJson(getTypesDef());
    }

    public ImmutableList<EnumTypeDefinition> getEnumTypeDefinitions() {
        return ImmutableList.copyOf(enumTypeDefinitionMap.values());
    }

    public ImmutableList<StructTypeDefinition> getStructTypeDefinitions() {
        return ImmutableList.copyOf(structTypeDefinitionMap.values());
    }

    public ImmutableList<HierarchicalTypeDefinition<ClassType>> getClassTypeDefinitions() {
        return ImmutableList.copyOf(classTypeDefinitions.values());
    }

    public ImmutableList<HierarchicalTypeDefinition<TraitType>> getTraitTypeDefinitions() {
        return ImmutableList.of();
    }

    private void createHiveObjectTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("GLOBAL", 1),
                new EnumValue("DATABASE", 2),
                new EnumValue("TABLE", 3),
                new EnumValue("PARTITION", 4),
                new EnumValue("COLUMN", 5),
        };

        EnumTypeDefinition definition = new EnumTypeDefinition(
                HiveDataTypes.HIVE_OBJECTTYPE.name(), values);
        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_OBJECTTYPE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_OBJECTTYPE.name());
    }

    private void createHivePrincipalTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("USER", 1),
                new EnumValue("ROLE", 2),
                new EnumValue("GROUP", 3),
        };

        EnumTypeDefinition definition = new EnumTypeDefinition(
                HiveDataTypes.HIVE_PRINCIPALTYPE.name(), values);

        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_PRINCIPALTYPE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PRINCIPALTYPE.name());
    }

    private void createFunctionTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("JAVA", 1),
        };

        EnumTypeDefinition definition = new EnumTypeDefinition(
                HiveDataTypes.HIVE_FUNCTIONTYPE.name(), values);
        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_FUNCTIONTYPE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_FUNCTIONTYPE.name());
    }

    private void createResourceTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("JAR", 1),
                new EnumValue("FILE", 2),
                new EnumValue("ARCHIVE", 3),
        };
        EnumTypeDefinition definition = new EnumTypeDefinition(
                HiveDataTypes.HIVE_RESOURCETYPE.name(), values);
        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_RESOURCETYPE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_RESOURCETYPE.name());
    }

    private void createSerDeStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serializationLib", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };
        StructTypeDefinition definition = new StructTypeDefinition(HiveDataTypes.HIVE_SERDE.name(),
                attributeDefinitions);
        structTypeDefinitionMap.put(HiveDataTypes.HIVE_SERDE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_SERDE.name());
    }

    /*
    private static final DataTypes.ArrayType STRING_ARRAY_TYPE =
            new DataTypes.ArrayType(DataTypes.STRING_TYPE);
    private static Multiplicity ZeroOrMore = new Multiplicity(0, Integer.MAX_VALUE, true);
    private void createSkewedInfoStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("skewedColNames",
                        String.format("array<%s>", DataTypes.STRING_TYPE.getName()),
                        ZeroOrMore, false, null),
                new AttributeDefinition("skewedColValues",
                        String.format("array<%s>", STRING_ARRAY_TYPE.getName()),
                        ZeroOrMore, false, null),
                new AttributeDefinition("skewedColValueLocationMaps", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };
        StructTypeDefinition definition = new StructTypeDefinition(
                DefinedTypes.HIVE_SKEWEDINFO.name(), attributeDefinitions);

        structTypeDefinitionMap.put(DefinedTypes.HIVE_SKEWEDINFO.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_SKEWEDINFO.name());
    }
    */

    private void createOrderStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("col", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("order", DataTypes.INT_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
        };

        StructTypeDefinition definition = new StructTypeDefinition(
                HiveDataTypes.HIVE_ORDER.name(), attributeDefinitions);
        structTypeDefinitionMap.put(HiveDataTypes.HIVE_ORDER.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_ORDER.name());
    }

    private void createStorageDescClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("cols",
                        String.format("array<%s>", HiveDataTypes.HIVE_COLUMN.name()),
                        Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("location", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("inputFormat", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("outputFormat", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("compressed", DataTypes.BOOLEAN_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("numBuckets", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serdeInfo", HiveDataTypes.HIVE_SERDE.name(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("bucketCols",
                        String.format("array<%s>", DataTypes.STRING_TYPE.getName()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sortCols",
                        String.format("array<%s>", HiveDataTypes.HIVE_ORDER.name()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                //new AttributeDefinition("skewedInfo", DefinedTypes.HIVE_SKEWEDINFO.name(),
                // Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("storedAsSubDirectories", DataTypes.BOOLEAN_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition = new HierarchicalTypeDefinition<>(
                ClassType.class, HiveDataTypes.HIVE_STORAGEDESC.name(), null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_STORAGEDESC.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_STORAGEDESC.name());
    }

    /** Revisit later after nested array types are handled by the typesystem **/

    private void createResourceUriStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("resourceType", HiveDataTypes.HIVE_RESOURCETYPE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("uri", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
        };
        StructTypeDefinition definition = new StructTypeDefinition(
                HiveDataTypes.HIVE_RESOURCEURI.name(), attributeDefinitions);
        structTypeDefinitionMap.put(HiveDataTypes.HIVE_RESOURCEURI.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_RESOURCEURI.name());
    }

    private void createDBClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("description", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("locationUri", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerType", HiveDataTypes.HIVE_PRINCIPALTYPE.name(),
                        Multiplicity.OPTIONAL, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_DB.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_DB.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_DB.name());
    }

    private void createTypeClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("type1", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("type2", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("fields", String.format("array<%s>",
                        HiveDataTypes.HIVE_COLUMN.name()), Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_TYPE.name(),
                        null, attributeDefinitions);

        classTypeDefinitions.put(HiveDataTypes.HIVE_TYPE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_TYPE.name());
    }

    private void createColumnClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                //new AttributeDefinition("type", DefinedTypes.HIVE_TYPE.name(), Multiplicity
                // .REQUIRED, false, null),
                new AttributeDefinition("type", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("comment", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_COLUMN.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_COLUMN.name(), definition);

        LOG.debug("Created definition for " + HiveDataTypes.HIVE_COLUMN.name());
    }

    private void createPartitionClass() throws MetadataException {

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("values", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("dbName", HiveDataTypes.HIVE_DB.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("tableName", HiveDataTypes.HIVE_TABLE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("lastAccessTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sd", HiveDataTypes.HIVE_STORAGEDESC.name(),
                        Multiplicity.REQUIRED, false, null),
                //new AttributeDefinition("columns", String.format("array<%s>", DefinedTypes
                // .HIVE_COLUMN.name()),
                //        Multiplicity.COLLECTION, true, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),

        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class,
                        HiveDataTypes.HIVE_PARTITION.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_PARTITION.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PARTITION.name());
    }

    private void createTableClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("tableName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("dbName", HiveDataTypes.HIVE_DB.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("owner", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("lastAccessTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("retention", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sd", HiveDataTypes.HIVE_STORAGEDESC.name(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("partitionKeys",
                        String.format("array<%s>", HiveDataTypes.HIVE_COLUMN.name()),
                        Multiplicity.OPTIONAL, false, null),
                //new AttributeDefinition("columns", String.format("array<%s>", DefinedTypes
                // .HIVE_COLUMN.name()),
                //        Multiplicity.COLLECTION, true, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("viewOriginalText", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("viewExpandedText", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("tableType", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("temporary", DataTypes.BOOLEAN_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_TABLE.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_TABLE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_TABLE.name());
    }

    private void createIndexClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("indexName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("indexHandlerClass", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("dbName", HiveDataTypes.HIVE_DB.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("lastAccessTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("origTableName", HiveDataTypes.HIVE_TABLE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("indexTableName", HiveDataTypes.HIVE_TABLE.name(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sd", HiveDataTypes.HIVE_STORAGEDESC.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("deferredRebuild", DataTypes.BOOLEAN_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_INDEX.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_INDEX.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_INDEX.name());
    }

    private void createFunctionClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("functionName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("dbName", HiveDataTypes.HIVE_DB.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("className", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerName", DataTypes.INT_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerType", HiveDataTypes.HIVE_PRINCIPALTYPE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("functionType", HiveDataTypes.HIVE_FUNCTIONTYPE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("resourceUris", HiveDataTypes.HIVE_RESOURCEURI.name(),
                        Multiplicity.COLLECTION, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_FUNCTION.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_FUNCTION.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_FUNCTION.name());
    }

    private void createRoleClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("roleName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("ownerName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_ROLE.name(),
                        null, attributeDefinitions);

        classTypeDefinitions.put(HiveDataTypes.HIVE_ROLE.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_ROLE.name());
    }

    private void createProcessClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("processName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("startTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("endTime", DataTypes.INT_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("userName", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("sourceTableNames",
                        String.format("array<%s>", HiveDataTypes.HIVE_TABLE.name()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("targetTableNames",
                        String.format("array<%s>", HiveDataTypes.HIVE_TABLE.name()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("queryText", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("queryPlan", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("queryId", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("queryGraph", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition = new HierarchicalTypeDefinition<>(
                ClassType.class, HiveDataTypes.HIVE_PROCESS.name(), null, attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_PROCESS.name(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PROCESS.name());
    }

    public static void main(String[] args) throws Exception {
        HiveDataModelGenerator hiveDataModelGenerator = new HiveDataModelGenerator();
        hiveDataModelGenerator.createDataModel();
        String hiveDataModelAsJSON = hiveDataModelGenerator.getDataModelAsJSON();
        System.out.println("hiveDataModelAsJSON = " + hiveDataModelAsJSON);
    }
}
