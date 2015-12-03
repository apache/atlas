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

package org.apache.atlas.hive.model;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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

    public static final String COMMENT = "comment";

    public static final String STORAGE_NUM_BUCKETS = "numBuckets";
    public static final String STORAGE_IS_STORED_AS_SUB_DIRS = "storedAsSubDirectories";

    public static final String NAME = "name";
    public static final String TABLE_NAME = "tableName";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String TABLE = "table";
    public static final String DB = "db";

    public HiveDataModelGenerator() {
        classTypeDefinitions = new HashMap<>();
        enumTypeDefinitionMap = new HashMap<>();
        structTypeDefinitionMap = new HashMap<>();
    }

    public void createDataModel() throws AtlasException {
        LOG.info("Generating the Hive Data Model....");

        // enums
        createHiveObjectTypeEnum();
        createHivePrincipalTypeEnum();
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
        createRoleClass();

        // DDL/DML Process
        createProcessClass();
    }

    public TypesDef getTypesDef() {
        return TypesUtil.getTypesDef(getEnumTypeDefinitions(), getStructTypeDefinitions(), getTraitTypeDefinitions(),
                getClassTypeDefinitions());
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

    private void createHiveObjectTypeEnum() throws AtlasException {
        EnumValue values[] = {new EnumValue("GLOBAL", 1), new EnumValue("DATABASE", 2), new EnumValue("TABLE", 3),
                new EnumValue("PARTITION", 4), new EnumValue("COLUMN", 5),};

        EnumTypeDefinition definition = new EnumTypeDefinition(HiveDataTypes.HIVE_OBJECT_TYPE.getName(), values);
        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_OBJECT_TYPE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_OBJECT_TYPE.getName());
    }

    private void createHivePrincipalTypeEnum() throws AtlasException {
        EnumValue values[] = {new EnumValue("USER", 1), new EnumValue("ROLE", 2), new EnumValue("GROUP", 3),};

        EnumTypeDefinition definition = new EnumTypeDefinition(HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName(), values);

        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName());
    }

    private void createResourceTypeEnum() throws AtlasException {
        EnumValue values[] = {new EnumValue("JAR", 1), new EnumValue("FILE", 2), new EnumValue("ARCHIVE", 3),};
        EnumTypeDefinition definition = new EnumTypeDefinition(HiveDataTypes.HIVE_RESOURCE_TYPE.getName(), values);
        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_RESOURCE_TYPE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_RESOURCE_TYPE.getName());
    }

    private void createSerDeStruct() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serializationLib", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),};
        StructTypeDefinition definition =
                new StructTypeDefinition(HiveDataTypes.HIVE_SERDE.getName(), attributeDefinitions);
        structTypeDefinitionMap.put(HiveDataTypes.HIVE_SERDE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_SERDE.getName());
    }

    private void createOrderStruct() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("col", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("order", DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null),};

        StructTypeDefinition definition =
                new StructTypeDefinition(HiveDataTypes.HIVE_ORDER.getName(), attributeDefinitions);
        structTypeDefinitionMap.put(HiveDataTypes.HIVE_ORDER.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_ORDER.getName());
    }

    private void createStorageDescClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("cols", String.format("array<%s>", HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.COLLECTION, true, null),
                new AttributeDefinition("location", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("inputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("outputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("compressed", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(STORAGE_NUM_BUCKETS, DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("serdeInfo", HiveDataTypes.HIVE_SERDE.getName(), Multiplicity.OPTIONAL, true,
                        null),
                new AttributeDefinition("bucketCols", String.format("array<%s>", DataTypes.STRING_TYPE.getName()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sortCols", String.format("array<%s>", HiveDataTypes.HIVE_ORDER.getName()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                //new AttributeDefinition("skewedInfo", DefinedTypes.HIVE_SKEWEDINFO.getName(),
                // Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(STORAGE_IS_STORED_AS_SUB_DIRS, DataTypes.BOOLEAN_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_STORAGEDESC.getName(),
                        ImmutableList.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_STORAGEDESC.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_STORAGEDESC.getName());
    }

    /** Revisit later after nested array types are handled by the typesystem **/

    private void createResourceUriStruct() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("resourceType", HiveDataTypes.HIVE_RESOURCE_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("uri", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),};
        StructTypeDefinition definition =
                new StructTypeDefinition(HiveDataTypes.HIVE_RESOURCEURI.getName(), attributeDefinitions);
        structTypeDefinitionMap.put(HiveDataTypes.HIVE_RESOURCEURI.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_RESOURCEURI.getName());
    }

    private void createDBClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(CLUSTER_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("description", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("locationUri", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerName", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("ownerType", HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_DB.getName(),
                        ImmutableList.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_DB.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_DB.getName());
    }

    private void createTypeClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("type1", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("type2", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("fields", String.format("array<%s>", HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.OPTIONAL, false, null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_TYPE.getName(), null,
                        attributeDefinitions);

        classTypeDefinitions.put(HiveDataTypes.HIVE_TYPE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_TYPE.getName());
    }

    private void createColumnClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("type", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(COMMENT, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_COLUMN.getName(),
                        ImmutableList.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_COLUMN.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_COLUMN.getName());
    }

    private void createPartitionClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("values", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(TABLE, HiveDataTypes.HIVE_TABLE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("lastAccessTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("sd", HiveDataTypes.HIVE_STORAGEDESC.getName(), Multiplicity.REQUIRED, true,
                        null),
                new AttributeDefinition("columns", DataTypes.arrayTypeName(HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.OPTIONAL, true, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_PARTITION.getName(),
                        ImmutableList.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_PARTITION.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PARTITION.getName());
    }

    private void createTableClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(TABLE_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(DB, HiveDataTypes.HIVE_DB.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("owner", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("lastAccessTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition(COMMENT, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("retention", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sd", HiveDataTypes.HIVE_STORAGEDESC.getName(), Multiplicity.OPTIONAL, true,
                        null),
                new AttributeDefinition("partitionKeys", DataTypes.arrayTypeName(HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.OPTIONAL, true, null),
                new AttributeDefinition("columns", DataTypes.arrayTypeName(HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.OPTIONAL, true, null),
                new AttributeDefinition("parameters", STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("viewOriginalText", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),
                new AttributeDefinition("viewExpandedText", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),
                new AttributeDefinition("tableType", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("temporary", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_TABLE.getName(),
                        ImmutableList.of("DataSet"), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_TABLE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_TABLE.getName());
    }

    private void createRoleClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("roleName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("ownerName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_ROLE.getName(), null,
                        attributeDefinitions);

        classTypeDefinitions.put(HiveDataTypes.HIVE_ROLE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_ROLE.getName());
    }

    private void createProcessClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("startTime", DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("endTime", DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("userName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("operationType", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("queryText", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("queryPlan", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("queryId", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("queryGraph", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_PROCESS.getName(),
                        ImmutableList.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_PROCESS.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PROCESS.getName());
    }

    public String getModelAsJson() throws AtlasException {
        createDataModel();
        return getDataModelAsJSON();
    }

    public static void main(String[] args) throws Exception {
        HiveDataModelGenerator hiveDataModelGenerator = new HiveDataModelGenerator();
        System.out.println("hiveDataModelAsJSON = " + hiveDataModelGenerator.getModelAsJson());

        TypesDef typesDef = hiveDataModelGenerator.getTypesDef();
        for (EnumTypeDefinition enumType : typesDef.enumTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - values %s", enumType.name, EnumType.class.getSimpleName(),
                    Arrays.toString(enumType.enumValues)));
        }
        for (StructTypeDefinition structType : typesDef.structTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - attributes %s", structType.typeName, StructType.class.getSimpleName(),
                    Arrays.toString(structType.attributeDefinitions)));
        }
        for (HierarchicalTypeDefinition<ClassType> classType : typesDef.classTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - super types [%s] - attributes %s", classType.typeName, ClassType.class.getSimpleName(),
                    StringUtils.join(classType.superTypes, ","), Arrays.toString(classType.attributeDefinitions)));
        }
        for (HierarchicalTypeDefinition<TraitType> traitType : typesDef.traitTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - %s", traitType.typeName, TraitType.class.getSimpleName(),
                    Arrays.toString(traitType.attributeDefinitions)));
        }
    }
}
