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
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.addons.ModelDefinitionDump;
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
    public static final String PARAMETERS = "parameters";
    public static final String COLUMNS = "columns";

    public static final String STORAGE_NUM_BUCKETS = "numBuckets";
    public static final String STORAGE_IS_STORED_AS_SUB_DIRS = "storedAsSubDirectories";

    public static final String NAME = "name";
    public static final String TABLE_NAME = "tableName";
    public static final String TABLE = "table";
    public static final String DB = "db";

    public static final String STORAGE_DESC = "sd";
    public static final String STORAGE_DESC_INPUT_FMT = "inputFormat";
    public static final String STORAGE_DESC_OUTPUT_FMT = "outputFormat";
    public static final String OWNER = "owner";

    public HiveDataModelGenerator() {
        classTypeDefinitions = new HashMap<>();
        enumTypeDefinitionMap = new HashMap<>();
        structTypeDefinitionMap = new HashMap<>();
    }

    public void createDataModel() throws AtlasException {
        LOG.info("Generating the Hive Data Model....");

        // enums
        createHivePrincipalTypeEnum();
        // structs
        createSerDeStruct();
        createOrderStruct();
        createStorageDescClass();

        // classes
        createDBClass();
        createColumnClass();
        createTableClass();

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

    private void createHivePrincipalTypeEnum() throws AtlasException {
        EnumValue values[] = {new EnumValue("USER", 1), new EnumValue("ROLE", 2), new EnumValue("GROUP", 3),};

        EnumTypeDefinition definition = new EnumTypeDefinition(HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName(), values);

        enumTypeDefinitionMap.put(HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName());
    }

    private void createSerDeStruct() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serializationLib", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),
                new AttributeDefinition(HiveDataModelGenerator.PARAMETERS, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),};
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
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_STORAGEDESC.getName(), null,
                        ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_STORAGEDESC.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_STORAGEDESC.getName());
    }

    /** Revisit later after nested array types are handled by the typesystem **/

    private void createDBClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition("description", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("locationUri", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition(HiveDataModelGenerator.PARAMETERS, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(OWNER, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("ownerType", HiveDataTypes.HIVE_PRINCIPAL_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_DB.getName(), null,
                    ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_DB.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_DB.getName());
    }

    private void createColumnClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("type", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(COMMENT, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_COLUMN.getName(), null,
                    ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_COLUMN.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_COLUMN.getName());
    }

    private void createTableClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(TABLE_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(DB, HiveDataTypes.HIVE_DB.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(OWNER, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("lastAccessTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition(COMMENT, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("retention", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(STORAGE_DESC, HiveDataTypes.HIVE_STORAGEDESC.getName(), Multiplicity.OPTIONAL, true,
                        null),
                new AttributeDefinition("partitionKeys", DataTypes.arrayTypeName(HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.OPTIONAL, true, null),
                new AttributeDefinition("columns", DataTypes.arrayTypeName(HiveDataTypes.HIVE_COLUMN.getName()),
                        Multiplicity.OPTIONAL, true, null),
                new AttributeDefinition(HiveDataModelGenerator.PARAMETERS, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("viewOriginalText", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),
                new AttributeDefinition("viewExpandedText", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL,
                        false, null),
                new AttributeDefinition("tableType", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition("temporary", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),};
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_TABLE.getName(), null,
                    ImmutableSet.of("DataSet"), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_TABLE.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_TABLE.getName());
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
                new HierarchicalTypeDefinition<>(ClassType.class, HiveDataTypes.HIVE_PROCESS.getName(), null,
                    ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(HiveDataTypes.HIVE_PROCESS.getName(), definition);
        LOG.debug("Created definition for " + HiveDataTypes.HIVE_PROCESS.getName());
    }

    public String getModelAsJson() throws AtlasException {
        createDataModel();
        return getDataModelAsJSON();
    }

    public static void main(String[] args) throws Exception {
        HiveDataModelGenerator hiveDataModelGenerator = new HiveDataModelGenerator();
        String modelAsJson = hiveDataModelGenerator.getModelAsJson();

        if (args.length==1) {
            ModelDefinitionDump.dumpModelToFile(args[0], modelAsJson);
            return;
        }

        System.out.println("hiveDataModelAsJSON = " + modelAsJson);

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
