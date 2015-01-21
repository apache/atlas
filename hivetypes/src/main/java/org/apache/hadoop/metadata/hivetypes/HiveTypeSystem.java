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

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.types.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveTypeSystem {

    public static final Log LOG = LogFactory.getLog(HiveTypeSystem.class);
    public static final class Holder {
        public static final HiveTypeSystem instance = new HiveTypeSystem();
    }

    private TypeSystem typeSystem;

    private boolean valid = false;

    public enum DefinedTypes {

        // Enums
        HIVE_OBJECTTYPE,
        HIVE_PRINCIPALTYPE,
        HIVE_RESOURCETYPE,
        HIVE_FUNCTIONTYPE,

        // Structs
        HIVE_SERDE,
        HIVE_STORAGEDESC,
        HIVE_SKEWEDINFO,
        HIVE_ORDER,
        HIVE_RESOURCEURI,


        // Classes
        HIVE_DB,
        HIVE_TABLE,
        HIVE_COLUMN,
        HIVE_PARTITION,
        HIVE_INDEX,
        HIVE_FUNCTION,
        HIVE_ROLE,
        HIVE_TYPE,
        //HIVE_VIEW,

    }



    private Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions;
    private Map<String, EnumTypeDefinition> enumTypeDefinitionMap;
    private Map<String, StructTypeDefinition> structTypeDefinitionMap;

    private DataTypes.MapType mapStrToStrMap;
    private DataTypes.ArrayType strArrayType;
    private Map<String, IDataType> typeMap;
    private List<IDataType> enumTypes;


    private static Multiplicity ZeroOrMore = new Multiplicity(0, Integer.MAX_VALUE, true);

    private HiveTypeSystem() {
        classTypeDefinitions = new HashMap<>();
        enumTypeDefinitionMap = new HashMap<>();
        structTypeDefinitionMap = new HashMap<>();
        typeMap = new HashMap<>();
        enumTypes = new ArrayList<>();
    }

    private void initialize() throws MetadataException {

        LOG.info("Initializing the Hive Typesystem");
        typeSystem = TypeSystem.getInstance();

        mapStrToStrMap =
                typeSystem.defineMapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE);
        strArrayType = typeSystem.defineArrayType(DataTypes.STRING_TYPE);


        createHiveObjectTypeEnum();
        createHivePrincipalTypeEnum();
        createFunctionTypeEnum();
        createResourceTypeEnum();


        createSerDeStruct();
        //createSkewedInfoStruct();
        createOrderStruct();
        createResourceUriStruct();
        createStorageDescStruct();

        createDBClass();
        createTypeClass();
        createColumnClass();
        createPartitionClass();
        createTableClass();
        createIndexClass();
        createFunctionClass();
        createRoleClass();

        for (EnumTypeDefinition def : getEnumTypeDefinitions()) {
            enumTypes.add(typeSystem.defineEnumType(def));
        }

        typeMap.putAll(
                typeSystem.defineTypes(getStructTypeDefinitions(), getTraitTypeDefinitions(), getClassTypeDefinitions()));
        valid = true;
    }


    public synchronized static HiveTypeSystem getInstance() throws MetadataException {
        HiveTypeSystem hs = Holder.instance;
        if (hs.valid) {
            LOG.info("Returning pre-initialized HiveTypeSystem singleton");
            return hs;
        }
        hs.initialize();
        return hs;
    }


    public IDataType getDataType(String typeName) {
        return typeMap.get(typeName);
    }

    public ImmutableList<HierarchicalType> getHierarchicalTypeDefinitions() {
        if (valid) {
            return ImmutableList.of(
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_DB.name()),
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_TABLE.name()),
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_COLUMN.name()),
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_PARTITION.name()),
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_INDEX.name()),
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_FUNCTION.name()),
                    (HierarchicalType) typeMap.get(DefinedTypes.HIVE_ROLE.name())
            );
        } else {
            return ImmutableList.of();
        }
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
                DefinedTypes.HIVE_OBJECTTYPE.name(), values);
        enumTypeDefinitionMap.put(DefinedTypes.HIVE_OBJECTTYPE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_OBJECTTYPE.name());
    }

    private void createHivePrincipalTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("USER", 1),
                new EnumValue("ROLE", 2),
                new EnumValue("GROUP", 3),
        };

        EnumTypeDefinition definition =  new EnumTypeDefinition(
                DefinedTypes.HIVE_PRINCIPALTYPE.name(), values);

        enumTypeDefinitionMap.put(DefinedTypes.HIVE_PRINCIPALTYPE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_PRINCIPALTYPE.name());

    }

    private void createFunctionTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("JAVA", 1),
        };

        EnumTypeDefinition definition =  new EnumTypeDefinition(
                DefinedTypes.HIVE_FUNCTIONTYPE.name(), values);
        enumTypeDefinitionMap.put(DefinedTypes.HIVE_FUNCTIONTYPE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_FUNCTIONTYPE.name());

    }

    private void createResourceTypeEnum() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("JAR", 1),
                new EnumValue("FILE", 2),
                new EnumValue("ARCHIVE", 3),
        };
        EnumTypeDefinition definition =  new EnumTypeDefinition(
                DefinedTypes.HIVE_RESOURCETYPE.name(), values);
        enumTypeDefinitionMap.put(DefinedTypes.HIVE_RESOURCETYPE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_RESOURCETYPE.name());

    }


    private void createSerDeStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serializationLib", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("parameters", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),
        };
        StructTypeDefinition definition = new StructTypeDefinition(DefinedTypes.HIVE_SERDE.name(), attributeDefinitions);
        structTypeDefinitionMap.put(DefinedTypes.HIVE_SERDE.name(), definition);

        LOG.debug("Created definition for " + DefinedTypes.HIVE_SERDE.name());

    }

    /** Revisit later after nested array types are handled by the typesystem **/
    /**
    private void createSkewedInfoStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("skewedColNames", String.format("array<%s>", DataTypes.STRING_TYPE.getName()),
                        ZeroOrMore, false, null),
                new AttributeDefinition("skewedColValues", String.format("array<%s>", strArrayType.getName()),
                        ZeroOrMore, false, null),
                new AttributeDefinition("skewedColValueLocationMaps", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),
        };
        StructTypeDefinition definition = new StructTypeDefinition(DefinedTypes.HIVE_SKEWEDINFO.name(), attributeDefinitions);

        structTypeDefinitionMap.put(DefinedTypes.HIVE_SKEWEDINFO.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_SKEWEDINFO.name());

    }
    **/

    private void createOrderStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("col", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("order", DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null),
        };

        StructTypeDefinition definition = new StructTypeDefinition(DefinedTypes.HIVE_ORDER.name(), attributeDefinitions);

        structTypeDefinitionMap.put(DefinedTypes.HIVE_ORDER.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_ORDER.name());

    }



    private void createStorageDescStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("cols", String.format("array<%s>", DefinedTypes.HIVE_COLUMN.name()), Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("location", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("inputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("outputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("compressed", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("numBuckets", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serdeInfo", DefinedTypes.HIVE_SERDE.name(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("bucketCols", String.format("array<%s>",DataTypes.STRING_TYPE.getName()), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sortCols", String.format("array<%s>", DefinedTypes.HIVE_ORDER.name()), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("parameters", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),
                //new AttributeDefinition("skewedInfo", DefinedTypes.HIVE_SKEWEDINFO.name(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("storedAsSubDirectories", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),

        };

        StructTypeDefinition definition =
                new StructTypeDefinition(DefinedTypes.HIVE_STORAGEDESC.name(), attributeDefinitions);

        structTypeDefinitionMap.put(DefinedTypes.HIVE_STORAGEDESC.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_STORAGEDESC.name());

    }

    private void createResourceUriStruct() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("resourceType", DefinedTypes.HIVE_RESOURCETYPE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("uri", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
        };
        StructTypeDefinition definition = new StructTypeDefinition(DefinedTypes.HIVE_RESOURCEURI.name(), attributeDefinitions);
        structTypeDefinitionMap.put(DefinedTypes.HIVE_RESOURCEURI.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_RESOURCEURI.name());

    }


    private void createDBClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("description", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("locationUri", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("parameters", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerName", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerType", DefinedTypes.HIVE_PRINCIPALTYPE.name(), Multiplicity.OPTIONAL, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_DB.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(DefinedTypes.HIVE_DB.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_DB.name());


    }

    private void createTypeClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("type1", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("type2", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("fields", String.format("array<%s>",
                        DefinedTypes.HIVE_COLUMN.name()), Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_TYPE.name(),
                        null, attributeDefinitions);

        classTypeDefinitions.put(DefinedTypes.HIVE_TYPE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_TYPE.name());

    }


    private void createColumnClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                //new AttributeDefinition("type", DefinedTypes.HIVE_TYPE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("type", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("comment", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_COLUMN.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(DefinedTypes.HIVE_COLUMN.name(), definition);

        LOG.debug("Created definition for " + DefinedTypes.HIVE_COLUMN.name());


    }


    private void createPartitionClass() throws MetadataException {

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("values", DataTypes.STRING_TYPE.getName(), Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("dbName", DefinedTypes.HIVE_DB.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("tableName", DefinedTypes.HIVE_TABLE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("lastAccessTime", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sd", DefinedTypes.HIVE_STORAGEDESC.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("parameters", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),

        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_PARTITION.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(DefinedTypes.HIVE_PARTITION.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_PARTITION.name());


    }

    private void createTableClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("tableName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("dbName", DefinedTypes.HIVE_DB.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("owner", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("lastAccessTime", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("retention", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("sd", DefinedTypes.HIVE_STORAGEDESC.name(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("partitionKeys", String.format("array<%s>", DefinedTypes.HIVE_COLUMN.name()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("parameters", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("viewOriginalText", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("viewExpandedText", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("tableType", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("temporary", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_TABLE.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(DefinedTypes.HIVE_TABLE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_TABLE.name());

    }

    private void createIndexClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("indexName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("indexHandleClass", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("dbName", DefinedTypes.HIVE_DB.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("lastAccessTime", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("origTableName", DefinedTypes.HIVE_TABLE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("indexTableName", DefinedTypes.HIVE_TABLE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("sd", DefinedTypes.HIVE_STORAGEDESC.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("parameters", mapStrToStrMap.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("deferredRebuild", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_INDEX.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(DefinedTypes.HIVE_INDEX.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_INDEX.name());

    }

    private void createFunctionClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("functionName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("dbName", DefinedTypes.HIVE_DB.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("className", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerName", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("ownerType", DefinedTypes.HIVE_PRINCIPALTYPE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("functionType", DefinedTypes.HIVE_FUNCTIONTYPE.name(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("resourceUris", DefinedTypes.HIVE_RESOURCEURI.name(), Multiplicity.COLLECTION, false, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_FUNCTION.name(),
                        null, attributeDefinitions);
        classTypeDefinitions.put(DefinedTypes.HIVE_FUNCTION.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_FUNCTION.name());

    }

    private void createRoleClass() throws MetadataException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("roleName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("createTime", DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("ownerName", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
        };
        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.HIVE_ROLE.name(),
                        null, attributeDefinitions);

        classTypeDefinitions.put(DefinedTypes.HIVE_ROLE.name(), definition);
        LOG.debug("Created definition for " + DefinedTypes.HIVE_ROLE.name());

    }

}
