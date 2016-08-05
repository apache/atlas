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

package org.apache.atlas.sqoop.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.addons.ModelDefinitionDump;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
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
 * Utility that generates Sqoop data model for both metastore entities and DDL/DML queries.
 */
public class SqoopDataModelGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SqoopDataModelGenerator.class);

    private final Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions;
    private final Map<String, EnumTypeDefinition> enumTypeDefinitionMap;
    private final Map<String, StructTypeDefinition> structTypeDefinitionMap;
    private static final DataTypes.MapType STRING_MAP_TYPE =
            new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE);

    public static final String USER = "userName";
    public static final String DB_STORE_TYPE = "dbStoreType";
    public static final String DB_STORE_USAGE = "storeUse";
    public static final String SOURCE = "source";
    public static final String DESCRIPTION = "description";
    public static final String STORE_URI = "storeUri";
    public static final String OPERATION = "operation";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String CMD_LINE_OPTS = "commandlineOpts";
    // multiple inputs and outputs for process
    public static final String INPUTS = "inputs";
    public static final String OUTPUTS = "outputs";

    public SqoopDataModelGenerator() {
        classTypeDefinitions = new HashMap<>();
        enumTypeDefinitionMap = new HashMap<>();
        structTypeDefinitionMap = new HashMap<>();
    }

    public void createDataModel() throws AtlasException {
        LOG.info("Generating the Sqoop Data Model....");

        // enums

        // structs

        // classes
        createSqoopDbStoreClass();

        // DDL/DML Process
        createSqoopProcessClass();
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

    private void createSqoopDbStoreClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(DB_STORE_TYPE,
                        DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, false, true, null),
                new AttributeDefinition(DB_STORE_USAGE,
                        DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(STORE_URI,
                        DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(SOURCE,
                        DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, false, true, null)
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, SqoopDataTypes.SQOOP_DBDATASTORE.getName(), null,
                    ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(SqoopDataTypes.SQOOP_DBDATASTORE.getName(), definition);
        LOG.debug("Created definition for " + SqoopDataTypes.SQOOP_DBDATASTORE.getName());
    }


    private void createSqoopProcessClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(OPERATION,
                        DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, false, true, null),
                new AttributeDefinition(CMD_LINE_OPTS, STRING_MAP_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(START_TIME, DataTypes.DATE_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(END_TIME, DataTypes.DATE_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition(USER,
                        DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, false, true, null),
        };

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, SqoopDataTypes.SQOOP_PROCESS.getName(), null,
                    ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(SqoopDataTypes.SQOOP_PROCESS.getName(), definition);
        LOG.debug("Created definition for " + SqoopDataTypes.SQOOP_PROCESS.getName());
    }

    public String getModelAsJson() throws AtlasException {
        createDataModel();
        return getDataModelAsJSON();
    }

    public static void main(String[] args) throws Exception {
        SqoopDataModelGenerator dataModelGenerator = new SqoopDataModelGenerator();
        String modelAsJson = dataModelGenerator.getModelAsJson();

        if (args.length == 1) {
            ModelDefinitionDump.dumpModelToFile(args[0], modelAsJson);
            return;
        }

        System.out.println("sqoopDataModelAsJSON = " + modelAsJson);

        TypesDef typesDef = dataModelGenerator.getTypesDef();
        for (EnumTypeDefinition enumType : typesDef.enumTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - values %s", enumType.name, EnumType.class.getSimpleName(),
                    Arrays.toString(enumType.enumValues)));
        }

        for (HierarchicalTypeDefinition<ClassType> classType : typesDef.classTypesAsJavaList()) {
            System.out.println(
                    String.format("%s(%s) - super types [%s] - attributes %s", classType.typeName,
                            ClassType.class.getSimpleName(), StringUtils.join(classType.superTypes, ","),
                            Arrays.toString(classType.attributeDefinitions)));
        }

    }
}
