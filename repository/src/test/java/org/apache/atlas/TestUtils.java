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

package org.apache.atlas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createUniqueRequiredAttrDef;

/**
 * Test utility class.
 */
public final class TestUtils {

    public static final long TEST_DATE_IN_LONG = 1418265358440L;

    private TestUtils() {
    }

    /**
     * Dumps the graph in GSON format in the path returned.
     *
     * @param titanGraph handle to graph
     * @return path to the dump file
     * @throws Exception
     */
    public static String dumpGraph(TitanGraph titanGraph) throws Exception {
        File tempFile = File.createTempFile("graph", ".gson");
        System.out.println("tempFile.getPath() = " + tempFile.getPath());
        GraphSONWriter.outputGraph(titanGraph, tempFile.getPath());

        GraphHelper.dumpToLog(titanGraph);
        return tempFile.getPath();
    }

    /**
     * Class Hierarchy is:
     * Department(name : String, employees : Array[Person])
     * Person(name : String, department : Department, manager : Manager)
     * Manager(subordinates : Array[Person]) extends Person
     * <p/>
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    public static void defineDeptEmployeeTypes(TypeSystem ts) throws AtlasException {

        String _description = "_description";
        EnumTypeDefinition orgLevelEnum =
                new EnumTypeDefinition("OrgLevel", "OrgLevel"+_description, new EnumValue("L1", 1), new EnumValue("L2", 2));

        StructTypeDefinition addressDetails =
                createStructTypeDef("Address", "Address"+_description, createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("city", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef(DEPARTMENT_TYPE, "Department"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.OPTIONAL,
                        true, "department"));

        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person", "Person"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                createOptionalAttrDef("orgLevel", "OrgLevel"),
                createOptionalAttrDef("address", "Address"),
                new AttributeDefinition("department", "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates"),
                new AttributeDefinition("mentor", "Person", Multiplicity.OPTIONAL, false, null));

        HierarchicalTypeDefinition<ClassType> managerTypeDef = createClassTypeDef("Manager", "Manager"+_description, ImmutableSet.of("Person"),
                new AttributeDefinition("subordinates", String.format("array<%s>", "Person"), Multiplicity.COLLECTION,
                        false, "manager"));

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef =
                createTraitTypeDef("SecurityClearance", "SecurityClearance"+_description, ImmutableSet.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, managerTypeDef));
    }

    public static final String DEPARTMENT_TYPE = "Department";
    public static final String PERSON_TYPE = "Person";

    public static ITypedReferenceableInstance createDeptEg1(TypeSystem ts) throws AtlasException {
        Referenceable hrDept = new Referenceable(DEPARTMENT_TYPE);
        Referenceable john = new Referenceable(PERSON_TYPE);

        Referenceable jane = new Referenceable("Manager", "SecurityClearance");
        Referenceable johnAddr = new Referenceable("Address");
        Referenceable janeAddr = new Referenceable("Address");
        Referenceable julius = new Referenceable("Manager");
        Referenceable juliusAddr = new Referenceable("Address");
        Referenceable max = new Referenceable("Person");
        Referenceable maxAddr = new Referenceable("Address");
        
        hrDept.set("name", "hr");
        john.set("name", "John");
        john.set("department", hrDept);
        johnAddr.set("street", "Stewart Drive");
        johnAddr.set("city", "Sunnyvale");
        john.set("address", johnAddr);

        jane.set("name", "Jane");
        jane.set("department", hrDept);
        janeAddr.set("street", "Great America Parkway");
        janeAddr.set("city", "Santa Clara");
        jane.set("address", janeAddr);

        julius.set("name", "Julius");
        julius.set("department", hrDept);
        juliusAddr.set("street", "Madison Ave");
        juliusAddr.set("city", "Newtonville");
        julius.set("address", juliusAddr);
        julius.set("subordinates", ImmutableList.<Referenceable>of());
        
        max.set("name", "Max");
        max.set("department", hrDept);
        maxAddr.set("street", "Ripley St");
        maxAddr.set("city", "Newton");
        max.set("address", maxAddr);
        max.set("manager", jane);
        max.set("mentor", julius);
        
        john.set("manager", jane);
        john.set("mentor", max);
        hrDept.set("employees", ImmutableList.of(john, jane, julius, max));

        jane.set("subordinates", ImmutableList.of(john, max));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);
        Assert.assertNotNull(hrDept2);

        return hrDept2;
    }

    public static final String DATABASE_TYPE = "hive_database";
    public static final String DATABASE_NAME = "foo";
    public static final String TABLE_TYPE = "hive_table";
    public static final String PROCESS_TYPE = "hive_process";
    public static final String COLUMN_TYPE = "column_type";
    public static final String TABLE_NAME = "bar";
    public static final String CLASSIFICATION = "classification";
    public static final String PII = "PII";
    public static final String SUPER_TYPE_NAME = "Base";
    public static final String STORAGE_DESC_TYPE = "hive_storagedesc";
    public static final String PARTITION_STRUCT_TYPE = "partition_struct_type";
    public static final String PARTITION_CLASS_TYPE = "partition_class_type";
    public static final String SERDE_TYPE = "serdeType";
    public static final String COLUMNS_MAP = "columnsMap";
    public static final String COLUMNS_ATTR_NAME = "columns";

    public static final String NAME = "name";

    public static TypesDef defineHiveTypes() {
        String _description = "_description";
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef(SUPER_TYPE_NAME, ImmutableSet.<String>of(),
                        createOptionalAttrDef("namespace", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("cluster", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("colo", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE + _description,ImmutableSet.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));


        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("serdeType", "serdeType" + _description,
                new AttributeDefinition[]{createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                    createRequiredAttrDef("serde", DataTypes.STRING_TYPE),
                    createOptionalAttrDef("description", DataTypes.STRING_TYPE)});

        EnumValue values[] = {new EnumValue("MANAGED", 1), new EnumValue("EXTERNAL", 2),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", "tableType" + _description, values);

        HierarchicalTypeDefinition<ClassType> columnsDefinition =
                createClassTypeDef(COLUMN_TYPE, ImmutableSet.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE));

        StructTypeDefinition partitionDefinition = new StructTypeDefinition("partition_struct_type", "partition_struct_type" + _description,
                new AttributeDefinition[]{createRequiredAttrDef("name", DataTypes.STRING_TYPE),});

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
            new AttributeDefinition("location", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("inputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("outputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("compressed", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.REQUIRED, false,
                null),
            new AttributeDefinition("numBuckets", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            };

        HierarchicalTypeDefinition<ClassType> storageDescClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, STORAGE_DESC_TYPE, STORAGE_DESC_TYPE + _description,
                ImmutableSet.of(SUPER_TYPE_NAME), attributeDefinitions);

        AttributeDefinition[] partClsAttributes = new AttributeDefinition[]{
            new AttributeDefinition("values", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                Multiplicity.OPTIONAL, false, null),
            new AttributeDefinition("table", TABLE_TYPE, Multiplicity.REQUIRED, false, null),
            new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("lastAccessTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("sd", STORAGE_DESC_TYPE, Multiplicity.REQUIRED, true,
                null),
            new AttributeDefinition("columns", DataTypes.arrayTypeName(COLUMN_TYPE),
                Multiplicity.OPTIONAL, true, null),
            new AttributeDefinition("parameters", new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE).getName(), Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> partClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, "partition_class_type", "partition_class_type" + _description,
                ImmutableSet.of(SUPER_TYPE_NAME), partClsAttributes);

        HierarchicalTypeDefinition<ClassType> processClsType =
                new HierarchicalTypeDefinition<>(ClassType.class, PROCESS_TYPE, PROCESS_TYPE + _description,
                        ImmutableSet.<String>of(), new AttributeDefinition[]{
                        new AttributeDefinition("outputs", "array<" + TABLE_TYPE + ">", Multiplicity.OPTIONAL, false, null)
                });

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE, TABLE_TYPE + _description, ImmutableSet.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        // enum
                        new AttributeDefinition("tableType", "tableType", Multiplicity.REQUIRED, false, null),
                        // array of strings
                        new AttributeDefinition("columnNames",
                                String.format("array<%s>", DataTypes.STRING_TYPE.getName()), Multiplicity.OPTIONAL,
                                false, null),
                        // array of classes
                        new AttributeDefinition("columns", String.format("array<%s>", COLUMN_TYPE),
                                Multiplicity.OPTIONAL, true, null),
                        // array of structs
                        new AttributeDefinition("partitions", String.format("array<%s>", "partition_struct_type"),
                                Multiplicity.OPTIONAL, true, null),
                        // map of primitives
                        new AttributeDefinition("parametersMap",
                                DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                                Multiplicity.OPTIONAL, true, null),
                        //map of classes -
                        new AttributeDefinition(COLUMNS_MAP,
                                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                                                                COLUMN_TYPE),
                                                        Multiplicity.OPTIONAL, true, null),
                         //map of structs
                        new AttributeDefinition("partitionsMap",
                                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                                                                "partition_struct_type"),
                                                        Multiplicity.OPTIONAL, true, null),
                        // struct reference
                        new AttributeDefinition("serde1", "serdeType", Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition("serde2", "serdeType", Multiplicity.OPTIONAL, false, null),
                        // class reference
                        new AttributeDefinition("database", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                        //class reference as composite
                        new AttributeDefinition("databaseComposite", DATABASE_TYPE, Multiplicity.OPTIONAL, true, null));

        HierarchicalTypeDefinition<TraitType> piiTypeDefinition =
                createTraitTypeDef(PII, PII + _description, ImmutableSet.<String>of());

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition =
                createTraitTypeDef(CLASSIFICATION, CLASSIFICATION + _description, ImmutableSet.<String>of(),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> fetlClassificationTypeDefinition =
                createTraitTypeDef("fetl" + CLASSIFICATION, "fetl" + CLASSIFICATION + _description, ImmutableSet.of(CLASSIFICATION),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition),
                ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition, fetlClassificationTypeDefinition, piiTypeDefinition),
                ImmutableList.of(superTypeDefinition, databaseTypeDefinition, columnsDefinition, tableTypeDefinition,
                        storageDescClsDef, partClsDef, processClsType));
    }

    public static Collection<IDataType> createHiveTypes(TypeSystem typeSystem) throws Exception {
        if (!typeSystem.isRegistered(TABLE_TYPE)) {
            TypesDef typesDef = defineHiveTypes();
            return typeSystem.defineTypes(typesDef).values();
        }
        return new ArrayList<>();
    }

    public static final String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    public static Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, dbName);
        entity.set("description", "us db");
        return entity;
    }

    public static Referenceable createTableEntity(String dbId) {
        Referenceable entity = new Referenceable(TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, tableName);
        entity.set("description", "random table");
        entity.set("type", "type");
        entity.set("tableType", "MANAGED");
        entity.set("database", new Id(dbId, 0, DATABASE_TYPE));
        entity.set("created", new Date());
        return entity;
    }

    public static Referenceable createColumnEntity() {
        Referenceable entity = new Referenceable(COLUMN_TYPE);
        entity.set(NAME, RandomStringUtils.randomAlphanumeric(10));
        entity.set("type", "VARCHAR(32)");
        return entity;
    }
}
