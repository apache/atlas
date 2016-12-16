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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.lang.RandomStringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.type.AtlasTypeUtil.createStructTypeDef;


/**
 * Test utility class.
 */
public final class TestUtilsV2 {

    public static final long TEST_DATE_IN_LONG = 1418265358440L;

    private TestUtilsV2() {
    }

    /**
     * Class Hierarchy is:
     * Department(name : String, employees : Array[Person])
     * Person(name : String, department : Department, manager : Manager)
     * Manager(subordinates : Array[Person]) extends Person
     * <p/>
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    public static AtlasTypesDef defineDeptEmployeeTypes() {

        String _description = "_description";
        AtlasEnumDef orgLevelEnum =
                new AtlasEnumDef("OrgLevel", "OrgLevel"+_description, "1.0",
                        Arrays.asList(
                                new AtlasEnumElementDef("L1", "Element"+_description, 1),
                                new AtlasEnumElementDef("L1", "Element"+_description, 2)
                        ));

        AtlasStructDef addressDetails =
                createStructTypeDef("Address", "Address"+_description,
                        AtlasTypeUtil.createRequiredAttrDef("street", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("city", "string"));

        AtlasEntityDef deptTypeDef =
                AtlasTypeUtil.createClassTypeDef(DEPARTMENT_TYPE, "Department"+_description, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                        new AtlasAttributeDef("employees", String.format("array<%s>", "Person"), true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1, false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        AtlasEntityDef personTypeDef = AtlasTypeUtil.createClassTypeDef("Person", "Person"+_description, ImmutableSet.<String>of(),
                AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                AtlasTypeUtil.createOptionalAttrDef("address", "Address"),
                AtlasTypeUtil.createOptionalAttrDef("birthday", "date"),
                AtlasTypeUtil.createOptionalAttrDef("hasPets", "boolean"),
                AtlasTypeUtil.createOptionalAttrDef("numberOfCars", "byte"),
                AtlasTypeUtil.createOptionalAttrDef("houseNumber", "short"),
                AtlasTypeUtil.createOptionalAttrDef("carMileage", "int"),
                AtlasTypeUtil.createOptionalAttrDef("age", "float"),
                AtlasTypeUtil.createOptionalAttrDef("numberOfStarsEstimate", "biginteger"),
                AtlasTypeUtil.createOptionalAttrDef("approximationOfPi", "bigdecimal")
        );

        AtlasEntityDef employeeTypeDef = AtlasTypeUtil.createClassTypeDef("Employee", "Employee"+_description, ImmutableSet.of("Person"),
                AtlasTypeUtil.createOptionalAttrDef("orgLevel", "OrgLevel"),
                new AtlasAttributeDef("department", "Department", false,
                        AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("manager", "Employee", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("mentor", "Employee", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                AtlasTypeUtil.createOptionalAttrDef("shares", "long"),
                AtlasTypeUtil.createOptionalAttrDef("salary", "double")

                );

        AtlasEntityDef managerTypeDef = AtlasTypeUtil.createClassTypeDef("Manager", "Manager"+_description, ImmutableSet.of("Employee"),
                new AtlasAttributeDef("subordinates", String.format("array<%s>", "Employee"), false, AtlasAttributeDef.Cardinality.SET,
                        1, 10, false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        AtlasClassificationDef securityClearanceTypeDef =
                AtlasTypeUtil.createTraitTypeDef("SecurityClearance", "SecurityClearance"+_description, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("level", "int"));

        return new AtlasTypesDef(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, employeeTypeDef, managerTypeDef));
    }

    public static AtlasTypesDef defineValidUpdatedDeptEmployeeTypes() {
        String _description = "_description_updated";
        AtlasEnumDef orgLevelEnum =
                new AtlasEnumDef("OrgLevel", "OrgLevel"+_description, "1.0",
                        Arrays.asList(
                                new AtlasEnumElementDef("L1", "Element"+ _description, 1),
                                new AtlasEnumElementDef("L1", "Element"+ _description, 2)
                        ));

        AtlasStructDef addressDetails =
                createStructTypeDef("Address", "Address"+_description,
                        AtlasTypeUtil.createRequiredAttrDef("street", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("city", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("zip", "int"));

        AtlasEntityDef deptTypeDef =
                AtlasTypeUtil.createClassTypeDef(DEPARTMENT_TYPE, "Department"+_description,
                        ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("dep-code", "string"),
                        new AtlasAttributeDef("employees", String.format("array<%s>", "Employee"), true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1, false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        AtlasEntityDef personTypeDef = AtlasTypeUtil.createClassTypeDef("Person", "Person"+_description,
                ImmutableSet.<String>of(),
                AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                AtlasTypeUtil.createOptionalAttrDef("email", "string"),
                AtlasTypeUtil.createOptionalAttrDef("address", "Address"),
                AtlasTypeUtil.createOptionalAttrDef("birthday", "date"),
                AtlasTypeUtil.createOptionalAttrDef("hasPets", "boolean"),
                AtlasTypeUtil.createOptionalAttrDef("numberOfCars", "byte"),
                AtlasTypeUtil.createOptionalAttrDef("houseNumber", "short"),
                AtlasTypeUtil.createOptionalAttrDef("carMileage", "int"),
                AtlasTypeUtil.createOptionalAttrDef("age", "float"),
                AtlasTypeUtil.createOptionalAttrDef("numberOfStarsEstimate", "biginteger"),
                AtlasTypeUtil.createOptionalAttrDef("approximationOfPi", "bigdecimal")
        );

        AtlasEntityDef employeeTypeDef = AtlasTypeUtil.createClassTypeDef("Employee", "Employee"+_description,
                ImmutableSet.of("Person"),
                AtlasTypeUtil.createOptionalAttrDef("orgLevel", "OrgLevel"),
                AtlasTypeUtil.createOptionalAttrDef("empCode", "string"),
                new AtlasAttributeDef("department", "Department", false,
                        AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("manager", "Employee", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("mentor", "Employee", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                AtlasTypeUtil.createOptionalAttrDef("shares", "long"),
                AtlasTypeUtil.createOptionalAttrDef("salary", "double")

        );

        AtlasEntityDef managerTypeDef = AtlasTypeUtil.createClassTypeDef("Manager", "Manager"+_description,
                ImmutableSet.of("Employee"),
                new AtlasAttributeDef("subordinates", String.format("array<%s>", "Employee"), false, AtlasAttributeDef.Cardinality.SET,
                        1, 10, false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        AtlasClassificationDef securityClearanceTypeDef =
                AtlasTypeUtil.createTraitTypeDef("SecurityClearance", "SecurityClearance"+_description, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("level", "int"));

        return new AtlasTypesDef(ImmutableList.of(orgLevelEnum),
                ImmutableList.of(addressDetails),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, employeeTypeDef, managerTypeDef));
    }

    public static AtlasTypesDef defineInvalidUpdatedDeptEmployeeTypes() {
        String _description = "_description_updated";
        // Test ordinal changes
        AtlasEnumDef orgLevelEnum =
                new AtlasEnumDef("OrgLevel", "OrgLevel"+_description, "1.0",
                        Arrays.asList(
                                new AtlasEnumElementDef("L2", "Element"+ _description, 1),
                                new AtlasEnumElementDef("L1", "Element"+ _description, 2),
                                new AtlasEnumElementDef("L3", "Element"+ _description, 3)
                        ));

        AtlasStructDef addressDetails =
                createStructTypeDef("Address", "Address"+_description,
                        AtlasTypeUtil.createRequiredAttrDef("street", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("city", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("zip", "int"));

        AtlasEntityDef deptTypeDef =
                AtlasTypeUtil.createClassTypeDef(DEPARTMENT_TYPE, "Department"+_description, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("dep-code", "string"),
                        new AtlasAttributeDef("employees", String.format("array<%s>", "Person"), true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1, false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        AtlasEntityDef personTypeDef = AtlasTypeUtil.createClassTypeDef("Person", "Person"+_description, ImmutableSet.<String>of(),
                AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                AtlasTypeUtil.createRequiredAttrDef("emp-code", "string"),
                AtlasTypeUtil.createOptionalAttrDef("orgLevel", "OrgLevel"),
                AtlasTypeUtil.createOptionalAttrDef("address", "Address"),
                new AtlasAttributeDef("department", "Department", false,
                        AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("manager", "Person", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("mentor", "Person", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                AtlasTypeUtil.createOptionalAttrDef("birthday", "date"),
                AtlasTypeUtil.createOptionalAttrDef("hasPets", "boolean"),
                AtlasTypeUtil.createOptionalAttrDef("numberOfCars", "byte"),
                AtlasTypeUtil.createOptionalAttrDef("houseNumber", "short"),
                AtlasTypeUtil.createOptionalAttrDef("carMileage", "int"),
                AtlasTypeUtil.createOptionalAttrDef("shares", "long"),
                AtlasTypeUtil.createOptionalAttrDef("salary", "double"),
                AtlasTypeUtil.createRequiredAttrDef("age", "float"),
                AtlasTypeUtil.createOptionalAttrDef("numberOfStarsEstimate", "biginteger"),
                AtlasTypeUtil.createOptionalAttrDef("approximationOfPi", "bigdecimal")
        );

        return new AtlasTypesDef(ImmutableList.of(orgLevelEnum),
                ImmutableList.of(addressDetails),
                ImmutableList.<AtlasClassificationDef>of(),
                ImmutableList.of(deptTypeDef, personTypeDef));
    }

    public static final String DEPARTMENT_TYPE = "Department";
    public static final String PERSON_TYPE = "Person";

    public static AtlasEntity createDeptEg1() {
        AtlasEntity hrDept = new AtlasEntity(DEPARTMENT_TYPE);
        AtlasEntity john = new AtlasEntity(PERSON_TYPE);

//        AtlasEntity jane = new AtlasEntity("Manager", "SecurityClearance");
        AtlasEntity jane = new AtlasEntity("Manager");
        AtlasEntity johnAddr = new AtlasEntity("Address");
        AtlasEntity janeAddr = new AtlasEntity("Address");
        AtlasEntity julius = new AtlasEntity("Manager");
        AtlasEntity juliusAddr = new AtlasEntity("Address");
        AtlasEntity max = new AtlasEntity("Person");
        AtlasEntity maxAddr = new AtlasEntity("Address");


        hrDept.setAttribute("name", "hr");
        john.setAttribute("name", "John");
        john.setAttribute("department", hrDept);
        johnAddr.setAttribute("street", "Stewart Drive");
        johnAddr.setAttribute("city", "Sunnyvale");
        john.setAttribute("address", johnAddr);

        john.setAttribute("birthday",new Date(1950, 5, 15));
        john.setAttribute("hasPets", true);
        john.setAttribute("numberOfCars", 1);
        john.setAttribute("houseNumber", 153);
        john.setAttribute("carMileage", 13364);
        john.setAttribute("shares", 15000);
        john.setAttribute("salary", 123345.678);
        john.setAttribute("age", 50);
        john.setAttribute("numberOfStarsEstimate", new BigInteger("1000000000000000000000"));
        john.setAttribute("approximationOfPi", new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307816406286"));

        jane.setAttribute("name", "Jane");
        jane.setAttribute("department", hrDept);
        janeAddr.setAttribute("street", "Great America Parkway");
        janeAddr.setAttribute("city", "Santa Clara");
        jane.setAttribute("address", janeAddr);
        janeAddr.setAttribute("street", "Great America Parkway");

        julius.setAttribute("name", "Julius");
        julius.setAttribute("department", hrDept);
        juliusAddr.setAttribute("street", "Madison Ave");
        juliusAddr.setAttribute("city", "Newtonville");
        julius.setAttribute("address", juliusAddr);
        julius.setAttribute("subordinates", ImmutableList.of());

        max.setAttribute("name", "Max");
        max.setAttribute("department", hrDept);
        maxAddr.setAttribute("street", "Ripley St");
        maxAddr.setAttribute("city", "Newton");
        max.setAttribute("address", maxAddr);
        max.setAttribute("manager", jane);
        max.setAttribute("mentor", julius);
        max.setAttribute("birthday",new Date(1979, 3, 15));
        max.setAttribute("hasPets", true);
        max.setAttribute("age", 36);
        max.setAttribute("numberOfCars", 2);
        max.setAttribute("houseNumber", 17);
        max.setAttribute("carMileage", 13);
        max.setAttribute("shares", Long.MAX_VALUE);
        max.setAttribute("salary", Double.MAX_VALUE);
        max.setAttribute("numberOfStarsEstimate", new BigInteger("1000000000000000000000000000000"));
        max.setAttribute("approximationOfPi", new BigDecimal("3.1415926535897932"));

        john.setAttribute("manager", jane);
        john.setAttribute("mentor", max);
        hrDept.setAttribute("employees", ImmutableList.of(john, jane, julius, max));

        jane.setAttribute("subordinates", ImmutableList.of(john, max));

        Map<String, Integer> secClearanceLevelMap = new HashMap<>();
        secClearanceLevelMap.put("level", 1);
        jane.setAttribute("SecurityClearance", secClearanceLevelMap);

        return hrDept;
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

    public static AtlasTypesDef simpleType(){
        AtlasEntityDef superTypeDefinition =
                AtlasTypeUtil.createClassTypeDef("h_type", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createOptionalAttrDef("attr", "string"));

        AtlasStructDef structTypeDefinition = new AtlasStructDef("s_type", "structType", "1.0",
                Arrays.asList(AtlasTypeUtil.createRequiredAttrDef("name", "string")));

        AtlasClassificationDef traitTypeDefinition =
                AtlasTypeUtil.createTraitTypeDef("t_type", "traitType", ImmutableSet.<String>of());

        AtlasEnumDef enumTypeDefinition = new AtlasEnumDef("e_type", "enumType", "1.0",
                Arrays.asList(new AtlasEnumElementDef("ONE", "Element Description", 1)));

        return AtlasTypeUtil.getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(traitTypeDefinition), ImmutableList.of(superTypeDefinition));
    }

    public static AtlasTypesDef simpleTypeUpdated(){
        AtlasEntityDef superTypeDefinition =
                AtlasTypeUtil.createClassTypeDef("h_type", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createOptionalAttrDef("attr", "string"));

        AtlasEntityDef newSuperTypeDefinition =
                AtlasTypeUtil.createClassTypeDef("new_h_type", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createOptionalAttrDef("attr", "string"));

        AtlasStructDef structTypeDefinition = new AtlasStructDef("s_type", "structType", "1.0",
                Arrays.asList(AtlasTypeUtil.createRequiredAttrDef("name", "string")));

        AtlasClassificationDef traitTypeDefinition =
                AtlasTypeUtil.createTraitTypeDef("t_type", "traitType", ImmutableSet.<String>of());

        AtlasEnumDef enumTypeDefinition = new AtlasEnumDef("e_type", "enumType",
                Arrays.asList(new AtlasEnumElementDef("ONE", "Element Description", 1)));
        return AtlasTypeUtil.getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(traitTypeDefinition), ImmutableList.of(superTypeDefinition, newSuperTypeDefinition));
    }

    public static AtlasTypesDef simpleTypeUpdatedDiff() {
        AtlasEntityDef newSuperTypeDefinition =
                AtlasTypeUtil.createClassTypeDef("new_h_type", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createOptionalAttrDef("attr", "string"));

        return AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
                ImmutableList.<AtlasStructDef>of(),
                ImmutableList.<AtlasClassificationDef>of(),
                ImmutableList.of(newSuperTypeDefinition));
    }

    public static AtlasTypesDef defineHiveTypes() {
        String _description = "_description";
        AtlasEntityDef superTypeDefinition =
                AtlasTypeUtil.createClassTypeDef(SUPER_TYPE_NAME, "SuperType_description", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createOptionalAttrDef("namespace", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("cluster", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("colo", "string"));
        AtlasEntityDef databaseTypeDefinition =
                AtlasTypeUtil.createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE + _description,ImmutableSet.of(SUPER_TYPE_NAME),
                        AtlasTypeUtil.createUniqueRequiredAttrDef(NAME, "string"),
                        AtlasTypeUtil.createOptionalAttrDef("created", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("description", "string"));


        AtlasStructDef structTypeDefinition = new AtlasStructDef("serdeType", "serdeType" + _description, "1.0",
                Arrays.asList(
                        AtlasTypeUtil.createRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("serde", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("description", "string")));

        AtlasEnumElementDef values[] = {
                new AtlasEnumElementDef("MANAGED", "Element Description", 1),
                new AtlasEnumElementDef("EXTERNAL", "Element Description", 2)};

        AtlasEnumDef enumTypeDefinition = new AtlasEnumDef("tableType", "tableType" + _description, "1.0", Arrays.asList(values));

        AtlasEntityDef columnsDefinition =
                AtlasTypeUtil.createClassTypeDef(COLUMN_TYPE, COLUMN_TYPE + "_description",
                        ImmutableSet.<String>of(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string")
                        );

        AtlasStructDef partitionDefinition = new AtlasStructDef("partition_struct_type", "partition_struct_type" + _description, "1.0",
                Arrays.asList(AtlasTypeUtil.createRequiredAttrDef("name", "string")));

        AtlasAttributeDef[] attributeDefinitions = new AtlasAttributeDef[]{
                new AtlasAttributeDef("location", "string", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("inputFormat", "string", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("outputFormat", "string", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("compressed", "boolean", false,
                        AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("numBuckets", "int", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
        };

        AtlasEntityDef storageDescClsDef =
                new AtlasEntityDef(STORAGE_DESC_TYPE, STORAGE_DESC_TYPE + _description, "1.0",
                        Arrays.asList(attributeDefinitions), ImmutableSet.of(SUPER_TYPE_NAME));

        AtlasAttributeDef[] partClsAttributes = new AtlasAttributeDef[]{
                new AtlasAttributeDef("values", "array<string>",
                        true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("table", TABLE_TYPE, false,
                        AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("createTime", "long", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("lastAccessTime", "long", true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("sd", STORAGE_DESC_TYPE, false,
                        AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("columns", String.format("array<%s>", COLUMN_TYPE),
                        true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                new AtlasAttributeDef("parameters", String.format("map<%s,%s>", "string", "string"), true,
                        AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                        false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())};

        AtlasEntityDef partClsDef =
                new AtlasEntityDef("partition_class_type", "partition_class_type" + _description, "1.0",
                        Arrays.asList(partClsAttributes), ImmutableSet.of(SUPER_TYPE_NAME));

        AtlasEntityDef processClsType =
                new AtlasEntityDef(PROCESS_TYPE, PROCESS_TYPE + _description, "1.0",
                        Arrays.asList(new AtlasAttributeDef("outputs", "array<" + TABLE_TYPE + ">", true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList())),
                        ImmutableSet.<String>of());

        AtlasEntityDef tableTypeDefinition =
                AtlasTypeUtil.createClassTypeDef(TABLE_TYPE, TABLE_TYPE + _description, ImmutableSet.of(SUPER_TYPE_NAME),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("description", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("created", "date"),
                        // enum
                        new AtlasAttributeDef("tableType", "tableType", false,
                                AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        // array of strings
                        new AtlasAttributeDef("columnNames",
                                String.format("array<%s>", "string"), true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        // array of classes
                        new AtlasAttributeDef("columns", String.format("array<%s>", COLUMN_TYPE),
                                true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        // array of structs
                        new AtlasAttributeDef("partitions", String.format("array<%s>", "partition_struct_type"),
                                true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        // map of primitives
                        new AtlasAttributeDef("parametersMap", String.format("map<%s,%s>", "string", "string"),
                                true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        //map of classes -
                        new AtlasAttributeDef(COLUMNS_MAP,
                                String.format("map<%s,%s>", "string", COLUMN_TYPE),
                                true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        //map of structs
                        new AtlasAttributeDef("partitionsMap",
                                String.format("map<%s,%s>", "string", "partition_struct_type"),
                                true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        // struct reference
                        new AtlasAttributeDef("serde1", "serdeType", true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        new AtlasAttributeDef("serde2", "serdeType", true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        // class reference
                        new AtlasAttributeDef("database", DATABASE_TYPE, false,
                                AtlasAttributeDef.Cardinality.SINGLE, 1, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()),
                        //class reference as composite
                        new AtlasAttributeDef("databaseComposite", DATABASE_TYPE, true,
                                AtlasAttributeDef.Cardinality.SINGLE, 0, 1,
                                false, false,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        AtlasClassificationDef piiTypeDefinition =
                AtlasTypeUtil.createTraitTypeDef(PII, PII + _description, ImmutableSet.<String>of());

        AtlasClassificationDef classificationTypeDefinition =
                AtlasTypeUtil.createTraitTypeDef(CLASSIFICATION, CLASSIFICATION + _description, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("tag", "string"));

        AtlasClassificationDef fetlClassificationTypeDefinition =
                AtlasTypeUtil.createTraitTypeDef("fetl" + CLASSIFICATION, "fetl" + CLASSIFICATION + _description, ImmutableSet.of(CLASSIFICATION),
                        AtlasTypeUtil.createRequiredAttrDef("tag", "string"));

        return AtlasTypeUtil.getTypesDef(ImmutableList.of(enumTypeDefinition),
                ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition, fetlClassificationTypeDefinition, piiTypeDefinition),
                ImmutableList.of(superTypeDefinition, databaseTypeDefinition, columnsDefinition, tableTypeDefinition,
                        storageDescClsDef, partClsDef, processClsType));
    }

    public static final String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    public static AtlasEntity createDBEntity() {
        AtlasEntity entity = new AtlasEntity(DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.setAttribute(NAME, dbName);
        entity.setAttribute("description", "us db");
        return entity;
    }

    public static AtlasEntity createTableEntity(String dbId) {
        AtlasEntity entity = new AtlasEntity(TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.setAttribute(NAME, tableName);
        entity.setAttribute("description", "random table");
        entity.setAttribute("type", "type");
        entity.setAttribute("tableType", "MANAGED");
        entity.setAttribute("database", dbId);
        entity.setAttribute("created", new Date());

        Map<String, Object> partAttributes = new HashMap<String, Object>() {{
            put("name", "part0");
        }};
        final AtlasStruct partitionStruct  = new AtlasStruct("partition_struct_type", partAttributes);

        entity.setAttribute("partitions", new ArrayList<AtlasStruct>() {{ add(partitionStruct); }});
        entity.setAttribute("parametersMap", new java.util.HashMap<String, String>() {{
            put("key1", "value1");
        }});
        return entity;
    }

    public static AtlasEntity createColumnEntity() {
        AtlasEntity entity = new AtlasEntity(COLUMN_TYPE);
        entity.setAttribute(NAME, RandomStringUtils.randomAlphanumeric(10));
        entity.setAttribute("type", "VARCHAR(32)");
        return entity;
    }

    public static List<AtlasClassificationDef> getClassificationWithValidSuperType() {
        AtlasClassificationDef securityClearanceTypeDef =
                AtlasTypeUtil.createTraitTypeDef("SecurityClearance1", "SecurityClearance_description", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("level", "int"));

        AtlasClassificationDef janitorSecurityClearanceTypeDef =
                AtlasTypeUtil.createTraitTypeDef("JanitorClearance", "JanitorClearance_description", ImmutableSet.of("SecurityClearance1"),
                        AtlasTypeUtil.createRequiredAttrDef("level", "int"));

        return Arrays.asList(securityClearanceTypeDef, janitorSecurityClearanceTypeDef);
    }
    public static List<AtlasClassificationDef> getClassificationWithValidAttribute(){
        return getClassificationWithValidSuperType();
    }

    public static List<AtlasEntityDef> getEntityWithValidSuperType() {
        AtlasEntityDef developerTypeDef = AtlasTypeUtil.createClassTypeDef("Developer", "Developer_description", ImmutableSet.of("Employee"),
                new AtlasAttributeDef("language", String.format("array<%s>", "string"), false, AtlasAttributeDef.Cardinality.SET,
                        1, 10, false, false,
                        Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));

        return Arrays.asList(developerTypeDef);
    }

    public static List<AtlasEntityDef> getEntityWithValidAttribute() {
        List<AtlasEntityDef> entityDefs = getEntityWithValidSuperType();
        entityDefs.get(1).getSuperTypes().clear();
        return entityDefs;
    }

    public static AtlasClassificationDef getClassificationWithInvalidSuperType() {
        AtlasClassificationDef classificationDef = simpleType().getClassificationDefs().get(0);
        classificationDef.getSuperTypes().add("!@#$%");
        return classificationDef;
    }

    public static AtlasEntityDef getEntityWithInvalidSuperType() {
        AtlasEntityDef entityDef = simpleType().getEntityDefs().get(0);
        entityDef.addSuperType("!@#$%");
        return entityDef;
    }
}
