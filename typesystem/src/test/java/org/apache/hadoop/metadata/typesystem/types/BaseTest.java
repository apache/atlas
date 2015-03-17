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

package org.apache.hadoop.metadata.typesystem.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.Struct;
import org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil;
import org.junit.Before;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public abstract class BaseTest {

    public static final String STRUCT_TYPE_1 = "t1";
    public static final String STRUCT_TYPE_2 = "t2";

    public static Struct createStruct() throws MetadataException {
        StructType structType = TypeSystem.getInstance().getDataType(
                StructType.class, STRUCT_TYPE_1);
        Struct s = new Struct(structType.getName());
        s.set("a", 1);
        s.set("b", true);
        s.set("c", (byte) 1);
        s.set("d", (short) 2);
        s.set("e", 1);
        s.set("f", 1);
        s.set("g", 1L);
        s.set("h", 1.0f);
        s.set("i", 1.0);
        s.set("j", BigInteger.valueOf(1L));
        s.set("k", new BigDecimal(1));
        s.set("l", new Date(1418265358440L));
        s.set("m", Lists.asList(1, new Integer[]{1}));
        s.set("n", Lists.asList(BigDecimal.valueOf(1.1),
                new BigDecimal[]{BigDecimal.valueOf(1.1)}));
        Map<String, Double> hm = Maps.newHashMap();
        hm.put("a", 1.0);
        hm.put("b", 2.0);
        s.set("o", hm);
        return s;
    }

    protected final TypeSystem getTypeSystem() {
        return TypeSystem.getInstance();
    }

    @Before
    public void setup() throws Exception {
        TypeSystem ts = TypeSystem.getInstance();
        ts.reset();

        StructType structType = ts.defineStructType(STRUCT_TYPE_1,
                true,
                TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                TypesUtil.createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                TypesUtil.createOptionalAttrDef("d", DataTypes.SHORT_TYPE),
                TypesUtil.createOptionalAttrDef("e", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("f", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("g", DataTypes.LONG_TYPE),
                TypesUtil.createOptionalAttrDef("h", DataTypes.FLOAT_TYPE),
                TypesUtil.createOptionalAttrDef("i", DataTypes.DOUBLE_TYPE),
                TypesUtil.createOptionalAttrDef("j", DataTypes.BIGINTEGER_TYPE),
                TypesUtil.createOptionalAttrDef("k", DataTypes.BIGDECIMAL_TYPE),
                TypesUtil.createOptionalAttrDef("l", DataTypes.DATE_TYPE),
                TypesUtil.createOptionalAttrDef("m", ts.defineArrayType(DataTypes.INT_TYPE)),
                TypesUtil.createOptionalAttrDef("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
                TypesUtil.createOptionalAttrDef("o",
                        ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)));
        System.out.println("defined structType = " + structType);

        StructType recursiveStructType = ts.defineStructType(STRUCT_TYPE_2,
                true,
                TypesUtil.createRequiredAttrDef("a", DataTypes.INT_TYPE),
                TypesUtil.createOptionalAttrDef("s", STRUCT_TYPE_2));
        System.out.println("defined recursiveStructType = " + recursiveStructType);
    }

    protected Map<String, IDataType> defineTraits(HierarchicalTypeDefinition... tDefs)
        throws MetadataException {

        return getTypeSystem().defineTraitTypes(tDefs);
    }

    /*
     * Class Hierarchy is:
     *   Department(name : String, employees : Array[Person])
     *   Person(name : String, department : Department, manager : Manager)
     *   Manager(subordinates : Array[Person]) extends Person
     *
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    protected void defineDeptEmployeeTypes(TypeSystem ts) throws MetadataException {

        HierarchicalTypeDefinition<ClassType> deptTypeDef = TypesUtil
                .createClassTypeDef("Department",
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        new AttributeDefinition("employees",
                                String.format("array<%s>", "Person"), Multiplicity.COLLECTION, true,
                                "department")
                );
        HierarchicalTypeDefinition<ClassType> personTypeDef = TypesUtil.createClassTypeDef("Person",
                ImmutableList.<String>of(),
                TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("department",
                        "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager",
                        "Manager", Multiplicity.OPTIONAL, false, "subordinates")
        );
        HierarchicalTypeDefinition<ClassType> managerTypeDef =
                TypesUtil.createClassTypeDef("Manager",
                        ImmutableList.of("Person"),
                        new AttributeDefinition("subordinates",
                                String.format("array<%s>", "Person"),
                                Multiplicity.COLLECTION, false, "manager")
                );

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef =
                TypesUtil.createTraitTypeDef(
                        "SecurityClearance",
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("level", DataTypes.INT_TYPE)
                );

        ts.defineTypes(ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef,
                        managerTypeDef));

        ImmutableList<HierarchicalType> types = ImmutableList.of(
                ts.getDataType(HierarchicalType.class, "SecurityClearance"),
                ts.getDataType(ClassType.class, "Department"),
                ts.getDataType(ClassType.class, "Person"),
                ts.getDataType(ClassType.class, "Manager")
        );
    }

    protected Referenceable createDeptEg1(TypeSystem ts) throws MetadataException {
        Referenceable hrDept = new Referenceable("Department");
        Referenceable john = new Referenceable("Person");
        Referenceable jane = new Referenceable("Manager", "SecurityClearance");

        hrDept.set("name", "hr");
        john.set("name", "John");
        john.set("department", hrDept);
        jane.set("name", "Jane");
        jane.set("department", hrDept);

        john.set("manager", jane);

        hrDept.set("employees", ImmutableList.of(john, jane));

        jane.set("subordinates", ImmutableList.of(john));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        return hrDept;
    }
}
