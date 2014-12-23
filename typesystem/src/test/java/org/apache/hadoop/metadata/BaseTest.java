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

package org.apache.hadoop.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.StructType;
import org.apache.hadoop.metadata.storage.memory.MemRepository;
import org.apache.hadoop.metadata.types.*;
import org.junit.Before;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public abstract class BaseTest {

    protected MetadataService ms;

    public static final String STRUCT_TYPE_1 = "t1";
    public static final String STRUCT_TYPE_2 = "t2";

    @Before
    public void setup() throws MetadataException {

        TypeSystem ts = new TypeSystem();
        MemRepository mr = new MemRepository(ts);
        ms = new MetadataService(mr, ts);
        MetadataService.setCurrentService(ms);

        StructType structType = ts.defineStructType(STRUCT_TYPE_1,
                true,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("d", DataTypes.SHORT_TYPE),
                createOptionalAttrDef("e", DataTypes.INT_TYPE),
                createOptionalAttrDef("f", DataTypes.INT_TYPE),
                createOptionalAttrDef("g", DataTypes.LONG_TYPE),
                createOptionalAttrDef("h", DataTypes.FLOAT_TYPE),
                createOptionalAttrDef("i", DataTypes.DOUBLE_TYPE),
                createOptionalAttrDef("j", DataTypes.BIGINTEGER_TYPE),
                createOptionalAttrDef("k", DataTypes.BIGDECIMAL_TYPE),
                createOptionalAttrDef("l", DataTypes.DATE_TYPE),
                createOptionalAttrDef("m", ts.defineArrayType(DataTypes.INT_TYPE)),
                createOptionalAttrDef("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
                createOptionalAttrDef("o", ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)));

        StructType recursiveStructType = ts.defineStructType(STRUCT_TYPE_2,
                true,
                createRequiredAttrDef("a", DataTypes.INT_TYPE),
                createOptionalAttrDef("s", STRUCT_TYPE_2));

    }

    public static Struct createStruct(MetadataService ms) throws MetadataException {
        StructType structType = (StructType) ms.getTypeSystem().getDataType(StructType.class, STRUCT_TYPE_1);
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
        s.set("m", Lists.<Integer>asList(Integer.valueOf(1), new Integer[]{Integer.valueOf(1)}));
        s.set("n", Lists.<BigDecimal>asList(BigDecimal.valueOf(1.1), new BigDecimal[]{BigDecimal.valueOf(1.1)}));
        Map<String, Double> hm = Maps.<String, Double>newHashMap();
        hm.put("a", 1.0);
        hm.put("b", 2.0);
        s.set("o", hm);
        return s;
    }

    public static AttributeDefinition createOptionalAttrDef(String name,
                                                            IDataType dataType
    ) {

        return new AttributeDefinition(name, dataType.getName(), Multiplicity.OPTIONAL, false, null);
    }

    public static AttributeDefinition createOptionalAttrDef(String name,
                                                            String dataType
    ) {
        return new AttributeDefinition(name, dataType, Multiplicity.OPTIONAL, false, null);
    }


    public static AttributeDefinition createRequiredAttrDef(String name,
                                                            IDataType dataType
    ) {

        return new AttributeDefinition(name, dataType.getName(), Multiplicity.REQUIRED, false, null);
    }

    public static AttributeDefinition createRequiredAttrDef(String name,
                                                            String dataType
    ) {

        return new AttributeDefinition(name, dataType, Multiplicity.REQUIRED, false, null);
    }

    protected Map<String, IDataType> defineTraits(HierarchicalTypeDefinition... tDefs) throws MetadataException {
        return ms.getTypeSystem().defineTraitTypes(tDefs);
    }

    protected HierarchicalTypeDefinition<TraitType> createTraitTypeDef(String name, ImmutableList<String> superTypes,
                                                                       AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(TraitType.class, name, superTypes, attrDefs);
    }

    protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(String name, ImmutableList<String> superTypes,
                                                                       AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);
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

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department", ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees",
                        String.format("array<%s>", "Person"), Multiplicity.COLLECTION, true, "department")
        );
        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person", ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("department",
                        "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager",
                        "Manager", Multiplicity.OPTIONAL, false, "subordinates")
        );
        HierarchicalTypeDefinition<ClassType> managerTypeDef = createClassTypeDef("Manager",
                ImmutableList.<String>of("Person"),
                new AttributeDefinition("subordinates",
                        String.format("array<%s>", "Person"), Multiplicity.COLLECTION, false, "manager")
        );

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef = createTraitTypeDef("SecurityClearance",
                ImmutableList.<String>of(),
                createRequiredAttrDef("level", DataTypes.INT_TYPE)
        );

        ts.defineTypes(ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(securityClearanceTypeDef),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of(deptTypeDef, personTypeDef, managerTypeDef));

        ImmutableList<HierarchicalType> types = ImmutableList.of(
                ts.getDataType(HierarchicalType.class, "SecurityClearance"),
                ts.getDataType(ClassType.class, "Department"),
                ts.getDataType(ClassType.class, "Person"),
                ts.getDataType(ClassType.class, "Manager")
        );

        ms.getRepository().defineTypes(types);

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

        hrDept.set("employees", ImmutableList.<Referenceable>of(john, jane));

        jane.set("subordinates", ImmutableList.<Referenceable>of(john));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        return hrDept;
    }

}
