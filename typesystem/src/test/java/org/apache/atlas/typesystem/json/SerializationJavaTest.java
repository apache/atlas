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

package org.apache.atlas.typesystem.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.BaseTest;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;

public class SerializationJavaTest extends BaseTest {


    @BeforeMethod
    public void setup() throws Exception {
        super.setup();
    }

    /*
     * Class Hierarchy is:
     *   Department(name : String, employees : Array[Person])
     *   Person(name : String, department : Department, manager : Manager)
     *   Manager(subordinates : Array[Person]) extends Person
     *
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    @Test
    public void test1() throws AtlasException {

        TypeSystem ts = getTypeSystem();

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department", ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.COLLECTION,
                        true, "department"));
        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person", ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("department", "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates"));
        HierarchicalTypeDefinition<ClassType> managerTypeDef =
                createClassTypeDef("Manager", ImmutableSet.<String>of("Person"),
                        new AttributeDefinition("subordinates", String.format("array<%s>", "Person"),
                                Multiplicity.COLLECTION, false, "manager"));

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef =
                createTraitTypeDef("SecurityClearance", ImmutableSet.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, managerTypeDef));

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

        String hrDeptStr = hrDept2.toString();

        Assert.assertEquals(hrDeptStr, "{\n" +
                "\tid : (type: Department, id: <unassigned>)\n" +
                "\tname : \thr\n" +
                "\temployees : \t[{\n" +
                "\tid : (type: Person, id: <unassigned>)\n" +
                "\tname : \tJohn\n" +
                "\tdepartment : (type: Department, id: <unassigned>)\n" +
                "\tmanager : (type: Manager, id: <unassigned>)\n" +
                "}, {\n" +
                "\tid : (type: Manager, id: <unassigned>)\n" +
                "\tsubordinates : \t[{\n" +
                "\tid : (type: Person, id: <unassigned>)\n" +
                "\tname : \tJohn\n" +
                "\tdepartment : (type: Department, id: <unassigned>)\n" +
                "\tmanager : (type: Manager, id: <unassigned>)\n" +
                "}]\n" +
                "\tname : \tJane\n" +
                "\tdepartment : (type: Department, id: <unassigned>)\n" +
                "\tmanager : <null>\n" +
                "\n" +
                "\tSecurityClearance : \t{\n" +
                "\t\tlevel : \t\t1\n" +
                "\t}}]\n" +
                "}");

        String jsonStr = Serialization$.MODULE$.toJson(hrDept2);
        //System.out.println(jsonStr);

        hrDept2 = Serialization$.MODULE$.fromJson(jsonStr);
        Assert.assertEquals(hrDept2.toString(), hrDeptStr);

    }

    @Test
    public void testTrait() throws AtlasException {

        TypeSystem ts = getTypeSystem();
        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef =
                createTraitTypeDef("SecurityClearance2", ImmutableSet.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());


        Struct s = new Struct("SecurityClearance2");
        s.set("level", 1);
        TraitType tType = ts.getDataType(TraitType.class, "SecurityClearance2");
        ITypedInstance t = tType.convert(s, Multiplicity.REQUIRED);
        String jsonStr = Serialization$.MODULE$.toJson(t);
        ITypedInstance t2 = Serialization$.MODULE$.traitFromJson(jsonStr);
        Assert.assertEquals(t.toString(), t2.toString());
    }
}