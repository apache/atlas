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

package org.apache.atlas.typesystem.types;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClassTest extends HierarchicalTypeTest<ClassType> {

    @BeforeMethod
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void test1() throws AtlasException {

        TypeSystem ts = getTypeSystem();

        defineDeptEmployeeTypes(ts);
        Referenceable hrDept = createDeptEg1(ts);
        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);


        Assert.assertEquals(hrDept2.toString(), "{\n" +
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
    }

    @Override
    protected HierarchicalTypeDefinition<ClassType> getTypeDefinition(String name, AttributeDefinition... attributes) {
        return new HierarchicalTypeDefinition(ClassType.class, name, null, attributes);
    }

    @Override
    protected HierarchicalTypeDefinition<ClassType> getTypeDefinition(String name, ImmutableList<String> superTypes,
                                                                      AttributeDefinition... attributes) {
        return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attributes);
    }

    @Override
    protected TypesDef getTypesDef(StructTypeDefinition typeDefinition) {
        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of((HierarchicalTypeDefinition<ClassType>) typeDefinition));
    }

    @Override
    protected TypesDef getTypesDef(HierarchicalTypeDefinition<ClassType>... typeDefinitions) {
        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.copyOf(typeDefinitions));
    }
}
