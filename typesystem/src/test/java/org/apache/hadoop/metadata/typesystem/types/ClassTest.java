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

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClassTest extends BaseTest {

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void test1() throws MetadataException {

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
}
