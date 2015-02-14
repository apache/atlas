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
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.tools.cmd.Meta;

public class StorageTest extends BaseTest {

    @Before
    public void setup() throws MetadataException {
        super.setup();
    }

    @Test
    public void test1() throws MetadataException {

        TypeSystem ts = getTypeSystem();

        defineDeptEmployeeTypes(ts);

        Referenceable hrDept = createDeptEg1(ts);
        ITypedReferenceableInstance hrDept2 = getRepository().create(hrDept);
        ITypedReferenceableInstance hrDept3 = getRepository().get(hrDept2.getId());
        Assert.assertEquals(hrDept3.toString(), "{\n" +
                "\tid : (type: Department, id: 1)\n" +
                "\tname : \thr\n" +
                "\temployees : \t[{\n" +
                "\tid : (type: Person, id: 2)\n" +
                "\tname : \tJohn\n" +
                "\tdepartment : (type: Department, id: 1)\n" +
                "\tmanager : (type: Manager, id: 3)\n" +
                "}, {\n" +
                "\tid : (type: Manager, id: 3)\n" +
                "\tsubordinates : \t[(type: Person, id: 2)]\n" +
                "\tname : \tJane\n" +
                "\tdepartment : (type: Department, id: 1)\n" +
                "\tmanager : <null>\n" +
                "\n" +
                "\tSecurityClearance : \t{\n" +
                "\t\tlevel : \t\t1\n" +
                "\t}}]\n" +
                "}");
    }

    @Test
    public void testGetPerson() throws MetadataException {
        TypeSystem ts = getTypeSystem();
        defineDeptEmployeeTypes(ts);

        Referenceable hrDept = createDeptEg1(ts);
        ITypedReferenceableInstance hrDept2 = getRepository().create(hrDept);

        Id e1Id = new Id(2, 0, "Person");
        ITypedReferenceableInstance e1 = getRepository().get(e1Id);
        Assert.assertEquals(e1.toString(), "{\n" +
                "\tid : (type: Person, id: 2)\n" +
                "\tname : \tJohn\n" +
                "\tdepartment : (type: Department, id: 1)\n" +
                "\tmanager : (type: Manager, id: 3)\n" +
                "}");
    }

    @Test
    public void testInvalidTypeName() throws MetadataException {
        TypeSystem ts = getTypeSystem();
        defineDeptEmployeeTypes(ts);

        Referenceable hrDept = createDeptEg1(ts);
        ITypedReferenceableInstance hrDept2 = getRepository().create(hrDept);

        Id e1Id = new Id(3, 0, "Person");
        try {
            ITypedReferenceableInstance e1 = getRepository().get(e1Id);
        } catch(RepositoryException re) {
            RepositoryException me = (RepositoryException) re.getCause();
            Assert.assertEquals(me.getMessage(), "Invalid Id (unknown) : (type: Person, id: 3)");

        }
    }

    @Test
    public void testGetManager() throws MetadataException {
        TypeSystem ts = getTypeSystem();
        defineDeptEmployeeTypes(ts);

        Referenceable hrDept = createDeptEg1(ts);
        ITypedReferenceableInstance hrDept2 = getRepository().create(hrDept);

        Id m1Id = new Id(3, 0, "Manager");
        ITypedReferenceableInstance m1 = getRepository().get(m1Id);
        Assert.assertEquals(m1.toString(), "{\n" +
                "\tid : (type: Manager, id: 3)\n" +
                "\tsubordinates : \t[(type: Person, id: 2)]\n" +
                "\tname : \tJane\n" +
                "\tdepartment : (type: Department, id: 1)\n" +
                "\tmanager : <null>\n" +
                "\n" +
                "\tSecurityClearance : \t{\n" +
                "\t\tlevel : \t\t1\n" +
                "\t}}");
    }
}