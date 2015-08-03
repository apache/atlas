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

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Struct;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StructTest extends BaseTest {

    StructType structType;
    StructType recursiveStructType;

    @Before
    public void setup() throws Exception {
        super.setup();
        structType = getTypeSystem().getDataType(StructType.class, STRUCT_TYPE_1);
        recursiveStructType = getTypeSystem().getDataType(StructType.class, STRUCT_TYPE_2);
    }

    @Test
    public void test1() throws AtlasException {
        Struct s = createStruct();
        ITypedStruct ts = structType.convert(s, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\ta : \t1\n" +
                "\tb : \ttrue\n" +
                "\tc : \t1\n" +
                "\td : \t2\n" +
                "\te : \t1\n" +
                "\tf : \t1\n" +
                "\tg : \t1\n" +
                "\th : \t1.0\n" +
                "\ti : \t1.0\n" +
                "\tj : \t1\n" +
                "\tk : \t1\n" +
                "\tl : \t" + TEST_DATE + "\n" +
                "\tm : \t[1, 1]\n" +
                "\tn : \t[1.1, 1.1]\n" +
                "\to : \t{a=1.0, b=2.0}\n" +
                "}");
    }

    @Test
    public void testRecursive() throws AtlasException {
        Struct s1 = new Struct(recursiveStructType.getName());
        s1.set("a", 1);
        Struct s2 = new Struct(recursiveStructType.getName());
        s2.set("a", 1);
        s2.set("s", s1);
        ITypedStruct ts = recursiveStructType.convert(s2, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\ta : \t1\n" +
                "\ts : \t{\n" +
                "\t\ta : \t\t1\n" +
                "\t\ts : <null>\n" +
                "\n" +
                "\t}\n" +
                "}");
    }

}
