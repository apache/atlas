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

package org.apache.metadata;

import org.apache.metadata.storage.StructInstance;
import org.apache.metadata.types.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StructTest extends BaseTest {

    StructType structType;
    StructType recursiveStructType;

    @Before
    public void setup() throws MetadataException {
        super.setup();
        structType = (StructType) ms.getTypeSystem().getDataType(StructType.class, STRUCT_TYPE_1);
        recursiveStructType = (StructType) ms.getTypeSystem().getDataType(StructType.class, STRUCT_TYPE_2);
    }

    @Test
    public void test1() throws MetadataException {
        Struct s = createStruct(ms);
        ITypedStruct ts = structType.convert(s, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\ta : 1\n" +
                "\tb : true\n" +
                "\tc : 1\n" +
                "\td : 2\n" +
                "\te : 1\n" +
                "\tf : 1\n" +
                "\tg : 1\n" +
                "\th : 1.0\n" +
                "\ti : 1.0\n" +
                "\tj : 1\n" +
                "\tk : 1\n" +
                "\tl : Wed Dec 10 18:35:58 PST 2014\n" +
                "\tm : [1, 1]\n" +
                "\tn : [1.1, 1.1]\n" +
                "\to : {b=2.0, a=1.0}\n" +
                "\n" +
                "}\n");
    }

    @Test
    public void testRecursive() throws MetadataException {
        Struct s1 = new Struct(recursiveStructType.getName());
        s1.set("a", 1);
        Struct s2 = new Struct(recursiveStructType.getName());
        s2.set("a", 1);
        s2.set("s", s1);
        ITypedStruct ts = recursiveStructType.convert(s2, Multiplicity.REQUIRED);
        Assert.assertEquals(ts.toString(), "{\n" +
                "\ta : 1\n" +
                "\ts : {\n" +
                "\ta : 1\n" +
                "\ts : {<null>\n" +
                "\n" +
                "\n" +
                "}\n" +
                "\n" +
                "\n" +
                "}\n");
    }

}
