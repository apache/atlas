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
import org.junit.Before;
import org.junit.Test;

public class StructTest extends BaseTest {

    StructType structType;
    StructType recursiveStructType;

    @Before
    public void setup() throws MetadataException {
        super.setup();
        structType = (StructType) ms.getTypeSystem().getDataType(STRUCT_TYPE_1);
        recursiveStructType = (StructType) ms.getTypeSystem().getDataType(STRUCT_TYPE_2);
    }

    @Test
    public void test1() throws MetadataException {
        Struct s = createStruct(ms);
        StructInstance ts = structType.convert(s, Multiplicity.REQUIRED);
        System.out.println(ts);
    }

    @Test
    public void testRecursive() throws MetadataException {
        Struct s1 = new Struct(recursiveStructType.getName());
        s1.set("a", 1);
        Struct s2 = new Struct(recursiveStructType.getName());
        s2.set("a", 1);
        s2.set("s", s1);
        StructInstance ts = recursiveStructType.convert(s2, Multiplicity.REQUIRED);
        System.out.println(ts);
    }

}
