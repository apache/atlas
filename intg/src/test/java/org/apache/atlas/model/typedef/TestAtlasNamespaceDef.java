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
package org.apache.atlas.model.typedef;

import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.atlas.model.typedef.AtlasNamespaceDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestAtlasNamespaceDef {

    @Test
    public void namespaceDefSerDes() {
        AtlasNamespaceDef namespaceDef = new AtlasNamespaceDef("test_namespace", "test_description", null);
        String jsonString = AtlasType.toJson(namespaceDef);

        AtlasNamespaceDef namespaceDef1 = AtlasType.fromJson(jsonString, AtlasNamespaceDef.class);
        assertEquals(namespaceDef, namespaceDef1,
                "Incorrect serialization/deserialization of AtlasNamespaceDef");
    }

    @Test
    public void namespaceDefEquality() {
        AtlasNamespaceDef namespaceDef1 = new AtlasNamespaceDef("test_namespace", "test_description", null);
        AtlasNamespaceDef namespaceDef2 = new AtlasNamespaceDef("test_namespace", "test_description", null);
        assertEquals(namespaceDef1, namespaceDef2, "Namespaces should be equal because the name of the" +
                "namespace is same");
    }

    @Test
    public void namespaceDefUnequality() {
        AtlasNamespaceDef namespaceDef1 = new AtlasNamespaceDef("test_namespace", "test_description", null);
        AtlasNamespaceDef namespaceDef2 = new AtlasNamespaceDef("test_namespace1", "test_description", null);
        assertNotEquals(namespaceDef1, namespaceDef2, "Namespaces should not be equal since they have a" +
                "different name");
    }

    @Test
    public void namespaceDefWithNamespaceAttributes() {
        AtlasNamespaceDef                namespaceDef1 = new AtlasNamespaceDef("test_namespace", "test_description", null);
        AtlasStructDef.AtlasAttributeDef nsAttr1       = new AtlasStructDef.AtlasAttributeDef("attr1", "int");
        AtlasStructDef.AtlasAttributeDef nsAttr2       = new AtlasStructDef.AtlasAttributeDef("attr2", "int");

        nsAttr1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));
        nsAttr2.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));

        namespaceDef1.setAttributeDefs(Arrays.asList(nsAttr1, nsAttr2));
        assertEquals(namespaceDef1.getAttributeDefs().size(), 2);
    }

    @Test
    public void namespaceDefWithNamespaceAttributesHavingCardinality() {
        AtlasNamespaceDef                namespaceDef1 = new AtlasNamespaceDef("test_namespace", "test_description", null);
        AtlasStructDef.AtlasAttributeDef nsAttr1       = new AtlasStructDef.AtlasAttributeDef("attr1", "int");
        AtlasStructDef.AtlasAttributeDef nsAttr2       = new AtlasStructDef.AtlasAttributeDef("attr2", "int");

        nsAttr1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));
        nsAttr2.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));
        nsAttr2.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        namespaceDef1.setAttributeDefs(Arrays.asList(nsAttr1, nsAttr2));
        assertEquals(namespaceDef1.getAttributeDefs().size(), 2);
    }
}