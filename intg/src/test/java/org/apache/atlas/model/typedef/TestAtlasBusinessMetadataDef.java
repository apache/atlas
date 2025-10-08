/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestAtlasBusinessMetadataDef {
    @Test
    public void businessMetadataDefSerDes() {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("test_businessMetadata", "test_description", null);
        String jsonString = AtlasType.toJson(businessMetadataDef);
        AtlasBusinessMetadataDef businessMetadataDef1 = AtlasType.fromJson(jsonString, AtlasBusinessMetadataDef.class);

        assertEquals(businessMetadataDef, businessMetadataDef1, "Incorrect serialization/deserialization of AtlasBusinessMetadataDef");
    }

    @Test
    public void businessMetadataDefEquality() {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef("test_businessMetadata", "test_description", null);
        AtlasBusinessMetadataDef businessMetadataDef2 = new AtlasBusinessMetadataDef("test_businessMetadata", "test_description", null);

        assertEquals(businessMetadataDef1, businessMetadataDef2, "businessMetadatas should be equal because the name of the" + "businessMetadata is same");
    }

    @Test
    public void businessMetadataDefUnequality() {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef("test_businessMetadata", "test_description", null);
        AtlasBusinessMetadataDef businessMetadataDef2 = new AtlasBusinessMetadataDef("test_businessMetadata1", "test_description", null);

        assertNotEquals(businessMetadataDef1, businessMetadataDef2, "businessMetadatas should not be equal since they have a" + "different name");
    }

    @Test
    public void businessMetadataDefWithAttributes() {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef("test_businessMetadata", "test_description", null);
        AtlasStructDef.AtlasAttributeDef nsAttr1 = new AtlasStructDef.AtlasAttributeDef("attr1", "int");
        AtlasStructDef.AtlasAttributeDef nsAttr2 = new AtlasStructDef.AtlasAttributeDef("attr2", "int");

        nsAttr1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));
        nsAttr2.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));

        businessMetadataDef1.setAttributeDefs(Arrays.asList(nsAttr1, nsAttr2));

        assertEquals(businessMetadataDef1.getAttributeDefs().size(), 2);
    }

    @Test
    public void businessMetadataDefWithAttributesHavingCardinality() {
        AtlasBusinessMetadataDef businessMetadataDef1 = new AtlasBusinessMetadataDef("test_businessMetadata", "test_description", null);
        AtlasStructDef.AtlasAttributeDef nsAttr1 = new AtlasStructDef.AtlasAttributeDef("attr1", "int");
        AtlasStructDef.AtlasAttributeDef nsAttr2 = new AtlasStructDef.AtlasAttributeDef("attr2", "int");

        nsAttr1.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));
        nsAttr2.setOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES, AtlasType.toJson(Collections.singleton("hive_table")));
        nsAttr2.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        businessMetadataDef1.setAttributeDefs(Arrays.asList(nsAttr1, nsAttr2));

        assertEquals(businessMetadataDef1.getAttributeDefs().size(), 2);
    }

    @Test
    public void testAllConstructors() {
        // Test default constructor
        AtlasBusinessMetadataDef def1 = new AtlasBusinessMetadataDef();
        assertNotEquals(def1, null);

        // Test constructor with name and description
        AtlasBusinessMetadataDef def2 = new AtlasBusinessMetadataDef("name", "description");
        assertEquals(def2.getName(), "name");
        assertEquals(def2.getDescription(), "description");

        // Test constructor with name, description, and typeVersion
        AtlasBusinessMetadataDef def3 = new AtlasBusinessMetadataDef("name", "description", "1.0");
        assertEquals(def3.getName(), "name");
        assertEquals(def3.getDescription(), "description");
        assertEquals(def3.getTypeVersion(), "1.0");

        // Test constructor with name, description, typeVersion, and attributeDefs
        AtlasStructDef.AtlasAttributeDef attr = new AtlasStructDef.AtlasAttributeDef("attr", "string");
        AtlasBusinessMetadataDef def4 = new AtlasBusinessMetadataDef("name", "description", "1.0", Arrays.asList(attr));
        assertEquals(def4.getName(), "name");
        assertEquals(def4.getDescription(), "description");
        assertEquals(def4.getTypeVersion(), "1.0");
        assertEquals(def4.getAttributeDefs().size(), 1);

        // Test constructor with all parameters
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        AtlasBusinessMetadataDef def5 = new AtlasBusinessMetadataDef("name", "description", "1.0", Arrays.asList(attr), options);
        assertEquals(def5.getName(), "name");
        assertEquals(def5.getDescription(), "description");
        assertEquals(def5.getTypeVersion(), "1.0");
        assertEquals(def5.getAttributeDefs().size(), 1);
        assertEquals(def5.getOptions().get("option1"), "value1");
    }

    @Test
    public void testCopyConstructor() {
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        AtlasStructDef.AtlasAttributeDef attr = new AtlasStructDef.AtlasAttributeDef("attr", "string");

        AtlasBusinessMetadataDef original = new AtlasBusinessMetadataDef("original", "original description", "1.0", Arrays.asList(attr), options);
        original.setGuid("test-guid");

        AtlasBusinessMetadataDef copy = new AtlasBusinessMetadataDef(original);

        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getDescription(), original.getDescription());
        assertEquals(copy.getTypeVersion(), original.getTypeVersion());
        assertEquals(copy.getAttributeDefs().size(), original.getAttributeDefs().size());
        assertEquals(copy.getOptions(), original.getOptions());
        assertEquals(copy.getGuid(), original.getGuid());
    }

    @Test
    public void testToString() {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("testName", "testDescription", "1.0");
        businessMetadataDef.setGuid("test-guid");

        String toStringResult = businessMetadataDef.toString();
        assertNotEquals(toStringResult, null);
        assertTrue(toStringResult.contains("AtlasBusinessMetadataDef"));
        assertTrue(toStringResult.contains("testName"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("testName", "testDescription");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = businessMetadataDef.toString(sb);
        assertNotEquals(result, null);
        assertTrue(result.toString().contains("testName"));
    }

    @Test
    public void testConstants() {
        assertEquals(AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES, "applicableEntityTypes");
        assertEquals(AtlasBusinessMetadataDef.ATTR_MAX_STRING_LENGTH, "maxStrLength");
        assertEquals(AtlasBusinessMetadataDef.ATTR_VALID_PATTERN, "validPattern");
    }

    @Test
    public void testHashCode() {
        AtlasBusinessMetadataDef def1 = new AtlasBusinessMetadataDef("name", "description", "1.0");
        AtlasBusinessMetadataDef def2 = new AtlasBusinessMetadataDef("name", "description", "1.0");

        assertEquals(def1.hashCode(), def2.hashCode());

        AtlasBusinessMetadataDef def3 = new AtlasBusinessMetadataDef("differentName", "description", "1.0");
        assertNotEquals(def1.hashCode(), def3.hashCode());
    }

    @Test
    public void testWithDifferentOptions() {
        Map<String, String> options1 = new HashMap<>();
        options1.put(AtlasBusinessMetadataDef.ATTR_MAX_STRING_LENGTH, "100");
        options1.put(AtlasBusinessMetadataDef.ATTR_VALID_PATTERN, "[a-zA-Z]*");

        AtlasBusinessMetadataDef def1 = new AtlasBusinessMetadataDef("testDef", "description", "1.0", null, options1);
        assertEquals(def1.getOption(AtlasBusinessMetadataDef.ATTR_MAX_STRING_LENGTH), "100");
        assertEquals(def1.getOption(AtlasBusinessMetadataDef.ATTR_VALID_PATTERN), "[a-zA-Z]*");
    }

    @Test
    public void testInheritedMethods() {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef("testName", "testDescription", "1.0");

        // Test inherited attribute methods
        AtlasStructDef.AtlasAttributeDef attr1 = new AtlasStructDef.AtlasAttributeDef("attr1", "string");
        AtlasStructDef.AtlasAttributeDef attr2 = new AtlasStructDef.AtlasAttributeDef("attr2", "int");

        businessMetadataDef.addAttribute(attr1);
        assertTrue(businessMetadataDef.hasAttribute("attr1"));
        assertEquals(businessMetadataDef.getAttribute("attr1").getName(), "attr1");

        businessMetadataDef.addAttribute(attr2);
        assertEquals(businessMetadataDef.getAttributeDefs().size(), 2);

        businessMetadataDef.removeAttribute("attr1");
        assertFalse(businessMetadataDef.hasAttribute("attr1"));
        assertTrue(businessMetadataDef.hasAttribute("attr2"));
    }
}
