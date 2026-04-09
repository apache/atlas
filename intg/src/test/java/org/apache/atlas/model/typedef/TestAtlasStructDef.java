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

import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasStructDef {
    @Test
    public void testStructDefSerDeEmpty() {
        AtlasStructDef structDef = new AtlasStructDef("emptyStructDef");

        String jsonString = AtlasType.toJson(structDef);

        AtlasStructDef structDef2 = AtlasType.fromJson(jsonString, AtlasStructDef.class);

        assertEquals(structDef2, structDef, "Incorrect serialization/deserialization of AtlasStructDef");
    }

    @Test
    public void testStructDefSerDe() {
        AtlasStructDef structDef = ModelTestUtil.getStructDef();

        String jsonString = AtlasType.toJson(structDef);

        AtlasStructDef structDef2 = AtlasType.fromJson(jsonString, AtlasStructDef.class);

        assertEquals(structDef2, structDef, "Incorrect serialization/deserialization of AtlasStructDef");
    }

    @Test
    public void testStructDefHasAttribute() {
        AtlasStructDef structDef = ModelTestUtil.getStructDef();

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            assertTrue(structDef.hasAttribute(attributeDef.getName()));
        }

        assertFalse(structDef.hasAttribute("01234-xyzabc-;''-)("));
    }

    @Test
    public void testStructDefAddAttribute() {
        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        structDef.addAttribute(new AtlasAttributeDef("newAttribute", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        assertTrue(structDef.hasAttribute("newAttribute"));
    }

    @Test
    public void testStructDefRemoveAttribute() {
        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        String attrName = structDef.getAttributeDefs().get(0).getName();
        assertTrue(structDef.hasAttribute(attrName));

        structDef.removeAttribute(attrName);
        assertFalse(structDef.hasAttribute(attrName));
    }

    @Test
    public void testStructDefSetAttributeDefs() {
        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        List<AtlasAttributeDef> oldAttributes = structDef.getAttributeDefs();
        List<AtlasAttributeDef> newttributes = ModelTestUtil.newAttributeDefsWithAllBuiltInTypes("newAttributes");

        structDef.setAttributeDefs(newttributes);

        for (AtlasAttributeDef attributeDef : oldAttributes) {
            assertFalse(structDef.hasAttribute(attributeDef.getName()));
        }

        for (AtlasAttributeDef attributeDef : newttributes) {
            assertTrue(structDef.hasAttribute(attributeDef.getName()));
        }
    }

    @Test
    public void testAllConstructors() {
        // Test default constructor
        AtlasStructDef def1 = new AtlasStructDef();
        assertNotNull(def1);

        // Test constructor with name
        AtlasStructDef def2 = new AtlasStructDef("TestStruct");
        assertEquals(def2.getName(), "TestStruct");

        // Test constructor with name and description
        AtlasStructDef def3 = new AtlasStructDef("TestStruct", "Test description");
        assertEquals(def3.getName(), "TestStruct");
        assertEquals(def3.getDescription(), "Test description");

        // Test constructor with name, description, and typeVersion
        AtlasStructDef def4 = new AtlasStructDef("TestStruct", "Test description", "1.0");
        assertEquals(def4.getName(), "TestStruct");
        assertEquals(def4.getDescription(), "Test description");
        assertEquals(def4.getTypeVersion(), "1.0");

        // Test constructor with name, description, typeVersion, and attributeDefs
        List<AtlasAttributeDef> attrs = Arrays.asList(new AtlasAttributeDef("attr1", "string"));
        AtlasStructDef def5 = new AtlasStructDef("TestStruct", "Test description", "1.0", attrs);
        assertEquals(def5.getName(), "TestStruct");
        assertEquals(def5.getDescription(), "Test description");
        assertEquals(def5.getTypeVersion(), "1.0");
        assertEquals(def5.getAttributeDefs().size(), 1);

        // Test constructor with all parameters
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        AtlasStructDef def6 = new AtlasStructDef("TestStruct", "Test description", "1.0", attrs, options);
        assertEquals(def6.getName(), "TestStruct");
        assertEquals(def6.getDescription(), "Test description");
        assertEquals(def6.getTypeVersion(), "1.0");
        assertEquals(def6.getAttributeDefs().size(), 1);
        assertEquals(def6.getOptions().get("option1"), "value1");
    }

    @Test
    public void testCopyConstructor() {
        AtlasStructDef original = ModelTestUtil.newStructDef();
        original.setGuid("test-guid");
        original.setCreatedBy("testUser");

        AtlasStructDef copy = new AtlasStructDef(original);

        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getDescription(), original.getDescription());
        assertEquals(copy.getTypeVersion(), original.getTypeVersion());
        assertEquals(copy.getAttributeDefs().size(), original.getAttributeDefs().size());
        assertEquals(copy.getGuid(), original.getGuid());
        assertEquals(copy.getCreatedBy(), original.getCreatedBy());
    }

    @Test
    public void testFindAttributeStaticMethod() throws Exception {
        AtlasAttributeDef attr1 = new AtlasAttributeDef("attr1", "string");
        AtlasAttributeDef attr2 = new AtlasAttributeDef("attr2", "int");
        List<AtlasAttributeDef> attributes = Arrays.asList(attr1, attr2);

        // Test finding existing attribute
        AtlasAttributeDef found = AtlasStructDef.findAttribute(attributes, "attr1");
        assertNotNull(found);
        assertEquals(found.getName(), "attr1");

        // Test case insensitive search
        AtlasAttributeDef foundCaseInsensitive = AtlasStructDef.findAttribute(attributes, "ATTR1");
        assertNotNull(foundCaseInsensitive);
        assertEquals(foundCaseInsensitive.getName(), "attr1");

        // Test finding non-existing attribute
        AtlasAttributeDef notFound = AtlasStructDef.findAttribute(attributes, "nonExistent");
        assertNull(notFound);

        // Test with empty list
        AtlasAttributeDef notFoundEmpty = AtlasStructDef.findAttribute(new ArrayList<>(), "attr1");
        assertNull(notFoundEmpty);

        // Test with null list
        AtlasAttributeDef notFoundNull = AtlasStructDef.findAttribute(null, "attr1");
        assertNull(notFoundNull);
    }

    @Test
    public void testGetAttribute() {
        AtlasStructDef structDef = new AtlasStructDef();
        AtlasAttributeDef attr = new AtlasAttributeDef("testAttr", "string");
        structDef.addAttribute(attr);

        AtlasAttributeDef found = structDef.getAttribute("testAttr");
        assertNotNull(found);
        assertEquals(found.getName(), "testAttr");

        AtlasAttributeDef notFound = structDef.getAttribute("nonExistent");
        assertNull(notFound);
    }

    @Test
    public void testAddAttributeWithNull() {
        AtlasStructDef structDef = new AtlasStructDef();
        int initialSize = structDef.getAttributeDefs().size();

        structDef.addAttribute(null);
        assertEquals(structDef.getAttributeDefs().size(), initialSize);
    }

    @Test
    public void testAddAttributeReplaceExisting() {
        AtlasStructDef structDef = new AtlasStructDef();

        AtlasAttributeDef attr1 = new AtlasAttributeDef("attr", "string");
        AtlasAttributeDef attr2 = new AtlasAttributeDef("attr", "int"); // Same name, different type

        structDef.addAttribute(attr1);
        assertEquals(structDef.getAttributeDefs().size(), 1);
        assertEquals(structDef.getAttribute("attr").getTypeName(), "string");

        structDef.addAttribute(attr2);
        assertEquals(structDef.getAttributeDefs().size(), 1); // Should replace, not add
        assertEquals(structDef.getAttribute("attr").getTypeName(), "int");
    }

    @Test
    public void testRemoveNonExistentAttribute() {
        AtlasStructDef structDef = new AtlasStructDef();
        AtlasAttributeDef attr = new AtlasAttributeDef("attr", "string");
        structDef.addAttribute(attr);

        int initialSize = structDef.getAttributeDefs().size();
        structDef.removeAttribute("nonExistent");
        assertEquals(structDef.getAttributeDefs().size(), initialSize);
    }

    @Test
    public void testSetAttributeDefsWithDuplicates() {
        AtlasStructDef structDef = new AtlasStructDef();

        AtlasAttributeDef attr1 = new AtlasAttributeDef("attr", "string");
        AtlasAttributeDef attr2 = new AtlasAttributeDef("ATTR", "int"); // Same name, different case
        AtlasAttributeDef attr3 = new AtlasAttributeDef("attr3", "boolean");

        List<AtlasAttributeDef> attributesWithDuplicates = Arrays.asList(attr1, attr2, attr3);
        structDef.setAttributeDefs(attributesWithDuplicates);

        // Should keep only the last one with the same name (case-insensitive)
        assertEquals(structDef.getAttributeDefs().size(), 2);
        assertTrue(structDef.hasAttribute("attr"));
        assertTrue(structDef.hasAttribute("attr3"));
        assertEquals(structDef.getAttribute("attr").getTypeName(), "int"); // Should be the last one
    }

    @Test
    public void testSetAttributeDefsWithNull() {
        AtlasStructDef structDef = new AtlasStructDef();
        structDef.setAttributeDefs(null);

        assertNotNull(structDef.getAttributeDefs());
        assertEquals(structDef.getAttributeDefs().size(), 0);
    }

    @Test
    public void testSetAttributeDefsWithSameReference() {
        AtlasStructDef structDef = new AtlasStructDef();
        List<AtlasAttributeDef> attrs = Arrays.asList(new AtlasAttributeDef("attr", "string"));
        structDef.setAttributeDefs(attrs);

        // Setting the same reference should not change anything
        structDef.setAttributeDefs(attrs);
        assertEquals(structDef.getAttributeDefs().size(), 1);
    }

    @Test
    public void testToString() {
        AtlasStructDef structDef = new AtlasStructDef("TestStruct", "Test description", "1.0");
        structDef.addAttribute(new AtlasAttributeDef("attr1", "string"));

        String toStringResult = structDef.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasStructDef"));
        assertTrue(toStringResult.contains("TestStruct"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasStructDef structDef = new AtlasStructDef("TestStruct");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = structDef.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().contains("TestStruct"));
    }

    @Test
    public void testEquals() {
        AtlasStructDef def1 = new AtlasStructDef("TestStruct", "description", "1.0");
        def1.addAttribute(new AtlasAttributeDef("attr1", "string"));

        AtlasStructDef def2 = new AtlasStructDef("TestStruct", "description", "1.0");
        def2.addAttribute(new AtlasAttributeDef("attr1", "string"));

        assertEquals(def1, def2);
        assertEquals(def1.hashCode(), def2.hashCode());

        // Test inequality
        AtlasStructDef def3 = new AtlasStructDef("DifferentStruct", "description", "1.0");
        assertNotEquals(def1, def3);
    }

    @Test
    public void testHashCode() {
        AtlasStructDef def1 = new AtlasStructDef("TestStruct", "description", "1.0");
        AtlasStructDef def2 = new AtlasStructDef("TestStruct", "description", "1.0");

        assertEquals(def1.hashCode(), def2.hashCode());
    }

    @Test
    public void testAtlasAttributeDefAllConstructors() {
        // Test default constructor
        AtlasAttributeDef attr1 = new AtlasAttributeDef();
        assertNotNull(attr1);

        // Test constructor with name and typeName
        AtlasAttributeDef attr2 = new AtlasAttributeDef("name", "string");
        assertEquals(attr2.getName(), "name");
        assertEquals(attr2.getTypeName(), "string");

        // Test constructor with unique and indexable flags
        AtlasAttributeDef attr3 = new AtlasAttributeDef("name", "string", true, true);
        assertEquals(attr3.getName(), "name");
        assertEquals(attr3.getTypeName(), "string");
        assertTrue(attr3.getIsUnique());
        assertTrue(attr3.getIsIndexable());

        // Test constructor with cardinality
        AtlasAttributeDef attr4 = new AtlasAttributeDef("name", "string", AtlasAttributeDef.Cardinality.SET, true, true);
        assertEquals(attr4.getName(), "name");
        assertEquals(attr4.getTypeName(), "string");
        assertEquals(attr4.getCardinality(), AtlasAttributeDef.Cardinality.SET);
        assertTrue(attr4.getIsUnique());
        assertTrue(attr4.getIsIndexable());

        // Test constructor with searchWeight
        AtlasAttributeDef attr5 = new AtlasAttributeDef("name", "string", 5);
        assertEquals(attr5.getName(), "name");
        assertEquals(attr5.getTypeName(), "string");
        assertEquals(attr5.getSearchWeight(), 5);

        // Test constructor with searchWeight and indexType
        AtlasAttributeDef attr6 = new AtlasAttributeDef("name", "string", 5, AtlasAttributeDef.IndexType.STRING);
        assertEquals(attr6.getName(), "name");
        assertEquals(attr6.getTypeName(), "string");
        assertEquals(attr6.getSearchWeight(), 5);
        assertEquals(attr6.getIndexType(), AtlasAttributeDef.IndexType.STRING);
    }

    @Test
    public void testAtlasAttributeDefCopyConstructor() {
        AtlasAttributeDef original = new AtlasAttributeDef("name", "string", true, true);
        original.setDescription("test description");
        original.setDefaultValue("defaultVal");
        original.setSearchWeight(10);
        original.setIndexType(AtlasAttributeDef.IndexType.STRING);
        original.setDisplayName("Display Name");

        AtlasAttributeDef copy = new AtlasAttributeDef(original);

        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getTypeName(), original.getTypeName());
        assertEquals(copy.getIsUnique(), original.getIsUnique());
        assertEquals(copy.getIsIndexable(), original.getIsIndexable());
        assertEquals(copy.getDescription(), original.getDescription());
        assertEquals(copy.getDefaultValue(), original.getDefaultValue());
        assertEquals(copy.getSearchWeight(), original.getSearchWeight());
        assertEquals(copy.getIndexType(), original.getIndexType());
        assertEquals(copy.getDisplayName(), original.getDisplayName());
    }

    @Test
    public void testAtlasAttributeDefOptions() {
        AtlasAttributeDef attr = new AtlasAttributeDef("name", "string");

        // Test setting options
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");
        attr.setOptions(options);

        assertEquals(attr.getOptions().size(), 2);
        assertEquals(attr.getOption("key1"), "value1");
        assertEquals(attr.getOption("key2"), "value2");

        // Test setting individual option
        attr.setOption("key3", "value3");
        assertEquals(attr.getOptions().size(), 3);
        assertEquals(attr.getOption("key3"), "value3");

        // Test setting options to null
        attr.setOptions(null);
        assertNull(attr.getOptions());
    }

    @Test
    public void testAtlasAttributeDefConstraints() {
        AtlasAttributeDef attr = new AtlasAttributeDef("name", "string");

        // Test adding constraints
        AtlasStructDef.AtlasConstraintDef constraint1 = new AtlasStructDef.AtlasConstraintDef("constraint1");
        attr.addConstraint(constraint1);

        assertNotNull(attr.getConstraints());
        assertEquals(attr.getConstraints().size(), 1);

        // Test setting constraints
        AtlasStructDef.AtlasConstraintDef constraint2 = new AtlasStructDef.AtlasConstraintDef("constraint2");
        List<AtlasStructDef.AtlasConstraintDef> constraints = Arrays.asList(constraint1, constraint2);
        attr.setConstraints(constraints);

        assertEquals(attr.getConstraints().size(), 2);

        // Test setting constraints to null
        attr.setConstraints(null);
        assertNull(attr.getConstraints());
    }

    @Test
    public void testAtlasAttributeDefSpecialOptions() {
        AtlasAttributeDef attr = new AtlasAttributeDef("name", "string");

        // Test soft reference option
        attr.setOption(AtlasAttributeDef.ATTRDEF_OPTION_SOFT_REFERENCE, "true");
        assertTrue(attr.isSoftReferenced());

        attr.setOption(AtlasAttributeDef.ATTRDEF_OPTION_SOFT_REFERENCE, "false");
        assertFalse(attr.isSoftReferenced());

        // Test append on partial update option
        attr.setOption(AtlasAttributeDef.ATTRDEF_OPTION_APPEND_ON_PARTIAL_UPDATE, "true");
        assertTrue(attr.isAppendOnPartialUpdate());

        attr.setOption(AtlasAttributeDef.ATTRDEF_OPTION_APPEND_ON_PARTIAL_UPDATE, "false");
        assertFalse(attr.isAppendOnPartialUpdate());
    }

    @Test
    public void testAtlasAttributeDefConstants() {
        assertEquals(AtlasAttributeDef.DEFAULT_SEARCHWEIGHT, -1);
        assertEquals(AtlasAttributeDef.SEARCH_WEIGHT_ATTR_NAME, "searchWeight");
        assertEquals(AtlasAttributeDef.INDEX_TYPE_ATTR_NAME, "indexType");
        assertEquals(AtlasAttributeDef.ATTRDEF_OPTION_SOFT_REFERENCE, "isSoftReference");
        assertEquals(AtlasAttributeDef.ATTRDEF_OPTION_APPEND_ON_PARTIAL_UPDATE, "isAppendOnPartialUpdate");
        assertEquals(AtlasAttributeDef.COUNT_NOT_SET, -1);
    }

    @Test
    public void testAtlasConstraintDefConstructors() {
        // Test default constructor
        AtlasStructDef.AtlasConstraintDef constraint1 = new AtlasStructDef.AtlasConstraintDef();
        assertNotNull(constraint1);

        // Test constructor with type
        AtlasStructDef.AtlasConstraintDef constraint2 = new AtlasStructDef.AtlasConstraintDef("foreignKey");
        assertEquals(constraint2.getType(), "foreignKey");

        // Test constructor with type and params
        Map<String, Object> params = new HashMap<>();
        params.put("param1", "value1");
        AtlasStructDef.AtlasConstraintDef constraint3 = new AtlasStructDef.AtlasConstraintDef("foreignKey", params);
        assertEquals(constraint3.getType(), "foreignKey");
        assertEquals(constraint3.getParams().size(), 1);
        assertEquals(constraint3.getParam("param1"), "value1");
    }

    @Test
    public void testAtlasConstraintDefCopyConstructor() {
        Map<String, Object> params = new HashMap<>();
        params.put("param1", "value1");
        AtlasStructDef.AtlasConstraintDef original = new AtlasStructDef.AtlasConstraintDef("foreignKey", params);

        AtlasStructDef.AtlasConstraintDef copy = new AtlasStructDef.AtlasConstraintDef(original);

        assertEquals(copy.getType(), original.getType());
        assertEquals(copy.getParams(), original.getParams());
        assertEquals(copy.getParam("param1"), original.getParam("param1"));
    }

    @Test
    public void testAtlasConstraintDefIsConstraintType() {
        AtlasStructDef.AtlasConstraintDef constraint = new AtlasStructDef.AtlasConstraintDef("foreignKey");

        assertTrue(constraint.isConstraintType("foreignKey"));
        assertTrue(constraint.isConstraintType("FOREIGNKEY")); // Case insensitive
        assertFalse(constraint.isConstraintType("otherType"));
    }
}
