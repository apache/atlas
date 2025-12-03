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

import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasRelationshipEndDef {
    @Test
    public void testDefaultConstructor() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef();

        assertNull(endDef.getType());
        assertNull(endDef.getName());
        assertEquals(endDef.getCardinality(), Cardinality.SINGLE);
        assertFalse(endDef.getIsContainer());
        assertFalse(endDef.getIsLegacyAttribute());
        assertNull(endDef.getDescription());
    }

    @Test
    public void testConstructorWithTypeNameAndName() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Table", "columns", Cardinality.SET);

        assertEquals(endDef.getType(), "Table");
        assertEquals(endDef.getName(), "columns");
        assertEquals(endDef.getCardinality(), Cardinality.SET);
        assertFalse(endDef.getIsContainer());
        assertFalse(endDef.getIsLegacyAttribute());
        assertNull(endDef.getDescription());
    }

    @Test
    public void testConstructorWithContainer() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Database", "tables", Cardinality.SET, true);

        assertEquals(endDef.getType(), "Database");
        assertEquals(endDef.getName(), "tables");
        assertEquals(endDef.getCardinality(), Cardinality.SET);
        assertTrue(endDef.getIsContainer());
        assertFalse(endDef.getIsLegacyAttribute());
        assertNull(endDef.getDescription());
    }

    @Test
    public void testConstructorWithLegacyAttribute() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Entity", "legacyAttr", Cardinality.SINGLE, false, true);

        assertEquals(endDef.getType(), "Entity");
        assertEquals(endDef.getName(), "legacyAttr");
        assertEquals(endDef.getCardinality(), Cardinality.SINGLE);
        assertFalse(endDef.getIsContainer());
        assertTrue(endDef.getIsLegacyAttribute());
        assertNull(endDef.getDescription());
    }

    @Test
    public void testConstructorWithDescription() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Entity", "relation", Cardinality.LIST, true, false, "Test description");

        assertEquals(endDef.getType(), "Entity");
        assertEquals(endDef.getName(), "relation");
        assertEquals(endDef.getCardinality(), Cardinality.LIST);
        assertTrue(endDef.getIsContainer());
        assertFalse(endDef.getIsLegacyAttribute());
        assertEquals(endDef.getDescription(), "Test description");
    }

    @Test
    public void testCopyConstructor() {
        AtlasRelationshipEndDef original = new AtlasRelationshipEndDef("Database", "tables", Cardinality.SET, true, false, "Original description");

        AtlasRelationshipEndDef copy = new AtlasRelationshipEndDef(original);

        assertEquals(copy.getType(), original.getType());
        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getCardinality(), original.getCardinality());
        assertEquals(copy.getIsContainer(), original.getIsContainer());
        assertEquals(copy.getIsLegacyAttribute(), original.getIsLegacyAttribute());
        assertEquals(copy.getDescription(), original.getDescription());

        // Ensure they are not the same object
        assertNotEquals(System.identityHashCode(copy), System.identityHashCode(original));
    }

    @Test
    public void testCopyConstructorWithNull() {
        AtlasRelationshipEndDef copy = new AtlasRelationshipEndDef(null);

        assertNull(copy.getType());
        assertNull(copy.getName());
        assertFalse(copy.getIsContainer());
        assertFalse(copy.getIsLegacyAttribute());
        assertNull(copy.getDescription());
    }

    @Test
    public void testSettersAndGetters() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef();

        endDef.setType("TestType");
        assertEquals(endDef.getType(), "TestType");

        endDef.setName("testName");
        assertEquals(endDef.getName(), "testName");

        endDef.setCardinality(Cardinality.LIST);
        assertEquals(endDef.getCardinality(), Cardinality.LIST);

        endDef.setIsContainer(true);
        assertTrue(endDef.getIsContainer());

        endDef.setIsLegacyAttribute(true);
        assertTrue(endDef.getIsLegacyAttribute());

        endDef.setDescription("Test description");
        assertEquals(endDef.getDescription(), "Test description");
    }

    @Test
    public void testEquals() {
        AtlasRelationshipEndDef endDef1 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, false, false, "desc1");
        AtlasRelationshipEndDef endDef2 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, false, false, "desc1");

        assertEquals(endDef1, endDef2);
        assertEquals(endDef1.hashCode(), endDef2.hashCode());

        // Test self equality
        assertEquals(endDef1, endDef1);

        // Test null
        assertNotEquals(endDef1, null);

        // Test different class
        assertNotEquals(endDef1, "string");

        // Test different type
        AtlasRelationshipEndDef endDef3 = new AtlasRelationshipEndDef("Type2", "name1", Cardinality.SINGLE, false, false, "desc1");
        assertNotEquals(endDef1, endDef3);

        // Test different name
        AtlasRelationshipEndDef endDef4 = new AtlasRelationshipEndDef("Type1", "name2", Cardinality.SINGLE, false, false, "desc1");
        assertNotEquals(endDef1, endDef4);

        // Test different cardinality
        AtlasRelationshipEndDef endDef5 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SET, false, false, "desc1");
        assertNotEquals(endDef1, endDef5);

        // Test different container flag
        AtlasRelationshipEndDef endDef6 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, true, false, "desc1");
        assertNotEquals(endDef1, endDef6);

        // Test different legacy attribute flag
        AtlasRelationshipEndDef endDef7 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, false, true, "desc1");
        assertNotEquals(endDef1, endDef7);

        // Test different description
        AtlasRelationshipEndDef endDef8 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, false, false, "desc2");
        assertNotEquals(endDef1, endDef8);
    }

    @Test
    public void testHashCode() {
        AtlasRelationshipEndDef endDef1 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, false, false, "desc1");
        AtlasRelationshipEndDef endDef2 = new AtlasRelationshipEndDef("Type1", "name1", Cardinality.SINGLE, false, false, "desc1");

        assertEquals(endDef1.hashCode(), endDef2.hashCode());

        // Different objects should have different hash codes (not guaranteed but likely)
        AtlasRelationshipEndDef endDef3 = new AtlasRelationshipEndDef("Type2", "name1", Cardinality.SINGLE, false, false, "desc1");
        assertNotEquals(endDef1.hashCode(), endDef3.hashCode());
    }

    @Test
    public void testToString() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Database", "tables", Cardinality.SET, true, false, "Database tables relationship");

        String toStringResult = endDef.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasRelationshipEndDef"));
        assertTrue(toStringResult.contains("Database"));
        assertTrue(toStringResult.contains("tables"));
        assertTrue(toStringResult.contains("SET"));
        assertTrue(toStringResult.contains("true"));
        assertTrue(toStringResult.contains("false"));
        assertTrue(toStringResult.contains("Database tables relationship"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Table", "columns", Cardinality.LIST, false, true, "Table columns");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = endDef.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("Table"));
        assertTrue(result.toString().contains("columns"));
        assertTrue(result.toString().contains("LIST"));
        assertTrue(result.toString().contains("true")); // for legacy attribute
        assertTrue(result.toString().contains("false")); // for container
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef("Entity", "relation", Cardinality.SINGLE);

        StringBuilder result = endDef.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("Entity"));
        assertTrue(result.toString().contains("relation"));
    }

    @Test
    public void testSerializationDeserialization() {
        AtlasRelationshipEndDef original = new AtlasRelationshipEndDef("Database", "tables", Cardinality.SET, true, false, "Database contains tables");

        String jsonString = AtlasType.toJson(original);
        AtlasRelationshipEndDef deserialized = AtlasType.fromJson(jsonString, AtlasRelationshipEndDef.class);

        assertEquals(deserialized, original);
        assertEquals(deserialized.getType(), original.getType());
        assertEquals(deserialized.getName(), original.getName());
        assertEquals(deserialized.getCardinality(), original.getCardinality());
        assertEquals(deserialized.getIsContainer(), original.getIsContainer());
        assertEquals(deserialized.getIsLegacyAttribute(), original.getIsLegacyAttribute());
        assertEquals(deserialized.getDescription(), original.getDescription());
    }

    @Test
    public void testAllCardinalityValues() {
        AtlasRelationshipEndDef endDefSingle = new AtlasRelationshipEndDef("Type", "attr", Cardinality.SINGLE);
        assertEquals(endDefSingle.getCardinality(), Cardinality.SINGLE);

        AtlasRelationshipEndDef endDefList = new AtlasRelationshipEndDef("Type", "attr", Cardinality.LIST);
        assertEquals(endDefList.getCardinality(), Cardinality.LIST);

        AtlasRelationshipEndDef endDefSet = new AtlasRelationshipEndDef("Type", "attr", Cardinality.SET);
        assertEquals(endDefSet.getCardinality(), Cardinality.SET);
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasRelationshipEndDef endDef1 = new AtlasRelationshipEndDef();
        AtlasRelationshipEndDef endDef2 = new AtlasRelationshipEndDef();

        assertEquals(endDef1, endDef2);

        endDef1.setType("Type1");
        assertNotEquals(endDef1, endDef2);

        endDef2.setType("Type1");
        assertEquals(endDef1, endDef2);

        endDef1.setName("name1");
        assertNotEquals(endDef1, endDef2);

        endDef2.setName("name1");
        assertEquals(endDef1, endDef2);

        endDef1.setDescription("desc1");
        assertNotEquals(endDef1, endDef2);

        endDef2.setDescription("desc1");
        assertEquals(endDef1, endDef2);
    }

    @Test
    public void testSetCardinalityMethod() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef();

        endDef.setCardinality(Cardinality.SET);
        assertEquals(endDef.getCardinality(), Cardinality.SET);

        endDef.setCardinality(Cardinality.LIST);
        assertEquals(endDef.getCardinality(), Cardinality.LIST);

        endDef.setCardinality(Cardinality.SINGLE);
        assertEquals(endDef.getCardinality(), Cardinality.SINGLE);
    }

    @Test
    public void testComplexScenario() {
        // Test a complex relationship end definition
        AtlasRelationshipEndDef tableEnd = new AtlasRelationshipEndDef(
                "hive_table",
                "columns",
                Cardinality.SET,
                true,
                false,
                "A table contains multiple columns");

        AtlasRelationshipEndDef columnEnd = new AtlasRelationshipEndDef(
                "hive_column",
                "table",
                Cardinality.SINGLE,
                false,
                false,
                "A column belongs to one table");

        // Verify table end
        assertEquals(tableEnd.getType(), "hive_table");
        assertEquals(tableEnd.getName(), "columns");
        assertEquals(tableEnd.getCardinality(), Cardinality.SET);
        assertTrue(tableEnd.getIsContainer());
        assertFalse(tableEnd.getIsLegacyAttribute());
        assertEquals(tableEnd.getDescription(), "A table contains multiple columns");

        // Verify column end
        assertEquals(columnEnd.getType(), "hive_column");
        assertEquals(columnEnd.getName(), "table");
        assertEquals(columnEnd.getCardinality(), Cardinality.SINGLE);
        assertFalse(columnEnd.getIsContainer());
        assertFalse(columnEnd.getIsLegacyAttribute());
        assertEquals(columnEnd.getDescription(), "A column belongs to one table");

        // Ensure they are different
        assertNotEquals(tableEnd, columnEnd);
    }

    @Test
    public void testBooleanGettersSetters() {
        AtlasRelationshipEndDef endDef = new AtlasRelationshipEndDef();

        // Test default values
        assertFalse(endDef.getIsContainer());
        assertFalse(endDef.getIsLegacyAttribute());

        // Test setters
        endDef.setIsContainer(true);
        assertTrue(endDef.getIsContainer());

        endDef.setIsLegacyAttribute(true);
        assertTrue(endDef.getIsLegacyAttribute());

        // Test setting back to false
        endDef.setIsContainer(false);
        assertFalse(endDef.getIsContainer());

        endDef.setIsLegacyAttribute(false);
        assertFalse(endDef.getIsLegacyAttribute());
    }
}
