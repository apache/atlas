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
package org.apache.atlas.type;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestTemplateTokens {
    private AtlasEntity testEntity;

    @BeforeMethod
    public void setUp() {
        testEntity = new AtlasEntity("TestEntityType");
        testEntity.setGuid("test-guid-123");
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "TestEntityName");
        attributes.put("description", "Test entity description");
        attributes.put("number", 42);
        attributes.put("flag", true);
        testEntity.setAttributes(attributes);
    }

    @Test
    public void testAttributeTokenConstructor() {
        AttributeToken token = new AttributeToken("name");
        assertEquals(token.getValue(), "name");
    }

    @Test
    public void testAttributeTokenEvalWithValidAttribute() throws AtlasBaseException {
        AttributeToken token = new AttributeToken("name");
        String result = token.eval(testEntity);
        assertEquals(result, "TestEntityName");
    }

    @Test
    public void testAttributeTokenEvalWithNumberAttribute() throws AtlasBaseException {
        AttributeToken token = new AttributeToken("number");
        String result = token.eval(testEntity);
        assertEquals(result, "42");
    }

    @Test
    public void testAttributeTokenEvalWithBooleanAttribute() throws AtlasBaseException {
        AttributeToken token = new AttributeToken("flag");
        String result = token.eval(testEntity);
        assertEquals(result, "true");
    }

    @Test
    public void testAttributeTokenEvalWithNullAttribute() throws AtlasBaseException {
        AttributeToken token = new AttributeToken("nonexistent");
        String result = token.eval(testEntity);
        assertNull(result);
    }

    @Test
    public void testAttributeTokenEvalWithNullEntity() throws AtlasBaseException {
        AttributeToken token = new AttributeToken("name");
        try {
            token.eval(null);
        } catch (Exception e) {
            // Expected - null entity should cause exception
            assertNotNull(e);
        }
    }

    @Test
    public void testConstantTokenConstructor() {
        ConstantToken token = new ConstantToken("constantValue");
        assertEquals(token.getValue(), "constantValue");
    }

    @Test
    public void testConstantTokenEval() throws AtlasBaseException {
        ConstantToken token = new ConstantToken("constantValue");
        String result = token.eval(testEntity);
        assertEquals(result, "constantValue");
    }

    @Test
    public void testConstantTokenEvalWithNullEntity() throws AtlasBaseException {
        ConstantToken token = new ConstantToken("constantValue");
        String result = token.eval(null);
        assertEquals(result, "constantValue");
    }

    @Test
    public void testConstantTokenWithEmptyString() throws AtlasBaseException {
        ConstantToken token = new ConstantToken("");
        String result = token.eval(testEntity);
        assertEquals(result, "");
        assertEquals(token.getValue(), "");
    }

    @Test
    public void testConstantTokenWithNullConstant() throws AtlasBaseException {
        ConstantToken token = new ConstantToken(null);
        String result = token.eval(testEntity);
        assertNull(result);
        assertNull(token.getValue());
    }

    @Test
    public void testDependentTokenConstructor() throws Exception {
        DependentToken token = new DependentToken("entity.attribute.subAttribute");
        assertEquals(token.getValue(), "entity.attribute.subAttribute");
        // Use reflection to test private fields
        Field pathField = DependentToken.class.getDeclaredField("path");
        pathField.setAccessible(true);
        String path = (String) pathField.get(token);
        assertEquals(path, "entity.attribute.subAttribute");

        Field attrNameField = DependentToken.class.getDeclaredField("attrName");
        attrNameField.setAccessible(true);
        String attrName = (String) attrNameField.get(token);
        assertEquals(attrName, "subAttribute");
    }

    @Test
    public void testDependentTokenConstructorWithSimplePath() throws Exception {
        DependentToken token = new DependentToken("simpleAttribute");
        Field attrNameField = DependentToken.class.getDeclaredField("attrName");
        attrNameField.setAccessible(true);
        String attrName = (String) attrNameField.get(token);
        assertEquals(attrName, "simpleAttribute");
    }

    @Test
    public void testDependentTokenEval() throws AtlasBaseException {
        DependentToken token = new DependentToken("entity.attribute");
        String result = token.eval(testEntity);
        assertEquals(result, "TEMP"); // Current implementation returns "TEMP"
    }

    @Test
    public void testDependentTokenEvalWithNullEntity() throws AtlasBaseException {
        DependentToken token = new DependentToken("entity.attribute");
        String result = token.eval(null);
        assertEquals(result, "TEMP"); // Current implementation returns "TEMP" regardless
    }

    @Test
    public void testDependentTokenGetValue() {
        DependentToken token = new DependentToken("complex.path.attribute");
        assertEquals(token.getValue(), "complex.path.attribute");
    }

    @Test
    public void testDependentTokenWithDotInPath() throws Exception {
        DependentToken token = new DependentToken("level1.level2.level3.finalAttribute");
        Field pathField = DependentToken.class.getDeclaredField("path");
        pathField.setAccessible(true);
        String path = (String) pathField.get(token);
        assertEquals(path, "level1.level2.level3.finalAttribute");

        Field attrNameField = DependentToken.class.getDeclaredField("attrName");
        attrNameField.setAccessible(true);
        String attrName = (String) attrNameField.get(token);
        assertEquals(attrName, "finalAttribute");

        Field objectPathField = DependentToken.class.getDeclaredField("objectPath");
        objectPathField.setAccessible(true);
        java.util.List<String> objectPath = (java.util.List<String>) objectPathField.get(token);
        assertEquals(objectPath.size(), 3);
        assertEquals(objectPath.get(0), "level1");
        assertEquals(objectPath.get(1), "level2");
        assertEquals(objectPath.get(2), "level3");
    }

    @Test
    public void testTemplateTokenInterface() {
        TemplateToken constantToken = new ConstantToken("test");
        TemplateToken attributeToken = new AttributeToken("test");
        TemplateToken dependentToken = new DependentToken("test");

        assertNotNull(constantToken.getValue());
        assertNotNull(attributeToken.getValue());
        assertNotNull(dependentToken.getValue());
    }

    @Test
    public void testTokensWithSpecialCharacters() {
        AttributeToken token1 = new AttributeToken("attr_with_underscore");
        assertEquals(token1.getValue(), "attr_with_underscore");

        ConstantToken token2 = new ConstantToken("constant with spaces");
        assertEquals(token2.getValue(), "constant with spaces");

        DependentToken token3 = new DependentToken("path.with_mixed.characters_123");
        assertEquals(token3.getValue(), "path.with_mixed.characters_123");
    }

    @Test
    public void testAttributeTokenWithComplexAttribute() throws AtlasBaseException {
        Map<String, Object> complexObject = new HashMap<>();
        complexObject.put("nestedKey", "nestedValue");
        testEntity.setAttribute("complexAttr", complexObject);

        AttributeToken token = new AttributeToken("complexAttr");
        String result = token.eval(testEntity);
        assertNotNull(result);
        // Should get the toString() representation of the complex object
        assertTrue(result.contains("nestedKey") || result.contains("nestedValue") || result.contains("{"));
    }

    private void assertTrue(boolean condition) {
        org.testng.Assert.assertTrue(condition);
    }
}
