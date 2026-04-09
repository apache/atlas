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

package org.apache.atlas.model.impexp;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAttributeTransform {
    private AttributeTransform attributeTransform;

    @BeforeMethod
    public void setUp() {
        attributeTransform = new AttributeTransform();
    }

    @Test
    public void testDefaultConstructor() {
        AttributeTransform transform = new AttributeTransform();

        assertNotNull(transform);
        assertNull(transform.getConditions());
        assertNull(transform.getAction());
    }

    @Test
    public void testParameterizedConstructor() {
        Map<String, String> conditions = new HashMap<>();
        conditions.put("condition1", "value1");
        conditions.put("condition2", "value2");

        Map<String, String> action = new HashMap<>();
        action.put("action1", "actionValue1");
        action.put("action2", "actionValue2");

        AttributeTransform transform = new AttributeTransform(conditions, action);

        assertNotNull(transform);
        assertEquals(transform.getConditions(), conditions);
        assertEquals(transform.getAction(), action);
        assertEquals(transform.getConditions().size(), 2);
        assertEquals(transform.getAction().size(), 2);
    }

    @Test
    public void testParameterizedConstructorWithNulls() {
        AttributeTransform transform = new AttributeTransform(null, null);

        assertNotNull(transform);
        assertNull(transform.getConditions());
        assertNull(transform.getAction());
    }

    @Test
    public void testParameterizedConstructorWithEmptyMaps() {
        Map<String, String> emptyConditions = new HashMap<>();
        Map<String, String> emptyAction = new HashMap<>();

        AttributeTransform transform = new AttributeTransform(emptyConditions, emptyAction);

        assertNotNull(transform);
        assertEquals(transform.getConditions(), emptyConditions);
        assertEquals(transform.getAction(), emptyAction);
        assertTrue(transform.getConditions().isEmpty());
        assertTrue(transform.getAction().isEmpty());
    }

    @Test
    public void testConditionsSetterGetter() {
        Map<String, String> conditions = new HashMap<>();
        conditions.put("key1", "value1");
        conditions.put("key2", "value2");

        attributeTransform.setConditions(conditions);

        assertEquals(attributeTransform.getConditions(), conditions);
        assertEquals(attributeTransform.getConditions().size(), 2);
        assertEquals(attributeTransform.getConditions().get("key1"), "value1");
        assertEquals(attributeTransform.getConditions().get("key2"), "value2");
    }

    @Test
    public void testConditionsSetterWithNull() {
        attributeTransform.setConditions(null);
        assertNull(attributeTransform.getConditions());
    }

    @Test
    public void testActionSetterGetter() {
        Map<String, String> action = new HashMap<>();
        action.put("actionKey1", "actionValue1");
        action.put("actionKey2", "actionValue2");

        attributeTransform.setAction(action);

        assertEquals(attributeTransform.getAction(), action);
        assertEquals(attributeTransform.getAction().size(), 2);
        assertEquals(attributeTransform.getAction().get("actionKey1"), "actionValue1");
        assertEquals(attributeTransform.getAction().get("actionKey2"), "actionValue2");
    }

    @Test
    public void testActionSetterWithNull() {
        attributeTransform.setAction(null);
        assertNull(attributeTransform.getAction());
    }

    @Test
    public void testAddCondition() {
        attributeTransform.addCondition("testAttribute", "testCondition");

        Map<String, String> conditions = attributeTransform.getConditions();
        assertNotNull(conditions);
        assertEquals(conditions.size(), 1);
        assertEquals(conditions.get("testAttribute"), "testCondition");
    }

    @Test
    public void testAddConditionWithNullConditions() {
        // Ensure conditions map is null initially
        attributeTransform.setConditions(null);
        attributeTransform.addCondition("attr1", "cond1");

        Map<String, String> conditions = attributeTransform.getConditions();
        assertNotNull(conditions);
        assertEquals(conditions.size(), 1);
        assertEquals(conditions.get("attr1"), "cond1");
    }

    @Test
    public void testAddMultipleConditions() {
        attributeTransform.addCondition("attr1", "cond1");
        attributeTransform.addCondition("attr2", "cond2");
        attributeTransform.addCondition("attr3", "cond3");

        Map<String, String> conditions = attributeTransform.getConditions();
        assertEquals(conditions.size(), 3);
        assertEquals(conditions.get("attr1"), "cond1");
        assertEquals(conditions.get("attr2"), "cond2");
        assertEquals(conditions.get("attr3"), "cond3");
    }

    @Test
    public void testAddConditionOverwrite() {
        attributeTransform.addCondition("sameAttr", "firstCondition");
        attributeTransform.addCondition("sameAttr", "secondCondition");

        Map<String, String> conditions = attributeTransform.getConditions();
        assertEquals(conditions.size(), 1);
        assertEquals(conditions.get("sameAttr"), "secondCondition");
    }

    @Test
    public void testAddConditionWithNullAttributeName() {
        attributeTransform.addCondition(null, "someCondition");

        Map<String, String> conditions = attributeTransform.getConditions();
        // Should not add anything when attribute name is null
        if (conditions != null) {
            assertTrue(conditions.isEmpty());
        }
    }

    @Test
    public void testAddConditionWithEmptyAttributeName() {
        attributeTransform.addCondition("", "someCondition");

        Map<String, String> conditions = attributeTransform.getConditions();
        if (conditions != null) {
            assertTrue(conditions.isEmpty());
        }
    }

    @Test
    public void testAddConditionWithNullConditionValue() {
        attributeTransform.addCondition("someAttribute", null);

        Map<String, String> conditions = attributeTransform.getConditions();
        if (conditions != null) {
            assertTrue(conditions.isEmpty());
        }
    }

    @Test
    public void testAddConditionWithEmptyConditionValue() {
        attributeTransform.addCondition("someAttribute", "");

        Map<String, String> conditions = attributeTransform.getConditions();
        if (conditions != null) {
            assertTrue(conditions.isEmpty());
        }
    }

    @Test
    public void testAddAction() {
        attributeTransform.addAction("testAttribute", "testAction");

        Map<String, String> action = attributeTransform.getAction();
        assertNotNull(action);
        assertEquals(action.size(), 1);
        assertEquals(action.get("testAttribute"), "testAction");
    }

    @Test
    public void testAddActionWithNullAction() {
        attributeTransform.setAction(null);
        attributeTransform.addAction("attr1", "action1");

        Map<String, String> action = attributeTransform.getAction();
        assertNotNull(action);
        assertEquals(action.size(), 1);
        assertEquals(action.get("attr1"), "action1");
    }

    @Test
    public void testAddMultipleActions() {
        attributeTransform.addAction("attr1", "action1");
        attributeTransform.addAction("attr2", "action2");
        attributeTransform.addAction("attr3", "action3");

        Map<String, String> action = attributeTransform.getAction();
        assertEquals(action.size(), 3);
        assertEquals(action.get("attr1"), "action1");
        assertEquals(action.get("attr2"), "action2");
        assertEquals(action.get("attr3"), "action3");
    }

    @Test
    public void testAddActionOverwrite() {
        attributeTransform.addAction("sameAttr", "firstAction");
        attributeTransform.addAction("sameAttr", "secondAction");

        Map<String, String> action = attributeTransform.getAction();
        assertEquals(action.size(), 1);
        assertEquals(action.get("sameAttr"), "secondAction");
    }

    @Test
    public void testAddActionWithNullAttributeName() {
        attributeTransform.addAction(null, "someAction");

        Map<String, String> action = attributeTransform.getAction();
        if (action != null) {
            assertTrue(action.isEmpty());
        }
    }

    @Test
    public void testAddActionWithEmptyAttributeName() {
        attributeTransform.addAction("", "someAction");

        Map<String, String> action = attributeTransform.getAction();
        if (action != null) {
            assertTrue(action.isEmpty());
        }
    }

    @Test
    public void testAddActionWithNullActionValue() {
        attributeTransform.addAction("someAttribute", null);

        Map<String, String> action = attributeTransform.getAction();
        if (action != null) {
            assertTrue(action.isEmpty());
        }
    }

    @Test
    public void testAddActionWithEmptyActionValue() {
        attributeTransform.addAction("someAttribute", "");

        Map<String, String> action = attributeTransform.getAction();
        if (action != null) {
            assertTrue(action.isEmpty());
        }
    }

    @Test
    public void testBoundaryValuesWithLongStrings() {
        StringBuilder longAttributeNameBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longAttributeNameBuilder.append("a");
        }
        String longAttributeName = longAttributeNameBuilder.toString();
        StringBuilder longConditionValueBuilder = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            longConditionValueBuilder.append("b");
        }
        String longConditionValue = longConditionValueBuilder.toString();
        StringBuilder longActionValueBuilder = new StringBuilder();
        for (int i = 0; i < 1500; i++) {
            longActionValueBuilder.append("c");
        }
        String longActionValue = longActionValueBuilder.toString();

        attributeTransform.addCondition(longAttributeName, longConditionValue);
        attributeTransform.addAction(longAttributeName, longActionValue);

        assertEquals(attributeTransform.getConditions().get(longAttributeName), longConditionValue);
        assertEquals(attributeTransform.getAction().get(longAttributeName), longActionValue);
    }

    @Test
    public void testSpecialCharactersInKeysAndValues() {
        String specialAttribute = "spcAtt";
        String specialCondition = "spcCon";
        String specialAction = "spcAct";

        attributeTransform.addCondition(specialAttribute, specialCondition);
        attributeTransform.addAction(specialAttribute, specialAction);

        assertEquals(attributeTransform.getConditions().get(specialAttribute), specialCondition);
        assertEquals(attributeTransform.getAction().get(specialAttribute), specialAction);
    }

    @Test
    public void testLargeNumberOfConditionsAndActions() {
        for (int i = 0; i < 1000; i++) {
            attributeTransform.addCondition("condition" + i, "condValue" + i);
            attributeTransform.addAction("action" + i, "actionValue" + i);
        }

        Map<String, String> conditions = attributeTransform.getConditions();
        Map<String, String> actions = attributeTransform.getAction();

        assertEquals(conditions.size(), 1000);
        assertEquals(actions.size(), 1000);
        assertEquals(conditions.get("condition500"), "condValue500");
        assertEquals(actions.get("action500"), "actionValue500");
    }

    @Test
    public void testWhitespaceHandling() {
        String attributeWithSpaces = "  attribute with spaces  ";
        String conditionWithSpaces = "  condition with spaces  ";
        String actionWithSpaces = "  action with spaces  ";

        attributeTransform.addCondition(attributeWithSpaces, conditionWithSpaces);
        attributeTransform.addAction(attributeWithSpaces, actionWithSpaces);

        assertEquals(attributeTransform.getConditions().get(attributeWithSpaces), conditionWithSpaces);
        assertEquals(attributeTransform.getAction().get(attributeWithSpaces), actionWithSpaces);
    }

    @Test
    public void testMixedEmptyAndValidValues() {
        attributeTransform.addCondition("validAttr1", "validCond1");
        attributeTransform.addAction("validAttr1", "validAction1");

        attributeTransform.addCondition("", "invalidCond");
        attributeTransform.addCondition(null, "invalidCond");
        attributeTransform.addCondition("validAttr2", "");
        attributeTransform.addCondition("validAttr3", null);

        attributeTransform.addAction("", "invalidAction");
        attributeTransform.addAction(null, "invalidAction");
        attributeTransform.addAction("validAttr2", "");
        attributeTransform.addAction("validAttr3", null);

        attributeTransform.addCondition("validAttr4", "validCond4");
        attributeTransform.addAction("validAttr4", "validAction4");

        Map<String, String> conditions = attributeTransform.getConditions();
        Map<String, String> actions = attributeTransform.getAction();

        assertEquals(conditions.size(), 2);
        assertEquals(actions.size(), 2);
        assertEquals(conditions.get("validAttr1"), "validCond1");
        assertEquals(conditions.get("validAttr4"), "validCond4");
        assertEquals(actions.get("validAttr1"), "validAction1");
        assertEquals(actions.get("validAttr4"), "validAction4");
    }

    @Test
    public void testComplexWorkflow() {
        AttributeTransform transform = new AttributeTransform();

        transform.addCondition("source.database", "old_database");
        transform.addCondition("source.table", "legacy_table");
        transform.addCondition("environment", "staging");

        transform.addAction("target.database", "new_database");
        transform.addAction("target.table", "modern_table");
        transform.addAction("environment", "production");

        transform.addCondition("data.format", "csv");
        transform.addAction("data.format", "parquet");

        Map<String, String> conditions = transform.getConditions();
        Map<String, String> actions = transform.getAction();

        assertEquals(conditions.size(), 4);
        assertEquals(actions.size(), 4);

        assertEquals(conditions.get("source.database"), "old_database");
        assertEquals(actions.get("target.database"), "new_database");
        assertEquals(conditions.get("data.format"), "csv");
        assertEquals(actions.get("data.format"), "parquet");

        transform.addCondition("environment", "development");
        transform.addAction("environment", "test");

        assertEquals(conditions.get("environment"), "development");
        assertEquals(actions.get("environment"), "test");
    }

    @Test
    public void testIndependentConditionsAndActions() {
        attributeTransform.addCondition("conditionOnlyKey", "conditionValue");
        attributeTransform.addAction("actionOnlyKey", "actionValue");

        Map<String, String> conditions = attributeTransform.getConditions();
        Map<String, String> actions = attributeTransform.getAction();

        assertEquals(conditions.size(), 1);
        assertEquals(actions.size(), 1);
        assertEquals(conditions.get("conditionOnlyKey"), "conditionValue");
        assertEquals(actions.get("actionOnlyKey"), "actionValue");
        assertNull(conditions.get("actionOnlyKey"));
        assertNull(actions.get("conditionOnlyKey"));
    }

    @Test
    public void testSettersAfterAddMethods() {
        attributeTransform.addCondition("attr1", "cond1");
        attributeTransform.addAction("attr1", "action1");

        Map<String, String> newConditions = new HashMap<>();
        newConditions.put("newAttr", "newCond");
        Map<String, String> newActions = new HashMap<>();
        newActions.put("newAttr", "newAction");

        attributeTransform.setConditions(newConditions);
        attributeTransform.setAction(newActions);

        assertEquals(attributeTransform.getConditions(), newConditions);
        assertEquals(attributeTransform.getAction(), newActions);
        assertNull(attributeTransform.getConditions().get("attr1"));
        assertNull(attributeTransform.getAction().get("attr1"));
    }
}
