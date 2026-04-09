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

import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestConstants {
    @Test
    public void testConstructorIsPrivate() throws Exception {
        Constructor<Constants> constructor = Constants.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));

        constructor.setAccessible(true);
        constructor.newInstance();
    }

    @Test
    public void testInternalPropertyKeyPrefix() {
        assertEquals(Constants.INTERNAL_PROPERTY_KEY_PREFIX, "__");
    }

    @Test
    public void testSharedSystemAttributesConstants() throws Exception {
        // Test that all shared system attribute constants are properly defined
        assertConstantValue("TYPE_NAME_PROPERTY_KEY", "typeName");
        assertConstantValue("STATE_PROPERTY_KEY", "state");
        assertConstantValue("CREATED_BY_KEY", "createdBy");
        assertConstantValue("MODIFIED_BY_KEY", "modifiedBy");
        assertConstantValue("TIMESTAMP_PROPERTY_KEY", "timestamp");
        assertConstantValue("MODIFICATION_TIMESTAMP_PROPERTY_KEY", "modificationTimestamp");
    }

    @Test
    public void testEntityOnlySystemAttributesConstants() throws Exception {
        // Test that all entity-only system attribute constants are properly defined
        assertConstantValue("GUID_PROPERTY_KEY", "guid");
        assertConstantValue("HISTORICAL_GUID_PROPERTY_KEY", "historicalGuids");
        assertConstantValue("LABELS_PROPERTY_KEY", "labels");
        assertConstantValue("CUSTOM_ATTRIBUTES_PROPERTY_KEY", "customAttributes");
        assertConstantValue("CLASSIFICATION_TEXT_KEY", "classificationsText");
        assertConstantValue("CLASSIFICATION_NAMES_KEY", "classificationNames");
        assertConstantValue("PROPAGATED_CLASSIFICATION_NAMES_KEY", "propagatedClassificationNames");
        assertConstantValue("IS_INCOMPLETE_PROPERTY_KEY", "isIncomplete");
        assertConstantValue("PENDING_TASKS_PROPERTY_KEY", "pendingTasks");
    }

    @Test
    public void testClassificationOnlySystemAttributesConstants() throws Exception {
        assertConstantValue("CLASSIFICATION_ENTITY_STATUS_PROPERTY_KEY", "entityStatus");
    }

    @Test
    public void testAllConstantsArePublicStaticFinal() {
        Field[] fields = Constants.class.getDeclaredFields();

        for (Field field : fields) {
            if (!field.getName().equals("INTERNAL_PROPERTY_KEY_PREFIX") &&
                    !field.getName().startsWith("$") &&
                    !field.isSynthetic()) {
                // All property key constants should be public static final
                assertTrue(Modifier.isPublic(field.getModifiers()),
                        "Field " + field.getName() + " should be public");
                assertTrue(Modifier.isStatic(field.getModifiers()),
                        "Field " + field.getName() + " should be static");
                assertTrue(Modifier.isFinal(field.getModifiers()),
                        "Field " + field.getName() + " should be final");
                assertEquals(field.getType(), String.class,
                        "Field " + field.getName() + " should be of type String");
            }
        }
    }

    @Test
    public void testConstantValuesAreNotNull() throws Exception {
        Field[] fields = Constants.class.getDeclaredFields();

        for (Field field : fields) {
            // Skip JaCoCo coverage data and other synthetic fields
            if (!field.getName().startsWith("$") && !field.isSynthetic()) {
                field.setAccessible(true);
                Object value = field.get(null);
                assertNotNull(value, "Constant " + field.getName() + " should not be null");
            }
        }
    }

    @Test
    public void testConstantValuesStartWithEncodedPrefix() throws Exception {
        Field[] fields = Constants.class.getDeclaredFields();

        for (Field field : fields) {
            // Skip JaCoCo coverage data and other synthetic fields
            if (!field.getName().equals("INTERNAL_PROPERTY_KEY_PREFIX") &&
                    !field.getName().startsWith("$") &&
                    !field.isSynthetic() &&
                    field.getType() == String.class) {
                field.setAccessible(true);
                String value = (String) field.get(null);

                assertNotNull(value, "Constant " + field.getName() + " should not be null");
                assertTrue(value.length() > 0, "Constant " + field.getName() + " should not be empty");
            }
        }
    }

    @Test
    public void testSpecificConstantValues() {
        // Test some specific constant values to ensure they are as expected
        assertNotNull(Constants.TYPE_NAME_PROPERTY_KEY);
        assertNotNull(Constants.GUID_PROPERTY_KEY);
        assertNotNull(Constants.STATE_PROPERTY_KEY);
        assertNotNull(Constants.CREATED_BY_KEY);
        assertNotNull(Constants.MODIFIED_BY_KEY);
    }

    @Test
    public void testClassIsNotInstantiable() {
        // Verify that Constants class follows the utility class pattern
        assertEquals(Constants.class.getDeclaredConstructors().length, 1);
        Constructor<?> constructor = Constants.class.getDeclaredConstructors()[0];
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
    }

    @Test
    public void testClassIsFinal() {
        // Verify that Constants class is final (good practice for utility classes)
        assertTrue(Modifier.isFinal(Constants.class.getModifiers()));
    }

    private void assertConstantValue(String constantName, String expectedSuffix) throws Exception {
        Field field = Constants.class.getDeclaredField(constantName);
        field.setAccessible(true);
        String value = (String) field.get(null);

        assertNotNull(value, "Constant " + constantName + " should not be null");

        assertTrue(value.length() > 0, "Constant " + constantName + " should not be empty");
    }
}
