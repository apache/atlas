/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import org.apache.atlas.v1.model.instance.Referenceable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class EntityAuditEventTest {
    private EntityAuditEvent entityAuditEvent;
    private String testEntityId;
    private Long testTimestamp;
    private String testUser;
    private EntityAuditEvent.EntityAuditAction testAction;
    private String testDetails;
    private Referenceable testEntityDefinition;

    @BeforeMethod
    public void setUp() throws Exception {
        testEntityId = "test-entity-id";
        testTimestamp = System.currentTimeMillis();
        testUser = "test-user";
        testAction = EntityAuditEvent.EntityAuditAction.ENTITY_CREATE;
        testDetails = "test-details";
        testEntityDefinition = new Referenceable("TestType");

        entityAuditEvent = new EntityAuditEvent(testEntityId, testTimestamp, testUser,
                testAction, testDetails, testEntityDefinition);
    }

    @Test
    public void testDefaultConstructor() {
        // Act
        EntityAuditEvent event = new EntityAuditEvent();

        // Assert
        assertNotNull(event);
        assertNull(event.getEntityId());
        assertEquals(event.getTimestamp(), 0L);
        assertNull(event.getUser());
        assertNull(event.getAction());
        assertNull(event.getDetails());
        assertNull(event.getEventKey());
        assertNull(event.getEntityDefinition());
    }

    @Test
    public void testParameterizedConstructor() throws Exception {
        // Assert
        assertNotNull(entityAuditEvent);
        assertEquals(entityAuditEvent.getEntityId(), testEntityId);
        assertEquals(entityAuditEvent.getTimestamp(), testTimestamp.longValue());
        assertEquals(entityAuditEvent.getUser(), testUser);
        assertEquals(entityAuditEvent.getAction(), testAction);
        assertEquals(entityAuditEvent.getDetails(), testDetails);
        assertEquals(entityAuditEvent.getEntityDefinition(), testEntityDefinition);
    }

    @Test
    public void testGetAndSetEntityId() {
        // Arrange
        String newEntityId = "new-entity-id";

        // Act
        entityAuditEvent.setEntityId(newEntityId);

        // Assert
        assertEquals(entityAuditEvent.getEntityId(), newEntityId);
    }

    @Test
    public void testGetAndSetTimestamp() {
        // Arrange
        long newTimestamp = System.currentTimeMillis() + 1000;

        // Act
        entityAuditEvent.setTimestamp(newTimestamp);

        // Assert
        assertEquals(entityAuditEvent.getTimestamp(), newTimestamp);
    }

    @Test
    public void testGetAndSetUser() {
        // Arrange
        String newUser = "new-user";

        // Act
        entityAuditEvent.setUser(newUser);

        // Assert
        assertEquals(entityAuditEvent.getUser(), newUser);
    }

    @Test
    public void testGetAndSetAction() {
        // Arrange
        EntityAuditEvent.EntityAuditAction newAction = EntityAuditEvent.EntityAuditAction.ENTITY_UPDATE;

        // Act
        entityAuditEvent.setAction(newAction);

        // Assert
        assertEquals(entityAuditEvent.getAction(), newAction);
    }

    @Test
    public void testGetAndSetDetails() {
        // Arrange
        String newDetails = "new-details";

        // Act
        entityAuditEvent.setDetails(newDetails);

        // Assert
        assertEquals(entityAuditEvent.getDetails(), newDetails);
    }

    @Test
    public void testGetAndSetEventKey() {
        // Arrange
        String eventKey = "test-event-key";

        // Act
        entityAuditEvent.setEventKey(eventKey);

        // Assert
        assertEquals(entityAuditEvent.getEventKey(), eventKey);
    }

    @Test
    public void testGetAndSetEntityDefinition() {
        // Arrange
        Referenceable newEntityDefinition = new Referenceable("NewTestType");

        // Act
        entityAuditEvent.setEntityDefinition(newEntityDefinition);

        // Assert
        assertEquals(entityAuditEvent.getEntityDefinition(), newEntityDefinition);
    }

    @Test
    public void testSetEntityDefinitionFromString() {
        // Arrange
        String jsonString = "{\"typeName\":\"TestType\",\"values\":{}}";

        // Act - explicitly cast to String to resolve ambiguity
        entityAuditEvent.setEntityDefinition((String) jsonString);

        // Assert
        assertNotNull(entityAuditEvent.getEntityDefinition());
    }

    @Test
    public void testGetEntityDefinitionString() {
        // Act
        String result = entityAuditEvent.getEntityDefinitionString();

        // Assert
        assertNotNull(result);
        assertTrue(result.contains("TestType"));
    }

    @Test
    public void testGetEntityDefinitionStringWithNullDefinition() {
        // Arrange
        entityAuditEvent.setEntityDefinition((Referenceable) null);

        // Act
        String result = entityAuditEvent.getEntityDefinitionString();

        // Assert
        assertNull(result);
    }

    @Test
    public void testFromString() {
        // Arrange
        String jsonString = entityAuditEvent.toString();

        // Act
        EntityAuditEvent result = EntityAuditEvent.fromString(jsonString);

        // Assert
        assertNotNull(result);
        assertEquals(result.getEntityId(), testEntityId);
        assertEquals(result.getUser(), testUser);
        assertEquals(result.getAction(), testAction);
    }

    @Test
    public void testToString() {
        // Act
        String result = entityAuditEvent.toString();

        // Assert
        assertNotNull(result);
        assertTrue(result.contains(testEntityId));
        assertTrue(result.contains(testUser));
        assertTrue(result.contains("ENTITY_CREATE"));
    }

    @Test
    public void testHashCode() throws Exception {
        // Arrange
        EntityAuditEvent sameEvent = new EntityAuditEvent(testEntityId, testTimestamp, testUser,
                testAction, testDetails, testEntityDefinition);
        EntityAuditEvent differentEvent = new EntityAuditEvent("different-id", testTimestamp, testUser,
                testAction, testDetails, testEntityDefinition);

        // Act & Assert
        assertEquals(entityAuditEvent.hashCode(), sameEvent.hashCode());
        assertNotEquals(entityAuditEvent.hashCode(), differentEvent.hashCode());
    }

    @Test
    public void testHashCodeWithNullFields() {
        // Arrange
        EntityAuditEvent eventWithNulls = new EntityAuditEvent();

        // Act
        int hashCode = eventWithNulls.hashCode();

        // Assert
        assertNotNull(hashCode);
    }

    @Test
    public void testEquals() throws Exception {
        // Arrange
        EntityAuditEvent sameEvent = new EntityAuditEvent(testEntityId, testTimestamp, testUser,
                testAction, testDetails, testEntityDefinition);
        EntityAuditEvent differentEvent = new EntityAuditEvent("different-id", testTimestamp, testUser,
                testAction, testDetails, testEntityDefinition);

        // Act & Assert
        assertTrue(entityAuditEvent.equals(entityAuditEvent)); // Same reference
        assertTrue(entityAuditEvent.equals(sameEvent)); // Same content
        assertFalse(entityAuditEvent.equals(differentEvent)); // Different content
        assertFalse(entityAuditEvent.equals(null)); // Null comparison
        assertFalse(entityAuditEvent.equals("string")); // Different class
    }

    @Test
    public void testEqualsWithDifferentTimestamp() throws Exception {
        // Arrange
        EntityAuditEvent differentTimestampEvent = new EntityAuditEvent(testEntityId, testTimestamp + 1000,
                testUser, testAction, testDetails, testEntityDefinition);

        // Act & Assert
        assertFalse(entityAuditEvent.equals(differentTimestampEvent));
    }

    @Test
    public void testEqualsWithDifferentUser() throws Exception {
        // Arrange
        EntityAuditEvent differentUserEvent = new EntityAuditEvent(testEntityId, testTimestamp,
                "different-user", testAction, testDetails, testEntityDefinition);

        // Act & Assert
        assertFalse(entityAuditEvent.equals(differentUserEvent));
    }

    @Test
    public void testEqualsWithDifferentAction() throws Exception {
        // Arrange
        EntityAuditEvent differentActionEvent = new EntityAuditEvent(testEntityId, testTimestamp,
                testUser, EntityAuditEvent.EntityAuditAction.ENTITY_DELETE, testDetails, testEntityDefinition);

        // Act & Assert
        assertFalse(entityAuditEvent.equals(differentActionEvent));
    }

    @Test
    public void testEqualsWithDifferentDetails() throws Exception {
        // Arrange
        EntityAuditEvent differentDetailsEvent = new EntityAuditEvent(testEntityId, testTimestamp,
                testUser, testAction, "different-details", testEntityDefinition);

        // Act & Assert
        assertFalse(entityAuditEvent.equals(differentDetailsEvent));
    }

    @Test
    public void testEqualsWithDifferentEventKey() throws Exception {
        // Arrange
        entityAuditEvent.setEventKey("event-key-1");
        EntityAuditEvent differentEventKeyEvent = new EntityAuditEvent(testEntityId, testTimestamp,
                testUser, testAction, testDetails, testEntityDefinition);
        differentEventKeyEvent.setEventKey("event-key-2");

        // Act & Assert
        assertFalse(entityAuditEvent.equals(differentEventKeyEvent));
    }

    @Test
    public void testEqualsWithDifferentEntityDefinition() throws Exception {
        // Arrange
        Referenceable differentEntityDefinition = new Referenceable("DifferentType");
        EntityAuditEvent differentEntityDefEvent = new EntityAuditEvent(testEntityId, testTimestamp,
                testUser, testAction, testDetails, differentEntityDefinition);

        // Act & Assert
        assertFalse(entityAuditEvent.equals(differentEntityDefEvent));
    }

    @Test
    public void testEqualsWithNullFields() {
        // Arrange
        EntityAuditEvent event1 = new EntityAuditEvent();
        EntityAuditEvent event2 = new EntityAuditEvent();

        // Act & Assert
        assertTrue(event1.equals(event2));
    }

    @Test
    public void testEntityAuditActionFromString() {
        // Test all valid action strings
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("ENTITY_CREATE"),
                EntityAuditEvent.EntityAuditAction.ENTITY_CREATE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("ENTITY_UPDATE"),
                EntityAuditEvent.EntityAuditAction.ENTITY_UPDATE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("ENTITY_DELETE"),
                EntityAuditEvent.EntityAuditAction.ENTITY_DELETE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("ENTITY_IMPORT_CREATE"),
                EntityAuditEvent.EntityAuditAction.ENTITY_IMPORT_CREATE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("ENTITY_IMPORT_UPDATE"),
                EntityAuditEvent.EntityAuditAction.ENTITY_IMPORT_UPDATE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("ENTITY_IMPORT_DELETE"),
                EntityAuditEvent.EntityAuditAction.ENTITY_IMPORT_DELETE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("TAG_ADD"),
                EntityAuditEvent.EntityAuditAction.TAG_ADD);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("TAG_DELETE"),
                EntityAuditEvent.EntityAuditAction.TAG_DELETE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("TAG_UPDATE"),
                EntityAuditEvent.EntityAuditAction.TAG_UPDATE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("PROPAGATED_TAG_ADD"),
                EntityAuditEvent.EntityAuditAction.PROPAGATED_TAG_ADD);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("PROPAGATED_TAG_DELETE"),
                EntityAuditEvent.EntityAuditAction.PROPAGATED_TAG_DELETE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("PROPAGATED_TAG_UPDATE"),
                EntityAuditEvent.EntityAuditAction.PROPAGATED_TAG_UPDATE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("TERM_ADD"),
                EntityAuditEvent.EntityAuditAction.TERM_ADD);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("TERM_DELETE"),
                EntityAuditEvent.EntityAuditAction.TERM_DELETE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("LABEL_ADD"),
                EntityAuditEvent.EntityAuditAction.LABEL_ADD);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("LABEL_DELETE"),
                EntityAuditEvent.EntityAuditAction.LABEL_DELETE);
    }

    @Test
    public void testEntityAuditActionFromStringWithLegacyClassificationNames() {
        // Test legacy classification names
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("CLASSIFICATION_ADD"),
                EntityAuditEvent.EntityAuditAction.TAG_ADD);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("CLASSIFICATION_DELETE"),
                EntityAuditEvent.EntityAuditAction.TAG_DELETE);
        assertEquals(EntityAuditEvent.EntityAuditAction.fromString("CLASSIFICATION_UPDATE"),
                EntityAuditEvent.EntityAuditAction.TAG_UPDATE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEntityAuditActionFromStringWithInvalidValue() {
        // Act
        EntityAuditEvent.EntityAuditAction.fromString("INVALID_ACTION");
    }

    @Test
    public void testEntityAuditActionFromStringExceptionMessage() {
        // Arrange
        String invalidAction = "INVALID_ACTION";

        try {
            // Act
            EntityAuditEvent.EntityAuditAction.fromString(invalidAction);
        } catch (IllegalArgumentException e) {
            // Assert
            assertTrue(e.getMessage().contains("No enum constant"));
            assertTrue(e.getMessage().contains("EntityAuditAction"));
            assertTrue(e.getMessage().contains(invalidAction));
        }
    }

    @Test
    public void testAllEntityAuditActionValues() {
        // Verify all enum values exist
        EntityAuditEvent.EntityAuditAction[] actions = EntityAuditEvent.EntityAuditAction.values();
        assertNotNull(actions);
        assertTrue(actions.length >= 15); // Ensure we have all expected actions

        // Verify specific actions exist
        boolean hasEntityCreate = false;
        boolean hasEntityUpdate = false;
        boolean hasEntityDelete = false;
        boolean hasTagAdd = false;
        boolean hasTagDelete = false;
        boolean hasTagUpdate = false;
        boolean hasPropagatedTagAdd = false;
        boolean hasPropagatedTagDelete = false;
        boolean hasPropagatedTagUpdate = false;
        boolean hasEntityImportCreate = false;
        boolean hasEntityImportUpdate = false;
        boolean hasEntityImportDelete = false;
        boolean hasTermAdd = false;
        boolean hasTermDelete = false;
        boolean hasLabelAdd = false;
        boolean hasLabelDelete = false;

        for (EntityAuditEvent.EntityAuditAction action : actions) {
            switch (action) {
                case ENTITY_CREATE:
                    hasEntityCreate = true;
                    break;
                case ENTITY_UPDATE:
                    hasEntityUpdate = true;
                    break;
                case ENTITY_DELETE:
                    hasEntityDelete = true;
                    break;
                case TAG_ADD:
                    hasTagAdd = true;
                    break;
                case TAG_DELETE:
                    hasTagDelete = true;
                    break;
                case TAG_UPDATE:
                    hasTagUpdate = true;
                    break;
                case PROPAGATED_TAG_ADD:
                    hasPropagatedTagAdd = true;
                    break;
                case PROPAGATED_TAG_DELETE:
                    hasPropagatedTagDelete = true;
                    break;
                case PROPAGATED_TAG_UPDATE:
                    hasPropagatedTagUpdate = true;
                    break;
                case ENTITY_IMPORT_CREATE:
                    hasEntityImportCreate = true;
                    break;
                case ENTITY_IMPORT_UPDATE:
                    hasEntityImportUpdate = true;
                    break;
                case ENTITY_IMPORT_DELETE:
                    hasEntityImportDelete = true;
                    break;
                case TERM_ADD:
                    hasTermAdd = true;
                    break;
                case TERM_DELETE:
                    hasTermDelete = true;
                    break;
                case LABEL_ADD:
                    hasLabelAdd = true;
                    break;
                case LABEL_DELETE:
                    hasLabelDelete = true;
                    break;
            }
        }

        // Assert all expected actions are present
        assertTrue(hasEntityCreate);
        assertTrue(hasEntityUpdate);
        assertTrue(hasEntityDelete);
        assertTrue(hasTagAdd);
        assertTrue(hasTagDelete);
        assertTrue(hasTagUpdate);
        assertTrue(hasPropagatedTagAdd);
        assertTrue(hasPropagatedTagDelete);
        assertTrue(hasPropagatedTagUpdate);
        assertTrue(hasEntityImportCreate);
        assertTrue(hasEntityImportUpdate);
        assertTrue(hasEntityImportDelete);
        assertTrue(hasTermAdd);
        assertTrue(hasTermDelete);
        assertTrue(hasLabelAdd);
        assertTrue(hasLabelDelete);
    }

    @Test
    public void testSerializableInterface() {
        // Verify that EntityAuditEvent implements Serializable
        assertTrue(java.io.Serializable.class.isAssignableFrom(EntityAuditEvent.class));
    }

    @Test
    public void testJacksonAnnotations() {
        // Verify Jackson annotations are properly set on the class
        assertTrue(entityAuditEvent.getClass().isAnnotationPresent(com.fasterxml.jackson.annotation.JsonAutoDetect.class));
        assertTrue(entityAuditEvent.getClass().isAnnotationPresent(com.fasterxml.jackson.annotation.JsonInclude.class));
        assertTrue(entityAuditEvent.getClass().isAnnotationPresent(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class));
    }

    @Test
    public void testXmlAnnotations() {
        // Verify XML annotations are properly set on the class
        assertTrue(entityAuditEvent.getClass().isAnnotationPresent(javax.xml.bind.annotation.XmlRootElement.class));
        assertTrue(entityAuditEvent.getClass().isAnnotationPresent(javax.xml.bind.annotation.XmlAccessorType.class));
    }
}
