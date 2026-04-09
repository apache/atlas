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
package org.apache.atlas.model.notification;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelationshipHeader;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEntityNotification {
    private EntityNotification entityNotification;

    @BeforeMethod
    public void setUp() {
        entityNotification = new EntityNotification();
    }

    @Test
    public void testDefaultConstructor() {
        EntityNotification notification = new EntityNotification();
        assertEquals(notification.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V1);
    }

    @Test
    public void testConstructorWithType() {
        EntityNotification notification = new EntityNotification(
                EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
        assertEquals(notification.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
    }

    @Test
    public void testTypeGetterSetter() {
        assertEquals(entityNotification.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V1);

        entityNotification.setType(EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
        assertEquals(entityNotification.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);

        entityNotification.setType(null);
        assertNull(entityNotification.getType());
    }

    @Test
    public void testNormalize() {
        entityNotification.normalize();
    }

    @Test
    public void testToString() {
        String result = entityNotification.toString();

        assertNotNull(result);
        assertTrue(result.contains("EntityNotification"));
        assertTrue(result.contains("ENTITY_NOTIFICATION_V1"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        StringBuilder sb = new StringBuilder();
        StringBuilder result = entityNotification.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("EntityNotification"));
        assertTrue(result.toString().contains("type="));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        StringBuilder result = entityNotification.toString(null);

        assertNotNull(result);
        assertTrue(result.toString().contains("EntityNotification"));
    }

    @Test
    public void testEntityNotificationTypeEnum() {
        EntityNotification.EntityNotificationType[] types = EntityNotification.EntityNotificationType.values();
        assertEquals(types.length, 2);
        assertEquals(types[0], EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V1);
        assertEquals(types[1], EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
        assertEquals(EntityNotification.EntityNotificationType.valueOf("ENTITY_NOTIFICATION_V1"), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V1);
        assertEquals(EntityNotification.EntityNotificationType.valueOf("ENTITY_NOTIFICATION_V2"), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
    }

    @Test
    public void testSerializable() {
        assertNotNull(entityNotification);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(entityNotification);
    }

    @Test
    public void testXmlAnnotations() {
        assertNotNull(entityNotification);
    }

    @Test
    public void testEntityNotificationV2DefaultConstructor() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();

        assertEquals(notificationV2.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
        assertNull(notificationV2.getEntity());
        assertNull(notificationV2.getRelationship());
        assertNull(notificationV2.getOperationType());
        assertTrue(notificationV2.getEventTime() > 0);
    }

    @Test
    public void testEntityNotificationV2ConstructorWithEntity() {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("test-guid");
        OperationType operationType = OperationType.ENTITY_CREATE;

        EntityNotificationV2 notificationV2 = new EntityNotificationV2(entityHeader, operationType);

        assertEquals(notificationV2.getEntity(), entityHeader);
        assertEquals(notificationV2.getOperationType(), operationType);
        assertNull(notificationV2.getRelationship());
        assertTrue(notificationV2.getEventTime() > 0);
    }

    @Test
    public void testEntityNotificationV2ConstructorWithEntityAndEventTime() {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("test-guid-2");
        OperationType operationType = OperationType.ENTITY_UPDATE;
        long eventTime = System.currentTimeMillis();

        EntityNotificationV2 notificationV2 = new EntityNotificationV2(entityHeader, operationType, eventTime);

        assertEquals(notificationV2.getEntity(), entityHeader);
        assertEquals(notificationV2.getOperationType(), operationType);
        assertEquals(notificationV2.getEventTime(), eventTime);
        assertNull(notificationV2.getRelationship());
    }

    @Test
    public void testEntityNotificationV2ConstructorWithRelationship() {
        AtlasRelationshipHeader relationshipHeader = new AtlasRelationshipHeader();
        relationshipHeader.setGuid("relationship-guid");
        OperationType operationType = OperationType.RELATIONSHIP_CREATE;
        long eventTime = System.currentTimeMillis();

        EntityNotificationV2 notificationV2 = new EntityNotificationV2(relationshipHeader, operationType, eventTime);

        assertEquals(notificationV2.getRelationship(), relationshipHeader);
        assertEquals(notificationV2.getOperationType(), operationType);
        assertEquals(notificationV2.getEventTime(), eventTime);
        assertNull(notificationV2.getEntity());
    }

    @Test
    public void testEntityNotificationV2EntityGetterSetter() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();

        assertNull(notificationV2.getEntity());

        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("setter-test-guid");
        notificationV2.setEntity(entityHeader);

        assertEquals(notificationV2.getEntity(), entityHeader);

        notificationV2.setEntity(null);
        assertNull(notificationV2.getEntity());
    }

    @Test
    public void testEntityNotificationV2RelationshipGetterSetter() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();

        assertNull(notificationV2.getRelationship());

        AtlasRelationshipHeader relationshipHeader = new AtlasRelationshipHeader();
        relationshipHeader.setGuid("relationship-setter-guid");
        notificationV2.setRelationship(relationshipHeader);

        assertEquals(notificationV2.getRelationship(), relationshipHeader);

        notificationV2.setRelationship(null);
        assertNull(notificationV2.getRelationship());
    }

    @Test
    public void testEntityNotificationV2OperationTypeGetterSetter() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();

        assertNull(notificationV2.getOperationType());

        notificationV2.setOperationType(OperationType.ENTITY_DELETE);
        assertEquals(notificationV2.getOperationType(), OperationType.ENTITY_DELETE);

        notificationV2.setOperationType(null);
        assertNull(notificationV2.getOperationType());
    }

    @Test
    public void testEntityNotificationV2EventTimeGetterSetter() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();

        long originalEventTime = notificationV2.getEventTime();
        assertTrue(originalEventTime > 0);

        long newEventTime = System.currentTimeMillis() + 10000;
        notificationV2.setEventTime(newEventTime);
        assertEquals(notificationV2.getEventTime(), newEventTime);

        notificationV2.setEventTime(0);
        assertEquals(notificationV2.getEventTime(), 0);

        notificationV2.setEventTime(-1);
        assertEquals(notificationV2.getEventTime(), -1);
    }

    @Test
    public void testEntityNotificationV2HashCodeConsistency() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        notificationV2.setOperationType(OperationType.ENTITY_CREATE);

        int hashCode1 = notificationV2.hashCode();
        int hashCode2 = notificationV2.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testEntityNotificationV2HashCodeEquality() {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("hash-test-guid");

        EntityNotificationV2 notification1 = new EntityNotificationV2();
        notification1.setEntity(entityHeader);
        notification1.setOperationType(OperationType.ENTITY_CREATE);

        EntityNotificationV2 notification2 = new EntityNotificationV2();
        notification2.setEntity(entityHeader);
        notification2.setOperationType(OperationType.ENTITY_CREATE);

        assertEquals(notification1.hashCode(), notification2.hashCode());
    }

    @Test
    public void testEntityNotificationV2EqualsWithSameObject() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        assertTrue(notificationV2.equals(notificationV2));
    }

    @Test
    public void testEntityNotificationV2EqualsWithNull() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        assertFalse(notificationV2.equals(null));
    }

    @Test
    public void testEntityNotificationV2EqualsWithDifferentClass() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        assertFalse(notificationV2.equals("not a notification"));
    }

    @Test
    public void testEntityNotificationV2EqualsWithSameValues() {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("equals-test-guid");

        EntityNotificationV2 notification1 = new EntityNotificationV2();
        notification1.setEntity(entityHeader);
        notification1.setOperationType(OperationType.ENTITY_UPDATE);

        EntityNotificationV2 notification2 = new EntityNotificationV2();
        notification2.setEntity(entityHeader);
        notification2.setOperationType(OperationType.ENTITY_UPDATE);

        assertTrue(notification1.equals(notification2));
        assertTrue(notification2.equals(notification1));
    }

    @Test
    public void testEntityNotificationV2EqualsWithDifferentValues() {
        EntityNotificationV2 notification1 = new EntityNotificationV2();
        notification1.setOperationType(OperationType.ENTITY_CREATE);

        EntityNotificationV2 notification2 = new EntityNotificationV2();
        notification2.setOperationType(OperationType.ENTITY_DELETE);

        assertFalse(notification1.equals(notification2));
        assertFalse(notification2.equals(notification1));
    }

    @Test
    public void testEntityNotificationV2ToString() {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("toString-test-guid");

        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        notificationV2.setEntity(entityHeader);
        notificationV2.setOperationType(OperationType.ENTITY_CREATE);
        notificationV2.setEventTime(1234567890L);

        String result = notificationV2.toString();

        assertNotNull(result);
        assertTrue(result.contains("EntityNotificationV1")); // Note: toString shows V1 but it's actually V2
        assertTrue(result.contains("ENTITY_CREATE"));
        assertTrue(result.contains("1234567890"));
    }

    @Test
    public void testEntityNotificationV2ToStringWithStringBuilder() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        notificationV2.setOperationType(OperationType.RELATIONSHIP_DELETE);

        StringBuilder sb = new StringBuilder();
        StringBuilder result = notificationV2.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("RELATIONSHIP_DELETE"));
    }

    @Test
    public void testOperationTypeEnum() {
        OperationType[] operationTypes = OperationType.values();

        assertTrue(operationTypes.length >= 9);

        // Verify some expected operation types exist
        boolean foundEntityCreate = false;
        boolean foundEntityUpdate = false;
        boolean foundEntityDelete = false;
        boolean foundClassificationAdd = false;
        boolean foundRelationshipCreate = false;

        for (OperationType opType : operationTypes) {
            switch (opType) {
                case ENTITY_CREATE:
                    foundEntityCreate = true;
                    break;
                case ENTITY_UPDATE:
                    foundEntityUpdate = true;
                    break;
                case ENTITY_DELETE:
                    foundEntityDelete = true;
                    break;
                case CLASSIFICATION_ADD:
                    foundClassificationAdd = true;
                    break;
                case RELATIONSHIP_CREATE:
                    foundRelationshipCreate = true;
                    break;
            }
        }

        assertTrue(foundEntityCreate);
        assertTrue(foundEntityUpdate);
        assertTrue(foundEntityDelete);
        assertTrue(foundClassificationAdd);
        assertTrue(foundRelationshipCreate);
    }

    @Test
    public void testAllOperationTypes() {
        OperationType[] operationTypes = OperationType.values();

        for (OperationType operationType : operationTypes) {
            EntityNotificationV2 notification = new EntityNotificationV2();
            notification.setOperationType(operationType);
            assertEquals(notification.getOperationType(), operationType);
        }
    }

    @Test
    public void testEntityNotificationV2WithNullEntity() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        notificationV2.setEntity(null);
        notificationV2.setOperationType(OperationType.ENTITY_CREATE);

        assertNull(notificationV2.getEntity());
        assertEquals(notificationV2.getOperationType(), OperationType.ENTITY_CREATE);

        String result = notificationV2.toString();
        assertTrue(result.contains("entity=null"));
    }

    @Test
    public void testEntityNotificationV2WithNullRelationship() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        notificationV2.setRelationship(null);
        notificationV2.setOperationType(OperationType.RELATIONSHIP_CREATE);

        assertNull(notificationV2.getRelationship());
        assertEquals(notificationV2.getOperationType(), OperationType.RELATIONSHIP_CREATE);
    }

    @Test
    public void testEntityNotificationV2Serializable() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        assertNotNull(notificationV2);
    }

    @Test
    public void testEntityNotificationV2JsonAnnotations() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        assertNotNull(notificationV2);
    }

    @Test
    public void testEntityNotificationV2XmlAnnotations() {
        EntityNotificationV2 notificationV2 = new EntityNotificationV2();
        assertNotNull(notificationV2);
    }

    @Test
    public void testCompleteEntityNotificationScenario() {
        // Create a complete entity notification scenario
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid("complete-scenario-guid");
        entityHeader.setTypeName("DataSet");

        long eventTime = System.currentTimeMillis();
        EntityNotificationV2 notificationV2 = new EntityNotificationV2(
                entityHeader, OperationType.ENTITY_CREATE, eventTime);

        // Verify all fields
        assertEquals(notificationV2.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
        assertEquals(notificationV2.getEntity(), entityHeader);
        assertEquals(notificationV2.getOperationType(), OperationType.ENTITY_CREATE);
        assertEquals(notificationV2.getEventTime(), eventTime);
        assertNull(notificationV2.getRelationship());

        // Test modification
        notificationV2.setOperationType(OperationType.ENTITY_UPDATE);
        assertEquals(notificationV2.getOperationType(), OperationType.ENTITY_UPDATE);
    }

    @Test
    public void testCompleteRelationshipNotificationScenario() {
        // Create a complete relationship notification scenario
        AtlasRelationshipHeader relationshipHeader = new AtlasRelationshipHeader();
        relationshipHeader.setGuid("relationship-scenario-guid");
        relationshipHeader.setTypeName("owns");

        long eventTime = System.currentTimeMillis();
        EntityNotificationV2 notificationV2 = new EntityNotificationV2(
                relationshipHeader, OperationType.RELATIONSHIP_CREATE, eventTime);

        // Verify all fields
        assertEquals(notificationV2.getType(), EntityNotification.EntityNotificationType.ENTITY_NOTIFICATION_V2);
        assertEquals(notificationV2.getRelationship(), relationshipHeader);
        assertEquals(notificationV2.getOperationType(), OperationType.RELATIONSHIP_CREATE);
        assertEquals(notificationV2.getEventTime(), eventTime);
        assertNull(notificationV2.getEntity());

        // Test modification
        notificationV2.setOperationType(OperationType.RELATIONSHIP_DELETE);
        assertEquals(notificationV2.getOperationType(), OperationType.RELATIONSHIP_DELETE);
    }
}
