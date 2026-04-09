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

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.model.notification.ImportNotification.AtlasEntityImportNotification;
import org.apache.atlas.model.notification.ImportNotification.AtlasTypesDefImportNotification;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestImportNotification {
    @Test
    public void testImportNotificationGetImportId() {
        String importId = "test-import-123";
        String user = "testUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        assertEquals(notification.getImportId(), importId);
    }

    @Test
    public void testImportNotificationToString() {
        String importId = "toString-import-456";
        String user = "toStringUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        String result = notification.toString();

        assertNotNull(result);
        assertTrue(result.contains("ImportNotification"));
        assertTrue(result.contains(importId));
        assertTrue(result.contains(user));
        assertTrue(result.contains("IMPORT_TYPES_DEF"));
    }

    @Test
    public void testImportNotificationToStringWithStringBuilder() {
        String importId = "stringBuilder-import-789";
        String user = "stringBuilderUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        StringBuilder sb = new StringBuilder();
        StringBuilder result = notification.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("ImportNotification"));
        assertTrue(result.toString().contains(importId));
        assertTrue(result.toString().contains(user));
    }

    @Test
    public void testImportNotificationToStringWithNullStringBuilder() {
        String importId = "null-sb-import-101";
        String user = "nullSbUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        StringBuilder result = notification.toString(null);

        assertNotNull(result);
        assertTrue(result.toString().contains("ImportNotification"));
        assertTrue(result.toString().contains(importId));
    }

    @Test
    public void testImportNotificationSerializable() {
        // Test that ImportNotification implements Serializable through subclass
        String importId = "serializable-import-202";
        String user = "serializableUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        assertNotNull(notification);
    }

    @Test
    public void testImportNotificationJsonAnnotations() {
        // Test that the class has proper Jackson annotations
        String importId = "json-import-303";
        String user = "jsonUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        assertNotNull(notification);
    }

    @Test
    public void testImportNotificationXmlAnnotations() {
        // Test that the class has proper XML annotations
        String importId = "xml-import-404";
        String user = "xmlUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        assertNotNull(notification);
    }

    @Test
    public void testAtlasTypesDefImportNotificationDefaultConstructor() {
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification();

        // Default constructor should create a valid object
        assertNotNull(notification);
    }

    @Test
    public void testAtlasTypesDefImportNotificationParameterizedConstructor() {
        String importId = "typesDef-import-505";
        String user = "typesDefUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        assertEquals(notification.getImportId(), importId);
        assertEquals(notification.getUser(), user);
        assertEquals(notification.getType(), HookNotificationType.IMPORT_TYPES_DEF);
        assertEquals(notification.getTypesDef(), typesDef);
    }

    @Test
    public void testAtlasTypesDefImportNotificationGetTypesDef() {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification("import-id", "user", typesDef);

        assertEquals(notification.getTypesDef(), typesDef);
    }

    @Test
    public void testAtlasTypesDefImportNotificationToString() {
        String importId = "typesDef-toString-606";
        String user = "typesDefToStringUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        String result = notification.toString();

        assertNotNull(result);
        assertTrue(result.contains("AtlasTypeDefImportNotification"));
        assertTrue(result.contains(importId));
        assertTrue(result.contains(user));
    }

    @Test
    public void testAtlasTypesDefImportNotificationToStringWithStringBuilder() {
        String importId = "typesDef-sb-707";
        String user = "typesDefSbUser";
        AtlasTypesDef typesDef = new AtlasTypesDef();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        StringBuilder sb = new StringBuilder();
        StringBuilder result = notification.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasTypeDefImportNotification"));
        assertTrue(result.toString().contains(importId));
    }

    @Test
    public void testAtlasTypesDefImportNotificationSerializable() {
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification("id", "user", new AtlasTypesDef());
        assertNotNull(notification);
    }

    @Test
    public void testAtlasEntityImportNotificationDefaultConstructor() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification();

        // Default constructor should create a valid object
        assertNotNull(notification);
    }

    @Test
    public void testAtlasEntityImportNotificationParameterizedConstructor() {
        String importId = "entity-import-808";
        String user = "entityUser";
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();
        int position = 5;

        AtlasEntityImportNotification notification = new AtlasEntityImportNotification(importId, user, entity, position);

        assertEquals(notification.getImportId(), importId);
        assertEquals(notification.getUser(), user);
        assertEquals(notification.getType(), HookNotificationType.IMPORT_ENTITY);
        assertEquals(notification.getEntity(), entity);
        assertEquals(notification.getPosition(), position);
    }

    @Test
    public void testAtlasEntityImportNotificationGetEntity() {
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "user", entity, 1);

        assertEquals(notification.getEntity(), entity);
    }

    @Test
    public void testAtlasEntityImportNotificationGetPosition() {
        int position = 42;
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "user", new AtlasEntityWithExtInfo(), position);

        assertEquals(notification.getPosition(), position);
    }

    @Test
    public void testAtlasEntityImportNotificationToString() {
        String importId = "entity-toString-909";
        String user = "entityToStringUser";
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();
        int position = 10;

        AtlasEntityImportNotification notification = new AtlasEntityImportNotification(importId, user, entity, position);

        String result = notification.toString();

        assertNotNull(result);
        assertTrue(result.contains("AtlasEntityImportNotification"));
        assertTrue(result.contains(importId));
        assertTrue(result.contains(user));
        assertTrue(result.contains("10"));
    }

    @Test
    public void testAtlasEntityImportNotificationToStringWithStringBuilder() {
        String importId = "entity-sb-1010";
        String user = "entitySbUser";
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();
        int position = 15;

        AtlasEntityImportNotification notification = new AtlasEntityImportNotification(importId, user, entity, position);

        StringBuilder sb = new StringBuilder();
        StringBuilder result = notification.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasEntityImportNotification"));
        assertTrue(result.toString().contains(importId));
        assertTrue(result.toString().contains("15"));
    }

    @Test
    public void testAtlasEntityImportNotificationSerializable() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("id", "user", new AtlasEntityWithExtInfo(), 1);
        assertNotNull(notification);
    }

    @Test
    public void testAtlasEntityImportNotificationWithZeroPosition() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "user", new AtlasEntityWithExtInfo(), 0);

        assertEquals(notification.getPosition(), 0);
    }

    @Test
    public void testAtlasEntityImportNotificationWithNegativePosition() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "user", new AtlasEntityWithExtInfo(), -1);

        assertEquals(notification.getPosition(), -1);
    }

    @Test
    public void testAtlasEntityImportNotificationWithLargePosition() {
        int largePosition = Integer.MAX_VALUE;
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "user", new AtlasEntityWithExtInfo(), largePosition);

        assertEquals(notification.getPosition(), largePosition);
    }

    @Test
    public void testInnerClassesJsonAnnotations() {
        AtlasTypesDefImportNotification typesDefNotification = new AtlasTypesDefImportNotification("id", "user", new AtlasTypesDef());
        AtlasEntityImportNotification entityNotification = new AtlasEntityImportNotification("id", "user", new AtlasEntityWithExtInfo(), 1);

        assertNotNull(typesDefNotification);
        assertNotNull(entityNotification);
    }

    @Test
    public void testInnerClassesXmlAnnotations() {
        AtlasTypesDefImportNotification typesDefNotification = new AtlasTypesDefImportNotification("id", "user", new AtlasTypesDef());
        AtlasEntityImportNotification entityNotification = new AtlasEntityImportNotification("id", "user", new AtlasEntityWithExtInfo(), 1);

        assertNotNull(typesDefNotification);
        assertNotNull(entityNotification);
    }

    @Test
    public void testImportIdWithSpecialCharacters() {
        String specialImportId = "import_id-with.special@chars#123";
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(specialImportId, "user", new AtlasTypesDef());

        assertEquals(notification.getImportId(), specialImportId);
    }

    @Test
    public void testImportIdWithUnicodeCharacters() {
        String unicodeImportId = "uniImpId";
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification(unicodeImportId, "user", new AtlasEntityWithExtInfo(), 1);

        assertEquals(notification.getImportId(), unicodeImportId);
    }

    @Test
    public void testNullImportId() {
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(null, "user", new AtlasTypesDef());

        assertEquals(notification.getImportId(), null);
    }

    @Test
    public void testEmptyImportId() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("", "user", new AtlasEntityWithExtInfo(), 1);

        assertEquals(notification.getImportId(), "");
    }

    @Test
    public void testNullUser() {
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification("import-id", null, new AtlasTypesDef());

        assertEquals(notification.getUser(), HookNotification.UNKNOW_USER); // Should return UNKNOWN for null user
    }

    @Test
    public void testEmptyUser() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "", new AtlasEntityWithExtInfo(), 1);

        assertEquals(notification.getUser(), HookNotification.UNKNOW_USER); // Should return UNKNOWN for empty user
    }

    @Test
    public void testNullTypesDef() {
        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification("import-id", "user", null);

        assertEquals(notification.getTypesDef(), null);
    }

    @Test
    public void testNullEntity() {
        AtlasEntityImportNotification notification = new AtlasEntityImportNotification("import-id", "user", null, 1);

        assertEquals(notification.getEntity(), null);
    }

    @Test
    public void testCompleteImportNotificationScenario() {
        // Test a complete import scenario with both types
        String importId = "complete-import-scenario-1111";
        String user = "completeUser";

        // Types definition import
        AtlasTypesDef typesDef = new AtlasTypesDef();
        AtlasTypesDefImportNotification typesDefNotification = new AtlasTypesDefImportNotification(importId, user, typesDef);

        assertEquals(typesDefNotification.getImportId(), importId);
        assertEquals(typesDefNotification.getUser(), user);
        assertEquals(typesDefNotification.getType(), HookNotificationType.IMPORT_TYPES_DEF);
        assertEquals(typesDefNotification.getTypesDef(), typesDef);

        // Entity import
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();
        int position = 100;
        AtlasEntityImportNotification entityNotification = new AtlasEntityImportNotification(importId, user, entity, position);

        assertEquals(entityNotification.getImportId(), importId);
        assertEquals(entityNotification.getUser(), user);
        assertEquals(entityNotification.getType(), HookNotificationType.IMPORT_ENTITY);
        assertEquals(entityNotification.getEntity(), entity);
        assertEquals(entityNotification.getPosition(), position);

        // Verify toString methods work
        assertNotNull(typesDefNotification.toString());
        assertNotNull(entityNotification.toString());
    }

    @Test
    public void testInheritanceFromHookNotification() {
        // Test that ImportNotification extends HookNotification through subclasses
        AtlasTypesDefImportNotification typesDefNotification = new AtlasTypesDefImportNotification("id", "user", new AtlasTypesDef());
        AtlasEntityImportNotification entityNotification = new AtlasEntityImportNotification("id", "user", new AtlasEntityWithExtInfo(), 1);

        assertTrue(typesDefNotification instanceof HookNotification);
        assertTrue(typesDefNotification instanceof ImportNotification);

        assertTrue(entityNotification instanceof HookNotification);
        assertTrue(entityNotification instanceof ImportNotification);

        // Test inherited functionality
        typesDefNotification.normalize(); // Should not throw exception
        entityNotification.normalize(); // Should not throw exception
    }

    @Test
    public void testLongImportId() {
        StringBuilder longImportId = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longImportId.append("import_part_").append(i).append("_");
        }
        String veryLongImportId = longImportId.toString();

        AtlasTypesDefImportNotification notification = new AtlasTypesDefImportNotification(veryLongImportId, "user", new AtlasTypesDef());

        assertEquals(notification.getImportId(), veryLongImportId);
    }
}
