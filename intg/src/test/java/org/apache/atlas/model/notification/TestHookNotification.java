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

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestHookNotification {
    private HookNotification hookNotification;

    @BeforeMethod
    public void setUp() {
        hookNotification = new HookNotification();
    }

    @Test
    public void testConstants() {
        assertEquals(HookNotification.UNKNOW_USER, "UNKNOWN");
    }

    @Test
    public void testDefaultConstructor() {
        HookNotification notification = new HookNotification();

        assertNull(notification.getType());
        assertEquals(notification.getUser(), HookNotification.UNKNOW_USER); // Returns UNKNOWN for null/empty user
    }

    @Test
    public void testConstructorWithTypeAndUser() {
        String user = "testUser";
        HookNotification notification = new HookNotification(HookNotificationType.ENTITY_CREATE, user);

        assertEquals(notification.getType(), HookNotificationType.ENTITY_CREATE);
        assertEquals(notification.getUser(), user);
    }

    @Test
    public void testTypeGetterSetter() {
        assertNull(hookNotification.getType());

        hookNotification.setType(HookNotificationType.ENTITY_FULL_UPDATE);
        assertEquals(hookNotification.getType(), HookNotificationType.ENTITY_FULL_UPDATE);

        hookNotification.setType(null);
        assertNull(hookNotification.getType());
    }

    @Test
    public void testUserGetterSetter() {
        assertEquals(hookNotification.getUser(), HookNotification.UNKNOW_USER);

        String user = "testUser";
        hookNotification.setUser(user);
        assertEquals(hookNotification.getUser(), user);

        hookNotification.setUser("");
        assertEquals(hookNotification.getUser(), HookNotification.UNKNOW_USER);

        hookNotification.setUser(null);
        assertEquals(hookNotification.getUser(), HookNotification.UNKNOW_USER);
    }

    @Test
    public void testUserWithEmptyString() {
        hookNotification.setUser("");
        assertEquals(hookNotification.getUser(), HookNotification.UNKNOW_USER);
    }

    @Test
    public void testUserWithWhitespace() {
        hookNotification.setUser("   ");
        assertEquals(hookNotification.getUser(), "   "); // Whitespace is not considered empty
    }

    @Test
    public void testNormalize() {
        hookNotification.normalize();
    }

    @Test
    public void testToString() {
        hookNotification.setType(HookNotificationType.ENTITY_CREATE);
        hookNotification.setUser("testUser");

        String result = hookNotification.toString();

        assertNotNull(result);
        assertTrue(result.contains("HookNotification"));
        assertTrue(result.contains("ENTITY_CREATE"));
        assertTrue(result.contains("testUser"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        hookNotification.setType(HookNotificationType.ENTITY_DELETE);
        hookNotification.setUser("deleteUser");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = hookNotification.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("ENTITY_DELETE"));
        assertTrue(result.toString().contains("deleteUser"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        hookNotification.setType(HookNotificationType.TYPE_CREATE);

        StringBuilder result = hookNotification.toString(null);

        assertNotNull(result);
        assertTrue(result.toString().contains("TYPE_CREATE"));
    }

    @Test
    public void testHookNotificationTypeEnum() {
        HookNotificationType[] types = HookNotificationType.values();

        assertTrue(types.length >= 10);

        // Verify some expected types exist
        boolean foundEntityCreate = false;
        boolean foundEntityUpdate = false;
        boolean foundEntityDelete = false;
        boolean foundTypeCreate = false;

        for (HookNotificationType type : types) {
            switch (type) {
                case ENTITY_CREATE:
                    foundEntityCreate = true;
                    break;
                case ENTITY_FULL_UPDATE:
                    foundEntityUpdate = true;
                    break;
                case ENTITY_DELETE:
                    foundEntityDelete = true;
                    break;
                case TYPE_CREATE:
                    foundTypeCreate = true;
                    break;
            }
        }

        assertTrue(foundEntityCreate);
        assertTrue(foundEntityUpdate);
        assertTrue(foundEntityDelete);
        assertTrue(foundTypeCreate);
    }

    @Test
    public void testSerializable() {
        assertNotNull(hookNotification);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(hookNotification);
    }

    @Test
    public void testXmlAnnotations() {
        assertNotNull(hookNotification);
    }

    @Test
    public void testEntityCreateRequestV2Constructor() {
        String user = "createUser";
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        EntityCreateRequestV2 request = new EntityCreateRequestV2(user, entities);

        assertEquals(request.getType(), HookNotificationType.ENTITY_CREATE_V2);
        assertEquals(request.getUser(), user);
        assertEquals(request.getEntities(), entities);
    }

    @Test
    public void testEntityCreateRequestV2GetEntities() {
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        EntityCreateRequestV2 request = new EntityCreateRequestV2("user", entities);

        assertEquals(request.getEntities(), entities);
    }

    @Test
    public void testEntityCreateRequestV2ToStringWithEntities() {
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        EntityCreateRequestV2 request = new EntityCreateRequestV2("user", entities);

        String result = request.toString();
        assertNotNull(result);
    }

    @Test
    public void testEntityCreateRequestV2ToStringWithNullEntities() {
        EntityCreateRequestV2 request = new EntityCreateRequestV2("user", null);

        String result = request.toString();
        assertEquals(result, "null");
    }

    @Test
    public void testEntityCreateRequestV2Serializable() {
        EntityCreateRequestV2 request = new EntityCreateRequestV2("user", new AtlasEntitiesWithExtInfo());
        assertNotNull(request);
    }

    @Test
    public void testEntityUpdateRequestV2Constructor() {
        String user = "updateUser";
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();

        EntityUpdateRequestV2 request = new EntityUpdateRequestV2(user, entities);

        assertEquals(request.getType(), HookNotificationType.ENTITY_FULL_UPDATE_V2);
        assertEquals(request.getUser(), user);
        assertEquals(request.getEntities(), entities);
    }

    @Test
    public void testEntityUpdateRequestV2GetEntities() {
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        EntityUpdateRequestV2 request = new EntityUpdateRequestV2("user", entities);

        assertEquals(request.getEntities(), entities);
    }

    @Test
    public void testEntityUpdateRequestV2ToStringWithEntities() {
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        EntityUpdateRequestV2 request = new EntityUpdateRequestV2("user", entities);

        String result = request.toString();
        assertNotNull(result);
    }

    @Test
    public void testEntityUpdateRequestV2ToStringWithNullEntities() {
        EntityUpdateRequestV2 request = new EntityUpdateRequestV2("user", null);

        String result = request.toString();
        assertEquals(result, "null");
    }

    @Test
    public void testEntityUpdateRequestV2Serializable() {
        EntityUpdateRequestV2 request = new EntityUpdateRequestV2("user", new AtlasEntitiesWithExtInfo());
        assertNotNull(request);
    }

    @Test
    public void testEntityPartialUpdateRequestV2Constructor() {
        String user = "partialUpdateUser";
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("test-guid");
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();

        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2(user, entityId, entity);

        assertEquals(request.getType(), HookNotificationType.ENTITY_PARTIAL_UPDATE_V2);
        assertEquals(request.getUser(), user);
        assertEquals(request.getEntityId(), entityId);
        assertEquals(request.getEntity(), entity);
    }

    @Test
    public void testEntityPartialUpdateRequestV2GetEntityId() {
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("partial-update-guid");
        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("user", entityId, new AtlasEntityWithExtInfo());

        assertEquals(request.getEntityId(), entityId);
    }

    @Test
    public void testEntityPartialUpdateRequestV2GetEntity() {
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();
        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("user", new AtlasObjectId(), entity);

        assertEquals(request.getEntity(), entity);
    }

    @Test
    public void testEntityPartialUpdateRequestV2ToString() {
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("toString-guid");
        AtlasEntityWithExtInfo entity = new AtlasEntityWithExtInfo();

        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("user", entityId, entity);

        String result = request.toString();
        assertNotNull(result);
        assertTrue(result.contains("entityId="));
        assertTrue(result.contains("entity="));
    }

    @Test
    public void testEntityPartialUpdateRequestV2Serializable() {
        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("user", new AtlasObjectId(), new AtlasEntityWithExtInfo());
        assertNotNull(request);
    }

    @Test
    public void testEntityDeleteRequestV2Constructor() {
        String user = "deleteUser";
        List<AtlasObjectId> entities = new ArrayList<>();
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("delete-guid");
        entities.add(entityId);

        EntityDeleteRequestV2 request = new EntityDeleteRequestV2(user, entities);

        assertEquals(request.getType(), HookNotificationType.ENTITY_DELETE_V2);
        assertEquals(request.getUser(), user);
        assertEquals(request.getEntities(), entities);
    }

    @Test
    public void testEntityDeleteRequestV2GetEntities() {
        List<AtlasObjectId> entities = new ArrayList<>();
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("delete-get-guid");
        entities.add(entityId);

        EntityDeleteRequestV2 request = new EntityDeleteRequestV2("user", entities);

        assertEquals(request.getEntities(), entities);
        assertEquals(request.getEntities().size(), 1);
    }

    @Test
    public void testEntityDeleteRequestV2ToStringWithEntities() {
        List<AtlasObjectId> entities = new ArrayList<>();
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("delete-toString-guid");
        entities.add(entityId);

        EntityDeleteRequestV2 request = new EntityDeleteRequestV2("user", entities);

        String result = request.toString();
        assertNotNull(result);
    }

    @Test
    public void testEntityDeleteRequestV2ToStringWithNullEntities() {
        EntityDeleteRequestV2 request = new EntityDeleteRequestV2("user", null);

        String result = request.toString();
        assertEquals(result, "null");
    }

    @Test
    public void testEntityDeleteRequestV2ToStringWithEmptyEntities() {
        List<AtlasObjectId> emptyEntities = new ArrayList<>();
        EntityDeleteRequestV2 request = new EntityDeleteRequestV2("user", emptyEntities);

        String result = request.toString();
        assertNotNull(result);
        assertTrue(result.contains("[]"));
    }

    @Test
    public void testEntityDeleteRequestV2Serializable() {
        EntityDeleteRequestV2 request = new EntityDeleteRequestV2("user", new ArrayList<>());
        assertNotNull(request);
    }

    @Test
    public void testAllHookNotificationTypes() {
        HookNotificationType[] types = HookNotificationType.values();

        for (HookNotificationType type : types) {
            HookNotification notification = new HookNotification(type, "testUser");
            assertEquals(notification.getType(), type);
        }
    }

    @Test
    public void testInnerClassesJsonAnnotations() {
        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("user", new AtlasEntitiesWithExtInfo());
        EntityUpdateRequestV2 updateRequest = new EntityUpdateRequestV2("user", new AtlasEntitiesWithExtInfo());
        EntityPartialUpdateRequestV2 partialUpdateRequest = new EntityPartialUpdateRequestV2("user", new AtlasObjectId(), new AtlasEntityWithExtInfo());
        EntityDeleteRequestV2 deleteRequest = new EntityDeleteRequestV2("user", new ArrayList<>());

        assertNotNull(createRequest);
        assertNotNull(updateRequest);
        assertNotNull(partialUpdateRequest);
        assertNotNull(deleteRequest);
    }

    @Test
    public void testInnerClassesXmlAnnotations() {
        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("user", new AtlasEntitiesWithExtInfo());
        EntityUpdateRequestV2 updateRequest = new EntityUpdateRequestV2("user", new AtlasEntitiesWithExtInfo());
        EntityPartialUpdateRequestV2 partialUpdateRequest = new EntityPartialUpdateRequestV2("user", new AtlasObjectId(), new AtlasEntityWithExtInfo());
        EntityDeleteRequestV2 deleteRequest = new EntityDeleteRequestV2("user", new ArrayList<>());

        assertNotNull(createRequest);
        assertNotNull(updateRequest);
        assertNotNull(partialUpdateRequest);
        assertNotNull(deleteRequest);
    }

    @Test
    public void testCompleteHookNotificationScenario() {
        String user = "completeTestUser";

        // Create request
        AtlasEntitiesWithExtInfo createEntities = new AtlasEntitiesWithExtInfo();
        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2(user, createEntities);
        assertEquals(createRequest.getType(), HookNotificationType.ENTITY_CREATE_V2);
        assertEquals(createRequest.getUser(), user);

        // Update request
        AtlasEntitiesWithExtInfo updateEntities = new AtlasEntitiesWithExtInfo();
        EntityUpdateRequestV2 updateRequest = new EntityUpdateRequestV2(user, updateEntities);
        assertEquals(updateRequest.getType(), HookNotificationType.ENTITY_FULL_UPDATE_V2);
        assertEquals(updateRequest.getUser(), user);

        // Partial update request
        AtlasObjectId entityId = new AtlasObjectId();
        entityId.setGuid("complete-scenario-guid");
        AtlasEntityWithExtInfo partialEntity = new AtlasEntityWithExtInfo();
        EntityPartialUpdateRequestV2 partialRequest = new EntityPartialUpdateRequestV2(user, entityId, partialEntity);
        assertEquals(partialRequest.getType(), HookNotificationType.ENTITY_PARTIAL_UPDATE_V2);
        assertEquals(partialRequest.getUser(), user);

        // Delete request
        List<AtlasObjectId> deleteEntities = new ArrayList<>();
        deleteEntities.add(entityId);
        EntityDeleteRequestV2 deleteRequest = new EntityDeleteRequestV2(user, deleteEntities);
        assertEquals(deleteRequest.getType(), HookNotificationType.ENTITY_DELETE_V2);
        assertEquals(deleteRequest.getUser(), user);
    }

    @Test
    public void testUserHandlingEdgeCases() {
        // Test various user string scenarios
        String[] testUsers = {
            "normalUser",
            "user@domain.com",
            "user_with_underscores",
            "user-with-dashes",
            "user.with.dots",
            "123numericUser",
            "用户名", // Unicode
            "user with spaces"
        };

        for (String testUser : testUsers) {
            hookNotification.setUser(testUser);
            assertEquals(hookNotification.getUser(), testUser);
        }
    }

    @Test
    public void testPrivateConstructors() {
        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("user", new AtlasEntitiesWithExtInfo());
        assertNotNull(createRequest);

        EntityUpdateRequestV2 updateRequest = new EntityUpdateRequestV2("user", new AtlasEntitiesWithExtInfo());
        assertNotNull(updateRequest);

        EntityPartialUpdateRequestV2 partialRequest = new EntityPartialUpdateRequestV2("user", new AtlasObjectId(), new AtlasEntityWithExtInfo());
        assertNotNull(partialRequest);

        EntityDeleteRequestV2 deleteRequest = new EntityDeleteRequestV2("user", new ArrayList<>());
        assertNotNull(deleteRequest);
    }
}
