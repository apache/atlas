/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification.hook;

import org.apache.atlas.notification.AbstractNotificationConsumer;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.codehaus.jettison.json.JSONArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class HookNotificationTest {

    @Test
    public void testMessageBackwardCompatibility() throws Exception {
        JSONArray jsonArray = new JSONArray();
        Referenceable entity = new Referenceable("sometype");
        entity.set("name", "somename");
        String entityJson = InstanceSerialization.toJson(entity, true);
        jsonArray.put(entityJson);

        HookNotification.HookNotificationMessage notification = AbstractNotificationConsumer.GSON.fromJson(
                jsonArray.toString(), HookNotification.HookNotificationMessage.class);
        assertNotNull(notification);
        assertEquals(notification.getType(), HookNotification.HookNotificationType.ENTITY_CREATE);
        HookNotification.EntityCreateRequest createRequest = (HookNotification.EntityCreateRequest) notification;
        assertEquals(createRequest.getEntities().size(), 1);
        assertEquals(createRequest.getEntities().get(0).getTypeName(), entity.getTypeName());
    }

    @Test
    public void testNewMessageSerDe() throws Exception {
        Referenceable entity1 = new Referenceable("sometype");
        entity1.set("attr", "value");
        entity1.set("complex", new Referenceable("othertype"));
        Referenceable entity2 = new Referenceable("newtype");
        HookNotification.EntityCreateRequest request = new HookNotification.EntityCreateRequest(entity1, entity2);

        String notificationJson = AbstractNotificationConsumer.GSON.toJson(request);
        HookNotification.HookNotificationMessage actualNotification = AbstractNotificationConsumer.GSON.fromJson(
                notificationJson, HookNotification.HookNotificationMessage.class);
        assertEquals(actualNotification.getType(), HookNotification.HookNotificationType.ENTITY_CREATE);
        HookNotification.EntityCreateRequest createRequest = (HookNotification.EntityCreateRequest) actualNotification;
        assertEquals(createRequest.getEntities().size(), 2);
        Referenceable actualEntity1 = createRequest.getEntities().get(0);
        assertEquals(actualEntity1.getTypeName(), "sometype");
        assertEquals(((Referenceable)actualEntity1.get("complex")).getTypeName(), "othertype");
        assertEquals(createRequest.getEntities().get(1).getTypeName(), "newtype");
    }
}
