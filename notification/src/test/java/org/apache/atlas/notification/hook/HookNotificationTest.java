/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification.hook;

import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class HookNotificationTest {
    private HookMessageDeserializer deserializer = new HookMessageDeserializer();

    @Test
    public void testNewMessageSerDe() throws Exception {
        Referenceable entity1 = new Referenceable("sometype");
        entity1.set("attr", "value");
        entity1.set("complex", new Referenceable("othertype"));
        Referenceable entity2 = new Referenceable("newtype");
        String user = "user";

        EntityCreateRequest request           = new EntityCreateRequest(user, entity1, entity2);
        String              notificationJson  = AtlasType.toV1Json(request);
        HookNotification    actualNotification = deserializer.deserialize(notificationJson);

        assertEquals(actualNotification.getType(), HookNotificationType.ENTITY_CREATE);
        assertEquals(actualNotification.getUser(), user);
        assertTrue(actualNotification instanceof EntityCreateRequest);

        EntityCreateRequest createRequest = (EntityCreateRequest) actualNotification;

        assertEquals(createRequest.getEntities().size(), 2);

        Referenceable actualEntity1 = createRequest.getEntities().get(0);

        assertEquals(actualEntity1.getTypeName(), "sometype");
        assertEquals(((Referenceable)actualEntity1.get("complex")).getTypeName(), "othertype");
        assertEquals(createRequest.getEntities().get(1).getTypeName(), "newtype");
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        //Code to generate the json, use it for hard-coded json used later in this test
        Referenceable entity = new Referenceable("sometype");
        entity.set("attr", "value");

        EntityCreateRequest request                  = new EntityCreateRequest(null, entity);
        String              notificationJsonFromCode = AtlasType.toV1Json(request);

        System.out.println(notificationJsonFromCode);

        //Json without user and assert that the string can be deserialised
        String notificationJson = "{\n"
                + "  \"entities\": [\n"
                + "    {\n"
                + "      \"jsonClass\": \"org.apache.atlas.typesystem.json.InstanceSerialization$_Reference\",\n"
                + "      \"id\": {\n"
                + "        \"jsonClass\": \"org.apache.atlas.typesystem.json.InstanceSerialization$_Id\",\n"
                + "        \"id\": \"-1459493350903186000\",\n"
                + "        \"version\": 0,\n"
                + "        \"typeName\": \"sometype\",\n"
                + "        \"state\": \"ACTIVE\"\n"
                + "      },\n"
                + "      \"typeName\": \"sometype\",\n"
                + "      \"values\": {\n"
                + "        \"attr\": \"value\"\n"
                + "      },\n"
                + "      \"traitNames\": [],\n"
                + "      \"traits\": {}\n"
                + "    }\n"
                + "  ],\n"
                + "  \"type\": \"ENTITY_CREATE\"\n"
                + "}";


        HookNotification actualNotification = deserializer.deserialize(notificationJson);

        assertEquals(actualNotification.getType(), HookNotificationType.ENTITY_CREATE);
        assertEquals(actualNotification.getUser(), HookNotification.UNKNOW_USER);
    }
}
