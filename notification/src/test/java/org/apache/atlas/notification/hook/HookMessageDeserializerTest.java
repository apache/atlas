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

import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.entity.EntityNotificationImplTest;
import org.apache.atlas.notification.hook.HookNotification.EntityUpdateRequest;
import org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * HookMessageDeserializer tests.
 */
public class HookMessageDeserializerTest {
    HookMessageDeserializer deserializer = new HookMessageDeserializer();

    @Test
    public void testDeserialize() throws Exception {
        Referenceable       entity  = generateEntityWithTrait();
        EntityUpdateRequest message = new EntityUpdateRequest("user1", entity);

        List<String> jsonMsgList = new ArrayList<>();

        AbstractNotification.createNotificationMessages(message, jsonMsgList);

        HookNotificationMessage deserializedMessage = deserialize(jsonMsgList);

        assertEqualMessage(deserializedMessage, message);
    }

    // validate deserialization of legacy message, which doesn't use MessageVersion
    @Test
    public void testDeserializeLegacyMessage() throws Exception {
        Referenceable       entity  = generateEntityWithTrait();
        EntityUpdateRequest message = new EntityUpdateRequest("user1", entity);

        String                  jsonMsg             = AbstractNotification.GSON.toJson(message);
        HookNotificationMessage deserializedMessage = deserializer.deserialize(jsonMsg);

        assertEqualMessage(deserializedMessage, message);
    }

    @Test
    public void testDeserializeCompressedMessage() throws Exception {
        Referenceable       entity  = generateLargeEntityWithTrait();
        EntityUpdateRequest message = new EntityUpdateRequest("user1", entity);

        List<String> jsonMsgList = new ArrayList<>();

        AbstractNotification.createNotificationMessages(message, jsonMsgList);

        assertTrue(jsonMsgList.size() == 1);

        String compressedMsg   = jsonMsgList.get(0);
        String uncompressedMsg = AbstractNotification.GSON.toJson(message);

        assertTrue(compressedMsg.length() < uncompressedMsg.length(), "Compressed message (" + compressedMsg.length() + ") should be shorter than uncompressed message (" + uncompressedMsg.length() + ")");

        HookNotificationMessage deserializedMessage = deserialize(jsonMsgList);

        assertEqualMessage(deserializedMessage, message);
    }

    @Test
    public void testDeserializeSplitMessage() throws Exception {
        Referenceable       entity  = generateVeryLargeEntityWithTrait();
        EntityUpdateRequest message = new EntityUpdateRequest("user1", entity);

        List<String> jsonMsgList = new ArrayList<>();

        AbstractNotification.createNotificationMessages(message, jsonMsgList);

        assertTrue(jsonMsgList.size() > 1);

        HookNotificationMessage deserializedMessage = deserialize(jsonMsgList);

        assertEqualMessage(deserializedMessage, message);
    }

    private Referenceable generateEntityWithTrait() {
        Referenceable ret = EntityNotificationImplTest.getEntity("id", new Struct("MyTrait", Collections.<String, Object>emptyMap()));

        return ret;
    }

    private HookNotificationMessage deserialize(List<String> jsonMsgList) {
        HookNotificationMessage deserializedMessage = null;

        for (String jsonMsg : jsonMsgList) {
            deserializedMessage = deserializer.deserialize(jsonMsg);

            if (deserializedMessage != null) {
                break;
            }
        }

        return deserializedMessage;
    }

    private void assertEqualMessage(HookNotificationMessage deserializedMessage, EntityUpdateRequest message) throws Exception {
        assertNotNull(deserializedMessage);
        assertEquals(deserializedMessage.getType(), message.getType());
        assertEquals(deserializedMessage.getUser(), message.getUser());

        assertTrue(deserializedMessage instanceof EntityUpdateRequest);

        EntityUpdateRequest deserializedEntityUpdateRequest = (EntityUpdateRequest) deserializedMessage;
        Referenceable       deserializedEntity              = deserializedEntityUpdateRequest.getEntities().get(0);
        Referenceable       entity                          = message.getEntities().get(0);
        String              traitName                       = entity.getTraits().get(0);

        assertEquals(deserializedEntity.getId(), entity.getId());
        assertEquals(deserializedEntity.getTypeName(), entity.getTypeName());
        assertEquals(deserializedEntity.getTraits(), entity.getTraits());
        assertEquals(deserializedEntity.getTrait(traitName).hashCode(), entity.getTrait(traitName).hashCode());

    }

    private Referenceable generateLargeEntityWithTrait() {
        Referenceable ret = EntityNotificationImplTest.getEntity("id", new Struct("MyTrait", Collections.<String, Object>emptyMap()));

        // add 100 attributes, each with value of size 10k
        // Json Size=1,027,984; GZipped Size=16,387 ==> will compress, but not split
        String attrValue = RandomStringUtils.randomAlphanumeric(10 * 1024); // use the same value for all attributes - to aid better compression
        for (int i = 0; i < 100; i++) {
            ret.set("attr_" + i, attrValue);
        }

        return ret;
    }

    private Referenceable generateVeryLargeEntityWithTrait() {
        Referenceable ret = EntityNotificationImplTest.getEntity("id", new Struct("MyTrait", Collections.<String, Object>emptyMap()));

        // add 300 attributes, each with value of size 10k
        // Json Size=3,082,384; GZipped Size=2,313,357 ==> will compress & split
        for (int i = 0; i < 300; i++) {
            ret.set("attr_" + i, RandomStringUtils.randomAlphanumeric(10 * 1024));
        }

        return ret;
    }
}
