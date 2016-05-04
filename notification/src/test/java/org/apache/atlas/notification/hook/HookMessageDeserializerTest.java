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
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * HookMessageDeserializer tests.
 */
public class HookMessageDeserializerTest {
    @Test
    public void testDeserialize() throws Exception {
        HookMessageDeserializer deserializer = new HookMessageDeserializer();

        Referenceable entity = EntityNotificationImplTest.getEntity("id");
        String traitName = "MyTrait";
        List<IStruct> traitInfo = new LinkedList<>();
        IStruct trait = new Struct(traitName, Collections.<String, Object>emptyMap());
        traitInfo.add(trait);

        HookNotification.EntityUpdateRequest message =
            new HookNotification.EntityUpdateRequest("user1", entity);

        String json = AbstractNotification.getMessageJson(message);

        HookNotification.HookNotificationMessage deserializedMessage = deserializer.deserialize(json);

        assertEquals(deserializedMessage.getType(), message.getType());
        assertEquals(deserializedMessage.getUser(), message.getUser());

        assertTrue(deserializedMessage instanceof HookNotification.EntityUpdateRequest);

        HookNotification.EntityUpdateRequest deserializedEntityUpdateRequest =
            (HookNotification.EntityUpdateRequest) deserializedMessage;

        Referenceable deserializedEntity = deserializedEntityUpdateRequest.getEntities().get(0);
        assertEquals(deserializedEntity.getId(), entity.getId());
        assertEquals(deserializedEntity.getTypeName(), entity.getTypeName());
        assertEquals(deserializedEntity.getTraits(), entity.getTraits());
        assertEquals(deserializedEntity.getTrait(traitName), entity.getTrait(traitName));
    }
}
