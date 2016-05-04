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

package org.apache.atlas.notification.entity;

import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * EntityMessageDeserializer tests.
 */
public class EntityMessageDeserializerTest {

    @Test
    public void testDeserialize() throws Exception {
        EntityMessageDeserializer deserializer = new EntityMessageDeserializer();

        Referenceable entity = EntityNotificationImplTest.getEntity("id");
        String traitName = "MyTrait";
        List<IStruct> traitInfo = new LinkedList<>();
        IStruct trait = new Struct(traitName, Collections.<String, Object>emptyMap());
        traitInfo.add(trait);

        EntityNotificationImpl notification =
            new EntityNotificationImpl(entity, EntityNotification.OperationType.TRAIT_ADD, traitInfo);

        String json = AbstractNotification.getMessageJson(notification);

        EntityNotification deserializedNotification = deserializer.deserialize(json);
        assertEquals(deserializedNotification.getOperationType(), notification.getOperationType());
        assertEquals(deserializedNotification.getEntity().getId(), notification.getEntity().getId());
        assertEquals(deserializedNotification.getEntity().getTypeName(), notification.getEntity().getTypeName());
        assertEquals(deserializedNotification.getEntity().getTraits(), notification.getEntity().getTraits());
        assertEquals(deserializedNotification.getEntity().getTrait(traitName),
            notification.getEntity().getTrait(traitName));
    }
}
