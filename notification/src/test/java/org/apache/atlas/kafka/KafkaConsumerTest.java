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

package org.apache.atlas.kafka;

import kafka.message.MessageAndMetadata;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.MessageVersion;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.IncompatibleVersionException;
import org.apache.atlas.notification.VersionedMessage;
import org.apache.atlas.notification.entity.EntityNotificationImplTest;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jettison.json.JSONException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * KafkaConsumer tests.
 */
public class KafkaConsumerTest {

    private static final String TRAIT_NAME = "MyTrait";


    @Mock
    private KafkaConsumer kafkaConsumer;


    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testReceive() throws Exception {


        MessageAndMetadata<String, String> messageAndMetadata = mock(MessageAndMetadata.class);

        Referenceable entity = getEntity(TRAIT_NAME);

        HookNotification.EntityUpdateRequest message =
            new HookNotification.EntityUpdateRequest("user1", entity);

        String json = AbstractNotification.GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), message));

        kafkaConsumer.assign(Arrays.asList(new TopicPartition("ATLAS_HOOK", 0)));
        List<ConsumerRecord> klist = new ArrayList<>();
        klist.add(new ConsumerRecord<String, String>("ATLAS_HOOK",
                0, 0L, "mykey", json));

        TopicPartition tp = new TopicPartition("ATLAS_HOOK",0);
        Map mp = new HashMap();
        mp.put(tp,klist);
        ConsumerRecords records = new ConsumerRecords(mp);


        when(kafkaConsumer.poll(1000)).thenReturn(records);
        when(messageAndMetadata.message()).thenReturn(json);


        AtlasKafkaConsumer consumer = new AtlasKafkaConsumer(NotificationInterface.NotificationType.HOOK.getDeserializer(), kafkaConsumer,false);
        List<AtlasKafkaMessage<HookNotification.HookNotificationMessage>> messageList = consumer.receive(1000);
        assertTrue(messageList.size() > 0);

        HookNotification.HookNotificationMessage consumedMessage  = messageList.get(0).getMessage();

        assertMessagesEqual(message, consumedMessage, entity);

    }

    @Test
    public void testNextVersionMismatch() throws Exception {

        MessageAndMetadata<String, String> messageAndMetadata = mock(MessageAndMetadata.class);

        Referenceable entity = getEntity(TRAIT_NAME);

        HookNotification.EntityUpdateRequest message =
            new HookNotification.EntityUpdateRequest("user1", entity);

        String json = AbstractNotification.GSON.toJson(new VersionedMessage<>(new MessageVersion("2.0.0"), message));

        kafkaConsumer.assign(Arrays.asList(new TopicPartition("ATLAS_HOOK", 0)));
        List<ConsumerRecord> klist = new ArrayList<>();
        klist.add(new ConsumerRecord<String, String>("ATLAS_HOOK",
                0, 0L, "mykey", json));

        TopicPartition tp = new TopicPartition("ATLAS_HOOK",0);
        Map mp = new HashMap();
        mp.put(tp,klist);
        ConsumerRecords records = new ConsumerRecords(mp);

        when(kafkaConsumer.poll(1000)).thenReturn(records);
        when(messageAndMetadata.message()).thenReturn(json);

        AtlasKafkaConsumer consumer =new AtlasKafkaConsumer(NotificationInterface.NotificationType.HOOK.getDeserializer(), kafkaConsumer ,false);
        try {
            List<AtlasKafkaMessage<HookNotification.HookNotificationMessage>> messageList = consumer.receive(1000);
            assertTrue(messageList.size() > 0);

            HookNotification.HookNotificationMessage consumedMessage  = messageList.get(0).getMessage();

            fail("Expected VersionMismatchException!");
        } catch (IncompatibleVersionException e) {
            e.printStackTrace();
        }

  }


    @Test
    public void testCommitIsCalledIfAutoCommitDisabled() {

        TopicPartition tp = new TopicPartition("ATLAS_HOOK",0);

        AtlasKafkaConsumer consumer =new AtlasKafkaConsumer(NotificationInterface.NotificationType.HOOK.getDeserializer(), kafkaConsumer, false);

        consumer.commit(tp, 1);

        verify(kafkaConsumer).commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(1)));
    }

    @Test
    public void testCommitIsNotCalledIfAutoCommitEnabled() {

        TopicPartition tp = new TopicPartition("ATLAS_HOOK",0);

        AtlasKafkaConsumer consumer =new AtlasKafkaConsumer(NotificationInterface.NotificationType.HOOK.getDeserializer(), kafkaConsumer, true);

        consumer.commit(tp, 1);

        verify(kafkaConsumer, never()).commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(1)));
    }

    private Referenceable getEntity(String traitName) {
        Referenceable entity = EntityNotificationImplTest.getEntity("id");
        List<IStruct> traitInfo = new LinkedList<>();
        IStruct trait = new Struct(traitName, Collections.<String, Object>emptyMap());
        traitInfo.add(trait);
        return entity;
    }

    private void assertMessagesEqual(HookNotification.EntityUpdateRequest message,
                                     HookNotification.HookNotificationMessage consumedMessage,
                                     Referenceable entity) throws JSONException {

        assertEquals(consumedMessage.getType(), message.getType());
        assertEquals(consumedMessage.getUser(), message.getUser());

        assertTrue(consumedMessage instanceof HookNotification.EntityUpdateRequest);

        HookNotification.EntityUpdateRequest deserializedEntityUpdateRequest =
            (HookNotification.EntityUpdateRequest) consumedMessage;

        Referenceable deserializedEntity = deserializedEntityUpdateRequest.getEntities().get(0);
        assertEquals(deserializedEntity.getId(), entity.getId());
        assertEquals(deserializedEntity.getTypeName(), entity.getTypeName());
        assertEquals(deserializedEntity.getTraits(), entity.getTraits());
        assertEquals(deserializedEntity.getTrait(TRAIT_NAME), entity.getTrait(TRAIT_NAME));
    }
}
