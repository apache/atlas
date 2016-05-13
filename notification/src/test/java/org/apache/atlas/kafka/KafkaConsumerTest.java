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

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.MessageVersion;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.IncompatibleVersionException;
import org.apache.atlas.notification.VersionedMessage;
import org.apache.atlas.notification.entity.EntityNotificationImplTest;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.codehaus.jettison.json.JSONException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

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
    private ConsumerConnector consumerConnector;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testNext() throws Exception {
        KafkaStream<String, String> stream = mock(KafkaStream.class);
        ConsumerIterator<String, String> iterator = mock(ConsumerIterator.class);
        MessageAndMetadata<String, String> messageAndMetadata = mock(MessageAndMetadata.class);

        Referenceable entity = getEntity(TRAIT_NAME);

        HookNotification.EntityUpdateRequest message =
            new HookNotification.EntityUpdateRequest("user1", entity);

        String json = AbstractNotification.GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), message));

        when(stream.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(messageAndMetadata).thenThrow(new NoSuchElementException());
        when(messageAndMetadata.message()).thenReturn(json);

        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
            new KafkaConsumer<>(
                    NotificationInterface.NotificationType.HOOK.getDeserializer(), stream, 99,
                    consumerConnector, false);

        assertTrue(consumer.hasNext());

        HookNotification.HookNotificationMessage consumedMessage = consumer.next();

        assertMessagesEqual(message, consumedMessage, entity);

        assertFalse(consumer.hasNext());
    }

    @Test
    public void testNextVersionMismatch() throws Exception {
        KafkaStream<String, String> stream = mock(KafkaStream.class);
        ConsumerIterator<String, String> iterator = mock(ConsumerIterator.class);
        MessageAndMetadata<String, String> messageAndMetadata = mock(MessageAndMetadata.class);

        Referenceable entity = getEntity(TRAIT_NAME);

        HookNotification.EntityUpdateRequest message =
            new HookNotification.EntityUpdateRequest("user1", entity);

        String json = AbstractNotification.GSON.toJson(new VersionedMessage<>(new MessageVersion("2.0.0"), message));

        when(stream.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(messageAndMetadata).thenThrow(new NoSuchElementException());
        when(messageAndMetadata.message()).thenReturn(json);

        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
            new KafkaConsumer<>(
                    NotificationInterface.NotificationType.HOOK.getDeserializer(), stream, 99,
                    consumerConnector, false);

        assertTrue(consumer.hasNext());

        try {
            consumer.next();
            fail("Expected VersionMismatchException!");
        } catch (IncompatibleVersionException e) {
            e.printStackTrace();
        }

        assertFalse(consumer.hasNext());
    }

    @Test
    public void testPeekMessage() throws Exception {
        KafkaStream<String, String> stream = mock(KafkaStream.class);
        ConsumerIterator<String, String> iterator = mock(ConsumerIterator.class);
        MessageAndMetadata<String, String> messageAndMetadata = mock(MessageAndMetadata.class);

        Referenceable entity = getEntity(TRAIT_NAME);

        HookNotification.EntityUpdateRequest message =
            new HookNotification.EntityUpdateRequest("user1", entity);

        String json = AbstractNotification.GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), message));

        when(stream.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.peek()).thenReturn(messageAndMetadata);
        when(messageAndMetadata.message()).thenReturn(json);

        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
            new KafkaConsumer<>(
                    NotificationInterface.NotificationType.HOOK.getDeserializer(), stream, 99,
                    consumerConnector, false);

        assertTrue(consumer.hasNext());

        HookNotification.HookNotificationMessage consumedMessage = consumer.peek();

        assertMessagesEqual(message, consumedMessage, entity);

        assertTrue(consumer.hasNext());
    }

    @Test
    public void testCommitIsCalledIfAutoCommitDisabled() {
        KafkaStream<String, String> stream = mock(KafkaStream.class);
        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
                new KafkaConsumer<>(
                        NotificationInterface.NotificationType.HOOK.getDeserializer(), stream, 99,
                        consumerConnector, false);

        consumer.commit();

        verify(consumerConnector).commitOffsets();
    }

    @Test
    public void testCommitIsNotCalledIfAutoCommitEnabled() {
        KafkaStream<String, String> stream = mock(KafkaStream.class);
        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
                new KafkaConsumer<>(
                        NotificationInterface.NotificationType.HOOK.getDeserializer(), stream, 99,
                        consumerConnector, true);

        consumer.commit();

        verify(consumerConnector, never()).commitOffsets();
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
