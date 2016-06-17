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

package org.apache.atlas.notification;

import com.google.inject.Inject;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.LocalAtlasClient;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Guice(modules = NotificationModule.class)
public class NotificationHookConsumerKafkaTest {

    @Inject
    private NotificationInterface notificationInterface;

    private KafkaNotification kafkaNotification;

    @BeforeTest
    public void setup() throws AtlasException, InterruptedException {
        kafkaNotification = startKafkaServer();
    }

    @AfterTest
    public void shutdown() {
        kafkaNotification.stop();
    }

    @Test
    public void testConsumerConsumesNewMessageWithAutoCommitDisabled() throws AtlasException, InterruptedException {

        produceMessage(new HookNotification.EntityCreateRequest("test_user1", createEntity()));

        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
                createNewConsumer(kafkaNotification, false);
        LocalAtlasClient localAtlasClient = mock(LocalAtlasClient.class);
        NotificationHookConsumer notificationHookConsumer =
                new NotificationHookConsumer(kafkaNotification, localAtlasClient);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(consumer);

        consumeOneMessage(consumer, hookConsumer);
        verify(localAtlasClient).setUser("test_user1");


        // produce another message, and make sure it moves ahead. If commit succeeded, this would work.
        produceMessage(new HookNotification.EntityCreateRequest("test_user2", createEntity()));
        consumeOneMessage(consumer, hookConsumer);
        verify(localAtlasClient).setUser("test_user2");

        kafkaNotification.close();
    }

    @Test
    public void testConsumerRemainsAtSameMessageWithAutoCommitEnabled()
            throws NotificationException, InterruptedException {

        produceMessage(new HookNotification.EntityCreateRequest("test_user3", createEntity()));

        NotificationConsumer<HookNotification.HookNotificationMessage> consumer =
                createNewConsumer(kafkaNotification, true);
        LocalAtlasClient localAtlasClient = mock(LocalAtlasClient.class);
        NotificationHookConsumer notificationHookConsumer =
                new NotificationHookConsumer(kafkaNotification, localAtlasClient);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(consumer);

        consumeOneMessage(consumer, hookConsumer);
        verify(localAtlasClient).setUser("test_user3");

        // produce another message, but this will not be consumed, as commit code is not executed in hook consumer.
        produceMessage(new HookNotification.EntityCreateRequest("test_user4", createEntity()));

        consumeOneMessage(consumer, hookConsumer);
        verify(localAtlasClient).setUser("test_user3");

        kafkaNotification.close();
    }

    NotificationConsumer<HookNotification.HookNotificationMessage> createNewConsumer(
            KafkaNotification kafkaNotification, boolean autoCommitEnabled) {
        return kafkaNotification.<HookNotification.HookNotificationMessage>createConsumers(
                NotificationInterface.NotificationType.HOOK, 1, autoCommitEnabled).get(0);
    }

    void consumeOneMessage(NotificationConsumer<HookNotification.HookNotificationMessage> consumer,
                           NotificationHookConsumer.HookConsumer hookConsumer) throws InterruptedException {
        while (!consumer.hasNext()) {
            Thread.sleep(1000);
        }
        hookConsumer.handleMessage(consumer.next());
    }

    Referenceable createEntity() {
        final Referenceable entity = new Referenceable(AtlasClient.DATA_SET_SUPER_TYPE);
        entity.set("name", "db" + randomString());
        entity.set("description", randomString());
        return entity;
    }

    KafkaNotification startKafkaServer() throws AtlasException, InterruptedException {
        KafkaNotification kafkaNotification = (KafkaNotification) notificationInterface;
        kafkaNotification.start();
        Thread.sleep(2000);
        return kafkaNotification;
    }

    protected String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    private void produceMessage(HookNotification.HookNotificationMessage message) throws NotificationException {
        notificationInterface.send(NotificationInterface.NotificationType.HOOK, message);
    }

}
