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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.EntityStream;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.lang.RandomStringUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import java.util.List;

import org.apache.atlas.kafka.AtlasKafkaConsumer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import org.apache.commons.configuration.Configuration;
import org.apache.atlas.ApplicationProperties;
import static org.testng.Assert.*;



public class NotificationHookConsumerKafkaTest {

    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String QUALIFIED_NAME = "qualifiedName";
    private NotificationInterface notificationInterface = NotificationProvider.get();


    @Mock
    private AtlasEntityStore atlasEntityStore;

    @Mock
    private ServiceState serviceState;

    @Mock
    private AtlasInstanceConverter instanceConverter;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    private KafkaNotification kafkaNotification;

    @BeforeTest
    public void setup() throws AtlasException, InterruptedException, AtlasBaseException {
        MockitoAnnotations.initMocks(this);
        AtlasType mockType = mock(AtlasType.class);
        when(typeRegistry.getType(anyString())).thenReturn(mockType);
        AtlasEntity.AtlasEntitiesWithExtInfo mockEntity = mock(AtlasEntity.AtlasEntitiesWithExtInfo.class);
        when(instanceConverter.toAtlasEntities(anyList())).thenReturn(mockEntity);
        kafkaNotification = startKafkaServer();
    }

    @AfterTest
    public void shutdown() {
        kafkaNotification.close();
        kafkaNotification.stop();
    }

    @Test
    public void testConsumerConsumesNewMessageWithAutoCommitDisabled() throws AtlasException, InterruptedException, AtlasBaseException {
        try {
            produceMessage(new HookNotification.EntityCreateRequest("test_user1", createEntity()));

            NotificationConsumer<HookNotificationMessage> consumer = createNewConsumer(kafkaNotification, false);
            NotificationHookConsumer notificationHookConsumer =
                    new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry);
            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(consumer);

            consumeOneMessage(consumer, hookConsumer);
            verify(atlasEntityStore).createOrUpdate(any(EntityStream.class), anyBoolean());

            // produce another message, and make sure it moves ahead. If commit succeeded, this would work.
            produceMessage(new HookNotification.EntityCreateRequest("test_user2", createEntity()));
            consumeOneMessage(consumer, hookConsumer);
            verify(atlasEntityStore,times(2)).createOrUpdate(any(EntityStream.class), anyBoolean());
            reset(atlasEntityStore);
        }
        finally {
            kafkaNotification.close();
        }
    }

    @Test(dependsOnMethods = "testConsumerConsumesNewMessageWithAutoCommitDisabled")
    public void testConsumerRemainsAtSameMessageWithAutoCommitEnabled() throws Exception {
        try {
            produceMessage(new HookNotification.EntityCreateRequest("test_user3", createEntity()));

            NotificationConsumer<HookNotificationMessage> consumer = createNewConsumer(kafkaNotification, true);

            assertNotNull (consumer);

            NotificationHookConsumer notificationHookConsumer =
                    new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry);
            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(consumer);


            consumeOneMessage(consumer, hookConsumer);
            verify(atlasEntityStore).createOrUpdate(any(EntityStream.class), anyBoolean());

            // produce another message, but this will not be consumed, as commit code is not executed in hook consumer.
            produceMessage(new HookNotification.EntityCreateRequest("test_user4", createEntity()));

            consumeOneMessage(consumer, hookConsumer);
            verify(atlasEntityStore,times(2)).createOrUpdate(any(EntityStream.class), anyBoolean());
        }
        finally {
            kafkaNotification.close();
        }
    }

    AtlasKafkaConsumer<HookNotificationMessage> createNewConsumer(KafkaNotification kafkaNotification, boolean autoCommitEnabled) {
        return (AtlasKafkaConsumer) kafkaNotification.createConsumers(NotificationInterface.NotificationType.HOOK, 1, autoCommitEnabled).get(0);
    }

    void consumeOneMessage(NotificationConsumer<HookNotificationMessage> consumer,
                           NotificationHookConsumer.HookConsumer hookConsumer) throws InterruptedException {
        try {
            long startTime = System.currentTimeMillis(); //fetch starting time
            while ((System.currentTimeMillis() - startTime) < 10000) {
                List<AtlasKafkaMessage<HookNotificationMessage>> messages = consumer.receive(1000L);

                for (AtlasKafkaMessage<HookNotificationMessage> msg : messages) {
                    hookConsumer.handleMessage(msg);
                }

                if (messages.size() > 0) {
                    break;
                }
            }
        } catch (AtlasServiceException | AtlasException e) {
            Assert.fail("Consumer failed with exception ", e);
        }
    }

    Referenceable createEntity() {
        final Referenceable entity = new Referenceable(AtlasClient.DATA_SET_SUPER_TYPE);
        entity.set(NAME, "db" + randomString());
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, randomString());
        return entity;
    }

    KafkaNotification startKafkaServer() throws AtlasException, InterruptedException {
        Configuration applicationProperties = ApplicationProperties.get();
        applicationProperties.setProperty("atlas.kafka.data", "target/" + RandomStringUtils.randomAlphanumeric(5));

        kafkaNotification = new KafkaNotification(applicationProperties);
        kafkaNotification.start();
        Thread.sleep(2000);
        return kafkaNotification;
    }

    protected String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    private void produceMessage(HookNotificationMessage message) throws NotificationException {
        kafkaNotification.send(NotificationInterface.NotificationType.HOOK, message);
    }

}
