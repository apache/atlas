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
package org.apache.atlas.notification;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.atlas.notification.NotificationInterface.NotificationType.ASYNC_IMPORT;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NotificationHookConsumerTest {
    @Mock
    private NotificationInterface notificationInterface;

    @Mock
    private Configuration configuration;

    @Mock
    private ExecutorService executorService;

    @Mock
    private AtlasEntityStore atlasEntityStore;

    @Mock
    private ServiceState serviceState;

    @Mock
    private AtlasInstanceConverter instanceConverter;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private AtlasMetricsUtil metricsUtil;

    @Mock
    private AsyncImporter asyncImporter;

    @BeforeMethod
    public void setup() throws AtlasBaseException {
        MockitoAnnotations.initMocks(this);

        AtlasType                mockType   = mock(AtlasType.class);
        AtlasEntitiesWithExtInfo mockEntity = new AtlasEntitiesWithExtInfo(mock(AtlasEntity.class));

        when(typeRegistry.getType(anyString())).thenReturn(mockType);
        when(instanceConverter.toAtlasEntities(anyList())).thenReturn(mockEntity);

        EntityMutationResponse mutationResponse = mock(EntityMutationResponse.class);

        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean())).thenReturn(mutationResponse);
    }

    @Test
    public void testConsumerCanProceedIfServerIsReady() throws Exception {
        NotificationHookConsumer              notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationHookConsumer.HookConsumer hookConsumer             = notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer        timer                    = mock(NotificationHookConsumer.Timer.class);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        assertTrue(hookConsumer.serverAvailable(timer));

        verifyNoInteractions(timer);
    }

    @Test
    public void testConsumerWaitsNTimesIfServerIsNotReadyNTimes() throws Exception {
        NotificationHookConsumer              notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationHookConsumer.HookConsumer hookConsumer             = notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer        timer                    = mock(NotificationHookConsumer.Timer.class);

        when(serviceState.getState())
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        assertTrue(hookConsumer.serverAvailable(timer));

        verify(timer, times(3)).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
    }

    @Test
    public void testCommitIsCalledWhenMessageIsProcessed() throws AtlasServiceException, AtlasException {
        NotificationHookConsumer              notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationConsumer                  consumer                 = mock(NotificationConsumer.class);
        NotificationHookConsumer.HookConsumer hookConsumer             = notificationHookConsumer.new HookConsumer(consumer);
        EntityCreateRequest                   message                  = mock(EntityCreateRequest.class);
        Referenceable                         mock                     = mock(Referenceable.class);

        when(message.getUser()).thenReturn("user");
        when(message.getType()).thenReturn(HookNotificationType.ENTITY_CREATE);
        when(message.getEntities()).thenReturn(Collections.singletonList(mock));

        hookConsumer.handleMessage(new AtlasKafkaMessage(message, -1, KafkaNotification.ATLAS_HOOK_TOPIC, -1));

        verify(consumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testCommitIsNotCalledEvenWhenMessageProcessingFails() throws AtlasServiceException, AtlasException, AtlasBaseException {
        NotificationHookConsumer              notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationConsumer                  consumer                 = mock(NotificationConsumer.class);
        NotificationHookConsumer.HookConsumer hookConsumer             = notificationHookConsumer.new HookConsumer(consumer);
        EntityCreateRequest                   message                  = new EntityCreateRequest("user", Collections.singletonList(mock(Referenceable.class)));

        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean())).thenThrow(new RuntimeException("Simulating exception in processing message"));

        hookConsumer.handleMessage(new AtlasKafkaMessage(message, -1, KafkaNotification.ATLAS_HOOK_TOPIC, -1));

        verifyNoInteractions(consumer);
    }

    @Test
    public void testConsumerProceedsWithFalseIfInterrupted() throws Exception {
        NotificationHookConsumer              notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationHookConsumer.HookConsumer hookConsumer             = notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer        timer                    = mock(NotificationHookConsumer.Timer.class);

        doThrow(new InterruptedException()).when(timer).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        assertFalse(hookConsumer.serverAvailable(timer));
    }

    @Test
    public void testConsumersStartedIfHAIsDisabled() throws Exception {
        List<NotificationConsumer<Object>> consumers                = new ArrayList();
        NotificationConsumer               notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        notificationHookConsumer.startInternal(configuration, executorService);

        verify(notificationInterface).createConsumers(NotificationType.HOOK, 1);
        verify(executorService, times(1)).submit(any(NotificationHookConsumer.HookConsumer.class));
    }

    @Test
    public void testConsumersAreNotStartedIfHAIsEnabled() throws Exception {
        List<NotificationConsumer<Object>> consumers                = new ArrayList();
        NotificationConsumer               notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);

        notificationHookConsumer.startInternal(configuration, executorService);

        verifyNoInteractions(notificationInterface);
    }

    @Test
    public void testConsumersAreStartedWhenInstanceBecomesActive() throws Exception {
        List<NotificationConsumer<Object>> consumers                = new ArrayList();
        NotificationConsumer               notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);

        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsActive();

        verify(notificationInterface).createConsumers(NotificationType.HOOK, 1);
        verify(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));
    }

    @Test
    public void testConsumersAreStoppedWhenInstanceBecomesPassive() throws Exception {
        List<NotificationConsumer<Object>> consumers                = new ArrayList();
        NotificationConsumer               notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);
        final NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                notificationHookConsumer.consumers.get(0).start();
                Thread.sleep(500);

                return null;
            }
        }).when(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));

        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsPassive();

        verify(notificationInterface).close();
        verify(executorService).shutdown();
        verify(notificationConsumerMock).wakeup();
    }

    @Test
    public void consumersStoppedBeforeStarting() throws Exception {
        List<NotificationConsumer<Object>> consumers                = new ArrayList();
        NotificationConsumer               notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);
        final NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);

        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsPassive();

        verify(notificationInterface).close();
        verify(executorService).shutdown();
    }

    @Test
    public void consumersThrowsIllegalStateExceptionThreadUsesPauseRetryLogic() throws Exception {
        final NotificationHookConsumer notificationHookConsumer = setupNotificationHookConsumer();

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                notificationHookConsumer.consumers.get(0).start();
                Thread.sleep(1000);

                return null;
            }
        }).when(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));

        notificationHookConsumer.startInternal(configuration, executorService);
        Thread.sleep(1000);

        assertTrue(notificationHookConsumer.consumers.get(0).isAlive());

        notificationHookConsumer.consumers.get(0).shutdown();
    }

    @Test
    public void consumersThrowsIllegalStateExceptionPauseRetryLogicIsInterrupted() throws Exception {
        final NotificationHookConsumer notificationHookConsumer = setupNotificationHookConsumer();

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                notificationHookConsumer.consumers.get(0).start();
                Thread.sleep(500);

                return null;
            }
        }).when(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));

        notificationHookConsumer.startInternal(configuration, executorService);
        Thread.sleep(500);

        notificationHookConsumer.consumers.get(0).shutdown();
        Thread.sleep(500);

        assertFalse(notificationHookConsumer.consumers.get(0).isAlive());
    }

    @Test
    public void onCloseImportConsumerShutdownConsumerAndDeletesTopic() throws Exception {
        String                             importId  = "1b198cf8b55fed2e7829efea11f77795";
        String                             topic     = AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId;
        List<NotificationConsumer<Object>> consumers = new ArrayList<>();

        NotificationConsumer notificationHookImportConsumerMock = mock(NotificationConsumer.class);

        when(notificationHookImportConsumerMock.subscription()).thenReturn(Collections.emptySet());
        when(notificationHookImportConsumerMock.getTopicPartition()).thenReturn(Collections.emptySet());
        doNothing().when(notificationHookImportConsumerMock).close();

        consumers.add(notificationHookImportConsumerMock);

        doNothing().when(notificationInterface).addTopicToNotificationType(ASYNC_IMPORT, topic);
        when(notificationInterface.createConsumers(ASYNC_IMPORT, 1)).thenReturn(consumers);
        doNothing().when(notificationInterface).deleteTopic(ASYNC_IMPORT, AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId);

        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);

        // setting this just so this test would not create hook consumers
        Field consumerDisabledField = NotificationHookConsumer.class.getDeclaredField("consumerDisabled");
        consumerDisabledField.setAccessible(true);
        consumerDisabledField.set(notificationHookConsumer, true);

        // initializing the executors
        notificationHookConsumer.startInternal(configuration, null);

        notificationHookConsumer.startAsyncImportConsumer(ASYNC_IMPORT, importId, "ATLAS_IMPORT_" + importId);

        // consumer created
        assertTrue(notificationHookConsumer.consumers.stream().anyMatch(consumer -> consumer.getName().contains(importId)));

        notificationHookConsumer.closeImportConsumer(importId, "ATLAS_IMPORT_" + importId);

        // consumer deleted / shutdown and topic deleted
        assertTrue(notificationHookConsumer.consumers.stream().noneMatch(consumer -> consumer.getName().contains(importId)));
        verify(notificationInterface).deleteTopic(ASYNC_IMPORT, AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId);
    }

    @Test
    public void testExecutorCreatedOnlyOnceAcrossStartAndHAActive() throws Exception {
        // Setup
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        List<NotificationConsumer<Object>> consumers = new ArrayList<>();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        TestableNotificationHookConsumer hookConsumer = new TestableNotificationHookConsumer();

        // Call startInternal() twice
        hookConsumer.startInternal(configuration, null);
        hookConsumer.startInternal(configuration, null);

        // Simulate HA active instance, which may call executor creation
        hookConsumer.instanceIsActive();

        // Validate executor was created only once
        assertEquals(hookConsumer.getExecutorCreationCount(), 1, "Executor should be created only once and reused");
    }

    @Test
    public void testMultipleInstanceIsActiveCallsOnlyCreateExecutorOnce() throws Exception {
        TestableNotificationHookConsumer notificationHookConsumer = new TestableNotificationHookConsumer();

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1))
                .thenReturn(Collections.singletonList(mock(NotificationConsumer.class)));

        notificationHookConsumer.instanceIsActive();
        notificationHookConsumer.instanceIsActive();  // should not recreate

        assertEquals(notificationHookConsumer.getExecutorCreationCount(), 1,
                "Executor should be created only once even if instanceIsActive is called multiple times");
    }

    @Test
    public void testStartInternalThenInstanceIsActiveDoesNotCreateExecutorAgain() throws Exception {
        TestableNotificationHookConsumer notificationHookConsumer =
                new TestableNotificationHookConsumer();

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1))
                .thenReturn(Collections.singletonList(mock(NotificationConsumer.class)));

        notificationHookConsumer.startInternal(configuration, null);
        notificationHookConsumer.instanceIsActive();  // executor already exists

        assertEquals(notificationHookConsumer.getExecutorCreationCount(), 1,
                "Executor should not be created again in instanceIsActive if already created in startInternal");
    }

    @Test
    public void testImportConsumerUsesExistingExecutor() throws Exception {
        TestableNotificationHookConsumer notificationHookConsumer =
                new TestableNotificationHookConsumer();

        String importId = "test-import-id";
        String topic = "ATLAS_IMPORT_" + importId;

        when(notificationInterface.createConsumers(NotificationType.ASYNC_IMPORT, 1))
                .thenReturn(Collections.singletonList(mock(NotificationConsumer.class)));

        // Manually trigger executor creation
        notificationHookConsumer.startInternal(configuration, null);

        // Call import consumer – should use the same executor
        notificationHookConsumer.startAsyncImportConsumer(NotificationType.ASYNC_IMPORT, importId, topic);

        assertEquals(notificationHookConsumer.getExecutorCreationCount(), 1,
                "startImportNotificationConsumer should reuse existing executor and not create a new one");
    }

    @Test
    public void testHookConsumersNotStartedWhenConsumersAreDisabled() throws Exception {
        // Arrange
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);

        // TestableNotificationHookConsumer with override that sets consumerDisabled = true
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter) {
            @Override
            protected ExecutorService createExecutor() {
                return mock(ExecutorService.class);
            }

            @Override
            void startHookConsumers() {
                throw new RuntimeException("startHookConsumers should not be called when consumers are disabled");
            }
        };

        // Use reflection to manually set the consumerDisabled field to true
        Field consumerDisabledField = NotificationHookConsumer.class.getDeclaredField("consumerDisabled");
        consumerDisabledField.setAccessible(true);
        consumerDisabledField.set(notificationHookConsumer, true);

        // Act
        notificationHookConsumer.startInternal(configuration, null);

        // Assert
        // No exception = test passed; if startHookConsumers() is invoked, it will throw
    }

    private NotificationHookConsumer setupNotificationHookConsumer() throws AtlasException {
        List<NotificationConsumer<Object>> consumers                = new ArrayList<>();
        NotificationConsumer               notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationConsumerMock.receive()).thenThrow(new IllegalStateException());
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
    }

    class TestableNotificationHookConsumer extends NotificationHookConsumer {
        int executorCreationCount;

        TestableNotificationHookConsumer() throws AtlasException {
            super(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        }

        @Override
        protected ExecutorService createExecutor() {
            executorCreationCount++;
            return mock(ExecutorService.class);
        }

        public int getExecutorCreationCount() {
            return executorCreationCount;
        }
    }
}
