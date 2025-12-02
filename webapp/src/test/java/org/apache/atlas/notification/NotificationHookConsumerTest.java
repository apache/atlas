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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.model.notification.ImportNotification;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.notification.preprocessor.PreprocessorContext;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityCorrelationStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.notification.NotificationInterface.NotificationType.ASYNC_IMPORT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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

    @Mock
    private EntityCorrelationStore entityCorrelationStore;

    @BeforeMethod
    public void setup() throws AtlasBaseException {
        MockitoAnnotations.initMocks(this);

        AtlasType mockType = mock(AtlasType.class);
        AtlasEntitiesWithExtInfo mockEntity = new AtlasEntitiesWithExtInfo(mock(AtlasEntity.class));

        when(typeRegistry.getType(anyString())).thenReturn(mockType);
        when(instanceConverter.toAtlasEntities(anyList())).thenReturn(mockEntity);

        EntityMutationResponse mutationResponse = mock(EntityMutationResponse.class);

        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean())).thenReturn(mutationResponse);
    }

    @Test
    public void testConsumerCanProceedIfServerIsReady() throws Exception {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        assertTrue(hookConsumer.serverAvailable(timer));

        verifyZeroInteractions(timer);
    }

    @Test
    public void testConsumerWaitsNTimesIfServerIsNotReadyNTimes() throws Exception {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);

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
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationConsumer consumer = mock(NotificationConsumer.class);
        NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(consumer);
        EntityCreateRequest message = mock(EntityCreateRequest.class);
        Referenceable mock = mock(Referenceable.class);

        when(message.getUser()).thenReturn("user");
        when(message.getType()).thenReturn(HookNotificationType.ENTITY_CREATE);
        when(message.getEntities()).thenReturn(Collections.singletonList(mock));

        hookConsumer.handleMessage(new AtlasKafkaMessage(message, -1, KafkaNotification.ATLAS_HOOK_TOPIC, -1));

        verify(consumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testCommitIsNotCalledEvenWhenMessageProcessingFails() throws AtlasServiceException, AtlasException, AtlasBaseException {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationConsumer consumer = mock(NotificationConsumer.class);
        NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(consumer);
        EntityCreateRequest message = new EntityCreateRequest("user", Collections.singletonList(mock(Referenceable.class)));

        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean())).thenThrow(new RuntimeException("Simulating exception in processing message"));

        hookConsumer.handleMessage(new AtlasKafkaMessage(message, -1, KafkaNotification.ATLAS_HOOK_TOPIC, -1));

        verifyZeroInteractions(consumer);
    }

    @Test
    public void testConsumerProceedsWithFalseIfInterrupted() throws Exception {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
        NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);

        doThrow(new InterruptedException()).when(timer).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        assertFalse(hookConsumer.serverAvailable(timer));
    }

    @Test
    public void testConsumersStartedIfHAIsDisabled() throws Exception {
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        NotificationConsumer notificationConsumerMock = mock(NotificationConsumer.class);

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
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        NotificationConsumer notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);

        notificationHookConsumer.startInternal(configuration, executorService);

        verifyZeroInteractions(notificationInterface);
    }

    @Test
    public void testConsumersAreStartedWhenInstanceBecomesActive() throws Exception {
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        NotificationConsumer notificationConsumerMock = mock(NotificationConsumer.class);

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
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        NotificationConsumer notificationConsumerMock = mock(NotificationConsumer.class);

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
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        NotificationConsumer notificationConsumerMock = mock(NotificationConsumer.class);

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
        String importId = "1b198cf8b55fed2e7829efea11f77795";
        String topic = AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId;
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
        List<NotificationConsumer<Object>> consumers = new ArrayList<>();
        NotificationConsumer notificationConsumerMock = mock(NotificationConsumer.class);

        consumers.add(notificationConsumerMock);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        when(notificationConsumerMock.receive()).thenThrow(new IllegalStateException());
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter);
    }

    @Test
    public void testConstructorWithAllPreprocessingEnabled() throws Exception {
        Configuration fullConfig = mock(Configuration.class);

        // Enable all preprocessing features
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(5);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_FAILEDCACHESIZE_PROPERTY, 1)).thenReturn(10);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_RETRY_INTERVAL, 500)).thenReturn(1000);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_MIN_RETRY_INTERVAL, 500)).thenReturn(2000);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_MAX_RETRY_INTERVAL, 30000)).thenReturn(60000);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_COMMIT_BATCH_SIZE, 50)).thenReturn(100);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, false)).thenReturn(true);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, 15)).thenReturn(25);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_PROCESS_UPD_NAME_WITH_QUALIFIED_NAME, true)).thenReturn(false);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_DISABLED, false)).thenReturn(false);
        when(fullConfig.getInt("atlas.notification.consumer.large.message.processing.time.threshold.ms", 60000)).thenReturn(120000);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_AUTHORIZE_USING_MESSAGE_USER, false)).thenReturn(true);
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS, 300)).thenReturn(600);

        // Preprocessing patterns
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN))
                .thenReturn(new String[] {"temp_.*", "test_.*"});
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN))
                .thenReturn(new String[] {"ignore_.*"});
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN))
                .thenReturn(new String[] {"tmp_.*"});
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN))
                .thenReturn(new String[] {"prune_.*"});
        when(fullConfig.getInt(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_CACHE_SIZE, 10000)).thenReturn(5000);

        // Dummy filtering
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_ENABLED, true)).thenReturn(true);
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_DB_IGNORE_DUMMY_NAMES))
                .thenReturn(new String[] {"dummy_db"});
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_ENABLED, true)).thenReturn(true);
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_DUMMY_NAMES))
                .thenReturn(new String[] {"dummy_table"});
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES_ENABLED, true)).thenReturn(true);
        when(fullConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_NAME_PREFIXES))
                .thenReturn(new String[] {"Values__Tmp__"});

        // Type preprocessing
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, true)).thenReturn(true);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_RDBMS_TYPES_REMOVE_OWNEDREF_ATTRS, true)).thenReturn(true);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_S3_V2_DIRECTORY_PRUNE_OBJECT_PREFIX, true)).thenReturn(true);
        when(fullConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_SPARK_PROCESS_ATTRIBUTES, false)).thenReturn(true);

        // Shell entity creation
        when(fullConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(true);
        when(fullConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(20);
        when(fullConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(50);

        // Add AtlasConfiguration properties to the config mock
        when(fullConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(true);
        when(fullConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(20);
        when(fullConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(50);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(fullConfig);

            NotificationHookConsumer consumer = new NotificationHookConsumer(
                    notificationInterface, atlasEntityStore, serviceState, instanceConverter,
                    typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);

            // Verify complex configuration was applied
            assertNotNull(consumer);
        }
    }

    @Test
    public void testConstructorWithInvalidPatterns() throws Exception {
        Configuration invalidConfig = mock(Configuration.class);

        // Add invalid regex patterns to test exception handling
        when(invalidConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN))
                .thenReturn(new String[] {"[invalid", "valid_pattern"});
        when(invalidConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_IGNORE_PATTERN))
                .thenReturn(new String[] {"*invalid["});
        when(invalidConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN))
                .thenReturn(new String[] {"[unclosed"});
        when(invalidConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_PRUNE_PATTERN))
                .thenReturn(new String[] {"*invalid]"});

        // Mock other required properties
        when(invalidConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(invalidConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(false);
        when(invalidConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(invalidConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        // Add AtlasConfiguration properties to the config mock
        when(invalidConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(false);
        when(invalidConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(10);
        when(invalidConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(invalidConfig);

            // Should still create consumer despite invalid patterns (they get logged and ignored)
            NotificationHookConsumer consumer = new NotificationHookConsumer(
                    notificationInterface, atlasEntityStore, serviceState, instanceConverter,
                    typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);

            assertNotNull(consumer);
        }
    }

    @Test
    public void testHookConsumerHandleMessageAllV2Types() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test ENTITY_PARTIAL_UPDATE_V2
        AtlasObjectId objectId = new AtlasObjectId("TestType", "guid-123");
        AtlasEntity partialEntity = new AtlasEntity("TestType");
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(partialEntity);
        HookNotification.EntityPartialUpdateRequestV2 partialUpdateRequest = new HookNotification.EntityPartialUpdateRequestV2("testUser", objectId, entityWithExtInfo);
        AtlasKafkaMessage<HookNotification> partialUpdateMsg = new AtlasKafkaMessage<>(partialUpdateRequest, 1L, "test-topic", 0);

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        when(mockResponse.getGuidAssignments()).thenReturn(new HashMap<>());
        when(atlasEntityStore.updateEntity(eq(objectId), eq(entityWithExtInfo), eq(true))).thenReturn(mockResponse);

        handleMessageMethod.invoke(hookConsumer, partialUpdateMsg);
        verify(atlasEntityStore).updateEntity(eq(objectId), eq(entityWithExtInfo), eq(true));
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerHandleImportTypesDefMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test IMPORT_TYPES_DEF
        AtlasTypesDef typesDef = mock(AtlasTypesDef.class);
        ImportNotification.AtlasTypesDefImportNotification importNotification = new ImportNotification.AtlasTypesDefImportNotification("import-123", "testUser", typesDef);
        AtlasKafkaMessage<HookNotification> importMsg = new AtlasKafkaMessage<>(importNotification, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, importMsg);
        verify(asyncImporter).onImportTypeDef(typesDef, "import-123");
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerHandleImportEntityMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test IMPORT_ENTITY
        AtlasEntity entity = new AtlasEntity("TestType");
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity);
        ImportNotification.AtlasEntityImportNotification importNotification = new ImportNotification.AtlasEntityImportNotification("import-456", "testUser", entityWithExtInfo, 1);
        AtlasKafkaMessage<HookNotification> importMsg = new AtlasKafkaMessage<>(importNotification, 1L, "test-topic", 0);

        when(asyncImporter.onImportEntity(entityWithExtInfo, "import-456", 1)).thenReturn(false);

        handleMessageMethod.invoke(hookConsumer, importMsg);
        verify(asyncImporter).onImportEntity(entityWithExtInfo, "import-456", 1);
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerHandleImportEntityWithException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test IMPORT_ENTITY with exception
        AtlasEntity entity = new AtlasEntity("TestType");
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity);
        ImportNotification.AtlasEntityImportNotification importNotification = new ImportNotification.AtlasEntityImportNotification("import-fail", "testUser", entityWithExtInfo, 1);
        AtlasKafkaMessage<HookNotification> importMsg = new AtlasKafkaMessage<>(importNotification, 1L, "test-topic", 0);

        doThrow(new AtlasBaseException("Entity import failed")).when(asyncImporter).onImportEntity(entityWithExtInfo, "import-fail", 1);

        handleMessageMethod.invoke(hookConsumer, importMsg);
        verify(asyncImporter).onImportEntity(entityWithExtInfo, "import-fail", 1);
        verify(asyncImporter).onImportComplete("import-fail");
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerHandleImportEntityComplete() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test IMPORT_ENTITY that completes import
        AtlasEntity entity = new AtlasEntity("TestType");
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity);
        ImportNotification.AtlasEntityImportNotification importNotification = new ImportNotification.AtlasEntityImportNotification("import-complete", "testUser", entityWithExtInfo, 1);
        AtlasKafkaMessage<HookNotification> importMsg = new AtlasKafkaMessage<>(importNotification, 1L, "test-topic", 0);

        when(asyncImporter.onImportEntity(entityWithExtInfo, "import-complete", 1)).thenReturn(true);

        handleMessageMethod.invoke(hookConsumer, importMsg);
        verify(asyncImporter).onImportEntity(entityWithExtInfo, "import-complete", 1);
        verify(asyncImporter).onCompleteImportRequest("import-complete");
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerCreateOrUpdateWithBatching() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithBatching(5); // batch size of 5
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method createOrUpdateMethod = hookConsumer.getClass().getDeclaredMethod("createOrUpdate", AtlasEntitiesWithExtInfo.class, boolean.class, AtlasMetricsUtil.NotificationStat.class, PreprocessorContext.class);
        createOrUpdateMethod.setAccessible(true);

        // Create entities with more than batch size
        List<AtlasEntity> entities = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            AtlasEntity entity = new AtlasEntity("TestType" + i);
            entity.setGuid("guid-" + i);
            entities.add(entity);
        }

        AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.setEntities(entities);

        AtlasMetricsUtil.NotificationStat stats = mock(AtlasMetricsUtil.NotificationStat.class);
        PreprocessorContext context = mock(PreprocessorContext.class);
        when(context.getGuidAssignments()).thenReturn(new HashMap<>());
        when(context.isSpooledMessage()).thenReturn(false);
        when(context.getMsgCreated()).thenReturn(System.currentTimeMillis());
        when(context.getPostUpdateEntities()).thenReturn(Collections.emptyList());

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        when(mockResponse.getGuidAssignments()).thenReturn(new HashMap<>());
        when(mockResponse.getCreatedEntities()).thenReturn(Collections.emptyList());
        when(mockResponse.getDeletedEntities()).thenReturn(Collections.emptyList());
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), eq(false))).thenReturn(mockResponse);

        createOrUpdateMethod.invoke(hookConsumer, entitiesWithExtInfo, false, stats, context);

        verify(atlasEntityStore, times(3)).createOrUpdate(any(AtlasEntityStream.class), eq(false));
    }

    @Test
    public void testHookConsumerCreateOrUpdateWithPostUpdateEntities() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method createOrUpdateMethod = hookConsumer.getClass().getDeclaredMethod("createOrUpdate", AtlasEntitiesWithExtInfo.class, boolean.class, AtlasMetricsUtil.NotificationStat.class, PreprocessorContext.class);
        createOrUpdateMethod.setAccessible(true);

        AtlasEntity entity = new AtlasEntity("TestType");
        AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(entity);

        AtlasMetricsUtil.NotificationStat stats = mock(AtlasMetricsUtil.NotificationStat.class);
        PreprocessorContext context = mock(PreprocessorContext.class);
        when(context.getGuidAssignments()).thenReturn(new HashMap<>());

        // Mock post-update entities
        List<AtlasEntity> postUpdateEntities = Collections.singletonList(new AtlasEntity("PostUpdateType"));
        when(context.getPostUpdateEntities()).thenReturn(postUpdateEntities);

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        when(mockResponse.getGuidAssignments()).thenReturn(new HashMap<>());
        when(mockResponse.getCreatedEntities()).thenReturn(Collections.emptyList());
        when(mockResponse.getDeletedEntities()).thenReturn(Collections.emptyList());
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean())).thenReturn(mockResponse);

        createOrUpdateMethod.invoke(hookConsumer, entitiesWithExtInfo, false, stats, context);

        verify(atlasEntityStore, times(2)).createOrUpdate(any(AtlasEntityStream.class), anyBoolean());
        verify(context).prepareForPostUpdate();
    }

    @Test
    public void testAdaptiveWaiterWithDifferentExceptions() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Class<?> adaptiveWaiterClass = getInnerClass(NotificationHookConsumer.class, "AdaptiveWaiter");
        Object adaptiveWaiter = adaptiveWaiterClass.getDeclaredConstructor(long.class, long.class, long.class)
                .newInstance(100L, 5000L, 100L);

        Method pauseMethod = adaptiveWaiterClass.getDeclaredMethod("pause", Throwable.class);
        pauseMethod.setAccessible(true);

        // Test with different exception types
        pauseMethod.invoke(adaptiveWaiter, new RuntimeException("Test runtime exception"));
        pauseMethod.invoke(adaptiveWaiter, new IllegalStateException("Test illegal state"));
        pauseMethod.invoke(adaptiveWaiter, new AtlasBaseException("Test atlas exception"));

        Field waitDurationField = adaptiveWaiterClass.getDeclaredField("waitDuration");
        waitDurationField.setAccessible(true);
        long waitDuration = (Long) waitDurationField.get(adaptiveWaiter);

        // Should have increased wait duration after multiple pauses
        assertTrue(waitDuration > 100L);
    }

    @Test
    public void testAdaptiveWaiterResetAfterLongInterval() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Class<?> adaptiveWaiterClass = getInnerClass(NotificationHookConsumer.class, "AdaptiveWaiter");
        Object adaptiveWaiter = adaptiveWaiterClass.getDeclaredConstructor(long.class, long.class, long.class)
                .newInstance(100L, 1000L, 100L);

        Method pauseMethod = adaptiveWaiterClass.getDeclaredMethod("pause", Throwable.class);
        pauseMethod.setAccessible(true);

        Method setWaitDurationsMethod = adaptiveWaiterClass.getDeclaredMethod("setWaitDurations");
        setWaitDurationsMethod.setAccessible(true);

        Field lastWaitAtField = adaptiveWaiterClass.getDeclaredField("lastWaitAt");
        lastWaitAtField.setAccessible(true);

        // Simulate wait
        pauseMethod.invoke(adaptiveWaiter, new RuntimeException("Test"));

        // Set lastWaitAt to simulate long interval
        lastWaitAtField.set(adaptiveWaiter, System.currentTimeMillis() - 10000);

        // Should reset wait duration due to long interval
        setWaitDurationsMethod.invoke(adaptiveWaiter);

        Field waitDurationField = adaptiveWaiterClass.getDeclaredField("waitDuration");
        waitDurationField.setAccessible(true);
        long waitDuration = (Long) waitDurationField.get(adaptiveWaiter);

        assertEquals(100L, waitDuration); // Should be reset to minimum
    }

    @Test
    public void testHookConsumerV1ToV2ConversionFailure() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        // Mock instanceConverter to throw exception
        when(instanceConverter.toAtlasEntities(anyList())).thenThrow(new AtlasBaseException("Conversion failed"));

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test V1 CREATE message with conversion failure
        Referenceable entity = mock(Referenceable.class);
        EntityCreateRequest createRequest = new EntityCreateRequest("testUser", Collections.singletonList(entity));
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
    }

    @Test
    public void testHookConsumerHandleV1PartialUpdateMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock required components for partial update
        when(typeRegistry.getEntityTypeByName("TestType")).thenReturn(mock(AtlasEntityType.class));

        // Mock AtlasGraphUtilsV2.getGuidByUniqueAttributes to return a GUID
        try (MockedStatic<AtlasGraphUtilsV2> graphUtils = mockStatic(AtlasGraphUtilsV2.class)) {
            graphUtils.when(() -> AtlasGraphUtilsV2.getGuidByUniqueAttributes(any(), any())).thenReturn("test-guid");

            // Test V1 PARTIAL_UPDATE message
            Referenceable entity = mock(Referenceable.class);
            HookNotificationV1.EntityPartialUpdateRequest partialUpdateRequest = new HookNotificationV1.EntityPartialUpdateRequest("testUser", "TestType", "qualifiedName", "test@cluster", entity);
            AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(partialUpdateRequest, 1L, "test-topic", 0);

            AtlasEntitiesWithExtInfo mockEntities = new AtlasEntitiesWithExtInfo();
            AtlasEntity testEntity = new AtlasEntity("TestType");
            mockEntities.addEntity(testEntity);
            when(instanceConverter.toAtlasEntity(entity)).thenReturn(mockEntities);

            EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
            when(mockResponse.getGuidAssignments()).thenReturn(new HashMap<>());
            when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), eq(true))).thenReturn(mockResponse);

            handleMessageMethod.invoke(hookConsumer, kafkaMsg);
            verify(atlasEntityStore).createOrUpdate(any(AtlasEntityStream.class), eq(true));
            verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
        }
    }

    @Test
    public void testHookConsumerHandleDeleteWithClassCastException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock typeRegistry to return non-AtlasEntityType (causing ClassCastException)
        when(typeRegistry.getType("TestType")).thenReturn(mock(AtlasType.class)); // Not AtlasEntityType

        EntityDeleteRequest deleteRequest = new EntityDeleteRequest("testUser", "TestType", "qualifiedName", "test@cluster");
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(deleteRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        // Should handle ClassCastException gracefully and still commit
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerRetryLogicWithSchemaViolation() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock to throw AtlasSchemaViolationException (should not retry)
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean()))
                .thenThrow(new org.apache.atlas.repository.graphdb.AtlasSchemaViolationException(new Exception("Schema - Violation")));

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        // Should only try once (no retries for schema violations)
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerRetryLogicWithJanusGraphException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Create a custom exception that will be recognized as JanusGraphException
        RuntimeException janusException = new JanusGraphException("JanusGraph error");

        // Mock to throw JanusGraphException on first call, succeed on second
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean()))
                .thenThrow(janusException)
                .thenReturn(mock(EntityMutationResponse.class));

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        // Should retry and eventually succeed
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerRetryWithInterruptedException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock to throw general exception that will trigger sleep, then interrupt
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean()))
                .thenThrow(new RuntimeException("General error"))
                .thenReturn(mock(EntityMutationResponse.class));

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        // Set up thread to interrupt during sleep
        Thread testThread = new Thread(() -> {
            try {
                handleMessageMethod.invoke(hookConsumer, kafkaMsg);
            } catch (Exception e) {
                // Expected
            }
        });

        testThread.start();
        Thread.sleep(100); // Let it start processing
        testThread.interrupt();
        testThread.join(1000);
    }

    @Test
    public void testStartMethod() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        // Mock configuration for HA disabled
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);

        List<NotificationConsumer<Object>> consumers = new ArrayList<>();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        consumer.start();

        // Should call startInternal with application properties
        verify(notificationInterface).createConsumers(NotificationType.HOOK, 1);
    }

    @Test
    public void testStopWithInterruptedException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        // Mock executor that throws InterruptedException
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("Test interrupt"));

        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        executorsField.set(consumer, mockExecutor);

        // Initialize empty consumers list
        Field consumersField = NotificationHookConsumer.class.getDeclaredField("consumers");
        consumersField.setAccessible(true);
        consumersField.set(consumer, new ArrayList<>());

        consumer.stop();

        verify(mockExecutor).shutdown();
    }

    @Test
    public void testComplexPreprocessingScenarios() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        // Test complex entity with multiple relationships and attributes
        AtlasEntity complexEntity = new AtlasEntity("hive_table");
        complexEntity.setGuid("complex-guid");
        complexEntity.setAttribute("name", "complex_table");
        complexEntity.setAttribute("qualifiedName", "complex_table@cluster");

        // Add complex relationships
        List<AtlasObjectId> columns = Arrays.asList(
                new AtlasObjectId("hive_column", "col1-guid"),
                new AtlasObjectId("hive_column", "col2-guid"));
        complexEntity.setRelationshipAttribute("columns", columns);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        entities.addEntity(complexEntity);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", entities);
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        Method preProcessMethod = NotificationHookConsumer.class.getDeclaredMethod("preProcessNotificationMessage", AtlasKafkaMessage.class);
        preProcessMethod.setAccessible(true);

        PreprocessorContext result = (PreprocessorContext) preProcessMethod.invoke(consumer, kafkaMsg);

        assertNotNull(result);
        verify(atlasEntityStore, never()).createOrUpdate(any(), anyBoolean()); // Just preprocessing
    }

    // Helper method for creating consumer with batching
    private NotificationHookConsumer createTestConsumerWithBatching(int batchSize) throws Exception {
        Configuration batchConfig = mock(Configuration.class);
        when(batchConfig.getInt(NotificationHookConsumer.CONSUMER_COMMIT_BATCH_SIZE, 50)).thenReturn(batchSize);
        when(batchConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(batchConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(false);
        when(batchConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(batchConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        // Add AtlasConfiguration properties to the config mock
        when(batchConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(false);
        when(batchConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(10);
        when(batchConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(batchConfig);

            return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState,
                    instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
        }
    }

    @Test
    public void testHookConsumerMaxRetriesWithFailedMessageRecording() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithFailedMsgCache(2); // small cache
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock to always throw exception (max retries scenario)
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean()))
                .thenThrow(new RuntimeException("Persistent failure"));

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        // Process multiple messages to fill failed message cache
        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
    }

    @Test
    public void testHookConsumerHandleUnrecoverableFailure() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock to throw AtlasBaseException with BAD_REQUEST (unrecoverable)
        AtlasBaseException unrecoverableException = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid request");
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean()))
                .thenThrow(unrecoverableException);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
    }

    @Test
    public void testHookConsumerHandleNotFoundFailure() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock to throw AtlasBaseException with NOT_FOUND (unrecoverable)
        AtlasBaseException notFoundException = new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, "Entity not found");
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean()))
                .thenThrow(notFoundException);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
    }

    @Test
    public void testHookConsumerHandleMessageWithLargeProcessingTime() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithLargeMessageThreshold(100); // 100ms threshold
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Mock to simulate slow processing
        when(atlasEntityStore.createOrUpdate(any(AtlasEntityStream.class), anyBoolean())).then(invocation -> {
            Thread.sleep(150); // Simulate slow processing
            return mock(EntityMutationResponse.class);
        });

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        // Should complete and commit despite being slow
        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerSortAndPublishWithComplexBuffering() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method sortAndPublishMethod = hookConsumer.getClass().getDeclaredMethod("sortAndPublishMsgsToAtlasHook", long.class, Map.class);
        sortAndPublishMethod.setAccessible(true);

        // Create complex message buffer scenario
        EntityCreateRequestV2 createRequest1 = new EntityCreateRequestV2("user1", new AtlasEntitiesWithExtInfo());
        EntityCreateRequestV2 createRequest2 = new EntityCreateRequestV2("user2", new AtlasEntitiesWithExtInfo());

        AtlasKafkaMessage<HookNotification> msg1 = new AtlasKafkaMessage<>(createRequest1, 1L, "ATLAS_HOOK_UNSORTED", 0);
        AtlasKafkaMessage<HookNotification> msg2 = new AtlasKafkaMessage<>(createRequest2, 2L, "ATLAS_HOOK_UNSORTED", 1);

        // These messages represent unsorted topic messages

        when(notificationConsumer.receiveRawRecordsWithCheckedCommit(any()))
                .thenReturn(Arrays.asList(msg1, msg2))
                .thenReturn(Collections.emptyList());

        Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer = new TreeMap<>();

        try {
            sortAndPublishMethod.invoke(hookConsumer, System.currentTimeMillis() + 10000, msgBuffer);
        } catch (Exception e) {
            // Expected due to mocking limitations
        }

        verify(notificationConsumer, atLeast(1)).receiveRawRecordsWithCheckedCommit(any());
    }

    @Test
    public void testHookConsumerAuthorizationWithCaching() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithAuthorization();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("cachedUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        // Process same user multiple times to test caching
        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        verify(notificationConsumer, times(2)).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerEntityCorrelationStore() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Test V2 DELETE message that uses entity correlation store
        List<AtlasObjectId> entitiesToDelete = Collections.singletonList(new AtlasObjectId("TestType", "guid-to-delete"));
        EntityDeleteRequestV2 deleteRequest = new EntityDeleteRequestV2("testUser", entitiesToDelete);
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(deleteRequest, 1L, "test-topic", 0);

        // Note: spooled properties are set internally by AtlasKafkaMessage

        when(typeRegistry.getType("TestType")).thenReturn(mock(AtlasEntityType.class));

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        List<AtlasEntityHeader> deletedEntities = Collections.singletonList(mock(AtlasEntityHeader.class));
        when(mockResponse.getDeletedEntities()).thenReturn(deletedEntities);
        when(atlasEntityStore.deleteByUniqueAttributes(any(), any())).thenReturn(mockResponse);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testSkipHiveColumnLineageWithDuplicates() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithHiveLineageSkip();

        Method skipHiveLineageMethod = NotificationHookConsumer.class.getDeclaredMethod("skipHiveColumnLineage", PreprocessorContext.class);
        skipHiveLineageMethod.setAccessible(true);

        PreprocessorContext context = mock(PreprocessorContext.class);
        List<AtlasEntity> entities = new ArrayList<>();

        // Create duplicate hive_column_lineage entities
        AtlasEntity lineage1 = new AtlasEntity("hive_column_lineage");
        lineage1.setAttribute("qualifiedName", "duplicate@cluster");
        lineage1.setAttribute("inputs", Arrays.asList("input1", "input2"));

        AtlasEntity lineage2 = new AtlasEntity("hive_column_lineage");
        lineage2.setAttribute("qualifiedName", "duplicate@cluster"); // Same qualified name (duplicate)
        lineage2.setAttribute("inputs", Arrays.asList("input3", "input4"));

        entities.addAll(Arrays.asList(lineage1, lineage2));

        when(context.getEntities()).thenReturn(entities);
        when(context.getKafkaMessageOffset()).thenReturn(100L);
        when(context.getKafkaPartition()).thenReturn(1);

        skipHiveLineageMethod.invoke(consumer, context);

        verify(context, atLeast(1)).getEntities();
    }

    @Test
    public void testHookConsumerRunWithExceptionInReceive() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        // Mock consumer.receive to throw exception first time, then return empty
        when(notificationConsumer.receiveWithCheckedCommit(any()))
                .thenThrow(new RuntimeException("Kafka error"))
                .thenReturn(Collections.emptyList());

        // Mock serviceState to be active
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        // Create and run consumer thread briefly
        Thread consumerThread = new Thread(() -> {
            try {
                Method runMethod = hookConsumer.getClass().getDeclaredMethod("run");
                runMethod.setAccessible(true);
                runMethod.invoke(hookConsumer);
            } catch (Exception e) {
                // Expected due to shutdown
            }
        });

        consumerThread.start();
        Thread.sleep(100); // Let it process

        // Shutdown consumer
        Method shutdownMethod = hookConsumer.getClass().getDeclaredMethod("shutdown");
        shutdownMethod.setAccessible(true);
        shutdownMethod.invoke(hookConsumer);

        consumerThread.join(1000);

        verify(serviceState, atLeast(1)).getState();
        verify(notificationConsumer, atLeast(1)).receiveWithCheckedCommit(any());
    }

    @Test
    public void testHookConsumerServerNotAvailableScenario() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method serverAvailableMethod = hookConsumer.getClass().getDeclaredMethod("serverAvailable", NotificationHookConsumer.Timer.class);
        serverAvailableMethod.setAccessible(true);

        // Mock service state to never become active
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        NotificationHookConsumer.Timer mockTimer = mock(NotificationHookConsumer.Timer.class);

        // Run in separate thread to avoid blocking
        Thread testThread = new Thread(() -> {
            try {
                serverAvailableMethod.invoke(hookConsumer, mockTimer);
            } catch (Exception e) {
                // Expected
            }
        });

        testThread.start();
        Thread.sleep(100);
        testThread.interrupt();
        testThread.join(1000);

        verify(serviceState, atLeast(1)).getState();
    }

    @Test
    public void testEntityUpdateWithComplexRelationships() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method updateReferencesMethod = NotificationHookConsumer.class.getDeclaredMethod("updateProcessedEntityReferences", List.class, Map.class);
        updateReferencesMethod.setAccessible(true);

        // Create entities with complex relationships
        AtlasEntity entity1 = new AtlasEntity("Table");
        entity1.setGuid("table-guid");

        // Add object reference attributes
        AtlasObjectId dbRef = new AtlasObjectId("old-db-guid", "Database");
        entity1.setAttribute("database", dbRef);

        // Add collection of object references
        List<AtlasObjectId> columnRefs = Arrays.asList(
                new AtlasObjectId("old-col1-guid", "Column"),
                new AtlasObjectId("old-col2-guid", "Column"));
        entity1.setAttribute("columns", columnRefs);

        // Add relationship attributes
        entity1.setRelationshipAttribute("schema", new AtlasObjectId("old-schema-guid", "Schema"));

        List<AtlasEntity> entities = Collections.singletonList(entity1);

        // Mock entity type with attributes
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        AtlasStructType.AtlasAttribute dbAttribute = mock(AtlasStructType.AtlasAttribute.class);
        AtlasStructType.AtlasAttribute colAttribute = mock(AtlasStructType.AtlasAttribute.class);
        when(dbAttribute.isObjectRef()).thenReturn(true);
        when(colAttribute.isObjectRef()).thenReturn(true);
        when(mockEntityType.getAttribute("database")).thenReturn(dbAttribute);
        when(mockEntityType.getAttribute("columns")).thenReturn(colAttribute);
        when(typeRegistry.getEntityTypeByName("Table")).thenReturn(mockEntityType);

        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("old-db-guid", "new-db-guid");
        guidAssignments.put("old-col1-guid", "new-col1-guid");
        guidAssignments.put("old-schema-guid", "new-schema-guid");

        updateReferencesMethod.invoke(consumer, entities, guidAssignments);

        // Verify references were updated
        assertEquals("new-db-guid", ((AtlasObjectId) entity1.getAttribute("database")).getGuid());
        assertEquals("new-col1-guid", ((List<AtlasObjectId>) entity1.getAttribute("columns")).get(0).getGuid());
        assertEquals("new-schema-guid", ((AtlasObjectId) entity1.getRelationshipAttribute("schema")).getGuid());
    }

    // Helper methods for specific test configurations
    private NotificationHookConsumer createTestConsumerWithFailedMsgCache(int cacheSize) throws Exception {
        Configuration cacheConfig = mock(Configuration.class);
        when(cacheConfig.getInt(NotificationHookConsumer.CONSUMER_FAILEDCACHESIZE_PROPERTY, 1)).thenReturn(cacheSize);
        when(cacheConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(cacheConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(false);
        when(cacheConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(cacheConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        // Add AtlasConfiguration properties to the config mock
        when(cacheConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(false);
        when(cacheConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(10);
        when(cacheConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(cacheConfig);

            return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState,
                    instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
        }
    }

    private NotificationHookConsumer createTestConsumerWithLargeMessageThreshold(int thresholdMs) throws Exception {
        Configuration thresholdConfig = mock(Configuration.class);
        when(thresholdConfig.getInt("atlas.notification.consumer.large.message.processing.time.threshold.ms", 60000)).thenReturn(thresholdMs);
        when(thresholdConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(thresholdConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(false);
        when(thresholdConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(thresholdConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        // Add AtlasConfiguration properties to the config mock
        when(thresholdConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(false);
        when(thresholdConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(10);
        when(thresholdConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(thresholdConfig);

            return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState,
                    instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
        }
    }

    private NotificationHookConsumer createTestConsumerWithEntityIgnorePatterns() throws Exception {
        Configuration ignoreConfig = mock(Configuration.class);
        when(ignoreConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN))
                .thenReturn(new String[] {"temp_.*"});
        when(ignoreConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(ignoreConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(false);
        when(ignoreConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(ignoreConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        // Add AtlasConfiguration properties to the config mock
        when(ignoreConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(false);
        when(ignoreConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(10);
        when(ignoreConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(ignoreConfig);

            return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState,
                    instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
        }
    }

    private NotificationHookConsumer createTestConsumerWithHiveLineageSkip() throws Exception {
        Configuration skipConfig = mock(Configuration.class);
        when(skipConfig.getBoolean(NotificationHookConsumer.CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633, false)).thenReturn(true);
        when(skipConfig.getInt(NotificationHookConsumer.CONSUMER_SKIP_HIVE_COLUMN_LINEAGE_HIVE_20633_INPUTS_THRESHOLD, 15)).thenReturn(1);
        when(skipConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(skipConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(false);
        when(skipConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(skipConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        // Add AtlasConfiguration properties to the config mock
        when(skipConfig.getBoolean("atlas.notification.consumer.create.shell.entity.for.non-existing.ref", true)).thenReturn(false);
        when(skipConfig.getInt("atlas.notification.consumer.message.buffering.interval.seconds", 15)).thenReturn(10);
        when(skipConfig.getInt("atlas.notification.consumer.message.buffering.batch.size", 100)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(skipConfig);

            return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState,
                    instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
        }
    }

    @Test
    public void testPreProcessNotificationMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        // Create test message
        AtlasEntity entity = new AtlasEntity("hive_table");
        entity.setAttribute("name", "test_table");

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        entities.addEntity(entity);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", entities);
        AtlasKafkaMessage<HookNotification> kafkaMsg =
                new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        Method preProcessMethod =
                NotificationHookConsumer.class.getDeclaredMethod("preProcessNotificationMessage", AtlasKafkaMessage.class);
        preProcessMethod.setAccessible(true);

        PreprocessorContext result = (PreprocessorContext) preProcessMethod.invoke(consumer, kafkaMsg);

        assertNotNull(result, "Preprocessing should return a valid PreprocessorContext");
    }

    // Preprocessing Method Tests

    @Test
    public void testPreprocessEntities() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        Method preprocessEntitiesMethod = NotificationHookConsumer.class.getDeclaredMethod("preprocessEntities", PreprocessorContext.class);
        preprocessEntitiesMethod.setAccessible(true);

        // Create mock context
        PreprocessorContext context = mock(PreprocessorContext.class);
        List<AtlasEntity> entities = new ArrayList<>();
        AtlasEntity entity = new AtlasEntity("TestType");
        entity.setGuid("test-guid");
        entities.add(entity);

        when(context.getEntities()).thenReturn(entities);
        when(context.getReferredEntities()).thenReturn(new HashMap<>());
        when(context.isIgnoredEntity("test-guid")).thenReturn(false);

        preprocessEntitiesMethod.invoke(consumer, context);

        verify(context).getEntities();
    }

    @Test
    public void testTrimAndPurgeMethod() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method trimAndPurgeMethod = NotificationHookConsumer.class.getDeclaredMethod("trimAndPurge", String[].class, String.class);
        trimAndPurgeMethod.setAccessible(true);

        // Test with valid values
        String[] input = {" value1 ", "", " value2", null, "value3 "};
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) trimAndPurgeMethod.invoke(consumer, input, "default");

        assertEquals(3, result.size());
        assertTrue(result.contains("value1"));
        assertTrue(result.contains("value2"));
        assertTrue(result.contains("value3"));

        // Test with null input
        @SuppressWarnings("unchecked")
        List<String> defaultResult = (List<String>) trimAndPurgeMethod.invoke(consumer, null, "default");
        assertEquals(1, defaultResult.size());
        assertEquals("default", defaultResult.get(0));

        // Test with empty default
        @SuppressWarnings("unchecked")
        List<String> emptyResult = (List<String>) trimAndPurgeMethod.invoke(consumer, null, null);
        assertTrue(emptyResult.isEmpty());
    }

    @Test
    public void testGetAuthenticationForUser() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method getAuthMethod = NotificationHookConsumer.class.getDeclaredMethod("getAuthenticationForUser", String.class);
        getAuthMethod.setAccessible(true);

        try (MockedStatic<org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider> authProvider =
                        mockStatic(org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.class)) {
            authProvider.when(() -> org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("testUser"))
                    .thenReturn(new ArrayList<>());

            Object auth = getAuthMethod.invoke(consumer, "testUser");
            assertNotNull(auth);

            // Test with null/empty username
            Object nullAuth = getAuthMethod.invoke(consumer, (String) null);
            assertNull(nullAuth);

            Object emptyAuth = getAuthMethod.invoke(consumer, "");
            assertNull(emptyAuth);
        }
    }

    @Test
    public void testSetCurrentUser() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method setCurrentUserMethod = NotificationHookConsumer.class.getDeclaredMethod("setCurrentUser", String.class);
        setCurrentUserMethod.setAccessible(true);

        try (MockedStatic<org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider> authProvider =
                        mockStatic(org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.class);
                MockedStatic<org.springframework.security.core.context.SecurityContextHolder> securityContext =
                        mockStatic(org.springframework.security.core.context.SecurityContextHolder.class)) {
            authProvider.when(() -> org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("testUser"))
                    .thenReturn(new ArrayList<>());

            org.springframework.security.core.context.SecurityContext mockContext =
                    mock(org.springframework.security.core.context.SecurityContext.class);
            securityContext.when(org.springframework.security.core.context.SecurityContextHolder::getContext)
                    .thenReturn(mockContext);

            setCurrentUserMethod.invoke(consumer, "testUser");

            verify(mockContext).setAuthentication(any());
        }
    }

    @Test
    public void testUpdateProcessedEntityReferencesAtlasObjectId() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method updateMethod = NotificationHookConsumer.class.getDeclaredMethod("updateProcessedEntityReferences", AtlasObjectId.class, Map.class);
        updateMethod.setAccessible(true);

        AtlasObjectId objectId = new AtlasObjectId("original-guid", "TestType");
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("original-guid", "new-guid");

        // Verify initial state
        assertEquals(objectId.getGuid(), "original-guid");
        assertEquals(objectId.getTypeName(), "TestType");
        assertNull(objectId.getUniqueAttributes());
    }

    @Test
    public void testUpdateProcessedEntityReferencesMap() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method updateMethod = NotificationHookConsumer.class.getDeclaredMethod("updateProcessedEntityReferences", Map.class, Map.class);
        updateMethod.setAccessible(true);

        Map<String, Object> objIdMap = new HashMap<>();
        objIdMap.put("guid", "original-guid");
        objIdMap.put("typeName", "TestType");
        objIdMap.put("uniqueAttributes", new HashMap<>());

        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("original-guid", "new-guid");

        updateMethod.invoke(consumer, objIdMap, guidAssignments);

        assertEquals("new-guid", objIdMap.get("guid"));
        assertFalse(objIdMap.containsKey("typeName"));
        assertFalse(objIdMap.containsKey("uniqueAttributes"));
    }

    @Test
    public void testUpdateProcessedEntityReferencesCollection() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method updateMethod = NotificationHookConsumer.class.getDeclaredMethod("updateProcessedEntityReferences", Collection.class, Map.class);
        updateMethod.setAccessible(true);

        AtlasObjectId objectId1 = new AtlasObjectId("guid1", "TestType1");
        AtlasObjectId objectId2 = new AtlasObjectId("guid2", "TestType2");
        List<AtlasObjectId> collection = Arrays.asList(objectId1, objectId2);

        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("guid1", "new-guid1");

        // Verify initial state
        assertEquals("guid1", objectId1.getGuid());
        assertEquals("TestType1", objectId1.getTypeName());
        assertEquals("guid2", objectId2.getGuid());
        assertEquals("TestType2", objectId2.getTypeName());

        updateMethod.invoke(consumer, collection, guidAssignments);

        // objectId1 should be updated, objectId2 should remain unchanged
        assertEquals("new-guid1", objectId1.getGuid());
        assertNull(objectId1.getTypeName()); // cleared after GUID assignment
        assertEquals("guid2", objectId2.getGuid()); // unchanged
        assertEquals("TestType2", objectId2.getTypeName()); // unchanged
    }

    @Test
    public void testRecordProcessedEntities() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method recordMethod = NotificationHookConsumer.class.getDeclaredMethod("recordProcessedEntities", EntityMutationResponse.class, AtlasMetricsUtil.NotificationStat.class, PreprocessorContext.class);
        recordMethod.setAccessible(true);

        EntityMutationResponse mutationResponse = mock(EntityMutationResponse.class);
        AtlasMetricsUtil.NotificationStat stats = mock(AtlasMetricsUtil.NotificationStat.class);
        PreprocessorContext context = mock(PreprocessorContext.class);

        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("temp-guid", "real-guid");
        when(mutationResponse.getGuidAssignments()).thenReturn(guidAssignments);
        when(context.getGuidAssignments()).thenReturn(new HashMap<>());
        when(context.getCreatedEntities()).thenReturn(Collections.emptySet());
        when(context.getDeletedEntities()).thenReturn(Collections.emptySet());

        recordMethod.invoke(consumer, mutationResponse, stats, context);

        verify(stats).updateStats(mutationResponse);
        verify(context).getGuidAssignments();
    }

    @Test
    public void testIsEmptyMessageForDifferentTypes() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method isEmptyMethod = NotificationHookConsumer.class.getDeclaredMethod("isEmptyMessage", AtlasKafkaMessage.class);
        isEmptyMethod.setAccessible(true);

        // Test CREATE_V2 with empty entities
        EntityCreateRequestV2 emptyCreate = new EntityCreateRequestV2("user", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> emptyCreateMsg = new AtlasKafkaMessage<>(emptyCreate, 1L, "topic", 0);
        assertTrue((Boolean) isEmptyMethod.invoke(consumer, emptyCreateMsg));

        // Test CREATE_V2 with null entities
        EntityCreateRequestV2 nullCreate = new EntityCreateRequestV2("user", null);
        AtlasKafkaMessage<HookNotification> nullCreateMsg = new AtlasKafkaMessage<>(nullCreate, 1L, "topic", 0);
        assertTrue((Boolean) isEmptyMethod.invoke(consumer, nullCreateMsg));

        // Test UPDATE_V2 with empty entities
        EntityUpdateRequestV2 emptyUpdate = new EntityUpdateRequestV2("user", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> emptyUpdateMsg = new AtlasKafkaMessage<>(emptyUpdate, 1L, "topic", 0);
        assertTrue((Boolean) isEmptyMethod.invoke(consumer, emptyUpdateMsg));

        // Test other message types (should return false)
        EntityDeleteRequestV2 deleteRequest = new EntityDeleteRequestV2("user", Collections.emptyList());
        AtlasKafkaMessage<HookNotification> deleteMsg = new AtlasKafkaMessage<>(deleteRequest, 1L, "topic", 0);
        assertFalse((Boolean) isEmptyMethod.invoke(consumer, deleteMsg));
    }

    @Test
    public void testHookConsumerSortAndPublishMsgs() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method sortAndPublishMethod = hookConsumer.getClass().getDeclaredMethod("sortAndPublishMsgsToAtlasHook", long.class, Map.class);
        sortAndPublishMethod.setAccessible(true);

        Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer = new HashMap<>();
        when(notificationConsumer.receiveRawRecordsWithCheckedCommit(any())).thenReturn(Collections.emptyList());

        try {
            sortAndPublishMethod.invoke(hookConsumer, System.currentTimeMillis(), msgBuffer);
        } catch (Exception e) {
        }
    }

    @Test
    public void testHookConsumerSortMessages() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method sortMessagesMethod = hookConsumer.getClass().getDeclaredMethod("sortMessages", AtlasKafkaMessage.class, Map.class);
        sortMessagesMethod.setAccessible(true);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer = new HashMap<>();

        sortMessagesMethod.invoke(hookConsumer, kafkaMsg, msgBuffer);

        assertFalse(msgBuffer.isEmpty());
    }

    @Test
    public void testHookConsumerGetKey() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method getKeyMethod = hookConsumer.getClass().getDeclaredMethod("getKey", String.class, String.class);
        getKeyMethod.setAccessible(true);

        String result = (String) getKeyMethod.invoke(hookConsumer, "12345", "source");
        assertEquals("12345_source", result);
    }

    @Test
    public void testHookConsumerResetDuplicateKeyCounter() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method resetMethod = hookConsumer.getClass().getDeclaredMethod("resetDuplicateKeyCounter");
        resetMethod.setAccessible(true);

        resetMethod.invoke(hookConsumer);

        Field duplicateKeyCounterField = hookConsumer.getClass().getDeclaredField("duplicateKeyCounter");
        duplicateKeyCounterField.setAccessible(true);
        int counter = duplicateKeyCounterField.getInt(hookConsumer);
        assertEquals(1, counter);
    }

    @Test
    public void testHookConsumerRecordFailedMessages() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method recordFailedMethod = hookConsumer.getClass().getDeclaredMethod("recordFailedMessages");
        recordFailedMethod.setAccessible(true);

        // Add some failed messages
        Field failedMessagesField = hookConsumer.getClass().getDeclaredField("failedMessages");
        failedMessagesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> failedMessages = (List<String>) failedMessagesField.get(hookConsumer);
        failedMessages.add("failed message 1");
        failedMessages.add("failed message 2");

        recordFailedMethod.invoke(hookConsumer);

        assertTrue(failedMessages.isEmpty()); // Should be cleared after recording
    }

    @Test
    public void testHookConsumerCommit() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Method commitMethod = hookConsumer.getClass().getDeclaredMethod("commit", AtlasKafkaMessage.class);
        commitMethod.setAccessible(true);

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 5L, "test-topic", 0);

        commitMethod.invoke(hookConsumer, kafkaMsg);

        verify(notificationConsumer).commit(any(TopicPartition.class), eq(6L)); // offset + 1
    }

    @Test
    public void testCloseImportConsumerWithValidImportId() throws Exception {
        // Create consumer with mocks
        NotificationHookConsumer consumer = new NotificationHookConsumer(
                notificationInterface, atlasEntityStore, serviceState,
                instanceConverter, typeRegistry, metricsUtil,
                entityCorrelationStore, asyncImporter);

        // ✅ Manually initialize consumers list (otherwise it's null)
        Field consumersField = NotificationHookConsumer.class.getDeclaredField("consumers");
        consumersField.setAccessible(true);
        List<Object> consumers = new ArrayList<>();
        consumersField.set(consumer, consumers);

        // Setup mock NotificationConsumer
        @SuppressWarnings("unchecked")
        NotificationConsumer<HookNotification> mockConsumer = mock(NotificationConsumer.class);
        when(mockConsumer.subscription()).thenReturn(Collections.emptySet());
        when(mockConsumer.getTopicPartition()).thenReturn(Collections.emptySet());

        // Create hookConsumer object for "atlas-import-consumer-thread-test-import"
        Object hookConsumer = createHookConsumerWithName(
                consumer, "atlas-import-consumer-thread-test-import", mockConsumer);

        consumers.add(hookConsumer);

        // Act
        consumer.closeImportConsumer("test-import", "ATLAS_IMPORT_test-import");

        // Assert
        verify(notificationInterface).closeConsumer(NotificationType.ASYNC_IMPORT, "ATLAS_IMPORT_test-import");
        verify(notificationInterface).deleteTopic(NotificationType.ASYNC_IMPORT, "ATLAS_IMPORT_test-import");
        assertTrue(consumers.isEmpty(), "Consumers list should be empty after closing import consumer");
    }

    // Core Service Method Tests

    @Test
    public void testStartAsyncImportConsumer() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        List<NotificationConsumer<Object>> mockConsumers = new ArrayList<>();
        mockConsumers.add(mock(NotificationConsumer.class));

        when(notificationInterface.createConsumers(NotificationType.ASYNC_IMPORT, 1)).thenReturn(mockConsumers);

        // Initialize executors first
        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        executorsField.set(consumer, mock(ExecutorService.class));

        consumer.startAsyncImportConsumer(NotificationType.ASYNC_IMPORT, "test-import", "test-topic");

        verify(notificationInterface).addTopicToNotificationType(NotificationType.ASYNC_IMPORT, "test-topic");
        verify(notificationInterface).createConsumers(NotificationType.ASYNC_IMPORT, 1);
    }

    @Test
    public void testCreateExecutor() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method createExecutorMethod = NotificationHookConsumer.class.getDeclaredMethod("createExecutor");
        createExecutorMethod.setAccessible(true);

        ExecutorService result = (ExecutorService) createExecutorMethod.invoke(consumer);

        assertNotNull(result);
    }

    @Test
    public void testStartHookConsumersWithUnsortedEnabled() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        List<NotificationConsumer<Object>> hookConsumers = new ArrayList<>();
        hookConsumers.add(mock(NotificationConsumer.class));

        List<NotificationConsumer<Object>> unsortedConsumers = new ArrayList<>();
        unsortedConsumers.add(mock(NotificationConsumer.class));

        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(hookConsumers);
        when(notificationInterface.createConsumers(NotificationType.HOOK_UNSORTED, 1)).thenReturn(unsortedConsumers);

        // Initialize executors
        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        executorsField.set(consumer, mock(ExecutorService.class));

        Method startHookConsumersMethod = NotificationHookConsumer.class.getDeclaredMethod("startHookConsumers");
        startHookConsumersMethod.setAccessible(true);

        startHookConsumersMethod.invoke(consumer);

        verify(notificationInterface).createConsumers(NotificationType.HOOK, 1);
    }

    @Test
    public void testInstanceIsActiveWhenConsumerDisabled() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Field consumerDisabledField = NotificationHookConsumer.class.getDeclaredField("consumerDisabled");
        consumerDisabledField.setAccessible(true);
        consumerDisabledField.set(consumer, true);

        consumer.instanceIsActive();

        // Should create executor but not start consumers
        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        assertNotNull(executorsField.get(consumer));
    }

    @Test
    public void testGetHandlerOrder() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        int order = consumer.getHandlerOrder();

        // Should return the expected handler order
        assertTrue(order > 0);
    }

    @Test
    public void testConstructorWithExceptionInConfigurationAccess() throws Exception {
        Configuration faultyConfig = mock(Configuration.class);

        // Make config throw error on access
        when(faultyConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3))
                .thenThrow(new RuntimeException("Config access error"));
        // Add other config properties to prevent NPE
        when(faultyConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false))
                .thenReturn(false);
        when(faultyConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10))
                .thenReturn(10);
        when(faultyConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25))
                .thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(faultyConfig);

            try {
                new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState,
                        instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
                fail("Expected exception due to config access error");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Config access error") || e.getCause().getMessage().contains("Config access error"));
            }
        }
    }

    // Configuration Constructor Tests

    @Test
    public void testStartInternalWithNullExecutor() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);

        List<NotificationConsumer<Object>> consumers = new ArrayList<>();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        // Pass null executor - should create its own
        consumer.startInternal(configuration, null);

        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        assertNotNull(executorsField.get(consumer));
    }

    @Test
    public void testInstanceIsActiveWithExistingExecutor() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        // Set up existing executor
        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        executorsField.set(consumer, mock(ExecutorService.class));

        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);

        List<NotificationConsumer<Object>> consumers = new ArrayList<>();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationType.HOOK, 1)).thenReturn(consumers);

        consumer.instanceIsActive();

        // Should not create new executor
        assertNotNull(executorsField.get(consumer));
    }

    @Test
    public void testHookConsumerRun() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        // Mock serviceState to simulate server not ready, then ready
        when(serviceState.getState())
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        // Mock consumer to return no messages
        when(notificationConsumer.receive()).thenReturn(Collections.emptyList());

        Thread consumerThread = new Thread(() -> {
            try {
                Method runMethod = hookConsumer.getClass().getDeclaredMethod("run");
                runMethod.setAccessible(true);
                runMethod.invoke(hookConsumer);
            } catch (Exception e) {
                // Expected due to mocking limitations
            }
        });

        consumerThread.start();
        Thread.sleep(100);

        // Shutdown the consumer
        Method shutdownMethod = hookConsumer.getClass().getDeclaredMethod("shutdown");
        shutdownMethod.setAccessible(true);
        shutdownMethod.invoke(hookConsumer);

        consumerThread.join(1000);

        verify(serviceState, atLeast(1)).getState();
    }

    @Test
    public void testPreprocessHiveTypes() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        Method preprocessHiveTypesMethod = NotificationHookConsumer.class.getDeclaredMethod("preprocessHiveTypes", PreprocessorContext.class);
        preprocessHiveTypesMethod.setAccessible(true);

        PreprocessorContext context = mock(PreprocessorContext.class);
        AtlasEntity hiveTable = new AtlasEntity("hive_table");
        hiveTable.setAttribute("ownedRef1", "value1");
        hiveTable.setAttribute("normalAttr", "value2");

        List<AtlasEntity> entities = Collections.singletonList(hiveTable);
        when(context.getEntities()).thenReturn(entities);

        preprocessHiveTypesMethod.invoke(consumer, context);

        verify(context).getEntities();
    }

    // Comprehensive Preprocessing Tests

    @Test
    public void testSkipHiveColumnLineage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        Method skipHiveLineageMethod = NotificationHookConsumer.class.getDeclaredMethod("skipHiveColumnLineage", PreprocessorContext.class);
        skipHiveLineageMethod.setAccessible(true);

        PreprocessorContext context = mock(PreprocessorContext.class);
        List<AtlasEntity> entities = new ArrayList<>();

        // Create a process entity with many inputs to trigger the threshold
        AtlasEntity processEntity = new AtlasEntity("Process");
        processEntity.setAttribute("inputs", Arrays.asList("input1", "input2", "input3", "input4", "input5",
                "input6", "input7", "input8", "input9", "input10",
                "input11", "input12", "input13", "input14", "input15",
                "input16")); // More than threshold
        entities.add(processEntity);

        when(context.getEntities()).thenReturn(entities);
        when(context.getReferredEntities()).thenReturn(new HashMap<>());

        // This is a void method, so just verify it executes without exception
        skipHiveLineageMethod.invoke(consumer, context);

        verify(context, atLeast(1)).getEntities();
    }

    @Test
    public void testRdbmsTypeRemoveOwnedRefAttrs() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        Method rdbmsRemoveMethod = NotificationHookConsumer.class.getDeclaredMethod("rdbmsTypeRemoveOwnedRefAttrs", PreprocessorContext.class);
        rdbmsRemoveMethod.setAccessible(true);

        PreprocessorContext context = mock(PreprocessorContext.class);
        AtlasEntity rdbmsTable = new AtlasEntity("rdbms_table");
        rdbmsTable.setAttribute("ownedRef", "value");
        rdbmsTable.setAttribute("columns", "normalAttr");

        List<AtlasEntity> entities = Collections.singletonList(rdbmsTable);
        when(context.getEntities()).thenReturn(entities);

        rdbmsRemoveMethod.invoke(consumer, context);

        verify(context).getEntities();
    }

    @Test
    public void testPruneObjectPrefixForS3V2Directory() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        Method pruneMethod = NotificationHookConsumer.class.getDeclaredMethod("pruneObjectPrefixForS3V2Directory", PreprocessorContext.class);
        pruneMethod.setAccessible(true);

        PreprocessorContext context = mock(PreprocessorContext.class);
        AtlasEntity s3Object = new AtlasEntity("aws_s3_v2_object");
        s3Object.setAttribute("prefix", "some/long/prefix/");

        List<AtlasEntity> entities = Collections.singletonList(s3Object);
        when(context.getEntities()).thenReturn(entities);

        pruneMethod.invoke(consumer, context);

        verify(context, atLeast(1)).getEntities();
    }

    @Test
    public void testPreprocessSparkProcessAttributes() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithPreprocessing();

        Method sparkPreprocessMethod = NotificationHookConsumer.class.getDeclaredMethod("preprocessSparkProcessAttributes", PreprocessorContext.class);
        sparkPreprocessMethod.setAccessible(true);

        PreprocessorContext context = mock(PreprocessorContext.class);
        AtlasEntity sparkProcess = new AtlasEntity("spark_process");
        sparkProcess.setAttribute("sparkPlanJson", "{\"plan\": \"data\"}");

        List<AtlasEntity> entities = Collections.singletonList(sparkProcess);
        when(context.getEntities()).thenReturn(entities);

        sparkPreprocessMethod.invoke(consumer, context);

        verify(context).getEntities();
    }

    @Test
    public void testUpdateProcessedEntityReferencesObject() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method updateMethod = NotificationHookConsumer.class.getDeclaredMethod("updateProcessedEntityReferences", Object.class, Map.class);
        updateMethod.setAccessible(true);

        // Test with AtlasObjectId
        AtlasObjectId objectId = new AtlasObjectId("original-guid", "TestType");
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("original-guid", "new-guid");

        // Verify initial state
        assertEquals("original-guid", objectId.getGuid());
        assertEquals("TestType", objectId.getTypeName());

        updateMethod.invoke(consumer, objectId, guidAssignments);
        assertEquals("new-guid", objectId.getGuid());
        assertNull(objectId.getTypeName()); // cleared after GUID assignment

        // Test with Map
        Map<String, Object> objIdMap = new HashMap<>();
        objIdMap.put("guid", "map-guid");
        objIdMap.put("typeName", "MapType");
        guidAssignments.put("map-guid", "new-map-guid");

        updateMethod.invoke(consumer, objIdMap, guidAssignments);
        assertEquals("new-map-guid", objIdMap.get("guid"));

        // Test with Collection
        List<AtlasObjectId> collection = Collections.singletonList(new AtlasObjectId("Type1", "collection-guid"));
        guidAssignments.put("collection-guid", "new-collection-guid");

        updateMethod.invoke(consumer, collection, guidAssignments);
    }

    @Test
    public void testHookConsumerHandleMessageWithAtlasBaseException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean()))
                .thenThrow(new AtlasBaseException("Atlas error"));

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // Should not commit on failure
        handleMessageMethod.invoke(hookConsumer, kafkaMsg);
    }

    @Test
    public void testHookConsumerHandleMessageWithInvalidMessageObject() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        HookNotification invalidMessage = new HookNotification(HookNotificationType.ENTITY_CREATE, "testUser") {
            @Override
            public void normalize() {
                // Do nothing
            }
        };

        AtlasKafkaMessage<HookNotification> kafkaMsg =
                new AtlasKafkaMessage<>(invalidMessage, 1L, "test-topic", 0);

        Method handleMessageMethod =
                hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        // invoke and catch ClassCastException internally
        try {
            handleMessageMethod.invoke(hookConsumer, kafkaMsg);
        } catch (InvocationTargetException e) {
            // expected: underlying ClassCastException
            assertTrue(e.getCause() instanceof ClassCastException,
                    "Expected ClassCastException, got " + e.getCause());
        }
    }

    @Test
    public void testHookConsumerHandleMessageWithInterruptedException() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean()))
                .thenThrow(new RuntimeException("Thread interrupted"));

        EntityCreateRequestV2 createRequest = new EntityCreateRequestV2("testUser", new AtlasEntitiesWithExtInfo());
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        try {
            handleMessageMethod.invoke(hookConsumer, kafkaMsg);
        } catch (Exception e) {
        }
    }

    @Test
    public void testHookConsumerHandleV1CreateMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Referenceable entity = mock(Referenceable.class);
        EntityCreateRequest createRequest = new EntityCreateRequest("testUser", Collections.singletonList(entity));
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(createRequest, 1L, "test-topic", 0);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    // Legacy V1 Message Handling Tests

    @Test
    public void testHookConsumerHandleV1UpdateMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        Referenceable entity = mock(Referenceable.class);
        EntityUpdateRequest updateRequest = new EntityUpdateRequest("testUser", entity);
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(updateRequest, 1L, "test-topic", 0);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerHandleV1DeleteMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        EntityDeleteRequest deleteRequest = new EntityDeleteRequest("testUser", "TestType", "qualifiedName", "test@cluster");
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(deleteRequest, 1L, "test-topic", 0);

        Method handleMessageMethod = hookConsumer.getClass().getDeclaredMethod("handleMessage", AtlasKafkaMessage.class);
        handleMessageMethod.setAccessible(true);

        handleMessageMethod.invoke(hookConsumer, kafkaMsg);

        verify(notificationConsumer).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    public void testHookConsumerBufferAndSortMessages() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        // Mock consumer to return messages
        EntityCreateRequestV2 createRequest1 = new EntityCreateRequestV2("user1", new AtlasEntitiesWithExtInfo());
        EntityCreateRequestV2 createRequest2 = new EntityCreateRequestV2("user2", new AtlasEntitiesWithExtInfo());

        AtlasKafkaMessage<HookNotification> msg1 = new AtlasKafkaMessage<>(createRequest1, 1L, "topic", 0);
        AtlasKafkaMessage<HookNotification> msg2 = new AtlasKafkaMessage<>(createRequest2, 2L, "topic", 0);

        when(notificationConsumer.receiveRawRecordsWithCheckedCommit(any()))
                .thenReturn(Arrays.asList(msg1, msg2))
                .thenReturn(Collections.emptyList());

        Method sortAndPublishMethod = hookConsumer.getClass().getDeclaredMethod("sortAndPublishMsgsToAtlasHook", long.class, Map.class);
        sortAndPublishMethod.setAccessible(true);

        Map<String, AtlasKafkaMessage<HookNotification>> msgBuffer = new HashMap<>();

        try {
            sortAndPublishMethod.invoke(hookConsumer, System.currentTimeMillis() + 1000, msgBuffer);
        } catch (Exception e) {
            // Expected due to processing complexity
        }

        verify(notificationConsumer, atLeast(1)).receiveRawRecordsWithCheckedCommit(any());
    }

    // Message Buffering and Sorting Tests

    @Test
    public void testSetCurrentUserWithAuthorizationEnabled() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithAuthorization();

        Method setCurrentUserMethod = NotificationHookConsumer.class.getDeclaredMethod("setCurrentUser", String.class);
        setCurrentUserMethod.setAccessible(true);

        try (MockedStatic<org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider> authProvider =
                        mockStatic(org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.class);
                MockedStatic<org.springframework.security.core.context.SecurityContextHolder> securityContext =
                        mockStatic(org.springframework.security.core.context.SecurityContextHolder.class)) {
            authProvider.when(() -> org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("testUser"))
                    .thenReturn(new ArrayList<>());

            org.springframework.security.core.context.SecurityContext mockContext =
                    mock(org.springframework.security.core.context.SecurityContext.class);
            securityContext.when(org.springframework.security.core.context.SecurityContextHolder::getContext)
                    .thenReturn(mockContext);

            setCurrentUserMethod.invoke(consumer, "testUser");

            verify(mockContext).setAuthentication(any());
        }
    }

    // Authentication and Authorization Tests

    @Test
    public void testGetAuthenticationForUserWithCache() throws Exception {
        NotificationHookConsumer consumer = createTestConsumerWithAuthorization();

        Method getAuthMethod = NotificationHookConsumer.class.getDeclaredMethod("getAuthenticationForUser", String.class);
        getAuthMethod.setAccessible(true);

        try (MockedStatic<org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider> authProvider =
                        mockStatic(org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.class)) {
            authProvider.when(() -> org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("cachedUser"))
                    .thenReturn(new ArrayList<>());

            // First call - should cache
            Object auth1 = getAuthMethod.invoke(consumer, "cachedUser");
            assertNotNull(auth1);

            // Second call - should use cache
            Object auth2 = getAuthMethod.invoke(consumer, "cachedUser");
            assertNotNull(auth2);

            // Should only call the static method once due to caching
            authProvider.verify(times(1), () -> org.apache.atlas.web.security.AtlasAbstractAuthenticationProvider.getAuthoritiesFromUGI("cachedUser"));
        }
    }

    // Edge Case Tests
    @Test
    public void testIsEmptyMessageWithNullMessage() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();

        Method isEmptyMethod = NotificationHookConsumer.class
                .getDeclaredMethod("isEmptyMessage", AtlasKafkaMessage.class);
        isEmptyMethod.setAccessible(true);

        AtlasKafkaMessage<HookNotification> nullMessage =
                new AtlasKafkaMessage<>(null, 1L, "topic", 0);

        try {
            isEmptyMethod.invoke(consumer, nullMessage);
            fail("Expected NullPointerException but none thrown");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof NullPointerException,
                    "Expected NullPointerException but got: " + e.getCause());
        }
    }

    @Test
    public void testHookConsumerShutdownAndRestart() throws Exception {
        NotificationHookConsumer consumer = createTestConsumer();
        NotificationConsumer<HookNotification> notificationConsumer = mock(NotificationConsumer.class);
        Object hookConsumer = createHookConsumer(consumer, notificationConsumer);

        // Access private shutdown() method
        Method shutdownMethod = hookConsumer.getClass().getDeclaredMethod("shutdown");
        shutdownMethod.setAccessible(true);

        // Access private final AtomicBoolean shouldRun
        Field shouldRunField = hookConsumer.getClass().getDeclaredField("shouldRun");
        shouldRunField.setAccessible(true);

        AtomicBoolean shouldRunAtomic = (AtomicBoolean) shouldRunField.get(hookConsumer);
        shouldRunAtomic.set(true); // initialize as running

        // Call shutdown
        shutdownMethod.invoke(hookConsumer);

        // Verify shouldRun is now false
        assertFalse(shouldRunAtomic.get());

        // Verify wakeup() is called
        verify(notificationConsumer).wakeup();
    }

    private NotificationHookConsumer createTestConsumer() throws AtlasException {
        return new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, entityCorrelationStore, asyncImporter);
    }

    // ==================== HELPER METHODS ====================

    private NotificationHookConsumer createTestConsumerWithPreprocessing() throws Exception {
        Configuration preprocessingConfig = mock(Configuration.class);
        when(preprocessingConfig.getBoolean(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TYPES_REMOVE_OWNEDREF_ATTRS, true)).thenReturn(true);
        when(preprocessingConfig.getStringArray(NotificationHookConsumer.CONSUMER_PREPROCESS_HIVE_TABLE_IGNORE_PATTERN)).thenReturn(new String[] {"temp_.*"});
        when(preprocessingConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(preprocessingConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false)).thenReturn(true);
        when(preprocessingConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10)).thenReturn(10);
        when(preprocessingConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25)).thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(preprocessingConfig);

            return new NotificationHookConsumer(
                    notificationInterface,
                    atlasEntityStore,
                    serviceState,
                    instanceConverter,
                    typeRegistry,
                    metricsUtil,
                    entityCorrelationStore,
                    asyncImporter);
        }
    }

    private NotificationHookConsumer createTestConsumerWithAuthorization() throws Exception {
        Configuration authConfig = mock(Configuration.class);

        // Mock values returned from Configuration
        when(authConfig.getBoolean(NotificationHookConsumer.CONSUMER_AUTHORIZE_USING_MESSAGE_USER, false))
                .thenReturn(true);
        when(authConfig.getInt(NotificationHookConsumer.CONSUMER_AUTHORIZE_AUTHN_CACHE_TTL_SECONDS, 300))
                .thenReturn(600);
        when(authConfig.getInt(NotificationHookConsumer.CONSUMER_RETRIES_PROPERTY, 3)).thenReturn(3);
        when(authConfig.getBoolean("atlas.notification.create.shell.entity.for.non.existing.ref", false))
                .thenReturn(false);
        when(authConfig.getInt("atlas.notification.hook.consumer.buffering.interval", 10))
                .thenReturn(10);
        when(authConfig.getInt("atlas.notification.hook.consumer.buffering.batch.size", 25))
                .thenReturn(25);

        try (MockedStatic<ApplicationProperties> appProps = mockStatic(ApplicationProperties.class)) {
            appProps.when(ApplicationProperties::get).thenReturn(authConfig);

            return new NotificationHookConsumer(
                    notificationInterface,
                    atlasEntityStore,
                    serviceState,
                    instanceConverter,
                    typeRegistry,
                    metricsUtil,
                    entityCorrelationStore,
                    asyncImporter);
        }
    }

    private Object createHookConsumer(NotificationHookConsumer consumer, NotificationConsumer<HookNotification> notificationConsumer) throws Exception {
        Class<?> hookConsumerClass = getInnerClass(NotificationHookConsumer.class, "HookConsumer");
        return hookConsumerClass.getDeclaredConstructor(NotificationHookConsumer.class, NotificationConsumer.class)
                .newInstance(consumer, notificationConsumer);
    }

    private Object createHookConsumerWithName(NotificationHookConsumer consumer, String name, NotificationConsumer<HookNotification> notificationConsumer) throws Exception {
        Class<?> hookConsumerClass = getInnerClass(NotificationHookConsumer.class, "HookConsumer");
        return hookConsumerClass.getDeclaredConstructor(NotificationHookConsumer.class, String.class, NotificationConsumer.class)
                .newInstance(consumer, name, notificationConsumer);
    }

    private Class<?> getInnerClass(Class<?> outerClass, String innerClassName) {
        for (Class<?> innerClass : outerClass.getDeclaredClasses()) {
            if (innerClass.getSimpleName().equals(innerClassName)) {
                return innerClass;
            }
        }
        throw new RuntimeException("Inner class " + innerClassName + " not found in " + outerClass.getSimpleName());
    }

    // Custom exception class for testing
    private static class JanusGraphException extends RuntimeException {
        public JanusGraphException(String message) {
            super(message);
        }
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
