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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.MessageSource;
import org.apache.atlas.notification.preprocessor.NotificationPreProcessor;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasTypeDefGraphStore;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class NotificationHookParallelConsumerTest {
    private static final String QUALIFIED_NAME = "qualifiedName";
    private static final String NAME           = "name";
    private static final String DESCRIPTION    = "description";

    private static final Logger FAILED_LOG = LoggerFactory.getLogger("FAILED");

    @SuppressWarnings("unchecked")
    private static NotificationConsumer<HookNotification> mockHookNotificationConsumer() {
        return (NotificationConsumer<HookNotification>) mock(NotificationConsumer.class);
    }

    private static void mockCreateConsumers(
            NotificationInterface notificationInterface,
            NotificationInterface.NotificationType notificationType,
            int numConsumers,
            List<NotificationConsumer<HookNotification>> consumers) {
        doReturn(consumers).when(notificationInterface).createConsumers(notificationType, numConsumers);
    }

    @FunctionalInterface
    private interface Throwing {
        void run() throws Exception;
    }

    /**
     * {@link NotificationPreProcessor} resolves {@link NotificationProvider#get()} in its constructor;
     * stub before {@link NotificationHookConsumer#getPreprocessorHookConsumers()}.
     */
    private void withMockedNotificationProvider(Throwing runnable) throws Exception {
        try (MockedStatic<NotificationProvider> np = mockStatic(NotificationProvider.class)) {
            np.when(NotificationProvider::get).thenReturn(notificationInterface);
            doNothing().when(notificationInterface).send(anyString(), anyList(), any(MessageSource.class), anyLong());
            runnable.run();
        }
    }

    @Mock
    private NotificationInterface notificationInterface;

    @Mock
    private AtlasEntityStore atlasEntityStore;

    @Mock
    private ServiceState serviceState;

    @Mock
    private AtlasInstanceConverter instanceConverter;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private AtlasTypeDefGraphStore typeDefStore;

    @Mock
    private AtlasMetricsUtil metricsUtil;

    @Mock
    private AsyncImporter asyncImporter;

    @Mock
    private NotificationInterface notificationInterfaceForPreProcessor;

    private AutoCloseable mocks;

    @BeforeMethod
    public void setup() throws AtlasBaseException {
        mocks = MockitoAnnotations.openMocks(this);

        AtlasType mockType = mock(AtlasType.class);
        AtlasEntitiesWithExtInfo mockEntity = new AtlasEntitiesWithExtInfo(mock(AtlasEntity.class));

        when(typeRegistry.getType(anyString())).thenReturn(mockType);
        when(instanceConverter.toAtlasEntities(anyList())).thenReturn(mockEntity);

        EntityMutationResponse mutationResponse = mock(EntityMutationResponse.class);
        when(atlasEntityStore.createOrUpdate(any(EntityStream.class), anyBoolean())).thenReturn(mutationResponse);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        doNothing().when(metricsUtil).onNotificationProcessorComplete(anyString(), anyInt(), anyLong(), any());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        resetParallelProcessingEnabled();
        if (mocks != null) {
            mocks.close();
        }
    }

    private void mockParallelProcessingEnabled(boolean enabled) throws AtlasException {
        setParallelProcessingEnabled(enabled);
    }

    private HookNotificationV1.EntityCreateRequest createTestMessage() {
        Referenceable entity = new Referenceable("test_type");
        entity.set(NAME, "testEntity");
        entity.set(DESCRIPTION, "Test entity for preprocessing");
        entity.set(QUALIFIED_NAME, "test.entity@cluster");
        return new HookNotificationV1.EntityCreateRequest("test_user", Collections.singletonList(entity));
    }

    @Test
    public void testQualifiedNameRouterSameRoutingKeyMapsToSameTopic() {
        QualifiedNameRouter router = new QualifiedNameRouter(5, "ATLAS_METADATA_");
        String t1 = router.getTargetTopic("db1.schema1");
        String t2 = router.getTargetTopic("db1.schema1");
        assertEquals(t1, t2);
    }

    @Test
    public void testQualifiedNameRouterDifferentKeysMayMapToDifferentTopics() {
        QualifiedNameRouter router = new QualifiedNameRouter(5, "ATLAS_METADATA_");
        String t1 = router.getTargetTopic("db1.schema1");
        String t2 = router.getTargetTopic("db9.schema9");
        assertNotEquals(t1, t2);
    }

    @Test
    public void testQualifiedNameRouterBlankKeyUsesFallbackPrefix() {
        QualifiedNameRouter router = new QualifiedNameRouter(2, "ATLAS_METADATA_");
        String t1 = router.getTargetTopic(null);
        assertTrue(t1.startsWith("ATLAS_METADATA_"));
    }

    @Test
    public void testNotificationPreProcessorRoutesCreateV2ToMetadataTopic() throws Exception {
        try (MockedStatic<NotificationProvider> np = mockStatic(NotificationProvider.class)) {
            np.when(NotificationProvider::get).thenReturn(notificationInterfaceForPreProcessor);
            doNothing().when(notificationInterfaceForPreProcessor).send(anyString(), anyList(), any(MessageSource.class), anyLong());

            Configuration config = defaultPreprocessorConfiguration();
            AtlasEntity entity = hiveTableEntity("db1.table1@cl1");
            EntityCreateRequestV2 request = entityCreateRequestV2("u1", entity);

            NotificationPreProcessor processor = new NotificationPreProcessor(config, metricsUtil, null, FAILED_LOG);
            AtlasKafkaMessage<HookNotification> kafkaMsg = preprocessKafkaMessage(request);

            assertNotNull(processor.handleMessage(kafkaMsg));

            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            verify(notificationInterfaceForPreProcessor).send(topicCaptor.capture(), anyList(), any(MessageSource.class), anyLong());
            assertTrue(topicCaptor.getValue().startsWith("ATLAS_METADATA_"));
        }
    }

    @Test
    public void testNotificationPreProcessorDistinctRoutingKeysTwoSends() throws Exception {
        try (MockedStatic<NotificationProvider> np = mockStatic(NotificationProvider.class)) {
            np.when(NotificationProvider::get).thenReturn(notificationInterfaceForPreProcessor);
            doNothing().when(notificationInterfaceForPreProcessor).send(anyString(), anyList(), any(MessageSource.class), anyLong());

            Configuration config = defaultPreprocessorConfiguration();
            AtlasEntity e1 = hiveTableEntity("alpha.beta1@cl");
            AtlasEntity e2 = hiveTableEntity("gamma.delta1@cl");
            EntityCreateRequestV2 request = entityCreateRequestV2("user", e1, e2);

            NotificationPreProcessor processor = new NotificationPreProcessor(config, metricsUtil, null, FAILED_LOG);
            processor.handleMessage(preprocessKafkaMessage(request));

            verify(notificationInterfaceForPreProcessor, times(2)).send(anyString(), anyList(), any(MessageSource.class), anyLong());
        }
    }

    @Test
    public void testNotificationPreProcessorCollectResultsNull() throws Exception {
        try (MockedStatic<NotificationProvider> np = mockStatic(NotificationProvider.class)) {
            np.when(NotificationProvider::get).thenReturn(notificationInterfaceForPreProcessor);
            doNothing().when(notificationInterfaceForPreProcessor).send(anyString(), anyList(), any(MessageSource.class), anyLong());
            NotificationPreProcessor processor = new NotificationPreProcessor(
                    defaultPreprocessorConfiguration(), metricsUtil, null, FAILED_LOG);
            assertNull(processor.collectResults());
        }
    }

    @Test
    public void testPreprocessorConsumersCreatedWhenParallelEnabledViaApplicationProperties() throws Exception {
        withMockedNotificationProvider(() -> {
            setParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(
                    notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil,
                    null, asyncImporter, typeDefStore);

            List<NotificationHookConsumer.HookConsumer> preprocessorConsumers = notificationHookConsumer.getPreprocessorHookConsumers();

            assertNotNull(preprocessorConsumers);
            assertEquals(preprocessorConsumers.size(), 1);
            verify(notificationInterface).createConsumers(NotificationInterface.NotificationType.HOOK_PREPROCESS, 1);
            verify(typeDefStore).registerTypeDefChangeListener(any(NotificationPreProcessor.class));
        });
    }

    @Test
    public void testStartHookConsumersDoesNotCreatePreprocessWhenParallelDisabledViaApplicationProperties() throws Exception {
        setParallelProcessingEnabled(false);

        NotificationConsumer<HookNotification> mockHookConsumer = mockHookNotificationConsumer();
        mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK, 1,
                Collections.singletonList(mockHookConsumer));

        NotificationHookConsumer consumer = new NotificationHookConsumer(
                notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil,
                null, asyncImporter, typeDefStore);

        Field executorsField = NotificationHookConsumer.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        executorsField.set(consumer, mock(ExecutorService.class));

        Method startHookConsumers = NotificationHookConsumer.class.getDeclaredMethod("startHookConsumers");
        startHookConsumers.setAccessible(true);
        startHookConsumers.invoke(consumer);

        verify(notificationInterface, never()).createConsumers(NotificationInterface.NotificationType.HOOK_PREPROCESS, 1);
    }

    @Test
    public void testPreprocessorSkipsTypeDefRegistrationWhenTypeDefStoreNull() throws Exception {
        withMockedNotificationProvider(() -> {
            setParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(
                    notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil,
                    null, asyncImporter, null);

            notificationHookConsumer.getPreprocessorHookConsumers();

            verify(typeDefStore, never()).registerTypeDefChangeListener(any());
        });
    }

    @Test
    public void testRoutingKeyFromFullQualifiedName_matrix_likePreprocessor() {
        String[][] cases = {
                {"db.table@cluster", "db.table"},
                {"db.table.column@cluster", "db.table"},
                {"db.table.column.nested@cluster", "db.table"},
                {"singlelevel@cluster", "singlelevel"},
                {"db.table:timestamp@cluster", "db.table"},
        };
        for (String[] c : cases) {
            assertEquals(routingKeyFromFullQualifiedNameByPreprocessor(c[0]), c[1], c[0]);
        }
    }

    @Test
    public void testRoutingKey_tableAndColumnEntities_shareSameRoutingKey() {
        String tableKey = routingKeyFromFullQualifiedNameByPreprocessor("db1.table1@cluster");
        String colKey   = routingKeyFromFullQualifiedNameByPreprocessor("db1.table1.col1@cluster");
        assertEquals(tableKey, "db1.table1");
        assertEquals(colKey, tableKey);
    }

    @Test
    public void testRoutingKey_differentDatabases_distinctKeys() {
        String k1 = routingKeyFromFullQualifiedNameByPreprocessor("db1.table1@cluster");
        String k2 = routingKeyFromFullQualifiedNameByPreprocessor("db2.table2@cluster");
        assertNotEquals(k1, k2);
    }

    @Test
    public void testQualifiedNameRouter_samePreprocessorRoutingKey_mapsToStableTopic() {
        QualifiedNameRouter router = new QualifiedNameRouter(5, "ATLAS_METADATA_");
        String key = routingKeyFromFullQualifiedNameByPreprocessor("db1.table1@cluster");
        assertEquals(router.getTargetTopic(key), router.getTargetTopic(key));
        assertTrue(router.getTargetTopic(key).startsWith("ATLAS_METADATA_"));
    }

    @Test
    public void testRoutingKey_threePartQualifiedName_usesFirstTwoDotSegments() {
        assertEquals(
                routingKeyFromFullQualifiedNameByPreprocessor("namespace.db.table@cluster"),
                "namespace.db");
    }

    @Test
    public void testRoutingKey_blankQualifiedName_noRoutingKey() {
        assertNull(routingKeyFromFullQualifiedNameByPreprocessor(""));
        assertNull(routingKeyFromFullQualifiedNameByPreprocessor(null));
    }

    @Test
    public void testRoutingKey_specialCharactersAndRouterTopics() {
        QualifiedNameRouter router = new QualifiedNameRouter(5, "ATLAS_METADATA_");
        String kHyphen = routingKeyFromFullQualifiedNameByPreprocessor("db-name.table-name@cluster");
        String kNs     = routingKeyFromFullQualifiedNameByPreprocessor("namespace.db.table@cluster");
        assertEquals(kHyphen, "db-name.table-name");
        assertEquals(kNs, "namespace.db");
        assertTrue(router.getTargetTopic(kHyphen).startsWith("ATLAS_METADATA_"));
        assertTrue(router.getTargetTopic(kNs).startsWith("ATLAS_METADATA_"));
    }

    @Test
    public void testPreprocessorConsumersAreCreatedWhenParallelProcessingEnabled() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationConsumer<HookNotification> mockHookConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK, 1,
                    Collections.singletonList(mockHookConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            List<NotificationHookConsumer.HookConsumer> preprocessorConsumers = notificationHookConsumer.getPreprocessorHookConsumers();

            assertNotNull(preprocessorConsumers);
            assertFalse(preprocessorConsumers.isEmpty());
            assertEquals(preprocessorConsumers.size(), 1);

            verify(notificationInterface).createConsumers(NotificationInterface.NotificationType.HOOK_PREPROCESS, 1);
        });
    }

    @Test
    public void testPreprocessorConsumersAreNotCreatedWhenParallelProcessingDisabled() throws Exception {
        mockParallelProcessingEnabled(false);

        NotificationConsumer<HookNotification> mockHookConsumer = mockHookNotificationConsumer();
        mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK, 1,
                Collections.singletonList(mockHookConsumer));

        new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

        verify(notificationInterface, never()).createConsumers(NotificationInterface.NotificationType.HOOK_PREPROCESS, 1);
    }

    @Test
    public void testPreprocessorConsumerUsesNotificationPreProcessorEntityProcessor() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            List<NotificationHookConsumer.HookConsumer> preprocessorConsumers = notificationHookConsumer.getPreprocessorHookConsumers();

            assertNotNull(preprocessorConsumers);
            assertEquals(preprocessorConsumers.size(), 1);

            NotificationHookConsumer.HookConsumer hookConsumer = preprocessorConsumers.get(0);

            Field entityProcessorField = NotificationHookConsumer.HookConsumer.class.getDeclaredField("entityProcessor");
            entityProcessorField.setAccessible(true);
            Object entityProcessor = entityProcessorField.get(hookConsumer);

            assertNotNull(entityProcessor);
            assertTrue(entityProcessor instanceof NotificationPreProcessor,
                    "EntityProcessor should be instance of NotificationPreProcessor");
        });
    }

    @Test
    public void testPreprocessorConsumerHandleMessageCommitsKafkaOffset() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            HookNotificationV1.EntityCreateRequest message = createTestMessage();
            AtlasKafkaMessage<HookNotification> kafkaMessage = new AtlasKafkaMessage<>(message, 0, "ATLAS_HOOK", 0);

            doNothing().when(mockPreprocessConsumer).commit(any(TopicPartition.class), anyLong());

            hookConsumer.handleMessage(kafkaMessage);

            verify(mockPreprocessConsumer, times(1)).commit(any(TopicPartition.class), anyLong());
        });
    }

    @Test
    public void testMultiplePreprocessorConsumersAreCreated() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockConsumer1 = mockHookNotificationConsumer();
            NotificationConsumer<HookNotification> mockConsumer2 = mockHookNotificationConsumer();

            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Arrays.asList(mockConsumer1, mockConsumer2));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            List<NotificationHookConsumer.HookConsumer> preprocessorConsumers = notificationHookConsumer.getPreprocessorHookConsumers();

            assertNotNull(preprocessorConsumers);
            assertEquals(preprocessorConsumers.size(), 2);

            for (NotificationHookConsumer.HookConsumer consumer : preprocessorConsumers) {
                Field entityProcessorField = NotificationHookConsumer.HookConsumer.class.getDeclaredField("entityProcessor");
                entityProcessorField.setAccessible(true);
                Object entityProcessor = entityProcessorField.get(consumer);

                assertTrue(entityProcessor instanceof NotificationPreProcessor);
            }
        });
    }

    @Test
    public void testPreprocessorConsumerThreadName() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);
            String threadName = hookConsumer.getName();

            assertNotNull(threadName);
            assertTrue(threadName.contains("preprocessor-consumer-thread"),
                    "Expected ATLAS_HOOK_PRE_PROCESSOR thread name (e.g. atlas-hook-preprocessor-consumer-thread)");
        });
    }

    @Test
    public void testGetPreprocessorHookConsumersReturnsEmptyListWhenNoConsumersCreated() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1, Collections.emptyList());

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            List<NotificationHookConsumer.HookConsumer> preprocessorConsumers = notificationHookConsumer.getPreprocessorHookConsumers();

            assertNotNull(preprocessorConsumers);
            assertTrue(preprocessorConsumers.isEmpty());
        });
    }

    @Test
    public void testConsumerFieldIsProperlyInitializedInPreprocessorHookConsumer() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            Field consumerField = NotificationHookConsumer.HookConsumer.class.getDeclaredField("consumer");
            consumerField.setAccessible(true);
            Object consumer = consumerField.get(hookConsumer);

            assertNotNull(consumer, "Consumer field should be initialized");
            assertEquals(consumer, mockPreprocessConsumer, "Consumer field should be the mock consumer");
        });
    }

    @Test
    public void testPreprocessorConsumerEntityProcessorIsNotSerialEntityProcessor() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            Field entityProcessorField = NotificationHookConsumer.HookConsumer.class.getDeclaredField("entityProcessor");
            entityProcessorField.setAccessible(true);
            Object entityProcessor = entityProcessorField.get(hookConsumer);

            assertTrue(entityProcessor instanceof NotificationPreProcessor);
            assertFalse(entityProcessor instanceof SerialEntityProcessor,
                    "Preprocessor consumer should NOT use SerialEntityProcessor");
        });
    }

    @Test
    public void testPreprocessorConsumersUseCorrectNotificationType() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            notificationHookConsumer.getPreprocessorHookConsumers();

            verify(notificationInterface, times(1)).createConsumers(NotificationInterface.NotificationType.HOOK_PREPROCESS, 1);

            verify(notificationInterface, never()).createConsumers(
                    NotificationInterface.NotificationType.HOOK_PREPROCESS, 8);
        });
    }

    @Test
    public void testGetPreprocessorHookConsumersCanBeCalledMultipleTimes() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            List<NotificationHookConsumer.HookConsumer> consumers1 = notificationHookConsumer.getPreprocessorHookConsumers();
            List<NotificationHookConsumer.HookConsumer> consumers2 = notificationHookConsumer.getPreprocessorHookConsumers();

            assertNotNull(consumers1);
            assertNotNull(consumers2);

            verify(notificationInterface, times(2)).createConsumers(NotificationInterface.NotificationType.HOOK_PREPROCESS, 1);
        });
    }

    @Test
    public void testPreprocessorConsumerReceivesMultipleMessagesCommitsEach() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            HookNotificationV1.EntityCreateRequest message1 = createTestMessage();
            HookNotificationV1.EntityCreateRequest message2 = createTestMessage();

            AtlasKafkaMessage<HookNotification> kafkaMessage1 = new AtlasKafkaMessage<>(message1, 0, "ATLAS_HOOK", 0);
            AtlasKafkaMessage<HookNotification> kafkaMessage2 = new AtlasKafkaMessage<>(message2, 1, "ATLAS_HOOK", 0);

            doNothing().when(mockPreprocessConsumer).commit(any(TopicPartition.class), anyLong());

            hookConsumer.handleMessage(kafkaMessage1);
            hookConsumer.handleMessage(kafkaMessage2);

            verify(mockPreprocessConsumer, times(2)).commit(any(TopicPartition.class), anyLong());
        });
    }

    @Test
    public void testPreprocessorErrorHandling_EmptyMessage() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            HookNotificationV1.EntityCreateRequest emptyMessage = new HookNotificationV1.EntityCreateRequest("test_user", Collections.emptyList());

            AtlasKafkaMessage<HookNotification> kafkaMessage = new AtlasKafkaMessage<>(emptyMessage, 0, "ATLAS_HOOK", 0);

            doNothing().when(mockPreprocessConsumer).commit(any(TopicPartition.class), anyLong());

            hookConsumer.handleMessage(kafkaMessage);

            verify(mockPreprocessConsumer, times(1)).commit(any(TopicPartition.class), anyLong());
        });
    }

    @Test
    public void testPreprocessorErrorHandling_InvalidEntityType() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            when(typeRegistry.getType(anyString())).thenReturn(null);

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            Referenceable entity = new Referenceable("invalid_type_that_does_not_exist");
            entity.set(QUALIFIED_NAME, "test.entity@cluster");
            HookNotificationV1.EntityCreateRequest message = new HookNotificationV1.EntityCreateRequest("test_user", Collections.singletonList(entity));

            AtlasKafkaMessage<HookNotification> kafkaMessage = new AtlasKafkaMessage<>(message, 0, "ATLAS_HOOK", 0);

            doNothing().when(mockPreprocessConsumer).commit(any(TopicPartition.class), anyLong());

            hookConsumer.handleMessage(kafkaMessage);

            verify(mockPreprocessConsumer, times(1)).commit(any(TopicPartition.class), anyLong());
        });
    }

    @Test
    public void testPreprocessorErrorHandling_ProcessingException() throws Exception {
        withMockedNotificationProvider(() -> {
            mockParallelProcessingEnabled(true);

            NotificationConsumer<HookNotification> mockPreprocessConsumer = mockHookNotificationConsumer();
            mockCreateConsumers(notificationInterface, NotificationInterface.NotificationType.HOOK_PREPROCESS, 1,
                    Collections.singletonList(mockPreprocessConsumer));

            NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry, metricsUtil, null, asyncImporter, typeDefStore);

            NotificationHookConsumer.HookConsumer hookConsumer = notificationHookConsumer.getPreprocessorHookConsumers().get(0);

            HookNotificationV1.EntityCreateRequest message = createTestMessage();
            AtlasKafkaMessage<HookNotification> kafkaMessage = new AtlasKafkaMessage<>(message, 0, "ATLAS_HOOK", 0);

            doThrow(new RuntimeException("Kafka commit failed")).when(mockPreprocessConsumer).commit(any(TopicPartition.class), anyLong());

            try {
                hookConsumer.handleMessage(kafkaMessage);
            } catch (Exception e) {
                // optional: exception may surface from handleMessage
            }

            verify(mockPreprocessConsumer, times(1)).commit(any(TopicPartition.class), anyLong());
        });
    }

    //Helper methods
    /**
     * Sets {@code atlas.notification.parallel.processing.enabled} on the live {@link ApplicationProperties}
     * configuration so {@link AtlasConfiguration#ATLAS_PARALLEL_PROCESSING_ENABLED#getBoolean()} reflects it.
     */
    public static void setParallelProcessingEnabled(boolean enabled) throws AtlasException {
        ApplicationProperties.get().setProperty(
                AtlasConfiguration.ATLAS_PARALLEL_PROCESSING_ENABLED.getPropertyName(), enabled);
    }

    /**
     * Resets parallel processing flag to {@code false} (matches enum default).
     */
    public static void resetParallelProcessingEnabled() throws AtlasException {
        setParallelProcessingEnabled(false);
    }

    /**
     * Mirrors {@link org.apache.atlas.notification.preprocessor.NotificationPreProcessor::getRoutingQualifiedName(Object)}
     * for a raw qualified-name string: drop the first {@code ':'} suffix (metadata/timestamp), same as production.
     */
    public static String stripQualifiedNameForRouting(String fullQualifiedNameFromEntity) {
        if (StringUtils.isBlank(fullQualifiedNameFromEntity)) {
            return null;
        }
        int colon = fullQualifiedNameFromEntity.indexOf(':');
        if (colon != -1) {
            return fullQualifiedNameFromEntity.substring(0, colon);
        }
        return fullQualifiedNameFromEntity;
    }

    /**
     * Mirrors {@code NotificationPreProcessor#getRoutingKey(String, String, Object, NotificationMetadata)} for the
     * qualifiedName branch (no rename): strip {@code @cluster}, then first two dot-separated components.
     */
    public static String routingKeyFromStrippedQualifiedName(String routingQualifiedName) {
        if (StringUtils.isBlank(routingQualifiedName)) {
            return null;
        }
        int at = routingQualifiedName.indexOf('@');
        String qNameWithoutCluster = at != -1 ? routingQualifiedName.substring(0, at) : routingQualifiedName;
        String[] parts = qNameWithoutCluster.split("\\.");
        if (parts.length >= 2) {
            return parts[0] + "." + parts[1];
        }
        if (parts.length == 1) {
            return parts[0];
        }
        return StringUtils.isNotBlank(qNameWithoutCluster) ? qNameWithoutCluster : routingQualifiedName;
    }

    /**
     * End-to-end: entity {@code qualifiedName} attribute value → routing key passed to {@link org.apache.atlas.notification.QualifiedNameRouter#getTargetTopic(String)}.
     */
    public static String routingKeyFromFullQualifiedNameByPreprocessor(String fullQualifiedNameFromEntity) {
        String stripped = stripQualifiedNameForRouting(fullQualifiedNameFromEntity);
        if (stripped == null) {
            return null;
        }
        return routingKeyFromStrippedQualifiedName(stripped);
    }

    public static Configuration defaultPreprocessorConfiguration() {
        Configuration config = new PropertiesConfiguration();
        config.setProperty("atlas.notification.processor.metadata.topic.count", 5);
        config.setProperty("atlas.notification.processor.lineage.topic.count", 3);
        config.setProperty("atlas.notification.processor.lineage.topic.enabled", true);
        config.setProperty("atlas.notification.hook.maxretries", 3);
        config.setProperty("atlas.notification.hook.failedcachesize", 10);
        return config;
    }

    public static AtlasEntity hiveTableEntity(String qualifiedName) {
        AtlasEntity entity = new AtlasEntity();
        entity.setTypeName("hive_table");
        entity.setGuid(java.util.UUID.randomUUID().toString());
        entity.setAttribute("qualifiedName", qualifiedName);
        entity.setAttribute("name", "t");
        return entity;
    }

    public static EntityCreateRequestV2 entityCreateRequestV2(String user, AtlasEntity... entities) {
        List<AtlasEntity> list = new ArrayList<>();
        Collections.addAll(list, entities);
        AtlasEntitiesWithExtInfo extInfo = new AtlasEntitiesWithExtInfo(list, null);
        return new EntityCreateRequestV2(user, extInfo);
    }

    public static AtlasKafkaMessage<HookNotification> preprocessKafkaMessage(HookNotification notification) {
        long now = System.currentTimeMillis();
        return new AtlasKafkaMessage<>(notification, 0L, "ATLAS_HOOK", 0, now, false);
    }
}
