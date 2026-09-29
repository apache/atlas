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
import org.apache.atlas.AtlasException;
import org.apache.atlas.kafka.AtlasKafkaConsumer;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.EmbeddedKafkaServer;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.server.common.service.ServiceState;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end: produce to embedded Kafka ATLAS_HOOK → consume → SerialEntityProcessor (ATLAS-5423).
 */
public class NotificationInvalidTypenameKafkaE2ETest {
    private static final String UNKNOWN_TYPE = "trino_table";

    private EmbeddedKafkaServer   kafkaServer;
    private KafkaNotification     kafkaNotification;
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
    private AtlasMetricsUtil metricsUtil;

    @Mock
    private AsyncImporter asyncImporter;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(typeRegistry.getEntityTypeByName(anyString())).thenReturn(null);

        Configuration applicationProperties = ApplicationProperties.get();
        applicationProperties.setProperty("atlas.kafka.data", "target/" + RandomStringUtils.randomAlphanumeric(8));

        kafkaServer           = new EmbeddedKafkaServer(applicationProperties);
        kafkaNotification     = new KafkaNotification(applicationProperties);
        notificationInterface = kafkaNotification;

        kafkaServer.start();
        kafkaNotification.start();
        Thread.sleep(2000);
    }

    @AfterMethod
    public void shutdown() {
        if (kafkaNotification != null) {
            kafkaNotification.close();
            kafkaNotification.stop();
        }
        if (kafkaServer != null) {
            kafkaServer.stop();
        }
    }

    @Test
    public void testKafkaHookInvalidTypenameDoesNotHitEntityStore() throws Exception {
        AtlasEntity entity = new AtlasEntity(UNKNOWN_TYPE);
        entity.setAttribute("qualifiedName", "e2e-atlas-5423-invalid-typename");

        AtlasObjectId objectId = new AtlasObjectId(UNKNOWN_TYPE,
                Collections.singletonMap("qualifiedName", "e2e-atlas-5423-invalid-typename"));

        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("admin", objectId,
                new AtlasEntity.AtlasEntityWithExtInfo(entity));

        kafkaNotification.send(NotificationInterface.NotificationType.HOOK, request);

        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(
                notificationInterface, atlasEntityStore, serviceState, instanceConverter, typeRegistry,
                metricsUtil, null, asyncImporter, null);

        NotificationConsumer<HookNotification> consumer = createConsumer(false);
        NotificationHookConsumer.HookConsumer  hookConsumer = notificationHookConsumer.new HookConsumer(consumer);

        assertTrue(pollAndHandleOneMessage(consumer, hookConsumer), "expected hook message to be consumed from Kafka");

        verify(atlasEntityStore, never()).updateEntity(any(), any(), anyBoolean());
        verify(atlasEntityStore, never()).createOrUpdate(any(), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    private NotificationConsumer<HookNotification> createConsumer(boolean autoCommit) throws AtlasException {
        return (NotificationConsumer<HookNotification>) (NotificationConsumer<?>) kafkaNotification
                .createConsumers(NotificationInterface.NotificationType.HOOK, 1, autoCommit)
                .get(0);
    }

    private boolean pollAndHandleOneMessage(NotificationConsumer<HookNotification> consumer,
                                            NotificationHookConsumer.HookConsumer hookConsumer) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;

        while (System.currentTimeMillis() < deadline) {
            List<AtlasKafkaMessage<HookNotification>> messages = consumer.receive();

            for (AtlasKafkaMessage<HookNotification> msg : messages) {
                hookConsumer.handleMessage(msg);
                return true;
            }

            Thread.sleep(250);
        }

        return false;
    }
}
