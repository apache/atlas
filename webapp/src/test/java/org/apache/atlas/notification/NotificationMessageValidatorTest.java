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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.notification.pc.Ticket;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class NotificationMessageValidatorTest {

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testValidateUnknownTypeNameFailsFast() throws Exception {
        AtlasTypeRegistry typeRegistry = mock(AtlasTypeRegistry.class);
        when(typeRegistry.getEntityTypeByName("trino_table")).thenReturn(null);

        AtlasEntity entity = new AtlasEntity("trino_table");
        AtlasObjectId objectId = new AtlasObjectId("trino_table", Collections.singletonMap("qualifiedName", "q1"));
        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("hook", objectId,
                new AtlasEntity.AtlasEntityWithExtInfo(entity));

        NotificationMessageValidator.validate(request, typeRegistry);
    }

    @Test
    public void testValidateKnownTypeNameSucceeds() throws Exception {
        AtlasTypeRegistry typeRegistry = mock(AtlasTypeRegistry.class);
        when(typeRegistry.getEntityTypeByName("hive_table")).thenReturn(mock(AtlasEntityType.class));

        AtlasEntity entity = new AtlasEntity("hive_table");
        AtlasObjectId objectId = new AtlasObjectId("hive_table", Collections.singletonMap("qualifiedName", "q1"));
        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("hook", objectId,
                new AtlasEntity.AtlasEntityWithExtInfo(entity));

        NotificationMessageValidator.validate(request, typeRegistry);
    }

    @Test
    public void testSerialEntityProcessorSkipsStoreForUnknownTypeName() throws Exception {
        AtlasTypeRegistry typeRegistry = mock(AtlasTypeRegistry.class);
        when(typeRegistry.getEntityTypeByName(anyString())).thenReturn(null);

        org.apache.atlas.repository.store.graph.AtlasEntityStore entityStore =
                mock(org.apache.atlas.repository.store.graph.AtlasEntityStore.class);

        org.apache.commons.configuration2.Configuration configuration = mock(org.apache.commons.configuration2.Configuration.class);
        when(configuration.getInt(anyString(), Mockito.anyInt())).thenAnswer(invocation -> invocation.getArgument(1));
        when(configuration.getBoolean(anyString(), anyBoolean())).thenAnswer(invocation -> invocation.getArgument(1));
        when(configuration.getStringArray(anyString())).thenReturn(null);

        SerialEntityProcessor processor = new SerialEntityProcessor(
                configuration,
                mock(org.apache.atlas.util.AtlasMetricsUtil.class),
                null,
                entityStore,
                mock(org.apache.atlas.repository.converters.AtlasInstanceConverter.class),
                new EntityCorrelationManager(mock(org.apache.atlas.repository.store.graph.EntityCorrelationStore.class)),
                typeRegistry,
                org.slf4j.LoggerFactory.getLogger("FAILED"),
                org.slf4j.LoggerFactory.getLogger("LARGE"),
                mock(org.apache.atlas.repository.impexp.AsyncImporter.class));

        AtlasEntity entity = new AtlasEntity("trino_table");
        AtlasObjectId objectId = new AtlasObjectId("trino_table", Collections.singletonMap("qualifiedName", "q1"));
        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("hook", objectId,
                new AtlasEntity.AtlasEntityWithExtInfo(entity));
        AtlasKafkaMessage<HookNotification> kafkaMsg = new AtlasKafkaMessage<>(request, 5L, "ATLAS_HOOK", 0);

        TopicPartitionOffsetResult result = processor.handleMessage(new Ticket(kafkaMsg));

        assertNotNull(result);
        verify(entityStore, never()).updateEntity(any(), any(), anyBoolean());
    }

    @Test
    public void testUnknownTypenameAtlasBaseExceptionIsNotRetryable() throws Exception {
        SerialEntityProcessor processor = new SerialEntityProcessor(
                mock(org.apache.commons.configuration2.Configuration.class),
                mock(org.apache.atlas.util.AtlasMetricsUtil.class),
                null,
                mock(org.apache.atlas.repository.store.graph.AtlasEntityStore.class),
                mock(org.apache.atlas.repository.converters.AtlasInstanceConverter.class),
                new EntityCorrelationManager(mock(org.apache.atlas.repository.store.graph.EntityCorrelationStore.class)),
                mock(AtlasTypeRegistry.class),
                org.slf4j.LoggerFactory.getLogger("FAILED"),
                org.slf4j.LoggerFactory.getLogger("LARGE"),
                mock(org.apache.atlas.repository.impexp.AsyncImporter.class));

        Method isRetryable = SerialEntityProcessor.class.getDeclaredMethod("isRetryableException", Throwable.class);
        isRetryable.setAccessible(true);

        AtlasBaseException unknownType = new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, "trino_table");
        assertFalse((Boolean) isRetryable.invoke(processor, unknownType));
    }
}
