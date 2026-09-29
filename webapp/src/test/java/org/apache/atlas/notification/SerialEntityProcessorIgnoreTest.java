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

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.atlas.notification.NotificationHookConsumer.CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ATLAS-5419: a type ignore pattern that drops ENTITY_CREATE_V2 must also drop
 * partial updates and deletes of that type. Otherwise the ignored type is
 * processed and fails later as an unknown typename.
 */
public class SerialEntityProcessorIgnoreTest {
    private AtlasEntityStore store;
    private SerialEntityProcessor processor;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(anyString(), anyBoolean())).thenAnswer(invocation -> invocation.getArgument(1));
        when(configuration.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));
        when(configuration.getStringArray(anyString())).thenReturn(new String[0]);
        when(configuration.getStringArray(eq(CONSUMER_PREPROCESS_ENTITY_TYPE_IGNORE_PATTERN)))
                .thenReturn(new String[] {"trino.*"});

        store = mock(AtlasEntityStore.class);
        when(store.updateEntity(any(), any(), anyBoolean())).thenReturn(new EntityMutationResponse());
        when(store.deleteByUniqueAttributes(any(), any())).thenReturn(new EntityMutationResponse());

        processor = new SerialEntityProcessor(configuration, mock(AtlasMetricsUtil.class), new HashMap<>(),
                store, mock(AtlasInstanceConverter.class), mock(EntityCorrelationManager.class),
                mock(AtlasTypeRegistry.class), mock(Logger.class), mock(Logger.class), mock(AsyncImporter.class));
    }

    @Test
    public void partialUpdateOfIgnoredTypeIsNotApplied() throws Exception {
        processor.handleMessage(message(partialUpdate("trino_catalog", "iceberg_catalog@cm")));

        verify(store, never()).updateEntity(any(), any(), anyBoolean());
    }

    @Test
    public void partialUpdateOfOtherTypeIsApplied() throws Exception {
        processor.handleMessage(message(partialUpdate("hive_table", "db.table@cm")));

        verify(store).updateEntity(any(), any(), eq(true));
    }

    @Test
    public void deleteOfIgnoredTypeIsNotApplied() throws Exception {
        processor.handleMessage(message(delete("trino_table", "iceberg.db.t@cm")));

        verify(store, never()).deleteByUniqueAttributes(any(), any());
    }

    @Test
    public void deleteOfOtherTypeIsApplied() throws Exception {
        processor.handleMessage(message(delete("hive_table", "db.table@cm")));

        verify(store).deleteByUniqueAttributes(any(), any());
    }

    private static EntityPartialUpdateRequestV2 partialUpdate(String typeName, String qualifiedName) {
        AtlasEntity entity = new AtlasEntity(typeName, "qualifiedName", qualifiedName);
        AtlasObjectId entityId = new AtlasObjectId(typeName, "qualifiedName", qualifiedName);
        return new EntityPartialUpdateRequestV2("hook", entityId, new AtlasEntityWithExtInfo(entity));
    }

    private static EntityDeleteRequestV2 delete(String typeName, String qualifiedName) {
        AtlasObjectId entityId = new AtlasObjectId(typeName, "qualifiedName", qualifiedName);
        return new EntityDeleteRequestV2("hook", Collections.singletonList(entityId));
    }

    private static AtlasKafkaMessage<org.apache.atlas.model.notification.HookNotification> message(
            org.apache.atlas.model.notification.HookNotification notification) {
        return new AtlasKafkaMessage<>(notification, 1L, "ATLAS_HOOK", 0);
    }
}
