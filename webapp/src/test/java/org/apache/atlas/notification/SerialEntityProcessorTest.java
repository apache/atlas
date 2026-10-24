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

import org.apache.atlas.RequestContext;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.ImportNotification;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.pc.Ticket;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.commons.configuration2.Configuration;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.springframework.security.core.Authentication;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class SerialEntityProcessorTest {
    private AtlasEntityStore entityStore;

    private SerialEntityProcessor sep;

    @BeforeMethod
    public void build() {
        Configuration cfg = mockConfiguration();

        entityStore = Mockito.mock(AtlasEntityStore.class);
        AtlasMetricsUtil metricsUtil = Mockito.mock(AtlasMetricsUtil.class);
        doNothing().when(metricsUtil).onNotificationProcessingComplete(anyString(), anyInt(), anyLong(), any());
        Map<String, Authentication> principals = Collections.emptyMap();
        AtlasTypeRegistry registry = Mockito.mock(AtlasTypeRegistry.class);
        Mockito.when(registry.getEntityTypeByName(Mockito.anyString())).thenAnswer(inv -> Mockito.mock(AtlasEntityType.class));

        sep = new SerialEntityProcessor(cfg, metricsUtil, principals, entityStore, Mockito.mock(AtlasInstanceConverter.class),
                Mockito.mock(EntityCorrelationManager.class), registry,
                Mockito.mock(Logger.class), Mockito.mock(Logger.class), Mockito.mock(AsyncImporter.class));
    }

    @AfterMethod
    public void resetCtx() {
        RequestContext.clear();
    }

    private AtlasKafkaMessage<HookNotification> kafka(HookNotification hn) {
        return new AtlasKafkaMessage<>(hn, 4L, "hook-topic", 0);
    }

    private Configuration mockConfiguration() {
        Configuration cfg = Mockito.mock(Configuration.class);
        Mockito.when(cfg.getInt(anyString(), anyInt())).thenAnswer(inv -> inv.getArgument(1));
        Mockito.when(cfg.getBoolean(anyString(), anyBoolean())).thenAnswer(inv -> inv.getArgument(1));
        Mockito.when(cfg.getStringArray(Mockito.anyString())).thenReturn(null);

        return cfg;
    }

    private SerialEntityProcessor processorWith(AsyncImporter asyncImporter) {
        return new SerialEntityProcessor(mockConfiguration(), Mockito.mock(AtlasMetricsUtil.class), Collections.emptyMap(),
                entityStore, Mockito.mock(AtlasInstanceConverter.class),
                Mockito.mock(EntityCorrelationManager.class),
                Mockito.mock(AtlasTypeRegistry.class), Mockito.mock(Logger.class), Mockito.mock(Logger.class), asyncImporter);
    }

    private HookNotification importEntityNotification(String importId, int position) {
        AtlasEntity entity = new AtlasEntity("hive_table");

        entity.setGuid("guid-" + position);
        entity.setAttribute("qualifiedName", "default.t" + position + "@cl1");

        return new ImportNotification.AtlasEntityImportNotification(importId, "svc", new AtlasEntity.AtlasEntityWithExtInfo(entity), position);
    }

    @Test
    public void lifecycle_noopMethodsAndTicketWrapper() {
        assertNull(sep.collectResults());
        sep.shutdown();
        AtlasEntitiesWithExtInfo empty = new AtlasEntitiesWithExtInfo();
        empty.setEntities(Collections.emptyList());
        Ticket tk = Mockito.mock(Ticket.class);
        Mockito.when(tk.getMessage()).thenReturn(kafka(new HookNotification.EntityCreateRequestV2("svc", empty)));
        Mockito.when(tk.getQualifiedNamesSet()).thenReturn(Collections.emptySet());
        assertNotNull(sep.handleMessage(tk));
    }

    @Test
    public void importTypesDefFailure_completesImportRequestWithoutClassCast() throws Exception {
        AsyncImporter asyncImporter = Mockito.mock(AsyncImporter.class);

        doThrow(Mockito.mock(org.apache.atlas.exception.AtlasBaseException.class))
                .when(asyncImporter).onImportTypeDef(any(AtlasTypesDef.class), anyString());

        SerialEntityProcessor processor = processorWith(asyncImporter);

        HookNotification message = new ImportNotification.AtlasTypesDefImportNotification("import-123", "svc", new AtlasTypesDef());
        assertNotNull(processor.handleMessage(kafka(message)));

        verify(asyncImporter, times(1)).onImportComplete("import-123");
        verify(asyncImporter, times(1)).onCompleteImportRequest("import-123");
    }

    @Test
    public void lastImportEntity_releasesImportQueue() throws Exception {
        AsyncImporter asyncImporter = Mockito.mock(AsyncImporter.class);

        Mockito.when(asyncImporter.onImportEntity(any(), anyString(), anyInt())).thenReturn(true);

        SerialEntityProcessor processor = processorWith(asyncImporter);

        assertNotNull(processor.handleMessage(kafka(importEntityNotification("import-123", 9))));

        verify(asyncImporter, times(1)).onImportComplete("import-123");
        verify(asyncImporter, times(1)).onCompleteImportRequest("import-123");
    }

    @Test
    public void inFlightImportEntity_keepsImportQueueHeld() throws Exception {
        AsyncImporter asyncImporter = Mockito.mock(AsyncImporter.class);

        Mockito.when(asyncImporter.onImportEntity(any(), anyString(), anyInt())).thenReturn(false);

        SerialEntityProcessor processor = processorWith(asyncImporter);

        assertNotNull(processor.handleMessage(kafka(importEntityNotification("import-123", 1))));

        verify(asyncImporter, never()).onImportComplete(anyString());
        verify(asyncImporter, never()).onCompleteImportRequest(anyString());
    }

    @Test
    public void failedImportEntity_releasesImportQueue() throws Exception {
        AsyncImporter asyncImporter = Mockito.mock(AsyncImporter.class);

        doThrow(Mockito.mock(org.apache.atlas.exception.AtlasBaseException.class))
                .when(asyncImporter).onImportEntity(any(), anyString(), anyInt());

        SerialEntityProcessor processor = processorWith(asyncImporter);

        assertNotNull(processor.handleMessage(kafka(importEntityNotification("import-123", 9))));

        verify(asyncImporter, times(1)).onImportComplete("import-123");
        verify(asyncImporter, times(1)).onCompleteImportRequest("import-123");
    }

    @Test
    public void importCompletionBookkeepingFailure_stillReleasesImportQueue() throws Exception {
        AsyncImporter asyncImporter = Mockito.mock(AsyncImporter.class);

        Mockito.when(asyncImporter.onImportEntity(any(), anyString(), anyInt())).thenReturn(true);
        doThrow(new org.apache.atlas.exception.AtlasBaseException("import completion failed"))
                .when(asyncImporter).onImportComplete(anyString());

        SerialEntityProcessor processor = processorWith(asyncImporter);

        processor.handleMessage(kafka(importEntityNotification("import-123", 9)));

        verify(asyncImporter, atLeastOnce()).onImportComplete("import-123");
        verify(asyncImporter, times(1)).onCompleteImportRequest("import-123");
    }
}
