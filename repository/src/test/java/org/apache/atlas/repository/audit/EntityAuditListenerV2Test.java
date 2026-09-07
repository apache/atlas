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
package org.apache.atlas.repository.audit;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.rulesengine.AtlasEntityAuditFilterService;
import org.apache.atlas.rulesengine.RuleAction;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class EntityAuditListenerV2Test {
    @Mock
    private EntityAuditRepository mockAuditRepository;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasInstanceConverter mockInstanceConverter;

    @Mock
    private AtlasEntityAuditFilterService mockAuditFilterService;

    @Mock
    private RuleAction mockDefaultAction;

    private EntityAuditListenerV2 entityAuditListenerV2;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        entityAuditListenerV2 = new EntityAuditListenerV2(mockAuditRepository, mockTypeRegistry, mockInstanceConverter);
    }

    @Test
    public void testPutAuditEventsFiltersDiscardedEvents() throws Exception {
        EntityAuditEventV2 acceptedEvent = createAuditEvent("guid-1", false);
        EntityAuditEventV2 discardedEvent = createAuditEvent("guid-2", true);

        invokePutAuditEvents(Arrays.asList(acceptedEvent, discardedEvent));

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockAuditRepository).putEventsV2(captor.capture());

        List<EntityAuditEventV2> persistedEvents = captor.getValue();
        assertEquals(persistedEvents.size(), 1);
        assertEquals(persistedEvents.get(0).getEntityId(), "guid-1");
    }

    @Test
    public void testPutAuditEventsSkipsRepositoryWhenAllDiscarded() throws Exception {
        EntityAuditEventV2 discardedEvent1 = createAuditEvent("guid-1", true);
        EntityAuditEventV2 discardedEvent2 = createAuditEvent("guid-2", true);

        invokePutAuditEvents(Arrays.asList(discardedEvent1, discardedEvent2));

        verify(mockAuditRepository, never()).putEventsV2(org.mockito.ArgumentMatchers.anyList());
    }

    @Test
    public void testApplyRulesDoesNotThrowWhenRuleEngineFails() throws Exception {
        when(mockAuditFilterService.isEntityAuditCustomFilterEnabled()).thenReturn(true);
        when(mockAuditFilterService.isDiscardByDefault()).thenReturn(false);
        when(mockAuditFilterService.getDefaultAction()).thenReturn(mockDefaultAction);
        when(mockDefaultAction.getResult()).thenReturn(RuleAction.Result.ACCEPT);
        when(mockAuditFilterService.fetchRules()).thenThrow(new AtlasBaseException("failed to load rules"));

        entityAuditListenerV2.setAuditFilterService(mockAuditFilterService);

        AtlasEntity entity = new AtlasEntity("hive_table");
        entity.setGuid("guid-apply-rules");
        entity.setAttribute("qualifiedName", "qn@test");

        EntityAuditEventV2 auditEvent = new EntityAuditEventV2();
        auditEvent.setAction(EntityAuditActionV2.ENTITY_CREATE);
        auditEvent.setDiscarded(false);

        invokeApplyRules(entity, auditEvent);

        assertFalse(auditEvent.isDiscarded());
    }

    @Test
    public void testCreateEventDoesNotThrowWhenRuleEngineFails() throws Exception {
        when(mockAuditFilterService.isEntityAuditCustomFilterEnabled()).thenReturn(true);
        when(mockAuditFilterService.isDiscardByDefault()).thenReturn(false);
        when(mockAuditFilterService.getDefaultAction()).thenReturn(mockDefaultAction);
        when(mockDefaultAction.getResult()).thenReturn(RuleAction.Result.ACCEPT);
        when(mockAuditFilterService.fetchRules()).thenThrow(new AtlasBaseException("failed to load rules"));

        entityAuditListenerV2.setAuditFilterService(mockAuditFilterService);

        AtlasEntity entity = new AtlasEntity("hive_table");
        entity.setGuid("guid-create-event");
        entity.setAttribute("qualifiedName", "qn@test");

        EntityAuditEventV2 auditEvent = new EntityAuditEventV2();
        invokeCreateEvent(auditEvent, entity, EntityAuditActionV2.ENTITY_CREATE, "Created: test");

        assertFalse(auditEvent.isDiscarded());
    }

    @Test
    public void testPutAuditEventsPassesThroughWhenNoneDiscarded() throws Exception {
        EntityAuditEventV2 acceptedEvent1 = createAuditEvent("guid-1", false);
        EntityAuditEventV2 acceptedEvent2 = createAuditEvent("guid-2", false);
        List<EntityAuditEventV2> events = Arrays.asList(acceptedEvent1, acceptedEvent2);

        invokePutAuditEvents(events);

        verify(mockAuditRepository).putEventsV2(events);
    }

    private void invokePutAuditEvents(List<EntityAuditEventV2> events) throws Exception {
        Method method = EntityAuditListenerV2.class.getDeclaredMethod("putAuditEvents", List.class);
        method.setAccessible(true);
        method.invoke(entityAuditListenerV2, events);
    }

    private void invokeApplyRules(AtlasEntity entity, EntityAuditEventV2 auditEvent) throws Exception {
        Method method = EntityAuditListenerV2.class.getDeclaredMethod("applyRules", AtlasEntity.class, EntityAuditEventV2.class);
        method.setAccessible(true);
        method.invoke(entityAuditListenerV2, entity, auditEvent);
    }

    private void invokeCreateEvent(EntityAuditEventV2 auditEvent, AtlasEntity entity,
            EntityAuditActionV2 action, String details) throws Exception {
        Method method = EntityAuditListenerV2.class.getDeclaredMethod("createEvent",
                EntityAuditEventV2.class, AtlasEntity.class, EntityAuditActionV2.class, String.class);
        method.setAccessible(true);
        method.invoke(entityAuditListenerV2, auditEvent, entity, action, details);
    }

    private EntityAuditEventV2 createAuditEvent(String entityId, boolean discarded) {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        event.setEntityId(entityId);
        event.setAction(EntityAuditActionV2.ENTITY_CREATE);
        event.setDiscarded(discarded);
        return event;
    }
}
