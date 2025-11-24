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
package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_ACTION_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_COUNT_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TTL_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TYPE_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_REDUCTION_TYPE_NAME;
import static org.apache.atlas.repository.Constants.CREATE_EVENTS_AGEOUT_ALLOWED_KEY;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_AUDIT_REDUCTION_NAME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_CUSTOM;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_SWEEPOUT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class AuditReductionTaskTest {
    @Mock private EntityAuditRepository auditRepository;
    @Mock private AtlasGraph graph;
    @Mock private AtlasTask task;
    @Mock private AtlasGraphQuery query;
    @Mock private AtlasVertex vertex;
    @Mock private Iterator<AtlasVertex> vertexIterator;

    private AuditReductionTask auditReductionTask;
    private MockedStatic<RequestContext> requestContextMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup basic task mocks
        when(task.getGuid()).thenReturn("test-task-guid");
        when(task.getCreatedBy()).thenReturn("testUser");

        auditReductionTask = new AuditReductionTask(task, auditRepository, graph);

        // Mock RequestContext
        requestContextMock = mockStatic(RequestContext.class);
        RequestContext mockContext = mock(RequestContext.class);
        requestContextMock.when(() -> RequestContext.get()).thenReturn(mockContext);
        requestContextMock.when(() -> RequestContext.clear()).then(invocation -> null);
        lenient().doNothing().when(mockContext).setUser(anyString(), any());
    }

    @AfterMethod
    public void tearDown() {
        if (requestContextMock != null) {
            requestContextMock.close();
        }
    }

    @Test
    public void testPerformWithValidParameters() throws Exception {
        // Setup task parameters
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);

        // Mock findVertex to return null (no vertex found)
        setupFindVertexMock(null);

        // Mock getStatus to return COMPLETE after setStatus is called
        when(task.getStatus()).thenReturn(COMPLETE);

        AtlasTask.Status result = auditReductionTask.perform();

        assertEquals(result, COMPLETE);
        verify(task).setStatus(COMPLETE);
    }

    @Test
    public void testPerformWithEmptyParameters() throws Exception {
        when(task.getParameters()).thenReturn(null);

        AtlasTask.Status result = auditReductionTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithEmptyUserName() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn("");

        AtlasTask.Status result = auditReductionTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithNullUserName() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn(null);

        AtlasTask.Status result = auditReductionTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithException() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);

        setupFindVertexMock(vertex);
        when(vertex.getProperty(anyString(), eq(List.class))).thenThrow(new RuntimeException("Test exception"));

        expectThrows(RuntimeException.class, () -> {
            try {
                auditReductionTask.perform();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        verify(task).setStatus(FAILED);
    }

    @Test
    public void testFindVertexReturnsExistingVertex() {
        setupFindVertexMock(vertex);

        AtlasVertex result = auditReductionTask.findVertex();

        assertNotNull(result);
        assertEquals(result, vertex);
        verify(graph).query();
        verify(query).has(PROPERTY_KEY_AUDIT_REDUCTION_NAME, AUDIT_REDUCTION_TYPE_NAME);
    }

    @Test
    public void testFindVertexReturnsNull() {
        setupFindVertexMock(null);

        AtlasVertex result = auditReductionTask.findVertex();

        assertNull(result);
    }

    @Test
    public void testRunWithNullVertex() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupFindVertexMock(null);

        invokeRunMethod(params);

        verify(auditRepository, never()).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testRunWithValidVertexAndGuids() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupFindVertexMock(vertex);

        // Setup vertex with guids
        List<String> guids = Arrays.asList("guid1", "guid2", "guid3");
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(guids);

        // Mock audit repository
        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository, times(3)).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
        verify(vertex, times(1)).setProperty(eq(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT), any());
    }

    @Test
    public void testRunWithEmptyGuids() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupFindVertexMock(vertex);

        // Setup vertex with empty guids
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(new ArrayList<>());

        invokeRunMethod(params);

        verify(auditRepository, never()).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testRunWithLargeAuditCount() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_COUNT_KEY, Integer.MAX_VALUE);
        setupFindVertexMock(vertex);

        List<String> guids = Arrays.asList("guid1");
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(guids);

        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), eq(Short.MAX_VALUE), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository).deleteEventsV2(anyString(), anySet(), eq(Short.MAX_VALUE), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testRunWithSmallAuditCount() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_COUNT_KEY, Integer.MIN_VALUE);
        setupFindVertexMock(vertex);

        List<String> guids = Arrays.asList("guid1");
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(guids);

        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), eq(Short.MIN_VALUE), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository).deleteEventsV2(anyString(), anySet(), eq(Short.MIN_VALUE), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testRunWithLargeBatchOfGuids() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupFindVertexMock(vertex);

        List<String> guids = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            guids.add("guid" + i);
        }
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(guids);

        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository, times(250)).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
        verify(vertex, times(3)).setProperty(eq(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT), any());
    }

    @Test
    public void testRunWithSweepAgingType() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_TYPE_KEY, AtlasAuditAgingType.SWEEP);
        setupFindVertexMock(vertex);

        List<String> guids = Arrays.asList("guid1");
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_SWEEPOUT, List.class)).thenReturn(guids);

        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testRunWithCustomAgingType() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_TYPE_KEY, AtlasAuditAgingType.CUSTOM);
        setupFindVertexMock(vertex);

        List<String> guids = Arrays.asList("guid1");
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_CUSTOM, List.class)).thenReturn(guids);

        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testRunWithMultipleActionTypes() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        Collection<String> actionTypes = Arrays.asList("ENTITY_CREATE", "ENTITY_UPDATE", "ENTITY_DELETE");
        params.put(AUDIT_AGING_ACTION_TYPES_KEY, actionTypes);
        setupFindVertexMock(vertex);

        List<String> guids = Arrays.asList("guid1");
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(guids);

        List<EntityAuditEventV2> deletedEvents = Arrays.asList(mock(EntityAuditEventV2.class));
        when(auditRepository.deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any())).thenReturn(deletedEvents);

        doNothing().when(vertex).setProperty(anyString(), any());

        invokeRunMethod(params);

        verify(auditRepository).deleteEventsV2(anyString(), anySet(), anyShort(), anyInt(), anyBoolean(), any());
    }

    @Test
    public void testConstructor() {
        AuditReductionTask task = new AuditReductionTask(this.task, auditRepository, graph);
        assertNotNull(task);
    }

    // Helper methods
    private Map<String, Object> createValidTaskParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(AUDIT_AGING_TYPE_KEY, AtlasAuditAgingType.DEFAULT);
        params.put(AUDIT_AGING_ACTION_TYPES_KEY, Arrays.asList("ENTITY_CREATE"));
        params.put(AUDIT_AGING_COUNT_KEY, 100);
        params.put(AUDIT_AGING_TTL_KEY, 86400);
        params.put(CREATE_EVENTS_AGEOUT_ALLOWED_KEY, true);
        return params;
    }

    private void setupFindVertexMock(AtlasVertex returnVertex) {
        when(graph.query()).thenReturn(query);
        when(query.has(anyString(), anyString())).thenReturn(query);
        when(query.vertices()).thenReturn(() -> vertexIterator);
        when(vertexIterator.hasNext()).thenReturn(returnVertex != null);
        if (returnVertex != null) {
            when(vertexIterator.next()).thenReturn(returnVertex);
        }
    }

    private void invokeRunMethod(Map<String, Object> params) throws Exception {
        Method runMethod = AuditReductionTask.class.getDeclaredMethod("run", Map.class);
        runMethod.setAccessible(true);
        runMethod.invoke(auditReductionTask, params);
    }

    private <T extends Throwable> T expectThrows(Class<T> expectedType, Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " to be thrown");
        } catch (Throwable throwable) {
            if (expectedType.isInstance(throwable)) {
                return expectedType.cast(throwable);
            }
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " but got " + throwable.getClass().getSimpleName(), throwable);
        }
    }
}
