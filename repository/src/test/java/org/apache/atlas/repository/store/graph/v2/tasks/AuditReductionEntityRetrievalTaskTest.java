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
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_ENTITY_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_SUBTYPES_INCLUDED_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TYPE_KEY;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.ATLAS_AUDIT_REDUCTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AuditReductionEntityRetrievalTaskTest {
    @Mock private AtlasDiscoveryService discoveryService;
    @Mock private AtlasTypeRegistry typeRegistry;
    @Mock private AtlasGraph graph;
    @Mock private AtlasTask task;
    @Mock private AtlasGraphQuery query;
    @Mock private AtlasVertex vertex;
    @Mock private Iterator<AtlasVertex> vertexIterator;

    private AuditReductionEntityRetrievalTask retrievalTask;
    private MockedStatic<RequestContext> requestContextMock;
    private MockedStatic<AtlasEntityType> atlasEntityTypeMock;
    private MockedStatic<AtlasGraphUtilsV2> atlasGraphUtilsMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup basic task mocks
        when(task.getGuid()).thenReturn("test-task-guid");
        when(task.getCreatedBy()).thenReturn("testUser");

        retrievalTask = new AuditReductionEntityRetrievalTask(task, graph, discoveryService, typeRegistry);

        // Mock RequestContext
        requestContextMock = mockStatic(RequestContext.class);
        RequestContext mockContext = mock(RequestContext.class);
        requestContextMock.when(() -> RequestContext.get()).thenReturn(mockContext);
        requestContextMock.when(() -> RequestContext.clear()).then(invocation -> null);
        lenient().doNothing().when(mockContext).setUser(anyString(), any());

        // Mock AtlasEntityType
        atlasEntityTypeMock = mockStatic(AtlasEntityType.class);

        // Mock AtlasGraphUtilsV2
        atlasGraphUtilsMock = mockStatic(AtlasGraphUtilsV2.class);
        atlasGraphUtilsMock.when(() -> AtlasGraphUtilsV2.setEncodedProperty(any(), any(), any())).then(invocation -> null);
    }

    @AfterMethod
    public void tearDown() {
        if (requestContextMock != null) {
            requestContextMock.close();
        }
        if (atlasEntityTypeMock != null) {
            atlasEntityTypeMock.close();
        }
        if (atlasGraphUtilsMock != null) {
            atlasGraphUtilsMock.close();
        }
    }

    @Test
    public void testPerformWithValidParameters() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);

        setupMocksForSuccessfulRun();

        when(task.getStatus()).thenReturn(COMPLETE);

        AtlasTask.Status result = retrievalTask.perform();

        assertEquals(result, COMPLETE);
        verify(task).setStatus(COMPLETE);
    }

    @Test
    public void testPerformWithEmptyParameters() throws Exception {
        when(task.getParameters()).thenReturn(null);

        AtlasTask.Status result = retrievalTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithEmptyUserName() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn("");

        AtlasTask.Status result = retrievalTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithNullUserName() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn(null);

        AtlasTask.Status result = retrievalTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithException() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getStatus()).thenReturn(COMPLETE);

        when(discoveryService.searchGUIDsWithParameters(any(), any(), any())).thenThrow(new RuntimeException("Test exception"));

        AtlasTask.Status result = retrievalTask.perform();

        assertEquals(result, COMPLETE);
        verify(task).setStatus(COMPLETE);
    }

    @Test
    public void testRunWithValidParameters() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupMocksForSuccessfulRun();

        invokeRunMethod(params);

        verify(discoveryService).searchGUIDsWithParameters(any(), any(), any());
        verify(discoveryService).createAndQueueAuditReductionTask(any(), eq(ATLAS_AUDIT_REDUCTION));
    }

    @Test
    public void testCreateAgingTaskWithEligibleGUIDs() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupMocksForSuccessfulRun();

        AtlasTask result = invokeCreateAgingTaskMethod(params);

        assertNotNull(result);
        verify(discoveryService).searchGUIDsWithParameters(any(), any(), any());
    }

    @Test
    public void testCreateAgingTaskWithInvalidEntityTypes() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        setupMocksForInvalidEntityTypes();

        AtlasTask result = invokeCreateAgingTaskMethod(params);

        assertNull(result);
    }

    @Test
    public void testCreateAgingTaskWithDefaultAgingTypeAndEntityTypes() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_TYPE_KEY, AtlasAuditAgingType.DEFAULT);

        setupMocksForSuccessfulRun();

        AtlasTask result = invokeCreateAgingTaskMethod(params);

        assertNotNull(result);
        assertTrue((Boolean) result.getParameters().get(AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY));
    }

    @Test
    public void testCreateAgingTaskWithNonDefaultAgingType() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_TYPE_KEY, AtlasAuditAgingType.CUSTOM);

        setupMocksForSuccessfulRun();

        AtlasTask result = invokeCreateAgingTaskMethod(params);

        assertNotNull(result);
        assertFalse((Boolean) result.getParameters().get(AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY));
    }

    @Test
    public void testValidateTypesAndIncludeSubTypesWithValidTypes() throws Exception {
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Table", "Column"));
        Collection<String> allEntityTypeNames = Arrays.asList("Table", "Column", "Database", "Schema");
        when(typeRegistry.getAllEntityDefNames()).thenReturn(allEntityTypeNames);

        atlasEntityTypeMock.when(() -> AtlasEntityType.getEntityTypesAndAllSubTypes(any(), any())).thenReturn(entityTypes);

        boolean result = invokeValidateTypesMethod(entityTypes, AtlasAuditAgingType.CUSTOM, true);

        assertTrue(result);
    }

    @Test
    public void testValidateTypesAndIncludeSubTypesWithWildcardTypes() throws Exception {
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Table*"));
        Collection<String> allEntityTypeNames = Arrays.asList("Table", "TablePartition", "Column", "Database");
        when(typeRegistry.getAllEntityDefNames()).thenReturn(allEntityTypeNames);

        atlasEntityTypeMock.when(() -> AtlasEntityType.getEntityTypesAndAllSubTypes(any(), any())).thenReturn(new HashSet<>(Arrays.asList("Table", "TablePartition")));

        boolean result = invokeValidateTypesMethod(entityTypes, AtlasAuditAgingType.CUSTOM, true);

        assertTrue(result);
        assertTrue(entityTypes.contains("Table"));
        assertTrue(entityTypes.contains("TablePartition"));
    }

    @Test
    public void testValidateTypesAndIncludeSubTypesWithInvalidTypes() throws Exception {
        Set<String> entityTypes = new HashSet<>(Arrays.asList("InvalidType"));
        Collection<String> allEntityTypeNames = Arrays.asList("Table", "Column", "Database");
        when(typeRegistry.getAllEntityDefNames()).thenReturn(allEntityTypeNames);

        atlasEntityTypeMock.when(() -> AtlasEntityType.getEntityTypesAndAllSubTypes(any(), any())).thenReturn(new HashSet<>());

        boolean result = invokeValidateTypesMethod(entityTypes, AtlasAuditAgingType.CUSTOM, true);

        assertFalse(result);
    }

    @Test
    public void testValidateTypesForDefaultAgingTypeWithInvalidTypes() throws Exception {
        Set<String> entityTypes = new HashSet<>(Arrays.asList("InvalidType"));
        Collection<String> allEntityTypeNames = Arrays.asList("Table", "Column", "Database");
        when(typeRegistry.getAllEntityDefNames()).thenReturn(allEntityTypeNames);

        atlasEntityTypeMock.when(() -> AtlasEntityType.getEntityTypesAndAllSubTypes(any(), any())).thenReturn(new HashSet<>());

        boolean result = invokeValidateTypesMethod(entityTypes, AtlasAuditAgingType.DEFAULT, true);

        assertTrue(result);
    }

    @Test
    public void testUpdateVertexWithGuidsAndCreateAgingTaskWithEmptyGuids() throws Exception {
        setupVertexMocks();
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(new ArrayList<>());

        AtlasTask result = invokeUpdateVertexMethod(vertex, PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, new HashSet<>(), createValidTaskParameters());

        assertNull(result);
    }

    @Test
    public void testUpdateVertexWithGuidsAndCreateAgingTaskWithExistingGuids() throws Exception {
        setupVertexMocks();
        List<String> existingGuids = new ArrayList<>(Arrays.asList("guid1", "guid2"));
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(existingGuids);

        Set<String> newGuids = new HashSet<>(Arrays.asList("guid3", "guid4"));
        AtlasTask mockTask = mock(AtlasTask.class);
        when(discoveryService.createAndQueueAuditReductionTask(any(), anyString())).thenReturn(mockTask);

        AtlasTask result = invokeUpdateVertexMethod(vertex, PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, newGuids, createValidTaskParameters());

        assertNotNull(result);
        verify(discoveryService).createAndQueueAuditReductionTask(any(), eq(ATLAS_AUDIT_REDUCTION));
    }

    @Test
    public void testUpdateVertexWithGuidsAndCreateAgingTaskWithNullExistingGuids() throws Exception {
        setupVertexMocks();
        when(vertex.getProperty(PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, List.class)).thenReturn(null);

        Set<String> newGuids = new HashSet<>(Arrays.asList("guid1", "guid2"));
        AtlasTask mockTask = mock(AtlasTask.class);
        when(discoveryService.createAndQueueAuditReductionTask(any(), anyString())).thenReturn(mockTask);

        AtlasTask result = invokeUpdateVertexMethod(vertex, PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT, newGuids, createValidTaskParameters());

        assertNotNull(result);
    }

    @Test
    public void testGetOrCreateVertexWhenVertexExists() throws Exception {
        setupVertexMocks();
        when(vertexIterator.hasNext()).thenReturn(true);
        when(vertexIterator.next()).thenReturn(vertex);

        AtlasVertex result = invokeGetOrCreateVertexMethod();

        assertNotNull(result);
        assertEquals(result, vertex);
        verify(graph, never()).addVertex();
    }

    @Test
    public void testGetOrCreateVertexWhenVertexDoesNotExist() throws Exception {
        setupVertexMocks();
        when(vertexIterator.hasNext()).thenReturn(false);
        AtlasVertex newVertex = mock(AtlasVertex.class);
        when(graph.addVertex()).thenReturn(newVertex);

        AtlasVertex result = invokeGetOrCreateVertexMethod();

        assertNotNull(result);
        assertEquals(result, newVertex);
        verify(graph).addVertex();
    }

    @Test
    public void testConstructor() {
        AuditReductionEntityRetrievalTask task = new AuditReductionEntityRetrievalTask(this.task, graph, discoveryService, typeRegistry);
        assertNotNull(task);
    }

    @Test
    public void testCreateAgingTaskWithEmptyEntityTypes() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        params.put(AUDIT_AGING_ENTITY_TYPES_KEY, new ArrayList<>());

        setupMocksForSuccessfulRun();

        AtlasTask result = invokeCreateAgingTaskMethod(params);

        assertNotNull(result);
        verify(discoveryService).searchGUIDsWithParameters(any(), any(), any());
    }

    @Test
    public void testRunWithExceptionHandling() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(discoveryService.searchGUIDsWithParameters(any(), any(), any())).thenThrow(new AtlasBaseException("Test exception"));

        invokeRunMethod(params);
    }

    // Helper methods
    private Map<String, Object> createValidTaskParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(AUDIT_AGING_ENTITY_TYPES_KEY, Arrays.asList("Table", "Column"));
        params.put(AUDIT_AGING_TYPE_KEY, AtlasAuditAgingType.CUSTOM);
        params.put(AUDIT_AGING_SUBTYPES_INCLUDED_KEY, true);
        return params;
    }

    private void setupMocksForSuccessfulRun() throws AtlasBaseException {
        Collection<String> allEntityTypeNames = Arrays.asList("Table", "Column", "Database", "Schema");
        when(typeRegistry.getAllEntityDefNames()).thenReturn(allEntityTypeNames);

        Set<String> guids = new HashSet<>(Arrays.asList("guid1", "guid2"));
        when(discoveryService.searchGUIDsWithParameters(any(), any(), any())).thenReturn(guids);

        setupVertexMocks();
        when(vertexIterator.hasNext()).thenReturn(true);
        when(vertexIterator.next()).thenReturn(vertex);
        when(vertex.getProperty(anyString(), eq(List.class))).thenReturn(new ArrayList<>());

        AtlasTask mockTask = mock(AtlasTask.class);
        Map<String, Object> taskParams = new HashMap<>();
        when(mockTask.getParameters()).thenReturn(taskParams);
        when(discoveryService.createAndQueueAuditReductionTask(any(), anyString())).thenReturn(mockTask);

        atlasEntityTypeMock.when(() -> AtlasEntityType.getEntityTypesAndAllSubTypes(any(), any())).thenReturn(new HashSet<>(Arrays.asList("Table", "Column")));
    }

    private void setupMocksForInvalidEntityTypes() {
        Collection<String> allEntityTypeNames = Arrays.asList("ValidType1", "ValidType2");
        when(typeRegistry.getAllEntityDefNames()).thenReturn(allEntityTypeNames);

        atlasEntityTypeMock.when(() -> AtlasEntityType.getEntityTypesAndAllSubTypes(any(), any())).thenReturn(new HashSet<>());
    }

    private void setupVertexMocks() {
        when(graph.query()).thenReturn(query);
        when(query.has(anyString(), anyString())).thenReturn(query);
        when(query.vertices()).thenReturn(() -> vertexIterator);
        when(graph.addVertex()).thenReturn(vertex);
        doNothing().when(vertex).setProperty(anyString(), any());
    }

    private void invokeRunMethod(Map<String, Object> params) throws Exception {
        Method runMethod = AuditReductionEntityRetrievalTask.class.getDeclaredMethod("run", Map.class);
        runMethod.setAccessible(true);
        runMethod.invoke(retrievalTask, params);
    }

    private AtlasTask invokeCreateAgingTaskMethod(Map<String, Object> params) throws Exception {
        Method createAgingTaskMethod = AuditReductionEntityRetrievalTask.class.getDeclaredMethod("createAgingTaskWithEligibleGUIDs", Map.class);
        createAgingTaskMethod.setAccessible(true);
        return (AtlasTask) createAgingTaskMethod.invoke(retrievalTask, params);
    }

    private boolean invokeValidateTypesMethod(Set<String> entityTypes, AtlasAuditAgingType agingType,
                                              boolean subTypesIncluded) throws Exception {
        Method validateTypesMethod = AuditReductionEntityRetrievalTask.class.getDeclaredMethod("validateTypesAndIncludeSubTypes", Set.class, AtlasAuditAgingType.class, boolean.class);
        validateTypesMethod.setAccessible(true);
        return (Boolean) validateTypesMethod.invoke(retrievalTask, entityTypes, agingType, subTypesIncluded);
    }

    private AtlasTask invokeUpdateVertexMethod(AtlasVertex vertex, String vertexProperty,
                                               Set<String> guids, Map<String, Object> params) throws Exception {
        Method updateVertexMethod = AuditReductionEntityRetrievalTask.class.getDeclaredMethod("updateVertexWithGuidsAndCreateAgingTask", AtlasVertex.class, String.class, Set.class, Map.class);
        updateVertexMethod.setAccessible(true);
        return (AtlasTask) updateVertexMethod.invoke(retrievalTask, vertex, vertexProperty, guids, params);
    }

    private AtlasVertex invokeGetOrCreateVertexMethod() throws Exception {
        Method getOrCreateVertexMethod = AuditReductionEntityRetrievalTask.class.getDeclaredMethod("getOrCreateVertex");
        getOrCreateVertexMethod.setAccessible(true);
        return (AtlasVertex) getOrCreateVertexMethod.invoke(retrievalTask);
    }

    private <T extends Throwable> T expectThrows(Class<T> expectedType, Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " to be thrown");
        } catch (Throwable throwable) {
            // Handle InvocationTargetException from reflection
            if (throwable instanceof InvocationTargetException) {
                Throwable cause = throwable.getCause();
                if (expectedType.isInstance(cause)) {
                    return expectedType.cast(cause);
                }
                throw new AssertionError("Expected " + expectedType.getSimpleName() + " but got " +
                    cause.getClass().getSimpleName(), cause);
            }
            if (expectedType.isInstance(throwable)) {
                return expectedType.cast(throwable);
            }
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " but got " +
                throwable.getClass().getSimpleName(), throwable);
        }
    }
}
