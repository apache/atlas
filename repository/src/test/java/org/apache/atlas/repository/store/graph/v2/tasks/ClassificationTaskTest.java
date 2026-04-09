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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasType;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_CLASSIFICATION_NAME;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_CLASSIFICATION_VERTEX_ID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_ENTITY_GUID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_RELATIONSHIP_EDGE_ID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_RELATIONSHIP_GUID;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask.PARAM_RELATIONSHIP_OBJECT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ClassificationTaskTest {
    @Mock private AtlasGraph graph;
    @Mock private EntityGraphMapper entityGraphMapper;
    @Mock private DeleteHandlerDelegate deleteDelegate;
    @Mock private AtlasRelationshipStore relationshipStore;
    @Mock private AtlasTask task;

    private TestClassificationTask classificationTask;
    private MockedStatic<RequestContext> requestContextMock;
    private MockedStatic<AtlasType> atlasTypeMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup basic task mocks
        when(task.getGuid()).thenReturn("test-task-guid");
        when(task.getCreatedBy()).thenReturn("testUser");
        when(task.getType()).thenReturn("TEST_CLASSIFICATION_TASK");

        classificationTask = new TestClassificationTask(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        // Mock RequestContext
        requestContextMock = mockStatic(RequestContext.class);
        RequestContext mockContext = mock(RequestContext.class);
        requestContextMock.when(() -> RequestContext.get()).thenReturn(mockContext);
        requestContextMock.when(() -> RequestContext.clear()).then(invocation -> null);
        lenient().doNothing().when(mockContext).setUser(anyString(), any());

        // Mock AtlasType
        atlasTypeMock = mockStatic(AtlasType.class);
    }

    @AfterMethod
    public void tearDown() {
        if (requestContextMock != null) {
            requestContextMock.close();
        }
        if (atlasTypeMock != null) {
            atlasTypeMock.close();
        }
    }

    @Test
    public void testPerformWithValidParameters() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);

        // Mock getStatus to return COMPLETE after setStatus is called
        when(task.getStatus()).thenReturn(COMPLETE);

        AtlasTask.Status result = classificationTask.perform();

        assertEquals(result, COMPLETE);
        verify(task).setStatus(COMPLETE);
        verify(graph).commit();
    }

    @Test
    public void testPerformWithEmptyParameters() throws Exception {
        when(task.getParameters()).thenReturn(null);

        AtlasTask.Status result = classificationTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithEmptyUserName() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn("");

        AtlasTask.Status result = classificationTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithNullUserName() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn(null);

        AtlasTask.Status result = classificationTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithException() throws Exception {
        Map<String, Object> params = createValidTaskParameters();
        when(task.getParameters()).thenReturn(params);

        classificationTask.setShouldThrowException(true);

        expectThrows(RuntimeException.class, () -> {
            try {
                classificationTask.perform();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        verify(task).setStatus(FAILED);
        verify(graph).commit();
    }

    @Test
    public void testToParametersWithEntityAndClassificationInfo() {
        String entityGuid = "entity-guid";
        String classificationVertexId = "classification-vertex-id";
        String relationshipGuid = "relationship-guid";
        String classificationName = "TestClassification";

        Map<String, Object> result = ClassificationTask.toParameters(entityGuid, classificationVertexId, relationshipGuid, classificationName);
        assertNotNull(result);
        assertEquals(result.get(PARAM_ENTITY_GUID), entityGuid);
        assertEquals(result.get(PARAM_CLASSIFICATION_VERTEX_ID), classificationVertexId);
        assertEquals(result.get(PARAM_RELATIONSHIP_GUID), relationshipGuid);
        assertEquals(result.get(PARAM_CLASSIFICATION_NAME), classificationName);
    }

    @Test
    public void testToParametersWithRelationshipEdgeAndObject() {
        String relationshipEdgeId = "relationship-edge-id";
        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setGuid("rel-guid");

        String relationshipJson = "{\"guid\":\"rel-guid\"}";
        atlasTypeMock.when(() -> AtlasType.toJson(relationship)).thenReturn(relationshipJson);

        Map<String, Object> result = ClassificationTask.toParameters(relationshipEdgeId, relationship);

        assertNotNull(result);
        assertEquals(result.get(PARAM_RELATIONSHIP_EDGE_ID), relationshipEdgeId);
        assertEquals(result.get(PARAM_RELATIONSHIP_OBJECT), relationshipJson);
    }

    @Test
    public void testSetStatusForRelationshipUpdateTask() throws Exception {
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE);
        Map<String, Object> params = new HashMap<>();
        params.put(PARAM_RELATIONSHIP_EDGE_ID, "edge-id");
        when(task.getParameters()).thenReturn(params);

        doNothing().when(entityGraphMapper).removePendingTaskFromEdge(anyString(), anyString());

        invokeSetStatusMethod(COMPLETE);

        verify(entityGraphMapper).removePendingTaskFromEdge(eq("edge-id"), eq("test-task-guid"));
        verify(entityGraphMapper, never()).removePendingTaskFromEntity(anyString(), anyString());
    }

    @Test
    public void testSetStatusForEntityTask() throws Exception {
        when(task.getType()).thenReturn("OTHER_TASK_TYPE");
        Map<String, Object> params = new HashMap<>();
        params.put(PARAM_ENTITY_GUID, "entity-guid");
        when(task.getParameters()).thenReturn(params);

        doNothing().when(entityGraphMapper).removePendingTaskFromEntity(anyString(), anyString());

        invokeSetStatusMethod(COMPLETE);

        verify(entityGraphMapper).removePendingTaskFromEntity(eq("entity-guid"), eq("test-task-guid"));
        verify(entityGraphMapper, never()).removePendingTaskFromEdge(anyString(), anyString());
    }

    @Test
    public void testSetStatusWithEntityNotFoundException() throws Exception {
        when(task.getType()).thenReturn("OTHER_TASK_TYPE");
        Map<String, Object> params = new HashMap<>();
        params.put(PARAM_ENTITY_GUID, "entity-guid");
        when(task.getParameters()).thenReturn(params);

        doThrow(new EntityNotFoundException("Entity not found"))
            .when(entityGraphMapper).removePendingTaskFromEntity(anyString(), anyString());

        invokeSetStatusMethod(COMPLETE);

        verify(entityGraphMapper).removePendingTaskFromEntity(eq("entity-guid"), eq("test-task-guid"));
    }

    @Test
    public void testSetStatusWithAtlasBaseException() throws Exception {
        when(task.getGuid()).thenReturn("test-task-guid");
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE);
        Map<String, Object> params = new HashMap<>();
        params.put(PARAM_RELATIONSHIP_EDGE_ID, "edge-id");
        when(task.getParameters()).thenReturn(params);

        doThrow(new AtlasBaseException("Atlas error"))
            .when(entityGraphMapper).removePendingTaskFromEdge(anyString(), anyString());

        invokeSetStatusMethod(COMPLETE);

        verify(entityGraphMapper).removePendingTaskFromEdge(eq("edge-id"), eq("test-task-guid"));
    }

    @Test
    public void testConstructor() {
        ClassificationTask task = new TestClassificationTask(this.task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        assertNotNull(task);
    }

    @Test
    public void testParameterConstants() {
        assertEquals(PARAM_ENTITY_GUID, "entityGuid");
        assertEquals(PARAM_CLASSIFICATION_VERTEX_ID, "classificationVertexId");
        assertEquals(PARAM_CLASSIFICATION_NAME, "classificationName");
        assertEquals(PARAM_RELATIONSHIP_GUID, "relationshipGuid");
        assertEquals(PARAM_RELATIONSHIP_OBJECT, "relationshipObject");
        assertEquals(PARAM_RELATIONSHIP_EDGE_ID, "relationshipEdgeId");
    }

    @Test
    public void testToParametersWithNullValues() {
        Map<String, Object> result = ClassificationTask.toParameters(null, null, null, null);

        assertNotNull(result);
        assertEquals(result.get(PARAM_ENTITY_GUID), null);
        assertEquals(result.get(PARAM_CLASSIFICATION_VERTEX_ID), null);
        assertEquals(result.get(PARAM_RELATIONSHIP_GUID), null);
        assertEquals(result.get(PARAM_CLASSIFICATION_NAME), null);
    }

    @Test
    public void testToParametersWithRelationshipNullValues() {
        atlasTypeMock.when(() -> AtlasType.toJson(null)).thenReturn("null");

        Map<String, Object> result = ClassificationTask.toParameters(null, null);

        assertNotNull(result);
        assertEquals(result.get(PARAM_RELATIONSHIP_EDGE_ID), null);
        assertEquals(result.get(PARAM_RELATIONSHIP_OBJECT), "null");
    }

    @Test
    public void testPerformWithEmptyParametersMap() throws Exception {
        when(task.getParameters()).thenReturn(new HashMap<>());

        AtlasTask.Status result = classificationTask.perform();

        assertEquals(result, FAILED);
    }

    // Helper methods and test implementation
    private Map<String, Object> createValidTaskParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(PARAM_ENTITY_GUID, "test-entity-guid");
        params.put(PARAM_CLASSIFICATION_VERTEX_ID, "test-classification-vertex-id");
        params.put(PARAM_RELATIONSHIP_GUID, "test-relationship-guid");
        params.put(PARAM_CLASSIFICATION_NAME, "TestClassification");
        return params;
    }

    private void invokeSetStatusMethod(AtlasTask.Status status) throws Exception {
        Method setStatusMethod = ClassificationTask.class.getDeclaredMethod("setStatus", AtlasTask.Status.class);
        setStatusMethod.setAccessible(true);
        setStatusMethod.invoke(classificationTask, status);
    }

    private <T extends Throwable> T expectThrows(Class<T> expectedType, Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " to be thrown");
        } catch (Throwable throwable) {
            if (expectedType.isInstance(throwable)) {
                return expectedType.cast(throwable);
            }
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " but got " +
                throwable.getClass().getSimpleName(), throwable);
        }
    }

    // Test implementation of ClassificationTask for testing abstract methods
    private static class TestClassificationTask extends ClassificationTask {
        private boolean shouldThrowException;

        public TestClassificationTask(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper,
                                     DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            if (shouldThrowException) {
                throw new RuntimeException("Test exception");
            }
        }

        public void setShouldThrowException(boolean shouldThrowException) {
            this.shouldThrowException = shouldThrowException;
        }
    }
}
