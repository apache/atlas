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

import org.apache.atlas.exception.AtlasBaseException;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

public class ClassificationPropagationTasksTest {
    @Mock private AtlasGraph graph;
    @Mock private EntityGraphMapper entityGraphMapper;
    @Mock private DeleteHandlerDelegate deleteDelegate;
    @Mock private AtlasRelationshipStore relationshipStore;
    @Mock private AtlasTask task;

    private MockedStatic<AtlasType> atlasTypeMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup basic task mocks
        when(task.getGuid()).thenReturn("test-task-guid");
        when(task.getCreatedBy()).thenReturn("testUser");

        // Mock AtlasType
        atlasTypeMock = mockStatic(AtlasType.class);
    }

    @AfterMethod
    public void tearDown() {
        if (atlasTypeMock != null) {
            atlasTypeMock.close();
        }
    }

    @Test
    public void testAddTaskRun() throws Exception {
        ClassificationPropagationTasks.Add addTask = new ClassificationPropagationTasks.Add(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_ENTITY_GUID, "entity-guid");
        parameters.put(PARAM_CLASSIFICATION_VERTEX_ID, "classification-vertex-id");
        parameters.put(PARAM_RELATIONSHIP_GUID, "relationship-guid");

        when(entityGraphMapper.propagateClassification(anyString(), anyString(), anyString())).thenReturn(Arrays.asList("guid1"));

        invokeRunMethod(addTask, parameters);

        verify(entityGraphMapper).propagateClassification(eq("entity-guid"), eq("classification-vertex-id"), eq("relationship-guid"));
    }

    @Test
    public void testAddTaskRunWithException() throws Exception {
        ClassificationPropagationTasks.Add addTask = new ClassificationPropagationTasks.Add(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_ENTITY_GUID, "entity-guid");
        parameters.put(PARAM_CLASSIFICATION_VERTEX_ID, "classification-vertex-id");
        parameters.put(PARAM_RELATIONSHIP_GUID, "relationship-guid");

        doThrow(new AtlasBaseException("Test exception")).when(entityGraphMapper).propagateClassification(anyString(), anyString(), anyString());

        expectThrows(AtlasBaseException.class, () -> invokeRunMethod(addTask, parameters));
    }

    @Test
    public void testAddTaskRunWithNullParameters() throws Exception {
        ClassificationPropagationTasks.Add addTask = new ClassificationPropagationTasks.Add(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_ENTITY_GUID, null);
        parameters.put(PARAM_CLASSIFICATION_VERTEX_ID, null);
        parameters.put(PARAM_RELATIONSHIP_GUID, null);

        when(entityGraphMapper.propagateClassification(anyString(), anyString(), anyString())).thenReturn(Arrays.asList("guid1"));

        invokeRunMethod(addTask, parameters);

        verify(entityGraphMapper).propagateClassification(null, null, null);
    }

    @Test
    public void testDeleteTaskRun() throws Exception {
        ClassificationPropagationTasks.Delete deleteTask = new ClassificationPropagationTasks.Delete(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_ENTITY_GUID, "entity-guid");
        parameters.put(PARAM_CLASSIFICATION_VERTEX_ID, "classification-vertex-id");

        when(entityGraphMapper.deleteClassificationPropagation(anyString(), anyString())).thenReturn(Arrays.asList("guid1"));

        invokeRunMethod(deleteTask, parameters);

        verify(entityGraphMapper).deleteClassificationPropagation(eq("entity-guid"), eq("classification-vertex-id"));
    }

    @Test
    public void testDeleteTaskRunWithException() throws Exception {
        ClassificationPropagationTasks.Delete deleteTask = new ClassificationPropagationTasks.Delete(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_ENTITY_GUID, "entity-guid");
        parameters.put(PARAM_CLASSIFICATION_VERTEX_ID, "classification-vertex-id");

        doThrow(new AtlasBaseException("Test exception")).when(entityGraphMapper).deleteClassificationPropagation(anyString(), anyString());

        expectThrows(AtlasBaseException.class, () -> invokeRunMethod(deleteTask, parameters));
    }

    @Test
    public void testDeleteTaskRunWithNullParameters() throws Exception {
        ClassificationPropagationTasks.Delete deleteTask = new ClassificationPropagationTasks.Delete(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_ENTITY_GUID, null);
        parameters.put(PARAM_CLASSIFICATION_VERTEX_ID, null);

        when(entityGraphMapper.deleteClassificationPropagation(anyString(), anyString())).thenReturn(Arrays.asList("guid1"));

        invokeRunMethod(deleteTask, parameters);

        verify(entityGraphMapper).deleteClassificationPropagation(null, null);
    }

    @Test
    public void testUpdateRelationshipTaskRun() throws Exception {
        ClassificationPropagationTasks.UpdateRelationship updateTask = new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setGuid("relationship-guid");

        String relationshipJson = "{\"guid\":\"relationship-guid\"}";
        atlasTypeMock.when(() -> AtlasType.fromJson(relationshipJson, AtlasRelationship.class)).thenReturn(relationship);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_RELATIONSHIP_EDGE_ID, "relationship-edge-id");
        parameters.put(PARAM_RELATIONSHIP_OBJECT, relationshipJson);

        doNothing().when(entityGraphMapper).updateTagPropagations(anyString(), any(AtlasRelationship.class));

        invokeRunMethod(updateTask, parameters);

        verify(entityGraphMapper).updateTagPropagations(eq("relationship-edge-id"), eq(relationship));
    }

    @Test
    public void testUpdateRelationshipTaskRunWithException() throws Exception {
        ClassificationPropagationTasks.UpdateRelationship updateTask = new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setGuid("relationship-guid");

        String relationshipJson = "{\"guid\":\"relationship-guid\"}";
        atlasTypeMock.when(() -> AtlasType.fromJson(relationshipJson, AtlasRelationship.class)).thenReturn(relationship);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_RELATIONSHIP_EDGE_ID, "relationship-edge-id");
        parameters.put(PARAM_RELATIONSHIP_OBJECT, relationshipJson);

        doThrow(new AtlasBaseException("Test exception")).when(entityGraphMapper).updateTagPropagations(anyString(), any(AtlasRelationship.class));

        expectThrows(AtlasBaseException.class, () -> invokeRunMethod(updateTask, parameters));
    }

    @Test
    public void testUpdateRelationshipTaskRunWithNullParameters() throws Exception {
        ClassificationPropagationTasks.UpdateRelationship updateTask = new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        atlasTypeMock.when(() -> AtlasType.fromJson(null, AtlasRelationship.class)).thenReturn(null);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_RELATIONSHIP_EDGE_ID, null);
        parameters.put(PARAM_RELATIONSHIP_OBJECT, null);

        invokeRunMethod(updateTask, parameters);

        verify(entityGraphMapper).updateTagPropagations(null, null);
    }

    @Test
    public void testUpdateRelationshipTaskRunWithInvalidJson() throws Exception {
        ClassificationPropagationTasks.UpdateRelationship updateTask = new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        String invalidJson = "invalid-json";
        atlasTypeMock.when(() -> AtlasType.fromJson(invalidJson, AtlasRelationship.class)).thenThrow(new RuntimeException("Invalid JSON"));

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PARAM_RELATIONSHIP_EDGE_ID, "relationship-edge-id");
        parameters.put(PARAM_RELATIONSHIP_OBJECT, invalidJson);

        expectThrows(RuntimeException.class, () -> invokeRunMethod(updateTask, parameters));
    }

    @Test
    public void testAddTaskConstructor() {
        ClassificationPropagationTasks.Add addTask = new ClassificationPropagationTasks.Add(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        assertNotNull(addTask);
    }

    @Test
    public void testDeleteTaskConstructor() {
        ClassificationPropagationTasks.Delete deleteTask = new ClassificationPropagationTasks.Delete(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        assertNotNull(deleteTask);
    }

    @Test
    public void testUpdateRelationshipTaskConstructor() {
        ClassificationPropagationTasks.UpdateRelationship updateTask = new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        assertNotNull(updateTask);
    }

    @Test
    public void testAddTaskRunWithEmptyParameters() throws Exception {
        ClassificationPropagationTasks.Add addTask = new ClassificationPropagationTasks.Add(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();

        when(entityGraphMapper.propagateClassification(anyString(), anyString(), anyString())).thenReturn(Arrays.asList("guid1"));

        invokeRunMethod(addTask, parameters);

        verify(entityGraphMapper).propagateClassification(null, null, null);
    }

    @Test
    public void testDeleteTaskRunWithEmptyParameters() throws Exception {
        ClassificationPropagationTasks.Delete deleteTask = new ClassificationPropagationTasks.Delete(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();

        when(entityGraphMapper.deleteClassificationPropagation(anyString(), anyString())).thenReturn(Arrays.asList("guid1"));

        invokeRunMethod(deleteTask, parameters);

        verify(entityGraphMapper).deleteClassificationPropagation(null, null);
    }

    @Test
    public void testUpdateRelationshipTaskRunWithEmptyParameters() throws Exception {
        ClassificationPropagationTasks.UpdateRelationship updateTask = new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);

        Map<String, Object> parameters = new HashMap<>();

        atlasTypeMock.when(() -> AtlasType.fromJson(null, AtlasRelationship.class)).thenReturn(null);

        invokeRunMethod(updateTask, parameters);

        verify(entityGraphMapper).updateTagPropagations(null, null);
    }

    // Helper methods
    private void invokeRunMethod(ClassificationTask task, Map<String, Object> parameters) throws Exception {
        Method runMethod = task.getClass().getDeclaredMethod("run", Map.class);
        runMethod.setAccessible(true);
        runMethod.invoke(task, parameters);
    }

    private <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingRunnable runnable) {
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

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
