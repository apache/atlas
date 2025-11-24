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

import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.tasks.AbstractTask;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_ADD;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_DELETE;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ClassificationPropagateTaskFactoryTest {
    @Mock private AtlasGraph graph;
    @Mock private EntityGraphMapper entityGraphMapper;
    @Mock private DeleteHandlerDelegate deleteDelegate;
    @Mock private AtlasRelationshipStore relationshipStore;
    @Mock private AtlasTask task;

    private ClassificationPropagateTaskFactory factory;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new ClassificationPropagateTaskFactory(graph, entityGraphMapper, deleteDelegate, relationshipStore);
    }

    @Test
    public void testCreateClassificationPropagationAddTask() {
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_ADD);
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof ClassificationPropagationTasks.Add);
    }

    @Test
    public void testCreateClassificationPropagationDeleteTask() {
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_DELETE);
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof ClassificationPropagationTasks.Delete);
    }

    @Test
    public void testCreateClassificationPropagationRelationshipUpdateTask() {
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE);
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof ClassificationPropagationTasks.UpdateRelationship);
    }

    @Test
    public void testCreateUnknownTaskType() {
        when(task.getType()).thenReturn("UNKNOWN_TYPE");
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNull(result);
    }

    @Test
    public void testGetSupportedTypes() {
        List<String> supportedTypes = factory.getSupportedTypes();

        assertNotNull(supportedTypes);
        assertEquals(supportedTypes.size(), 3);
        assertTrue(supportedTypes.contains(CLASSIFICATION_PROPAGATION_ADD));
        assertTrue(supportedTypes.contains(CLASSIFICATION_PROPAGATION_DELETE));
        assertTrue(supportedTypes.contains(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE));
    }

    @Test
    public void testConstructor() {
        ClassificationPropagateTaskFactory testFactory = new ClassificationPropagateTaskFactory(graph, entityGraphMapper, deleteDelegate, relationshipStore);
        assertNotNull(testFactory);
    }

    @Test
    public void testStaticConstants() {
        assertEquals(CLASSIFICATION_PROPAGATION_ADD, "CLASSIFICATION_PROPAGATION_ADD");
        assertEquals(CLASSIFICATION_PROPAGATION_DELETE, "CLASSIFICATION_PROPAGATION_DELETE");
        assertEquals(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE, "CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE");
    }

    @Test
    public void testCreateWithNullTaskType() {
        when(task.getType()).thenReturn(null);
        when(task.getGuid()).thenReturn("test-guid");

        expectThrows(NullPointerException.class, () -> factory.create(task));
    }

    @Test
    public void testCreateWithEmptyTaskType() {
        when(task.getType()).thenReturn("");
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNull(result);
    }

    @Test
    public void testCreateWithValidTaskButNullGuid() {
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_ADD);
        when(task.getGuid()).thenReturn(null);

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof ClassificationPropagationTasks.Add);
    }

    @Test
    public void testCreateWithValidTaskButEmptyGuid() {
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_ADD);
        when(task.getGuid()).thenReturn("");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof ClassificationPropagationTasks.Add);
    }

    @Test
    public void testSupportedTypesImmutability() {
        List<String> supportedTypes1 = factory.getSupportedTypes();
        List<String> supportedTypes2 = factory.getSupportedTypes();

        // Test that the same content is returned
        assertEquals(supportedTypes1, supportedTypes2);
        assertEquals(supportedTypes1.size(), 3);
    }

    @Test
    public void testCreateAllSupportedTaskTypes() {
        // Test CLASSIFICATION_PROPAGATION_ADD
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_ADD);
        when(task.getGuid()).thenReturn("guid1");
        AbstractTask task1 = factory.create(task);
        assertNotNull(task1);
        assertTrue(task1 instanceof ClassificationPropagationTasks.Add);

        // Test CLASSIFICATION_PROPAGATION_DELETE
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_DELETE);
        when(task.getGuid()).thenReturn("guid2");
        AbstractTask task2 = factory.create(task);
        assertNotNull(task2);
        assertTrue(task2 instanceof ClassificationPropagationTasks.Delete);

        // Test CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE
        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE);
        when(task.getGuid()).thenReturn("guid3");
        AbstractTask task3 = factory.create(task);
        assertNotNull(task3);
        assertTrue(task3 instanceof ClassificationPropagationTasks.UpdateRelationship);
    }

    @Test
    public void testFactoryDependencyInjection() {
        assertNotNull(factory);

        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_ADD);
        AbstractTask addTask = factory.create(task);
        assertNotNull(addTask);

        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_DELETE);
        AbstractTask deleteTask = factory.create(task);
        assertNotNull(deleteTask);

        when(task.getType()).thenReturn(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE);
        AbstractTask updateTask = factory.create(task);
        assertNotNull(updateTask);
    }

    @Test
    public void testCreateWithCaseSensitiveTaskTypes() {
        // Test with lowercase
        when(task.getType()).thenReturn("classification_propagation_add");
        when(task.getGuid()).thenReturn("test-guid");
        AbstractTask result = factory.create(task);
        assertNull(result);

        // Test with mixed case
        when(task.getType()).thenReturn("Classification_Propagation_Add");
        AbstractTask result2 = factory.create(task);
        assertNull(result2);
    }

    @Test
    public void testAllTaskTypeConstants() {
        // Test that all expected task type constants are defined correctly
        assertEquals(CLASSIFICATION_PROPAGATION_ADD, "CLASSIFICATION_PROPAGATION_ADD");
        assertEquals(CLASSIFICATION_PROPAGATION_DELETE, "CLASSIFICATION_PROPAGATION_DELETE");
        assertEquals(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE, "CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE");
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
}
