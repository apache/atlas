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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_CUSTOM;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_SWEEPOUT;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.AGING_TYPE_PROPERTY_KEY_MAP;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.ATLAS_AUDIT_REDUCTION;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.MAX_PENDING_TASKS_ALLOWED;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AuditReductionTaskFactoryTest {
    @Mock private EntityAuditRepository auditRepository;
    @Mock private AtlasGraph graph;
    @Mock private AtlasDiscoveryService discoveryService;
    @Mock private AtlasTypeRegistry typeRegistry;
    @Mock private AtlasTask task;

    private AuditReductionTaskFactory factory;
    private MockedStatic<ApplicationProperties> applicationPropertiesMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new AuditReductionTaskFactory(auditRepository, graph, discoveryService, typeRegistry);
    }

    @AfterMethod
    public void tearDown() {
        if (applicationPropertiesMock != null) {
            applicationPropertiesMock.close();
        }
    }

    @Test
    public void testCreateAuditReductionTask() {
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION);
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof AuditReductionTask);
    }

    @Test
    public void testCreateAuditReductionEntityRetrievalTask() {
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL);
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof AuditReductionEntityRetrievalTask);
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
        assertEquals(supportedTypes.size(), 2);
        assertTrue(supportedTypes.contains(ATLAS_AUDIT_REDUCTION));
        assertTrue(supportedTypes.contains(ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL));
    }

    @Test
    public void testConstructor() {
        AuditReductionTaskFactory testFactory = new AuditReductionTaskFactory(auditRepository, graph, discoveryService, typeRegistry);
        assertNotNull(testFactory);
    }

    @Test
    public void testStaticConstants() {
        assertEquals(ATLAS_AUDIT_REDUCTION, "ATLAS_AUDIT_REDUCTION");
        assertEquals(ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL, "AUDIT_REDUCTION_ENTITY_RETRIEVAL");
        assertTrue(MAX_PENDING_TASKS_ALLOWED > 0);
    }

    @Test
    public void testAgingTypePropertyKeyMap() {
        assertNotNull(AGING_TYPE_PROPERTY_KEY_MAP);
        assertEquals(AGING_TYPE_PROPERTY_KEY_MAP.size(), 3);
        assertEquals(AGING_TYPE_PROPERTY_KEY_MAP.get(AtlasAuditAgingType.DEFAULT), PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT);
        assertEquals(AGING_TYPE_PROPERTY_KEY_MAP.get(AtlasAuditAgingType.SWEEP), PROPERTY_KEY_GUIDS_TO_SWEEPOUT);
        assertEquals(AGING_TYPE_PROPERTY_KEY_MAP.get(AtlasAuditAgingType.CUSTOM), PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_CUSTOM);
    }

    @Test
    public void testStaticInitializationWithValidConfiguration() throws Exception {
        // Test static initialization by accessing static fields
        Field maxPendingTasksField = AuditReductionTaskFactory.class.getDeclaredField("MAX_PENDING_TASKS_ALLOWED");
        maxPendingTasksField.setAccessible(true);
        int maxPendingTasks = (int) maxPendingTasksField.get(null);

        assertTrue(maxPendingTasks > 0);
    }

    @Test
    public void testStaticInitializationAgingTypePropertyKeyMap() throws Exception {
        Field agingTypeMapField = AuditReductionTaskFactory.class.getDeclaredField("AGING_TYPE_PROPERTY_KEY_MAP");
        agingTypeMapField.setAccessible(true);
        Map<AtlasAuditAgingType, String> map = (Map<AtlasAuditAgingType, String>) agingTypeMapField.get(null);

        assertNotNull(map);
        assertEquals(map.size(), 3);
        assertTrue(map.containsKey(AtlasAuditAgingType.DEFAULT));
        assertTrue(map.containsKey(AtlasAuditAgingType.SWEEP));
        assertTrue(map.containsKey(AtlasAuditAgingType.CUSTOM));
    }

    @Test
    public void testAllTaskTypeConstants() {
        // Test that all expected task type constants are defined
        assertEquals(ATLAS_AUDIT_REDUCTION, "ATLAS_AUDIT_REDUCTION");
        assertEquals(ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL, "AUDIT_REDUCTION_ENTITY_RETRIEVAL");
    }

    @Test
    public void testFactoryCreateWithNullTaskType() {
        when(task.getType()).thenReturn(null);
        when(task.getGuid()).thenReturn("test-guid");

        // This will cause NPE in switch statement since null.equals() will fail
        expectThrows(NullPointerException.class, () -> factory.create(task));
    }

    @Test
    public void testFactoryCreateWithEmptyTaskType() {
        when(task.getType()).thenReturn("");
        when(task.getGuid()).thenReturn("test-guid");

        AbstractTask result = factory.create(task);

        assertNull(result);
    }

    @Test
    public void testFactoryCreateWithValidTaskButNullGuid() {
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION);
        when(task.getGuid()).thenReturn(null);

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof AuditReductionTask);
    }

    @Test
    public void testFactoryCreateWithValidTaskButEmptyGuid() {
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION);
        when(task.getGuid()).thenReturn("");

        AbstractTask result = factory.create(task);

        assertNotNull(result);
        assertTrue(result instanceof AuditReductionTask);
    }

    @Test
    public void testSupportedTypesImmutability() {
        List<String> supportedTypes1 = factory.getSupportedTypes();
        List<String> supportedTypes2 = factory.getSupportedTypes();

        // Test that the same instance is returned (or at least same content)
        assertEquals(supportedTypes1, supportedTypes2);
        assertEquals(supportedTypes1.size(), 2);
    }

    @Test
    public void testCreateBothSupportedTaskTypes() {
        // Test ATLAS_AUDIT_REDUCTION
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION);
        when(task.getGuid()).thenReturn("guid1");
        AbstractTask task1 = factory.create(task);
        assertNotNull(task1);
        assertTrue(task1 instanceof AuditReductionTask);

        // Test ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL);
        when(task.getGuid()).thenReturn("guid2");
        AbstractTask task2 = factory.create(task);
        assertNotNull(task2);
        assertTrue(task2 instanceof AuditReductionEntityRetrievalTask);
    }

    @Test
    public void testFactoryDependencyInjection() {
        // Test that all dependencies are properly injected
        assertNotNull(factory);

        // Create tasks to ensure dependencies are used
        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION);
        AbstractTask auditTask = factory.create(task);
        assertNotNull(auditTask);

        when(task.getType()).thenReturn(ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL);
        AbstractTask retrievalTask = factory.create(task);
        assertNotNull(retrievalTask);
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
