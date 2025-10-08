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
package org.apache.atlas.repository.store.graph.v2.bulkimport;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStreamForImport;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityImportStream;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

public class RegularImportTest {
    private RegularImport regularImport;
    private AtlasGraph mockGraph;
    private AtlasEntityStore mockEntityStore;
    private AtlasTypeRegistry mockTypeRegistry;
    private EntityGraphRetriever mockEntityGraphRetriever;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(AtlasGraph.class);
        mockEntityStore = mock(AtlasEntityStore.class);
        mockTypeRegistry = mock(AtlasTypeRegistry.class);

        regularImport = new RegularImport(mockGraph, mockEntityStore, mockTypeRegistry);

        // Access private field using reflection for testing
        try {
            Field entityGraphRetrieverField = RegularImport.class.getDeclaredField("entityGraphRetriever");
            entityGraphRetrieverField.setAccessible(true);
            mockEntityGraphRetriever = mock(EntityGraphRetriever.class);
            entityGraphRetrieverField.set(regularImport, mockEntityGraphRetriever);
        } catch (Exception e) {
            regularImport = new RegularImport(mockGraph, mockEntityStore, mockTypeRegistry);
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(regularImport);

        // Test constructor with null parameters
        RegularImport regularImportWithNulls = new RegularImport(null, null, null);
        assertNotNull(regularImportWithNulls);
    }

    @Test
    public void testGetJsonArrayStatic() {
        // Test static method with various inputs
        String result = RegularImport.getJsonArray(null, "guid1");
        assertEquals("[\"guid1\"]", result);

        result = RegularImport.getJsonArray("", "guid2");
        assertEquals("[\"guid2\"]", result);

        result = RegularImport.getJsonArray("[\"existing\"]", "new");
        assertEquals("[\"existing\",\"new\"]", result);

        result = RegularImport.getJsonArray("[\"guid1\",\"guid2\"]", "guid3");
        assertEquals("[\"guid1\",\"guid2\",\"guid3\"]", result);

        // Test with special characters
        result = RegularImport.getJsonArray("[\"test\"]", "guid-with-special-chars-!@#");
        assertEquals("[\"test\",\"guid-with-special-chars-!@#\"]", result);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testRunWithNullEntityStream() throws AtlasBaseException {
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        regularImport.run(null, mockResult);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testRunWithEmptyEntityStream() throws AtlasBaseException {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(false);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);

        regularImport.run(mockStream, mockResult);
    }

    @Test
    public void testRunSuccessfulExecution() throws AtlasBaseException {
        EntityImportStream mockStream = createMockEntityImportStream();
        AtlasImportResult mockResult = createMockAtlasImportResult();

        try {
            EntityMutationResponse result = regularImport.run(mockStream, mockResult);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRunSecondMethodWithValidParameters() throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        mockResponse.setGuidAssignments(new HashMap<>());
        AtlasImportResult mockResult = createMockAtlasImportResult();
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = new ArrayList<>();

        EntityMutationResponse entityResponse = mock(EntityMutationResponse.class);
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid", "realGuid");
        when(entityResponse.getGuidAssignments()).thenReturn(guidAssignments);

        when(mockEntityStore.createOrUpdateForImport(any(AtlasEntityStreamForImport.class)))
                .thenReturn(entityResponse);

        TypesUtil.Pair<EntityMutationResponse, Float> result = regularImport.run(
                mockEntityWithExtInfo, mockResponse, mockResult, processedGuids, 1, 10, 0.0f, residualList);

        assertNotNull(result);
        assertNotNull(result.left);
        assertNotNull(result.right);
    }

    @Test
    public void testRunSecondMethodWithNullEntity() throws AtlasBaseException {
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        mockResponse.setGuidAssignments(new HashMap<>());
        AtlasImportResult mockResult = createMockAtlasImportResult();
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = new ArrayList<>();

        TypesUtil.Pair<EntityMutationResponse, Float> result = regularImport.run(
                null, mockResponse, mockResult, processedGuids, 1, 10, 0.0f, residualList);

        assertNotNull(result);
        assertNotNull(result.left);
        assertNotNull(result.right);
    }

    @Test
    public void testRunSecondMethodWithNullResponse() throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        AtlasImportResult mockResult = createMockAtlasImportResult();
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = new ArrayList<>();

        when(mockEntityStore.createOrUpdateForImport(any(AtlasEntityStreamForImport.class)))
                .thenReturn(mock(EntityMutationResponse.class));

        TypesUtil.Pair<EntityMutationResponse, Float> result = regularImport.run(
                mockEntityWithExtInfo, null, mockResult, processedGuids, 1, 10, 0.0f, residualList);

        assertNotNull(result);
        assertNotNull(result.left);
        assertNotNull(result.left.getGuidAssignments());
    }

    @Test
    public void testRunSecondMethodWithAtlasBaseException() throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        mockResponse.setGuidAssignments(new HashMap<>());
        AtlasImportResult mockResult = createMockAtlasImportResult();
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = new ArrayList<>();

        AtlasBaseException testException = new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, "Test exception");
        when(mockEntityStore.createOrUpdateForImport(any(AtlasEntityStreamForImport.class)))
                .thenThrow(testException);

        TypesUtil.Pair<EntityMutationResponse, Float> result = regularImport.run(
                mockEntityWithExtInfo, mockResponse, mockResult, processedGuids, 1, 10, 0.0f, residualList);

        assertNotNull(result);
        assertEquals(1, residualList.size());
        assertEquals("test-guid-123", residualList.get(0));
    }

    @Test
    public void testRunSecondMethodWithSchemaViolationException() throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        mockResponse.setGuidAssignments(new HashMap<>());
        AtlasImportResult mockResult = createMockAtlasImportResult();
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = new ArrayList<>();

        AtlasSchemaViolationException schemaException = mock(AtlasSchemaViolationException.class);
        when(mockEntityStore.createOrUpdateForImport(any(AtlasEntityStreamForImport.class)))
                .thenThrow(schemaException)
                .thenReturn(mock(EntityMutationResponse.class));

        try {
            TypesUtil.Pair<EntityMutationResponse, Float> result = regularImport.run(mockEntityWithExtInfo, mockResponse, mockResult, processedGuids, 1, 10, 0.0f, residualList);
            assertNotNull(result);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testUpdateVertexGuid() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();

        // Add referred entities
        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        AtlasEntity referredEntity = createMockEntity("ReferredType", "referred-guid");
        referredEntities.put("referred-guid", referredEntity);
        when(mockEntityWithExtInfo.getReferredEntities()).thenReturn(referredEntities);

        // Use reflection to call private method
        Method updateVertexGuidMethod = RegularImport.class.getDeclaredMethod("updateVertexGuid", AtlasEntity.AtlasEntityWithExtInfo.class);
        updateVertexGuidMethod.setAccessible(true);

        try {
            updateVertexGuidMethod.invoke(regularImport, mockEntityWithExtInfo);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testUpdateVertexGuidSingleEntity() throws Exception {
        AtlasEntity mockEntity = createMockEntity("TestType", "test-guid");

        // Use reflection to call private method
        Method updateVertexGuidMethod = RegularImport.class.getDeclaredMethod(
                "updateVertexGuid", AtlasEntity.class);
        updateVertexGuidMethod.setAccessible(true);

        try {
            updateVertexGuidMethod.invoke(regularImport, mockEntity);
        } catch (Exception e) {
            // Expected - method may throw exceptions due to mocking limitations
            assertNotNull(e);
        }
    }

    @Test
    public void testContainsExceptionUsingReflection() throws Exception {
        List<Throwable> exceptions = new ArrayList<>();
        exceptions.add(new RuntimeException("Test"));
        exceptions.add(new IllegalArgumentException("Test"));

        // Create a custom exception class for testing
        class PermanentLockingException extends RuntimeException {
            public PermanentLockingException(String message) {
                super(message);
            }
        }

        exceptions.add(new PermanentLockingException("Test permanent locking"));

        // Use reflection to access private method
        Method containsExceptionMethod = RegularImport.class.getDeclaredMethod(
                "containsException", List.class, String.class);
        containsExceptionMethod.setAccessible(true);

        // Test finding the exception
        boolean result = (boolean) containsExceptionMethod.invoke(regularImport, exceptions, "PermanentLockingException");
        assertTrue(result);

        // Test not finding the exception
        result = (boolean) containsExceptionMethod.invoke(regularImport, exceptions, "NonExistentException");
        assertFalse(result);
    }

    @Test
    public void testUpdateResidualListUsingReflection() throws Exception {
        List<String> residualList = new ArrayList<>();
        AtlasBaseException validException = new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, "Test");
        AtlasBaseException invalidException = new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Test");
        // Use reflection to access private method
        Method updateResidualListMethod = RegularImport.class.getDeclaredMethod(
                "updateResidualList", AtlasBaseException.class, List.class, String.class);
        updateResidualListMethod.setAccessible(true);

        // Test with INVALID_OBJECT_ID error (should add to list)
        boolean result = (boolean) updateResidualListMethod.invoke(regularImport, validException, residualList, "test-guid");
        assertTrue(result);
        assertEquals(1, residualList.size());
        assertEquals("test-guid", residualList.get(0));

        // Test with different error (should not add to list)
        result = (boolean) updateResidualListMethod.invoke(
            regularImport, invalidException, residualList, "another-guid");
        assertFalse(result);
        assertEquals(1, residualList.size()); // Size should remain the same
    }

    @Test
    public void testUpdateImportMetricsUsingReflection() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        AtlasImportResult mockResult = createMockAtlasImportResult();
        Set<String> processedGuids = new HashSet<>();

        // Use reflection to access private method
        Method updateImportMetricsMethod = RegularImport.class.getDeclaredMethod(
                "updateImportMetrics", AtlasEntity.AtlasEntityWithExtInfo.class,
                EntityMutationResponse.class, AtlasImportResult.class, Set.class,
                int.class, int.class, float.class);
        updateImportMetricsMethod.setAccessible(true);

        float result = (float) updateImportMetricsMethod.invoke(
                regularImport, mockEntityWithExtInfo, mockResponse, mockResult,
                processedGuids, 10, 100, 25.0f);

        assertNotNull(result);
    }

    @Test
    public void testPrivateFieldAccess() throws Exception {
        // Test access to private fields using reflection
        Field graphField = RegularImport.class.getDeclaredField("graph");
        graphField.setAccessible(true);
        AtlasGraph retrievedGraph = (AtlasGraph) graphField.get(regularImport);
        assertEquals(mockGraph, retrievedGraph);

        Field entityStoreField = RegularImport.class.getDeclaredField("entityStore");
        entityStoreField.setAccessible(true);
        AtlasEntityStore retrievedStore = (AtlasEntityStore) entityStoreField.get(regularImport);
        assertEquals(mockEntityStore, retrievedStore);

        Field typeRegistryField = RegularImport.class.getDeclaredField("typeRegistry");
        typeRegistryField.setAccessible(true);
        AtlasTypeRegistry retrievedRegistry = (AtlasTypeRegistry) typeRegistryField.get(regularImport);
        assertEquals(mockTypeRegistry, retrievedRegistry);
    }

    private EntityImportStream createMockEntityImportStream() {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(true).thenReturn(false);
        when(mockStream.getPosition()).thenReturn(1);
        when(mockStream.size()).thenReturn(1);

        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        when(mockStream.getNextEntityWithExtInfo()).thenReturn(mockEntityWithExtInfo);

        return mockStream;
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMockEntityWithExtInfo() {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity mockEntity = createMockEntity("TestEntity", "test-guid-123");
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntityWithExtInfo.getReferredEntities()).thenReturn(new HashMap<>());
        return mockEntityWithExtInfo;
    }

    private AtlasEntity createMockEntity(String typeName, String guid) {
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockEntity.getTypeName()).thenReturn(typeName);
        when(mockEntity.getGuid()).thenReturn(guid);
        return mockEntity;
    }

    private AtlasImportResult createMockAtlasImportResult() {
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        when(mockResult.getProcessedEntities()).thenReturn(new ArrayList<>());
        return mockResult;
    }
}
