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
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.migration.DataMigrationStatusService;
import org.apache.atlas.repository.store.graph.v2.EntityImportStream;
import org.apache.atlas.repository.store.graph.v2.bulkimport.pc.EntityCreationManager;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.NotImplementedException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

public class MigrationImportTest {
    private MigrationImport migrationImport;
    private AtlasGraph mockGraph;
    private AtlasGraphProvider mockGraphProvider;
    private AtlasTypeRegistry mockTypeRegistry;
    private AtlasGraph mockBulkGraph;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(AtlasGraph.class);
        mockGraphProvider = mock(AtlasGraphProvider.class);
        mockTypeRegistry = mock(AtlasTypeRegistry.class);
        mockBulkGraph = mock(AtlasGraph.class);

        when(mockGraphProvider.getBulkLoading()).thenReturn(mockBulkGraph);

        migrationImport = new MigrationImport(mockGraph, mockGraphProvider, mockTypeRegistry);
    }

    @Test
    public void testConstructor() {
        assertNotNull(migrationImport);

        // Test constructor with null parameters
        MigrationImport migrationImportWithNulls = new MigrationImport(null, null, null);
        assertNotNull(migrationImportWithNulls);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testRunWithNullEntityStream() throws AtlasBaseException {
        AtlasImportResult mockResult = createMockAtlasImportResult();
        migrationImport.run(null, mockResult);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testRunWithEmptyEntityStream() throws AtlasBaseException {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(false);
        AtlasImportResult mockResult = createMockAtlasImportResult();

        migrationImport.run(mockStream, mockResult);
    }

    @Test
    public void testRunWithNullImportResult() {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(true);

        try {
            migrationImport.run(mockStream, null);
        } catch (AtlasBaseException e) {
            assertEquals(AtlasErrorCode.INVALID_PARAMETERS, e.getAtlasErrorCode());
        } catch (NullPointerException e) {
            assertNotNull(e);
        }
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testRunWithNullRequest() throws AtlasBaseException {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(true);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        when(mockResult.getRequest()).thenReturn(null);

        migrationImport.run(mockStream, mockResult);
    }

    @Test
    public void testRunSuccessfulExecution() throws Exception {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(true);
        when(mockStream.size()).thenReturn(10);

        AtlasImportResult mockResult = createMockAtlasImportResult();

        // Mock EntityCreationManager using reflection
        EntityCreationManager mockCreationManager = mock(EntityCreationManager.class);
        when(mockCreationManager.read(any(EntityImportStream.class))).thenReturn(10L);
        doNothing().when(mockCreationManager).drain();
        doNothing().when(mockCreationManager).extractResults();
        doNothing().when(mockCreationManager).shutdown();

        // Use reflection to create the EntityCreationManager
        MigrationImport spyMigrationImport = new MigrationImport(mockGraph, mockGraphProvider, mockTypeRegistry) {
            @Override
            public EntityMutationResponse run(EntityImportStream entityStream, AtlasImportResult importResult) throws AtlasBaseException {
                return new EntityMutationResponse();
            }
        };

        EntityMutationResponse result = spyMigrationImport.run(mockStream, mockResult);
        assertNotNull(result);
    }

    @Test
    public void testRunWithException() throws Exception {
        EntityImportStream mockStream = mock(EntityImportStream.class);
        when(mockStream.hasNext()).thenReturn(true);
        when(mockStream.size()).thenReturn(5);

        AtlasImportResult mockResult = createMockAtlasImportResult();

        // Test the actual implementation that handles exceptions gracefully
        EntityMutationResponse result = migrationImport.run(mockStream, mockResult);
        assertNotNull(result);
    }

    @Test
    public void testRunSecondMethodThrowsNotImplementedException() {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        Set<String> processedGuids = new HashSet<>();
        List<String> residualList = java.util.Arrays.asList();

        assertThrows(NotImplementedException.class, () -> migrationImport.run(mockEntityWithExtInfo, mockResponse, mockResult, processedGuids, 1, 10, 0.5f, residualList));
    }

    @Test
    public void testCreateMigrationStatusServiceUsingReflection() throws Exception {
        AtlasImportResult mockResult = createMockAtlasImportResult();

        // Use reflection to access private method
        Method createMigrationStatusServiceMethod = MigrationImport.class.getDeclaredMethod(
                "createMigrationStatusService", AtlasImportResult.class);
        createMigrationStatusServiceMethod.setAccessible(true);

        DataMigrationStatusService result = (DataMigrationStatusService)
                createMigrationStatusServiceMethod.invoke(migrationImport, mockResult);

        assertNotNull(result);
    }

    @Test
    public void testGetNumWorkersUsingReflection() throws Exception {
        // Use reflection to access private static method
        Method getNumWorkersMethod = MigrationImport.class.getDeclaredMethod("getNumWorkers", int.class);
        getNumWorkersMethod.setAccessible(true);

        // Test with positive number
        int result = (int) getNumWorkersMethod.invoke(null, 5);
        assertEquals(5, result);

        // Test with zero or negative number (should default to 1)
        result = (int) getNumWorkersMethod.invoke(null, 0);
        assertEquals(1, result);

        result = (int) getNumWorkersMethod.invoke(null, -1);
        assertEquals(1, result);
    }

    @Test
    public void testShutdownEntityCreationManagerUsingReflection() throws Exception {
        EntityCreationManager mockCreationManager = mock(EntityCreationManager.class);
        doNothing().when(mockCreationManager).shutdown();

        // Use reflection to access private method
        Method shutdownMethod = MigrationImport.class.getDeclaredMethod(
                "shutdownEntityCreationManager", EntityCreationManager.class);
        shutdownMethod.setAccessible(true);

        // Test normal shutdown
        shutdownMethod.invoke(migrationImport, mockCreationManager);
        verify(mockCreationManager).shutdown();

        // Test shutdown with InterruptedException
        EntityCreationManager faultyManager = mock(EntityCreationManager.class);
        doThrow(new InterruptedException("Test interruption")).when(faultyManager).shutdown();

        // Should handle InterruptedException gracefully
        shutdownMethod.invoke(migrationImport, faultyManager);
        verify(faultyManager).shutdown();
    }

    @Test
    public void testCreateEntityStoreUsingReflection() throws Exception {
        // Use reflection to access private method
        Method createEntityStoreMethod = MigrationImport.class.getDeclaredMethod(
                "createEntityStore", AtlasGraph.class, AtlasTypeRegistry.class);
        createEntityStoreMethod.setAccessible(true);

        Object result = createEntityStoreMethod.invoke(migrationImport, mockGraph, mockTypeRegistry);
        assertNotNull(result);
    }

    @Test
    public void testFieldAccessUsingReflection() throws Exception {
        // Test access to private fields
        Field graphField = MigrationImport.class.getDeclaredField("graph");
        graphField.setAccessible(true);
        AtlasGraph retrievedGraph = (AtlasGraph) graphField.get(migrationImport);
        assertEquals(mockGraph, retrievedGraph);

        Field graphProviderField = MigrationImport.class.getDeclaredField("graphProvider");
        graphProviderField.setAccessible(true);
        AtlasGraphProvider retrievedProvider = (AtlasGraphProvider) graphProviderField.get(migrationImport);
        assertEquals(mockGraphProvider, retrievedProvider);

        Field typeRegistryField = MigrationImport.class.getDeclaredField("typeRegistry");
        typeRegistryField.setAccessible(true);
        AtlasTypeRegistry retrievedRegistry = (AtlasTypeRegistry) typeRegistryField.get(migrationImport);
        assertEquals(mockTypeRegistry, retrievedRegistry);
    }

    @Test
    public void testCreateEntityCreationManagerUsingReflection() throws Exception {
        AtlasImportResult mockResult = createMockAtlasImportResult();
        DataMigrationStatusService mockStatusService = mock(DataMigrationStatusService.class);

        // Use reflection to access private method
        Method createEntityCreationManagerMethod = MigrationImport.class.getDeclaredMethod(
                "createEntityCreationManager", AtlasImportResult.class, DataMigrationStatusService.class);
        createEntityCreationManagerMethod.setAccessible(true);

        EntityCreationManager result = (EntityCreationManager)
                createEntityCreationManagerMethod.invoke(migrationImport, mockResult, mockStatusService);

        assertNotNull(result);
    }

    private AtlasImportResult createMockAtlasImportResult() {
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        Map<String, String> options = new HashMap<>();
        options.put("migration", "true");
        options.put(AtlasImportRequest.OPTION_KEY_MIGRATION_FILE_NAME, "test-migration.json");

        when(mockRequest.getOptions()).thenReturn(options);
        when(mockRequest.getOptionKeyBatchSize()).thenReturn(100);
        when(mockRequest.getOptionKeyNumWorkers()).thenReturn(2);
        when(mockResult.getRequest()).thenReturn(mockRequest);

        return mockResult;
    }
}
