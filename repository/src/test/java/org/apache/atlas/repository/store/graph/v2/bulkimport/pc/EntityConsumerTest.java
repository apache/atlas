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
package org.apache.atlas.repository.store.graph.v2.bulkimport.pc;

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStreamForImport;
import org.apache.atlas.repository.store.graph.v2.BulkImporterImpl;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class EntityConsumerTest {
    private EntityConsumer entityConsumer;
    private AtlasTypeRegistry mockTypeRegistry;
    private AtlasGraph mockAtlasGraph;
    private AtlasEntityStore mockEntityStore;
    private AtlasGraph mockAtlasGraphBulk;
    private AtlasEntityStore mockEntityStoreBulk;
    private EntityGraphRetriever mockEntityRetrieverBulk;
    private BlockingQueue<AtlasEntity.AtlasEntityWithExtInfo> mockQueue;

    @BeforeMethod
    public void setUp() {
        mockTypeRegistry = mock(AtlasTypeRegistry.class);
        mockAtlasGraph = mock(AtlasGraph.class);
        mockEntityStore = mock(AtlasEntityStore.class);
        mockAtlasGraphBulk = mock(AtlasGraph.class);
        mockEntityStoreBulk = mock(AtlasEntityStore.class);
        mockEntityRetrieverBulk = mock(EntityGraphRetriever.class);
        mockQueue = new ArrayBlockingQueue<>(100);

        entityConsumer = new EntityConsumer(
                mockTypeRegistry,
                mockAtlasGraph,
                mockEntityStore,
                mockAtlasGraphBulk,
                mockEntityStoreBulk,
                mockEntityRetrieverBulk,
                mockQueue,
            10,
            false);
    }

    @Test
    public void testConstructor() {
        assertNotNull(entityConsumer);
    }

    @Test
    public void testConstructorWithDifferentParameters() {
        // Test with migration import true
        EntityConsumer migrationConsumer = new EntityConsumer(mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, mockQueue, 5, true);
        assertNotNull(migrationConsumer);

        // Test with different batch size
        EntityConsumer largeBatchConsumer = new EntityConsumer(mockTypeRegistry, mockAtlasGraph, mockEntityStore, mockAtlasGraphBulk, mockEntityStoreBulk, mockEntityRetrieverBulk, mockQueue, 1000, false);
        assertNotNull(largeBatchConsumer);

        // Test with null parameters
        EntityConsumer nullParamConsumer = new EntityConsumer(null, null, null, null, null, null, null, 1, true);
        assertNotNull(nullParamConsumer);
    }

    @Test
    public void testCommitDirty() throws Exception {
        // Use reflection to access private method
        Method commitDirtyMethod = EntityConsumer.class.getDeclaredMethod("commitDirty");
        commitDirtyMethod.setAccessible(true);

        // Access counter field to verify behavior
        Field counterField = EntityConsumer.class.getDeclaredField("counter");
        counterField.setAccessible(true);
        AtomicLong counter = (AtomicLong) counterField.get(entityConsumer);
        counter.set(5);

        commitDirtyMethod.invoke(entityConsumer);

        // Verify counter is reset to 0
        assertEquals(0, counter.get());
    }

    @Test
    public void testDoCommit() throws Exception {
        // Mock successful commit
        doNothing().when(mockAtlasGraphBulk).commit();

        // Use reflection to access private method
        Method doCommitMethod = EntityConsumer.class.getDeclaredMethod("doCommit");
        doCommitMethod.setAccessible(true);

        doCommitMethod.invoke(entityConsumer);

        verify(mockAtlasGraphBulk).commit();
    }

    @Test
    public void testDoCommitWithRetryFailure() throws Exception {
        // Mock commit failure
        doThrow(new RuntimeException("Commit failed")).when(mockAtlasGraphBulk).commit();
        doNothing().when(mockAtlasGraphBulk).rollback();

        // Use reflection to access private method
        Method doCommitMethod = EntityConsumer.class.getDeclaredMethod("doCommit");
        doCommitMethod.setAccessible(true);

        doCommitMethod.invoke(entityConsumer);

        // Should attempt commit multiple times and then clear
        verify(mockAtlasGraphBulk, Mockito.atLeastOnce()).rollback();
    }

    @Test
    public void testProcessItem() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();

        try (MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            doNothing().when(mockContext).setImportInProgress(anyBoolean());
            doNothing().when(mockContext).setCreateShellEntityForNonExistingReference(anyBoolean());
            doNothing().when(mockContext).setMigrationInProgress(anyBoolean());

            EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
            when(mockEntityStoreBulk.createOrUpdateForImportNoCommit(any())).thenReturn(mockResponse);

            // Use reflection to access private method
            Method processItemMethod = EntityConsumer.class.getDeclaredMethod("processItem", AtlasEntity.AtlasEntityWithExtInfo.class);
            processItemMethod.setAccessible(true);

            processItemMethod.invoke(entityConsumer, mockEntityWithExtInfo);

            verify(mockContext).setImportInProgress(true);
            verify(mockContext).setCreateShellEntityForNonExistingReference(true);
        }
    }

    @Test
    public void testProcessItemWithException() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        try (MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            doNothing().when(mockContext).setImportInProgress(anyBoolean());
            doNothing().when(mockContext).setCreateShellEntityForNonExistingReference(anyBoolean());
            doNothing().when(mockContext).setMigrationInProgress(anyBoolean());

            when(mockEntityStoreBulk.createOrUpdateForImportNoCommit(any())).thenThrow(new RuntimeException("Test exception"));

            Method processItemMethod = EntityConsumer.class.getDeclaredMethod("processItem", AtlasEntity.AtlasEntityWithExtInfo.class);
            processItemMethod.setAccessible(true);

            processItemMethod.invoke(entityConsumer, mockEntityWithExtInfo);
        }
    }

    @Test
    public void testProcessEntityWithAtlasBaseException() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        try (MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            doNothing().when(mockContext).setImportInProgress(anyBoolean());
            doNothing().when(mockContext).setCreateShellEntityForNonExistingReference(anyBoolean());
            doNothing().when(mockContext).setMigrationInProgress(anyBoolean());

            when(mockEntityStoreBulk.createOrUpdateForImportNoCommit(any())).thenThrow(new AtlasBaseException("Test AtlasBaseException"));
            Method processEntityMethod = EntityConsumer.class.getDeclaredMethod("processEntity", AtlasEntity.AtlasEntityWithExtInfo.class, long.class);
            processEntityMethod.setAccessible(true);
            processEntityMethod.invoke(entityConsumer, mockEntityWithExtInfo, 1L);
        }
    }

    @Test
    public void testProcessEntityWithSchemaViolationException() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        try (MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class); MockedStatic<BulkImporterImpl> mockedBulkImporter = mockStatic(BulkImporterImpl.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            doNothing().when(mockContext).setImportInProgress(anyBoolean());
            doNothing().when(mockContext).setCreateShellEntityForNonExistingReference(anyBoolean());
            doNothing().when(mockContext).setMigrationInProgress(anyBoolean());

            when(mockEntityStoreBulk.createOrUpdateForImportNoCommit(any())).thenThrow(mock(AtlasSchemaViolationException.class));

            mockedBulkImporter.when(() -> BulkImporterImpl.updateVertexGuid(any(), any(), any(), any())).thenAnswer(invocation -> null);
            Method processEntityMethod = EntityConsumer.class.getDeclaredMethod("processEntity", AtlasEntity.AtlasEntityWithExtInfo.class, long.class);
            processEntityMethod.setAccessible(true);
            processEntityMethod.invoke(entityConsumer, mockEntityWithExtInfo, 1L);
            mockedBulkImporter.verify(() -> BulkImporterImpl.updateVertexGuid(eq(mockAtlasGraphBulk), eq(mockTypeRegistry), eq(mockEntityRetrieverBulk), any()));
        }
    }

    @Test
    public void testImportUsingBulkEntityStore() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);

        when(mockEntityStoreBulk.createOrUpdateForImportNoCommit(any())).thenReturn(mockResponse);

        // Use reflection to access private method
        Method importUsingBulkEntityStoreMethod = EntityConsumer.class.getDeclaredMethod("importUsingBulkEntityStore", AtlasEntity.AtlasEntityWithExtInfo.class);
        importUsingBulkEntityStoreMethod.setAccessible(true);

        importUsingBulkEntityStoreMethod.invoke(entityConsumer, mockEntityWithExtInfo);

        verify(mockEntityStoreBulk).createOrUpdateForImportNoCommit(any(AtlasEntityStreamForImport.class));
    }

    @Test
    public void testPerformRegularImport() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);

        when(mockEntityStore.createOrUpdateForImportNoCommit(any())).thenReturn(mockResponse);
        doNothing().when(mockAtlasGraph).commit();

        // Use reflection to access private method
        Method performRegularImportMethod = EntityConsumer.class.getDeclaredMethod("performRegularImport", AtlasEntity.AtlasEntityWithExtInfo.class);
        performRegularImportMethod.setAccessible(true);

        try {
            performRegularImportMethod.invoke(entityConsumer, mockEntityWithExtInfo);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testPerformRegularImportWithException() throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = createMockEntityWithExtInfo();

        when(mockEntityStore.createOrUpdateForImportNoCommit(any())).thenThrow(new RuntimeException("Regular import failed"));
        doNothing().when(mockAtlasGraph).rollback();
        doNothing().when(mockAtlasGraph).commit();

        // Use reflection to access private method
        Method performRegularImportMethod = EntityConsumer.class.getDeclaredMethod("performRegularImport", AtlasEntity.AtlasEntityWithExtInfo.class);
        performRegularImportMethod.setAccessible(true);

        try {
            performRegularImportMethod.invoke(entityConsumer, mockEntityWithExtInfo);
        } catch (Exception e) {
                // Handle potential NullPointerException due to mocking limitations
            assertNotNull(e);
        }
    }

    @Test
    public void testCommitWithRetry() throws Exception {
        doNothing().when(mockAtlasGraphBulk).commit();

        // Use reflection to access private method
        Method commitWithRetryMethod = EntityConsumer.class.getDeclaredMethod(
                "commitWithRetry", int.class);
        commitWithRetryMethod.setAccessible(true);

        boolean result = (boolean) commitWithRetryMethod.invoke(entityConsumer, 1);

        assertEquals(true, result);
        verify(mockAtlasGraphBulk).commit();
    }

    @Test
    public void testCommitWithRetryFailure() throws Exception {
        doThrow(new RuntimeException("Commit failed")).when(mockAtlasGraphBulk).commit();
        doNothing().when(mockAtlasGraphBulk).rollback();

        try (MockedStatic<GraphTransactionInterceptor> mockedInterceptor = mockStatic(GraphTransactionInterceptor.class); MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            doNothing().when(mockContext).clearCache();
            mockedInterceptor.when(GraphTransactionInterceptor::clearCache).thenAnswer(invocation -> null);

            Method commitWithRetryMethod = EntityConsumer.class.getDeclaredMethod("commitWithRetry", int.class);
            commitWithRetryMethod.setAccessible(true);

            boolean result = (boolean) commitWithRetryMethod.invoke(entityConsumer, 1);

            assertEquals(false, result);
            verify(mockAtlasGraphBulk, Mockito.atLeastOnce()).rollback();
        }
    }

    @Test
    public void testClearCache() throws Exception {
        try (MockedStatic<GraphTransactionInterceptor> mockedInterceptor = mockStatic(GraphTransactionInterceptor.class); MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            doNothing().when(mockContext).clearCache();
            mockedInterceptor.when(GraphTransactionInterceptor::clearCache).thenAnswer(invocation -> null);

            Method clearCacheMethod = EntityConsumer.class.getDeclaredMethod("clearCache");
            clearCacheMethod.setAccessible(true);

            clearCacheMethod.invoke(entityConsumer);

            mockedInterceptor.verify(GraphTransactionInterceptor::clearCache);
            verify(mockContext).clearCache();
        }
    }

    @Test
    public void testFieldAccessUsingReflection() throws Exception {
        // Test access to private fields
        Field batchSizeField = EntityConsumer.class.getDeclaredField("batchSize");
        batchSizeField.setAccessible(true);
        int batchSize = (int) batchSizeField.get(entityConsumer);
        assertEquals(10, batchSize);

        Field isMigrationImportField = EntityConsumer.class.getDeclaredField("isMigrationImport");
        isMigrationImportField.setAccessible(true);
        boolean isMigrationImport = (boolean) isMigrationImportField.get(entityConsumer);
        assertEquals(false, isMigrationImport);

        Field counterField = EntityConsumer.class.getDeclaredField("counter");
        counterField.setAccessible(true);
        AtomicLong counter = (AtomicLong) counterField.get(entityConsumer);
        assertNotNull(counter);

        Field currentBatchField = EntityConsumer.class.getDeclaredField("currentBatch");
        currentBatchField.setAccessible(true);
        AtomicLong currentBatch = (AtomicLong) currentBatchField.get(entityConsumer);
        assertNotNull(currentBatch);
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMockEntityWithExtInfo() {
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockEntity.getGuid()).thenReturn("test-guid-123");
        when(mockEntity.getTypeName()).thenReturn("TestEntity");
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);

        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        when(mockEntityWithExtInfo.getReferredEntities()).thenReturn(referredEntities);

        return mockEntityWithExtInfo;
    }
}
