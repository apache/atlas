/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.services;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class PurgeBatchExecutorTest {
    private static final Set<String> BATCH = Collections.singleton("guid1");

    @Test
    public void testExecuteBatchSuccess() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        when(mockStore.purgeEntitiesInBatch(BATCH)).thenReturn(mockResponse);

        PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);
        EntityMutationResponse response = executor.executeBatch(BATCH);

        assertEquals(response, mockResponse);
        verify(mockStore, times(1)).purgeEntitiesInBatch(BATCH);
    }

    @Test
    public void testIsRetryableLockConflictReturnsFalseForNull() {
        assertFalse(PurgeBatchExecutor.isRetryableLockConflict(null));
    }

    @Test
    public void testIsRetryableLockConflictReturnsFalseForNonRetryableException() {
        assertFalse(PurgeBatchExecutor.isRetryableLockConflict(new RuntimeException("unexpected")));
    }

    @Test
    public void testIsRetryableLockConflictReturnsFalseForPermanentBackendException() {
        PermanentBackendException backendException = new PermanentBackendException("backend failure");

        assertFalse(PurgeBatchExecutor.isRetryableLockConflict(backendException));
    }

    @Test
    public void testIsRetryableLockConflictMatchesPermanentLockingException() {
        PermanentLockingException ple = new PermanentLockingException("lock conflict");

        assertTrue(PurgeBatchExecutor.isRetryableLockConflict(ple));
    }

    @Test
    public void testIsRetryableLockConflictMatchesWrappedCause() {
        PermanentLockingException ple = new PermanentLockingException("lock conflict");
        RuntimeException wrapped = new RuntimeException(new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, ple));

        assertTrue(PurgeBatchExecutor.isRetryableLockConflict(wrapped));
    }

    @Test
    public void testExecuteBatchClearsCachesBeforeRetry() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        PermanentLockingException ple       = new PermanentLockingException("lock conflict");
        AtlasBaseException wrappedException = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, ple);

        when(mockStore.purgeEntitiesInBatch(BATCH))
                .thenThrow(wrappedException)
                .thenReturn(mockResponse);

        try (MockedStatic<GraphTransactionInterceptor> interceptor = mockStatic(GraphTransactionInterceptor.class);
                MockedStatic<RequestContext> requestContextStatic = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            requestContextStatic.when(RequestContext::get).thenReturn(mockContext);
            interceptor.when(GraphTransactionInterceptor::clearCache).thenAnswer(invocation -> null);
            doNothing().when(mockContext).clearCache();

            PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);
            EntityMutationResponse response = executor.executeBatch(BATCH);

            assertEquals(response, mockResponse);
            interceptor.verify(GraphTransactionInterceptor::clearCache, times(1));
            verify(mockContext).clearCache();
            verify(mockStore, times(2)).purgeEntitiesInBatch(BATCH);
        }
    }

    @Test
    public void testExecuteBatchRetryOnPermanentLockingException() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        EntityMutationResponse mockResponse = new EntityMutationResponse();

        PermanentLockingException ple = new PermanentLockingException("Locking conflict");
        AtlasBaseException wrappedException = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, ple);

        when(mockStore.purgeEntitiesInBatch(BATCH))
                .thenThrow(wrappedException)
                .thenThrow(wrappedException)
                .thenReturn(mockResponse);

        PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);

        long start = System.currentTimeMillis();
        EntityMutationResponse response = executor.executeBatch(BATCH);
        long duration = System.currentTimeMillis() - start;

        assertEquals(response, mockResponse);
        verify(mockStore, times(3)).purgeEntitiesInBatch(BATCH);
        assertTrue(duration >= 1000, "Expected backoff delays but finished in " + duration + " ms");
    }

    @Test
    public void testExecuteBatchFailsAfterMaxLockingConflicts() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        PermanentLockingException ple = new PermanentLockingException("lock conflict");
        AtlasBaseException wrappedException = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, ple);

        when(mockStore.purgeEntitiesInBatch(BATCH)).thenThrow(wrappedException);

        PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> executor.executeBatch(BATCH));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
        verify(mockStore, times(3)).purgeEntitiesInBatch(BATCH);
    }

    @Test
    public void testExecuteBatchNoRetryOnNonLockingException() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        RuntimeException nonRetryable = new RuntimeException("unexpected");
        AtlasBaseException wrappedException = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, nonRetryable);

        when(mockStore.purgeEntitiesInBatch(BATCH)).thenThrow(wrappedException);

        PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> executor.executeBatch(BATCH));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
        verify(mockStore, times(1)).purgeEntitiesInBatch(BATCH);
    }

    @Test
    public void testExecuteBatchNoRetryOnPermanentBackendException() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        PermanentBackendException backendException = new PermanentBackendException("backend failure");
        AtlasBaseException wrappedException = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, backendException);

        when(mockStore.purgeEntitiesInBatch(BATCH)).thenThrow(wrappedException);

        PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);

        AtlasBaseException ex = expectThrows(AtlasBaseException.class, () -> executor.executeBatch(BATCH));

        assertEquals(ex.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
        verify(mockStore, times(1)).purgeEntitiesInBatch(BATCH);
    }
}
