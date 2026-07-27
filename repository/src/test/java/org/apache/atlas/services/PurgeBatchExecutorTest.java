package org.apache.atlas.services;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
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

    @DataProvider(name = "retryableLockConflictExceptionClassNames")
    public Object[][] retryableLockConflictExceptionClassNames() {
        return new Object[][] {
                {"org.janusgraph.diskstorage.locking.PermanentLockingException"},
                {"com.sleepycat.je.LockTimeoutException"},
                {"com.sleepycat.je.DeadlockException"},
                {"org.janusgraph.diskstorage.PermanentBackendException"}
        };
    }

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
    public void testIsRetryableLockConflictMatchesWrappedCause() {
        PermanentLockingException ple = new PermanentLockingException("lock conflict");
        RuntimeException wrapped = new RuntimeException(new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, ple));

        assertTrue(PurgeBatchExecutor.isRetryableLockConflict(wrapped));
    }

    @Test(dataProvider = "retryableLockConflictExceptionClassNames")
    public void testIsRetryableLockConflictMatchesKnownTypes(String className) throws Exception {
        Exception conflict = newExceptionByClassName(className, "lock conflict");

        assertTrue(PurgeBatchExecutor.RETRYABLE_LOCK_CONFLICT_EXCEPTION_CLASS_NAMES.contains(className));
        assertTrue(PurgeBatchExecutor.isRetryableLockConflict(conflict));
        // Use message+cause form: RuntimeException(Throwable) calls cause.toString(), which NPEs on
        // partially-initialized Berkeley JE DatabaseException instances created for this test.
        assertTrue(PurgeBatchExecutor.isRetryableLockConflict(wrapWithCause(conflict)));
    }

    @Test
    public void testExecuteBatchClearsCachesBeforeRetry() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        PermanentLockingException ple         = new PermanentLockingException("lock conflict");
        AtlasBaseException wrappedException   = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, ple);

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

    @Test(dataProvider = "retryableLockConflictExceptionClassNames")
    public void testExecuteBatchRetriesOnKnownLockConflictTypes(String className) throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        Exception conflict = newExceptionByClassName(className, "lock conflict");
        AtlasBaseException wrappedException = new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, conflict);

        when(mockStore.purgeEntitiesInBatch(BATCH))
                .thenThrow(wrappedException)
                .thenReturn(mockResponse);

        PurgeBatchExecutor executor = new PurgeBatchExecutor(mockStore);
        EntityMutationResponse response = executor.executeBatch(BATCH);

        assertEquals(response, mockResponse);
        verify(mockStore, times(2)).purgeEntitiesInBatch(BATCH);
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

    private static RuntimeException wrapWithCause(Throwable cause) {
        return new RuntimeException("wrapped", cause);
    }

    private static Exception newExceptionByClassName(String className, String message) throws Exception {
        try {
            Class<?> clazz = Class.forName(className);
            try {
                return (Exception) clazz.getConstructor(String.class).newInstance(message);
            } catch (NoSuchMethodException e) {
                try {
                    return (Exception) clazz.getConstructor().newInstance();
                } catch (NoSuchMethodException e2) {
                    java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                    f.setAccessible(true);
                    sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
                    return (Exception) unsafe.allocateInstance(clazz);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new org.testng.SkipException("Required exception class not on classpath: " + className);
        }
    }
}
