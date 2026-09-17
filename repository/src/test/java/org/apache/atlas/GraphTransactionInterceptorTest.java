/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.tasks.TaskManagement;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class GraphTransactionInterceptorTest {
    @AfterMethod
    public void afterMethod() {
        RequestContext.clear();
        GraphTransactionInterceptor.clearCache();
    }

    @Test
    public void invoke_retriesOnJanusLockConflictAndCommitsOnSuccess() throws Throwable {
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        TaskManagement taskManagement = Mockito.mock(TaskManagement.class);
        GraphTransactionInterceptor interceptor = new GraphTransactionInterceptor(graph, taskManagement);
        MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
        Method method = TestTxnTarget.class.getMethod("execute");

        when(invocation.getMethod()).thenReturn(method);
        when(invocation.proceed())
                .thenThrow(new RuntimeException(new PermanentLockingException("lock conflict")))
                .thenReturn("ok");

        Object result = interceptor.invoke(invocation);

        assertEquals(result, "ok");
        verify(invocation, times(2)).proceed();
        verify(graph, times(1)).rollback();
        verify(graph, times(1)).commit();
    }

    @Test
    public void invoke_nonRetryableExceptionRollsBackAndPropagates() throws Throwable {
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        TaskManagement taskManagement = Mockito.mock(TaskManagement.class);
        GraphTransactionInterceptor interceptor = new GraphTransactionInterceptor(graph, taskManagement);
        MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
        Method method = TestTxnTarget.class.getMethod("execute");

        when(invocation.getMethod()).thenReturn(method);
        when(invocation.proceed()).thenThrow(new IllegalStateException("boom"));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> interceptor.invoke(invocation));

        assertEquals(ex.getMessage(), "boom");
        verify(invocation, times(1)).proceed();
        verify(graph, times(1)).rollback();
        verify(graph, times(0)).commit();
    }

    /**
     * A retried attempt has to run its post-transaction hooks, because those hooks are what release
     * what the attempt was holding.  Dropping them stranded the type-registry update lock once per
     * retry, and that lock lives as long as the process: a handful of retries during startup left the
     * node rejecting every later type update with "another type update might be in progress".
     */
    @Test
    public void invoke_runsHooksOfTheAbandonedAttemptBeforeRetrying() throws Throwable {
        AtlasGraph                  graph          = Mockito.mock(AtlasGraph.class);
        TaskManagement              taskManagement = Mockito.mock(TaskManagement.class);
        GraphTransactionInterceptor interceptor    = new GraphTransactionInterceptor(graph, taskManagement);
        MethodInvocation            invocation     = Mockito.mock(MethodInvocation.class);
        Method                      method         = TestTxnTarget.class.getMethod("execute");
        List<Boolean>               hookOutcomes   = new ArrayList<>();

        when(invocation.getMethod()).thenReturn(method);
        when(invocation.proceed()).thenAnswer(attempt -> {
            new RecordingHook(hookOutcomes);

            if (hookOutcomes.isEmpty()) {
                throw new RuntimeException(new PermanentLockingException("lock conflict"));
            }

            return "ok";
        });

        assertEquals(interceptor.invoke(invocation), "ok");
        assertEquals(hookOutcomes, Arrays.asList(false, true),
                "The abandoned attempt's hook must run as a failure, and the successful one as a success");
    }

    /**
     * Both nodes reach for a schema element the graph creates on demand, and the store refuses the
     * second by name.  The element is there by the time the loser looks again, so the request it was
     * carrying - a classification being attached, say - should be repeated rather than failed.
     */
    @Test
    public void invoke_retriesWhenAPeerDefinedTheSameSchemaElementFirst() throws Throwable {
        AtlasGraph                  graph          = Mockito.mock(AtlasGraph.class);
        TaskManagement              taskManagement = Mockito.mock(TaskManagement.class);
        GraphTransactionInterceptor interceptor    = new GraphTransactionInterceptor(graph, taskManagement);
        MethodInvocation            invocation     = Mockito.mock(MethodInvocation.class);
        Method                      method         = TestTxnTarget.class.getMethod("execute");

        when(invocation.getMethod()).thenReturn(method);
        when(invocation.proceed())
                .thenThrow(new AtlasSchemaViolationException(new SchemaViolationException("Adding this property for key "
                        + "[~T$SchemaName] and value [rt__entityGuid] violates a uniqueness constraint [SchemaNameIndex]")))
                .thenReturn("ok");

        assertEquals(interceptor.invoke(invocation), "ok");
        verify(invocation, times(2)).proceed();
        verify(graph, times(1)).commit();
    }

    /** A duplicate of anything else is not a race, and repeating it would only fail again. */
    @Test
    public void invoke_doesNotRetryARealDuplicate() throws Throwable {
        AtlasGraph                  graph          = Mockito.mock(AtlasGraph.class);
        TaskManagement              taskManagement = Mockito.mock(TaskManagement.class);
        GraphTransactionInterceptor interceptor    = new GraphTransactionInterceptor(graph, taskManagement);
        MethodInvocation            invocation     = Mockito.mock(MethodInvocation.class);
        Method                      method         = TestTxnTarget.class.getMethod("execute");

        when(invocation.getMethod()).thenReturn(method);
        when(invocation.proceed()).thenThrow(new AtlasSchemaViolationException(new SchemaViolationException(
                "Adding this property for key [qualifiedName] and value [db@cl] violates a uniqueness constraint")));

        expectThrows(AtlasSchemaViolationException.class, () -> interceptor.invoke(invocation));

        verify(invocation, times(1)).proceed();
        verify(graph, times(0)).commit();
    }

    private static class RecordingHook extends GraphTransactionInterceptor.PostTransactionHook {
        private final List<Boolean> outcomes;

        private RecordingHook(List<Boolean> outcomes) {
            this.outcomes = outcomes;
        }

        @Override
        public void onComplete(boolean isSuccess) {
            outcomes.add(isSuccess);
        }
    }

    public static class TestTxnTarget {
        public String execute() {
            return "ok";
        }
    }
}
