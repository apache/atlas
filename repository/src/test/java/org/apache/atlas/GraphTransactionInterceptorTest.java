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
import org.apache.atlas.tasks.TaskManagement;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

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

    public static class TestTxnTarget {
        public String execute() {
            return "ok";
        }
    }
}
