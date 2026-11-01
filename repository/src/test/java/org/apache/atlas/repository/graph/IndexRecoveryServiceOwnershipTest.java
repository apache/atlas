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
package org.apache.atlas.repository.graph;

import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class IndexRecoveryServiceOwnershipTest {
    @Test
    public void stopMonitoringAfterOwnershipLoss_stopsRecoveryWithoutUpdatingStartTime() throws Exception {
        AtlasGraph graph = Mockito.mock(AtlasGraph.class);
        AtlasGraphManagement management = Mockito.mock(AtlasGraphManagement.class);
        IndexRecoveryService.RecoveryInfoManagement recoveryInfoManagement =
                spy(new IndexRecoveryService.RecoveryInfoManagement(graph));

        Mockito.when(graph.getManagementSystem()).thenReturn(management);

        Object recoveryThread = newRecoveryThread(recoveryInfoManagement, graph, "owner-1", 60_000L);
        Object txRecoveryObject = new Object();
        setField(recoveryThread, "txRecoveryObject", txRecoveryObject);

        invokePrivateMethod(recoveryThread, "stopMonitoringAfterOwnershipLoss");

        verify(management, times(1)).stopIndexRecovery(txRecoveryObject);
        verify(management, times(1)).printIndexRecoveryStats(txRecoveryObject);
        verify(management, times(2)).setIsSuccess(true);
        verify(recoveryInfoManagement, never()).updateStartTime(anyLong());
    }

    private static Object newRecoveryThread(IndexRecoveryService.RecoveryInfoManagement recoveryInfoManagement,
                                            AtlasGraph graph,
                                            String ownerId,
                                            long ownerLeaseMillis) throws Exception {
        Constructor<IndexRecoveryService.RecoveryThread> constructor = IndexRecoveryService.RecoveryThread.class
                .getDeclaredConstructor(IndexRecoveryService.RecoveryInfoManagement.class,
                        AtlasGraph.class,
                        long.class,
                        long.class,
                        String.class,
                        long.class);
        constructor.setAccessible(true);
        return constructor.newInstance(recoveryInfoManagement, graph, 0L, 1L, ownerId, ownerLeaseMillis);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void invokePrivateMethod(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        method.invoke(target);
    }
}
