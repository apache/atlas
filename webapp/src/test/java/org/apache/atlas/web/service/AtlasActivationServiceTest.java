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
package org.apache.atlas.web.service;

import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.server.common.service.ServiceState;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AtlasActivationServiceTest {
    @Mock
    private ActiveStateChangeHandler handlerOne;

    @Mock
    private ActiveStateChangeHandler handlerTwo;

    @Mock
    private ServiceState serviceState;

    @Mock
    private AtlasMetricsUtil metricsUtil;

    @Mock
    private AtlasAuditService auditService;

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void start_ordersHandlersAndMarksServiceActive() throws Exception {
        when(handlerOne.getHandlerOrder()).thenReturn(20);
        when(handlerTwo.getHandlerOrder()).thenReturn(5);

        Set<ActiveStateChangeHandler> handlers = new HashSet<>(Arrays.asList(handlerOne, handlerTwo));
        AtlasActivationService service = new AtlasActivationService(handlers, serviceState, metricsUtil, auditService);

        service.start();

        verify(metricsUtil).onServerStart();
        verify(serviceState).becomingActive();

        InOrder inOrder = inOrder(handlerTwo, handlerOne);
        inOrder.verify(handlerTwo).instanceIsActive();
        inOrder.verify(handlerOne).instanceIsActive();

        verify(metricsUtil).onServerActivation();
        verify(serviceState).setActive();
        verify(auditService).add(org.mockito.Matchers.eq(AtlasAuditEntry.AuditOperation.SERVER_START), any(), any(), isNull(String.class), isNull(String.class), anyLong());
        verify(auditService).add(org.mockito.Matchers.eq(AtlasAuditEntry.AuditOperation.SERVER_STATE_ACTIVE), any(), any(), isNull(String.class), isNull(String.class), anyLong());
    }

    @Test
    public void start_handlerFailureDoesNotThrowAndSkipsActiveState() throws Exception {
        when(handlerOne.getHandlerOrder()).thenReturn(5);
        when(handlerTwo.getHandlerOrder()).thenReturn(10);
        org.mockito.Mockito.doThrow(new RuntimeException("activation failed")).when(handlerOne).instanceIsActive();

        Set<ActiveStateChangeHandler> handlers = new HashSet<>(Arrays.asList(handlerOne, handlerTwo));
        AtlasActivationService service = new AtlasActivationService(handlers, serviceState, metricsUtil, auditService);

        service.start();

        verify(metricsUtil).onServerStart();
        verify(serviceState).becomingActive();
        verify(handlerOne).instanceIsActive();
        verify(handlerTwo, never()).instanceIsActive();
        verify(metricsUtil, never()).onServerActivation();
        verify(serviceState, never()).setActive();
        verify(auditService, never()).add(any(), any(), any(), any(), any(), anyLong());
    }

    @Test
    public void start_initializerModeInvokesExitHook() throws Exception {
        when(handlerOne.getHandlerOrder()).thenReturn(1);
        Set<ActiveStateChangeHandler> handlers = new HashSet<>(Arrays.asList(handlerOne));
        AtlasActivationService service = spy(new AtlasActivationService(handlers, serviceState, metricsUtil, auditService));
        doNothing().when(service).exitAfterInitialization();

        try (MockedStatic<AtlasRunMode> runModeMock = org.mockito.Mockito.mockStatic(AtlasRunMode.class)) {
            AtlasRunMode initializerMode = org.mockito.Mockito.mock(AtlasRunMode.class);
            runModeMock.when(AtlasRunMode::current).thenReturn(initializerMode);
            when(initializerMode.exitsAfterInit()).thenReturn(true);

            service.start();
        }

        verify(handlerOne).instanceIsActive();
        verify(service).exitAfterInitialization();
    }
}
