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
package org.apache.atlas.web.metrics;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.server.common.service.EmbeddedServer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class WebappAdminAuditHookTest {
    private static final String ATLAS_USER = "atlas";

    @Mock
    private AtlasAuditService auditService;

    private WebappAdminAuditHook hook;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        hook = new WebappAdminAuditHook(auditService);
    }

    @Test
    public void testOnServerStartInvokesAuditWithServerStartOperation() throws AtlasBaseException {
        hook.onServerStart();

        verify(auditService, times(1)).add(
                eq(ATLAS_USER),
                eq(AuditOperation.SERVER_START),
                anyString(),
                eq(EmbeddedServer.SERVER_START_TIME),
                any(Date.class),
                isNull(),
                isNull(),
                eq(0));
    }

    @Test
    public void testOnServerActivationInvokesAuditWithServerStateActiveOperation() throws AtlasBaseException {
        hook.onServerActivation();

        ArgumentCaptor<Date> startCaptor = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Date> endCaptor = ArgumentCaptor.forClass(Date.class);

        verify(auditService, times(1)).add(
                eq(ATLAS_USER),
                eq(AuditOperation.SERVER_STATE_ACTIVE),
                anyString(),
                startCaptor.capture(),
                endCaptor.capture(),
                isNull(),
                isNull(),
                eq(0));

        assertNotNull(startCaptor.getValue());
        assertNotNull(endCaptor.getValue());
        assertEquals(startCaptor.getValue(), endCaptor.getValue());
    }

    @Test
    public void testOnServerStartSwallowsAtlasBaseException() throws AtlasBaseException {
        doThrow(new AtlasBaseException("x")).when(auditService).add(
                anyString(),
                any(AuditOperation.class),
                anyString(),
                any(Date.class),
                any(Date.class),
                isNull(),
                isNull(),
                anyLong());

        hook.onServerStart();

        verify(auditService, times(1)).add(
                eq(ATLAS_USER),
                eq(AuditOperation.SERVER_START),
                anyString(),
                eq(EmbeddedServer.SERVER_START_TIME),
                any(Date.class),
                isNull(),
                isNull(),
                eq(0));
    }
}
