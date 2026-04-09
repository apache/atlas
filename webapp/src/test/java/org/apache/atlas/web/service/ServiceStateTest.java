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

package org.apache.atlas.web.service;

import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ServiceStateTest {
    @Mock
    private Configuration configuration;

    @Mock
    private AtlasAuditService auditService;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldBeActiveIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.ACTIVE);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowTransitionIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        serviceState.becomingPassive();

        fail("Should not allow transition");
    }

    @Test
    public void testShouldChangeStateIfHAIsEnabled() {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);

        serviceState.becomingPassive();

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.BECOMING_PASSIVE);
    }

    @Test
    public void testDefaultConstructor() throws AtlasException {
        ServiceState serviceState = new ServiceState();
        assertNotNull(serviceState);
    }

    @Test
    public void testShouldBePassiveIfHAIsEnabled() {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.PASSIVE);
    }

    @Test
    public void testShouldBeMigratingIfMigrationModeSet() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getString(AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME, "")).thenReturn("migration.txt");

        ServiceState serviceState = new ServiceState(configuration);

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.MIGRATING);
    }

    @Test
    public void testBecomingActive() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        setAuditService(serviceState, auditService);

        serviceState.becomingActive();

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.BECOMING_ACTIVE);
    }

    @Test
    public void testSetActive() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        setAuditService(serviceState, auditService);

        serviceState.setActive();

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.ACTIVE);
        // Verify that audit service was called twice (SERVER_START and SERVER_STATE_ACTIVE)
        verify(auditService, org.mockito.Mockito.times(2)).add(any(), any(), any(), anyObject(), anyObject(), anyLong());
    }

    @Test
    public void testSetPassive() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        setAuditService(serviceState, auditService);

        serviceState.setPassive();

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.PASSIVE);
    }

    @Test
    public void testSetMigration() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        setAuditService(serviceState, auditService);

        serviceState.setMigration();

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.MIGRATING);
    }

    @Test
    public void testIsInstanceInTransitionBecomingActive() {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        serviceState.becomingActive();

        assertTrue(serviceState.isInstanceInTransition());
    }

    @Test
    public void testIsInstanceInTransitionBecomingPassive() {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        serviceState.becomingPassive();

        assertTrue(serviceState.isInstanceInTransition());
    }

    @Test
    public void testIsInstanceInTransitionActive() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        setAuditService(serviceState, auditService);
        serviceState.setActive();

        assertFalse(serviceState.isInstanceInTransition());
    }

    @Test
    public void testIsInstanceInMigration() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getString(AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME, "")).thenReturn("migration.txt");

        ServiceState serviceState = new ServiceState(configuration);

        assertTrue(serviceState.isInstanceInMigration());
    }

    @Test
    public void testIsInstanceInMigrationFalse() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        assertFalse(serviceState.isInstanceInMigration());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowBecomingActiveIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        serviceState.becomingActive();

        fail("Should not allow transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowSetActiveIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        serviceState.setActive();

        fail("Should not allow transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowSetPassiveIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        serviceState.setPassive();

        fail("Should not allow transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowSetMigrationIfHAIsDisabled() {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ServiceState serviceState = new ServiceState(configuration);

        serviceState.setMigration();

        fail("Should not allow transition");
    }

    @Test
    public void testAuditServerStatusWithException() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);

        ServiceState serviceState = new ServiceState(configuration);
        setAuditService(serviceState, auditService);

        doThrow(new AtlasBaseException("Test exception")).when(auditService).add(any(), any(), any(), anyObject(), anyObject(), anyInt());

        serviceState.setActive();

        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.ACTIVE);
    }

    private void setAuditService(ServiceState serviceState, AtlasAuditService auditService) throws Exception {
        Field field = ServiceState.class.getDeclaredField("auditService");
        field.setAccessible(true);
        field.set(serviceState, auditService);
    }
}
