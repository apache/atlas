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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.server.common.service.HighAvailability;
import org.apache.atlas.server.common.service.ServiceState;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
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

    @Mock
    private HighAvailability highAvailability;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    private ServiceState newServiceState(boolean haEnabled) {
        when(highAvailability.isHAEnabled(configuration)).thenReturn(haEnabled);
        return new ServiceState(configuration, highAvailability);
    }

    @Test
    public void testShouldBeActiveIfHAIsDisabled() {
        ServiceState serviceState = newServiceState(false);
        assertEquals(ServiceState.ServiceStateValue.ACTIVE, serviceState.getState());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowTransitionIfHAIsDisabled() {
        ServiceState serviceState = newServiceState(false);
        serviceState.becomingPassive();
        fail("Should not allow transition");
    }

    @Test
    public void testShouldChangeStateIfHAIsEnabled() {
        ServiceState serviceState = newServiceState(true);
        serviceState.becomingPassive();
        assertEquals(ServiceState.ServiceStateValue.BECOMING_PASSIVE, serviceState.getState());
    }

    @Test
    public void testConstructor() {
        ServiceState serviceState = newServiceState(true);
        assertNotNull(serviceState);
    }

    @Test
    public void testShouldBePassiveIfHAIsEnabled() {
        ServiceState serviceState = newServiceState(true);
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.PASSIVE);
    }

    @Test
    public void testShouldBeMigratingIfMigrationModeSet() {
        when(configuration.getString(AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME, "")).thenReturn("migration.txt");
        ServiceState serviceState = newServiceState(false);
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.MIGRATING);
    }

    @Test
    public void testBecomingActive() throws Exception {
        ServiceState serviceState = newServiceState(true);
        serviceState.becomingActive();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.BECOMING_ACTIVE);
    }

    @Test
    public void testSetActive() throws Exception {
        ServiceState serviceState = newServiceState(true);
        serviceState.setActive();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.ACTIVE);
    }

    @Test
    public void testSetPassive() throws Exception {
        ServiceState serviceState = newServiceState(true);
        serviceState.setPassive();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.PASSIVE);
    }

    @Test
    public void testSetMigration() throws Exception {
        ServiceState serviceState = newServiceState(true);
        serviceState.setMigration();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.MIGRATING);
    }

    @Test
    public void testIsInstanceInTransitionBecomingActive() {
        ServiceState serviceState = newServiceState(true);
        serviceState.becomingActive();
        assertTrue(serviceState.isInstanceInTransition());
    }

    @Test
    public void testIsInstanceInTransitionBecomingPassive() {
        ServiceState serviceState = newServiceState(true);
        serviceState.becomingPassive();
        assertTrue(serviceState.isInstanceInTransition());
    }

    @Test
    public void testIsInstanceInTransitionActive() throws Exception {
        ServiceState serviceState = newServiceState(true);
        serviceState.setActive();
        assertFalse(serviceState.isInstanceInTransition());
    }

    @Test
    public void testIsInstanceInMigration() {
        when(configuration.getString(AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME, "")).thenReturn("migration.txt");
        ServiceState serviceState = newServiceState(false);
        assertTrue(serviceState.isInstanceInMigration());
    }

    @Test
    public void testIsInstanceInMigrationFalse() {
        ServiceState serviceState = newServiceState(false);
        assertFalse(serviceState.isInstanceInMigration());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowBecomingActiveIfHAIsDisabled() {
        ServiceState serviceState = newServiceState(false);
        serviceState.becomingActive();
        fail("Should not allow transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowSetActiveIfHAIsDisabled() {
        ServiceState serviceState = newServiceState(false);
        serviceState.setActive();
        fail("Should not allow transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowSetPassiveIfHAIsDisabled() {
        ServiceState serviceState = newServiceState(false);
        serviceState.setPassive();
        fail("Should not allow transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testShouldDisallowSetMigrationIfHAIsDisabled() {
        ServiceState serviceState = newServiceState(false);
        serviceState.setMigration();
        fail("Should not allow transition");
    }

    @Test
    public void testAuditServerStatusWithException() throws Exception {
        if (!hasAuditServiceField()) {
            throw new SkipException("ServiceState no longer exposes auditService field in atlas-server-common design.");
        }

        ServiceState serviceState = newServiceState(true);
        setAuditService(serviceState, auditService);
        doThrow(new AtlasBaseException("Test exception")).when(auditService).add(any(), any(), any(), any(), any(), anyInt());
        serviceState.setActive();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.ACTIVE);
    }

    private boolean hasAuditServiceField() {
        try {
            ServiceState.class.getDeclaredField("auditService");
            return true;
        } catch (NoSuchFieldException e) {
            return false;
        }
    }

    private void setAuditService(ServiceState serviceState, AtlasAuditService auditService) throws Exception {
        Field field = ServiceState.class.getDeclaredField("auditService");
        field.setAccessible(true);
        field.set(serviceState, auditService);
    }
}
