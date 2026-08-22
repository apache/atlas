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
import org.apache.atlas.server.common.service.ServiceState;
import org.apache.commons.configuration2.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ServiceStateTest {
    @Mock
    private Configuration configuration;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    private ServiceState newServiceState(String migrationModeFile) {
        when(configuration.getString(AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME, "")).thenReturn(migrationModeFile);
        return new ServiceState(configuration);
    }

    @Test
    public void constructor_defaultsToBecomingActiveWhenNotInMigration() {
        ServiceState serviceState = newServiceState("");
        assertNotNull(serviceState);
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.BECOMING_ACTIVE);
    }

    @Test
    public void constructor_setsMigratingWhenMigrationModeConfigured() {
        ServiceState serviceState = newServiceState("migration.txt");
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.MIGRATING);
    }

    @Test
    public void becomingActive_setsTransitionState() {
        ServiceState serviceState = newServiceState("");
        serviceState.setActive();
        serviceState.becomingActive();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.BECOMING_ACTIVE);
        assertTrue(serviceState.isInstanceInTransition());
    }

    @Test
    public void setActive_marksNodeActive() {
        ServiceState serviceState = newServiceState("");
        serviceState.setActive();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.ACTIVE);
        assertTrue(serviceState.isActive());
        assertFalse(serviceState.isInstanceInTransition());
        assertFalse(serviceState.isInstanceInMigration());
    }

    @Test
    public void setMigration_marksNodeMigrating() {
        ServiceState serviceState = newServiceState("");
        serviceState.setMigration();
        assertEquals(serviceState.getState(), ServiceState.ServiceStateValue.MIGRATING);
        assertTrue(serviceState.isInstanceInMigration());
        assertFalse(serviceState.isActive());
        assertFalse(serviceState.isInstanceInTransition());
    }

    @Test
    public void stateName_matchesCurrentState() {
        ServiceState serviceState = newServiceState("");
        serviceState.setActive();
        assertEquals(serviceState.getStateName(), ServiceState.ServiceStateValue.ACTIVE.toString());
    }
}
