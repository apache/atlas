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
package org.apache.atlas;

import org.testng.annotations.Test;

import static org.apache.atlas.AtlasRunMode.INITIALIZER;
import static org.apache.atlas.AtlasRunMode.METADATA_SERVER;
import static org.apache.atlas.AtlasRunMode.MONOLITHIC;
import static org.apache.atlas.AtlasRunMode.NOTIFICATION_PROCESSOR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link AtlasRunMode} predicates.
 * The singleton {@code current()} value is JVM-scoped and cannot be reset in tests,
 * so predicates are tested directly on the enum values.
 */
public class AtlasRunModeTest {
    // ---- runsInitialization ----

    @Test
    public void monolithic_runsInitialization() {
        assertTrue(MONOLITHIC.runsInitialization());
    }

    @Test
    public void initializer_runsInitialization() {
        assertTrue(INITIALIZER.runsInitialization());
    }

    @Test
    public void metadataServer_doesNotRunInitialization() {
        assertFalse(METADATA_SERVER.runsInitialization());
    }

    @Test
    public void notificationProcessor_doesNotRunInitialization() {
        assertFalse(NOTIFICATION_PROCESSOR.runsInitialization());
    }

    // ---- runsServer ----

    @Test
    public void monolithic_runsServer() {
        assertTrue(MONOLITHIC.runsServer());
    }

    @Test
    public void initializer_doesNotRunServer() {
        assertFalse(INITIALIZER.runsServer());
    }

    @Test
    public void metadataServer_runsServer() {
        assertTrue(METADATA_SERVER.runsServer());
    }

    @Test
    public void notificationProcessor_runsServer() {
        assertTrue(NOTIFICATION_PROCESSOR.runsServer());
    }

    // ---- runsMetadataServer ----

    @Test
    public void monolithic_runsMetadataServer() {
        assertTrue(MONOLITHIC.runsMetadataServer());
    }

    @Test
    public void initializer_doesNotRunMetadataServer() {
        assertFalse(INITIALIZER.runsMetadataServer());
    }

    @Test
    public void metadataServer_runsMetadataServer() {
        assertTrue(METADATA_SERVER.runsMetadataServer());
    }

    @Test
    public void notificationProcessor_doesNotRunMetadataServer() {
        assertFalse(NOTIFICATION_PROCESSOR.runsMetadataServer());
    }

    // ---- runsNotificationProcessing ----

    @Test
    public void monolithic_runsNotificationProcessing() {
        assertTrue(MONOLITHIC.runsNotificationProcessing());
    }

    @Test
    public void initializer_doesNotRunNotificationProcessing() {
        assertFalse(INITIALIZER.runsNotificationProcessing());
    }

    @Test
    public void metadataServer_doesNotRunNotificationProcessing() {
        assertFalse(METADATA_SERVER.runsNotificationProcessing());
    }

    @Test
    public void notificationProcessor_runsNotificationProcessing() {
        assertTrue(NOTIFICATION_PROCESSOR.runsNotificationProcessing());
    }

    // ---- runsIndexSetup ----

    @Test
    public void monolithic_runsIndexSetup() {
        assertTrue(MONOLITHIC.runsIndexSetup());
    }

    @Test
    public void initializer_runsIndexSetup() {
        assertTrue(INITIALIZER.runsIndexSetup());
    }

    @Test
    public void metadataServer_runsIndexSetup() {
        assertTrue(METADATA_SERVER.runsIndexSetup());
    }

    @Test
    public void notificationProcessor_doesNotRunIndexSetup() {
        assertFalse(NOTIFICATION_PROCESSOR.runsIndexSetup());
    }

    // ---- exitsAfterInit ----

    @Test
    public void onlyInitializer_exitsAfterInit() {
        assertFalse(MONOLITHIC.exitsAfterInit());
        assertTrue(INITIALIZER.exitsAfterInit());
        assertFalse(METADATA_SERVER.exitsAfterInit());
        assertFalse(NOTIFICATION_PROCESSOR.exitsAfterInit());
    }
}
