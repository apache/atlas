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
package org.apache.atlas.repository.graph;

import com.google.inject.Inject;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class RecoveryInfoManagementTest extends AtlasTestBase {
    @Inject
    private AtlasGraph atlasGraph;

    @BeforeTest
    public void setupTest() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    @BeforeClass
    public void initialize() throws Exception {
        super.initialize();
    }

    @AfterClass
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void verifyCreateUpdate() {
        IndexRecoveryService.RecoveryInfoManagement rm  = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);
        long                                        now = System.currentTimeMillis();

        rm.updateStartTime(now);

        long storedTime = rm.getStartTime();

        assertEquals(now, storedTime);
    }

    @Test
    public void verifyOwnershipClaimAndRelease() {
        IndexRecoveryService.RecoveryInfoManagement rm = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);

        assertTrue(rm.tryClaimOwnership("node-1", 60_000));
        assertFalse(rm.tryClaimOwnership("node-2", 60_000));
        assertTrue(rm.tryClaimOwnership("node-1", 60_000));

        rm.releaseOwnership("node-1");

        assertTrue(rm.tryClaimOwnership("node-2", 60_000));
    }

    @Test
    public void verifyOnlyOwnerCanReleaseOwnership() {
        IndexRecoveryService.RecoveryInfoManagement rm = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);

        assertTrue(rm.tryClaimOwnership("node-1", 60_000));

        rm.releaseOwnership("node-2");

        assertFalse(rm.tryClaimOwnership("node-2", 60_000));
        assertTrue(rm.tryClaimOwnership("node-1", 60_000));
    }

    @Test
    public void verifyOwnershipReclaimAfterLeaseExpiry() throws Exception {
        IndexRecoveryService.RecoveryInfoManagement rm = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);
        String ownerKey = getIndexRecoveryStaticString("INDEX_RECOVERY_OWNER_KEY");
        String leaseUntilKey = getIndexRecoveryStaticString("INDEX_RECOVERY_LEASE_UNTIL_KEY");

        rm.tryClaimOwnership("seed-owner", 1);
        AtlasVertex vertex = rm.findVertex();
        assertTrue(vertex != null, "Recovery ownership vertex should exist");
        AtlasGraphUtilsV2.setEncodedProperty(vertex, ownerKey, "stale-owner");
        AtlasGraphUtilsV2.setEncodedProperty(vertex, leaseUntilKey, System.currentTimeMillis() - 1_000L);
        atlasGraph.commit();

        assertTrue(rm.tryClaimOwnership("node-2", 60_000),
                "Second owner should reclaim once stale owner lease expires");
        assertFalse(rm.isOwner("stale-owner"), "Expired owner should no longer be recognized");
        assertTrue(rm.isOwner("node-2"), "New owner should hold valid lease");
    }

    private String getIndexRecoveryStaticString(String fieldName) throws Exception {
        Field field = IndexRecoveryService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (String) field.get(null);
    }
}
