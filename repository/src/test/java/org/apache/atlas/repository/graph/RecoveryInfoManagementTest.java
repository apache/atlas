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
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.tasks.GraphClaim;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

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

    /**
     * The claim outlives a test method, so without this each test would inherit whichever node the
     * previous one left holding it and the results would depend on execution order.
     */
    @BeforeMethod
    public void releaseIndexClaim() {
        Iterator<AtlasVertex> holders = atlasGraph.query().has(Constants.CLAIM_KEY, Constants.CLAIM_INDEX).vertices().iterator();

        while (holders.hasNext()) {
            AtlasVertex holder = holders.next();

            GraphClaim.releaseClaim(holder);
            atlasGraph.removeVertex(holder);
        }

        atlasGraph.commit();
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

    /**
     * A node that dies holding the claim must not keep it forever, or index recovery stops for good.
     */
    @Test
    public void verifyOwnershipReclaimAfterLeaseExpiry() throws Exception {
        IndexRecoveryService.RecoveryInfoManagement rm = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);

        assertTrue(rm.tryClaimOwnership("stale-owner", 1), "First owner should take the claim");

        Thread.sleep(10);

        assertTrue(rm.tryClaimOwnership("node-2", 60_000),
                "Second owner should reclaim once the stale owner's lease expires");
        assertFalse(rm.isOwner("stale-owner"), "Expired owner should no longer be recognized");
        assertTrue(rm.isOwner("node-2"), "New owner should hold a valid lease");
    }

    /**
     * Expiry is the holder's business.  A peer that checks with a shorter threshold than the holder
     * asked for must not conclude the claim has lapsed, or long-running work gets displaced while
     * it is still going.
     */
    @Test
    public void ownershipSurvivesAPeerCheckingWithAShorterThreshold() {
        IndexRecoveryService.RecoveryInfoManagement rm = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);

        assertTrue(rm.tryClaimOwnership("long-runner", TimeUnit.HOURS.toMillis(6)),
                "First owner should take the claim");

        assertFalse(rm.tryClaimOwnership("impatient-peer", 1),
                "A peer's own threshold must not decide when someone else's claim lapses");
        assertTrue(rm.isOwner("long-runner"), "The original owner should still hold the claim");
    }

    /**
     * The claim has to survive being handed back and forth, since the claim vertex is created and
     * deleted each time and a leftover uniqueness entry would lock everyone out permanently.
     */
    @Test
    public void ownershipCanBeTakenAgainAfterRepeatedHandover() {
        IndexRecoveryService.RecoveryInfoManagement rm = new IndexRecoveryService.RecoveryInfoManagement(atlasGraph);

        for (int round = 0; round < 3; round++) {
            assertTrue(rm.tryClaimOwnership("node-1", 60_000), "node-1 should take the claim in round " + round);

            rm.releaseOwnership("node-1");

            assertTrue(rm.tryClaimOwnership("node-2", 60_000), "node-2 should take the claim in round " + round);

            rm.releaseOwnership("node-2");
        }
    }
}
