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

package org.apache.atlas.tasks;

import org.apache.atlas.TestModules;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.IndexRecoveryService.RecoveryInfoManagement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * How the deferred startup activities coordinate with <em>each other</em>, against a real graph.
 *
 * <p>Each activity is tested on its own elsewhere, which leaves the relationships between them
 * untested - and those are what a refactor gets wrong.  Two activities accidentally sharing a claim
 * name would serialise work that should run in parallel, or let one steal another's claim, and every
 * single-activity test would still pass.  Index setup and index recovery sharing a name is likewise
 * deliberate rather than duplication, and would go unnoticed if someone gave them one each.
 *
 * <p>These all turn on state that is already committed and visible, so they are decided by the
 * claim logic rather than by the backend.  The separate question of who wins when nodes claim at the
 * same instant is not covered here; see {@link GraphClaimConcurrencyTest} for why a unit test on the
 * embedded backend cannot answer it.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class ClusterClaimCoordinationTest {
    private static final long ONE_MINUTE      = 60000L;
    private static final long ALREADY_EXPIRED = 1L;

    /** Every claim a starting node might take, so one test's leftovers cannot decide another's result. */
    private static final String[] COORDINATED_CLAIMS = {
            Constants.CLAIM_TYPEDEF_BOOTSTRAP,
            Constants.CLAIM_INDEX,
            Constants.CLAIM_TASK_RUNNER,
            Constants.CLAIM_PURGE,
            Constants.CLAIM_ASYNC_IMPORT
    };

    @Inject
    private AtlasGraph graph;

    @BeforeMethod
    public void releaseClaimsLeftByOtherTests() {
        for (String claimName : COORDINATED_CLAIMS) {
            clearClaim(claimName);
        }
    }

    @AfterMethod
    public void releaseClaimsTakenByThisTest() {
        for (String claimName : COORDINATED_CLAIMS) {
            clearClaim(claimName);
        }
    }

    /**
     * The activities a starting node coordinates are independent, so a node doing one must not shut
     * peers out of the others.  If two of them named the same claim, the cluster would quietly run
     * them one after another - correct-looking, and slow in a way nothing reports.
     */
    @Test
    public void everyCoordinatedActivityHasAClaimOfItsOwn() {
        for (int activity = 0; activity < COORDINATED_CLAIMS.length; activity++) {
            String claimName = COORDINATED_CLAIMS[activity];
            String ownerId   = "node-doing-" + activity;

            assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, ownerId, ONE_MINUTE),
                    claimName + " could not be taken while other activities were under way, so it shares a name with one of them");
        }

        for (int activity = 0; activity < COORDINATED_CLAIMS.length; activity++) {
            assertEquals(ownerOf(COORDINATED_CLAIMS[activity]), "node-doing-" + activity,
                    COORDINATED_CLAIMS[activity] + " ended up held by the wrong activity");
        }
    }

    /**
     * Index setup and index recovery share one claim on purpose, so that recovery never runs against a
     * half-built index.  Both reach it through the same entry point, and the sharing is easy to
     * mistake for an oversight, so it is worth stating as a requirement.
     */
    @Test
    public void indexSetupAndIndexRecoveryContendForOneClaim() {
        RecoveryInfoManagement indexLease = new RecoveryInfoManagement(graph);

        assertTrue(indexLease.tryClaimOwnership("node-building-the-index", ONE_MINUTE));

        assertFalse(indexLease.tryClaimOwnership("node-recovering-the-index", ONE_MINUTE),
                "recovery must not start while another node is still building the index");
        assertTrue(indexLease.isOwner("node-building-the-index"));
        assertFalse(indexLease.isOwner("node-recovering-the-index"));

        indexLease.releaseOwnership("node-building-the-index");

        assertTrue(indexLease.tryClaimOwnership("node-recovering-the-index", ONE_MINUTE),
                "once the index is built the lease should pass to recovery");
    }

    /**
     * Patches are claimed one name each rather than under a single "patching" claim, so a node applying
     * one patch does not stop peers applying the others.
     */
    @Test
    public void patchesAreClaimedIndividuallySoUnrelatedOnesStillRun() {
        String onePatch     = Constants.CLAIM_PATCH_PREFIX + "PATCH_001";
        String anotherPatch = Constants.CLAIM_PATCH_PREFIX + "PATCH_002";

        try {
            assertTrue(GraphClaim.claimLeaseAndCommit(graph, onePatch, "node-1", ONE_MINUTE));
            assertTrue(GraphClaim.claimLeaseAndCommit(graph, anotherPatch, "node-2", ONE_MINUTE),
                    "a peer applying a different patch is not competing for this one");

            assertFalse(GraphClaim.claimLeaseAndCommit(graph, onePatch, "node-2", ONE_MINUTE),
                    "the same patch must not be applied by two nodes at once");
        } finally {
            clearClaim(onePatch);
            clearClaim(anotherPatch);
        }
    }

    /**
     * Loading the bootstrap models is done by one node while the others read the finished types back,
     * so a peer has to tell "someone is loading, wait for them" from "nobody is, go ahead".  A refused
     * claim alone does not distinguish the two, which is why the holder is asked about separately.
     */
    @Test
    public void aPeerCanTellAWorkingHolderFromNoHolderAtAll() {
        String bootstrap = Constants.CLAIM_TYPEDEF_BOOTSTRAP;

        assertFalse(GraphClaim.hasLiveHolder(graph, bootstrap), "nobody has started, so there is nobody to wait for");

        assertTrue(GraphClaim.claimLeaseAndCommit(graph, bootstrap, "loading-node", ONE_MINUTE));
        assertTrue(GraphClaim.hasLiveHolder(graph, bootstrap), "a peer must wait while the models are being loaded");

        GraphClaim.releaseLeaseAndCommit(graph, bootstrap, "loading-node");
        assertFalse(GraphClaim.hasLiveHolder(graph, bootstrap), "loading finished, so there is nothing left to wait for");
    }

    /**
     * The reason waiting is conditional on the lease rather than on someone's name being present: a
     * node that died mid-load leaves its claim behind, and peers that waited for a name would wait for
     * a node that is never coming back.
     */
    @Test
    public void aPeerDoesNotWaitForAHolderThatStoppedHonouringItsLease() throws InterruptedException {
        String bootstrap = Constants.CLAIM_TYPEDEF_BOOTSTRAP;

        assertTrue(GraphClaim.claimLeaseAndCommit(graph, bootstrap, "node-that-died", ALREADY_EXPIRED));

        Thread.sleep(10L);

        assertFalse(GraphClaim.hasLiveHolder(graph, bootstrap),
                "a lapsed claim is not somebody working, and waiting for it would stall every peer");
        assertTrue(GraphClaim.claimLeaseAndCommit(graph, bootstrap, "peer-node", ONE_MINUTE));
    }

    /**
     * These are the claim names the product itself uses, so anything left behind here is picked up by
     * the next test class as a peer still working.  The read has to start a fresh transaction: a
     * snapshot this thread opened earlier will not show a claim committed since, and the cleanup would
     * then quietly decide there was nothing to release.
     */
    private void clearClaim(String claimName) {
        graph.commit();

        String owner = GraphClaim.claimedBy(GraphClaim.holderOf(graph, claimName));

        if (owner != null) {
            GraphClaim.releaseLeaseAndCommit(graph, claimName, owner);
        }

        graph.commit();
    }

    private String ownerOf(String claimName) {
        graph.commit();

        return GraphClaim.claimedBy(GraphClaim.holderOf(graph, claimName));
    }
}
