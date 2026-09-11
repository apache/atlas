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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Claim behaviour against a real graph, rather than against a mocked one.
 *
 * <p>{@link GraphClaimTest} covers the same code with a mocked graph, which can only show that a
 * refusal is <em>interpreted</em> correctly once the store issues one.  These tests use the store, so
 * they also show that the claim key really is registered as unique, that a refusal really arrives,
 * and that dropping a claim leaves the name usable - none of which a mock can demonstrate.
 *
 * <h3>What is deliberately not covered here</h3>
 * The property these mechanisms exist for is that nodes reading a free claim <em>at the same
 * instant</em> cannot all take it.  That cannot be tested on this backend.  Unit tests run on
 * BerkeleyJE, which reports transaction isolation to JanusGraph, so JanusGraph does not engage its
 * key locker and the uniqueness constraint is only checked against already-committed data.  A race
 * measured here admits several winners and leaves several holder vertices, whatever the code does.
 *
 * <p>The production backends do arbitrate: the rdbms store refuses the duplicate at the write via a
 * uniqueness side table, and HBase has no transaction isolation so JanusGraph locks and refuses at
 * commit.  Verifying that needs one of those backends and is out of scope for a unit test.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class GraphClaimConcurrencyTest {
    private static final long ONE_MINUTE      = 60000L;
    private static final long ALREADY_EXPIRED = 1L;
    private static final int  NODES           = 8;

    @Inject
    private AtlasGraph graph;

    /**
     * That the claim key is registered unique at all, and that the store's refusal is recognised as a
     * lost claim rather than escaping as a fault.  The mocked test asserts the classification of an
     * exception it was handed; this one asserts the store produces that exception.
     */
    @Test
    public void aClaimAlreadyHeldIsRefusedToASecondVertex() throws AtlasBaseException {
        String      claimName = "ATLAS_TEST_DUPLICATE";
        AtlasVertex first     = graph.addVertex();

        GraphClaim.claim(first, claimName, "first-node");
        graph.commit();

        Boolean tookIt = GraphClaim.attempt(() -> {
            GraphClaim.claim(graph.addVertex(), claimName, "second-node");
            graph.commit();

            return Boolean.TRUE;
        });

        graph.rollback();

        assertNull(tookIt, "a claim name already on another vertex must be refused by the store");
        assertEquals(holdersOf(claimName), 1);
        assertEquals(ownerOf(claimName), "first-node");
    }

    /**
     * Contention must be confined to the claim being contended.  Patches are claimed one name each so
     * unrelated ones can still run in parallel; if claiming serialised every claimant regardless of
     * name, that parallelism would quietly disappear.
     */
    @Test
    public void separateClaimsAreNotInEachOthersWay() throws Exception {
        List<Boolean> outcomes = race(NODES, ownerId -> GraphClaim.claimLeaseAndCommit(graph, "ATLAS_TEST_OWN_" + ownerId, ownerId, ONE_MINUTE));

        assertEquals(winnersIn(outcomes), NODES, "nodes claiming different names are not competing and all should succeed");

        for (int node = 0; node < NODES; node++) {
            assertEquals(holdersOf("ATLAS_TEST_OWN_node-" + node), 1);
        }
    }

    /**
     * A holder that is still working must be left alone, however many peers check on it at once.  This
     * is the case peers get wrong by reasoning about elapsed time instead of the lease: an earlier
     * rule reclaimed anything claimed before the checking node started, so a node joining later
     * declared every in-flight claim abandoned.
     */
    @Test
    public void aLiveLeaseIsLeftAloneByEveryPeer() throws Exception {
        String claimName = "ATLAS_TEST_HELD";

        assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, "working-node", ONE_MINUTE));

        List<Boolean> outcomes = race(NODES, ownerId -> GraphClaim.claimLeaseAndCommit(graph, claimName, ownerId, ONE_MINUTE));

        assertEquals(winnersIn(outcomes), 0, "a lease that has not lapsed belongs to its holder");
        assertEquals(ownerOf(claimName), "working-node");
        assertEquals(holdersOf(claimName), 1);
    }

    /**
     * A node that dies holding a claim must not block the cluster forever.  Taking over drops the
     * abandoned claim and re-adds it, so this also covers the drop leaving the name usable.
     */
    @Test
    public void aLapsedLeaseIsTakenOverByAPeer() throws InterruptedException {
        String claimName = "ATLAS_TEST_ABANDONED";

        assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, "node-that-died", ALREADY_EXPIRED));

        Thread.sleep(10L);

        assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, "peer-node", ONE_MINUTE),
                "a lease nobody is honouring any more must be available to a peer");
        assertEquals(ownerOf(claimName), "peer-node");
        assertEquals(holdersOf(claimName), 1);
    }

    /**
     * Releasing removes the claim vertex, and removing a vertex can leave its uniqueness entry behind.
     * A stranded entry is invisible - nothing holds the claim - but the name can never be taken again,
     * so the cluster would stall after one clean handover.
     */
    @Test
    public void aReleasedClaimCanBeTakenAgain() {
        String claimName = "ATLAS_TEST_HANDOVER";

        assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, "first-node", ONE_MINUTE));

        GraphClaim.releaseLeaseAndCommit(graph, claimName, "first-node");

        assertEquals(holdersOf(claimName), 0, "a released claim should be held by nobody");
        assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, "second-node", ONE_MINUTE),
                "the name must still be claimable, or the release stranded its uniqueness entry");
        assertEquals(ownerOf(claimName), "second-node");
    }

    /**
     * Repeated handovers must leave the graph as they found it.  A claim vertex leaked per round would
     * go unnoticed until the store had accumulated a great many of them.
     */
    @Test
    public void handingTheClaimOnRepeatedlyLeavesNothingBehind() {
        String claimName = "ATLAS_TEST_ROUNDS";

        for (int round = 0; round < 5; round++) {
            String owner = "node-" + round;

            assertTrue(GraphClaim.claimLeaseAndCommit(graph, claimName, owner, ONE_MINUTE), "round " + round + " could not take the claim");
            assertEquals(holdersOf(claimName), 1, "round " + round + " left more than one holder");

            GraphClaim.releaseLeaseAndCommit(graph, claimName, owner);

            assertEquals(holdersOf(claimName), 0, "round " + round + " did not give the claim back");
        }
    }

    /**
     * Runs one attempt per node with every thread held at a barrier until the last one arrives, so the
     * attempts overlap instead of being spread out by thread startup.
     */
    private List<Boolean> race(int nodes, Function<String, Boolean> attempt) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        CyclicBarrier   gate = new CyclicBarrier(nodes);

        try {
            List<Future<Boolean>> attempts = new ArrayList<>(nodes);

            for (int node = 0; node < nodes; node++) {
                String ownerId = "node-" + node;

                attempts.add(pool.submit(() -> {
                    gate.await();

                    return attempt.apply(ownerId);
                }));
            }

            List<Boolean> outcomes = new ArrayList<>(nodes);

            for (Future<Boolean> attemptResult : attempts) {
                outcomes.add(attemptResult.get());
            }

            return outcomes;
        } finally {
            pool.shutdownNow();
        }
    }

    private static int winnersIn(List<Boolean> outcomes) {
        return (int) outcomes.stream().filter(Boolean::booleanValue).count();
    }

    /**
     * How many vertices carry the claim.  Reading it needs a transaction opened after the claimants
     * committed theirs, or this thread's snapshot predates the work it is checking on.
     */
    private int holdersOf(String claimName) {
        graph.commit();

        Iterator<AtlasVertex> holders = graph.query().has(Constants.CLAIM_KEY, claimName).vertices().iterator();
        int                   held    = 0;

        while (holders.hasNext()) {
            holders.next();

            held++;
        }

        return held;
    }

    private String ownerOf(String claimName) {
        graph.commit();

        return GraphClaim.claimedBy(GraphClaim.holderOf(graph, claimName));
    }
}
