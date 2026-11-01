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
package org.apache.atlas.tasks;

import org.apache.atlas.repository.graphdb.AtlasGraph;

/**
 * A {@link GraphClaimable} that guards one shared resource for as long as it keeps the lease -
 * purge, index recovery, index initialization - rather than claiming individual work items.
 *
 * <h3>Why these cannot mark a vertex directly</h3>
 * A claimant of individual work items marks the item's own vertex, and uniqueness of the claim name
 * keeps the other nodes off it.  A guard of a shared resource has no such vertex: every node would
 * be writing to the same one, and writing a claim to a vertex that already carries it changes
 * nothing.  So the claim is recorded on a vertex made for the purpose, and creating that vertex is
 * what the store adjudicates.
 *
 * <h3>Leases</h3>
 * The holder works for as long as it likes and may die without releasing, so the claim carries a
 * timestamp and is refreshed by calling {@link #tryClaim()} again.  Peers leave it alone until it
 * goes stale.  {@link #leaseMillis()} therefore answers "how long after the holder falls silent may
 * another node take over", which is a liveness question, not a duration-of-work one: renew well
 * inside it, and do not set it so short that a slow holder is displaced while still working.
 *
 * <p>Implementations supply three things and inherit the rest.
 */
public interface GraphLeaseClaimable extends GraphClaimable<Boolean> {
    AtlasGraph graph();

    /** Identifies this node, and must stay stable for as long as it holds the claim. */
    String ownerId();

    /** How long a silent holder keeps the claim before a peer may take it over. */
    long leaseMillis();

    /**
     * Takes the lease, or renews it if this node already holds it.
     *
     * @return {@code true} if this node holds the claim and may proceed
     */
    @Override
    default Boolean tryClaim() {
        return GraphClaim.claimLeaseAndCommit(graph(), claimName(), ownerId(), leaseMillis());
    }

    /**
     * Gives up the lease on a clean shutdown, so a peer can take over immediately instead of waiting
     * out the lease.  Does nothing if this node no longer holds it.
     */
    default void releaseClaim() {
        GraphClaim.releaseLeaseAndCommit(graph(), claimName(), ownerId());
    }

    /**
     * Whether this node still holds the claim.  Worth re-checking during long stretches of work,
     * since a holder that stopped renewing may have been displaced.
     */
    default boolean holdsClaim() {
        return GraphClaim.holdsLease(graph(), claimName(), ownerId());
    }
}
