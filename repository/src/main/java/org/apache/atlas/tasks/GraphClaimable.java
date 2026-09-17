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

import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasVertex;

/**
 * Contract for any Atlas subsystem that performs a deferred action
 * (task execution, async import, purge, index recovery) in an
 * active-active cluster where only ONE node must process each work
 * item at a time.
 *
 * <h3>The CAS Pattern</h3>
 * Inside a single {@link GraphTransaction}, find a work item in its
 * <em>claimable</em> state (e.g. {@code PENDING}, {@code WAITING}) and
 * write the <em>claimed</em> state (e.g. {@code IN_PROGRESS},
 * {@code PROCESSING}).
 *
 * <p>The compare must be performed by the store, via
 * {@link GraphClaim}.  Writing a status field is not by itself a swap:
 * two nodes can both read the claimable state and both write, and
 * neither write fails.  Whether that is caught depends entirely on the
 * backend, which is not something a caller should be reasoning about.
 *
 * <h3>What a caller may assume</h3>
 * Only that a claim attempt returns the claimed item, or {@code null} /
 * {@code false} when the item was not claimable — whether because
 * nothing was available or because another node won.
 *
 * <p>Callers must <em>not</em> assume which mechanism decided that, or
 * when.  Backends differ: some refuse the write immediately, others only
 * at commit, and some report contention as a retryable locking failure
 * instead.  Wrapping the attempt in
 * {@link GraphClaim#attempt(GraphClaim.ClaimAttempt)} collapses all
 * of those into {@code null}.
 *
 * <h3>Known implementations</h3>
 * <ul>
 *   <li>{@code TaskRegistry#claimNextPendingTask()} — claims the next
 *       queued async task ({@code PENDING → IN_PROGRESS}).</li>
 *   <li>{@code AsyncImportService#claimNextWaitingImport()} — claims
 *       the next queued import ({@code WAITING → PROCESSING}).</li>
 *   <li>{@code AtlasPatchRegistry#tryClaimPatchExecution()} — claims a
 *       patch for the duration of its application.</li>
 *   <li>{@code PurgeService} and {@code IndexRecoveryService} — guard a
 *       single shared resource, so they claim through
 *       {@link GraphLeaseClaimable}.</li>
 * </ul>
 *
 * @param <T> the type returned on successful claim (e.g. {@code AtlasTask},
 *            {@code AtlasAsyncImportRequest}); use {@link Boolean} for
 *            boolean-result claims.
 */
public interface GraphClaimable<T> {
    /**
     * What this claimant serialises on.  Every node competing for the same work must return the same
     * name, because uniqueness of the name is what admits exactly one of them.
     */
    String claimName();

    /**
     * Runs {@link #tryClaim()} and reports a lost race as {@code null}, whichever backend decided it
     * and whenever it was decided.  This is the method callers should use.
     */
    default T attemptClaim() throws AtlasBaseException {
        return GraphClaim.attempt(this::tryClaim);
    }

    /**
     * Records this claimant's claim on {@code holder}, for implementations of {@link #tryClaim()}.
     *
     * <p>{@code holder} must be a vertex belonging to the work item being claimed - uniqueness
     * distinguishes vertices, so a claim written to a vertex that every node shares excludes
     * nobody.  Claimants guarding a single shared resource should use {@link GraphLeaseClaimable}
     * instead of calling this.
     *
     * @throws ClaimConflictException if another node holds the claim
     */
    default void takeClaim(AtlasVertex holder, String ownerId) {
        GraphClaim.claim(holder, claimName(), ownerId);
    }

    /**
     * Takes over a claim abandoned by a node that died mid-work, for {@link #recoverStaleClaims()}.
     * Use this rather than {@link #takeClaim} when the holder already carries someone else's claim.
     *
     * @throws ClaimConflictException if another node recovered it first
     */
    default void takeOverClaim(AtlasVertex holder, String ownerId) {
        GraphClaim.takeOverClaim(holder, claimName(), ownerId);
    }

    /** Gives the claim back, so the next claimant can take it. */
    default void releaseClaim(AtlasVertex holder) {
        GraphClaim.releaseClaim(holder);
    }

    /**
     * Atomically claims the next available work item by transitioning its
     * status from the <em>claimable</em> state to the <em>claimed</em>
     * state inside a single {@link GraphTransaction}.
     *
     * <p>Only one node in the cluster claims a given item.  All others must
     * not execute the action.
     *
     * <p>Implementations may signal a lost race either by returning
     * {@code null} / {@code false} or by throwing, since the store may only
     * refuse the write once the transaction commits.  Call through
     * {@link GraphClaim#attempt(GraphClaim.ClaimAttempt)} to see a single
     * outcome.
     *
     * @return the claimed item on success, or {@code null} / {@code false}
     *         when nothing is claimable (no item in claimable state, or
     *         another node already claimed it)
     * @throws AtlasBaseException if an unrecoverable error occurs during
     *         the claim attempt
     */
    T tryClaim() throws AtlasBaseException;

    /**
     * Performs implementation-specific stale-claim recovery before a claim
     * attempt. Implementations that don't need recovery can keep the default
     * no-op behavior.
     *
     * @throws AtlasBaseException if an unrecoverable error occurs during
     *         recovery
     */
    default void recoverStaleClaims() throws AtlasBaseException {
    }
}
