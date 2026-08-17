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

/**
 * Contract for any Atlas subsystem that performs a deferred action
 * (task execution, async import, purge, index recovery) in an
 * active-active cluster where only ONE node must process each work
 * item at a time.
 *
 * <h3>The CAS Pattern</h3>
 * Every implementation follows the same Compare-And-Swap idiom backed
 * by JanusGraph/HBase row-level optimistic locking:
 * <ol>
 *   <li>Inside a single {@link GraphTransaction}, query for the work
 *       item in its <em>claimable</em> state (e.g. {@code PENDING},
 *       {@code WAITING}).</li>
 *   <li>If found, write the <em>claimed</em> state (e.g.
 *       {@code IN_PROGRESS}, {@code PROCESSING}) atomically.</li>
 *   <li>Commit.  JanusGraph's HBase locking ensures only one node's
 *       commit succeeds.  The loser gets a
 *       {@code PermanentLockingException}; the
 *       {@code GraphTransactionInterceptor} retries; on retry the item
 *       is no longer in the claimable state, so the method returns
 *       {@code null} / {@code false}.</li>
 * </ol>
 *
 * <h3>Known implementations</h3>
 * <ul>
 *   <li>{@code TaskRegistry#tryClaimTask(String)} — claims a single
 *       async task ({@code PENDING → IN_PROGRESS}).</li>
 *   <li>{@code AsyncImportService#claimNextWaitingImport()} — claims
 *       the next queued import ({@code WAITING → PROCESSING}).</li>
 * </ul>
 *
 * <h3>Open items following the same pattern</h3>
 * <ul>
 *   <li>PurgeService — purge-owner vertex (single-instance guard).</li>
 *   <li>IndexRecoveryService — recovery-owner vertex.</li>
 *   <li>DataMigrationService — migration-owner vertex.</li>
 * </ul>
 *
 * @param <T> the type returned on successful claim (e.g. {@code AtlasTask},
 *            {@code AtlasAsyncImportRequest}); use {@link Boolean} for
 *            boolean-result claims.
 */
public interface GraphClaimable<T> {
    /**
     * Atomically claims the next available work item by transitioning its
     * status from the <em>claimable</em> state to the <em>claimed</em>
     * state inside a single {@link GraphTransaction}.
     *
     * <p>Only the node whose transaction commits first proceeds to execute
     * the deferred action.  All other nodes receive {@code null} or
     * {@code false} and must not execute the action.
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
