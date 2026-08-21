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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Iterator;

/**
 * The Compare-And-Swap behind {@link GraphClaimable}, with the compare performed by the graph
 * store instead of by the claimant.
 *
 * <h3>Why the status field is not enough</h3>
 * Reading a claimable status, concluding it is free, and writing your own marker is not a swap:
 * the read and the write are separate steps, so two nodes can both read "free" and both write.
 * Neither write fails, because a plain property write has nothing to fail against - it can only
 * overwrite.  Row locking does not help; it decides <em>when</em> the second node writes, not
 * whether it is allowed to.
 *
 * <h3>What makes the compare real</h3>
 * {@link Constants#CLAIM_KEY} is registered as a globally unique property key, so writing a claim
 * name is a write that can be refused.  Exactly one vertex in the cluster may hold a given name,
 * and the losing claimant is told so by the store.  Because every claimant writes the same name,
 * the conflict arises whether the nodes picked the same work item or different ones.
 *
 * <h3>Where the claim is recorded matters</h3>
 * Uniqueness discriminates between <em>vertices</em>, not between writers of one vertex - writing a
 * claim that is already present is a no-op, and overwriting one drops the old uniqueness entry
 * first.  A claim recorded on a vertex every node shares is therefore not exclusive at all.  So:
 * <ul>
 *   <li>{@link #claimLease} is the choice whenever two nodes might go after the same thing, which
 *       includes anything picked by a query rather than owned outright.  It records the claim on a
 *       vertex it creates, so the store adjudicates the creation and only one node can win, whatever
 *       each of them had in mind.</li>
 *   <li>{@link #claim} marks an existing vertex, and is exclusive only between nodes marking
 *       <em>different</em> vertices.  Nodes that both mark the same one write the same entry and both
 *       writes stand.  The rdbms side table happens to refuse the second write, which makes this the
 *       kind of mistake that passes on one backend and not the other.</li>
 * </ul>
 *
 * <h3>Backends</h3>
 * The refusal arrives differently depending on the store, and callers should not have to care:
 * <ul>
 *   <li><b>rdbms</b> - the uniqueness entry is inserted as the property is written, so the
 *       conflict is raised inside {@link #claim} and surfaces as {@link ClaimConflictException}.</li>
 *   <li><b>JanusGraph composite index</b> - uniqueness is only checked at commit, well after
 *       {@code claim} has returned, so the conflict surfaces from the surrounding transaction as a
 *       schema violation (or as a locking exception that the transaction interceptor retries, at
 *       which point the item is already taken).</li>
 * </ul>
 * Route claim attempts through {@link #attempt} and both cases come back as {@code null}.
 */
public final class GraphClaim {
    private static final Logger LOG = LoggerFactory.getLogger(GraphClaim.class);

    private static final String POSTGRES_UNIQUE_VIOLATION_SQL_STATE  = "23505";
    private static final String INTEGRITY_CONSTRAINT_SQL_STATE_CLASS = "23";

    private GraphClaim() {
    }

    /**
     * Runs a claim attempt and reports a lost race as {@code null}, whichever backend decided it
     * and whenever it was decided.
     *
     * <p>This must wrap the whole claim call rather than sit inside it, because on some backends
     * the store only refuses the write when the surrounding transaction commits - which happens
     * after the claiming method has already returned.
     *
     * @return whatever the attempt produced, or {@code null} if another node holds the claim
     */
    public static <T> T attempt(ClaimAttempt<T> claimAttempt) throws AtlasBaseException {
        try {
            return claimAttempt.get();
        } catch (Exception exception) {
            if (isClaimConflict(exception)) {
                LOG.debug("GraphClaim: another node holds the claim");

                return null;
            }

            throw exception;
        }
    }

    /**
     * Takes the named claim on behalf of {@code ownerId}, using {@code holder} as the vertex that
     * records it.
     *
     * @throws ClaimConflictException if another vertex already holds this claim name; the caller
     *         has claimed nothing and its transaction must roll back
     */
    public static void claim(AtlasVertex holder, String claimName, String ownerId) {
        try {
            AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_KEY, claimName);
            AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_OWNER_KEY, ownerId);
            AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_TIME_KEY, System.currentTimeMillis());
        } catch (Exception exception) {
            if (isUniquenessViolation(exception)) {
                LOG.debug("GraphClaim: claim '{}' already held, node={} did not take it", claimName, ownerId);

                throw new ClaimConflictException(claimName, exception);
            }

            throw exception;
        }
    }

    /**
     * Gives up the claim recorded on {@code holder}, letting the next claimant take it.  Safe to
     * call on a vertex holding nothing, so callers can release unconditionally on every exit path
     * rather than tracking whether they claimed.
     */
    /**
     * Takes a claim another node left behind, for stale-work recovery.
     *
     * <p>Overwriting the previous holder's name in place would not be adjudicated - a claim is only
     * refused when it is added to a vertex that lacks one - so the abandoned claim is dropped first
     * and re-added.  Several nodes may notice the same abandoned claim and all drop it; the re-add
     * still admits only one of them.
     *
     * @throws ClaimConflictException if another node took it over first
     */
    public static void takeOverClaim(AtlasVertex holder, String claimName, String ownerId) {
        if (heldClaim(holder) != null && !StringUtils.equals(ownerId, claimedBy(holder))) {
            releaseClaim(holder);
        }

        claim(holder, claimName, ownerId);
    }

    public static void releaseClaim(AtlasVertex holder) {
        if (holder == null || heldClaim(holder) == null) {
            return;
        }

        // Writing null removes the uniqueness entry as well as the property.
        AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_KEY, null);
        AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_OWNER_KEY, null);
        AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_TIME_KEY, null);
        AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_EXPIRY_KEY, null);
    }

    public static String heldClaim(AtlasVertex holder) {
        return holder == null ? null : AtlasGraphUtilsV2.getEncodedProperty(holder, Constants.CLAIM_KEY, String.class);
    }

    public static String claimedBy(AtlasVertex holder) {
        return holder == null ? null : AtlasGraphUtilsV2.getEncodedProperty(holder, Constants.CLAIM_OWNER_KEY, String.class);
    }

    public static Long claimedAt(AtlasVertex holder) {
        return holder == null ? null : AtlasGraphUtilsV2.getEncodedProperty(holder, Constants.CLAIM_TIME_KEY, Long.class);
    }

    /** When the holder's lease lapses, or {@code null} for claims held until explicitly released. */
    public static Long expiryOf(AtlasVertex holder) {
        return holder == null ? null : AtlasGraphUtilsV2.getEncodedProperty(holder, Constants.CLAIM_EXPIRY_KEY, Long.class);
    }

    /**
     * The vertex currently holding {@code claimName}, or {@code null} if nobody holds it.  There can
     * only be one, which is the whole point.
     */
    public static AtlasVertex holderOf(AtlasGraph graph, String claimName) {
        Iterator<AtlasVertex> holders = graph.query().has(Constants.CLAIM_KEY, claimName).vertices().iterator();

        return holders.hasNext() ? holders.next() : null;
    }

    // ------------------------------------------------------------------ leases

    /**
     * Takes or renews a lease on a shared resource, for claimants with no vertex of their own to
     * mark.  The claim is recorded on a purpose-made vertex, so that creating it - rather than
     * overwriting a field every node can overwrite - is what the store adjudicates.
     *
     * <p>Leases exist because the holder may die without releasing.  An expired claim is dropped so
     * it can be taken again; if several nodes notice the expiry together they all drop it, and the
     * subsequent create still admits only one of them.
     *
     * @return {@code true} if this node now holds the claim, {@code false} if another node holds a
     *         lease that has not yet expired
     */
    public static boolean claimLease(AtlasGraph graph, String claimName, String ownerId, long leaseMillis) {
        AtlasVertex holder = holderOf(graph, claimName);
        long        now    = System.currentTimeMillis();

        if (holder != null) {
            String currentOwner = claimedBy(holder);
            Long   expiresAt    = expiryOf(holder);

            if (StringUtils.equals(ownerId, currentOwner)) {
                setLeaseWindow(holder, now, leaseMillis);

                LOG.debug("GraphClaim: renewed lease on '{}' for node={}", claimName, ownerId);

                return true;
            }

            if (expiresAt != null && expiresAt > now) {
                LOG.debug("GraphClaim: lease on '{}' is held by node={} until {}, not taking it",
                        claimName, currentOwner, expiresAt);

                return false;
            }

            LOG.warn("GraphClaim: lease on '{}' held by node={} lapsed at {}, reclaiming for node={}",
                    claimName, currentOwner, expiresAt, ownerId);

            discardClaimVertex(graph, holder);
        }

        try {
            AtlasVertex claimVertex = graph.addVertex();

            AtlasGraphUtilsV2.setEncodedProperty(claimVertex, Constants.CLAIM_VERTEX_TYPE_KEY, Constants.CLAIM_VERTEX_TYPE_NAME);

            claim(claimVertex, claimName, ownerId);
            setLeaseWindow(claimVertex, now, leaseMillis);

            LOG.info("GraphClaim: node={} took lease on '{}' until {}", ownerId, claimName, now + leaseMillis);

            return true;
        } catch (ClaimConflictException conflict) {
            LOG.debug("GraphClaim: node={} lost the race for lease on '{}'", ownerId, claimName);

            return false;
        }
    }

    /**
     * Takes or renews a lease and commits it, reporting a lost race as {@code false}.
     *
     * <p>The commit is part of taking the claim, not an afterthought: a claim no other node can see
     * excludes nobody, and on backends without a uniqueness side table the store only gets to refuse
     * the claim <em>at</em> commit.  This is the entry point for callers that are not already inside a
     * transaction; use {@link #claimLease} directly when they are.
     */
    public static boolean claimLeaseAndCommit(AtlasGraph graph, String claimName, String ownerId, long leaseMillis) {
        try {
            if (!claimLease(graph, claimName, ownerId, leaseMillis)) {
                // A refused claim can leave the transaction unable to commit - the rdbms store marks it
                // rollback-only when the store rejects the write - and committing it anyway turns an
                // ordinary lost race into an unexplained commit failure.  There is nothing to keep.
                rollbackQuietly(graph);

                return false;
            }

            graph.commit();

            return true;
        } catch (Exception exception) {
            rollbackQuietly(graph);

            if (isClaimConflict(exception) || isHeldByAnotherNode(graph, claimName, ownerId)) {
                LOG.debug("GraphClaim: node={} lost the race for lease on '{}'", ownerId, claimName);
            } else {
                // Any other failure is reported the same way, because it has the same answer: this node
                // does not hold the claim.  Guessing otherwise is the one outcome that is unsafe.  The
                // detail is kept for the case where it really was the infrastructure and not a peer.
                LOG.warn("GraphClaim: node={} could not confirm a lease on '{}'; treating it as not held",
                        ownerId, claimName, exception);
            }

            return false;
        }
    }

    /** Gives up a lease and commits, so a peer can take over without waiting for it to lapse. */
    public static void releaseLeaseAndCommit(AtlasGraph graph, String claimName, String ownerId) {
        try {
            releaseLease(graph, claimName, ownerId);

            graph.commit();
        } catch (Exception exception) {
            rollbackQuietly(graph);

            if (isClaimConflict(exception)) {
                // The store refused the release because the claim was no longer there to release -
                // already given up, or taken over after lapsing.  Either way it is not ours any more,
                // which is exactly what this call was trying to achieve.
                LOG.debug("GraphClaim: node={} found no lease of its own to release on '{}'", ownerId, claimName);
            } else {
                LOG.warn("GraphClaim: node={} could not release the lease on '{}'; it will lapse instead",
                        ownerId, claimName, exception);
            }
        }
    }

    /**
     * Asks the store who holds the claim, for a failure whose cause does not say.
     *
     * <p>A store can refuse a claim in ways that look nothing like a constraint violation: the rdbms
     * store surfaces the refused commit as a failure to roll back, which discards the original cause.
     * Rather than read the wreckage, this asks the only question that matters - is the claim someone
     * else's now - so that a lost race is reported as one instead of as a fault.
     */
    private static boolean isHeldByAnotherNode(AtlasGraph graph, String claimName, String ownerId) {
        try {
            String holder = claimedBy(holderOf(graph, claimName));

            return StringUtils.isNotBlank(holder) && !StringUtils.equals(holder, ownerId);
        } catch (Exception exception) {
            LOG.debug("GraphClaim: could not read the holder of '{}' after a failed claim", claimName, exception);

            return false;
        }
    }

    /**
     * Rolls back without letting the rollback itself become the failure.  A store that has just failed
     * a commit may be in no state to roll back either, and losing the claim outcome to a secondary
     * error would turn "someone else has it" into a startup failure.
     */
    private static void rollbackQuietly(AtlasGraph graph) {
        try {
            graph.rollback();
        } catch (Exception rollbackFailure) {
            LOG.debug("GraphClaim: rollback after a failed claim did not complete", rollbackFailure);
        }
    }

    private static void setLeaseWindow(AtlasVertex holder, long now, long leaseMillis) {
        AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_TIME_KEY, now);
        AtlasGraphUtilsV2.setEncodedProperty(holder, Constants.CLAIM_EXPIRY_KEY, now + leaseMillis);
    }

    /**
     * Gives up a lease taken by {@link #claimLease}, but only if this node still holds it - a node
     * whose lease already expired and was reclaimed must not disturb the new holder.
     */
    public static void releaseLease(AtlasGraph graph, String claimName, String ownerId) {
        AtlasVertex holder = holderOf(graph, claimName);

        if (holder == null) {
            return;
        }

        if (!StringUtils.equals(ownerId, claimedBy(holder))) {
            LOG.debug("GraphClaim: not releasing '{}', it is held by node={} rather than node={}",
                    claimName, claimedBy(holder), ownerId);

            return;
        }

        discardClaimVertex(graph, holder);

        LOG.info("GraphClaim: node={} released lease on '{}'", ownerId, claimName);
    }

    /**
     * Whether any node holds the named lease and is still honouring it.  Lets a caller tell "a peer is
     * working on this" from "nobody is", which are the same answer to a claim attempt but call for
     * different behaviour: wait for the first, look into the second.
     */
    public static boolean hasLiveHolder(AtlasGraph graph, String claimName) {
        Long expiresAt = expiryOf(holderOf(graph, claimName));

        return expiresAt != null && expiresAt > System.currentTimeMillis();
    }

    /**
     * Whether this node holds the named lease <em>and</em> it has not lapsed.  A holder that fell
     * behind on renewals must assume a peer has taken over, so this answers "may I keep going" rather
     * than "is my name on it".
     */
    public static boolean holdsLease(AtlasGraph graph, String claimName, String ownerId) {
        AtlasVertex holder = holderOf(graph, claimName);

        if (!StringUtils.equals(ownerId, claimedBy(holder))) {
            return false;
        }

        Long expiresAt = expiryOf(holder);

        return expiresAt != null && expiresAt > System.currentTimeMillis();
    }

    /**
     * Drops a claim and, when the vertex exists only to carry it, the vertex too.  The claim must be
     * released before the vertex goes: removing a vertex leaves its uniqueness entry behind, and a
     * stranded entry means nobody can ever claim that name again.
     */
    private static void discardClaimVertex(AtlasGraph graph, AtlasVertex holder) {
        boolean isClaimOnlyVertex = Constants.CLAIM_VERTEX_TYPE_NAME.equals(
                AtlasGraphUtilsV2.getEncodedProperty(holder, Constants.CLAIM_VERTEX_TYPE_KEY, String.class));

        releaseClaim(holder);

        if (isClaimOnlyVertex) {
            graph.removeVertex(holder);
        }
    }

    /**
     * Whether a failure means some other node holds the claim, covering both the conflict raised
     * inside {@link #claim} and the one raised later at commit.
     */
    public static boolean isClaimConflict(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof ClaimConflictException) {
                return true;
            }

            if (cause.getCause() == cause) {
                break;
            }
        }

        return isUniquenessViolation(throwable);
    }

    /**
     * Recognises the store refusing a duplicate claim name.  Each backend has its own way of saying
     * it, and the differences are not cosmetic:
     *
     * <ul>
     *   <li>The rdbms store keeps uniqueness in a side table of its own, so a refused claim arrives
     *       as the database's integrity-constraint error, at the moment of the write.</li>
     *   <li>Other backends have no such side table - {@code getUniqueKeyHandler()} is null for them -
     *       and rely on JanusGraph's unique composite index instead.  That index is marked
     *       {@code ConsistencyModifier.LOCK}, so the refusal comes at commit, and it comes as either
     *       a schema violation or as lock contention depending on whether JanusGraph spotted the
     *       duplicate itself or simply failed to get the lock.</li>
     * </ul>
     *
     * <p>Lock contention counts as a refusal here.  It means this node did not get the claim, which
     * is all a claimant needs to know; retrying would only re-lose the race to whoever holds it.
     */
    static boolean isUniquenessViolation(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException && isUniqueViolationSqlState(((SQLException) cause).getSQLState())) {
                return true;
            }

            String className = cause.getClass().getName();

            if (className.endsWith("SchemaViolationException")
                    || className.endsWith("ConstraintViolationException")
                    || className.endsWith("PermanentLockingException")) {
                return true;
            }

            if (cause.getCause() == cause) {
                break;
            }
        }

        return false;
    }

    private static boolean isUniqueViolationSqlState(String sqlState) {
        if (sqlState == null) {
            return false;
        }

        return sqlState.equals(POSTGRES_UNIQUE_VIOLATION_SQL_STATE) || sqlState.startsWith(INTEGRITY_CONSTRAINT_SQL_STATE_CLASS);
    }

    /**
     * A claim attempt, shaped to match {@link GraphClaimable#tryClaim()} so implementations can be
     * passed to {@link #attempt} directly as a method reference.
     */
    @FunctionalInterface
    public interface ClaimAttempt<T> {
        T get() throws AtlasBaseException;
    }
}
