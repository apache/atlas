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
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Losing a claim has to look the same to a caller no matter which backend decided it, since that
 * is the signal telling a node to leave the work to someone else instead of running it too.
 */
public class GraphClaimTest {
    private static final String CLAIM_NAME = "ATLAS_TASK_RUNNER";

    @Test
    public void claimKeyIsGloballyUniqueEvenWhenThisNodeNeverBuiltTheIndexes() {
        assertTrue(GraphBackedSearchIndexer.isGlobalUniqueIndexKey(Constants.CLAIM_KEY),
                "Uniqueness must be enforced on every node, including those that skip index initialization, "
                        + "otherwise those nodes claim work with nothing to stop them");
    }

    // ------------------------------------------------------------------ backend parity

    /**
     * On the rdbms backend the uniqueness entry is written with the property, so the store refuses
     * the claim while {@code claim()} is still running.
     */
    @Test
    public void conflictRaisedDuringTheClaimComesBackAsNothingClaimed() throws AtlasBaseException {
        Object claimed = GraphClaim.attempt(() -> {
            throw new ClaimConflictException(CLAIM_NAME, new SQLException("duplicate key", "23505"));
        });

        assertNull(claimed, "A claim refused while it was being taken means another node has it");
    }

    /**
     * Backends enforcing uniqueness through a JanusGraph composite index only refuse at commit,
     * which happens after the claiming method has already returned successfully.
     */
    @Test
    public void conflictRaisedAfterTheClaimReturnedComesBackAsNothingClaimed() throws AtlasBaseException {
        Object claimed = GraphClaim.attempt(() -> {
            throw new RuntimeException("commit failed", new AtlasSchemaViolationException(new RuntimeException("unique index")));
        });

        assertNull(claimed, "A claim refused at commit means another node has it, same as any other timing");
    }

    @Test
    public void successfulClaimIsPassedStraightThrough() throws AtlasBaseException {
        assertEquals(GraphClaim.attempt(() -> "task-1"), "task-1");
    }

    @Test
    public void nothingAvailableIsAlreadyNull() throws AtlasBaseException {
        assertNull(GraphClaim.attempt(() -> null));
    }

    /**
     * Swallowing real failures would silently stop tasks from running, with nothing in the logs to
     * explain why.
     */
    @Test
    public void genuineFailureIsNotMistakenForContention() {
        try {
            GraphClaim.attempt(() -> {
                throw new IllegalStateException("no active transaction");
            });

            fail("A failure unrelated to claiming must reach the caller");
        } catch (IllegalStateException expected) {
            assertEquals(expected.getMessage(), "no active transaction");
        } catch (AtlasBaseException unexpected) {
            fail("Unexpected exception type: " + unexpected);
        }
    }

    @Test
    public void checkedFailureFromTheClaimReachesTheCaller() {
        try {
            GraphClaim.attempt(() -> {
                throw new AtlasBaseException("graph unavailable");
            });

            fail("A checked failure must reach the caller");
        } catch (AtlasBaseException expected) {
            assertEquals(expected.getMessage(), "graph unavailable");
        }
    }

    /**
     * A node that did not get the lease has nothing worth keeping, and on the rdbms backend the
     * refused write leaves the transaction unable to commit at all - so committing it anyway reports
     * an ordinary lost race as an unexplained infrastructure failure.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void aLeaseThisNodeDidNotGetIsNotCommitted() {
        AtlasGraph      graph      = mock(AtlasGraph.class);
        AtlasGraphQuery query      = mock(AtlasGraphQuery.class);
        AtlasVertex     peerHolder = mock(AtlasVertex.class);

        when(graph.query()).thenReturn(query);
        when(query.has(Constants.CLAIM_KEY, CLAIM_NAME)).thenReturn(query);
        when(query.vertices()).thenReturn(Collections.singletonList(peerHolder));
        when(peerHolder.getProperty(Constants.CLAIM_OWNER_KEY, String.class)).thenReturn("a-peer");
        when(peerHolder.getProperty(Constants.CLAIM_EXPIRY_KEY, Long.class)).thenReturn(System.currentTimeMillis() + 60000);

        assertFalse(GraphClaim.claimLeaseAndCommit(graph, CLAIM_NAME, "this-node", 30000),
                "The lease is held by a peer, so this node has not taken it");

        verify(graph, never()).commit();
        verify(graph).rollback();
    }

    // ------------------------------------------------------------------ conflict detection

    @Test
    public void postgresUniqueViolationIsALostClaim() {
        SQLException uniqueViolation = new SQLException("duplicate key value violates unique constraint", "23505");

        assertTrue(GraphClaim.isClaimConflict(new RuntimeException("persisting", uniqueViolation)));
    }

    @Test
    public void otherIntegrityConstraintFailuresAreAlsoALostClaim() {
        assertTrue(GraphClaim.isClaimConflict(new SQLException("integrity constraint violated", "23000")));
    }

    @Test
    public void janusSchemaViolationIsALostClaim() {
        AtlasSchemaViolationException schemaViolation = new AtlasSchemaViolationException(new RuntimeException("unique index"));

        assertTrue(GraphClaim.isClaimConflict(new RuntimeException("commit failed", schemaViolation)));
    }

    @Test
    public void jpaConstraintViolationIsALostClaim() {
        assertTrue(GraphClaim.isClaimConflict(new RuntimeException("insert failed", new ConstraintViolationException())));
    }

    /**
     * Backends with no uniqueness side table rely on a composite index marked
     * {@code ConsistencyModifier.LOCK}, and when JanusGraph cannot get that lock it reports contention
     * rather than a constraint violation.  Reading that as anything other than a lost claim would let
     * two nodes run the same work on those backends.
     */
    @Test
    public void lockContentionIsALostClaim() {
        assertTrue(GraphClaim.isClaimConflict(new RuntimeException("commit failed", new PermanentLockingException())));
    }

    @Test
    public void lockContentionAtCommitComesBackAsNothingClaimed() throws AtlasBaseException {
        Object claimed = GraphClaim.attempt(() -> {
            throw new RuntimeException("commit failed", new PermanentLockingException());
        });

        assertNull(claimed, "Failing to get the lock means another node has the claim");
    }

    @Test
    public void claimConflictRaisedByClaimIsALostClaim() {
        assertTrue(GraphClaim.isClaimConflict(new ClaimConflictException(CLAIM_NAME, new SQLException("dup", "23505"))));
    }

    @Test
    public void unrelatedFailureIsNotALostClaim() {
        assertFalse(GraphClaim.isClaimConflict(new IllegalStateException("no active transaction")));
    }

    @Test
    public void sqlFailureWithADifferentStateIsNotALostClaim() {
        assertFalse(GraphClaim.isClaimConflict(new SQLException("connection refused", "08006")));
    }

    @Test
    public void selfReferencingCauseDoesNotHangTheClaimPath() {
        assertFalse(GraphClaim.isClaimConflict(new SelfCausedException()));
    }

    /**
     * Named to match what a JPA provider throws; detection is by name because those classes are
     * not on the compile classpath here.
     */
    private static class ConstraintViolationException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Named to match what JanusGraph throws when it cannot acquire the index lock; detection is by
     * name because the class is not on the compile classpath here.
     */
    private static class PermanentLockingException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static class SelfCausedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }
}
