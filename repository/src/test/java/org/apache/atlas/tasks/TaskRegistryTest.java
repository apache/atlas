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

import org.apache.atlas.AtlasException;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class TaskRegistryTest {
    @Inject
    AtlasGraph graph;

    @Inject
    TaskRegistry registry;

    @BeforeMethod(alwaysRun = true)
    public void clearTasksBeforeEach() throws AtlasBaseException {
        BaseTaskFixture.TASK_TEST_LOCK.lock();
        clearAllTasks();
    }

    @AfterMethod(alwaysRun = true)
    public void unlockTaskTestExecution() {
        BaseTaskFixture.TASK_TEST_LOCK.unlock();
    }

    @Test
    public void basic() throws AtlasException, AtlasBaseException {
        AtlasTask task = new AtlasTask("abcd", "test", Collections.singletonMap("p1", "p1"));

        assertNull(registry.getById(task.getGuid()));

        AtlasTask   taskFromVertex = registry.save(task);
        AtlasVertex taskVertex     = registry.getVertex(task.getGuid());

        assertEquals(taskFromVertex.getGuid(), task.getGuid());
        assertEquals(taskFromVertex.getType(), task.getType());
        assertEquals(taskFromVertex.getAttemptCount(), task.getAttemptCount());
        assertEquals(taskFromVertex.getParameters(), task.getParameters());
        assertEquals(taskFromVertex.getCreatedBy(), task.getCreatedBy());

        taskFromVertex.incrementAttemptCount();
        taskFromVertex.setStatusPending();
        registry.updateStatus(taskVertex, taskFromVertex);
        registry.commit();

        taskFromVertex = registry.getById(task.getGuid());

        assertNotNull(taskVertex);
        assertEquals(taskFromVertex.getStatus(), AtlasTask.Status.PENDING);
        assertEquals(taskFromVertex.getAttemptCount(), 1);

        registry.deleteByGuid(taskFromVertex.getGuid());

        try {
            AtlasTask t = registry.getById(taskFromVertex.getGuid());

            assertNull(t);
        } catch (IllegalStateException e) {
            assertTrue(true, "Indicates vertex is deleted!");
        }
    }

    @Test
    public void pendingTasks() throws AtlasBaseException {
        final int    maxTasks       = 3;
        final String taskTypeFormat = "abcd:%d";

        for (int i = 0; i < maxTasks; i++) {
            AtlasTask task = new AtlasTask(String.format(taskTypeFormat, i), "test", Collections.singletonMap("p1", "p1"));

            registry.save(task);
        }

        List<AtlasTask> pendingTasks = registry.getPendingTasks();

        assertEquals(pendingTasks.size(), maxTasks);

        for (int i = 0; i < maxTasks; i++) {
            assertEquals(pendingTasks.get(i).getType(), String.format(taskTypeFormat, i));
            registry.deleteByGuid(pendingTasks.get(i).getGuid());
        }

        graph.commit();

        pendingTasks = registry.getPendingTasks();

        assertEquals(pendingTasks.size(), 0);
    }

    @Test
    public void claimNextPendingTask_claimsAPendingTask() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("claimType", "test", java.util.Collections.singletonMap("k", "v"));

        registry.save(task);
        graph.commit();

        AtlasTask claimed = registry.claimNextPendingTask();

        assertNotNull(claimed, "claimNextPendingTask must hand out a PENDING task");
        assertEquals(claimed.getGuid(), task.getGuid());

        AtlasTask updated = registry.getById(task.getGuid());

        assertEquals(updated.getStatus(), AtlasTask.Status.IN_PROGRESS,
                "Status must be IN_PROGRESS after a successful claim");

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    @Test
    public void claimNextPendingTask_returnsNull_whenNothingIsPending() throws AtlasBaseException {
        clearAllTasks();

        assertNull(registry.claimNextPendingTask(), "claimNextPendingTask must return null when there is nothing to do");
    }

    @Test
    public void claimNextPendingTask_returnsNull_whenAnotherTaskIsInProgress() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask first = new AtlasTask("t1", "test", java.util.Collections.emptyMap());

        Thread.sleep(5);

        AtlasTask second = new AtlasTask("t2", "test", java.util.Collections.emptyMap());

        registry.save(first);
        registry.save(second);
        graph.commit();

        assertNotNull(registry.claimNextPendingTask(), "First claim must succeed");
        graph.commit();

        assertNull(registry.claimNextPendingTask(),
                "Only one task may run in the cluster at a time, so nothing else may be claimed");

        registry.deleteByGuid(first.getGuid());
        registry.deleteByGuid(second.getGuid());
        graph.commit();
    }

    /**
     * Ordering is the reason the cluster runs one task at a time: an add and a delete of the same
     * classification must be applied in the order they were created.
     */
    @Test
    public void claimNextPendingTask_handsOutTasksOldestFirst() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask older = new AtlasTask("older", "test", java.util.Collections.emptyMap());

        Thread.sleep(5);

        AtlasTask newer = new AtlasTask("newer", "test", java.util.Collections.emptyMap());

        // Saved newest-first, so a claim that trusted insertion order would pick the wrong one.
        registry.save(newer);
        registry.save(older);
        graph.commit();

        AtlasTask firstClaim = registry.claimNextPendingTask();

        assertNotNull(firstClaim);
        assertEquals(firstClaim.getGuid(), older.getGuid(), "Oldest task must be handed out first");

        registry.deleteByGuid(older.getGuid());
        graph.commit();

        AtlasTask secondClaim = registry.claimNextPendingTask();

        assertNotNull(secondClaim);
        assertEquals(secondClaim.getGuid(), newer.getGuid(), "Newer task must follow the older one");

        registry.deleteByGuid(newer.getGuid());
        graph.commit();
    }

    @Test
    public void recoverStaleInProgressTasks_returnsAbandonedTaskToPending() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("stale", "test", java.util.Collections.emptyMap());

        registry.save(task);
        graph.commit();

        assertNotNull(registry.claimNextPendingTask(), "first claim should move task to IN_PROGRESS");
        graph.commit();

        // Threshold of zero makes the in-progress claim immediately look abandoned.
        new TaskRegistry(graph, 0L).recoverStaleInProgressTasks();
        graph.commit();

        AtlasTask reclaimed = registry.claimNextPendingTask();

        assertNotNull(reclaimed, "A task abandoned by a dead node must become claimable again");
        assertEquals(reclaimed.getGuid(), task.getGuid());

        AtlasTask updated = registry.getById(task.getGuid());

        assertEquals(updated.getStatus(), AtlasTask.Status.IN_PROGRESS);

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    @Test
    public void recoverStaleInProgressTasks_preservesOrderingAfterReclaim() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask older = new AtlasTask("older-recover", "test", java.util.Collections.emptyMap());

        Thread.sleep(5);

        AtlasTask newer = new AtlasTask("newer-recover", "test", java.util.Collections.emptyMap());

        registry.save(older);
        registry.save(newer);
        graph.commit();

        AtlasTask claimed = registry.claimNextPendingTask();

        assertNotNull(claimed);
        assertEquals(claimed.getGuid(), older.getGuid(), "oldest task should be claimed first");
        graph.commit();

        new TaskRegistry(graph, 0L).recoverStaleInProgressTasks();
        graph.commit();

        AtlasTask afterRecovery = registry.claimNextPendingTask();

        assertNotNull(afterRecovery);
        assertEquals(afterRecovery.getGuid(), older.getGuid(),
                "A reclaimed task keeps its place in line ahead of newer tasks");

        registry.deleteByGuid(older.getGuid());
        registry.deleteByGuid(newer.getGuid());
        graph.commit();
    }

    /**
     * The status field alone cannot serialise anything - two nodes can both read PENDING and both
     * write IN_PROGRESS.  The claim marker is what the store refuses to write twice, so a claim
     * that does not take it is not actually exclusive.
     */
    @Test
    public void claimMarksTheTaskItHandsOut() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("claimed", "test", Collections.emptyMap());

        registry.save(task);
        graph.commit();

        assertNotNull(registry.claimNextPendingTask());
        graph.commit();

        AtlasVertex slot = GraphClaim.holderOf(graph, Constants.CLAIM_TASK_RUNNER);

        assertNotNull(slot, "Handing out a task must take the cluster-wide runner slot");
        assertNotNull(GraphClaim.claimedBy(slot), "The claiming node must be recorded as the owner");

        // The slot deliberately does not live on the task: uniqueness separates vertices, not writers
        // of one vertex, so two nodes marking the same task would both succeed on a backend that has
        // no uniqueness side table to refuse the second write.
        assertNull(GraphClaim.heldClaim(registry.getVertex(task.getGuid())),
                "The claim must not be recorded on the task vertex, where it would exclude nobody");

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    /**
     * A task must not be handed out while any node holds the runner slot, which is what keeps an add
     * and a delete of one classification from running at the same time.  Recording the claim on the
     * task instead was not enough: two nodes that picked the same task wrote the same marker, and
     * uniqueness has nothing to refuse when both writers agree, so both ran it.
     */
    @Test
    public void noTaskIsHandedOutWhileAnotherNodeHoldsTheRunnerSlot() throws AtlasBaseException {
        clearAllTasks();

        assertTrue(GraphClaim.claimLease(graph, Constants.CLAIM_TASK_RUNNER, "another-node", 60_000L),
                "The peer should be able to take a free slot");
        graph.commit();

        AtlasTask task = new AtlasTask("waits-its-turn", "test", Collections.emptyMap());

        registry.save(task);
        graph.commit();

        assertNull(registry.claimNextPendingTask(), "A pending task must wait while a peer holds the slot");
        graph.commit();

        GraphClaim.releaseLease(graph, Constants.CLAIM_TASK_RUNNER, "another-node");
        graph.commit();

        assertNotNull(registry.claimNextPendingTask(), "Once the slot is free the task must be handed out");
        graph.commit();

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    /**
     * A poll that hands out nothing must leave the slot free, whether it never took the slot or took
     * it and found the queue already emptied by a peer.  Holding it regardless would stop the whole
     * cluster from running anything until the lease lapsed.
     */
    @Test
    public void aPollThatHandsOutNothingLeavesTheSlotFree() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("vanishes", "test", Collections.emptyMap());

        registry.save(task);
        graph.commit();

        registry.deleteByGuid(task.getGuid());
        graph.commit();

        assertNull(registry.claimNextPendingTask(), "There is nothing left to hand out");
        graph.commit();

        assertNull(GraphClaim.holderOf(graph, Constants.CLAIM_TASK_RUNNER),
                "A poll that hands out no task must not leave the runner slot taken");
    }

    /**
     * Only the status on the task vertex decides whether it may be handed out.  The index that finds
     * candidates can lag behind the vertex, and a task a peer has just finished is still returned as
     * pending; running it a second time is what duplicated propagated classifications.  A task in
     * that state must also not hide the tasks queued behind it.
     */
    @Test
    public void aTaskThatIsNoLongerPendingIsNotHandedOut() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask finished = new AtlasTask("already-done", "test", Collections.emptyMap());

        registry.save(finished);
        graph.commit();

        Thread.sleep(5);

        AtlasTask queued = new AtlasTask("still-waiting", "test", Collections.emptyMap());

        registry.save(queued);
        graph.commit();

        finished.setStatus(AtlasTask.Status.COMPLETE);
        registry.updateStatus(registry.getVertex(finished.getGuid()), finished);
        graph.commit();

        AtlasTask handedOut = registry.claimNextPendingTask();

        graph.commit();

        assertNotNull(handedOut, "The finished task must not stop the one behind it from being handed out");
        assertEquals(handedOut.getGuid(), queued.getGuid(), "The finished task must never be handed out again");

        registry.deleteByGuid(finished.getGuid());
        registry.deleteByGuid(queued.getGuid());
        graph.commit();
    }

    @Test
    public void finishingATaskGivesTheClaimBack() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("finishes", "test", Collections.emptyMap());

        registry.save(task);
        graph.commit();

        AtlasTask claimed = registry.claimNextPendingTask();

        assertNotNull(claimed);
        graph.commit();

        claimed.setStatus(AtlasTask.Status.COMPLETE);
        registry.updateStatus(registry.getVertex(task.getGuid()), claimed);
        graph.commit();

        assertNull(GraphClaim.holderOf(graph, Constants.CLAIM_TASK_RUNNER),
                "A finished task must hand the runner slot back, or no node could ever claim again");

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    /**
     * Deleting a vertex does not clear the uniqueness entry the store keeps for it, so the claim
     * has to be given up explicitly before the task disappears.
     */
    @Test
    public void deletingAClaimedTaskDoesNotStrandTheClaim() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask first = new AtlasTask("deleted-while-claimed", "test", Collections.emptyMap());

        registry.save(first);
        graph.commit();

        assertNotNull(registry.claimNextPendingTask());
        graph.commit();

        registry.deleteByGuid(first.getGuid());
        graph.commit();

        Thread.sleep(5);

        AtlasTask second = new AtlasTask("after-delete", "test", Collections.emptyMap());

        registry.save(second);
        graph.commit();

        AtlasTask claimed = registry.claimNextPendingTask();

        assertNotNull(claimed, "The claim must be free again once the task holding it is gone");
        assertEquals(claimed.getGuid(), second.getGuid());

        registry.deleteByGuid(second.getGuid());
        graph.commit();
    }

    @Test
    public void staleRecoveryTakesTheClaimOffTheAbandonedTask() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("abandoned", "test", Collections.emptyMap());

        registry.save(task);
        graph.commit();

        assertNotNull(registry.claimNextPendingTask());
        graph.commit();

        new TaskRegistry(graph, 0L).recoverStaleInProgressTasks();
        graph.commit();

        assertNull(GraphClaim.heldClaim(registry.getVertex(task.getGuid())),
                "A task abandoned by a dead node must release its claim, otherwise the cluster stalls permanently");

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    private void clearAllTasks() throws AtlasBaseException {
        for (AtlasTask task : registry.getAll()) {
            registry.deleteByGuid(task.getGuid());
        }

        // A test that leaves a task mid-flight also leaves the cluster-wide runner slot taken, and the
        // next test's claim would then fail for a reason that has nothing to do with what it checks.
        AtlasVertex slot = GraphClaim.holderOf(graph, Constants.CLAIM_TASK_RUNNER);

        if (slot != null) {
            GraphClaim.releaseLease(graph, Constants.CLAIM_TASK_RUNNER, GraphClaim.claimedBy(slot));
        }

        graph.commit();
    }
}
