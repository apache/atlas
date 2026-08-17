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
import static org.testng.Assert.assertFalse;
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
    public void tryClaimTask_claimsAPendingTask() throws AtlasBaseException {
        AtlasTask task = new AtlasTask("claimType", "test", java.util.Collections.singletonMap("k", "v"));
        registry.save(task);
        graph.commit();

        boolean claimed = registry.tryClaimTask(task.getGuid());

        assertTrue(claimed, "tryClaimTask must return true for a PENDING task");

        AtlasTask updated = registry.getById(task.getGuid());
        assertEquals(updated.getStatus(), AtlasTask.Status.IN_PROGRESS,
                "Status must be IN_PROGRESS after a successful claim");

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    @Test
    public void tryClaimTask_returnsFalse_forNonExistentTask() throws AtlasBaseException {
        boolean claimed = registry.tryClaimTask("guid-does-not-exist");
        assertFalse(claimed, "tryClaimTask must return false when task is not found");
    }

    @Test
    public void tryClaimTask_returnsFalse_forAlreadyClaimedTask() throws AtlasBaseException {
        AtlasTask task = new AtlasTask("alreadyClaimed", "test", java.util.Collections.emptyMap());
        registry.save(task);
        graph.commit();

        // First claim — should succeed
        boolean first = registry.tryClaimTask(task.getGuid());
        assertTrue(first, "First claim must succeed");

        // Second claim on IN_PROGRESS task — must fail (status != PENDING)
        boolean second = registry.tryClaimTask(task.getGuid());
        assertFalse(second, "Second claim must return false — task is already IN_PROGRESS");

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    @Test
    public void tryClaimTask_returnsFalse_whenAnotherTaskIsInProgress() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task1 = new AtlasTask("t1", "test", java.util.Collections.emptyMap());
        AtlasTask task2 = new AtlasTask("t2", "test", java.util.Collections.emptyMap());

        registry.save(task1);
        registry.save(task2);
        graph.commit();

        List<AtlasTask> pending = registry.getPendingTasks();
        assertEquals(pending.size(), 2);

        String oldestPendingGuid = pending.get(0).getGuid();
        String otherGuid = pending.get(1).getGuid();

        boolean first = registry.tryClaimTask(oldestPendingGuid);
        assertTrue(first, "Oldest task claim must succeed");

        boolean second = registry.tryClaimTask(otherGuid);
        assertFalse(second, "Second task claim must fail while another task is IN_PROGRESS");

        registry.deleteByGuid(task1.getGuid());
        registry.deleteByGuid(task2.getGuid());
        graph.commit();
    }

    @Test
    public void tryClaimTask_returnsFalse_forOutOfOrderPendingTask() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask older = new AtlasTask("older", "test", java.util.Collections.emptyMap());
        Thread.sleep(5);
        AtlasTask newer = new AtlasTask("newer", "test", java.util.Collections.emptyMap());

        registry.save(older);
        registry.save(newer);
        graph.commit();

        boolean newerFirst = registry.tryClaimTask(newer.getGuid());
        assertFalse(newerFirst, "Newer task must not be claimable before oldest PENDING task");

        boolean olderThen = registry.tryClaimTask(older.getGuid());
        assertTrue(olderThen, "Oldest PENDING task must be claimable");

        registry.deleteByGuid(older.getGuid());
        registry.deleteByGuid(newer.getGuid());
        graph.commit();
    }

    @Test
    public void tryClaimTask_recoversStaleInProgressTask() throws AtlasBaseException {
        clearAllTasks();

        AtlasTask task = new AtlasTask("stale", "test", java.util.Collections.emptyMap());
        registry.save(task);
        graph.commit();

        assertTrue(registry.tryClaimTask(task.getGuid()), "first claim should move task to IN_PROGRESS");
        graph.commit();

        TaskRegistry zeroThresholdRegistry = new TaskRegistry(graph, 0L);
        zeroThresholdRegistry.recoverStaleInProgressTasks();
        graph.commit();
        boolean reclaimedClaim = registry.tryClaimTask(task.getGuid());

        assertTrue(reclaimedClaim, "stale IN_PROGRESS task should be reclaimed and claimed again");

        AtlasTask updated = registry.getById(task.getGuid());
        assertEquals(updated.getStatus(), AtlasTask.Status.IN_PROGRESS);

        registry.deleteByGuid(task.getGuid());
        graph.commit();
    }

    @Test
    public void tryClaimTask_recoveryPreservesFifoAfterReclaim() throws AtlasBaseException, InterruptedException {
        clearAllTasks();

        AtlasTask older = new AtlasTask("older-recover", "test", java.util.Collections.emptyMap());
        Thread.sleep(5);
        AtlasTask newer = new AtlasTask("newer-recover", "test", java.util.Collections.emptyMap());

        registry.save(older);
        registry.save(newer);
        graph.commit();

        assertTrue(registry.tryClaimTask(older.getGuid()), "oldest task should be claimed first");
        graph.commit();

        TaskRegistry zeroThresholdRegistry = new TaskRegistry(graph, 0L);
        zeroThresholdRegistry.recoverStaleInProgressTasks();
        graph.commit();
        boolean      newerClaim            = registry.tryClaimTask(newer.getGuid());
        assertFalse(newerClaim, "reclaimed oldest task must still be claimed before newer task");

        boolean olderReclaimed = registry.tryClaimTask(older.getGuid());
        assertTrue(olderReclaimed, "oldest reclaimed task must remain FIFO-claimable");

        registry.deleteByGuid(older.getGuid());
        registry.deleteByGuid(newer.getGuid());
        graph.commit();
    }

    private void clearAllTasks() throws AtlasBaseException {
        for (AtlasTask task : registry.getAll()) {
            registry.deleteByGuid(task.getGuid());
        }
        graph.commit();
    }
}
