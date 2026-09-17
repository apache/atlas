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

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class TaskExecutorTest extends BaseTaskFixture {
    /** Long enough that the background poll never interferes with what a test is asserting. */
    private static final long LONG_POLL_MS = TimeUnit.HOURS.toMillis(1);

    /** Only ever waited out when the work is not going to arrive, so it can afford to be generous. */
    private static final long COMPLETION_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long COMPLETION_POLL_MS    = 25L;

    @Inject
    private AtlasGraph graph;

    @Inject
    private TaskRegistry taskRegistry;

    @Inject
    private TaskManagement taskManagement;

    @Test
    public void noTasksExecuted() {
        TaskManagementTest.SpyingFactory spyingFactory  = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory>         taskFactoryMap = new HashMap<>();

        TaskManagement.createTaskTypeFactoryMap(new HashMap<>(), spyingFactory);

        TaskManagement.Statistics statistics = new TaskManagement.Statistics();

        new TaskExecutor(taskRegistry, taskFactoryMap, statistics);

        assertEquals(statistics.getTotal(), 0);
    }

    @Test
    public void tasksNotPersistedIsNotExecuted() throws InterruptedException {
        TaskManagementTest.SpyingFactory spyingFactory  = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory>         taskFactoryMap = new HashMap<>();

        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        TaskManagement.Statistics statistics   = new TaskManagement.Statistics();
        TaskExecutor              taskExecutor = new TaskExecutor(taskRegistry, taskFactoryMap, statistics);

        taskExecutor.addAll(Collections.singletonList(new AtlasTask(SPYING_TASK_ADD, "test", Collections.emptyMap())));

        taskExecutor.waitUntilDone();

        assertEquals(statistics.getTotal(), 0);
    }

    @Test
    public void persistedIsExecuted() throws AtlasBaseException, InterruptedException {
        TaskManagementTest.SpyingFactory spyingFactory  = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory>         taskFactoryMap = new HashMap<>();

        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        AtlasTask addTask           = taskManagement.createTask("add", "test", Collections.emptyMap());
        AtlasTask errorThrowingTask = taskManagement.createTask("errorThrowingTask", "test", Collections.emptyMap());

        TaskManagement.Statistics statistics = new TaskManagement.Statistics();

        graph.commit();

        TaskExecutor.TaskConsumer addConsumer   = new TaskExecutor.TaskConsumer(addTask, taskRegistry, taskFactoryMap, statistics);
        TaskExecutor.TaskConsumer errorConsumer = new TaskExecutor.TaskConsumer(errorThrowingTask, taskRegistry, taskFactoryMap, statistics);

        addConsumer.run();
        errorConsumer.run();

        assertEquals(statistics.getTotal(), 2);
        assertEquals(statistics.getTotalSuccess(), 1);
        assertEquals(statistics.getTotalError(), 1);

        assertNotNull(spyingFactory.getAddTask());
        assertNotNull(spyingFactory.getErrorTask());

        assertTrue(spyingFactory.getAddTask().taskPerformed());
        assertTrue(spyingFactory.getErrorTask().taskPerformed());

        assertTaskUntilFail(errorThrowingTask, taskFactoryMap, statistics);
    }

    /**
     * The regression this design exists for.  Several tasks are queued at once; every one of
     * them must run.  The previous executor asked to run one nominated task and blocked the
     * only worker thread waiting for its turn, so the rest were never reached.
     */
    @Test
    public void allQueuedTasksAreExecuted() throws Exception {
        TaskManagementTest.SpyingFactory spyingFactory  = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory>         taskFactoryMap = new HashMap<>();

        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        List<AtlasTask> queued = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            queued.add(taskManagement.createTask(SPYING_TASK_ADD, "test", Collections.emptyMap()));
        }

        graph.commit();

        TaskManagement.Statistics statistics   = new TaskManagement.Statistics();
        TaskExecutor              taskExecutor = new TaskExecutor(taskRegistry, taskFactoryMap, statistics);

        taskExecutor.addAll(queued);

        awaitAtLeast(statistics::getTotalSuccess, queued.size());

        assertEquals(statistics.getTotalSuccess(), queued.size(),
                "Every queued task must be executed, not just the first claimable one");
    }

    /**
     * The worker thread outlives any single drain and holds a graph transaction across them.  A
     * task committed by a request thread after that transaction opened must still be visible to
     * the next drain, otherwise the worker goes permanently blind to new work.
     */
    @Test
    public void tasksCommittedAfterAnEarlierDrainAreStillPickedUp() throws Exception {
        TaskManagementTest.SpyingFactory spyingFactory  = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory>         taskFactoryMap = new HashMap<>();

        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        TaskManagement.Statistics statistics   = new TaskManagement.Statistics();
        TaskExecutor              taskExecutor = new TaskExecutor(taskRegistry, taskFactoryMap, statistics);

        // First drain finds an empty queue and settles the worker onto its graph view.
        taskExecutor.wakeUp();
        taskExecutor.waitUntilDone();

        assertEquals(statistics.getTotal(), 0, "Nothing should have run yet");

        AtlasTask task = taskManagement.createTask(SPYING_TASK_ADD, "test", Collections.emptyMap());

        graph.commit();

        taskExecutor.wakeUp();

        awaitAtLeast(statistics::getTotalSuccess, 1);

        assertEquals(statistics.getTotalSuccess(), 1,
                "A task committed after the first drain must still be claimed by the next one");
        assertNotNull(task.getGuid());
    }

    @Test
    public void drainKeepsClaimingUntilRegistryHasNothingLeft() throws Exception {
        TaskManagementTest.SpyingFactory spyingFactory  = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory>         taskFactoryMap = new HashMap<>();

        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        AtlasTask first  = taskManagement.createTask(SPYING_TASK_ADD, "test", Collections.emptyMap());
        AtlasTask second = taskManagement.createTask(SPYING_TASK_ADD, "test", Collections.emptyMap());

        graph.commit();

        Queue<AtlasTask> handOut      = new LinkedList<>(Arrays.asList(first, second));
        AtomicInteger    claimCalls   = new AtomicInteger();
        AtomicInteger    recoverCalls = new AtomicInteger();

        GraphClaimable<AtlasTask> claimSource = new GraphClaimable<AtlasTask>() {
            @Override
            public String claimName() {
                return Constants.CLAIM_TASK_RUNNER;
            }

            @Override
            public AtlasTask tryClaim() {
                claimCalls.incrementAndGet();

                return handOut.poll();
            }

            @Override
            public void recoverStaleClaims() {
                recoverCalls.incrementAndGet();
            }
        };

        TaskManagement.Statistics statistics   = new TaskManagement.Statistics();
        TaskExecutor              taskExecutor = new TaskExecutor(taskRegistry, claimSource, taskFactoryMap, statistics, LONG_POLL_MS);

        taskExecutor.wakeUp();

        awaitAtLeast(statistics::getTotalSuccess, 2);
        // The claim that comes back empty is what ends the drain, and it follows the last task.
        awaitAtLeast(claimCalls::get, 3);

        assertEquals(statistics.getTotalSuccess(), 2, "Both handed-out tasks must be executed");
        assertEquals(claimCalls.get(), 3, "Drain must keep claiming until the registry returns nothing");
        assertEquals(recoverCalls.get(), 1, "Stale claims are recovered once per drain, not once per claim");
    }

    /**
     * A drain that cannot claim anything must return the worker immediately rather than hold it
     * waiting — another node owns the running task and will carry on draining when it is done.
     */
    @Test
    public void drainReturnsImmediatelyWhenAnotherNodeHoldsTheClaim() throws Exception {
        GraphClaimable<AtlasTask> nothingClaimable = new GraphClaimable<AtlasTask>() {
            @Override
            public String claimName() {
                return Constants.CLAIM_TASK_RUNNER;
            }

            @Override
            public AtlasTask tryClaim() {
                return null;
            }
        };

        TaskManagement.Statistics statistics   = new TaskManagement.Statistics();
        TaskExecutor              taskExecutor = new TaskExecutor(taskRegistry, nothingClaimable, new HashMap<>(), statistics, LONG_POLL_MS);

        long start = System.currentTimeMillis();

        taskExecutor.wakeUp();
        taskExecutor.waitUntilDone();

        assertTrue(System.currentTimeMillis() - start < TimeUnit.SECONDS.toMillis(30),
                "Drain must not block the worker when nothing is claimable");
        assertEquals(statistics.getTotal(), 0, "Nothing may be executed when nothing was claimed");
    }

    @Test
    public void taskWithNoRegisteredFactoryIsFailedRatherThanLeftInProgress() throws Exception {
        AtlasTask orphan = taskManagement.createTask(SPYING_TASK_ADD, "test", Collections.emptyMap());

        graph.commit();

        TaskManagement.Statistics statistics = new TaskManagement.Statistics();

        // No factory registered for this type, so the consumer cannot run it.
        new TaskExecutor.TaskConsumer(orphan, taskRegistry, new HashMap<>(), statistics).run();

        graph.commit();

        AtlasTask stored = taskManagement.getByGuid(orphan.getGuid());

        assertNotNull(stored);
        assertEquals(stored.getStatus(), AtlasTask.Status.FAILED,
                "An unrunnable task must not stay IN_PROGRESS — it would block every other task in the cluster");
    }

    /**
     * Waits for the work to arrive rather than for a fixed interval.  {@link
     * TaskExecutor#waitUntilDone()} sleeps for a set time, which is a guess at how long the graph
     * transactions behind it will take.  The suite runs several JVMs at once and the guess is
     * sometimes short under that load, so the assertion after it fails for want of waiting rather
     * than for anything it checks.  Waiting on the count the test goes on to assert cannot be short,
     * and it returns as soon as the work is done instead of always paying the whole interval.
     *
     * <p>Kept for the counts a test expects to <em>rise</em>.  Asserting that nothing ran is still a
     * fixed wait, since there is no arrival to wait for.
     */
    private static void awaitAtLeast(IntSupplier counter, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + COMPLETION_TIMEOUT_MS;

        while (counter.getAsInt() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(COMPLETION_POLL_MS);
        }
    }

    private void assertTaskUntilFail(AtlasTask errorThrowingTask, Map<String, TaskFactory> taskFactoryMap, TaskManagement.Statistics statistics)
            throws AtlasBaseException {
        AtlasTask errorTaskFromDB = taskManagement.getByGuid(errorThrowingTask.getGuid());

        assertNotNull(errorTaskFromDB);
        assertTrue(StringUtils.isNotEmpty(errorTaskFromDB.getErrorMessage()));
        assertEquals(errorTaskFromDB.getAttemptCount(), 1);
        assertEquals(errorTaskFromDB.getStatus(), AtlasTask.Status.PENDING);

        for (int i = errorTaskFromDB.getAttemptCount(); i <= AtlasTask.MAX_ATTEMPT_COUNT; i++) {
            new TaskExecutor.TaskConsumer(errorThrowingTask, taskRegistry, taskFactoryMap, statistics).run();
        }

        graph.commit();

        assertEquals(errorThrowingTask.getStatus(), AtlasTask.Status.FAILED);
    }
}
