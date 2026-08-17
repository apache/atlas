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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class TaskExecutorTest extends BaseTaskFixture {
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

        GraphClaimable<Boolean> forceClaim = () -> true;
        TaskExecutor.TaskConsumer addConsumer = new TaskExecutor.TaskConsumer(
                addTask, forceClaim, taskRegistry, taskFactoryMap, statistics);
        TaskExecutor.TaskConsumer errorConsumer = new TaskExecutor.TaskConsumer(
                errorThrowingTask, forceClaim, taskRegistry, taskFactoryMap, statistics);

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

    @Test
    public void taskConsumer_skipsExecution_whenClaimReturnsFalse() throws InterruptedException {
        // Simulate another node already claimed the task (claimAction returns false).
        TaskManagement.Statistics statistics = new TaskManagement.Statistics();
        Map<String, TaskFactory> factoryMap = new HashMap<>();

        AtlasTask task = new AtlasTask("add", "test", Collections.emptyMap());

        // GraphClaimable that always returns false (another node won)
        GraphClaimable<Boolean> alreadyClaimed = () -> false;

        TaskExecutor.TaskConsumer consumer = new TaskExecutor.TaskConsumer(
                task, alreadyClaimed, taskRegistry, factoryMap, statistics, 1, 1);

        consumer.run();

        // No work should have been counted — task was skipped
        assertEquals(statistics.getTotal(), 0,
                "Statistics must stay at 0 when claim returns false");
    }

    @Test
    public void taskConsumer_retriesClaim_untilClaimSucceeds() throws Exception {
        TaskManagementTest.SpyingFactory spyingFactory = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory> taskFactoryMap = new HashMap<>();
        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        AtlasTask addTask = taskManagement.createTask("add", "test", Collections.emptyMap());
        graph.commit();

        TaskManagement.Statistics statistics = new TaskManagement.Statistics();
        AtomicInteger claimAttempts = new AtomicInteger(0);

        // Simulate "wait until current task completes": first claim fails, second succeeds.
        GraphClaimable<Boolean> delayedClaim = () -> claimAttempts.incrementAndGet() >= 2;

        TaskExecutor.TaskConsumer consumer = new TaskExecutor.TaskConsumer(
                addTask, delayedClaim, taskRegistry, taskFactoryMap, statistics, 5, 5);

        consumer.run();

        assertTrue(claimAttempts.get() >= 2, "TaskConsumer must retry claim before giving up");
        assertEquals(statistics.getTotalSuccess(), 1,
                "Task must execute after a successful retry claim");
    }

    @Test
    public void taskConsumer_executesTask_whenClaimReturnsTrue() throws Exception {
        TaskManagementTest.SpyingFactory spyingFactory = new TaskManagementTest.SpyingFactory();
        Map<String, TaskFactory> taskFactoryMap = new HashMap<>();
        TaskManagement.createTaskTypeFactoryMap(taskFactoryMap, spyingFactory);

        AtlasTask addTask = taskManagement.createTask("add", "test", Collections.emptyMap());
        graph.commit();

        TaskManagement.Statistics statistics = new TaskManagement.Statistics();

        // Claim always succeeds — simulates this node winning the CAS
        GraphClaimable<Boolean> winningClaim = () -> true;

        TaskExecutor.TaskConsumer consumer = new TaskExecutor.TaskConsumer(
                addTask, winningClaim, taskRegistry, taskFactoryMap, statistics);

        consumer.run();

        Thread.sleep(200);
        assertEquals(statistics.getTotalSuccess(), 1,
                "Task must execute and succeed when claim returns true");
    }

    private void assertTaskUntilFail(AtlasTask errorThrowingTask, Map<String, TaskFactory> taskFactoryMap, TaskManagement.Statistics statistics)
            throws AtlasBaseException {
        AtlasTask errorTaskFromDB = taskManagement.getByGuid(errorThrowingTask.getGuid());

        assertNotNull(errorTaskFromDB);
        assertTrue(StringUtils.isNotEmpty(errorTaskFromDB.getErrorMessage()));
        assertEquals(errorTaskFromDB.getAttemptCount(), 1);
        assertEquals(errorTaskFromDB.getStatus(), AtlasTask.Status.PENDING);

        GraphClaimable<Boolean> forceClaim = () -> true;

        for (int i = errorTaskFromDB.getAttemptCount(); i <= AtlasTask.MAX_ATTEMPT_COUNT; i++) {
            TaskExecutor.TaskConsumer retryConsumer = new TaskExecutor.TaskConsumer(
                    errorThrowingTask, forceClaim, taskRegistry, taskFactoryMap, statistics);
            retryConsumer.run();
        }
        graph.commit();
        assertEquals(errorThrowingTask.getStatus(), AtlasTask.Status.FAILED);
    }
}
