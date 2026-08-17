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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

    private static final TaskLogger TASK_LOG         = TaskLogger.getLogger();
    private static final String     TASK_NAME_FORMAT = "atlas-task-%d-";

    private final TaskRegistry              registry;
    private final Map<String, TaskFactory>  taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;
    private final ExecutorService           executorService;

    public TaskExecutor(TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
        this.registry           = registry;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics         = statistics;
        this.executorService    = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(TASK_NAME_FORMAT + Thread.currentThread().getName())
                .build());
    }

    public void addAll(List<AtlasTask> tasks) {
        for (AtlasTask task : tasks) {
            if (task == null) {
                continue;
            }

            TASK_LOG.log(task);

            // Build a per-task GraphClaimable that atomically transitions
            // PENDING → IN_PROGRESS for exactly this task's GUID.
            // TaskConsumer uses GraphClaimable for both stale-claim recovery and
            // the claim step, so callers stay decoupled from TaskRegistry.
            final String              taskGuid    = task.getGuid();
            GraphClaimable<Boolean>   claimAction = new GraphClaimable<Boolean>() {
                @Override
                public Boolean tryClaim() {
                    return registry.tryClaimTask(taskGuid);
                }

                @Override
                public void recoverStaleClaims() {
                    registry.recoverStaleInProgressTasks();
                }
            };

            this.executorService.submit(new TaskConsumer(task, claimAction, this.registry, this.taskTypeFactoryMap, this.statistics));
        }
    }

    @VisibleForTesting
    void waitUntilDone() throws InterruptedException {
        Thread.sleep(5000);
    }

    static class TaskConsumer implements Runnable {
        private static final int MAX_ATTEMPT_COUNT = 3;
        private static final int DEFAULT_MAX_CLAIM_ATTEMPTS = 600;
        private static final int DEFAULT_CLAIM_RETRY_WAIT_MS = (int) TimeUnit.SECONDS.toMillis(1);

        private final GraphClaimable<Boolean>   claimAction;
        private final Map<String, TaskFactory>  taskTypeFactoryMap;
        private final TaskRegistry              registry;
        private final TaskManagement.Statistics statistics;
        private final AtlasTask                 task;
        private final int                       maxClaimAttempts;
        private final int                       claimRetryWaitMs;

        /**
         * @param task        the task to execute
         * @param claimAction the {@link GraphClaimable} that performs stale-claim
         *                    recovery and CAS claim ({@code PENDING → IN_PROGRESS}).
         *                    Only if {@code claimAction.tryClaim()} returns {@code true} does
         *                    this consumer proceed to execute the task.
         * @param registry    the registry used for vertex lookup, status updates and
         *                    delete-on-complete (all graph operations except the claim)
         * @param taskTypeFactoryMap factories keyed by task type
         * @param statistics  execution counters
         */
        public TaskConsumer(AtlasTask task, GraphClaimable<Boolean> claimAction, TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
            this(task, claimAction, registry, taskTypeFactoryMap, statistics, DEFAULT_MAX_CLAIM_ATTEMPTS, DEFAULT_CLAIM_RETRY_WAIT_MS);
        }

        @VisibleForTesting
        TaskConsumer(AtlasTask task, GraphClaimable<Boolean> claimAction, TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap,
                     TaskManagement.Statistics statistics, int maxClaimAttempts, int claimRetryWaitMs) {
            this.task               = task;
            this.claimAction        = claimAction;
            this.registry           = registry;
            this.taskTypeFactoryMap = taskTypeFactoryMap;
            this.statistics         = statistics;
            this.maxClaimAttempts   = maxClaimAttempts;
            this.claimRetryWaitMs   = claimRetryWaitMs;
        }

        @Override
        public void run() {
            AtlasVertex taskVertex = null;
            int         attemptCount;

            try {
                // GraphClaimable.recoverStaleClaims() + tryClaim(): recover stale
                // claims first, then atomically transition PENDING → IN_PROGRESS.
                // In active-active mode multiple nodes may queue the same PENDING task on
                // startup.  Only the node whose @GraphTransaction commits first proceeds;
                // all other nodes receive false and skip without executing the task.
                // Same contract as AsyncImportService.claimNextWaitingImport().
                boolean claimed = tryClaimWithWait();
                if (!claimed) {
                    TASK_LOG.warn("Task skipped - already claimed by another node or not PENDING.", task);
                    return;
                }

                taskVertex = registry.getVertex(task.getGuid());

                if (taskVertex == null) {
                    TASK_LOG.warn("Task not scheduled as it was not found or status was COMPLETE!", task);

                    return;
                }

                statistics.increment(1);

                attemptCount = task.getAttemptCount();

                if (attemptCount >= MAX_ATTEMPT_COUNT) {
                    TASK_LOG.warn("Max retry count for task exceeded! Skipping!", task);

                    return;
                }

                performTask(taskVertex, task);
            } catch (InterruptedException exception) {
                registry.updateStatus(taskVertex, task);

                TASK_LOG.error("{}: {}: Interrupted!", task, exception);

                statistics.error();
            } catch (Exception exception) {
                if (task != null) {
                    task.updateStatusFromAttemptCount();

                    registry.updateStatus(taskVertex, task);

                    TASK_LOG.error("Error executing task. Please perform the operation again!", task, exception);
                } else {
                    LOG.error("Error executing. Please perform the operation again!", exception);
                }

                statistics.error();
            } finally {
                if (task != null) {
                    this.registry.commit();

                    TASK_LOG.log(task);
                }
            }
        }

        private boolean tryClaimWithWait() throws Exception {
            int claimAttempt = 0;

            while (claimAttempt < maxClaimAttempts) {
                claimAction.recoverStaleClaims();

                if (claimAction.tryClaim()) {
                    return true;
                }

                claimAttempt++;

                if (claimAttempt < maxClaimAttempts) {
                    Thread.sleep(claimRetryWaitMs);
                }
            }

            return false;
        }

        private void performTask(AtlasVertex taskVertex, AtlasTask task) throws Exception {
            TaskFactory factory = taskTypeFactoryMap.get(task.getType());

            if (factory == null) {
                LOG.error("taskTypeFactoryMap does not contain task of type: {}", task.getType());
                return;
            }

            AbstractTask runnableTask = factory.create(task);

            runnableTask.run();

            registry.deleteComplete(taskVertex, task);

            statistics.successPrint();
        }
    }

    static class TaskLogger {
        private static final Logger LOG = LoggerFactory.getLogger("TASKS");

        public static TaskLogger getLogger() {
            return new TaskLogger();
        }

        public void info(String message) {
            LOG.info(message);
        }

        public void log(AtlasTask task) {
            LOG.info(AtlasType.toJson(task));
        }

        public void warn(String message, AtlasTask task) {
            LOG.warn(message, AtlasType.toJson(task));
        }

        public void error(String s, AtlasTask task, Exception exception) {
            LOG.error(s, AtlasType.toJson(task), exception);
        }
    }
}
