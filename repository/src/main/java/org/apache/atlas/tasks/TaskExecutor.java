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
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs graph tasks on this node, one at a time, pulling work from {@link TaskRegistry}.
 *
 * <p>Nothing is assigned to this node: creating a task only wakes the worker, which then asks
 * the registry for whichever task is next in the cluster and keeps going until the registry
 * has nothing to hand out.  Any node can execute any task, so work is never stranded behind a
 * busy peer, and the worker only ever holds a task it has already been granted.
 */
public class TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

    private static final TaskLogger TASK_LOG          = TaskLogger.getLogger();
    private static final String     TASK_NAME_FORMAT  = "atlas-task-%d-";
    private static final String     POLL_NAME_FORMAT  = "atlas-task-poll-%d-";
    private static final long       SHUTDOWN_WAIT_SEC = 30L;

    private final TaskRegistry               registry;
    private final GraphClaimable<AtlasTask>  claimSource;
    private final Map<String, TaskFactory>   taskTypeFactoryMap;
    private final TaskManagement.Statistics  statistics;
    private final ExecutorService            executorService;
    private final ScheduledExecutorService   pollService;
    private final AtomicBoolean              drainScheduled = new AtomicBoolean(false);

    public TaskExecutor(TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
        this(registry, claimableOver(registry), taskTypeFactoryMap, statistics, TaskManagement.getPollIntervalMs());
    }

    /**
     * Adapts the registry to {@link GraphClaimable}.  The calls are routed through the injected
     * {@code registry} reference on purpose: that is the transaction-managed proxy, so each claim
     * and recovery runs in its own graph transaction.  {@link TaskRegistry} cannot implement the
     * interface itself — the generic signature would produce a synthetic bridge method that the
     * transaction interceptor may bind to instead of the real one.
     */
    private static GraphClaimable<AtlasTask> claimableOver(TaskRegistry registry) {
        return new GraphClaimable<AtlasTask>() {
            @Override
            public String claimName() {
                return Constants.CLAIM_TASK_RUNNER;
            }

            @Override
            public AtlasTask tryClaim() {
                return registry.claimNextPendingTask();
            }

            @Override
            public void recoverStaleClaims() {
                registry.recoverStaleInProgressTasks();
            }
        };
    }

    @VisibleForTesting
    TaskExecutor(TaskRegistry registry, GraphClaimable<AtlasTask> claimSource, Map<String, TaskFactory> taskTypeFactoryMap,
                 TaskManagement.Statistics statistics, long pollIntervalMs) {
        this.registry           = registry;
        this.claimSource        = claimSource;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics         = statistics;
        this.executorService    = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(TASK_NAME_FORMAT + Thread.currentThread().getName())
                .build());
        this.pollService        = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(POLL_NAME_FORMAT + Thread.currentThread().getName())
                .build());

        // Wake-ups on task creation cover the common case, but a task can be left pending with
        // every worker idle — a peer died holding it, or it outlived the run that created it.
        this.pollService.scheduleWithFixedDelay(this::wakeUp, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void addAll(List<AtlasTask> tasks) {
        for (AtlasTask task : tasks) {
            if (task != null) {
                TASK_LOG.log(task);
            }
        }

        wakeUp();
    }

    /**
     * Asks the worker to drain whatever the cluster has pending.  Cheap to call and safe to
     * call often: overlapping requests collapse into the one drain that is already queued.
     */
    public void wakeUp() {
        if (!drainScheduled.compareAndSet(false, true)) {
            LOG.debug("TaskExecutor: wakeUp ignored, a drain is already scheduled");

            return;
        }

        try {
            LOG.debug("TaskExecutor: scheduling drain");

            this.executorService.submit(this::drain);
        } catch (Exception exception) {
            drainScheduled.set(false);

            LOG.warn("TaskExecutor: could not schedule task drain", exception);
        }
    }

    public void shutdown() {
        pollService.shutdownNow();
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(SHUTDOWN_WAIT_SEC, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException exception) {
            executorService.shutdownNow();

            Thread.currentThread().interrupt();
        }
    }

    /**
     * Claims and runs tasks until the registry has none to give.  Returning empty-handed is the
     * normal way to finish: it means the queue is empty, or a peer is running a task and will
     * carry on draining when it is done.
     */
    private void drain() {
        // Cleared first so a task created while this drain is running schedules another one.
        drainScheduled.set(false);

        LOG.debug("TaskExecutor: drain starting");

        try {
            refreshGraphView();

            claimSource.recoverStaleClaims();
        } catch (Exception exception) {
            LOG.warn("TaskExecutor: stale task recovery failed", exception);
        }

        while (true) {
            AtlasTask task;

            try {
                refreshGraphView();

                // A peer winning the race comes back as null here, same as an empty queue: either
                // way there is nothing for this worker to run.
                task = GraphClaim.attempt(claimSource::tryClaim);
            } catch (Exception exception) {
                LOG.warn("TaskExecutor: could not claim next task", exception);

                return;
            }

            if (task == null) {
                LOG.debug("TaskExecutor: nothing claimable, drain finished");

                return;
            }

            new TaskConsumer(task, this.registry, this.taskTypeFactoryMap, this.statistics).run();
        }
    }

    /**
     * Closes the graph transaction this worker thread is holding.  The thread is long-lived and
     * its transaction would otherwise keep serving the snapshot taken on first use, leaving the
     * drain permanently blind to tasks committed by request threads after that point.
     */
    private void refreshGraphView() {
        registry.commit();
    }

    @VisibleForTesting
    void waitUntilDone() throws InterruptedException {
        Thread.sleep(5000);
    }

    static class TaskConsumer implements Runnable {
        private static final int MAX_ATTEMPT_COUNT = 3;

        private final Map<String, TaskFactory>  taskTypeFactoryMap;
        private final TaskRegistry              registry;
        private final TaskManagement.Statistics statistics;
        private final AtlasTask                 task;

        /**
         * @param task       a task already claimed by this node, i.e. {@code IN_PROGRESS} in the
         *                   graph.  Every exit path must leave it in a terminal state, because
         *                   one task stuck {@code IN_PROGRESS} halts the whole cluster.
         * @param registry   used for vertex lookup, status updates and delete-on-complete
         * @param taskTypeFactoryMap factories keyed by task type
         * @param statistics execution counters
         */
        public TaskConsumer(AtlasTask task, TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
            this.task               = task;
            this.registry           = registry;
            this.taskTypeFactoryMap = taskTypeFactoryMap;
            this.statistics         = statistics;
        }

        @Override
        public void run() {
            AtlasVertex taskVertex = null;
            int         attemptCount;

            try {
                taskVertex = registry.getVertex(task.getGuid());

                if (taskVertex == null) {
                    TASK_LOG.warn("Task not scheduled as it was not found!", task);

                    return;
                }

                statistics.increment(1);

                attemptCount = task.getAttemptCount();

                if (attemptCount >= MAX_ATTEMPT_COUNT) {
                    TASK_LOG.warn("Max retry count for task exceeded! Skipping!", task);

                    failTask(taskVertex);

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

        /**
         * Moves a task this node cannot run out of {@code IN_PROGRESS}, so it stops holding up
         * every other task in the cluster.
         */
        private void failTask(AtlasVertex taskVertex) {
            task.setStatus(AtlasTask.Status.FAILED);

            registry.updateStatus(taskVertex, task);
        }

        private void performTask(AtlasVertex taskVertex, AtlasTask task) throws Exception {
            TaskFactory factory = taskTypeFactoryMap.get(task.getType());

            if (factory == null) {
                LOG.error("taskTypeFactoryMap does not contain task of type: {}", task.getType());

                failTask(taskVertex);

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
