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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.ICuratorFactory;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.metrics.TaskMetricsService;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class TaskExecutor {
    private static final Logger     PERF_LOG         = AtlasPerfTracer.getPerfLogger("atlas.task");
    private static final Logger     LOG              = LoggerFactory.getLogger(TaskExecutor.class);
    private static final TaskLogger TASK_LOG         = TaskLogger.getLogger();
    private static final String     TASK_NAME_FORMAT = "atlas-task-%d-";

    private static final boolean perfEnabled = AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG);

    private final ExecutorService taskExecutorService;
    private final TaskRegistry registry;
    private final Map<String, TaskFactory> taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;
    private final ICuratorFactory curatorFactory;
    private final boolean isActiveActiveHAEnabled;
    private final String zkRoot;
    private final MetricsRegistry metricRegistry;
    private final TaskMetricsService taskMetricsService;

    private TaskQueueWatcher watcher;
    private Thread watcherThread;
    private RedisService redisService;

    public TaskExecutor(TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                        ICuratorFactory curatorFactory, RedisService redisService, final String zkRoot, boolean isActiveActiveHAEnabled, 
                        MetricsRegistry metricsRegistry, TaskMetricsService taskMetricsService) {
        this.taskExecutorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                                    .setDaemon(true)
                                                                    .setNameFormat(TASK_NAME_FORMAT + Thread.currentThread().getName())
                                                                    .build());

        this.registry = registry;
        this.statistics = statistics;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.curatorFactory = curatorFactory;
        this.redisService = redisService;
        this.isActiveActiveHAEnabled = isActiveActiveHAEnabled;
        this.zkRoot = zkRoot;
        this.metricRegistry = metricsRegistry;
        this.taskMetricsService = taskMetricsService;
    }

    public Thread startWatcherThread() {
        watcher = new TaskQueueWatcher(taskExecutorService, registry, taskTypeFactoryMap, statistics, 
                                     curatorFactory, redisService, zkRoot, isActiveActiveHAEnabled, metricRegistry, taskMetricsService);
        watcherThread = new Thread(watcher);
        watcherThread.start();
        return watcherThread;
    }

    public void stopQueueWatcher() {
        if (watcher != null) {
            watcher.shutdown();
        }
    }

    static class TaskConsumer implements Runnable {
        private static final int MAX_ATTEMPT_COUNT = 3;

        private final Map<String, TaskFactory>  taskTypeFactoryMap;
        private final TaskRegistry              registry;
        private final TaskManagement.Statistics statistics;
        private final AtlasTask                 task;
        private CountDownLatch  latch;

        AtlasPerfTracer perf = null;

        public TaskConsumer(AtlasTask task, TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                            CountDownLatch latch) {
            this.task               = task;
            this.registry           = registry;
            this.taskTypeFactoryMap = taskTypeFactoryMap;
            this.statistics         = statistics;
            this.latch = latch;
        }

        @Override
        public void run() {
            AtlasVertex taskVertex = null;
            int         attemptCount;

            try {
                RequestContext.get().setTraceId("task-"+task.getGuid());
                if (task == null) {
                    TASK_LOG.info("Task not scheduled as it was not found");
                    return;
                }

                TASK_LOG.info("Task guid = "+task.getGuid());
                taskVertex = registry.getVertex(task.getGuid());
                if (taskVertex == null) {
                    TASK_LOG.warn("Task not scheduled as vertex not found", task);
                }

                if (task.getStatus() == AtlasTask.Status.COMPLETE) {
                    TASK_LOG.warn("Task not scheduled as status was COMPLETE!", task);
                }

                if (perfEnabled) {
                    perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, String.format("atlas.task:%s", task.getGuid(), task.getType()));
                }

                statistics.increment(1);

                attemptCount = task.getAttemptCount();

                if (attemptCount >= MAX_ATTEMPT_COUNT) {
                    TASK_LOG.warn("Max retry count for task exceeded! Skipping!", task);

                    task.setStatus(AtlasTask.Status.FAILED);
                    registry.updateStatus(taskVertex, task);

                    return;
                }

                LOG.info(String.format("Started performing task with guid: %s", task.getGuid()));

                performTask(taskVertex, task);

                LOG.info(String.format("Finished task with guid: %s", task.getGuid()));

            } catch (InterruptedException exception) {
                registry.updateStatus(taskVertex, task);
                TASK_LOG.error("{}: {}: Interrupted!", task, exception);

                statistics.error();
            } catch (Exception exception) {
                if (task != null) {
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

                latch.countDown();
                RequestContext.get().clearCache();
                AtlasPerfTracer.log(perf);
            }
        }

        private void performTask(AtlasVertex taskVertex, AtlasTask task) throws Exception {
            TaskFactory  factory      = taskTypeFactoryMap.get(task.getType());
            if (factory == null) {
                LOG.error("taskTypeFactoryMap does not contain task of type: {}", task.getType());
                return;
            }

            AbstractTask runnableTask = factory.create(task);

            registry.inProgress(taskVertex, task);

            runnableTask.run();

            registry.complete(taskVertex, task);

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