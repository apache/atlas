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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.ICuratorFactory;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskQueueWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueWatcher.class);
    private static final TaskExecutor.TaskLogger TASK_LOG = TaskExecutor.TaskLogger.getLogger();
    private final String zkRoot;
    private final boolean isActiveActiveHAEnabled;

    private TaskRegistry registry;
    private final ExecutorService executorService;
    private final Map<String, TaskFactory> taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;
    private final ICuratorFactory curatorFactory;
    private final RedisService redisService;

    private static long pollInterval = AtlasConfiguration.TASKS_REQUEUE_POLL_INTERVAL.getLong();
    private static final String TASK_LOCK = "/task-lock";
    private static final String ATLAS_TASK_LOCK = "atlas:task:lock";

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public TaskQueueWatcher(ExecutorService executorService, TaskRegistry registry,
                            Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                            ICuratorFactory curatorFactory, RedisService redisService, final String zkRoot, boolean isActiveActiveHAEnabled) {

        this.registry = registry;
        this.executorService = executorService;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics = statistics;
        this.curatorFactory = curatorFactory;
        this.redisService = redisService;
        this.zkRoot = zkRoot;
        this.isActiveActiveHAEnabled = isActiveActiveHAEnabled;
    }

    public void shutdown() {
        shouldRun.set(false);
        LOG.info("TaskQueueWatcher: Shutdown");
    }

    @Override
    public void run() {
        boolean isMaintenanceMode = AtlasConfiguration.ATLAS_MAINTENANCE_MODE.getBoolean();
        if (isMaintenanceMode) {
            LOG.info("TaskQueueWatcher: Maintenance mode is enabled, new tasks will not be loaded into the queue until next restart");
            return;
        }
        shouldRun.set(true);

        if (LOG.isDebugEnabled()) {
            LOG.debug("TaskQueueWatcher: running {}:{}", Thread.currentThread().getName(), Thread.currentThread().getId());
        }
        while (shouldRun.get()) {
            try {
                if (!redisService.acquireDistributedLock(ATLAS_TASK_LOCK)) {
                    Thread.sleep(AtlasConstants.TASK_WAIT_TIME_MS);
                    continue;
                }

                TasksFetcher fetcher = new TasksFetcher(registry);

                Thread tasksFetcherThread = new Thread(fetcher);
                tasksFetcherThread.start();
                tasksFetcherThread.join();

                List<AtlasTask> tasks = fetcher.getTasks();
                if (CollectionUtils.isNotEmpty(tasks)) {
                    final CountDownLatch latch = new CountDownLatch(tasks.size());
                    submitAll(tasks, latch);
                    LOG.info("Submitted {} tasks to the queue", tasks.size());
                    waitForTasksToComplete(latch);
                } else {
                    redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
                }
                Thread.sleep(pollInterval);
            } catch (InterruptedException interruptedException) {
                LOG.error("TaskQueueWatcher: Interrupted: thread is terminated, new tasks will not be loaded into the queue until next restart");
                break;
            } catch (Exception e) {
                LOG.error("TaskQueueWatcher: Exception occurred " + e.getMessage(), e);
            } finally {
                redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
            }
        }
    }

    private void waitForTasksToComplete(final CountDownLatch latch) throws InterruptedException {
        if (latch.getCount() != 0) {
            LOG.info("TaskQueueWatcher: Waiting on Latch, current count: {}", latch.getCount());
            latch.await();
            LOG.info("TaskQueueWatcher: Waiting completed on Latch, current count: {}", latch.getCount());
        }
    }

    private void submitAll(List<AtlasTask> tasks, CountDownLatch latch) {
        if (CollectionUtils.isNotEmpty(tasks)) {

            for (AtlasTask task : tasks) {
                if (task != null) {
                    TASK_LOG.log(task);
                }

                this.executorService.submit(new TaskExecutor.TaskConsumer(task, this.registry, this.taskTypeFactoryMap, this.statistics, latch));
            }

            LOG.info("TasksFetcher: Submitted {} tasks to the queue", tasks.size());
        } else {
            LOG.info("TasksFetcher: No task to queue");
        }
    }

    static class TasksFetcher implements Runnable {
        private TaskRegistry registry;
        private List<AtlasTask> tasks = new ArrayList<>();

        public TasksFetcher(TaskRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("TasksFetcher: Fetching tasks for queuing");
            }

            this.tasks = registry.getTasksForReQueue();
        }

        public List<AtlasTask> getTasks() {
            return tasks;
        }
    }

    @PreDestroy
    public void cleanUp() {
        if (!Objects.isNull(this.executorService)) {
            this.redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
            this.executorService.shutdownNow();
            try {
                this.executorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
