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
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.service.config.ConfigKey;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.repository.metrics.TaskMetricsService;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskQueueWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueWatcher.class);
    private static final TaskExecutor.TaskLogger TASK_LOG = TaskExecutor.TaskLogger.getLogger();
    private final String zkRoot;
    private final boolean isActiveActiveHAEnabled;
    private final MetricsRegistry metricRegistry;

    private TaskRegistry registry;
    private final ExecutorService executorService;
    private final Map<String, TaskFactory> taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;
    private final ICuratorFactory curatorFactory;
    private final RedisService redisService;
    private final TaskMetricsService taskMetricsService;

    private static long pollInterval = AtlasConfiguration.TASKS_REQUEUE_POLL_INTERVAL.getLong();
    private static final String TASK_LOCK = "/task-lock";
    private static final String ATLAS_TASK_LOCK = "atlas:task:lock";

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);
    private final AtomicBoolean maintenanceModeActivated = new AtomicBoolean(false);
    private final String podId = System.getenv().getOrDefault("HOSTNAME", "unknown-pod");

    public TaskQueueWatcher(ExecutorService executorService, TaskRegistry registry,
                            Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                            ICuratorFactory curatorFactory, RedisService redisService, final String zkRoot, boolean isActiveActiveHAEnabled, MetricsRegistry metricsRegistry,
                            TaskMetricsService taskMetricsService) {

        this.registry = registry;
        this.executorService = executorService;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics = statistics;
        this.curatorFactory = curatorFactory;
        this.redisService = redisService;
        this.zkRoot = zkRoot;
        this.isActiveActiveHAEnabled = isActiveActiveHAEnabled;
        this.metricRegistry = metricsRegistry;
        this.taskMetricsService = taskMetricsService;
    }

    public void shutdown() {
        shouldRun.set(false);
        LOG.info("TaskQueueWatcher: Shutdown");
    }

    @Override
    public void run() {
        if (isMaintenanceModeEnabled()) {
            LOG.info("TaskQueueWatcher: Maintenance mode is enabled, new tasks will not be loaded into the queue until next restart");
            return;
        }
        shouldRun.set(true);

        if (LOG.isDebugEnabled()) {
            LOG.debug("TaskQueueWatcher: running {}:{}", Thread.currentThread().getName(), Thread.currentThread().getId());
        }
        LOG.info("TaskQueueWatcher: Time constants - pollInterval: {}, TASK_WAIT_TIME_MS: {}", pollInterval, AtlasConstants.TASK_WAIT_TIME_MS);

        while (shouldRun.get()) {
            // Quick check - if MM is enabled AND we've already recorded activation, skip lock acquisition
            // We must still acquire lock at least once to set activation metadata
            if (isMaintenanceModeEnabled() && maintenanceModeActivated.get()) {
                LOG.info("TaskQueueWatcher: Maintenance mode enabled (already activated), pausing task processing for {} ms", pollInterval);
                try {
                    Thread.sleep(pollInterval);
                } catch (InterruptedException e) {
                    LOG.warn("TaskQueueWatcher: Sleep interrupted during maintenance mode");
                    break;
                }
                continue;
            }

            RequestContext requestContext = RequestContext.get();
            requestContext.setMetricRegistry(this.metricRegistry);
            TasksFetcher fetcher = new TasksFetcher(registry);
            boolean lockAcquired = false;
            try {
                if (!redisService.acquireDistributedLock(ATLAS_TASK_LOCK)) {
                    LOG.info("TaskQueueWatcher: Failed to acquire distributed lock, sleeping for TASK_WAIT_TIME_MS: {}", AtlasConstants.TASK_WAIT_TIME_MS);
                    Thread.sleep(AtlasConstants.TASK_WAIT_TIME_MS);
                    continue;
                }
                LOG.info("TaskQueueWatcher: Acquired distributed lock: {}", ATLAS_TASK_LOCK);
                lockAcquired = true;

                // We now hold the lock - we're the "processing pod"
                // Check MM again - if enabled, set activation and release lock
                if (isMaintenanceModeEnabled()) {
                    if (!maintenanceModeActivated.get()) {
                        setMaintenanceModeActivated();
                        LOG.info("TaskQueueWatcher: Maintenance mode ACTIVATED - task processing paused by lock-holding pod {}", podId);
                    }
                    continue; // Release lock in finally, don't process
                }

                // Reset local flag when MM is not enabled
                maintenanceModeActivated.set(false);

                List<AtlasTask> tasks = fetcher.getTasks();
                
                // Update queue size metric
                taskMetricsService.updateQueueSize(tasks != null ? tasks.size() : 0);
                
                if (CollectionUtils.isNotEmpty(tasks)) {
                    final CountDownLatch latch = new CountDownLatch(tasks.size());
                    submitAll(tasks, latch);
                    LOG.info("Submitted {} tasks to the queue", tasks.size());
                    waitForTasksToComplete(latch);
                } else {
                    LOG.info("TaskQueueWatcher: No tasks fetched during this cycle.");
                }
            } catch (InterruptedException interruptedException) {
                LOG.error("TaskQueueWatcher: Interrupted: thread is terminated, new tasks will not be loaded into the queue until next restart");
                break;
            } catch (Exception e) {
                LOG.error("TaskQueueWatcher: Exception occurred " + e.getMessage(), e);
            } finally {
                fetcher.clearTasks();
                if (lockAcquired) {
                    redisService.releaseDistributedLock(ATLAS_TASK_LOCK);
                    LOG.info("TaskQueueWatcher: Released Task Lock in finally");
                }
            }
            try{
                LOG.info("TaskQueueWatcher: Sleeping for pollInterval: {}", pollInterval);
                Thread.sleep(pollInterval);}
            catch (Exception e){
                LOG.warn("TaskQueueWatcher: Sleep interrupted, exiting.");
                break;
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
        if (CollectionUtils.isEmpty(tasks)) {
            LOG.info("TasksFetcher: No task to queue");
            return;
        }

        int submittedCount = 0;
        
        for (AtlasTask task : tasks) {
            if (task == null) {
                continue;
            }

            String taskGuid = task.getGuid();
            boolean taskSubmitted = false;
            
            // Keep trying until the task is submitted
            while (!taskSubmitted) {
                if (isMemoryTooHigh()) {
                    LOG.warn("High memory usage detected ({}%), pausing task submission for task: {}", 
                        getMemoryUsagePercent() * 100, taskGuid);
                    
                    try {
                        // Wait for memory to be freed
                        Thread.sleep(AtlasConfiguration.TASK_HIGH_MEMORY_PAUSE_MS.getLong());
                        
                        // Suggest GC if memory is still high after initial wait
                        if (isMemoryTooHigh()) {
                            LOG.info("Memory still high after pause, suggesting garbage collection");
                            System.gc();
                            Thread.sleep(1000); // Give GC time to work
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Sleep interrupted while waiting for memory to free", e);
                        Thread.currentThread().interrupt();
                        return; // Exit if interrupted
                    }
                } else {
                    // Memory is okay, submit the task
                    TASK_LOG.log(task);
                    this.executorService.submit(new TaskExecutor.TaskConsumer(task, 
                        this.registry, this.taskTypeFactoryMap, this.statistics, latch));
                    
                    taskSubmitted = true;
                    submittedCount++;
                    LOG.debug("Successfully submitted task: {}", taskGuid);
                }
            }
        }

        if (submittedCount > 0) {
            LOG.info("TasksFetcher: Submitted {} tasks to the queue", submittedCount);
        }
    }

    private boolean isMemoryTooHigh() {
        return getMemoryUsagePercent() > (AtlasConfiguration.TASK_MEMORY_THRESHOLD_PERCENT.getInt() / 100.0);
    }

    private double getMemoryUsagePercent() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;

        return (double) usedMemory / maxMemory;
    }

    /**
     * Check if maintenance mode is enabled dynamically.
     * Uses DynamicConfigStore if enabled, otherwise falls back to static configuration.
     *
     * @return true if maintenance mode is enabled, false otherwise
     */
    private boolean isMaintenanceModeEnabled() {
        try {
            if (DynamicConfigStore.isEnabled()) {
                return DynamicConfigStore.getConfigAsBoolean(ConfigKey.MAINTENANCE_MODE.getKey());
            }
        } catch (Exception e) {
            LOG.debug("Error checking DynamicConfigStore for maintenance mode, falling back to static config", e);
        }
        return AtlasConfiguration.ATLAS_MAINTENANCE_MODE.getBoolean();
    }

    /**
     * Set the maintenance mode activation flags in ConfigStore.
     * Called when task processing is actually paused due to maintenance mode.
     */
    private void setMaintenanceModeActivated() {
        try {
            if (DynamicConfigStore.isEnabled()) {
                String now = Instant.now().toString();
                DynamicConfigStore.setConfig(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey(), now, "system");
                DynamicConfigStore.setConfig(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey(), podId, "system");
                maintenanceModeActivated.set(true);
                LOG.info("TaskQueueWatcher: Maintenance mode ACTIVATED at {} by pod {}", now, podId);
            }
        } catch (Exception e) {
            LOG.warn("TaskQueueWatcher: Failed to set maintenance mode activation flags", e);
        }
    }



    static class TasksFetcher {
        private TaskRegistry registry;
        private List<AtlasTask> tasks = new ArrayList<>();

        public TasksFetcher(TaskRegistry registry) {
            this.registry = registry;
        }

        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("TasksFetcher: Fetching tasks for queuing");
            }

            this.tasks = registry.getTasksForReQueue();
            RequestContext requestContext = RequestContext.get();
            requestContext.clearCache();
        }

        public List<AtlasTask> getTasks() {
            run();
            return tasks;
        }

        public void clearTasks() {
            this.tasks.clear();
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
