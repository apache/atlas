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
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class TaskQueueWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueWatcher.class);
    private static final TaskExecutor.TaskLogger TASK_LOG         = TaskExecutor.TaskLogger.getLogger();

    private TaskRegistry registry;
    private final ExecutorService executorService;
    private final Map<String, TaskFactory> taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;

    private static long pollInterval = AtlasConfiguration.TASKS_REQUEUE_POLL_INTERVAL.getLong();

    private CountDownLatch latch = null;

    public TaskQueueWatcher(ExecutorService executorService, TaskRegistry registry,
                            Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                            CountDownLatch latch) {

        this.registry = registry;
        this.executorService = executorService;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics = statistics;
        this.latch = latch;
    }

    @Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("TaskQueueWatcher: running {}:{}", Thread.currentThread().getName(), Thread.currentThread().getId());
        }
        RequestContext.setWatcherThreadAlive(true);

        while (true) {
            try {
                if (TaskManagement.hasStopped()) {
                    break;
                }

                if (latch != null && latch.getCount() != 0) {
                    LOG.info("TaskQueueWatcher: Waiting on Latch, current count: {}", latch.getCount());
                    latch.await();
                }

                if (latch != null) {
                    LOG.info("TaskQueueWatcher: Latch wait complete!!");
                }

                TasksFetcher fetcher = new TasksFetcher(registry);

                Thread tasksFetcherThread = new Thread(fetcher);
                tasksFetcherThread.start();
                tasksFetcherThread.join();

                List<AtlasTask> tasks = fetcher.getTasks();
                if (CollectionUtils.isNotEmpty(tasks)) {
                    addAll(tasks);
                } else {
                    LOG.info("No tasks to queue, sleeping for {} ms", pollInterval);
                }

                Thread.sleep(pollInterval);

            } catch (InterruptedException interruptedException) {
                LOG.error("TaskQueueWatcher: Interrupted");
                LOG.error("TaskQueueWatcher thread is terminated, new tasks will not be loaded into the queue until next restart");
                RequestContext.setWatcherThreadAlive(false);
                break;
            } catch (Exception e){
                LOG.error("TaskQueueWatcher: Exception occurred");
                e.printStackTrace();
            }
        }
    }

    private void addAll(List<AtlasTask> tasks) {
        if (CollectionUtils.isNotEmpty(tasks)) {
            latch = new CountDownLatch(tasks.size());

            for (AtlasTask task : tasks) {
                if (task == null) {
                    continue;
                }
                TASK_LOG.log(task);

                this.executorService.submit(new TaskExecutor.TaskConsumer(task, this.registry, this.taskTypeFactoryMap, this.statistics, latch));
            }

            LOG.info("TasksFetcher: Submitted {} tasks to the queue", tasks.size());
        } else {
            if (LOG.isDebugEnabled()){
                LOG.debug("TasksFetcher: No task to queue");
            }
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
            if (LOG.isDebugEnabled()){
                LOG.debug("TasksFetcher: Fetching tasks for queuing");
            }
            LOG.info("TasksFetcher: Fetching tasks for queuing");

            this.tasks = registry.getTasksForReQueue();
        }

        public List<AtlasTask> getTasks() {
            return tasks;
        }
    }
}
