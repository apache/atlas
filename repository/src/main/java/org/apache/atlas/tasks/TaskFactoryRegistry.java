/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.tasks;

import io.micrometer.core.instrument.Timer;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.metrics.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Set;

@Component
public class TaskFactoryRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TaskFactoryRegistry.class);

    private final TaskManagement taskManagement;

    @Inject
    public TaskFactoryRegistry(TaskManagement taskManagement, Set<TaskFactory> factories) {
        this.taskManagement = taskManagement;
        for (TaskFactory factory : factories) {
            taskManagement.addFactory(factory);
        }

        LOG.info("TaskFactoryRegistry: TaskManagement updated with factories: {}", factories.size());
    }

    @PostConstruct
    public void startTaskManagement() throws AtlasException {
        Timer.Sample sample = Timer.start(MetricUtils.getMeterRegistry());
        try {
            if (taskManagement.isWatcherActive()) {
                LOG.info("TaskFactoryRegistry: TaskManagement already started!");
                return;
            }

            LOG.info("TaskFactoryRegistry: Starting TaskManagement...");
            taskManagement.start();
        } catch (AtlasException e) {
            LOG.error("Error starting TaskManagement!", e);
            throw e;
        } finally {
            sample.stop(Timer.builder("atlas.startup.taskmanager.start.duration")
                    .description("Time taken to start task management during Atlas startup")
                    .register(MetricUtils.getMeterRegistry()));
        }
    }
}
