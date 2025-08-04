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
import org.apache.atlas.RequestContext;

import org.apache.atlas.model.tasks.AtlasTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.model.tasks.AtlasTask.Status;

public abstract class AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTask.class);
    private final AtlasTask task;

    public static final String X_ATLAN_TASK_GUID = "x-atlan-task-guid";

    public AbstractTask(AtlasTask task) {
        this.task = task;
    }

    public void run() throws Exception {
        try {
            RequestContext.get().addRequestContextHeader(X_ATLAN_TASK_GUID, getTaskGuid());
            perform();
        } catch (Exception exception) {
            task.setStatus(Status.FAILED);

            task.setErrorMessage(exception.getMessage());

            task.incrementAttemptCount();

            throw exception;
        } finally {
            task.end();
        }
    }

    protected void setStatus(Status status) {
        task.setStatus(status);
    }

    public Status getStatus() {
        return this.task.getStatus();
    }

    public String getTaskGuid() {
        return task.getGuid();
    }

    public String getTaskType() {
        return task.getType();
    }

    protected AtlasTask getTaskDef() {
        return this.task;
    }

    public abstract Status perform() throws Exception;

    public AtlasTask getTask() {
        return task;
    }

}