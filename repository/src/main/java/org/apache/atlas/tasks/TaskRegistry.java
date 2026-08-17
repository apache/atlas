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
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.TASK_GUID;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;

@Lazy
@Component
public class TaskRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRegistry.class);

    private final AtlasGraph graph;
    private final long       inProgressStaleThresholdMs;
    private final String     nodeId;

    @Inject
    public TaskRegistry(AtlasGraph graph) {
        this(graph, AtlasConfiguration.TASK_CLAIM_STALE_THRESHOLD_MS.getLong());
    }

    @VisibleForTesting
    TaskRegistry(AtlasGraph graph, long inProgressStaleThresholdMs) {
        this.graph = graph;
        this.inProgressStaleThresholdMs = inProgressStaleThresholdMs;
        this.nodeId = buildNodeId();
    }

    @GraphTransaction
    public AtlasTask save(AtlasTask task) {
        AtlasVertex vertex = createVertex(task);

        return toAtlasTask(vertex);
    }

    @GraphTransaction
    public List<AtlasTask> getPendingTasks() {
        List<AtlasTask> ret = new ArrayList<>();

        try {
            AtlasGraphQuery query = graph.query()
                    .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                    .has(Constants.TASK_STATUS, AtlasTask.Status.PENDING)
                    .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

            for (AtlasVertex vertex : (Iterable<AtlasVertex>) query.vertices()) {
                ret.add(toAtlasTask(vertex));
            }
        } catch (Exception exception) {
            LOG.error("Error fetching pending tasks!", exception);
        } finally {
            graph.commit();
        }

        return ret;
    }

    @GraphTransaction
    public List<AtlasTask> getPendingTasksByType(String type) {
        List<AtlasTask> ret = new ArrayList<>();

        try {
            AtlasGraphQuery query = graph.query()
                    .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                    .has(Constants.TASK_STATUS, AtlasTask.Status.PENDING)
                    .has(Constants.TASK_TYPE, type)
                    .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

            for (AtlasVertex vertex : (Iterable<AtlasVertex>) query.vertices()) {
                ret.add(toAtlasTask(vertex));
            }
        } catch (Exception exception) {
            LOG.error("Error fetching pending tasks by type!", exception);
        }

        return ret;
    }

    @GraphTransaction
    public void updateStatus(AtlasVertex taskVertex, AtlasTask task) {
        if (taskVertex == null) {
            return;
        }

        setEncodedProperty(taskVertex, Constants.TASK_ATTEMPT_COUNT, task.getAttemptCount());
        setEncodedProperty(taskVertex, Constants.TASK_STATUS, task.getStatus().toString());
        setEncodedProperty(taskVertex, Constants.TASK_UPDATED_TIME, System.currentTimeMillis());
        setEncodedProperty(taskVertex, Constants.TASK_ERROR_MESSAGE, task.getErrorMessage());
    }

    @GraphTransaction
    public void deleteByGuid(String guid) throws AtlasBaseException {
        try {
            AtlasGraphQuery query = graph.query()
                    .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                    .has(TASK_GUID, guid);

            Iterator<AtlasVertex> results = query.vertices().iterator();

            if (results.hasNext()) {
                graph.removeVertex(results.next());
            }
        } catch (Exception exception) {
            LOG.error("Error: deletingByGuid: {}", guid);

            throw new AtlasBaseException(exception);
        }
    }

    /**
     * Atomically claims a task for execution on this node by transitioning its status
     * from {@code PENDING} to {@code IN_PROGRESS} inside a single graph transaction.
     *
     * <p>In an active-active cluster every node calls {@code queuePendingTasks()} on startup
     * and may also receive task dispatch calls at runtime.  Without a claim step, multiple
     * nodes would execute the same task concurrently.  This method provides the
     * Compare-And-Swap (CAS) guard:
     * <ul>
     *   <li>The first node whose transaction commits wins the claim and must execute the task.</li>
     *   <li>Any other node that concurrently attempts the same CAS gets a JanusGraph
     *       {@code PermanentLockingException}. The {@link GraphTransactionInterceptor} retries,
     *       but on retry the vertex status is already {@code IN_PROGRESS} (or {@code COMPLETE}),
     *       so the query returns no results and the method returns {@code false}.</li>
     * </ul>
     *
     * @param taskGuid the GUID of the task to claim
     * @return {@code true} if this node successfully claimed the task; {@code false} if the
     *         task was not found, was not in {@code PENDING} state, or was already claimed
     *         by another node
     */
    @GraphTransaction
    public boolean tryClaimTask(String taskGuid) {
        // AsyncImport-style global serialization: allow claiming only when
        // there is no task already IN_PROGRESS.
        if (hasAnyTaskInProgress()) {
            LOG.debug("TaskRegistry.tryClaimTask({}): node={} claim denied, global IN_PROGRESS task exists", taskGuid, nodeId);
            return false;
        }

        // Preserve FIFO order: only the oldest PENDING task is claimable.
        // This prevents newer tasks from leapfrogging older tasks when multiple
        // nodes race to claim tasks at startup/runtime.
        if (!isOldestPendingTask(taskGuid)) {
            LOG.debug("TaskRegistry.tryClaimTask({}): node={} claim denied, not oldest pending task", taskGuid, nodeId);
            return false;
        }

        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .has(Constants.TASK_GUID, taskGuid)
                .has(Constants.TASK_STATUS, AtlasTask.Status.PENDING.toString());

        Iterator<AtlasVertex> results = query.vertices().iterator();

        if (!results.hasNext()) {
            // Task not found or not PENDING — already claimed or completed by another node.
            LOG.debug("TaskRegistry.tryClaimTask({}): node={} claim denied, task not PENDING/found", taskGuid, nodeId);
            return false;
        }

        AtlasVertex taskVertex = results.next();
        long        now        = System.currentTimeMillis();

        setEncodedProperty(taskVertex, Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS.toString());
        setEncodedProperty(taskVertex, Constants.TASK_START_TIME, now);
        setEncodedProperty(taskVertex, Constants.TASK_UPDATED_TIME, now);

        LOG.info("TaskRegistry.tryClaimTask({}): node={} claimed IN_PROGRESS", taskGuid, nodeId);
        return true;
    }

    @GraphTransaction
    public void recoverStaleInProgressTasks() {
        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .has(Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS.toString());
        long now = System.currentTimeMillis();

        for (AtlasVertex vertex : (Iterable<AtlasVertex>) query.vertices()) {
            String taskGuid    = vertex.getProperty(Constants.TASK_GUID, String.class);
            Long   updatedTime = vertex.getProperty(Constants.TASK_UPDATED_TIME, Long.class);

            if (!isStaleInProgress(updatedTime, now)) {
                continue;
            }

            LOG.warn("TaskRegistry.recoverStaleInProgressTasks(): recovering stale IN_PROGRESS task {} back to PENDING",
                    taskGuid);
            LOG.warn("TaskRegistry.recoverStaleInProgressTasks(): node={} recovered stale task {}", nodeId, taskGuid);
            setEncodedProperty(vertex, Constants.TASK_STATUS, AtlasTask.Status.PENDING.toString());
            setEncodedProperty(vertex, Constants.TASK_UPDATED_TIME, now);
        }
    }

    private String buildNodeId() {
        String runMode  = AtlasRunMode.current().name();
        String hostName = System.getenv("HOSTNAME");
        String jvmId    = ManagementFactory.getRuntimeMXBean().getName();

        if (hostName == null || hostName.trim().isEmpty()) {
            hostName = "unknown-host";
        }

        return runMode + "@" + hostName + "#" + jvmId;
    }

    private boolean hasAnyTaskInProgress() {
        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .has(Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS.toString());

        return query.vertices().iterator().hasNext();
    }

    private boolean isStaleInProgress(Long updatedTime, long now) {
        if (updatedTime == null || updatedTime <= 0L) {
            return true;
        }

        return now - updatedTime >= inProgressStaleThresholdMs;
    }

    private boolean isOldestPendingTask(String taskGuid) {
        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .has(Constants.TASK_STATUS, AtlasTask.Status.PENDING.toString())
                .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

        Iterator<AtlasVertex> pending = query.vertices().iterator();

        if (!pending.hasNext()) {
            return false;
        }

        AtlasVertex oldestPending = pending.next();
        String oldestGuid = oldestPending.getProperty(Constants.TASK_GUID, String.class);

        return taskGuid.equals(oldestGuid);
    }

    @GraphTransaction
    public void deleteComplete(AtlasVertex taskVertex, AtlasTask task) {
        updateStatus(taskVertex, task);

        deleteVertex(taskVertex);
    }

    @GraphTransaction
    public AtlasTask getById(String guid) {
        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .has(TASK_GUID, guid);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        return results.hasNext() ? toAtlasTask(results.next()) : null;
    }

    @GraphTransaction
    public AtlasVertex getVertex(String taskGuid) {
        AtlasGraphQuery query = graph.query().has(Constants.TASK_GUID, taskGuid);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        return results.hasNext() ? results.next() : null;
    }

    @GraphTransaction
    public List<AtlasTask> getAll() {
        List<AtlasTask> ret = new ArrayList<>();

        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

        for (AtlasVertex atlasVertex : (Iterable<AtlasVertex>) query.vertices()) {
            ret.add(toAtlasTask(atlasVertex));
        }

        return ret;
    }

    public void commit() {
        this.graph.commit();
    }

    public AtlasTask createVertex(String taskType, String createdBy, Map<String, Object> parameters) {
        AtlasTask ret = new AtlasTask(taskType, createdBy, parameters);

        createVertex(ret);

        return ret;
    }

    private void deleteVertex(AtlasVertex taskVertex) {
        if (taskVertex == null) {
            return;
        }

        graph.removeVertex(taskVertex);
    }

    private AtlasTask toAtlasTask(AtlasVertex v) {
        AtlasTask ret = new AtlasTask();

        String guid = v.getProperty(Constants.TASK_GUID, String.class);
        if (guid != null) {
            ret.setGuid(guid);
        }

        String type = v.getProperty(Constants.TASK_TYPE, String.class);
        if (type != null) {
            ret.setType(type);
        }

        String status = v.getProperty(Constants.TASK_STATUS, String.class);
        if (status != null) {
            ret.setStatus(status);
        }

        String createdBy = v.getProperty(Constants.TASK_CREATED_BY, String.class);
        if (createdBy != null) {
            ret.setCreatedBy(createdBy);
        }

        Long createdTime = v.getProperty(Constants.TASK_CREATED_TIME, Long.class);
        if (createdTime != null) {
            ret.setCreatedTime(new Date(createdTime));
        }

        Long updatedTime = v.getProperty(Constants.TASK_UPDATED_TIME, Long.class);
        if (updatedTime != null) {
            ret.setUpdatedTime(new Date(updatedTime));
        }

        Long startTime = v.getProperty(Constants.TASK_START_TIME, Long.class);
        if (startTime != null) {
            ret.setStartTime(new Date(startTime));
        }

        Long endTime = v.getProperty(Constants.TASK_END_TIME, Long.class);
        if (endTime != null) {
            ret.setEndTime(new Date(endTime));
        }

        String parametersJson = v.getProperty(Constants.TASK_PARAMETERS, String.class);
        if (parametersJson != null) {
            ret.setParameters(AtlasType.fromJson(parametersJson, Map.class));
        }

        Integer attemptCount = v.getProperty(Constants.TASK_ATTEMPT_COUNT, Integer.class);
        if (attemptCount != null) {
            ret.setAttemptCount(attemptCount);
        }

        String errorMessage = v.getProperty(Constants.TASK_ERROR_MESSAGE, String.class);
        if (errorMessage != null) {
            ret.setErrorMessage(errorMessage);
        }

        return ret;
    }

    private AtlasVertex createVertex(AtlasTask task) {
        AtlasVertex ret = graph.addVertex();

        setEncodedProperty(ret, Constants.TASK_GUID, task.getGuid());
        setEncodedProperty(ret, Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME);
        setEncodedProperty(ret, Constants.TASK_STATUS, task.getStatus().toString());
        setEncodedProperty(ret, Constants.TASK_TYPE, task.getType());
        setEncodedProperty(ret, Constants.TASK_CREATED_BY, task.getCreatedBy());
        setEncodedProperty(ret, Constants.TASK_CREATED_TIME, task.getCreatedTime());
        setEncodedProperty(ret, Constants.TASK_UPDATED_TIME, task.getUpdatedTime());

        if (task.getStartTime() != null) {
            setEncodedProperty(ret, Constants.TASK_START_TIME, task.getStartTime().getTime());
        }

        if (task.getEndTime() != null) {
            setEncodedProperty(ret, Constants.TASK_END_TIME, task.getEndTime().getTime());
        }

        setEncodedProperty(ret, Constants.TASK_PARAMETERS, AtlasJson.toJson(task.getParameters()));
        setEncodedProperty(ret, Constants.TASK_ATTEMPT_COUNT, task.getAttemptCount());
        setEncodedProperty(ret, Constants.TASK_ERROR_MESSAGE, task.getErrorMessage());

        return ret;
    }
}
