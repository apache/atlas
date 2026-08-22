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

    /**
     * Records the outcome of a task and gives the cluster-wide claim back.  Every path out of
     * execution comes through here, so releasing the claim at this one point is what keeps a
     * finished task from blocking the rest of the cluster forever.
     */
    @GraphTransaction
    public void updateStatus(AtlasVertex taskVertex, AtlasTask task) {
        if (taskVertex == null) {
            return;
        }

        GraphClaim.releaseLease(graph, Constants.CLAIM_TASK_RUNNER, nodeId);

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
                deleteVertex(results.next());
            }
        } catch (Exception exception) {
            LOG.error("Error: deletingByGuid: {}", guid);

            throw new AtlasBaseException(exception);
        }
    }

    /**
     * Atomically claims the next task this node may execute, transitioning it from
     * {@code PENDING} to {@code IN_PROGRESS} inside a single graph transaction.
     *
     * <p>Claiming is a <em>pull</em>: the caller does not nominate a task, it asks for
     * whichever task is next in line.  This is what makes cluster-wide ordering workable.
     * Tasks are created on whichever node served the request, but any node may execute any
     * task, so a task is never stranded in the queue of a node that is busy or unable to run
     * it.  A caller that nominated a specific task would have to wait for its turn while
     * holding a worker thread, and the task it is waiting for may well be behind it in that
     * same worker's queue — a deadlock.
     *
     * <p>Two invariants are enforced here, both required by classification propagation:
     * <ul>
     *   <li>At most one task runs in the cluster at any time, so an add and a delete of the
     *       same classification can never overlap.</li>
     *   <li>Tasks are handed out oldest-first, so those two never run out of order.</li>
     * </ul>
     *
     * <p>The race between nodes is settled by {@link GraphClaim}, not by the status write, which
     * would serialise nothing: two nodes can both read {@code PENDING} and both write
     * {@code IN_PROGRESS}.  The claim is a lease on a single cluster-wide runner slot, held on a
     * vertex of its own rather than on the task being claimed.  Marking the task itself is not
     * exclusive: uniqueness stops two <em>vertices</em> from holding one claim name, so nodes that
     * picked different tasks conflict, but nodes that picked the <em>same</em> task write the same
     * marker and both writes stand.  Taking the slot, by contrast, means creating a vertex that only
     * one node can create, whichever task each of them had in mind.
     *
     * <p>The slot is leased so that a node dying mid-task cannot keep the cluster idle forever; the
     * lease runs for the same stale threshold that returns its abandoned task to {@code PENDING}.
     *
     * <p>The slot is taken and committed <em>before</em> a task is looked for, which is what makes
     * the claim exclusive rather than merely optimistic.  The store gets to refuse a claim only when
     * the claim is committed, so a claim that rides along in the same transaction as the work
     * excludes nobody: both nodes read the slot as free, both write it, and neither commit conflicts
     * because each has let go of the slot by the time the other commits.  Committing the claim first
     * also gives this node a fresh view of the queue, without which it can pick up a task a peer has
     * just finished.
     *
     * <p>Call through {@link GraphClaim#attempt(GraphClaim.ClaimAttempt)}: a lost race may
     * surface as a return of {@code null} or as a thrown conflict, depending on when the backend
     * refuses the write.
     *
     * @return the claimed task, or {@code null} if a task is already running elsewhere in the
     *         cluster or there is nothing pending
     * @throws ClaimConflictException if another node won the race for this task
     */
    @GraphTransaction
    public AtlasTask claimNextPendingTask() {
        // Both of these run before the slot is taken, so that a node polling while its own task is
        // still running cannot end up releasing the slot it holds for that task.  Neither is the
        // exclusion - a stale view can report either wrongly - they only avoid taking a slot this
        // node has no use for.  The slot itself is what excludes.
        if (hasAnyTaskInProgress()) {
            LOG.debug("TaskRegistry.claimNextPendingTask(): node={} no claim, a task is already in progress", nodeId);

            return null;
        }

        if (findOldestPendingVertex() == null) {
            return null;
        }

        if (!GraphClaim.claimLeaseAndCommit(graph, Constants.CLAIM_TASK_RUNNER, nodeId, inProgressStaleThresholdMs)) {
            LOG.debug("TaskRegistry.claimNextPendingTask(): node={} no claim, another node holds the runner slot", nodeId);

            return null;
        }

        AtlasTask ret = takeOldestPendingTask();

        if (ret == null) {
            GraphClaim.releaseLeaseAndCommit(graph, Constants.CLAIM_TASK_RUNNER, nodeId);
        }

        return ret;
    }

    /**
     * Marks the oldest pending task as this node's, with the runner slot already held and committed.
     * The queue is read again here rather than reusing the candidate found before the claim: that
     * candidate came from a view taken before the claim was committed, and a peer may have finished
     * it in the meantime.
     */
    private AtlasTask takeOldestPendingTask() {
        AtlasVertex taskVertex = findOldestPendingVertex();

        if (taskVertex == null) {
            return null;
        }

        long now = System.currentTimeMillis();

        setEncodedProperty(taskVertex, Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS.toString());
        setEncodedProperty(taskVertex, Constants.TASK_START_TIME, now);
        setEncodedProperty(taskVertex, Constants.TASK_UPDATED_TIME, now);

        AtlasTask ret = toAtlasTask(taskVertex);

        LOG.info("TaskRegistry.claimNextPendingTask(): node={} claimed {} ({})", nodeId, ret.getGuid(), ret.getType());

        return ret;
    }

    /**
     * Returns {@code IN_PROGRESS} tasks whose owner has gone quiet for longer than the stale
     * threshold back to {@code PENDING}.  Without this a node that died mid-task would hold the
     * cluster-wide slot forever and no task would ever run again.
     */
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

            LOG.warn("TaskRegistry.recoverStaleInProgressTasks(): node={} recovering stale IN_PROGRESS task {} back to PENDING",
                    nodeId, taskGuid);

            // The runner slot the dead node held is not released here: it is leased for this same
            // threshold, so it has lapsed too and the next claimant takes it over.  This clears a
            // claim only if the task was marked by a node running a build that recorded claims on
            // task vertices, which would otherwise outlive the task and block every later claim.
            GraphClaim.releaseClaim(vertex);

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

    /**
     * Returns the oldest {@code PENDING} task vertex, or {@code null} if there is none.
     *
     * <p>The ordering is computed here rather than delegated to {@code orderBy()} on the graph
     * query: {@link Constants#TASK_CREATED_TIME} is written as a {@link Date} but indexed as a
     * {@code Long}, so the store-level sort cannot be relied upon to return the true oldest
     * vertex.  Every node must agree on which task is next, otherwise the claim stops being a
     * race that exactly one participant wins.
     *
     * <p>Each candidate's status is confirmed on the vertex itself, because the index that produced
     * the candidate can lag behind it: a task another node finished moments ago is still returned as
     * {@code PENDING} and would be run a second time.  Candidates are filtered as they are scanned
     * rather than after the oldest is chosen, so a lagging entry cannot hide the tasks behind it.
     */
    private AtlasVertex findOldestPendingVertex() {
        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                .has(Constants.TASK_STATUS, AtlasTask.Status.PENDING.toString());

        AtlasVertex ret            = null;
        long        oldestCreated  = Long.MAX_VALUE;
        String      oldestGuid     = null;

        for (AtlasVertex vertex : (Iterable<AtlasVertex>) query.vertices()) {
            if (!isPending(vertex)) {
                continue;
            }

            long   created = readCreatedTime(vertex);
            String guid    = vertex.getProperty(Constants.TASK_GUID, String.class);

            // GUID breaks ties so that concurrent claimers converge on the same vertex.
            if (created < oldestCreated || (created == oldestCreated && compareGuids(guid, oldestGuid) < 0)) {
                ret           = vertex;
                oldestCreated = created;
                oldestGuid    = guid;
            }
        }

        return ret;
    }

    /**
     * Whether the vertex itself still says {@code PENDING}.  A vertex the owning node has already
     * deleted reads as no status at all, which is equally not claimable.
     */
    private static boolean isPending(AtlasVertex vertex) {
        try {
            return AtlasTask.Status.PENDING.toString().equals(vertex.getProperty(Constants.TASK_STATUS, String.class));
        } catch (Exception exception) {
            LOG.debug("TaskRegistry: skipping a task vertex that could no longer be read", exception);

            return false;
        }
    }

    private static int compareGuids(String guid, String otherGuid) {
        if (guid == null) {
            return otherGuid == null ? 0 : 1;
        }

        return otherGuid == null ? -1 : guid.compareTo(otherGuid);
    }

    /**
     * Reads the task creation time, tolerating both representations found on task vertices:
     * {@code createVertex()} stores a {@link Date} while claim/update paths store epoch millis.
     * A vertex with no usable creation time sorts last so it can never wedge the queue.
     */
    private static long readCreatedTime(AtlasVertex vertex) {
        Object value = vertex.getProperty(Constants.TASK_CREATED_TIME, Object.class);

        if (value instanceof Date) {
            return ((Date) value).getTime();
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        return Long.MAX_VALUE;
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

        // Removing the vertex does not clear its uniqueness entries, so a claim left on it would
        // survive the task and no node could ever claim again.  Current claims live on the runner
        // slot rather than here; this covers tasks marked by an earlier build.
        GraphClaim.releaseClaim(taskVertex);

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
