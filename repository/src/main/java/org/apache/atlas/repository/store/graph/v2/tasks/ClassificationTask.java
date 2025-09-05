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
package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.apache.atlas.repository.metrics.TaskMetricsService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE;

public abstract class ClassificationTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationTask.class);

    public static final String PARAM_ENTITY_GUID              = "entityGuid";
    public static final String PARAM_SOURCE_VERTEX_ID         = "sourceVertexId";
    public static final String PARAM_TO_ENTITY_GUID           = "toEntityGuid";
    public static final String PARAM_DELETED_EDGE_IDS         = "deletedEdgeIds"; // TODO: Will be deprecated
    public static final String PARAM_DELETED_EDGE_ID          = "deletedEdgeId";
    public static final String PARAM_CLASSIFICATION_VERTEX_ID = "classificationVertexId";
    public static final String PARAM_RELATIONSHIP_GUID        = "relationshipGuid";
    public static final String PARAM_RELATIONSHIP_OBJECT      = "relationshipObject";
    public static final String PARAM_RELATIONSHIP_EDGE_ID     = "relationshipEdgeId";

    public static final String PARAM_CLASSIFICATION_NAME      = "classificationName";
    public static final String PARAM_BATCH_LIMIT      = "batchLimit";
    public static final String PARAM_REFERENCED_VERTEX_ID     = "referencedVertexId";
    public static final String PARAM_IS_TERM_ENTITY_EDGE       = "isTermEntityEdge";
    public static final String PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_LINEAGE = "previousRestrictPropagationThroughLineage";

    public static final String PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_HIERARCHY = "previousRestrictPropagationThroughHierarchy";
  
    protected final AtlasGraph             graph;
    protected final EntityGraphMapper      entityGraphMapper;
    protected final DeleteHandlerDelegate  deleteDelegate;
    protected final AtlasRelationshipStore relationshipStore;
    protected final TaskMetricsService taskMetricsService;


    public ClassificationTask(AtlasTask task,
                              AtlasGraph graph,
                              EntityGraphMapper entityGraphMapper,
                              DeleteHandlerDelegate deleteDelegate,
                              AtlasRelationshipStore relationshipStore,
                              TaskMetricsService taskMetricsService) {
        super(task);
        this.graph             = graph;
        this.entityGraphMapper = entityGraphMapper;
        this.deleteDelegate    = deleteDelegate;
        this.relationshipStore = relationshipStore;
        this.taskMetricsService = taskMetricsService;
    }

    @Override
    public AtlasTask.Status perform() throws AtlasBaseException {
        String threadName = Thread.currentThread().getName();
        Map<String, Object> params = getTaskDef().getParameters();
        TaskContext context = new TaskContext();
        long startTime = System.currentTimeMillis();
        String taskType = getTaskType();
        String version = org.apache.atlas.service.FeatureFlagStore.isTagV2Enabled() ? "v2" : "v1";
        String tenant = System.getenv("DOMAIN_NAME");

        if (MapUtils.isEmpty(params)) {
            LOG.warn("Task: {}: Unable to process task: Parameters is not readable!", getTaskGuid());
            taskMetricsService.recordTaskError(taskType, version, tenant, "MISSING_PARAMS");
            return FAILED;
        }

        String userName = getTaskDef().getCreatedBy();

        if (StringUtils.isEmpty(userName)) {
            LOG.warn("Task: {}: Unable to process task as user name is empty!", getTaskGuid());
            taskMetricsService.recordTaskError(taskType, version, tenant, "MISSING_USER");
            return FAILED;
        }

        RequestContext.get().setUser(userName, null);

        try {
            // Record task start
            taskMetricsService.recordTaskStart(taskType, version, tenant);

            // Set up MDC context first
            MDC.put("task_id", getTaskGuid());
            MDC.put("task_type", taskType);
            MDC.put("tag_name", getTaskDef().getTagTypeName());
            MDC.put("entity_guid", getTaskDef().getEntityGuid());
            MDC.put("tag_version", version);
            MDC.put("thread_name", threadName);
            MDC.put("thread_id", String.valueOf(Thread.currentThread().getId()));
            MDC.put("status", "in_progress");
            MDC.put("assets_affected", "0");

            LOG.info("Starting classification task execution");
            setStatus(IN_PROGRESS);
            run(params, context);
            setStatus(AtlasTask.Status.COMPLETE);
            int assetsAffected = context.getAssetsAffected();
            MDC.put("assets_affected", String.valueOf(assetsAffected));
            MDC.put("status", "success");

            // Record successful completion
            taskMetricsService.recordTaskEnd(
                taskType, 
                version,
                tenant,
                System.currentTimeMillis() - startTime,
                assetsAffected,
                true
            );

            return AtlasTask.Status.COMPLETE;
        } catch (AtlasBaseException e) {
            MDC.put("assets_affected", "0");
            MDC.put("status", "failed");
            MDC.put("error", e.getMessage());
            LOG.error("Classification task failed", e);
            setStatus(AtlasTask.Status.FAILED);

            // Record failure
            taskMetricsService.recordTaskEnd(
                taskType,
                version,
                tenant,
                System.currentTimeMillis() - startTime,
                0,
                false
            );
            taskMetricsService.recordTaskError(taskType, version, tenant, e.getClass().getSimpleName());

            throw e;
        } catch (Throwable t) {
            MDC.put("assets_affected", "0");
            MDC.put("status", "failed");
            MDC.put("error", t.getMessage());
            LOG.error("Unexpected error in classification task", t);
            setStatus(AtlasTask.Status.FAILED);

            // Record failure
            taskMetricsService.recordTaskEnd(
                taskType,
                version,
                tenant,
                System.currentTimeMillis() - startTime,
                0,
                false
            );
            taskMetricsService.recordTaskError(taskType, version, tenant, t.getClass().getSimpleName());

            throw new AtlasBaseException(t);
        } finally {
            // failsafe invocation to make sure endTime is set
            getTask().end();
            // Log final state after task.end() has been called by AbstractTask
            MDC.put("startTime", String.valueOf(getTaskDef().getStartTime()));
            MDC.put("endTime", String.valueOf(getTaskDef().getEndTime()));
            if (getTaskDef().getStartTime() != null && getTaskDef().getEndTime() != null) {
                long duration = getTaskDef().getEndTime().getTime() - getTaskDef().getStartTime().getTime();
                MDC.put("duration_ms", String.valueOf(duration));
            }

            // Log with all MDC values before clearing
            LOG.info("Classification task completed. Assets affected: {}, Duration: {} ms",
                MDC.get("assets_affected"),
                MDC.get("duration_ms"));

            MDC.clear();  // Clear MDC at the end
        }
    }

    public static Map<String, Object> toParameters(String entityGuid, String classificationVertexId, String relationshipGuid, Boolean restrictPropagationThroughLineage,Boolean restrictPropagationThroughHierarchy) {
        return new HashMap<>() {{
            put(PARAM_ENTITY_GUID, entityGuid);
            put(PARAM_CLASSIFICATION_VERTEX_ID, classificationVertexId);
            put(PARAM_RELATIONSHIP_GUID, relationshipGuid);
            put(PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_LINEAGE, restrictPropagationThroughLineage);
            put(PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_HIERARCHY, restrictPropagationThroughHierarchy);
        }};
    }

    public static Map<String, Object> toParameters(String entityGuid, String classificationVertexId, String relationshipGuid) {
        return new HashMap<>() {{
            put(PARAM_ENTITY_GUID, entityGuid);
            put(PARAM_CLASSIFICATION_VERTEX_ID, classificationVertexId);
            put(PARAM_RELATIONSHIP_GUID, relationshipGuid);
        }};
    }

    public static Map<String, Object> toParameters(String relationshipEdgeId, AtlasRelationship relationship) {
        return new HashMap<>() {{
            put(PARAM_RELATIONSHIP_EDGE_ID, relationshipEdgeId);
            put(PARAM_RELATIONSHIP_OBJECT, AtlasType.toJson(relationship));
        }};
    }

    public static Map<String, Object> toParameters(String classificationId) {
        return new HashMap<>() {{
            put(PARAM_CLASSIFICATION_VERTEX_ID, classificationId);
        }};
    }

    protected void setStatus(AtlasTask.Status status) {
        super.setStatus(status);
        LOG.info(String.format("ClassificationTask status is set %s for the task: %s ", status, super.getTaskGuid()));

        try {
            if (CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE.equals(getTaskType())) {
                entityGraphMapper.removePendingTaskFromEdge((String) getTaskDef().getParameters().get(PARAM_RELATIONSHIP_EDGE_ID), getTaskGuid());
            } else {
                entityGraphMapper.removePendingTaskFromEntity((String) getTaskDef().getParameters().get(PARAM_ENTITY_GUID), getTaskGuid());
            }
        } catch (EntityNotFoundException | AtlasBaseException e) {
            LOG.warn("Error updating associated element for: {}", getTaskGuid(), e);
        }
        graph.commit();
    }

    protected abstract void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException;

    public static class TaskContext {
        private int assetsAffected = 0;

        public void incrementAssetsAffected(int count) {
            this.assetsAffected += count;
        }

        public int getAssetsAffected() {
            return assetsAffected;
        }
    }
}
