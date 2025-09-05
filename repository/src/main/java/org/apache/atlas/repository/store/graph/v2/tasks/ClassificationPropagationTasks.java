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
package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.metrics.TaskMetricsService;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ClassificationPropagationTasks {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationPropagationTasks.class);

    public static class Add extends ClassificationTask {
        public Add(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            String entityGuid = (String) parameters.get(PARAM_ENTITY_GUID);
            String toEntityGuid = (String) parameters.get(PARAM_TO_ENTITY_GUID);
            String tagTypeName = getTaskDef().getTagTypeName();
            String parentEntityGuid = getTaskDef().getParentEntityGuid();
            Boolean previousRestrictPropagationThroughLineage = (Boolean) parameters.get(PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_LINEAGE);
            Boolean previousRestrictPropagationThroughHierarchy = (Boolean) parameters.get(PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_HIERARCHY);

            if (org.apache.atlas.service.FeatureFlagStore.isTagV2Enabled()) {
                LOG.info("Using v2 tag flow (Cassandra) for Add propagation task");
                int assetsAffected = entityGraphMapper.propagateClassificationV2_Optimised(parameters, entityGuid, tagTypeName, parentEntityGuid, toEntityGuid);
                context.incrementAssetsAffected(assetsAffected);
            } else {
                LOG.info("Using v1 tag flow (JanusGraph) for Add propagation task");
                String classificationVertexId = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
                String relationshipGuid = (String) parameters.get(PARAM_RELATIONSHIP_GUID);
                int assetsAffected = entityGraphMapper.propagateClassification(entityGuid, classificationVertexId, relationshipGuid, previousRestrictPropagationThroughLineage, previousRestrictPropagationThroughHierarchy);
                context.incrementAssetsAffected(assetsAffected);
            }
        }
    }

    public static class UpdateText extends ClassificationTask {
        public UpdateText(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            String tagTypeName = getTaskDef().getTagTypeName();
            String entityGuid = (String) parameters.get(PARAM_ENTITY_GUID);

            if (org.apache.atlas.service.FeatureFlagStore.isTagV2Enabled()) {
                LOG.info("Using v2 tag flow (Cassandra) for UpdateText propagation task");
                int totalUpdated = entityGraphMapper.updateClassificationTextPropagationV2(entityGuid, tagTypeName);
                context.incrementAssetsAffected(totalUpdated);
            } else {
                LOG.info("Using v1 tag flow (JanusGraph) for UpdateText propagation task");
                String classificationVertexId = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
                int totalUpdated = entityGraphMapper.updateClassificationTextPropagation(classificationVertexId);
                context.incrementAssetsAffected(totalUpdated);
            }
        }
    }

    public static class Delete extends ClassificationTask {
        public Delete(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            String entityGuid = (String) parameters.get(PARAM_ENTITY_GUID);
            String sourceVertexId = (String) parameters.get(PARAM_SOURCE_VERTEX_ID);
            String tagTypeName = getTaskDef().getTagTypeName();
            String parentEntityGuid = getTaskDef().getParentEntityGuid();

            if (org.apache.atlas.service.FeatureFlagStore.isTagV2Enabled()) {
                LOG.info("Using v2 tag flow (Cassandra) for Delete propagation task");
                int totalDeleted = entityGraphMapper.deleteClassificationPropagationV2(entityGuid, sourceVertexId, parentEntityGuid, tagTypeName);
                context.incrementAssetsAffected(totalDeleted);
            } else {
                LOG.info("Using v1 tag flow (JanusGraph) for Delete propagation task");
                String classificationVertexId = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
                int totalDeleted = entityGraphMapper.deleteClassificationPropagation(entityGuid, classificationVertexId).size();
                context.incrementAssetsAffected(totalDeleted);
            }
        }
    }

    public static class RefreshPropagation extends ClassificationTask {
        public RefreshPropagation(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            String classificationTypeName = getTaskDef().getTagTypeName();
            String sourceEntity = getTaskDef().getEntityGuid();
            String parentEntityGuid = getTaskDef().getParentEntityGuid();

            if (org.apache.atlas.service.FeatureFlagStore.isTagV2Enabled()) {
                LOG.info("Using v2 tag flow (Cassandra) for RefreshPropagation task");
                int affected = entityGraphMapper.classificationRefreshPropagationV2_new(parameters, parentEntityGuid, sourceEntity, classificationTypeName);
                context.incrementAssetsAffected(affected);
            } else {
                LOG.info("Using v1 tag flow (JanusGraph) for RefreshPropagation task");
                String classificationVertexId = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
                int affected = entityGraphMapper.classificationRefreshPropagation(classificationVertexId);
                context.incrementAssetsAffected(affected);
            }
        }
    }

    public static class UpdateRelationship extends ClassificationTask {
        public UpdateRelationship(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            String relationshipEdgeId = (String) parameters.get(PARAM_RELATIONSHIP_EDGE_ID);
            AtlasRelationship relationship = AtlasType.fromJson((String) parameters.get(PARAM_RELATIONSHIP_OBJECT), AtlasRelationship.class);

            entityGraphMapper.updateTagPropagations(relationshipEdgeId, relationship);
        }
    }

    public static class CleanUpClassificationPropagation extends ClassificationTask {
        public CleanUpClassificationPropagation(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);
        }

        @Override
        protected void run(Map<String, Object> parameters, TaskContext context) throws AtlasBaseException {
            String classificationName = (String) parameters.get(PARAM_CLASSIFICATION_NAME);
            int batchLimit = -1;
            if(parameters.containsKey(PARAM_BATCH_LIMIT)) {
                batchLimit = (int) parameters.get(PARAM_BATCH_LIMIT);
            }

            int affected = entityGraphMapper.cleanUpClassificationPropagation(classificationName, batchLimit);
            context.incrementAssetsAffected(affected);
        }
    }
}