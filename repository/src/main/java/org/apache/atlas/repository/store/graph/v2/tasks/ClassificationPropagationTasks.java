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
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasType;

import java.util.Map;
import java.util.Set;

public class ClassificationPropagationTasks {
    public static class Add extends ClassificationTask {
        public Add(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String entityGuid               = (String) parameters.get(PARAM_ENTITY_GUID);
            String classificationVertexId   = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
            String relationshipGuid         = (String) parameters.get(PARAM_RELATIONSHIP_GUID);
            Boolean previousRestrictPropagationThroughLineage = (Boolean) parameters.get(PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_LINEAGE);
            Boolean previousRestrictPropagationThroughHierarchy = (Boolean) parameters.get(PARAM_PREVIOUS_CLASSIFICATION_RESTRICT_PROPAGATE_THROUGH_HIERARCHY);
            entityGraphMapper.propagateClassification(entityGuid, classificationVertexId, relationshipGuid, previousRestrictPropagationThroughLineage,previousRestrictPropagationThroughHierarchy);
        }
    }

    public static class Delete extends ClassificationTask {
        public Delete(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String entityGuid             = (String) parameters.get(PARAM_ENTITY_GUID);
            String classificationVertexId = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);

            entityGraphMapper.deleteClassificationPropagation(entityGuid, classificationVertexId);
        }
    }

    // TODO: Will be deprecated
    public static class DeleteOnlyPropagations extends ClassificationTask {
        public DeleteOnlyPropagations(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            if (parameters.get(PARAM_DELETED_EDGE_IDS) != null) {
                Set<String> deletedEdgeIds    =  AtlasType.fromJson((String) parameters.get(PARAM_DELETED_EDGE_IDS), Set.class);
                entityGraphMapper.deleteClassificationOnlyPropagation(deletedEdgeIds);
            } else {
                String deletedEdgeId          =  (String) parameters.get(PARAM_DELETED_EDGE_ID);
                String classificationVertexId =  (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
                entityGraphMapper.deleteClassificationOnlyPropagation(deletedEdgeId, classificationVertexId);
            }
        }
    }

    // TODO: Will be deprecated
    public static class DeleteOnlyPropagationsOnHardDelete extends ClassificationTask {
        public DeleteOnlyPropagationsOnHardDelete(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String classificationVertexId =  (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);
            String referencedVertexId = (String) parameters.get(PARAM_REFERENCED_VERTEX_ID);
            boolean isTermEntityEdge = (boolean) parameters.get(PARAM_IS_TERM_ENTITY_EDGE);

            entityGraphMapper.deleteClassificationOnlyPropagation(classificationVertexId, referencedVertexId, isTermEntityEdge);
        }
    }

    public static class RefreshPropagation extends ClassificationTask {
        public RefreshPropagation(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String            classificationVertexId = (String) parameters.get(PARAM_CLASSIFICATION_VERTEX_ID);

            entityGraphMapper.classificationRefreshPropagation(classificationVertexId);
        }
    }

    public static class UpdateRelationship extends ClassificationTask {
        public UpdateRelationship(AtlasTask task, AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore) {
            super(task, graph, entityGraphMapper, deleteDelegate, relationshipStore);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String            relationshipEdgeId = (String) parameters.get(PARAM_RELATIONSHIP_EDGE_ID);
            AtlasRelationship relationship       = AtlasType.fromJson((String) parameters.get(PARAM_RELATIONSHIP_OBJECT), AtlasRelationship.class);

            entityGraphMapper.updateTagPropagations(relationshipEdgeId, relationship);
        }
    }
}
