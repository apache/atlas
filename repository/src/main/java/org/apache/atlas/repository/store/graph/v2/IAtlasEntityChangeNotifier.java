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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasStructType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IAtlasEntityChangeNotifier {
    void onEntitiesMutated(EntityMutationResponse entityMutationResponse, boolean isImport) throws AtlasBaseException;

    void notifyRelationshipMutation(List<AtlasRelationship> relationships, EntityNotification.EntityNotificationV2.OperationType operationType) throws AtlasBaseException;

    void onClassificationAddedToEntity(AtlasEntity entity, List<AtlasClassification> addedClassifications) throws AtlasBaseException;

    void onClassificationsAddedToEntities(List<AtlasEntity> entities, List<AtlasClassification> addedClassifications, boolean forceInline) throws AtlasBaseException;

    void onClassificationsAddedToEntitiesV2(Set<AtlasVertex> vertices, List<AtlasClassification> addedClassifications, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;

    void onClassificationPropagationAddedToEntities(List<AtlasEntity> entities, List<AtlasClassification> addedClassifications, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;

    void onClassificationPropagationAddedToEntitiesV2(Set<AtlasVertex> vertices, List<AtlasClassification> addedClassifications, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;

    void onClassificationUpdatedToEntitiesV2(List<AtlasEntity> entities, AtlasClassification updatedClassification, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;

    void onClassificationUpdatedToEntitiesV2(Set<AtlasVertex> vertices, AtlasClassification updatedClassification, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;

    void onClassificationDeletedFromEntity(AtlasEntity entity, List<AtlasClassification> deletedClassifications) throws AtlasBaseException;

    void onClassificationsDeletedFromEntities(List<AtlasEntity> entities, List<AtlasClassification> deletedClassifications) throws AtlasBaseException;
    void onClassificationDeletedFromEntities(List<AtlasEntity> entities, AtlasClassification deletedClassification) throws AtlasBaseException;

    void onClassificationDeletedFromEntitiesV2(Set<AtlasVertex> vertices, AtlasClassification deletedClassification) throws AtlasBaseException;

    void onClassificationPropagationDeleted(List<AtlasEntity> entities, List<AtlasClassification> deletedClassifications, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;

    void onClassificationPropagationDeletedV2(Set<AtlasVertex> vertices, AtlasClassification deletedClassification, boolean forceInline, RequestContext requestContext) throws AtlasBaseException;
    
    void onClassificationPropagationDeleted(AtlasEntity entity, List<AtlasClassification> deletedClassifications, boolean forceInline) throws AtlasBaseException;

    void onTermAddedToEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException;

    void onTermDeletedFromEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException;

    void onLabelsUpdatedFromEntity(String entityGuid, Set<String> addedLabels, Set<String> deletedLabels) throws AtlasBaseException;

    void notifyPropagatedEntities() throws AtlasBaseException;

    void onClassificationUpdatedToEntity(AtlasEntity entity, List<AtlasClassification> updatedClassifications) throws AtlasBaseException;

    void onClassificationUpdatedToEntity(AtlasEntity entity, List<AtlasClassification> updatedClassifications, boolean forceInline) throws AtlasBaseException;

    void onClassificationUpdatedToEntities(List<AtlasEntity> entities, AtlasClassification updatedClassification) throws AtlasBaseException;

    void onBusinessAttributesUpdated(String entityGuid, Map<String, Map<String, Object>> updatedBusinessAttributes) throws AtlasBaseException;

    void notifyDifferentialEntityChanges(EntityMutationResponse entityMutationResponse, boolean isImport) throws AtlasBaseException;
}
