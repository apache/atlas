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
package org.apache.atlas.repository.store.graph.v2.bulkimport;

import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.repository.store.graph.v2.IAtlasEntityChangeNotifier;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityChangeNotifierNop implements IAtlasEntityChangeNotifier {
    @Override
    public void onEntitiesMutated(EntityMutationResponse entityMutationResponse, boolean isImport) {
    }

    @Override
    public void notifyRelationshipMutation(AtlasRelationship relationship, EntityNotification.EntityNotificationV2.OperationType operationType) {
    }

    @Override
    public void onClassificationAddedToEntity(AtlasEntity entity, List<AtlasClassification> addedClassifications) {
    }

    @Override
    public void onClassificationsAddedToEntities(List<AtlasEntity> entities, List<AtlasClassification> addedClassifications) {
    }

    @Override
    public void onClassificationDeletedFromEntity(AtlasEntity entity, List<AtlasClassification> deletedClassifications) {
    }

    @Override
    public void onClassificationsDeletedFromEntities(List<AtlasEntity> entities, List<AtlasClassification> deletedClassifications) {
    }

    @Override
    public void onTermAddedToEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) {
    }

    @Override
    public void onTermDeletedFromEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) {
    }

    @Override
    public void onLabelsUpdatedFromEntity(String entityGuid, Set<String> addedLabels, Set<String> deletedLabels) {
    }

    @Override
    public void notifyPropagatedEntities() {
    }

    @Override
    public void onClassificationUpdatedToEntity(AtlasEntity entity, List<AtlasClassification> updatedClassifications) {
    }

    @Override
    public void onBusinessAttributesUpdated(String entityGuid, Map<String, Map<String, Object>> updatedBusinessAttributes) {
    }
}
