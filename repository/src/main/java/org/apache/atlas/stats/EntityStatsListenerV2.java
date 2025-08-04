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
package org.apache.atlas.stats;

import org.apache.atlas.annotation.EnableConditional;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListenerV2;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.Constants;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@EnableConditional(property = "atlas.enable.entity.stats", isDefault = true)
@Order(8)
public class EntityStatsListenerV2 implements EntityChangeListenerV2 {

    private final StatsClient statsClient;

    @Inject
    public EntityStatsListenerV2(StatsClient statsClient) {
        this.statsClient = statsClient;
    }


    @Override
    public void onEntitiesAdded(List<AtlasEntity> entities, boolean isImport) throws AtlasBaseException {
        if (entities != null) {
            entities.forEach(e -> this.statsClient.increment(Constants.ENTITIES_ADDED_METRIC));
        }
    }

    @Override
    public void onEntitiesUpdated(List<AtlasEntity> entities, boolean isImport) throws AtlasBaseException {
        if (entities != null) {
            entities.forEach(e -> this.statsClient.increment(Constants.ENTITIES_UPDATED_METRIC));
        }
    }

    @Override
    public void onEntitiesDeleted(List<AtlasEntity> entities, boolean isImport) throws AtlasBaseException {
        if (entities != null) {
            entities.forEach(e -> this.statsClient.increment(Constants.ENTITIES_DELETED_METRIC));
        }
    }

    @Override
    public void onEntitiesPurged(List<AtlasEntity> entities) throws AtlasBaseException {
        if (entities != null) {
            entities.forEach(e -> this.statsClient.increment(Constants.ENTITIES_PURGED_METRIC));
        }
    }

    @Override
    public void onClassificationsAdded(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (classifications != null) {
            classifications.forEach(e -> this.statsClient.increment(Constants.CLASSIFICATIONS_ADDED_METRIC));
        }
    }

    @Override
    public void onClassificationsAdded(List<AtlasEntity> entities, List<AtlasClassification> classifications, boolean forceInline) throws AtlasBaseException {
        if (classifications != null) {
            classifications.forEach(e -> this.statsClient.increment(Constants.CLASSIFICATIONS_ADDED_METRIC));
        }
    }

    @Override
    public void onClassificationPropagationsAdded(List<AtlasEntity> entities, List<AtlasClassification> classifications, boolean forceInline) throws AtlasBaseException {
        if (classifications != null) {
            classifications.forEach(e -> this.statsClient.increment(Constants.CLASSIFICATIONS_ADDED_METRIC));
        }
    }

    @Override
    public void onClassificationsUpdated(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (classifications != null) {
            classifications.forEach(e -> this.statsClient.increment(Constants.CLASSIFICATIONS_UPDATED_METRIC));
        }
    }

    @Override
    public void onClassificationPropagationUpdated(AtlasEntity entity, List<AtlasClassification> classifications, boolean forceInline) throws AtlasBaseException {
        onClassificationsUpdated(entity, classifications);
    }

    @Override
    public void onClassificationsDeleted(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (classifications != null) {
            classifications.forEach(e -> this.statsClient.increment(Constants.CLASSIFICATIONS_DELETED_METRIC));
        }
    }

    @Override
    public void onClassificationsDeleted(List<AtlasEntity> entities, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (classifications != null) {
            classifications.forEach(e -> this.statsClient.increment(Constants.CLASSIFICATIONS_DELETED_METRIC));
        }
    }

    @Override
    public void onTermAdded(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entities) throws AtlasBaseException {
        if (entities != null) {
            entities.forEach(e -> this.statsClient.increment(Constants.TERMS_ADDED_METRIC));
        }

    }

    @Override
    public void onTermDeleted(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entities) throws AtlasBaseException {
        if (entities != null) {
            entities.forEach(e -> this.statsClient.increment(Constants.TERMS_DELETED_METRIC));
        }
    }

    @Override
    public void onRelationshipsAdded(List<AtlasRelationship> relationships, boolean isImport) throws AtlasBaseException {
        if (relationships != null) {
            relationships.forEach(e -> this.statsClient.increment(Constants.RELATIONSHIPS_ADDED_METRIC));
        }
    }

    @Override
    public void onRelationshipsUpdated(List<AtlasRelationship> relationships, boolean isImport) throws AtlasBaseException {
        if (relationships != null) {
            relationships.forEach(e -> this.statsClient.increment(Constants.RELATIONSHIPS_UPDATED_METRIC));
        }
    }

    @Override
    public void onRelationshipsDeleted(List<AtlasRelationship> relationships, boolean isImport) throws AtlasBaseException {
        if (relationships != null) {
            relationships.forEach(e -> this.statsClient.increment(Constants.RELATIONSHIPS_DELETED_METRIC));
        }
    }

    @Override
    public void onRelationshipsPurged(List<AtlasRelationship> relationships) throws AtlasBaseException {
        if (relationships != null) {
            relationships.forEach(e -> this.statsClient.increment(Constants.RELATIONSHIPS_PURGED_METRIC));
        }
    }

    @Override
    public void onLabelsAdded(AtlasEntity entity, Set<String> labels) throws AtlasBaseException {
        if (labels != null) {
            labels.forEach(e -> this.statsClient.increment(Constants.LABELS_ADDED_METRIC));
        }
    }

    @Override
    public void onLabelsDeleted(AtlasEntity entity, Set<String> labels) throws AtlasBaseException {
        if (labels != null) {
            labels.forEach(e -> this.statsClient.increment(Constants.LABELS_DELETED_METRIC));
        }
    }

    @Override
    public void onBusinessAttributesUpdated(AtlasEntity entity, Map<String, Map<String, Object>> updatedBusinessAttributes) throws AtlasBaseException {
        if (updatedBusinessAttributes != null) {
            updatedBusinessAttributes.keySet().forEach(e -> this.statsClient.increment(Constants.BA_UPDATED_METRIC));
        }
    }

    @Override
    public void onClassificationsDeletedV2(AtlasEntity entity, List<AtlasClassification> deletedClassifications, boolean forceInline) throws AtlasBaseException {
        onClassificationsDeleted(entity, deletedClassifications);
    }
}