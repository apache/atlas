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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class EntityChangeNotifierNopTest {
    private EntityChangeNotifierNop entityChangeNotifierNop;

    @BeforeMethod
    public void setUp() {
        entityChangeNotifierNop = new EntityChangeNotifierNop();
    }

    @Test
    public void testOnEntitiesMutated() {
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);

        // Test with isImport true
        entityChangeNotifierNop.onEntitiesMutated(mockResponse, true);

        // Test with isImport false
        entityChangeNotifierNop.onEntitiesMutated(mockResponse, false);

        // Test with null response
        entityChangeNotifierNop.onEntitiesMutated(null, true);
    }

    @Test
    public void testNotifyRelationshipMutation() {
        AtlasRelationship mockRelationship = mock(AtlasRelationship.class);
        EntityNotification.EntityNotificationV2.OperationType operationType = EntityNotification.EntityNotificationV2.OperationType.ENTITY_CREATE;

        // Test with CREATE operation
        entityChangeNotifierNop.notifyRelationshipMutation(mockRelationship, operationType);

        // Test with UPDATE operation
        entityChangeNotifierNop.notifyRelationshipMutation(mockRelationship,
                EntityNotification.EntityNotificationV2.OperationType.ENTITY_UPDATE);

        // Test with DELETE operation
        entityChangeNotifierNop.notifyRelationshipMutation(mockRelationship,
                EntityNotification.EntityNotificationV2.OperationType.ENTITY_DELETE);

        // Test with null values
        entityChangeNotifierNop.notifyRelationshipMutation(null, operationType);
        entityChangeNotifierNop.notifyRelationshipMutation(mockRelationship, null);
    }

    @Test
    public void testOnClassificationAddedToEntity() {
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        List<AtlasClassification> classifications = Arrays.asList(mock(AtlasClassification.class), mock(AtlasClassification.class));

        // Test with valid parameters
        entityChangeNotifierNop.onClassificationAddedToEntity(mockEntity, classifications);

        // Test with null entity
        entityChangeNotifierNop.onClassificationAddedToEntity(null, classifications);

        // Test with null classifications
        entityChangeNotifierNop.onClassificationAddedToEntity(mockEntity, null);

        // Test with empty classifications
        entityChangeNotifierNop.onClassificationAddedToEntity(mockEntity, Arrays.asList());
    }

    @Test
    public void testOnClassificationsAddedToEntities() {
        List<AtlasEntity> entities = Arrays.asList(mock(AtlasEntity.class), mock(AtlasEntity.class));
        List<AtlasClassification> classifications = Arrays.asList(mock(AtlasClassification.class));

        // Test with valid parameters
        entityChangeNotifierNop.onClassificationsAddedToEntities(entities, classifications);

        // Test with null entities
        entityChangeNotifierNop.onClassificationsAddedToEntities(null, classifications);

        // Test with null classifications
        entityChangeNotifierNop.onClassificationsAddedToEntities(entities, null);
    }

    @Test
    public void testOnClassificationDeletedFromEntity() {
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        List<AtlasClassification> classifications = Arrays.asList(mock(AtlasClassification.class));

        // Test with valid parameters
        entityChangeNotifierNop.onClassificationDeletedFromEntity(mockEntity, classifications);

        // Test with null entity
        entityChangeNotifierNop.onClassificationDeletedFromEntity(null, classifications);

        // Test with null classifications
        entityChangeNotifierNop.onClassificationDeletedFromEntity(mockEntity, null);
    }

    @Test
    public void testOnClassificationsDeletedFromEntities() {
        List<AtlasEntity> entities = Arrays.asList(mock(AtlasEntity.class));
        List<AtlasClassification> classifications = Arrays.asList(mock(AtlasClassification.class));

        // Test with valid parameters
        entityChangeNotifierNop.onClassificationsDeletedFromEntities(entities, classifications);

        // Test with null parameters
        entityChangeNotifierNop.onClassificationsDeletedFromEntities(null, null);
    }

    @Test
    public void testOnTermAddedToEntities() {
        AtlasGlossaryTerm mockTerm = mock(AtlasGlossaryTerm.class);
        List<AtlasRelatedObjectId> entityIds = Arrays.asList(mock(AtlasRelatedObjectId.class), mock(AtlasRelatedObjectId.class));

        // Test with valid parameters
        entityChangeNotifierNop.onTermAddedToEntities(mockTerm, entityIds);

        // Test with null term
        entityChangeNotifierNop.onTermAddedToEntities(null, entityIds);

        // Test with null entity IDs
        entityChangeNotifierNop.onTermAddedToEntities(mockTerm, null);
    }

    @Test
    public void testOnTermDeletedFromEntities() {
        AtlasGlossaryTerm mockTerm = mock(AtlasGlossaryTerm.class);
        List<AtlasRelatedObjectId> entityIds = Arrays.asList(mock(AtlasRelatedObjectId.class));

        // Test with valid parameters
        entityChangeNotifierNop.onTermDeletedFromEntities(mockTerm, entityIds);

        // Test with null parameters
        entityChangeNotifierNop.onTermDeletedFromEntities(null, null);
    }

    @Test
    public void testOnLabelsUpdatedFromEntity() {
        String entityGuid = "test-guid-123";
        Set<String> addedLabels = new HashSet<>(Arrays.asList("label1", "label2"));
        Set<String> deletedLabels = new HashSet<>(Arrays.asList("label3"));

        // Test with valid parameters
        entityChangeNotifierNop.onLabelsUpdatedFromEntity(entityGuid, addedLabels, deletedLabels);

        // Test with null guid
        entityChangeNotifierNop.onLabelsUpdatedFromEntity(null, addedLabels, deletedLabels);

        // Test with empty guid
        entityChangeNotifierNop.onLabelsUpdatedFromEntity("", addedLabels, deletedLabels);

        // Test with null labels
        entityChangeNotifierNop.onLabelsUpdatedFromEntity(entityGuid, null, null);

        // Test with empty labels
        entityChangeNotifierNop.onLabelsUpdatedFromEntity(entityGuid, new HashSet<>(), new HashSet<>());
    }

    @Test
    public void testNotifyPropagatedEntities() {
        // Test multiple calls to ensure no side effects
        entityChangeNotifierNop.notifyPropagatedEntities();
        entityChangeNotifierNop.notifyPropagatedEntities();
        entityChangeNotifierNop.notifyPropagatedEntities();
    }

    @Test
    public void testOnClassificationUpdatedToEntity() {
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        List<AtlasClassification> updatedClassifications = Arrays.asList(mock(AtlasClassification.class), mock(AtlasClassification.class));

        // Test with valid parameters
        entityChangeNotifierNop.onClassificationUpdatedToEntity(mockEntity, updatedClassifications);

        // Test with null entity
        entityChangeNotifierNop.onClassificationUpdatedToEntity(null, updatedClassifications);

        // Test with null classifications
        entityChangeNotifierNop.onClassificationUpdatedToEntity(mockEntity, null);
    }

    @Test
    public void testOnBusinessAttributesUpdated() {
        String entityGuid = "business-attr-guid-123";
        Map<String, Map<String, Object>> updatedBusinessAttributes = new HashMap<>();
        Map<String, Object> businessAttrValues = new HashMap<>();
        businessAttrValues.put("attr1", "value1");
        businessAttrValues.put("attr2", 123);
        businessAttrValues.put("attr3", true);
        updatedBusinessAttributes.put("category1", businessAttrValues);

        // Test with valid parameters
        entityChangeNotifierNop.onBusinessAttributesUpdated(entityGuid, updatedBusinessAttributes);

        // Test with null guid
        entityChangeNotifierNop.onBusinessAttributesUpdated(null, updatedBusinessAttributes);

        // Test with empty guid
        entityChangeNotifierNop.onBusinessAttributesUpdated("", updatedBusinessAttributes);

        // Test with null attributes
        entityChangeNotifierNop.onBusinessAttributesUpdated(entityGuid, null);

        // Test with empty attributes
        entityChangeNotifierNop.onBusinessAttributesUpdated(entityGuid, new HashMap<>());
    }
}
