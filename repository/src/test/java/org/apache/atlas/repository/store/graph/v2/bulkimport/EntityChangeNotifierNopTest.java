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

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.EntityChangeListenerV2;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.FullTextMapperV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityChangeNotifier;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class EntityChangeNotifierNopTest {
    private EntityChangeNotifierNop entityChangeNotifierNop;

    private AtlasEntityChangeNotifier atlasEntityChangeNotifier;
    private EntityChangeListenerV2 mockListener;
    private AtlasInstanceConverter mockInstanceConverter;
    private FullTextMapperV2 mockFullTextMapper;
    private AtlasTypeRegistry mockTypeRegistry;

    @BeforeMethod
    public void setUp() {
        entityChangeNotifierNop = new EntityChangeNotifierNop();
    }

    @BeforeMethod
    public void setUpAtlasEntityChangeNotifier() {
        RequestContext.clear();
        RequestContext.get();

        mockListener = mock(EntityChangeListenerV2.class);
        mockInstanceConverter = mock(AtlasInstanceConverter.class);
        mockFullTextMapper = mock(FullTextMapperV2.class);
        mockTypeRegistry = mock(AtlasTypeRegistry.class);

        // Need to provide both V1 and V2 listeners to pass the isEmpty check
        // The actual test is focused on V2 listeners
        Set<EntityChangeListenerV2> listenersV2 = new HashSet<>();
        listenersV2.add(mockListener);

        // Create a mock V1 listener to pass the isEmpty check
        EntityChangeListener mockV1Listener = mock(EntityChangeListener.class);
        Set<EntityChangeListener> listenersV1 = new HashSet<>();
        listenersV1.add(mockV1Listener);

        atlasEntityChangeNotifier = new AtlasEntityChangeNotifier(
                listenersV1,
                listenersV2,
                mockInstanceConverter,
                mockFullTextMapper,
                mockTypeRegistry);
    }

    @AfterMethod
    public void tearDown() {
        RequestContext.clear();
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

    /**
     * Scenario 1: Single normal hive_table entity
     * Expected: Notification should be sent
     */
    @Test
    public void testNotifyListeners_SingleNormalEntity() throws AtlasBaseException {
        // Create a single hive_table entity header
        AtlasEntityHeader hiveTableHeader = new AtlasEntityHeader();
        hiveTableHeader.setTypeName("hive_table");
        hiveTableHeader.setGuid("hive-table-guid-001");
        hiveTableHeader.setDisplayText("audit_test_table1");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "audit_test_table1");
        attributes.put("qualifiedName", "audit_test_db.audit_test_table1@cm");
        attributes.put("owner", "hive");
        hiveTableHeader.setAttributes(attributes);

        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();
        mutationResponse.addEntity(EntityOperation.CREATE, hiveTableHeader);

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should be called with the entity (filtering should allow normal entities through)
        verify(mockListener, times(1)).onEntitiesAdded(anyList(), anyBoolean());
    }

    /**
     * Scenario 2: Single internal type entity (__AtlasAuditEntry)
     * Expected: Notification should NOT be sent
     */
    @Test
    public void testNotifyListeners_SingleInternalEntity_AuditEntry() throws AtlasBaseException {
        // Create a single __AtlasAuditEntry entity header
        AtlasEntityHeader auditEntryHeader = new AtlasEntityHeader();
        auditEntryHeader.setTypeName("__AtlasAuditEntry");
        auditEntryHeader.setGuid("audit-entry-guid-001");
        auditEntryHeader.setDisplayText("__audit_entry_001");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("qualifiedName", "__audit_entry_001");
        attributes.put("entityId", "dummy-entity-id");
        attributes.put("entityType", "hive_table");
        attributes.put("operation", "OTHERS");
        auditEntryHeader.setAttributes(attributes);

        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();
        mutationResponse.addEntity(EntityOperation.CREATE, auditEntryHeader);

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should NOT be called (internal entity filtered out)
        verify(mockListener, never()).onEntitiesAdded(anyList(), anyBoolean());
    }

    /**
     * Scenario 2b: Single internal type entity (__AtlasUserProfile)
     * Expected: Notification should NOT be sent
     */
    @Test
    public void testNotifyListeners_SingleInternalEntity_UserProfile() throws AtlasBaseException {
        // Create a single __AtlasUserProfile entity header
        AtlasEntityHeader userProfileHeader = new AtlasEntityHeader();
        userProfileHeader.setTypeName("__AtlasUserProfile");
        userProfileHeader.setGuid("user-profile-guid-001");
        userProfileHeader.setDisplayText("__user_profile_001");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("qualifiedName", "__user_profile_001");
        attributes.put("name", "testuser");
        attributes.put("fullName", "Test User");
        userProfileHeader.setAttributes(attributes);

        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();
        mutationResponse.addEntity(EntityOperation.CREATE, userProfileHeader);

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should NOT be called (internal entity filtered out)
        verify(mockListener, never()).onEntitiesAdded(anyList(), anyBoolean());
    }

    /**
     * Scenario 2c: Single internal type entity (__AtlasMetricsStat)
     * Expected: Notification should NOT be sent
     */
    @Test
    public void testNotifyListeners_SingleInternalEntity_MetricsStat() throws AtlasBaseException {
        // Create a single __AtlasMetricsStat entity header
        AtlasEntityHeader metricsStatHeader = new AtlasEntityHeader();
        metricsStatHeader.setTypeName("__AtlasMetricsStat");
        metricsStatHeader.setGuid("metrics-stat-guid-001");
        metricsStatHeader.setDisplayText("__metrics_stat_001");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("qualifiedName", "__metrics_stat_001");
        attributes.put("metricsId", "metric_001");
        attributes.put("metricName", "dummyMetric");
        attributes.put("metricValue", 100);
        metricsStatHeader.setAttributes(attributes);

        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();
        mutationResponse.addEntity(EntityOperation.CREATE, metricsStatHeader);

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should NOT be called (internal entity filtered out)
        verify(mockListener, never()).onEntitiesAdded(anyList(), anyBoolean());
    }

    /**
     * Scenario 3: Bulk normal hive_table entities
     * Expected: Notification should be sent for all
     */
    @Test
    public void testNotifyListeners_BulkNormalEntities() throws AtlasBaseException {
        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();

        // Create multiple hive_table entity headers
        for (int i = 1; i <= 3; i++) {
            AtlasEntityHeader hiveTableHeader = new AtlasEntityHeader();
            hiveTableHeader.setTypeName("hive_table");
            hiveTableHeader.setGuid("hive-table-guid-00" + i);
            hiveTableHeader.setDisplayText("audit_test_table" + i);

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("name", "audit_test_table" + i);
            attributes.put("qualifiedName", "audit_test_db.audit_test_table" + i + "@cm");
            attributes.put("owner", "hive");
            hiveTableHeader.setAttributes(attributes);

            mutationResponse.addEntity(EntityOperation.CREATE, hiveTableHeader);
        }

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should be called with all entities
        verify(mockListener, times(1)).onEntitiesAdded(anyList(), anyBoolean());
    }

    /**
     * Scenario 4: Bulk internal type entities
     * Expected: Notification should NOT be sent
     */
    @Test
    public void testNotifyListeners_BulkInternalEntities() throws AtlasBaseException {
        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();

        // Add __AtlasAuditEntry
        AtlasEntityHeader auditEntryHeader = new AtlasEntityHeader();
        auditEntryHeader.setTypeName("__AtlasAuditEntry");
        auditEntryHeader.setGuid("audit-entry-guid-001");
        auditEntryHeader.setDisplayText("__audit_entry_001");
        Map<String, Object> auditAttrs = new HashMap<>();
        auditAttrs.put("qualifiedName", "__audit_entry_001");
        auditEntryHeader.setAttributes(auditAttrs);
        mutationResponse.addEntity(EntityOperation.CREATE, auditEntryHeader);

        // Add __AtlasUserProfile
        AtlasEntityHeader userProfileHeader = new AtlasEntityHeader();
        userProfileHeader.setTypeName("__AtlasUserProfile");
        userProfileHeader.setGuid("user-profile-guid-001");
        userProfileHeader.setDisplayText("__user_profile_001");
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put("qualifiedName", "__user_profile_001");
        userProfileHeader.setAttributes(userAttrs);
        mutationResponse.addEntity(EntityOperation.CREATE, userProfileHeader);

        // Add __AtlasMetricsStat
        AtlasEntityHeader metricsStatHeader = new AtlasEntityHeader();
        metricsStatHeader.setTypeName("__AtlasMetricsStat");
        metricsStatHeader.setGuid("metrics-stat-guid-001");
        metricsStatHeader.setDisplayText("__metrics_stat_001");
        Map<String, Object> metricsAttrs = new HashMap<>();
        metricsAttrs.put("qualifiedName", "__metrics_stat_001");
        metricsStatHeader.setAttributes(metricsAttrs);
        mutationResponse.addEntity(EntityOperation.CREATE, metricsStatHeader);

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should NOT be called (all internal entities filtered out)
        verify(mockListener, never()).onEntitiesAdded(anyList(), anyBoolean());
    }

    /**
     * Scenario 5: Bulk mixed (normal and internal type entities)
     * Expected: Notification should be sent ONLY for normal entities
     */
    @Test
    public void testNotifyListeners_BulkMixedEntities() throws AtlasBaseException {
        // Create mutation response
        EntityMutationResponse mutationResponse = new EntityMutationResponse();

        // Add normal hive_table entities
        for (int i = 1; i <= 3; i++) {
            AtlasEntityHeader hiveTableHeader = new AtlasEntityHeader();
            hiveTableHeader.setTypeName("hive_table");
            hiveTableHeader.setGuid("hive-table-guid-5" + i);
            hiveTableHeader.setDisplayText("audit_test_table5" + i);

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("name", "audit_test_table5" + i);
            attributes.put("qualifiedName", "audit_test_db.audit_test_table5" + i + "@cm");
            attributes.put("owner", "hive");
            hiveTableHeader.setAttributes(attributes);

            mutationResponse.addEntity(EntityOperation.CREATE, hiveTableHeader);
        }

        // Add internal entities
        AtlasEntityHeader auditEntryHeader = new AtlasEntityHeader();
        auditEntryHeader.setTypeName("__AtlasAuditEntry");
        auditEntryHeader.setGuid("audit-entry-guid-051");
        auditEntryHeader.setDisplayText("__audit_entry_051");
        Map<String, Object> auditAttrs = new HashMap<>();
        auditAttrs.put("qualifiedName", "__audit_entry_051");
        auditEntryHeader.setAttributes(auditAttrs);
        mutationResponse.addEntity(EntityOperation.CREATE, auditEntryHeader);

        AtlasEntityHeader userProfileHeader = new AtlasEntityHeader();
        userProfileHeader.setTypeName("__AtlasUserProfile");
        userProfileHeader.setGuid("user-profile-guid-051");
        userProfileHeader.setDisplayText("__user_profile_051");
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put("qualifiedName", "__user_profile_051");
        userProfileHeader.setAttributes(userAttrs);
        mutationResponse.addEntity(EntityOperation.CREATE, userProfileHeader);

        AtlasEntityHeader metricsStatHeader = new AtlasEntityHeader();
        metricsStatHeader.setTypeName("__AtlasMetricsStat");
        metricsStatHeader.setGuid("metrics-stat-guid-051");
        metricsStatHeader.setDisplayText("__metrics_stat_051");
        Map<String, Object> metricsAttrs = new HashMap<>();
        metricsAttrs.put("qualifiedName", "__metrics_stat_051");
        metricsStatHeader.setAttributes(metricsAttrs);
        mutationResponse.addEntity(EntityOperation.CREATE, metricsStatHeader);

        // Execute
        atlasEntityChangeNotifier.onEntitiesMutated(mutationResponse, false);

        // Verify: listener should be called (only non-internal entities passed)
        verify(mockListener, times(1)).onEntitiesAdded(anyList(), anyBoolean());
    }
}
