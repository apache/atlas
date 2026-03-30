/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to
 * you under the Apache License, Version 2.0 (the
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
package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_USERS;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for StakeholderTitlePreProcessor.processDelete
 *
 * Tests validate that StakeholderTitle deletion is blocked only when
 * there are ACTIVE stakeholder entities with users assigned.
 * Empty stakeholders (no users) should not prevent deletion.
 */
public class StakeholderTitlePreProcessorTest {

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private AtlasEntityStore entityStore;

    @Mock
    private AtlasVertex mockTitleVertex;

    private StakeholderTitlePreProcessor preprocessor;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        preprocessor = new StakeholderTitlePreProcessor(null, typeRegistry, entityRetriever, entityStore);
    }

    /**
     * Test that deletion succeeds when there are no stakeholders
     */
    @Test
    public void testDeleteWithNoStakeholders() throws AtlasBaseException {
        AtlasEntity titleEntity = createStakeholderTitleEntity(null);
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        // Should not throw exception
        preprocessor.processDelete(mockTitleVertex);
    }

    /**
     * Test that deletion succeeds when all stakeholders are soft-deleted (entity status = DELETED)
     * even though relationships are still ACTIVE
     */
    @Test
    public void testDeleteWithSoftDeletedStakeholders() throws AtlasBaseException {
        List<AtlasRelatedObjectId> stakeholders = new ArrayList<>();

        // Stakeholder 1: DELETED entity with ACTIVE relationship
        stakeholders.add(createRelatedObjectId("guid-1", AtlasEntity.Status.DELETED, AtlasRelationship.Status.ACTIVE));

        // Stakeholder 2: DELETED entity with ACTIVE relationship
        stakeholders.add(createRelatedObjectId("guid-2", AtlasEntity.Status.DELETED, AtlasRelationship.Status.ACTIVE));

        AtlasEntity titleEntity = createStakeholderTitleEntity(stakeholders);
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        // Should not throw exception - soft-deleted stakeholders should not block deletion
        preprocessor.processDelete(mockTitleVertex);
    }

    /**
     * Test that deletion succeeds when all stakeholders have DELETED relationships
     * even if entity status is ACTIVE
     */
    @Test
    public void testDeleteWithDeletedRelationships() throws AtlasBaseException {
        List<AtlasRelatedObjectId> stakeholders = new ArrayList<>();

        // Stakeholder with ACTIVE entity but DELETED relationship
        stakeholders.add(createRelatedObjectId("guid-1", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.DELETED));

        AtlasEntity titleEntity = createStakeholderTitleEntity(stakeholders);
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        // Should not throw exception - deleted relationships should not block deletion
        preprocessor.processDelete(mockTitleVertex);
    }

    /**
     * Test that deletion succeeds when stakeholders are ACTIVE but have no users assigned
     * This is the key scenario from GOV-736
     * Verifies auto-deletion of empty stakeholders
     */
    @Test
    public void testDeleteWithActiveStakeholdersButNoUsers() throws AtlasBaseException {
        List<AtlasRelatedObjectId> stakeholders = new ArrayList<>();

        // Active stakeholders with no users
        stakeholders.add(createRelatedObjectId("guid-1", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.ACTIVE));
        stakeholders.add(createRelatedObjectId("guid-2", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.ACTIVE));

        // Mock stakeholder vertices with empty user lists
        AtlasVertex mockStakeholder1 = mock(AtlasVertex.class);
        AtlasVertex mockStakeholder2 = mock(AtlasVertex.class);

        when(entityRetriever.getEntityVertex("guid-1")).thenReturn(mockStakeholder1);
        when(entityRetriever.getEntityVertex("guid-2")).thenReturn(mockStakeholder2);

        // Mock stakeholder entities
        AtlasEntity stakeholderEntity1 = new AtlasEntity("Stakeholder");
        stakeholderEntity1.setGuid("guid-1");
        stakeholderEntity1.setAttribute(QUALIFIED_NAME, "stakeholder-1-qn");

        AtlasEntity stakeholderEntity2 = new AtlasEntity("Stakeholder");
        stakeholderEntity2.setGuid("guid-2");
        stakeholderEntity2.setAttribute(QUALIFIED_NAME, "stakeholder-2-qn");

        when(entityRetriever.toAtlasEntity(mockStakeholder1)).thenReturn(stakeholderEntity1);
        when(entityRetriever.toAtlasEntity(mockStakeholder2)).thenReturn(stakeholderEntity2);

        when(mockStakeholder1.getMultiValuedProperty(eq(ATTR_PERSONA_USERS), eq(String.class)))
            .thenReturn(Collections.emptyList());
        when(mockStakeholder2.getMultiValuedProperty(eq(ATTR_PERSONA_USERS), eq(String.class)))
            .thenReturn(Collections.emptyList());

        AtlasEntity titleEntity = createStakeholderTitleEntity(stakeholders);
        titleEntity.setAttribute(NAME, "Test Title");
        titleEntity.setGuid("title-guid");
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        // Should not throw exception - empty stakeholders should be auto-deleted
        preprocessor.processDelete(mockTitleVertex);

        // Verify bulk deletion was called with both stakeholder GUIDs
        verify(entityStore, times(1)).deleteByIds(argThat(guids ->
            guids != null &&
            guids.size() == 2 &&
            guids.contains("guid-1") &&
            guids.contains("guid-2")
        ));
    }

    /**
     * Test that deletion is BLOCKED when there is at least one ACTIVE stakeholder
     * with users assigned
     */
    @Test
    public void testDeleteBlockedWithActiveStakeholderWithUsers() throws AtlasBaseException {
        List<AtlasRelatedObjectId> stakeholders = new ArrayList<>();

        // Mix of stakeholders with and without users
        stakeholders.add(createRelatedObjectId("guid-1", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.ACTIVE));
        stakeholders.add(createRelatedObjectId("guid-2", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.ACTIVE)); // This has users
        stakeholders.add(createRelatedObjectId("guid-3", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.ACTIVE));

        // Mock stakeholder vertices
        AtlasVertex mockStakeholder1 = mock(AtlasVertex.class);
        AtlasVertex mockStakeholder2 = mock(AtlasVertex.class);
        AtlasVertex mockStakeholder3 = mock(AtlasVertex.class);

        when(entityRetriever.getEntityVertex("guid-1")).thenReturn(mockStakeholder1);
        when(entityRetriever.getEntityVertex("guid-2")).thenReturn(mockStakeholder2);
        when(entityRetriever.getEntityVertex("guid-3")).thenReturn(mockStakeholder3);

        // Mock stakeholder entities
        AtlasEntity stakeholderEntity1 = new AtlasEntity("Stakeholder");
        stakeholderEntity1.setGuid("guid-1");
        stakeholderEntity1.setAttribute(QUALIFIED_NAME, "stakeholder-1-qn");

        AtlasEntity stakeholderEntity2 = new AtlasEntity("Stakeholder");
        stakeholderEntity2.setGuid("guid-2");
        stakeholderEntity2.setAttribute(QUALIFIED_NAME, "stakeholder-2-qn");

        AtlasEntity stakeholderEntity3 = new AtlasEntity("Stakeholder");
        stakeholderEntity3.setGuid("guid-3");
        stakeholderEntity3.setAttribute(QUALIFIED_NAME, "stakeholder-3-qn");

        when(entityRetriever.toAtlasEntity(mockStakeholder1)).thenReturn(stakeholderEntity1);
        when(entityRetriever.toAtlasEntity(mockStakeholder2)).thenReturn(stakeholderEntity2);
        when(entityRetriever.toAtlasEntity(mockStakeholder3)).thenReturn(stakeholderEntity3);

        // First stakeholder has no users
        when(mockStakeholder1.getMultiValuedProperty(eq(ATTR_PERSONA_USERS), eq(String.class)))
            .thenReturn(Collections.emptyList());

        // Second stakeholder has users - this should block deletion
        when(mockStakeholder2.getMultiValuedProperty(eq(ATTR_PERSONA_USERS), eq(String.class)))
            .thenReturn(List.of("user1@example.com", "user2@example.com"));

        // Third stakeholder has no users
        when(mockStakeholder3.getMultiValuedProperty(eq(ATTR_PERSONA_USERS), eq(String.class)))
            .thenReturn(Collections.emptyList());

        AtlasEntity titleEntity = createStakeholderTitleEntity(stakeholders);
        titleEntity.setAttribute(NAME, "Test Title");
        titleEntity.setGuid("title-guid");
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        try {
            preprocessor.processDelete(mockTitleVertex);
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            // Expected - should throw OPERATION_NOT_SUPPORTED
            assert e.getAtlasErrorCode() == OPERATION_NOT_SUPPORTED;
            assert e.getMessage().contains("Can not delete StakeholderTitle as it has reference to Active Stakeholder with users");
        }

        // Verify no deletion was attempted
        verify(entityStore, never()).deleteByIds(anyList());
    }

    /**
     * Test that deletion succeeds when stakeholders are PURGED
     */
    @Test
    public void testDeleteWithPurgedStakeholders() throws AtlasBaseException {
        List<AtlasRelatedObjectId> stakeholders = new ArrayList<>();

        // PURGED stakeholders
        stakeholders.add(createRelatedObjectId("guid-1", AtlasEntity.Status.PURGED, AtlasRelationship.Status.ACTIVE));
        stakeholders.add(createRelatedObjectId("guid-2", AtlasEntity.Status.PURGED, AtlasRelationship.Status.PURGED));

        AtlasEntity titleEntity = createStakeholderTitleEntity(stakeholders);
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        // Should not throw exception
        preprocessor.processDelete(mockTitleVertex);
    }

    /**
     * Test that deletion succeeds when all stakeholders have null user lists
     */
    @Test
    public void testDeleteWithActiveStakeholdersWithNullUsers() throws AtlasBaseException {
        List<AtlasRelatedObjectId> stakeholders = new ArrayList<>();

        stakeholders.add(createRelatedObjectId("guid-1", AtlasEntity.Status.ACTIVE, AtlasRelationship.Status.ACTIVE));

        // Mock stakeholder vertex with null user list
        AtlasVertex mockStakeholder1 = mock(AtlasVertex.class);

        when(entityRetriever.getEntityVertex("guid-1")).thenReturn(mockStakeholder1);

        // Mock stakeholder entity
        AtlasEntity stakeholderEntity1 = new AtlasEntity("Stakeholder");
        stakeholderEntity1.setGuid("guid-1");
        stakeholderEntity1.setAttribute(QUALIFIED_NAME, "stakeholder-1-qn");

        when(entityRetriever.toAtlasEntity(mockStakeholder1)).thenReturn(stakeholderEntity1);
        when(mockStakeholder1.getMultiValuedProperty(eq(ATTR_PERSONA_USERS), eq(String.class))).thenReturn(null);

        AtlasEntity titleEntity = createStakeholderTitleEntity(stakeholders);
        titleEntity.setAttribute(NAME, "Test Title");
        titleEntity.setGuid("title-guid");
        when(entityRetriever.toAtlasEntity(mockTitleVertex)).thenReturn(titleEntity);
        when(mockTitleVertex.getMultiValuedProperty(any(), any())).thenReturn(List.of("default/domain/*/super"));

        // Should not throw exception - null user list should be auto-deleted
        preprocessor.processDelete(mockTitleVertex);

        // Verify bulk deletion was called with the stakeholder GUID
        verify(entityStore, times(1)).deleteByIds(argThat(guids ->
            guids != null &&
            guids.size() == 1 &&
            guids.contains("guid-1")
        ));
    }

    // Helper methods

    private AtlasEntity createStakeholderTitleEntity(List<AtlasRelatedObjectId> stakeholders) {
        AtlasEntity entity = new AtlasEntity("StakeholderTitle");
        entity.setAttribute(QUALIFIED_NAME, "stakeholderTitle/domain/default/test123");

        if (stakeholders != null) {
            Map<String, Object> relationshipAttributes = new HashMap<>();
            relationshipAttributes.put("stakeholders", stakeholders);
            entity.setRelationshipAttributes(relationshipAttributes);
        }

        return entity;
    }

    private AtlasRelatedObjectId createRelatedObjectId(
            String guid,
            AtlasEntity.Status entityStatus,
            AtlasRelationship.Status relationshipStatus) {

        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId(
            guid,
            "Stakeholder",
            entityStatus,
            "rel-guid-" + guid,
            relationshipStatus,
            null
        );

        return relatedObjectId;
    }
}
