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
package org.apache.atlas.repository.audit;

import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.EntityAuditEvent.EntityAuditAction;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class EntityAuditListenerTest {
    @Mock
    private EntityAuditRepository mockAuditRepository;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private RequestContext mockRequestContext;

    private EntityAuditListener entityAuditListener;
    private MockedStatic<RequestContext> mockedRequestContext;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        entityAuditListener = new EntityAuditListener(mockAuditRepository, mockTypeRegistry);
        // Mock RequestContext static methods
        mockedRequestContext = mockStatic(RequestContext.class);
        mockedRequestContext.when(RequestContext::get).thenReturn(mockRequestContext);
        when(mockRequestContext.startMetricRecord(anyString())).thenReturn(null);
        doNothing().when(mockRequestContext).endMetricRecord(any());
        when(mockRequestContext.getRequestTime()).thenReturn(System.currentTimeMillis());
        when(mockRequestContext.getUser()).thenReturn("testUser");
    }

    @AfterMethod
    public void tearDown() {
        if (mockedRequestContext != null) {
            mockedRequestContext.close();
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(entityAuditListener);
    }

    @Test
    public void testGetV1AuditPrefixForEntityCreate() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_CREATE);

        // Then
        assertEquals(result, "Created: ");
    }

    @Test
    public void testGetV1AuditPrefixForEntityUpdate() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_UPDATE);

        // Then
        assertEquals(result, "Updated: ");
    }

    @Test
    public void testGetV1AuditPrefixForEntityDelete() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_DELETE);

        // Then
        assertEquals(result, "Deleted: ");
    }

    @Test
    public void testGetV1AuditPrefixForTagAdd() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.TAG_ADD);

        // Then
        assertEquals(result, "Added trait: ");
    }

    @Test
    public void testGetV1AuditPrefixForTagDelete() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.TAG_DELETE);

        // Then
        assertEquals(result, "Deleted trait: ");
    }

    @Test
    public void testGetV1AuditPrefixForTagUpdate() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.TAG_UPDATE);

        // Then
        assertEquals(result, "Updated trait: ");
    }

    @Test
    public void testGetV1AuditPrefixForEntityImportCreate() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_IMPORT_CREATE);

        // Then
        assertEquals(result, "Created by import: ");
    }

    @Test
    public void testGetV1AuditPrefixForEntityImportUpdate() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_IMPORT_UPDATE);

        // Then
        assertEquals(result, "Updated by import: ");
    }

    @Test
    public void testGetV1AuditPrefixForEntityImportDelete() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_IMPORT_DELETE);

        // Then
        assertEquals(result, "Deleted by import: ");
    }

    @Test
    public void testGetV1AuditPrefixForTermAdd() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.TERM_ADD);

        // Then
        assertEquals(result, "Added term: ");
    }

    @Test
    public void testGetV1AuditPrefixForTermDelete() {
        // When
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.TERM_DELETE);

        // Then
        assertEquals(result, "Deleted term: ");
    }

    @Test
    public void testGetV1AuditPrefixForUnknownAction() {
        String result = EntityAuditListener.getV1AuditPrefix(EntityAuditAction.ENTITY_CREATE);

        // Then
        assertEquals(result, "Created: ");
    }

    @Test
    public void testGetV2AuditPrefixForEntityCreate() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_CREATE);

        // Then
        assertEquals(result, "Created: ");
    }

    @Test
    public void testGetV2AuditPrefixForEntityUpdate() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_UPDATE);

        // Then
        assertEquals(result, "Updated: ");
    }

    @Test
    public void testGetV2AuditPrefixForEntityDelete() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_DELETE);

        // Then
        assertEquals(result, "Deleted: ");
    }

    @Test
    public void testGetV2AuditPrefixForTagAdd() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.TAG_ADD);

        // Then
        assertEquals(result, "Added classification: ");
    }

    @Test
    public void testGetV2AuditPrefixForTagDelete() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.TAG_DELETE);

        // Then
        assertEquals(result, "Deleted classification: ");
    }

    @Test
    public void testGetV2AuditPrefixForTagUpdate() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.TAG_UPDATE);

        // Then
        assertEquals(result, "Updated classification: ");
    }

    @Test
    public void testGetV2AuditPrefixForEntityImportCreate() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_IMPORT_CREATE);

        // Then
        assertEquals(result, "Created by import: ");
    }

    @Test
    public void testGetV2AuditPrefixForEntityImportUpdate() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_IMPORT_UPDATE);

        // Then
        assertEquals(result, "Updated by import: ");
    }

    @Test
    public void testGetV2AuditPrefixForEntityImportDelete() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_IMPORT_DELETE);

        // Then
        assertEquals(result, "Deleted by import: ");
    }

    @Test
    public void testGetV2AuditPrefixForTermAdd() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.TERM_ADD);

        // Then
        assertEquals(result, "Added term: ");
    }

    @Test
    public void testGetV2AuditPrefixForTermDelete() {
        // When
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.TERM_DELETE);

        // Then
        assertEquals(result, "Deleted term: ");
    }

    @Test
    public void testGetV2AuditPrefixForUnknownAction() {
        String result = EntityAuditListener.getV2AuditPrefix(EntityAuditAction.ENTITY_CREATE);

        // Then
        assertEquals(result, "Created: ");
    }

    @Test
    public void testOnEntitiesAddedWithRegularImport() throws AtlasException {
        // Given
        Referenceable entity1 = createTestReferenceable("entity1", "DataSet");
        Referenceable entity2 = createTestReferenceable("entity2", "Table");
        Collection<Referenceable> entities = Arrays.asList(entity1, entity2);

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onEntitiesAdded(entities, false);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnEntitiesAddedWithImport() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Collection<Referenceable> entities = Collections.singletonList(entity);

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onEntitiesAdded(entities, true);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
    }

    @Test
    public void testOnEntitiesUpdatedWithRegularUpdate() throws AtlasException {
        // Given
        Referenceable entity1 = createTestReferenceable("entity1", "DataSet");
        Referenceable entity2 = createTestReferenceable("entity2", "Table");
        Collection<Referenceable> entities = Arrays.asList(entity1, entity2);

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onEntitiesUpdated(entities, false);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnEntitiesUpdatedWithImport() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Collection<Referenceable> entities = Collections.singletonList(entity);

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onEntitiesUpdated(entities, true);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
    }

    @Test
    public void testOnTraitsAdded() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Struct trait1 = new Struct("PII");
        Struct trait2 = new Struct("Confidential");
        Collection<Struct> traits = Arrays.asList(trait1, trait2);

        doNothing().when(mockAuditRepository).putEventsV1(any(EntityAuditEvent.class));

        // When
        entityAuditListener.onTraitsAdded(entity, traits);

        // Then
        verify(mockAuditRepository, times(2)).putEventsV1(any(EntityAuditEvent.class));
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnTraitsAddedWithNullTraits() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");

        // When
        entityAuditListener.onTraitsAdded(entity, null);

        verify(mockAuditRepository, times(0)).putEventsV1(any(EntityAuditEvent.class));
    }

    @Test
    public void testOnTraitsDeleted() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Struct trait1 = new Struct("PII");
        Struct trait2 = new Struct("Confidential");
        Collection<Struct> traits = Arrays.asList(trait1, trait2);

        doNothing().when(mockAuditRepository).putEventsV1(any(EntityAuditEvent.class));

        // When
        entityAuditListener.onTraitsDeleted(entity, traits);

        // Then
        verify(mockAuditRepository, times(2)).putEventsV1(any(EntityAuditEvent.class));
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnTraitsDeletedWithNullTraits() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");

        // When
        entityAuditListener.onTraitsDeleted(entity, null);

        verify(mockAuditRepository, times(0)).putEventsV1(any(EntityAuditEvent.class));
    }

    @Test
    public void testOnTraitsUpdated() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Struct trait1 = new Struct("PII");
        Struct trait2 = new Struct("Confidential");
        Collection<Struct> traits = Arrays.asList(trait1, trait2);

        doNothing().when(mockAuditRepository).putEventsV1(any(EntityAuditEvent.class));

        // When
        entityAuditListener.onTraitsUpdated(entity, traits);

        // Then
        verify(mockAuditRepository, times(2)).putEventsV1(any(EntityAuditEvent.class));
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnTraitsUpdatedWithNullTraits() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");

        // When
        entityAuditListener.onTraitsUpdated(entity, null);

        verify(mockAuditRepository, times(0)).putEventsV1(any(EntityAuditEvent.class));
    }

    @Test
    public void testOnEntitiesDeletedWithRegularDelete() throws AtlasException {
        // Given
        Referenceable entity1 = createTestReferenceable("entity1", "DataSet");
        Referenceable entity2 = createTestReferenceable("entity2", "Table");
        Collection<Referenceable> entities = Arrays.asList(entity1, entity2);

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onEntitiesDeleted(entities, false);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnEntitiesDeletedWithImport() throws AtlasException {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Collection<Referenceable> entities = Collections.singletonList(entity);

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onEntitiesDeleted(entities, true);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
    }

    @Test
    public void testOnTermAdded() throws AtlasException {
        // Given
        Referenceable entity1 = createTestReferenceable("entity1", "DataSet");
        Referenceable entity2 = createTestReferenceable("entity2", "Table");
        Collection<Referenceable> entities = Arrays.asList(entity1, entity2);
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setName("TestTerm");
        term.setGuid("term-guid-123");

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onTermAdded(entities, term);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testOnTermDeleted() throws AtlasException {
        // Given
        Referenceable entity1 = createTestReferenceable("entity1", "DataSet");
        Referenceable entity2 = createTestReferenceable("entity2", "Table");
        Collection<Referenceable> entities = Arrays.asList(entity1, entity2);
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setName("TestTerm");
        term.setGuid("term-guid-123");

        doNothing().when(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());

        // When
        entityAuditListener.onTermDeleted(entities, term);

        // Then
        verify(mockAuditRepository).putEventsV1(ArgumentMatchers.<List<EntityAuditEvent>>any());
        verify(mockRequestContext).startMetricRecord("entityAudit");
        verify(mockRequestContext).endMetricRecord(any());
    }

    @Test
    public void testGetAuditEvents() throws AtlasException {
        // Given
        String guid = "test-guid-123";
        List<EntityAuditEvent> expectedEvents = Arrays.asList(new EntityAuditEvent("guid1", System.currentTimeMillis(), "user1", EntityAuditAction.ENTITY_CREATE, "details1", null), new EntityAuditEvent("guid2", System.currentTimeMillis(), "user2", EntityAuditAction.ENTITY_UPDATE, "details2", null));
        when(mockAuditRepository.listEventsV1(eq(guid), eq(null), eq((short) 10))).thenReturn(expectedEvents);

        // When
        List<EntityAuditEvent> result = entityAuditListener.getAuditEvents(guid);

        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        verify(mockAuditRepository).listEventsV1(eq(guid), eq(null), eq((short) 10));
    }

    @Test
    public void testCreateEventWithAction() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        setupMockForAuditDetailGeneration(entity);

        Method method = EntityAuditListener.class.getDeclaredMethod("createEvent", Referenceable.class, EntityAuditAction.class);
        method.setAccessible(true);

        // When
        EntityAuditEvent result = (EntityAuditEvent) method.invoke(entityAuditListener, entity, EntityAuditAction.ENTITY_CREATE);

        // Then
        assertNotNull(result);
        assertEquals(result.getEntityId(), "entity1");
        assertEquals(result.getAction(), EntityAuditAction.ENTITY_CREATE);
        assertEquals(result.getUser(), "testUser");
    }

    @Test
    public void testCreateEventWithDetails() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Method method = EntityAuditListener.class.getDeclaredMethod("createEvent", Referenceable.class, EntityAuditAction.class, String.class);
        method.setAccessible(true);

        // When
        EntityAuditEvent result = (EntityAuditEvent) method.invoke(entityAuditListener, entity, EntityAuditAction.ENTITY_CREATE, "Custom details");

        // Then
        assertNotNull(result);
        assertEquals(result.getEntityId(), "entity1");
        assertEquals(result.getAction(), EntityAuditAction.ENTITY_CREATE);
        assertEquals(result.getDetails(), "Custom details");
        assertEquals(result.getUser(), "testUser");
    }

    @Test
    public void testGetAuditEventDetail() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        setupMockForAuditDetailGeneration(entity);

        Method method = EntityAuditListener.class.getDeclaredMethod("getAuditEventDetail", Referenceable.class, EntityAuditAction.class);
        method.setAccessible(true);

        // When
        String result = (String) method.invoke(entityAuditListener, entity, EntityAuditAction.ENTITY_CREATE);

        // Then
        assertNotNull(result);
        assertTrue(result.startsWith("Created: "));
    }

    @Test
    public void testGetAuditEventDetailWithLargeEntity() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        setupMockForAuditDetailGeneration(entity);
        when(mockAuditRepository.repositoryMaxSize()).thenReturn(100L); // Small max size to trigger pruning

        Method method = EntityAuditListener.class.getDeclaredMethod("getAuditEventDetail", Referenceable.class, EntityAuditAction.class);
        method.setAccessible(true);

        // When
        String result = (String) method.invoke(entityAuditListener, entity, EntityAuditAction.ENTITY_CREATE);

        // Then
        assertNotNull(result);
        assertTrue(result.startsWith("Created: "));
    }

    @Test
    public void testPruneEntityAttributesForAudit() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        setupMockForEntityTypePruning(entity);

        Method method = EntityAuditListener.class.getDeclaredMethod("pruneEntityAttributesForAudit", Referenceable.class);
        method.setAccessible(true);

        // When
        method.invoke(entityAuditListener, entity);
    }

    @Test
    public void testRestoreEntityAttributes() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Map<String, Object> prunedAttributes = new HashMap<>();
        prunedAttributes.put("excludedAttr", "excludedValue");
        setupMockForEntityTypePruning(entity);

        Method method = EntityAuditListener.class.getDeclaredMethod("restoreEntityAttributes", Referenceable.class, Map.class);
        method.setAccessible(true);

        // When
        method.invoke(entityAuditListener, entity, prunedAttributes);
    }

    @Test
    public void testRestoreEntityAttributesWithEmptyPrunedAttributes() throws Exception {
        // Given
        Referenceable entity = createTestReferenceable("entity1", "DataSet");
        Map<String, Object> emptyPrunedAttributes = new HashMap<>();

        Method method = EntityAuditListener.class.getDeclaredMethod("restoreEntityAttributes", Referenceable.class, Map.class);
        method.setAccessible(true);

        // When
        method.invoke(entityAuditListener, entity, emptyPrunedAttributes);
    }

    @Test
    public void testPruneAttributes() throws Exception {
        // Given
        Referenceable attribute = createTestReferenceable("attr1", "AttributeType");
        setupMockForEntityTypePruning(attribute);

        Method method = EntityAuditListener.class.getDeclaredMethod("pruneAttributes", Map.class, Referenceable.class);
        method.setAccessible(true);

        // When
        method.invoke(entityAuditListener, null, attribute);
    }

    @Test
    public void testRestoreAttributes() throws Exception {
        // Given
        Map<String, Object> prunedAttributes = new HashMap<>();
        prunedAttributes.put("attr1", new HashMap<String, Object>());
        Referenceable attributeEntity = createTestReferenceable("attr1", "AttributeType");

        Method method = EntityAuditListener.class.getDeclaredMethod("restoreAttributes", Map.class, Referenceable.class);
        method.setAccessible(true);

        // When
        method.invoke(entityAuditListener, prunedAttributes, attributeEntity);
    }

    private Referenceable createTestReferenceable(String id, String typeName) {
        // Add some test attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "testName");
        attributes.put("description", "testDescription");
        Referenceable entity = new Referenceable(id, typeName, attributes);
        return entity;
    }

    private void setupMockForAuditDetailGeneration(Referenceable entity) throws AtlasException {
        when(mockAuditRepository.getAuditExcludeAttributes(anyString())).thenReturn(Collections.emptyList());
        when(mockAuditRepository.repositoryMaxSize()).thenReturn(-1L); // No size limit
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockTypeRegistry.getEntityTypeByName(entity.getTypeName())).thenReturn(mockEntityType);
        when(mockEntityType.getAllAttributes()).thenReturn(new HashMap<>());
    }

    private void setupMockForEntityTypePruning(Referenceable entity) throws AtlasException {
        when(mockAuditRepository.getAuditExcludeAttributes(anyString())).thenReturn(Arrays.asList("excludedAttr"));
        AtlasEntityType mockEntityType = mock(AtlasEntityType.class);
        when(mockTypeRegistry.getEntityTypeByName(entity.getTypeName())).thenReturn(mockEntityType);
        AtlasStructType.AtlasAttribute mockAttribute = mock(AtlasStructType.AtlasAttribute.class);
        when(mockAttribute.getName()).thenReturn("excludedAttr");
        when(mockAttribute.isOwnedRef()).thenReturn(false);
        Map<String, AtlasStructType.AtlasAttribute> attributes = new HashMap<>();
        attributes.put("excludedAttr", mockAttribute);
        when(mockEntityType.getAllAttributes()).thenReturn(attributes);
    }
}
