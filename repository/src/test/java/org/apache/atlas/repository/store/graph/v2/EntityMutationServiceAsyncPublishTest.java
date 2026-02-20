package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for async ingestion publish logic in EntityMutationService.
 * Verifies that publishAsyncIngestionEvent is called correctly in the finally blocks.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityMutationServiceAsyncPublishTest {

    private AtlasEntityStoreV2 entityStore;
    private EntityMutationPostProcessor entityMutationPostProcessor;
    private AtlasTypeRegistry typeRegistry;
    private AtlasEntityStore entitiesStore;
    private EntityGraphMapper entityGraphMapper;
    private IAtlasEntityChangeNotifier entityChangeNotifier;
    private AtlasInstanceConverter instanceConverter;
    private EntityGraphRetriever entityGraphRetriever;
    private AtlasRelationshipStore relationshipStore;
    private AsyncIngestionProducer asyncIngestionProducer;

    private EntityMutationService entityMutationService;
    private MockedStatic<DynamicConfigStore> mockedDynamicConfig;

    @BeforeEach
    void setUp() {
        entityStore = mock(AtlasEntityStoreV2.class);
        entityMutationPostProcessor = mock(EntityMutationPostProcessor.class);
        typeRegistry = mock(AtlasTypeRegistry.class);
        entitiesStore = mock(AtlasEntityStore.class);
        entityGraphMapper = mock(EntityGraphMapper.class);
        entityChangeNotifier = mock(IAtlasEntityChangeNotifier.class);
        instanceConverter = mock(AtlasInstanceConverter.class);
        entityGraphRetriever = mock(EntityGraphRetriever.class);
        relationshipStore = mock(AtlasRelationshipStore.class);
        asyncIngestionProducer = mock(AsyncIngestionProducer.class);

        entityMutationService = new EntityMutationService(
                entityStore, entityMutationPostProcessor, typeRegistry, entitiesStore,
                entityGraphMapper, entityChangeNotifier, instanceConverter,
                entityGraphRetriever, relationshipStore, asyncIngestionProducer);

        mockedDynamicConfig = mockStatic(DynamicConfigStore.class);

        // Set up RequestContext for each test
        RequestContext ctx = RequestContext.get();
        ctx.setTraceId("test-trace");
        ctx.setUser("admin", null);
    }

    @AfterEach
    void tearDown() {
        mockedDynamicConfig.close();
        RequestContext.clear();
    }

    // =================== Test 1: createOrUpdate success + async enabled ===================

    @Test
    void testCreateOrUpdate_graphSuccess_asyncEnabled_publishesBulkEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        when(entityStore.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                .thenReturn(mockResponse);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        BulkRequestContext context = new BulkRequestContext.Builder()
                .setOriginalEntities(entities)
                .build();

        EntityStream entityStream = mock(EntityStream.class);
        EntityMutationResponse result = entityMutationService.createOrUpdate(entityStream, context);

        assertSame(mockResponse, result);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.BULK_CREATE_OR_UPDATE),
                anyMap(),
                eq(entities),
                any(RequestMetadata.class));
    }

    // =================== Test 2: createOrUpdate success + async disabled ===================

    @Test
    void testCreateOrUpdate_graphSuccess_asyncDisabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(false);

        when(entityStore.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                .thenReturn(new EntityMutationResponse());

        BulkRequestContext context = new BulkRequestContext.Builder().build();
        entityMutationService.createOrUpdate(mock(EntityStream.class), context);

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== Test 3: createOrUpdate graph failure ===================

    @Test
    void testCreateOrUpdate_graphFailure_asyncEnabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        when(entityStore.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                .thenThrow(new AtlasBaseException("graph failure"));

        BulkRequestContext context = new BulkRequestContext.Builder().build();

        assertThrows(AtlasBaseException.class, () ->
                entityMutationService.createOrUpdate(mock(EntityStream.class), context));

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== Test 4: deleteById success ===================

    @Test
    void testDeleteById_graphSuccess_asyncEnabled_publishesDeleteEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        when(entityStore.deleteById("guid-123")).thenReturn(mockResponse);

        entityMutationService.deleteById("guid-123");

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_BY_GUID),
                eq(Map.of()),
                argThat(payload -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) payload;
                    @SuppressWarnings("unchecked")
                    List<String> guids = (List<String>) map.get("guids");
                    return guids.contains("guid-123");
                }),
                any(RequestMetadata.class));
    }

    // =================== Test 5: deleteByIds success ===================

    @Test
    void testDeleteByIds_graphSuccess_asyncEnabled_publishesDeleteByGuidsEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        List<String> guids = List.of("guid-1", "guid-2", "guid-3");
        when(entityStore.deleteByIds(guids)).thenReturn(new EntityMutationResponse());

        entityMutationService.deleteByIds(guids);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_BY_GUIDS),
                eq(Map.of()),
                eq(Map.of("guids", guids)),
                any(RequestMetadata.class));
    }

    // =================== Test 6: setClassifications success ===================

    @Test
    void testSetClassifications_graphFailure_asyncEnabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasEntityHeaders entityHeaders = mock(AtlasEntityHeaders.class);
        when(entityHeaders.getGuidHeaderMap()).thenReturn(Map.of());

        // setClassifications internally creates a ClassificationAssociator.Updater which
        // requires real typeRegistry/entityStore instances. With mocks, the Updater constructor
        // will throw, setting isGraphTransactionFailed=true. We verify that in this failure
        // scenario, publishEvent is NOT called (correct gating behavior).
        try {
            entityMutationService.setClassifications(entityHeaders, true);
        } catch (Exception e) {
            // Expected - ClassificationAssociator.Updater fails with mock dependencies
        }

        // Updater threw => isGraphTransactionFailed=true => no publish
        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== Test 7: deleteByUniqueAttributes single ===================

    @Test
    void testDeleteByUniqueAttributes_single_graphSuccess_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTypeName()).thenReturn("Table");

        Map<String, Object> uniqAttrs = Map.of("qualifiedName", "db.schema.table1");
        when(entityStore.deleteByUniqueAttributes(entityType, uniqAttrs))
                .thenReturn(new EntityMutationResponse());

        entityMutationService.deleteByUniqueAttributes(entityType, uniqAttrs);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_BY_UNIQUE_ATTRIBUTE),
                eq(Map.of("typeName", "Table")),
                eq(Map.of("uniqueAttributes", uniqAttrs)),
                any(RequestMetadata.class));
    }

    // =================== Test 8: deleteByUniqueAttributes bulk ===================

    @Test
    void testDeleteByUniqueAttributes_bulk_graphSuccess_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        List<AtlasObjectId> objectIds = List.of(
                new AtlasObjectId("Table", Map.of("qualifiedName", "db.schema.t1")),
                new AtlasObjectId("Table", Map.of("qualifiedName", "db.schema.t2")));
        when(entityStore.deleteByUniqueAttributes(objectIds))
                .thenReturn(new EntityMutationResponse());

        entityMutationService.deleteByUniqueAttributes(objectIds);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.BULK_DELETE_BY_UNIQUE_ATTRIBUTES),
                eq(Map.of()),
                eq(Map.of("objectIds", objectIds)),
                any(RequestMetadata.class));
    }

    // =================== Test 9: restoreByIds success ===================

    @Test
    void testRestoreByIds_graphSuccess_asyncEnabled_publishesRestoreEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        List<String> guids = List.of("guid-a", "guid-b");
        when(entityStore.restoreByIds(guids)).thenReturn(new EntityMutationResponse());

        entityMutationService.restoreByIds(guids);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.RESTORE_BY_GUIDS),
                eq(Map.of()),
                eq(Map.of("guids", guids)),
                any(RequestMetadata.class));
    }

    // =================== Test 10: producer throws, main flow unaffected ===================

    @Test
    void testPublishEvent_producerThrows_doesNotFailMainFlow() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        when(entityStore.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                .thenReturn(mockResponse);

        // Make the producer throw
        when(asyncIngestionProducer.publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class)))
                .thenThrow(new RuntimeException("Kafka exploded"));

        BulkRequestContext context = new BulkRequestContext.Builder().build();
        EntityMutationResponse result = entityMutationService.createOrUpdate(mock(EntityStream.class), context);

        // Main flow should still succeed despite Kafka failure
        assertSame(mockResponse, result);
    }

    // =================== Test 11: graph failure => exception propagated ===================

    @Test
    void testCreateOrUpdate_graphFailure_publishEvent_neverCalled() {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasBaseException graphException = new AtlasBaseException("JanusGraph error");
        try {
            when(entityStore.createOrUpdate(any(EntityStream.class), any(BulkRequestContext.class)))
                    .thenThrow(graphException);
        } catch (AtlasBaseException e) {
            fail("Should not throw during mock setup");
        }

        BulkRequestContext context = new BulkRequestContext.Builder().build();

        AtlasBaseException thrown = assertThrows(AtlasBaseException.class, () ->
                entityMutationService.createOrUpdate(mock(EntityStream.class), context));

        assertEquals("JanusGraph error", thrown.getMessage());
        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== Test 12: updateByUniqueAttributes success ===================

    @Test
    void testUpdateByUniqueAttributes_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        when(entityType.getTypeName()).thenReturn("Table");

        Map<String, Object> uniqAttrs = Map.of("qualifiedName", "db.schema.table1");
        AtlasEntityWithExtInfo updatedEntityInfo = new AtlasEntityWithExtInfo(new AtlasEntity("Table"));

        when(entityStore.updateByUniqueAttributes(entityType, uniqAttrs, updatedEntityInfo))
                .thenReturn(new EntityMutationResponse());

        entityMutationService.updateByUniqueAttributes(entityType, uniqAttrs, updatedEntityInfo);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.UPDATE_BY_UNIQUE_ATTRIBUTE),
                eq(Map.of("typeName", "Table")),
                argThat(payload -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) payload;
                    return map.containsKey("uniqueAttributes") && map.containsKey("entity");
                }),
                any(RequestMetadata.class));
    }

    // =================== Test 13: addClassifications success ===================

    @Test
    void testAddClassifications_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-123";
        List<AtlasClassification> classifications = List.of(new AtlasClassification("PII"));

        entityMutationService.addClassifications(guid, classifications);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.ADD_CLASSIFICATIONS),
                eq(Map.of()),
                argThat(payload -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) payload;
                    return guid.equals(map.get("guid")) && map.containsKey("classifications");
                }),
                any(RequestMetadata.class));
    }

    // =================== Test 14: updateClassifications success ===================

    @Test
    void testUpdateClassifications_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-456";
        List<AtlasClassification> classifications = List.of(new AtlasClassification("Confidential"));

        entityMutationService.updateClassifications(guid, classifications);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.UPDATE_CLASSIFICATIONS),
                eq(Map.of()),
                argThat(payload -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) payload;
                    return guid.equals(map.get("guid")) && map.containsKey("classifications");
                }),
                any(RequestMetadata.class));
    }

    // =================== Test 15: deleteClassification (single) success ===================

    @Test
    void testDeleteClassification_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-789";
        String classificationName = "PII";

        entityMutationService.deleteClassification(guid, classificationName);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_CLASSIFICATION),
                eq(Map.of()),
                eq(Map.of("guid", guid, "classificationName", classificationName)),
                any(RequestMetadata.class));
    }

    // =================== Test 16: deleteClassification (with associatedEntityGuid) success ===================

    @Test
    void testDeleteClassificationWithAssociated_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-789";
        String classificationName = "PII";
        String associatedEntityGuid = "assoc-guid-111";

        entityMutationService.deleteClassification(guid, classificationName, associatedEntityGuid);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_CLASSIFICATION),
                eq(Map.of()),
                eq(Map.of("guid", guid, "classificationName", classificationName,
                           "associatedEntityGuid", associatedEntityGuid)),
                any(RequestMetadata.class));
    }

    // =================== Test 17: addClassification bulk success ===================

    @Test
    void testAddClassificationBulk_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        List<String> guids = List.of("guid-1", "guid-2");
        AtlasClassification classification = new AtlasClassification("Sensitive");

        entityMutationService.addClassification(guids, classification);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.ADD_CLASSIFICATION_BULK),
                eq(Map.of()),
                argThat(payload -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) payload;
                    return map.containsKey("guids") && map.containsKey("classification");
                }),
                any(RequestMetadata.class));
    }

    // =================== Test 18: deleteRelationshipById success ===================

    @Test
    void testDeleteRelationshipById_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "rel-guid-123";

        entityMutationService.deleteRelationshipById(guid);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_RELATIONSHIP_BY_GUID),
                eq(Map.of()),
                eq(Map.of("guid", guid)),
                any(RequestMetadata.class));
    }

    // =================== Test 19: deleteRelationshipsByIds success ===================

    @Test
    void testDeleteRelationshipsByIds_graphSuccess_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        List<String> guids = List.of("rel-guid-1", "rel-guid-2");

        entityMutationService.deleteRelationshipsByIds(guids);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.DELETE_RELATIONSHIPS_BY_GUIDS),
                eq(Map.of()),
                eq(Map.of("guids", guids)),
                any(RequestMetadata.class));
    }
}
