package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionEventType;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionProducer;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.repository.store.graph.v2.RequestMetadata;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.type.AtlasType;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for async ingestion publish logic in RelationshipREST.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RelationshipRESTAsyncPublishTest {

    private AtlasRelationshipStore relationshipStore;
    private EntityMutationService entityMutationService;
    private AsyncIngestionProducer asyncIngestionProducer;
    private RelationshipREST relationshipREST;
    private MockedStatic<DynamicConfigStore> mockedDynamicConfig;

    @BeforeEach
    void setUp() {
        relationshipStore = mock(AtlasRelationshipStore.class);
        entityMutationService = mock(EntityMutationService.class);
        asyncIngestionProducer = mock(AsyncIngestionProducer.class);

        relationshipREST = new RelationshipREST(relationshipStore, entityMutationService, asyncIngestionProducer);

        mockedDynamicConfig = mockStatic(DynamicConfigStore.class);

        RequestContext ctx = RequestContext.get();
        ctx.setTraceId("test-trace");
        ctx.setUser("admin", null);
    }

    @AfterEach
    void tearDown() {
        mockedDynamicConfig.close();
        RequestContext.clear();
    }

    // =================== create() tests ===================

    @Test
    void testCreate_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasRelationship input = new AtlasRelationship("testRelType");
        AtlasRelationship created = new AtlasRelationship("testRelType");
        created.setGuid("rel-guid-123");

        when(relationshipStore.create(input)).thenReturn(created);

        AtlasRelationship result = relationshipREST.create(input);

        assertSame(created, result);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.RELATIONSHIP_CREATE),
                eq(Map.of()),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));
    }

    @Test
    void testCreate_asyncDisabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(false);

        AtlasRelationship input = new AtlasRelationship("testRelType");
        when(relationshipStore.create(input)).thenReturn(input);

        relationshipREST.create(input);

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    @Test
    void testCreate_producerThrows_doesNotFailMainFlow() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasRelationship input = new AtlasRelationship("testRelType");
        AtlasRelationship created = new AtlasRelationship("testRelType");
        created.setGuid("rel-guid-456");

        when(relationshipStore.create(input)).thenReturn(created);
        when(asyncIngestionProducer.publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class)))
                .thenThrow(new RuntimeException("Kafka exploded"));

        AtlasRelationship result = relationshipREST.create(input);

        assertSame(created, result);
    }

    // =================== createOrUpdate() tests ===================

    @Test
    void testCreateOrUpdate_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasRelationship rel1 = new AtlasRelationship("type1");
        AtlasRelationship rel2 = new AtlasRelationship("type2");
        List<AtlasRelationship> input = List.of(rel1, rel2);

        AtlasRelationship out1 = new AtlasRelationship("type1");
        out1.setGuid("guid-1");
        AtlasRelationship out2 = new AtlasRelationship("type2");
        out2.setGuid("guid-2");
        List<AtlasRelationship> output = List.of(out1, out2);

        when(relationshipStore.createOrUpdate(input)).thenReturn(output);

        List<AtlasRelationship> result = relationshipREST.createOrUpdate(input);

        assertSame(output, result);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.RELATIONSHIP_BULK_CREATE_OR_UPDATE),
                eq(Map.of()),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));
    }

    @Test
    void testCreateOrUpdate_asyncDisabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(false);

        List<AtlasRelationship> input = List.of(new AtlasRelationship("type1"));
        when(relationshipStore.createOrUpdate(input)).thenReturn(input);

        relationshipREST.createOrUpdate(input);

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    @Test
    void testCreateOrUpdate_failure_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        List<AtlasRelationship> input = List.of(new AtlasRelationship("type1"));
        when(relationshipStore.createOrUpdate(input)).thenThrow(new RuntimeException("store failed"));

        assertThrows(RuntimeException.class, () -> relationshipREST.createOrUpdate(input));

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== update() tests ===================

    @Test
    void testUpdate_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasRelationship input = new AtlasRelationship("testRelType");
        AtlasRelationship updated = new AtlasRelationship("testRelType");
        updated.setGuid("rel-guid-789");

        when(relationshipStore.update(input)).thenReturn(updated);

        AtlasRelationship result = relationshipREST.update(input);

        assertSame(updated, result);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.RELATIONSHIP_UPDATE),
                eq(Map.of()),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));
    }

    @Test
    void testUpdate_asyncDisabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(false);

        AtlasRelationship input = new AtlasRelationship("testRelType");
        when(relationshipStore.update(input)).thenReturn(input);

        relationshipREST.update(input);

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    @Test
    void testUpdate_producerThrows_doesNotFailMainFlow() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        AtlasRelationship input = new AtlasRelationship("testRelType");
        AtlasRelationship updated = new AtlasRelationship("testRelType");
        updated.setGuid("rel-guid-999");

        when(relationshipStore.update(input)).thenReturn(updated);
        when(asyncIngestionProducer.publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class)))
                .thenThrow(new RuntimeException("Kafka exploded"));

        AtlasRelationship result = relationshipREST.update(input);

        assertSame(updated, result);
    }
}
