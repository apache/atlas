package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionProducer;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.repository.store.graph.v2.RequestMetadata;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

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
                eq("RELATIONSHIP_CREATE"),
                eq(Map.of()),
                eq(created),
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
}
