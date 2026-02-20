package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionEventType;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionProducer;
import org.apache.atlas.repository.store.graph.v2.RequestMetadata;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for async ingestion publish logic in EntityREST business metadata methods.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EntityRESTAsyncPublishTest {

    private AtlasEntityStore entitiesStore;
    private AsyncIngestionProducer asyncIngestionProducer;
    private EntityREST entityREST;
    private MockedStatic<DynamicConfigStore> mockedDynamicConfig;

    @BeforeEach
    void setUp() {
        entitiesStore = mock(AtlasEntityStore.class);
        asyncIngestionProducer = mock(AsyncIngestionProducer.class);

        entityREST = new EntityREST(
                null, // typeRegistry
                entitiesStore,
                null, // esBasedAuditRepository
                null, // entityGraphRetriever
                null, // entityMutationService
                null, // repairAttributeService
                null, // repairIndex
                asyncIngestionProducer);

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
    void testAddOrUpdateBusinessAttributes_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-123";
        Map<String, Map<String, Object>> businessAttributes = Map.of("bmName", Map.of("attr1", "value1"));

        entityREST.addOrUpdateBusinessAttributes(guid, false, businessAttributes);

        verify(entitiesStore).addOrUpdateBusinessAttributes(guid, businessAttributes, false);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES),
                argThat(meta -> guid.equals(meta.get("guid")) && Boolean.FALSE.equals(meta.get("isOverwrite"))),
                eq(businessAttributes),
                any(RequestMetadata.class));
    }

    @Test
    void testAddOrUpdateBusinessAttributesByDisplayName_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-456";
        Map<String, Map<String, Object>> businessAttributes = Map.of("bmDisplayName", Map.of("attr1", "value1"));

        entityREST.addOrUpdateBusinessAttributesByDisplayName(guid, true, businessAttributes);

        verify(entitiesStore).addOrUpdateBusinessAttributesByDisplayName(guid, businessAttributes, true);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME),
                argThat(meta -> guid.equals(meta.get("guid")) && Boolean.TRUE.equals(meta.get("isOverwrite"))),
                eq(businessAttributes),
                any(RequestMetadata.class));
    }

    @Test
    void testRemoveBusinessAttributes_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-789";
        Map<String, Map<String, Object>> businessAttributes = Map.of("bmName", Map.of("attr1", "value1"));

        entityREST.removeBusinessAttributes(guid, businessAttributes);

        verify(entitiesStore).removeBusinessAttributes(guid, businessAttributes);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.REMOVE_BUSINESS_ATTRIBUTES),
                eq(Map.of("guid", guid)),
                eq(businessAttributes),
                any(RequestMetadata.class));
    }

    @Test
    void testAddOrUpdateBusinessAttributesByName_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-abc";
        String bmName = "myBM";
        Map<String, Object> businessAttributes = Map.of("attr1", "value1");

        entityREST.addOrUpdateBusinessAttributes(guid, bmName, businessAttributes);

        verify(entitiesStore).addOrUpdateBusinessAttributes(guid, Collections.singletonMap(bmName, businessAttributes), false);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES),
                argThat(meta -> guid.equals(meta.get("guid"))
                        && Boolean.FALSE.equals(meta.get("isOverwrite"))
                        && bmName.equals(meta.get("bmName"))),
                eq(businessAttributes),
                any(RequestMetadata.class));
    }

    @Test
    void testRemoveBusinessAttributesByName_asyncEnabled_publishesEvent() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        String guid = "guid-def";
        String bmName = "myBM";
        Map<String, Object> businessAttributes = Map.of("attr1", "value1");

        entityREST.removeBusinessAttributes(guid, bmName, businessAttributes);

        verify(entitiesStore).removeBusinessAttributes(guid, Collections.singletonMap(bmName, businessAttributes));
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.REMOVE_BUSINESS_ATTRIBUTES),
                argThat(meta -> guid.equals(meta.get("guid")) && bmName.equals(meta.get("bmName"))),
                eq(businessAttributes),
                any(RequestMetadata.class));
    }

    @Test
    void testAddOrUpdateBusinessAttributes_asyncDisabled_noPublish() throws AtlasBaseException {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(false);

        entityREST.addOrUpdateBusinessAttributes("guid-123", false, Map.of("bm", Map.of("a", "b")));

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }
}
