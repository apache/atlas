package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.TypeCacheRefresher;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionEventType;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionProducer;
import org.apache.atlas.repository.store.graph.v2.RequestMetadata;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for async ingestion publish logic in TypesREST.
 * Verifies that publishTypeDefAsyncEvent is called correctly after successful retry wrappers.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TypesRESTAsyncPublishTest {

    private AtlasTypeDefStore typeDefStore;
    private RedisService redisService;
    private TypeCacheRefresher typeCacheRefresher;
    private AsyncIngestionProducer asyncIngestionProducer;

    private TypesREST typesREST;
    private MockedStatic<DynamicConfigStore> mockedDynamicConfig;

    @BeforeEach
    void setUp() throws Exception {
        typeDefStore = mock(AtlasTypeDefStore.class);
        redisService = mock(RedisService.class);
        typeCacheRefresher = mock(TypeCacheRefresher.class);
        asyncIngestionProducer = mock(AsyncIngestionProducer.class);

        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty("atlas.typedef.lock.name", "test-lock");

        typesREST = new TypesREST(typeDefStore, redisService, config, typeCacheRefresher, asyncIngestionProducer);

        // Mock lock acquisition
        Lock mockLock = mock(Lock.class);
        when(redisService.acquireDistributedLockV2(anyString())).thenReturn(mockLock);

        // Mock redis version
        when(redisService.getValue(anyString(), anyString())).thenReturn("1");

        mockedDynamicConfig = mockStatic(DynamicConfigStore.class);

        // Set up RequestContext
        RequestContext ctx = RequestContext.get();
        ctx.setTraceId("test-trace");
        ctx.setUser("admin", null);
        ctx.setUri("/api/atlas/v2/types/typedefs");
    }

    @AfterEach
    void tearDown() {
        mockedDynamicConfig.close();
        RequestContext.clear();
    }

    private AtlasTypesDef createEmptyTypesDef() {
        return new AtlasTypesDef();
    }

    // =================== Test 1: Create success + async enabled -> publishes TYPEDEF_CREATE ===================

    @Test
    void testCreate_success_asyncEnabled_publishesEvent() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();
        AtlasTypesDef result = createEmptyTypesDef();
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class))).thenReturn(result);

        typesREST.createAtlasTypeDefs(input, false);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.TYPEDEF_CREATE),
                anyMap(),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));
    }

    // =================== Test 2: Create success + async disabled -> no publish ===================

    @Test
    void testCreate_success_asyncDisabled_noPublish() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(false);
        AtlasTypesDef input = createEmptyTypesDef();
        AtlasTypesDef result = createEmptyTypesDef();
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class))).thenReturn(result);

        typesREST.createAtlasTypeDefs(input, false);

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== Test 3: Create failure -> no publish ===================

    @Test
    void testCreate_failure_noPublish() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class)))
                .thenThrow(new AtlasBaseException("create failed"));

        assertThrows(AtlasBaseException.class, () -> typesREST.createAtlasTypeDefs(input, false));

        verify(asyncIngestionProducer, never()).publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class));
    }

    // =================== Test 4: Update success -> publishes TYPEDEF_UPDATE ===================

    @Test
    void testUpdate_success_asyncEnabled_publishesEvent() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();
        AtlasTypesDef result = createEmptyTypesDef();
        when(typeDefStore.updateTypesDef(any(AtlasTypesDef.class))).thenReturn(result);

        typesREST.updateAtlasTypeDefs(input, false, false);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.TYPEDEF_UPDATE),
                anyMap(),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));
    }

    // =================== Test 5: Delete success -> publishes TYPEDEF_DELETE ===================

    @Test
    void testDelete_success_asyncEnabled_publishesEvent() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();

        typesREST.deleteAtlasTypeDefs(input);

        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.TYPEDEF_DELETE),
                anyMap(),
                eq(input),
                any(RequestMetadata.class));
    }

    // =================== Test 6: DeleteByName success -> publishes TYPEDEF_DELETE_BY_NAME ===================

    @SuppressWarnings("unchecked")
    @Test
    void testDeleteByName_success_asyncEnabled_publishesEvent() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);

        typesREST.deleteAtlasTypeByName("MyType");

        ArgumentCaptor<Map<String, Object>> payloadCaptor = ArgumentCaptor.forClass(Map.class);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.TYPEDEF_DELETE_BY_NAME),
                anyMap(),
                payloadCaptor.capture(),
                any(RequestMetadata.class));

        Map<String, Object> payload = payloadCaptor.getValue();
        assertEquals("MyType", payload.get("typeName"));
    }

    // =================== Test 7: Producer throws -> main flow still succeeds ===================

    @Test
    void testCreate_producerThrows_mainFlowSucceeds() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();
        AtlasTypesDef result = createEmptyTypesDef();
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class))).thenReturn(result);
        when(asyncIngestionProducer.publishEvent(anyString(), anyMap(), any(), any(RequestMetadata.class)))
                .thenThrow(new RuntimeException("Kafka down"));

        // Should NOT throw even though producer failed
        AtlasTypesDef returned = typesREST.createAtlasTypeDefs(input, false);
        assertNotNull(returned);
    }

    // =================== Test 8: Create passes allowDuplicateDisplayName in operationMetadata ===================

    @SuppressWarnings("unchecked")
    @Test
    void testCreate_operationMetadataContainsAllowDuplicateDisplayName() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();
        AtlasTypesDef result = createEmptyTypesDef();
        when(typeDefStore.createTypesDef(any(AtlasTypesDef.class))).thenReturn(result);

        typesREST.createAtlasTypeDefs(input, true);

        ArgumentCaptor<Map<String, Object>> opMetaCaptor = ArgumentCaptor.forClass(Map.class);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.TYPEDEF_CREATE),
                opMetaCaptor.capture(),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));

        Map<String, Object> opMeta = opMetaCaptor.getValue();
        assertEquals(true, opMeta.get("allowDuplicateDisplayName"));
    }

    // =================== Test 9: Update passes patch and allowDuplicateDisplayName ===================

    @SuppressWarnings("unchecked")
    @Test
    void testUpdate_operationMetadataContainsPatchAndAllowDuplicate() throws Exception {
        mockedDynamicConfig.when(DynamicConfigStore::isAsyncIngestionEnabled).thenReturn(true);
        AtlasTypesDef input = createEmptyTypesDef();
        AtlasTypesDef result = createEmptyTypesDef();
        when(typeDefStore.updateTypesDef(any(AtlasTypesDef.class))).thenReturn(result);

        typesREST.updateAtlasTypeDefs(input, true, true);

        ArgumentCaptor<Map<String, Object>> opMetaCaptor = ArgumentCaptor.forClass(Map.class);
        verify(asyncIngestionProducer).publishEvent(
                eq(AsyncIngestionEventType.TYPEDEF_UPDATE),
                opMetaCaptor.capture(),
                argThat(arg -> AtlasType.toJson(arg).equals(AtlasType.toJson(input))),
                any(RequestMetadata.class));

        Map<String, Object> opMeta = opMetaCaptor.getValue();
        assertEquals(true, opMeta.get("patch"));
        assertEquals(true, opMeta.get("allowDuplicateDisplayName"));
    }
}
