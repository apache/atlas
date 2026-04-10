package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityMutationPostProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TagDenormDLQReplayServiceTest {

    @Mock
    private AtlasEntityStore entityStore;

    @Mock
    private EntityMutationPostProcessor postProcessor;

    private TagDenormDLQReplayService service;
    private ObjectMapper objectMapper;

    private MockedStatic<ApplicationProperties> mockApplicationProperties;

    @BeforeEach
    void setUp() throws AtlasException {
        cleanupStaticMocks();

        mockApplicationProperties = mockStatic(ApplicationProperties.class);
        org.apache.commons.configuration.Configuration mockConfig = mock(org.apache.commons.configuration.Configuration.class);
        mockApplicationProperties.when(ApplicationProperties::get).thenReturn(mockConfig);
        when(mockConfig.getString("atlas.graph.kafka.bootstrap.servers")).thenReturn("localhost:9092");

        service = spy(new TagDenormDLQReplayService(entityStore, postProcessor));

        ReflectionTestUtils.setField(service, "dlqTopic", "TEST_TAG_DENORM_DLQ");
        ReflectionTestUtils.setField(service, "consumerGroupId", "test_tag_denorm_group");
        ReflectionTestUtils.setField(service, "enabled", true);
        ReflectionTestUtils.setField(service, "maxRetries", 3);
        ReflectionTestUtils.setField(service, "exponentialBackoffBaseDelayMs", 100);
        ReflectionTestUtils.setField(service, "exponentialBackoffMaxDelayMs", 1000);
        ReflectionTestUtils.setField(service, "exponentialBackoffMultiplier", 2.0);

        // Clear internal tracking maps for test isolation
        Map<String, ?> retryTracker = (Map<String, ?>) ReflectionTestUtils.getField(service, "retryTracker");
        Map<String, Long> backoffTracker = (Map<String, Long>) ReflectionTestUtils.getField(service, "backoffTracker");
        if (retryTracker != null) retryTracker.clear();
        if (backoffTracker != null) backoffTracker.clear();

        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void tearDown() {
        cleanupStaticMocks();
    }

    private void cleanupStaticMocks() {
        if (mockApplicationProperties != null) {
            try {
                mockApplicationProperties.close();
            } catch (Exception e) {
                // Ignore
            }
            mockApplicationProperties = null;
        }
    }

    // --- replayTagDenormDLQEntry tests ---

    @Test
    void testReplayEntry_Success() throws Exception {
        String dlqJson = createValidDLQMessage("guid-1", "guid-2", "guid-3");

        when(entityStore.repairClassificationMappingsV2(anyList())).thenReturn(Collections.emptyMap());

        ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson);

        verify(entityStore).repairClassificationMappingsV2(argThat(guids ->
                guids.size() == 3 && guids.containsAll(Arrays.asList("guid-1", "guid-2", "guid-3"))));
        verify(postProcessor, never()).executeESOperations(anyList());
    }

    @Test
    void testReplayEntry_SuccessWithDeferredOps() throws Exception {
        String dlqJson = createValidDLQMessage("guid-1");

        when(entityStore.repairClassificationMappingsV2(anyList())).thenAnswer(invocation -> {
            // Simulate repairClassificationMappingsV2 adding deferred ops
            ESDeferredOperation mockOp = mock(ESDeferredOperation.class);
            RequestContext.get().addESDeferredOperation(mockOp);
            return Collections.emptyMap();
        });

        ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson);

        verify(postProcessor).executeESOperations(anyList());
        // Verify deferred ops are cleared after processing
        assertTrue(RequestContext.get().getESDeferredOperations().isEmpty(),
                "Deferred ops should be cleared after processing");
    }

    @Test
    void testReplayEntry_InvalidJson() {
        String invalidJson = "{invalid json}";

        Exception thrown = assertThrows(Exception.class, () ->
                ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", invalidJson));

        assertNotNull(thrown);
    }

    @Test
    void testReplayEntry_WrongType() throws Exception {
        ObjectNode message = objectMapper.createObjectNode();
        message.put("type", "SOMETHING_ELSE");
        message.putObject("vertices").put("123", "guid-1");
        String dlqJson = objectMapper.writeValueAsString(message);

        ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson);

        verify(entityStore, never()).repairClassificationMappingsV2(anyList());
    }

    @Test
    void testReplayEntry_EmptyVertices() throws Exception {
        ObjectNode message = objectMapper.createObjectNode();
        message.put("type", "TAG_DENORM_SYNC");
        message.putObject("vertices");
        String dlqJson = objectMapper.writeValueAsString(message);

        ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson);

        verify(entityStore, never()).repairClassificationMappingsV2(anyList());
    }

    @Test
    void testReplayEntry_MissingVerticesField() throws Exception {
        ObjectNode message = objectMapper.createObjectNode();
        message.put("type", "TAG_DENORM_SYNC");
        String dlqJson = objectMapper.writeValueAsString(message);

        ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson);

        verify(entityStore, never()).repairClassificationMappingsV2(anyList());
    }

    @Test
    void testReplayEntry_MissingGuids() throws Exception {
        ObjectNode message = objectMapper.createObjectNode();
        message.put("type", "TAG_DENORM_SYNC");
        ObjectNode vertices = message.putObject("vertices");
        vertices.put("123", "");
        vertices.put("456", "");
        String dlqJson = objectMapper.writeValueAsString(message);

        ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson);

        verify(entityStore, never()).repairClassificationMappingsV2(anyList());
    }

    @Test
    void testReplayEntry_PartialFailure() throws Exception {
        String dlqJson = createValidDLQMessage("guid-1", "guid-2");

        Map<String, String> errors = new HashMap<>();
        errors.put("guid-2", "Cassandra read failed");
        when(entityStore.repairClassificationMappingsV2(anyList())).thenReturn(errors);

        Exception thrown = assertThrows(Exception.class, () ->
                ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson));

        Throwable cause = thrown.getCause();
        assertTrue(cause instanceof AtlasBaseException,
                "Expected AtlasBaseException but got: " + (cause != null ? cause.getClass() : "null"));

        // Verify deferred ops are still cleared even on failure
        assertTrue(RequestContext.get().getESDeferredOperations().isEmpty(),
                "Deferred ops should be cleared even on partial failure");
    }

    @Test
    void testReplayEntry_RepairThrows() throws Exception {
        String dlqJson = createValidDLQMessage("guid-1");

        when(entityStore.repairClassificationMappingsV2(anyList()))
                .thenThrow(new AtlasBaseException("Cassandra down"));

        Exception thrown = assertThrows(Exception.class, () ->
                ReflectionTestUtils.invokeMethod(service, "replayTagDenormDLQEntry", dlqJson));

        assertNotNull(thrown);

        // Verify deferred ops are still cleared even when repair throws
        assertTrue(RequestContext.get().getESDeferredOperations().isEmpty(),
                "Deferred ops should be cleared even when repair throws");
    }

    // --- Exponential backoff tests ---

    @Test
    void testExponentialBackoff_Progression() {
        String retryKey = "0-100";

        long delay1 = ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        assertEquals(100, delay1, "First delay should be base delay");

        long delay2 = ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        assertEquals(200, delay2, "Second delay should be 100 * 2.0");

        long delay3 = ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        assertEquals(400, delay3, "Third delay should be 200 * 2.0");

        long delay4 = ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        assertEquals(800, delay4, "Fourth delay should be 400 * 2.0");
    }

    @Test
    void testExponentialBackoff_CappedAtMax() {
        String retryKey = "0-100";

        for (int i = 0; i < 10; i++) {
            ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        }

        long finalDelay = ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);

        assertEquals(1000, finalDelay, "Delay should be capped at maxDelayMs (1000)");
    }

    @Test
    void testExponentialBackoff_ResetOnSuccess() {
        String retryKey = "0-100";

        // Build up delay
        ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);

        // Reset
        ReflectionTestUtils.invokeMethod(service, "resetExponentialBackoff", retryKey);

        // Should be back to base delay
        long delayAfterReset = ReflectionTestUtils.invokeMethod(service, "calculateExponentialBackoff", retryKey);
        assertEquals(100, delayAfterReset, "After reset, delay should return to base delay");
    }

    // --- Status & health tests ---

    @Test
    void testGetStatus_AllFields() {
        ReflectionTestUtils.setField(service, "processedCount", new AtomicInteger(100));
        ReflectionTestUtils.setField(service, "errorCount", new AtomicInteger(5));
        ReflectionTestUtils.setField(service, "skippedCount", new AtomicInteger(2));

        Map<String, Object> status = service.getStatus();

        assertEquals(true, status.get("enabled"));
        assertEquals(100, status.get("processedCount"));
        assertEquals(5, status.get("errorCount"));
        assertEquals(2, status.get("skippedCount"));
        assertEquals("TEST_TAG_DENORM_DLQ", status.get("topic"));
        assertEquals("test_tag_denorm_group", status.get("consumerGroup"));
        assertEquals(3, status.get("maxRetries"));

        @SuppressWarnings("unchecked")
        Map<String, Object> backoffConfig = (Map<String, Object>) status.get("exponentialBackoffConfig");
        assertNotNull(backoffConfig);
        assertEquals(100, backoffConfig.get("baseDelayMs"));
        assertEquals(1000, backoffConfig.get("maxDelayMs"));
        assertEquals(2.0, backoffConfig.get("multiplier"));
    }

    @Test
    void testIsHealthy_WhenHealthy() {
        ReflectionTestUtils.setField(service, "isHealthy", new AtomicBoolean(true));

        Thread mockThread = mock(Thread.class);
        when(mockThread.isAlive()).thenReturn(true);
        ReflectionTestUtils.setField(service, "replayThread", mockThread);

        assertTrue(service.isHealthy(), "Service should be healthy when flag is true and thread is alive");
    }

    @Test
    void testIsHealthy_WhenUnhealthy() {
        ReflectionTestUtils.setField(service, "isHealthy", new AtomicBoolean(false));

        Thread mockThread = mock(Thread.class);
        when(mockThread.isAlive()).thenReturn(true);
        ReflectionTestUtils.setField(service, "replayThread", mockThread);

        assertFalse(service.isHealthy(), "Service should be unhealthy when flag is false");
    }

    @Test
    void testIsHealthy_WhenThreadDead() {
        ReflectionTestUtils.setField(service, "isHealthy", new AtomicBoolean(true));

        Thread mockThread = mock(Thread.class);
        when(mockThread.isAlive()).thenReturn(false);
        ReflectionTestUtils.setField(service, "replayThread", mockThread);

        assertFalse(service.isHealthy(), "Service should be unhealthy when thread is dead");
    }

    @Test
    void testIsHealthy_WhenThreadNull() {
        ReflectionTestUtils.setField(service, "isHealthy", new AtomicBoolean(true));
        ReflectionTestUtils.setField(service, "replayThread", null);

        assertFalse(service.isHealthy(), "Service should be unhealthy when thread is null");
    }

    // --- Helper methods ---

    private String createValidDLQMessage(String... guids) throws Exception {
        ObjectNode message = objectMapper.createObjectNode();
        message.put("type", "TAG_DENORM_SYNC");
        message.put("timestamp", System.currentTimeMillis());

        ObjectNode vertices = message.putObject("vertices");
        int vertexId = 1000;
        for (String guid : guids) {
            vertices.put(String.valueOf(vertexId++), guid);
        }

        return objectMapper.writeValueAsString(message);
    }
}
