package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.util.RepairIndex;
import org.janusgraph.diskstorage.dlq.DLQEntry;
import org.janusgraph.diskstorage.dlq.SerializableIndexMutation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DLQReplayServiceTest {

    @Mock
    private RepairIndex repairIndex;

    private DLQReplayService dlqReplayService;
    private ObjectMapper objectMapper;
    
    private MockedStatic<ApplicationProperties> mockApplicationProperties;

    @BeforeEach
    void setUp() throws AtlasException {
        // Clean up any existing static mocks first (defensive programming)
        cleanupStaticMocks();
        
        // Mock ApplicationProperties (static)
        mockApplicationProperties = mockStatic(ApplicationProperties.class);
        org.apache.commons.configuration.Configuration mockConfig = mock(org.apache.commons.configuration.Configuration.class);
        mockApplicationProperties.when(ApplicationProperties::get).thenReturn(mockConfig);
        when(mockConfig.getString("atlas.graph.kafka.bootstrap.servers")).thenReturn("localhost:9092");

        // Create service - now it will use mocked esIndex
        dlqReplayService = spy(new DLQReplayService(repairIndex));
        
        // Set test configuration values
        ReflectionTestUtils.setField(dlqReplayService, "dlqTopic", "TEST_DLQ");
        ReflectionTestUtils.setField(dlqReplayService, "consumerGroupId", "test_group");
        ReflectionTestUtils.setField(dlqReplayService, "maxRetries", 3);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffBaseDelayMs", 100);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMaxDelayMs", 1000);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMultiplier", 2.0);
        
        // Clear internal tracking maps to ensure test isolation
        Map<String, Integer> retryTracker = (Map<String, Integer>) ReflectionTestUtils.getField(dlqReplayService, "retryTracker");
        Map<String, Long> backoffTracker = (Map<String, Long>) ReflectionTestUtils.getField(dlqReplayService, "backoffTracker");
        if (retryTracker != null) {
            retryTracker.clear();
        }
        if (backoffTracker != null) {
            backoffTracker.clear();
        }
        
        objectMapper = new ObjectMapper();
    }
    
    @AfterEach
    void tearDown() {
        cleanupStaticMocks();
    }
    
    private void cleanupStaticMocks() {
        // Close static mocks to prevent "already registered" errors
        if (mockApplicationProperties != null) {
            try {
                mockApplicationProperties.close();
            } catch (Exception e) {
                // Ignore - mock might already be closed
            }
            mockApplicationProperties = null;
        }
    }

    @Test
    void testReplayDLQEntry_Success() throws Exception {
        // Arrange
        String dlqJson = createValidDLQJson();
        
        doNothing().when(repairIndex).reindexVerticesByIds(anyString(), anySet());

        // Act
        ReflectionTestUtils.invokeMethod(dlqReplayService, "replayDLQEntry", dlqJson);
        
        // Assert
        verify(repairIndex).reindexVerticesByIds(anyString(), anySet());
    }

    @Test
    void testReplayDLQEntry_JsonParsingError() throws Exception {
        // Arrange
        String invalidJson = "{invalid json}";
        
        // Act & Assert
        // ReflectionTestUtils wraps exceptions in UndeclaredThrowableException, so we need to unwrap
        Exception thrown = assertThrows(Exception.class, () -> {
            ReflectionTestUtils.invokeMethod(dlqReplayService, "replayDLQEntry", invalidJson);
        });
        
        // Unwrap the exception
        Throwable cause = thrown.getCause();
        assertTrue(cause instanceof IOException, 
            "Expected IOException but got: " + (cause != null ? cause.getClass() : "null"));
        
        verify(repairIndex, never()).reindexVerticesByIds(anyString(), anySet());
    }

    @Test
    void testExponentialBackoff_Progression() {
        // Arrange
        String retryKey = "0-100";
        
        // Act & Assert - Should follow exponential progression
        long delay1 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        assertEquals(100, delay1, "First delay should be base delay");
        
        long delay2 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        assertEquals(200, delay2, "Second delay should be 100 * 2.0");
        
        long delay3 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        assertEquals(400, delay3, "Third delay should be 200 * 2.0");
        
        long delay4 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        assertEquals(800, delay4, "Fourth delay should be 400 * 2.0");
    }

    @Test
    void testExponentialBackoff_CappedAtMax() {
        // Arrange
        String retryKey = "0-100";
        
        // Act - Call enough times to exceed max
        for (int i = 0; i < 10; i++) {
            ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        }
        
        long finalDelay = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        
        // Assert
        assertEquals(1000, finalDelay, "Delay should be capped at maxDelayMs (1000)");
    }

    @Test
    void testExponentialBackoff_ResetOnSuccess() {
        // Arrange
        String retryKey = "0-100";
        
        // Build up delay
        ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        
        // Act - Reset
        ReflectionTestUtils.invokeMethod(dlqReplayService, "resetExponentialBackoff", retryKey);
        
        // Assert - Should be back to base delay
        long delayAfterReset = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        assertEquals(100, delayAfterReset, "After reset, delay should return to base delay");
    }

    @Test
    void testRetryTracking_IncrementAndSkip() {
        // Arrange
        Map<String, Integer> retryTracker = new HashMap<>();
        ReflectionTestUtils.setField(dlqReplayService, "retryTracker", retryTracker);
        String retryKey = "0-100";
        
        // Act - Simulate 3 failures
        retryTracker.put(retryKey, 1);
        int count1 = retryTracker.get(retryKey);
        
        retryTracker.put(retryKey, 2);
        int count2 = retryTracker.get(retryKey);
        
        retryTracker.put(retryKey, 3);
        int count3 = retryTracker.get(retryKey);
        
        // Assert
        assertEquals(1, count1);
        assertEquals(2, count2);
        assertEquals(3, count3);
        assertTrue(count3 >= 3, "Should reach maxRetries");
    }

    @Test
    void testGetStatus_AllFields() {
        // Arrange
        ReflectionTestUtils.setField(dlqReplayService, "processedCount", 
            new java.util.concurrent.atomic.AtomicInteger(100));
        ReflectionTestUtils.setField(dlqReplayService, "errorCount", 
            new java.util.concurrent.atomic.AtomicInteger(5));
        ReflectionTestUtils.setField(dlqReplayService, "skippedCount", 
            new java.util.concurrent.atomic.AtomicInteger(2));
        
        Map<String, Integer> retryTracker = new HashMap<>();
        retryTracker.put("0-100", 2);
        ReflectionTestUtils.setField(dlqReplayService, "retryTracker", retryTracker);
        
        Map<String, Long> backoffTracker = new HashMap<>();
        backoffTracker.put("0-100", 4000L);
        ReflectionTestUtils.setField(dlqReplayService, "backoffTracker", backoffTracker);
        
        // Act
        Map<String, Object> status = dlqReplayService.getStatus();
        
        // Assert
        assertEquals(100, status.get("processedCount"));
        assertEquals(5, status.get("errorCount"));
        assertEquals(2, status.get("skippedCount"));
        assertEquals(1, status.get("activeRetries"));
        assertEquals(1, status.get("activeBackoffs"));
        assertEquals("TEST_DLQ", status.get("topic"));
        assertEquals("test_group", status.get("consumerGroup"));
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
        // Arrange
        ReflectionTestUtils.setField(dlqReplayService, "isHealthy", 
            new java.util.concurrent.atomic.AtomicBoolean(true));
        
        Thread mockThread = mock(Thread.class);
        when(mockThread.isAlive()).thenReturn(true);
        ReflectionTestUtils.setField(dlqReplayService, "replayThread", mockThread);
        
        // Act
        boolean healthy = dlqReplayService.isHealthy();
        
        // Assert
        assertTrue(healthy, "Service should be healthy when flag is true and thread is alive");
    }

    @Test
    void testIsHealthy_WhenUnhealthy() {
        // Arrange
        ReflectionTestUtils.setField(dlqReplayService, "isHealthy", 
            new java.util.concurrent.atomic.AtomicBoolean(false));
        
        Thread mockThread = mock(Thread.class);
        when(mockThread.isAlive()).thenReturn(true);
        ReflectionTestUtils.setField(dlqReplayService, "replayThread", mockThread);
        
        // Act
        boolean healthy = dlqReplayService.isHealthy();
        
        // Assert
        assertFalse(healthy, "Service should be unhealthy when flag is false");
    }

    @Test
    void testIsHealthy_WhenThreadDead() {
        // Arrange
        ReflectionTestUtils.setField(dlqReplayService, "isHealthy", 
            new java.util.concurrent.atomic.AtomicBoolean(true));
        
        Thread mockThread = mock(Thread.class);
        when(mockThread.isAlive()).thenReturn(false);
        ReflectionTestUtils.setField(dlqReplayService, "replayThread", mockThread);
        
        // Act
        boolean healthy = dlqReplayService.isHealthy();
        
        // Assert
        assertFalse(healthy, "Service should be unhealthy when thread is dead");
    }

    // Helper methods

    private String createValidDLQJson() throws Exception {
        DLQEntry entry = createDLQEntry();
        return objectMapper.writeValueAsString(entry);
    }

    private DLQEntry createDLQEntry() {
        Map<String, Map<String, SerializableIndexMutation>> mutations = new HashMap<>();
        Map<String, SerializableIndexMutation> storeMutations = new HashMap<>();
        
        List<SerializableIndexMutation.SerializableIndexEntry> additions = new ArrayList<>();
        additions.add(new SerializableIndexMutation.SerializableIndexEntry("field1", "value1"));
        
        List<SerializableIndexMutation.SerializableIndexEntry> deletions = new ArrayList<>();
        
        SerializableIndexMutation mutation = new SerializableIndexMutation(
            true, false, additions, deletions
        );
        
        storeMutations.put("doc1", mutation);
        mutations.put("vertex_index", storeMutations);
        
        return new DLQEntry("janusgraph", "vertex_index", mutations, "", null, 0L, null);
    }
}

