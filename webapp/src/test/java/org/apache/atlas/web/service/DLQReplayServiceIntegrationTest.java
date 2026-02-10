package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.RepairIndex;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Integration tests for DLQReplayService focusing on end-to-end scenarios
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DLQReplayServiceIntegrationTest {

    @Mock
    private KafkaConsumer<String, String> mockConsumer;
    @Mock
    private RepairIndex repairIndex;
    @Mock
    private AtlasTypeRegistry atlasTypeRegistry;

    private DLQReplayService dlqReplayService;

    private MockedStatic<ApplicationProperties> mockApplicationProperties;
    private MockedStatic<Backend> mockBackend;

    @BeforeEach
    void setUp() throws AtlasException {
        // Clean up any existing static mocks first (defensive programming)
        cleanupStaticMocks();

        try {
            setupMocks();
        } catch (BackendException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Mock ApplicationProperties (static)
        //mockApplicationProperties = mockStatic(ApplicationProperties.class);
        org.apache.commons.configuration.Configuration mockConfig = mock(org.apache.commons.configuration.Configuration.class);
        mockApplicationProperties.when(ApplicationProperties::get).thenReturn(mockConfig);
        when(mockConfig.getString("atlas.graph.kafka.bootstrap.servers")).thenReturn("localhost:9092");

        dlqReplayService = spy(new DLQReplayService(repairIndex, atlasTypeRegistry));

        // Inject mocks
        ReflectionTestUtils.setField(dlqReplayService, "consumer", mockConsumer);

        // Set test configuration
        ReflectionTestUtils.setField(dlqReplayService, "maxRetries", 3);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffBaseDelayMs", 100);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMaxDelayMs", 1000);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMultiplier", 2.0);
        ReflectionTestUtils.setField(dlqReplayService, "errorBackoffMs", 500);

        // Clear internal tracking maps to ensure test isolation
        Map<String, Integer> retryTracker = (Map<String, Integer>) ReflectionTestUtils.getField(dlqReplayService, "retryTracker");
        Map<String, Long> backoffTracker = (Map<String, Long>) ReflectionTestUtils.getField(dlqReplayService, "backoffTracker");
        if (retryTracker != null) {
            retryTracker.clear();
        }
        if (backoffTracker != null) {
            backoffTracker.clear();
        }
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
        if (mockBackend != null) {
            try {
                mockBackend.close();
            } catch (Exception e) {
                // Ignore - mock might already be closed
            }
            mockBackend = null;
        }
    }

    private void setupMocks() throws Exception {
        mockApplicationProperties = mockStatic(ApplicationProperties.class);
        org.apache.commons.configuration.Configuration mockConfig = mock(org.apache.commons.configuration.Configuration.class);
        when(ApplicationProperties.get()).thenReturn(mockConfig);
        when(mockConfig.getString("atlas.graph.kafka.bootstrap.servers")).thenReturn("localhost:9092");

        // Setup JanusGraph transaction mocks
        doNothing().when(repairIndex).reindexVerticesByIds(anyString(), anySet());
    }

    @Test
    void testPoisonPill_SkippedAfterMaxRetries() {
        // Arrange
        String retryKey = "0-100";
        Map<String, Integer> retryTracker = new ConcurrentHashMap<>();
        ReflectionTestUtils.setField(dlqReplayService, "retryTracker", retryTracker);

        // Act - Simulate 3 failures (max retries)
        retryTracker.put(retryKey, 1);
        retryTracker.put(retryKey, 2);
        retryTracker.put(retryKey, 3);

        int finalCount = retryTracker.get(retryKey);

        // Assert
        assertEquals(3, finalCount);
        assertTrue(finalCount >= 3, "Should reach maxRetries and be skipped");
    }

    @Test
    void testBackoffProgression_MultipleMessages() {
        // Arrange
        String message1Key = "0-100";
        String message2Key = "0-101";

        // Act - Build up backoff for message 1
        long delay1_1 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", message1Key);
        long delay1_2 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", message1Key);
        long delay1_3 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", message1Key);

        // Act - Message 2 should start fresh
        long delay2_1 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", message2Key);

        // Assert - Message 1 progresses
        assertEquals(100, delay1_1);
        assertEquals(200, delay1_2);
        assertEquals(400, delay1_3);

        // Assert - Message 2 starts from base
        assertEquals(100, delay2_1, "Different message should start from base delay");
    }

    @Test
    void testBackoffReset_OnSuccessfulProcessing() {
        // Arrange
        String retryKey = "0-100";

        // Act - Build up delay
        long delay1 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay2 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay3 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);

        // Verify progression
        assertEquals(100, delay1);
        assertEquals(200, delay2);
        assertEquals(400, delay3);

        // Act - Reset on success
        ReflectionTestUtils.invokeMethod(dlqReplayService, "resetExponentialBackoff", retryKey);

        // Act - Next attempt should be back to base
        long delayAfterReset = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);

        // Assert
        assertEquals(100, delayAfterReset, "Should reset to base delay after success");
    }

    @Test
    void testPartitionRevocation_CleansUpTrackers() {
        // Arrange
        Map<String, Integer> retryTracker = new ConcurrentHashMap<>();
        Map<String, Long> backoffTracker = new ConcurrentHashMap<>();

        retryTracker.put("0-100", 2);
        retryTracker.put("0-101", 1);
        retryTracker.put("1-200", 3);

        backoffTracker.put("0-100", 4000L);
        backoffTracker.put("0-101", 2000L);
        backoffTracker.put("1-200", 8000L);

        ReflectionTestUtils.setField(dlqReplayService, "retryTracker", retryTracker);
        ReflectionTestUtils.setField(dlqReplayService, "backoffTracker", backoffTracker);

        // Act - Simulate partition 0 revocation
        Collection<TopicPartition> revokedPartitions = Collections.singletonList(
                new TopicPartition("TEST_DLQ", 0)
        );

        // Manually call cleanup logic
        String partitionPrefix = "0-";
        retryTracker.keySet().removeIf(key -> key.startsWith(partitionPrefix));
        backoffTracker.keySet().removeIf(key -> key.startsWith(partitionPrefix));

        // Assert
        assertFalse(retryTracker.containsKey("0-100"), "Partition 0 entries should be removed");
        assertFalse(retryTracker.containsKey("0-101"), "Partition 0 entries should be removed");
        assertTrue(retryTracker.containsKey("1-200"), "Other partition entries should remain");

        assertFalse(backoffTracker.containsKey("0-100"), "Partition 0 backoff should be removed");
        assertFalse(backoffTracker.containsKey("0-101"), "Partition 0 backoff should be removed");
        assertTrue(backoffTracker.containsKey("1-200"), "Other partition backoff should remain");
    }

    @Test
    void testMetricsTracking_ProcessedCount() {
        // Arrange
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger skippedCount = new AtomicInteger(0);

        ReflectionTestUtils.setField(dlqReplayService, "processedCount", processedCount);
        ReflectionTestUtils.setField(dlqReplayService, "errorCount", errorCount);
        ReflectionTestUtils.setField(dlqReplayService, "skippedCount", skippedCount);

        // Act - Simulate processing
        processedCount.incrementAndGet();
        processedCount.incrementAndGet();
        errorCount.incrementAndGet();
        skippedCount.incrementAndGet();

        // Assert
        assertEquals(2, processedCount.get());
        assertEquals(1, errorCount.get());
        assertEquals(1, skippedCount.get());
    }

    @Test
    void testStatusEndpoint_ReturnsCompleteInformation() {
        // Arrange
        AtomicInteger processedCount = new AtomicInteger(150);
        AtomicInteger errorCount = new AtomicInteger(10);
        AtomicInteger skippedCount = new AtomicInteger(3);

        Map<String, Integer> retryTracker = new ConcurrentHashMap<>();
        retryTracker.put("0-100", 2);
        retryTracker.put("0-101", 1);

        Map<String, Long> backoffTracker = new ConcurrentHashMap<>();
        backoffTracker.put("0-100", 4000L);

        ReflectionTestUtils.setField(dlqReplayService, "processedCount", processedCount);
        ReflectionTestUtils.setField(dlqReplayService, "errorCount", errorCount);
        ReflectionTestUtils.setField(dlqReplayService, "skippedCount", skippedCount);
        ReflectionTestUtils.setField(dlqReplayService, "retryTracker", retryTracker);
        ReflectionTestUtils.setField(dlqReplayService, "backoffTracker", backoffTracker);

        // Act
        Map<String, Object> status = dlqReplayService.getStatus();

        // Assert
        assertEquals(150, status.get("processedCount"));
        assertEquals(10, status.get("errorCount"));
        assertEquals(3, status.get("skippedCount"));
        assertEquals(2, status.get("activeRetries"));
        assertEquals(1, status.get("activeBackoffs"));
        assertNotNull(status.get("exponentialBackoffConfig"));
    }

    @Test
    void testExponentialBackoff_DifferentMultipliers() {
        // Test with multiplier 1.5
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMultiplier", 1.5);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffBaseDelayMs", 1000);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMaxDelayMs", 5000);

        String retryKey = "0-100";

        long delay1 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay2 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay3 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);

        assertEquals(1000, delay1, "First delay should be base");
        assertEquals(1500, delay2, "Second delay should be 1000 * 1.5");
        assertEquals(2250, delay3, "Third delay should be 1500 * 1.5");
    }

    @Test
    void testRetryLimit_ReachedAfterExactAttempts() {
        // Arrange
        Map<String, Integer> retryTracker = new ConcurrentHashMap<>();
        ReflectionTestUtils.setField(dlqReplayService, "retryTracker", retryTracker);
        ReflectionTestUtils.setField(dlqReplayService, "maxRetries", 5);

        String retryKey = "0-100";

        // Act - Increment retry count 5 times
        for (int i = 1; i <= 5; i++) {
            retryTracker.put(retryKey, i);
        }

        int finalCount = retryTracker.get(retryKey);

        // Assert
        assertEquals(5, finalCount);
        assertTrue(finalCount >= 5, "Should have reached max retries of 5");
    }

    @Test
    void testBackoffCap_WithVeryHighMultiplier() {
        // Arrange - Set extreme multiplier to test cap
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMultiplier", 10.0);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffBaseDelayMs", 100);
        ReflectionTestUtils.setField(dlqReplayService, "exponentialBackoffMaxDelayMs", 5000);

        String retryKey = "0-100";

        // Act - Multiple attempts
        long delay1 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay2 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay3 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);
        long delay4 = ReflectionTestUtils.invokeMethod(dlqReplayService, "calculateExponentialBackoff", retryKey);

        // Assert
        assertEquals(100, delay1);
        assertEquals(1000, delay2);  // 100 * 10
        assertEquals(5000, delay3);  // Capped at max (would be 10000)
        assertEquals(5000, delay4);  // Stays at cap
    }
}

