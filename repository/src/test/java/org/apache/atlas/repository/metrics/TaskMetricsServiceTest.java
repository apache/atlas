package org.apache.atlas.repository.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskMetricsServiceTest {
    
    private TaskMetricsService metricsService;
    private MeterRegistry meterRegistry;
    
    @BeforeEach
    void setup() {
        meterRegistry = new SimpleMeterRegistry();
        metricsService = new TaskMetricsService(meterRegistry);
    }
    
    @Test
    void testTaskStartMetrics() {
        // Given
        String taskType = "CLASSIFICATION_PROPAGATION_ADD";
        String version = "v2";
        String tenant = "test-tenant";
        
        // When
        metricsService.recordTaskStart(taskType, version, tenant);
        
        // Then
        assertEquals(1.0, meterRegistry.get("atlas.classification.tasks.total")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .counter().count(), 0.01);
                
        assertEquals(1, meterRegistry.get("atlas.classification.tasks.in_progress")
                .gauge().value(), 0.01);
    }
    
    @Test
    void testTaskEndMetrics() {
        // Given
        String taskType = "CLASSIFICATION_PROPAGATION_ADD";
        String version = "v2";
        String tenant = "test-tenant";
        int assetsAffected = 100;
        
        // When
        metricsService.recordTaskEnd(taskType, version, tenant, 1000, assetsAffected, true);
        
        // Then
        assertEquals(100.0, meterRegistry.get("atlas.classification.assets.affected.total")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .tag("status", "success")
                .counter().count(), 0.01);
                
        assertEquals(1.0, meterRegistry.get("atlas.classification.tasks.status")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .tag("status", "success")
                .counter().count(), 0.01);
                
        assertTrue(meterRegistry.get("atlas.classification.task.duration")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .tag("status", "success")
                .timer().count() > 0);
    }
    
    @Test
    void testErrorMetrics() {
        // Given
        String taskType = "CLASSIFICATION_PROPAGATION_ADD";
        String version = "v2";
        String tenant = "test-tenant";
        String errorType = "ValidationError";
        
        // When
        metricsService.recordTaskError(taskType, version, tenant, errorType);
        
        // Then
        assertEquals(1.0, meterRegistry.get("atlas.classification.tasks.errors.total")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .tag("error", errorType)
                .counter().count(), 0.01);
    }
    
    @Test
    void testQueueSizeMetrics() {
        // When
        metricsService.updateQueueSize(5);
        
        // Then
        assertEquals(5, meterRegistry.get("atlas.classification.tasks.queue.size")
                .gauge().value(), 0.01);
    }
    
    @Test
    void testTaskEndMetricsFailure() {
        // Given
        String taskType = "CLASSIFICATION_PROPAGATION_ADD";
        String version = "v2";
        String tenant = "test-tenant";
        int assetsAffected = 0;
        
        // When
        metricsService.recordTaskEnd(taskType, version, tenant, 1000, assetsAffected, false);
        
        // Then
        assertEquals(1.0, meterRegistry.get("atlas.classification.tasks.status")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .tag("status", "failure")
                .counter().count(), 0.01);
                
        assertTrue(meterRegistry.get("atlas.classification.task.duration")
                .tag("type", taskType)
                .tag("version", version)
                .tag("tenant", tenant)
                .tag("status", "failure")
                .timer().count() > 0);
    }
} 