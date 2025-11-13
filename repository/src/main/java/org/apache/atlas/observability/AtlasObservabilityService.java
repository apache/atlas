package org.apache.atlas.observability;

import io.micrometer.core.instrument.*;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.springframework.stereotype.Service;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.atlas.repository.store.graph.v2.utils.MDCScope;

@Service
public class AtlasObservabilityService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV2.class);
    private static final String METRIC_COMPONENT = "atlas_observability_bulk";
    private static final String UNKNOWN_CLIENT_ORIGIN = "unknown";
    private final MeterRegistry meterRegistry;

    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> distributionSummaryCache = new ConcurrentHashMap<>();
    
    // Gauges for real-time monitoring
    private final AtomicInteger operationsInProgress = new AtomicInteger(0);
    private final AtomicInteger totalOperations = new AtomicInteger(0);
    
    @Inject
    public AtlasObservabilityService() {
        this(getMeterRegistry());
    }
    
    // Constructor for testing
    AtlasObservabilityService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize gauges for real-time monitoring
        Gauge.builder(METRIC_COMPONENT + "_operations_in_progress", operationsInProgress, AtomicInteger::get)
                .description("Current number of Atlas operations in progress")
                .tag("component", "observability")
                .register(meterRegistry);

        Gauge.builder(METRIC_COMPONENT + "_total_operations", totalOperations, AtomicInteger::get)
                .description("Total number of Atlas operations processed")
                .tag("component", "observability")
                .register(meterRegistry);
    }
    
    private String getMetricKey(String metricName, String... tags) {
        StringBuilder key = new StringBuilder(metricName);
        for (String tag : tags) {
            key.append(":").append(tag);
        }
        return key.toString();
    }
    
    /**
     * Normalizes client origin to prevent null values in Micrometer tags.
     * Micrometer tags cannot have null values.
     */
    private String normalizeClientOrigin(String clientOrigin) {
        return clientOrigin != null ? clientOrigin : UNKNOWN_CLIENT_ORIGIN;
    }
    
    public void recordCreateOrUpdateDuration(AtlasObservabilityData data) {
        Timer timer = getOrCreateTimer("duration", normalizeClientOrigin(data.getXAtlanClientOrigin()));
        timer.record(data.getDuration(), TimeUnit.MILLISECONDS);
    }
    
    public void recordPayloadSize(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("payload_size", normalizeClientOrigin(data.getXAtlanClientOrigin()));
        summary.record(data.getPayloadAssetSize());
    }
    
    public void recordPayloadBytes(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("payload_bytes", normalizeClientOrigin(data.getXAtlanClientOrigin()));
        summary.record(data.getPayloadRequestBytes());
    }
    
    public void recordArrayRelationships(AtlasObservabilityData data) {
        String clientOrigin = normalizeClientOrigin(data.getXAtlanClientOrigin());
        // Record total count
        DistributionSummary summary = getOrCreateDistributionSummary("array_relationships", clientOrigin);
        summary.record(data.getTotalArrayRelationships());
        
        // Record individual relationship types and counts
        recordRelationshipMap("relationship_attributes", data.getRelationshipAttributes(), clientOrigin);
        recordRelationshipMap("append_relationship_attributes", data.getAppendRelationshipAttributes(), clientOrigin);
        recordRelationshipMap("remove_relationship_attributes", data.getRemoveRelationshipAttributes(), clientOrigin);
    }
    
    public void recordArrayAttributes(AtlasObservabilityData data) {
        String clientOrigin = normalizeClientOrigin(data.getXAtlanClientOrigin());
        // Record total count
        DistributionSummary summary = getOrCreateDistributionSummary("array_attributes", clientOrigin);
        summary.record(data.getTotalArrayAttributes());
        // Record individual attribute types and counts
        if (data.getArrayAttributes() != null && !data.getArrayAttributes().isEmpty()) {
            for (Map.Entry<String, Integer> entry : data.getArrayAttributes().entrySet()) {
                Counter counter = getOrCreateCounter("attributes", clientOrigin,
                    "attribute_name", entry.getKey());
                counter.increment(entry.getValue());
            }
        }
    }
    
    private void recordRelationshipMap(String metricName, Map<String, Integer> relationshipMap, String clientOrigin) {
        if (relationshipMap != null && !relationshipMap.isEmpty()) {
            for (Map.Entry<String, Integer> entry : relationshipMap.entrySet()) {
                Counter counter = getOrCreateCounter(metricName, clientOrigin,
                    "relationship_name", entry.getKey());
                counter.increment(entry.getValue());
            }
        }
    }
    
    
    
    public void recordTimingMetrics(AtlasObservabilityData data) {
        String clientOrigin = normalizeClientOrigin(data.getXAtlanClientOrigin());
        recordTimingMetric("diff_calc", data.getDiffCalcTime(), clientOrigin);
        recordTimingMetric("lineage_calc", data.getLineageCalcTime(), clientOrigin);
        recordTimingMetric("validation", data.getValidationTime(), clientOrigin);
        recordTimingMetric("ingestion", data.getIngestionTime(), clientOrigin);
    }
    
    private void recordTimingMetric(String operation, long durationMs, String clientOrigin) {
        Timer timer = getOrCreateTimer(operation + "_time", clientOrigin);
        timer.record(durationMs, TimeUnit.MILLISECONDS);
    }
    
    public void recordOperationCount(String operation, String status) {
        Counter counter = getOrCreateCounter("operations", UNKNOWN_CLIENT_ORIGIN, 
            "operation", operation, "status", status);
        counter.increment();
        // Note: totalOperations is incremented by recordOperationEnd/recordOperationFailure, not here
    }
    
    /**
     * Record operation start - increments operations in progress
     */
    public void recordOperationStart(String operation) {
        operationsInProgress.incrementAndGet();
    }
    
    /**
     * Record operation end - decrements operations in progress and increments total
     */
    public void recordOperationEnd(String operation, String status) {
        operationsInProgress.decrementAndGet();
        totalOperations.incrementAndGet();
        recordOperationCount(operation, status);
    }
    
    /**
     * Record operation failure - decrements operations in progress and records failure
     */
    public void recordOperationFailure(String operation, String errorType) {
        operationsInProgress.decrementAndGet();
        totalOperations.incrementAndGet();
        recordOperationCount(operation, "failure");
        
        // Record specific error type
        Counter errorCounter = getOrCreateCounter("operation_errors", UNKNOWN_CLIENT_ORIGIN,
            "operation", operation, "error_type", errorType);
        errorCounter.increment();
    }
    
    /**
     * Log detailed observability data for error cases only.
     * This includes high-cardinality fields like traceId, vertexIds, assetGuids
     * that should NOT be sent to Prometheus.
     */
    public void logErrorDetails(AtlasObservabilityData data, String errorMessage, Throwable throwable) {
        // Use MDCScope for proper MDC management - automatically restores previous state
        try (MDCScope scope = MDCScope.of("filter", "atlas-observability")) {
            // Log structured data for debugging - goes to ClickHouse
            // This includes traceId, vertexIds, assetGuids for error correlation
            LOG.error("Atlas createOrUpdate error: {} | traceId: {} | assetGuids: {} | vertexIds: {} | error: {}", 
                errorMessage,
                data.getTraceId(),
                data.getAssetGuids(),
                data.getVertexIds(),
                throwable != null ? throwable.getMessage() : "unknown");
        }
    }
    
    private Timer getOrCreateTimer(String metricName, String clientOrigin, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 1];
            allTags[0] = normalizedClientOrigin;
            System.arraycopy(additionalTags, 0, allTags, 1, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin);
        }
        
        return timerCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }
            
            return Timer.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability timing metric")
                    .tags(tags)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Key percentiles
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                        // Fast operations (milliseconds)
                        Duration.ofMillis(100),      // 100ms
                        Duration.ofMillis(500),      // 500ms
                        Duration.ofMillis(1000),     // 1s
                        // Medium operations (seconds)
                        Duration.ofSeconds(5),       // 5s
                        Duration.ofSeconds(10),      // 10s
                        Duration.ofSeconds(30),      // 30s
                        Duration.ofMinutes(1),       // 1m
                        Duration.ofMinutes(5),       // 5m
                        // Long operations (minutes)
                        Duration.ofMinutes(15),      // 15m
                        Duration.ofMinutes(30),      // 30m
                        Duration.ofHours(1),         // 1h
                        Duration.ofHours(2)          // 2h
                    )
                    .minimumExpectedValue(Duration.ofMillis(100))
                    .maximumExpectedValue(Duration.ofHours(4))
                    .register(meterRegistry);
        });
    }

    private Counter getOrCreateCounter(String metricName, String clientOrigin, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 1];
            allTags[0] = normalizedClientOrigin;
            System.arraycopy(additionalTags, 0, allTags, 1, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin);
        }
        
        return counterCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }

            return Counter.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability counter metric")
                    .tags(tags)
                    .register(meterRegistry);
        });
    }

    
    private DistributionSummary getOrCreateDistributionSummary(String metricName, String clientOrigin, String... additionalTags) {
        String normalizedClientOrigin = normalizeClientOrigin(clientOrigin);
        String key;
        if (additionalTags != null && additionalTags.length > 0) {
            // Include clientOrigin in cache key to prevent collisions
            String[] allTags = new String[additionalTags.length + 1];
            allTags[0] = normalizedClientOrigin;
            System.arraycopy(additionalTags, 0, allTags, 1, additionalTags.length);
            key = getMetricKey(metricName, allTags);
        } else {
            key = getMetricKey(metricName, normalizedClientOrigin);
        }
        
        return distributionSummaryCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", normalizedClientOrigin);
            if (additionalTags != null && additionalTags.length > 0) {
                tags = tags.and(Tags.of(additionalTags));
            }
            
            // Configure SLOs based on metric type
            DistributionSummary.Builder builder = DistributionSummary.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Atlas observability distribution metric")
                    .tags(tags)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Key percentiles
                    .publishPercentileHistogram();
            
            // Configure SLOs based on metric type
            if (metricName.contains("payload_bytes")) {
                // Payload bytes SLOs (bytes)
                builder.serviceLevelObjectives(
                    1024.0,           // 1KB
                    10240.0,          // 10KB
                    102400.0,         // 100KB
                    1048576.0,        // 1MB
                    10485760.0,       // 10MB
                    104857600.0,      // 100MB
                    1073741824.0,     // 1GB
                    10737418240.0     // 10GB
                )
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(107374182400.0) // 100GB
                .baseUnit("bytes");
            } else if (metricName.contains("payload_size") || metricName.contains("array")) {
                // Payload size and array metrics SLOs (count)
                builder.serviceLevelObjectives(
                    1.0,              // 1 item
                    10.0,             // 10 items
                    100.0,            // 100 items
                    1000.0,           // 1K items
                    10000.0,          // 10K items
                    100000.0,         // 100K items
                    1000000.0,        // 1M items
                    10000000.0        // 10M items
                )
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(100000000.0) // 100M items
                .baseUnit("items");
            } else {
                // Default SLOs for other metrics
                builder.serviceLevelObjectives(
                    1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0
                )
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(10000000.0);
            }
            
            return builder.register(meterRegistry);
        });
    }
    
    
}