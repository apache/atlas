package org.apache.atlas.observability;

import io.micrometer.core.instrument.*;
import org.apache.atlas.RequestContext;
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
    private static final Logger LOG = LoggerFactory.getLogger("OBSERVABILITY");
    private static final String METRIC_COMPONENT = "atlas_observability_bulk";
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
    
    public void recordCreateOrUpdateDuration(AtlasObservabilityData data) {
        Timer timer = getOrCreateTimer("duration", data.getXAtlanClientOrigin());
        timer.record(data.getDuration(), TimeUnit.MILLISECONDS);
    }
    
    public void recordPayloadSize(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("payload_size", data.getXAtlanClientOrigin());
        summary.record(data.getPayloadAssetSize());
        RequestContext context = RequestContext.get();
        context.endMetricRecord(context.startMetricRecord("entities_count"), data.getPayloadAssetSize());
    }
    
    public void recordPayloadBytes(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("payload_bytes", data.getXAtlanClientOrigin());
        summary.record(data.getPayloadRequestBytes());
    }
    
    public void recordArrayRelationships(AtlasObservabilityData data) {
        // Record total count
        DistributionSummary summary = getOrCreateDistributionSummary("array_relationships", data.getXAtlanClientOrigin());
        summary.record(data.getTotalArrayRelationships());
        
        // Record individual relationship types and counts
        recordRelationshipMap("relationship_attributes", data.getRelationshipAttributes(), data.getXAtlanClientOrigin());
        recordRelationshipMap("append_relationship_attributes", data.getAppendRelationshipAttributes(), data.getXAtlanClientOrigin());
        recordRelationshipMap("remove_relationship_attributes", data.getRemoveRelationshipAttributes(), data.getXAtlanClientOrigin());
    }
    
    public void recordArrayAttributes(AtlasObservabilityData data) {
        // Record total count
        DistributionSummary summary = getOrCreateDistributionSummary("array_attributes", data.getXAtlanClientOrigin());
        summary.record(data.getTotalArrayAttributes());
        // Record individual attribute types and counts
        if (data.getArrayAttributes() != null && !data.getArrayAttributes().isEmpty()) {
            for (Map.Entry<String, Integer> entry : data.getArrayAttributes().entrySet()) {
                Counter counter = getOrCreateCounter("attributes", data.getXAtlanClientOrigin(),
                    "attribute_name", entry.getKey());
                counter.increment(entry.getValue());
            }
        }
    }
    
    private void recordRelationshipMap(String metricName, Map<String, Integer> relationshipMap, String clientOrigin) {
        RequestContext context = RequestContext.get();
        int totalCount = 0;
        if (relationshipMap != null && !relationshipMap.isEmpty()) {
            for (Map.Entry<String, Integer> entry : relationshipMap.entrySet()) {
                Counter counter = getOrCreateCounter(metricName, clientOrigin,
                    "relationship_name", entry.getKey());
                counter.increment(entry.getValue());
                context.endMetricRecord(context.startMetricRecord("relation:-" + entry.getKey()), entry.getValue());
                totalCount += entry.getValue();
            }
            context.endMetricRecord(context.startMetricRecord("relations_count"), totalCount);
        }
    }
    
    
    
    public void recordTimingMetrics(AtlasObservabilityData data) {
        
        recordTimingMetric("diff_calc", data.getDiffCalcTime(), data.getXAtlanClientOrigin());
        recordTimingMetric("lineage_calc", data.getLineageCalcTime(), data.getXAtlanClientOrigin());
        recordTimingMetric("validation", data.getValidationTime(), data.getXAtlanClientOrigin());
        recordTimingMetric("ingestion", data.getIngestionTime(), data.getXAtlanClientOrigin());
        recordTimingMetric("notification", data.getNotificationTime(), data.getXAtlanClientOrigin());
        recordTimingMetric("audit_log", data.getAuditLogTime(), data.getXAtlanClientOrigin());
    }
    
    private void recordTimingMetric(String operation, long durationMs, String clientOrigin) {
        if (durationMs > 0) {
            Timer timer = getOrCreateTimer(operation + "_time", clientOrigin != null ? clientOrigin : "unknown");
            timer.record(durationMs, TimeUnit.MILLISECONDS);
        } else {
            LOG.debug("Skipping {} metric recording - duration is 0", operation);
        }
    }
    
    public void recordOperationCount(String operation, String status) {
        Counter counter = getOrCreateCounter("operations", "unknown", 
            "operation", operation, "status", status);
        counter.increment();
        totalOperations.incrementAndGet();
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
        Counter errorCounter = getOrCreateCounter("operation_errors", "unknown",
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
        String key = getMetricKey(metricName, clientOrigin);
        if (additionalTags != null && additionalTags.length > 0) {
            key = getMetricKey(metricName,  additionalTags);
        }
        
        return timerCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", clientOrigin);
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
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofHours(4))
                    .register(meterRegistry);
        });
    }

    private Counter getOrCreateCounter(String metricName, String clientOrigin, String... additionalTags) {
        String key = getMetricKey(metricName, clientOrigin);
        if (additionalTags != null && additionalTags.length > 0) {
            key = getMetricKey(metricName, additionalTags);
        }
        
        return counterCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", clientOrigin);
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
        String key = getMetricKey(metricName, clientOrigin);
        if (additionalTags != null && additionalTags.length > 0) {
            key = getMetricKey(metricName,additionalTags);
        }
        
        return distributionSummaryCache.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("client_origin", clientOrigin);
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
            if (metricName.contains("payload")) {
                // Payload size SLOs (bytes)
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
            } else if (metricName.contains("array")) {
                // Array size SLOs (count)
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