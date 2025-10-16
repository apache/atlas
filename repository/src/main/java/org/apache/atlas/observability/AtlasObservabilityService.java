package org.apache.atlas.observability;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Service;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@Service
public class AtlasObservabilityService {
    private static final Logger LOG = LoggerFactory.getLogger("OBSERVABILITY");
    private static final String METRIC_COMPONENT = "atlas_observability";
    private final MeterRegistry meterRegistry;
    
    // Metric caches
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> summaries = new ConcurrentHashMap<>();
    
    @Inject
    public AtlasObservabilityService() {
        this(getMeterRegistry());
    }
    
    // Constructor for testing
    AtlasObservabilityService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    public void recordCreateOrUpdateDuration(AtlasObservabilityData data) {
        Timer timer = getOrCreateTimer("createOrUpdate.duration", 
            "client_origin", data.getXAtlanClientOrigin());
        
        timer.record(data.getDuration(), TimeUnit.MILLISECONDS);
    }
    
    public void recordPayloadSize(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("createOrUpdate.payload_size",
            "client_origin", data.getXAtlanClientOrigin());
        
        summary.record(data.getPayloadAssetSize());
    }
    
    public void recordPayloadBytes(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("createOrUpdate.payload_bytes",
            "client_origin", data.getXAtlanClientOrigin());
        
        summary.record(data.getPayloadRequestBytes());
    }
    
    public void recordArrayRelationships(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("createOrUpdate.array_relationships",
            "client_origin", data.getXAtlanClientOrigin());
        
        summary.record(data.getTotalArrayRelationships());
    }
    
    public void recordArrayAttributes(AtlasObservabilityData data) {
        DistributionSummary summary = getOrCreateDistributionSummary("createOrUpdate.array_attributes",
            "client_origin", data.getXAtlanClientOrigin());
        
        summary.record(data.getTotalArrayAttributes());
    }
    
    public void recordTimingMetrics(AtlasObservabilityData data) {
        recordTimingMetric("diff_calc", data.getDiffCalcTime());
        recordTimingMetric("lineage_calc", data.getLineageCalcTime());
        recordTimingMetric("validation", data.getValidationTime());
        recordTimingMetric("ingestion", data.getIngestionTime());
        recordTimingMetric("notification", data.getNotificationTime());
        recordTimingMetric("audit_log", data.getAuditLogTime());
    }
    
    private void recordTimingMetric(String operation, long durationMs) {
        if (durationMs > 0) {
            Timer timer = getOrCreateTimer("createOrUpdate." + operation + "_time");
            timer.record(durationMs, TimeUnit.MILLISECONDS);
        }
    }
    
    public void recordOperationCount(String operation, String status) {
        Counter counter = getOrCreateCounter("createOrUpdate.operations",
            "operation", operation,
            "status", status);
        
        counter.increment();
    }
    
    /**
     * Log detailed observability data for error cases only.
     * This includes high-cardinality fields like traceId, vertexIds, assetGuids
     * that should NOT be sent to Prometheus.
     */
    public void logErrorDetails(AtlasObservabilityData data, String errorMessage, Throwable throwable) {
        // Set MDC filter for observability logs
        MDC.put("filter", "atlas-observability");
        
        try {
            // Log structured data for debugging - goes to ClickHouse
            // This includes traceId, vertexIds, assetGuids for error correlation
            LOG.error("Atlas createOrUpdate error: {} | traceId: {} | assetGuids: {} | vertexIds: {} | error: {}", 
                errorMessage,
                data.getTraceId(),
                data.getAssetGuids(),
                data.getVertexIds(),
                throwable != null ? throwable.getMessage() : "unknown");
        } finally {
            // Clean up MDC
            MDC.remove("filter");
        }
    }
    
    private Timer getOrCreateTimer(String metricName, String... tags) {
        String key = getMetricKey(metricName, tags);
        return timers.computeIfAbsent(key, k -> 
            Timer.builder(METRIC_COMPONENT + "_" + metricName)
                .description("Atlas observability timing metric")
                .tags(tags)
                .register(meterRegistry));
    }
    
    private Counter getOrCreateCounter(String metricName, String... tags) {
        String key = getMetricKey(metricName, tags);
        return counters.computeIfAbsent(key, k ->
            Counter.builder(METRIC_COMPONENT + "_" + metricName)
                .description("Atlas observability counter metric")
                .tags(tags)
                .register(meterRegistry));
    }
    
    private DistributionSummary getOrCreateDistributionSummary(String metricName, String... tags) {
        String key = getMetricKey(metricName, tags);
        return summaries.computeIfAbsent(key, k ->
            DistributionSummary.builder(METRIC_COMPONENT + "_" + metricName)
                .description("Atlas observability distribution metric")
                .tags(tags)
                .register(meterRegistry));
    }
    
    private String getMetricKey(String metricName, String... tags) {
        StringBuilder key = new StringBuilder(metricName);
        for (String tag : tags) {
            key.append(":").append(tag);
        }
        return key.toString();
    }
}