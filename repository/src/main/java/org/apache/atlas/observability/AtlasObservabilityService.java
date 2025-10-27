package org.apache.atlas.observability;

import io.micrometer.core.instrument.*;
import org.apache.atlas.RequestContext;
import org.springframework.stereotype.Service;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.atlas.repository.store.graph.v2.utils.MDCScope;

@Service
public class AtlasObservabilityService {
    private static final Logger LOG = LoggerFactory.getLogger("OBSERVABILITY");
    private static final String METRIC_COMPONENT = "atlas_observability_bulk";
    private final MeterRegistry meterRegistry;
    
    // Note: Micrometer handles metric caching internally, so we don't need to cache them ourselves
    
    @Inject
    public AtlasObservabilityService() {
        this(getMeterRegistry());
    }
    
    // Constructor for testing
    AtlasObservabilityService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
        RequestContext context = RequestContext.get();
        context.endMetricRecord(context.startMetricRecord("entities_bytes"), data.getPayloadRequestBytes());
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
        Tags tags = Tags.of("client_origin", clientOrigin);
        if (additionalTags != null && additionalTags.length > 0) {
            tags = tags.and(Tags.of(additionalTags));
        }
        return Timer.builder(METRIC_COMPONENT + "_" + metricName)
                .description("Atlas observability timing metric")
                .tags(tags)
                .register(meterRegistry);
    }

    private Counter getOrCreateCounter(String metricName, String clientOrigin, String... additionalTags) {
        Tags tags = Tags.of("client_origin", clientOrigin);
        if (additionalTags != null && additionalTags.length > 0) {
            tags = tags.and(Tags.of(additionalTags));
        }

        return Counter.builder(METRIC_COMPONENT + "_" + metricName)
                .description("Atlas observability counter metric")
                .tags(tags)
                .register(meterRegistry);
    }

    
    private DistributionSummary getOrCreateDistributionSummary(String metricName, String clientOrigin, String... additionalTags) {
        Tags tags = Tags.of("client_origin", clientOrigin);
        if (additionalTags != null && additionalTags.length > 0) {
            tags = tags.and(Tags.of(additionalTags));
        }
        return DistributionSummary.builder(METRIC_COMPONENT + "_" + metricName)
                .description("Atlas observability distribution metric")
                .tags(tags)
                .register(meterRegistry);
    }
    
    
}