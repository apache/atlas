package org.apache.atlas.repository.metrics;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Service;
import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.time.Duration;
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

@Service
public class TaskMetricsService {
    private static final String METRIC_COMPONENT = "atlas_classification";
    private final Map<String, Counter> taskCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> taskTimers = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> assetsSummaries = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    // Gauges
    private final AtomicInteger tasksInProgress = new AtomicInteger(0);
    private final AtomicInteger taskQueueSize = new AtomicInteger(0);

    @Inject
    public TaskMetricsService() {
        this(getMeterRegistry());
    }

    // Constructor for testing
    TaskMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize gauges
        Gauge.builder(METRIC_COMPONENT + "_tasks_in_progress", tasksInProgress, AtomicInteger::get)
                .description("Current number of classification tasks in progress")
                .tag("component", "classification")
                .register(meterRegistry);

        Gauge.builder(METRIC_COMPONENT + "_tasks_queue_size", taskQueueSize, AtomicInteger::get)
                .description("Current size of the classification task queue")
                .tag("component", "classification")
                .register(meterRegistry);
    }

    private String getMetricKey(String metricName, String... tags) {
        StringBuilder key = new StringBuilder(metricName);
        for (String tag : tags) {
            key.append(":").append(tag);
        }
        return key.toString();
    }

    private Counter getOrCreateTaskCounter(String metricName, String taskType, String version, String tenant, String... additionalTags) {
        String key = getMetricKey(metricName, taskType, version, tenant);
        return taskCounters.computeIfAbsent(key, k -> {
            Tags tags = Tags.of("type", taskType, "version", version, "tenant", tenant)
                          .and(Tags.of(additionalTags));
            return Counter.builder(METRIC_COMPONENT + "_" + metricName)
                    .description("Classification task metric: " + metricName)
                    .tags(tags)
                    .register(meterRegistry);
        });
    }

    private Timer getOrCreateTaskTimer(String taskType, String version, String tenant, String status) {
        String key = getMetricKey("duration", taskType, version, tenant, status);
        return taskTimers.computeIfAbsent(key, k -> {
            return Timer.builder(METRIC_COMPONENT + "_task_duration_seconds")
                    .description("Classification task execution duration")
                    .tags("type", taskType, "version", version, "tenant", tenant, "status", status)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Added 75th percentile
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                        // Short tasks
                        Duration.ofSeconds(1),      // 1s
                        Duration.ofSeconds(10),     // 10s
                        Duration.ofSeconds(30),     // 30s
                        // Medium tasks
                        Duration.ofMinutes(1),      // 1m
                        Duration.ofMinutes(5),      // 5m
                        Duration.ofMinutes(15),     // 15m
                        Duration.ofMinutes(30),     // 30m
                        // Long tasks
                        Duration.ofHours(1),        // 1h
                        Duration.ofHours(2),        // 2h
                        Duration.ofHours(4),        // 4h
                        Duration.ofHours(8),        // 8h
                        Duration.ofHours(12)        // 12h
                    )
                    .minimumExpectedValue(Duration.ofSeconds(1))
                    .maximumExpectedValue(Duration.ofHours(24))
                    .register(meterRegistry);
        });
    }

    private DistributionSummary getOrCreateAssetsSummary(String taskType, String version, String tenant) {
        String key = getMetricKey("assets_per_task", taskType, version, tenant);
        return assetsSummaries.computeIfAbsent(key, k -> {
            return DistributionSummary.builder(METRIC_COMPONENT + "_assets_per_task")
                    .description("Number of assets affected per task")
                    .tags("type", taskType, "version", version, "tenant", tenant)
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99) // Added 75th percentile
                    .publishPercentileHistogram()
                    .baseUnit("assets")
                    .scale(1.0)
                    .serviceLevelObjectives(
                        // Small tasks
                        10.0,        // 10 assets
                        100.0,       // 100 assets
                        1000.0,      // 1K assets
                        // Medium tasks
                        10000.0,     // 10K assets
                        50000.0,     // 50K assets
                        100000.0,    // 100K assets
                        // Large tasks
                        500000.0,    // 500K assets
                        1000000.0,   // 1M assets
                        5000000.0,   // 5M assets
                        10000000.0   // 10M assets
                    )
                    .minimumExpectedValue(1.0)
                    .maximumExpectedValue(20000000.0) // 20M assets max
                    .register(meterRegistry);
        });
    }

    public void recordTaskStart(String taskType, String version, String tenant) {
        getOrCreateTaskCounter("tasks_total", taskType, version, tenant).increment();
        tasksInProgress.incrementAndGet();
    }

    public void recordTaskEnd(String taskType, String version, String tenant, long durationMs, int assetsAffected, boolean success) {
        String status = success ? "success" : "failure";
        tasksInProgress.decrementAndGet();
        
        // Record duration
        getOrCreateTaskTimer(taskType, version, tenant, status)
            .record(java.time.Duration.ofMillis(durationMs));

        // Record assets affected
        getOrCreateTaskCounter("assets_affected_total", taskType, version, tenant, "status", status)
            .increment(assetsAffected);

        // Record assets per task distribution
        getOrCreateAssetsSummary(taskType, version, tenant)
            .record(assetsAffected);

        // Record task status
        getOrCreateTaskCounter("tasks_status", taskType, version, tenant, "status", status)
            .increment();
    }

    public void recordTaskError(String taskType, String version, String tenant, String errorType) {
        getOrCreateTaskCounter("tasks_errors_total", taskType, version, tenant, "error", errorType)
            .increment();
    }

    public void updateQueueSize(int size) {
        taskQueueSize.set(size);
    }
} 