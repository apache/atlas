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
    private final Map<String, Counter> taskCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> taskTimers = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> assetsSummaries = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    // Gauges
    private final AtomicInteger tasksInProgress;
    private final AtomicInteger taskQueueSize;

    @Inject
    public TaskMetricsService() {
        this(getMeterRegistry());
    }

    // Constructor for testing
    TaskMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize gauges
        this.tasksInProgress = new AtomicInteger(0);
        Gauge.builder("atlas.classification.tasks.in_progress", tasksInProgress, AtomicInteger::get)
                .description("Current number of classification tasks in progress")
                .tag("component", "classification")
                .register(meterRegistry);

        this.taskQueueSize = new AtomicInteger(0);
        Gauge.builder("atlas.classification.tasks.queue.size", taskQueueSize, AtomicInteger::get)
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
            return Counter.builder("atlas.classification." + metricName)
                    .description("Classification task metric: " + metricName)
                    .tags(tags)
                    .register(meterRegistry);
        });
    }

    private Timer getOrCreateTaskTimer(String taskType, String version, String tenant, String status) {
        String key = getMetricKey("duration", taskType, version, tenant, status);
        return taskTimers.computeIfAbsent(key, k -> {
            return Timer.builder("atlas.classification.task.duration")
                    .description("Classification task execution duration")
                    .tags("type", taskType, "version", version, "tenant", tenant, "status", status)
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                        Duration.ofMillis(100),
                        Duration.ofMillis(500),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(5)
                    )
                    .register(meterRegistry);
        });
    }

    private DistributionSummary getOrCreateAssetsSummary(String taskType, String version, String tenant) {
        String key = getMetricKey("assets_per_task", taskType, version, tenant);
        return assetsSummaries.computeIfAbsent(key, k -> {
            return DistributionSummary.builder("atlas.classification.assets.per.task")
                    .description("Number of assets affected per task")
                    .tags("type", taskType, "version", version, "tenant", tenant)
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .minimumExpectedValue(1.0)
                    .maximumExpectedValue(1_000_000.0)
                    .register(meterRegistry);
        });
    }

    public void recordTaskStart(String taskType, String version, String tenant) {
        getOrCreateTaskCounter("tasks.total", taskType, version, tenant).increment();
        tasksInProgress.incrementAndGet();
    }

    public void recordTaskEnd(String taskType, String version, String tenant, long durationMs, int assetsAffected, boolean success) {
        String status = success ? "success" : "failure";
        tasksInProgress.decrementAndGet();
        
        // Record duration
        getOrCreateTaskTimer(taskType, version, tenant, status)
            .record(java.time.Duration.ofMillis(durationMs));

        // Record assets affected
        getOrCreateTaskCounter("assets.affected.total", taskType, version, tenant, "status", status)
            .increment(assetsAffected);

        // Record assets per task distribution
        getOrCreateAssetsSummary(taskType, version, tenant)
            .record(assetsAffected);

        // Record task status
        getOrCreateTaskCounter("tasks.status", taskType, version, tenant, "status", status)
            .increment();
    }

    public void recordTaskError(String taskType, String version, String tenant, String errorType) {
        getOrCreateTaskCounter("tasks.errors.total", taskType, version, tenant, "error", errorType)
            .increment();
    }

    public void updateQueueSize(int size) {
        taskQueueSize.set(size);
    }
} 