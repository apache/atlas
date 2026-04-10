package org.apache.atlas.web.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.service.metrics.MetricUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ESDeferredOperation;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.EntityMutationPostProcessor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes tag denorm DLQ messages and repairs ES from Cassandra truth.
 *
 * For each message containing failed vertex IDs + GUIDs:
 * 1. Extract GUIDs from the DLQ message
 * 2. Delegate to {@link AtlasEntityStore#repairClassificationMappingsV2(List)} which handles:
 *    - GUID → AtlasVertex resolution
 *    - Cassandra read via tagDAO.getAllClassificationsForVertex (with executeWithRetry)
 *    - Normalization via mapClassificationsV2
 *    - Denorm computation via TagDeNormAttributesUtil.getAllAttributesForAllTagsForRepair
 *    - Queues ESDeferredOperation
 * 3. Flush deferred ES operations (same pattern as EntityMutationService)
 *
 * Uses Kafka consumer groups to ensure only one instance processes messages.
 * Follows the same mature patterns as {@link DLQReplayService}:
 * pause/resume, seek-back, retry tracking, exponential backoff, poison pill handling.
 */
@Service
public class TagDenormDLQReplayService {

    private static final Logger log = LoggerFactory.getLogger(TagDenormDLQReplayService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROPERTY_PREFIX = "atlas.kafka";
    private static final String MESSAGE_TYPE_TAG_DENORM_SYNC = "TAG_DENORM_SYNC";
    private static final int REPAIR_BATCH_SIZE = 100;

    private String bootstrapServers;

    @Value("${atlas.kafka.tag.denorm.dlq.topic:ATLAS_TAG_DENORM_DLQ}")
    private String dlqTopic = "ATLAS_TAG_DENORM_DLQ";

    @Value("${atlas.kafka.tag.denorm.dlq.consumerGroupId:atlas_tag_denorm_dlq_replay_group}")
    private String consumerGroupId = "atlas_tag_denorm_dlq_replay_group";

    @Value("${atlas.kafka.tag.denorm.dlq.enabled:true}")
    private boolean enabled = true;

    @Value("${atlas.kafka.tag.denorm.dlq.maxRetries:5}")
    private int maxRetries = 5;

    // Kafka consumer configuration
    @Value("${atlas.kafka.tag.denorm.dlq.maxPollRecords:1}")
    private int maxPollRecords = 1;

    @Value("${atlas.kafka.tag.denorm.dlq.maxPollIntervalMs:600000}")
    private int maxPollIntervalMs = 600000; // 10 minutes

    @Value("${atlas.kafka.tag.denorm.dlq.sessionTimeoutMs:90000}")
    private int sessionTimeoutMs = 90000; // 90 seconds

    @Value("${atlas.kafka.tag.denorm.dlq.heartbeatIntervalMs:30000}")
    private int heartbeatIntervalMs = 30000; // 30 seconds

    // Timing configuration
    @Value("${atlas.kafka.tag.denorm.dlq.pollTimeoutSeconds:15}")
    private int pollTimeoutSeconds = 15;

    @Value("${atlas.kafka.tag.denorm.dlq.shutdownWaitSeconds:60}")
    private int shutdownWaitSeconds = 60;

    @Value("${atlas.kafka.tag.denorm.dlq.consumerCloseTimeoutSeconds:30}")
    private int consumerCloseTimeoutSeconds = 30;

    @Value("${atlas.kafka.tag.denorm.dlq.errorBackoffMs:10000}")
    private int errorBackoffMs = 10000; // 10 seconds (for permanent exceptions)

    // Exponential backoff configuration for transient exceptions
    @Value("${atlas.kafka.tag.denorm.dlq.exponentialBackoff.baseDelayMs:1000}")
    private int exponentialBackoffBaseDelayMs = 1000; // 1 second

    @Value("${atlas.kafka.tag.denorm.dlq.exponentialBackoff.maxDelayMs:60000}")
    private int exponentialBackoffMaxDelayMs = 60000; // 60 seconds

    @Value("${atlas.kafka.tag.denorm.dlq.exponentialBackoff.multiplier:2.0}")
    private double exponentialBackoffMultiplier = 2.0;

    @Value("${atlas.kafka.tag.denorm.dlq.trackerCleanupIntervalMs:300000}")
    private long trackerCleanupIntervalMs = 300000; // 5 minutes

    @Value("${atlas.kafka.tag.denorm.dlq.trackerMaxAgeMs:3600000}")
    private long trackerMaxAgeMs = 3600000; // 1 hour

    @Value("${atlas.kafka.tag.denorm.dlq.retryDelayMs:5000}")
    private int retryDelayMs = 5000; // 5 seconds

    // Track retry attempts with timestamps for cleanup
    private static class RetryTrackerEntry {
        int retryCount;
        long lastAttemptTime;

        RetryTrackerEntry(int retryCount) {
            this.retryCount = retryCount;
            this.lastAttemptTime = System.currentTimeMillis();
        }
    }

    private final Map<String, RetryTrackerEntry> retryTracker = new ConcurrentHashMap<>();
    private final Map<String, Long> backoffTracker = new ConcurrentHashMap<>();
    private volatile long lastTrackerCleanupTime = System.currentTimeMillis();

    private volatile KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(true);
    private volatile Thread replayThread;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicInteger skippedCount = new AtomicInteger(0);

    private final AtlasEntityStore entityStore;
    private final EntityMutationPostProcessor postProcessor;
    private MeterRegistry meterRegistry;

    @Inject
    public TagDenormDLQReplayService(AtlasEntityStore entityStore,
                                      EntityMutationPostProcessor postProcessor) throws AtlasException {
        this.entityStore = entityStore;
        this.postProcessor = postProcessor;
        this.bootstrapServers = ApplicationProperties.get().getString("atlas.graph.kafka.bootstrap.servers");

        try {
            this.meterRegistry = MetricUtils.getMeterRegistry();
        } catch (Exception e) {
            log.warn("Failed to get meter registry for DLQ replay metrics", e);
        }
    }

    /**
     * Start the DLQ replay service.
     */
    @PostConstruct
    public synchronized void startReplay() {
        if (!enabled) {
            log.info("Tag denorm DLQ replay service is disabled");
            return;
        }

        if (isRunning.get()) {
            log.warn("Tag denorm DLQ replay is already running");
            return;
        }

        try {
            startConsumer();
        } catch (Exception e) {
            log.error("Failed to start tag denorm DLQ replay service. Service will be unavailable but pod will not crash.", e);
            isHealthy.set(false);
        }
    }

    private void startConsumer() {
        log.info("Starting tag denorm DLQ replay service for topic: {} with consumer group: {}", dlqTopic, consumerGroupId);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual commit after success
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Optimized settings for long-running message processing with pause/resume pattern
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs));
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMs));

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(dlqTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("Consumer group partitions revoked. Partitions: {}", partitions);

                // Clean up retry tracker and backoff tracker for revoked partitions to prevent memory leak
                for (TopicPartition partition : partitions) {
                    String partitionPrefix = partition.partition() + "-";
                    retryTracker.keySet().removeIf(key -> key.startsWith(partitionPrefix));
                    backoffTracker.keySet().removeIf(key -> key.startsWith(partitionPrefix));
                    log.info("Cleaned up retry and backoff trackers for revoked partition: {}", partition);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Consumer group partitions assigned: {}", partitions);

                for (TopicPartition partition : partitions) {
                    try {
                        long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                        long committedOffset = -1;
                        OffsetAndMetadata committed = consumer.committed(Collections.singleton(partition)).get(partition);
                        if (committed != null) {
                            committedOffset = committed.offset();
                        }
                        long position = consumer.position(partition);

                        log.info("Partition {} - End offset: {}, Committed offset: {}, Current position: {}, " +
                                        "Messages available: {}",
                                partition, endOffset, committedOffset, position,
                                endOffset - position);
                    } catch (Exception e) {
                        log.error("Error checking offsets for partition: " + partition, e);
                    }
                }
            }
        });

        isRunning.set(true);
        isHealthy.set(true);

        replayThread = new Thread(this::processMessages, "TagDenorm-DLQ-Replay-Thread");
        replayThread.setDaemon(false);
        replayThread.start();

        log.info("Tag denorm DLQ replay service started successfully");
    }

    /**
     * Gracefully shutdown the tag denorm DLQ replay service with proper thread join.
     */
    @PreDestroy
    public synchronized void shutdown() {
        if (!isRunning.get()) {
            log.info("Tag denorm DLQ replay service is not running, nothing to shutdown");
            return;
        }

        log.info("Shutting down tag denorm DLQ replay service...");
        isRunning.set(false);

        // Step 1: Interrupt the consumer's poll() to wake it up
        if (consumer != null) {
            try {
                consumer.wakeup();
                log.info("Sent wakeup signal to Kafka consumer");
            } catch (Exception e) {
                log.error("Error sending wakeup to consumer", e);
            }
        }

        // Step 2: Wait for the replay thread to finish current work
        if (replayThread != null && replayThread.isAlive()) {
            try {
                long shutdownTimeoutMs = TimeUnit.SECONDS.toMillis(shutdownWaitSeconds);
                log.info("Waiting up to {}ms for replay thread to complete current work...", shutdownTimeoutMs);

                replayThread.join(shutdownTimeoutMs);

                if (replayThread.isAlive()) {
                    log.error("Replay thread did not terminate within timeout of {}ms! " +
                                    "Current message processing may be interrupted. Thread state: {}",
                            shutdownTimeoutMs, replayThread.getState());
                } else {
                    log.info("Replay thread terminated gracefully");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for replay thread to finish");
            }
        }

        // Step 3: Close consumer
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(consumerCloseTimeoutSeconds));
                log.info("Kafka consumer closed successfully");
            } catch (Exception e) {
                log.error("Error closing Kafka consumer during shutdown", e);
            }
        }

        log.info("Tag denorm DLQ replay service shutdown complete. Total processed: {}, Total errors: {}, Total skipped: {}",
                processedCount.get(), errorCount.get(), skippedCount.get());
    }

    /**
     * Process messages from the DLQ topic using pause/resume pattern.
     */
    private void processMessages() {
        log.info("Tag denorm DLQ replay thread started, polling for messages...");

        try {
            while (isRunning.get()) {
                try {
                    // Periodic cleanup of stale tracker entries
                    cleanupStaleTrackersIfNeeded();

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));

                    if (records.isEmpty()) {
                        for (TopicPartition partition : consumer.assignment()) {
                            try {
                                long currentPosition = consumer.position(partition);
                                long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                                if (currentPosition >= endOffset) {
                                    log.debug("No messages available - Partition {} at end (Position: {}, End: {})",
                                            partition, currentPosition, endOffset);
                                } else {
                                    log.info("No messages returned despite availability - Partition {} (Position: {}, End: {}, Available: {})",
                                            partition, currentPosition, endOffset, endOffset - currentPosition);
                                }
                            } catch (Exception e) {
                                log.error("Error checking position after empty poll", e);
                            }
                        }
                        continue;
                    }

                    log.debug("Received {} tag denorm DLQ messages to replay", records.count());

                    // PAUSE consumption immediately to prevent timeout during processing
                    Set<TopicPartition> pausedPartitions = consumer.assignment();
                    consumer.pause(pausedPartitions);
                    log.info("Paused consumption on partitions: {} to process messages", pausedPartitions);

                    Long failedOffset = null;
                    TopicPartition failedPartition = null;

                    try {
                        for (ConsumerRecord<String, String> record : records) {
                            String retryKey = record.partition() + "-" + record.offset();

                            try {
                                try {
                                    log.info("Processing tag denorm DLQ entry at offset: {} from partition: {}",
                                            record.offset(), record.partition());

                                    long processingStartTime = System.currentTimeMillis();
                                    replayTagDenormDLQEntry(record.value());
                                    long processingTime = System.currentTimeMillis() - processingStartTime;

                                    processedCount.incrementAndGet();
                                    emitReplayMessageMetric("success", null);

                                    // Success - remove from retry tracker, reset backoff, and commit offset
                                    retryTracker.remove(retryKey);
                                    resetExponentialBackoff(retryKey);

                                    Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                                            new TopicPartition(record.topic(), record.partition()),
                                            new OffsetAndMetadata(record.offset() + 1)
                                    );
                                    consumer.commitSync(offsets);

                                    log.debug("Successfully replayed tag denorm DLQ entry (offset: {}, partition: {}) in {}ms",
                                            record.offset(), record.partition(), processingTime);

                                } catch (Exception e) {
                                    errorCount.incrementAndGet();
                                    emitReplayMessageMetric("failed", e.getClass().getSimpleName());

                                    // Track retry attempts for ALL exceptions (AtlasBaseException + others)
                                    RetryTrackerEntry entry = retryTracker.get(retryKey);
                                    int retryCount;
                                    if (entry == null) {
                                        retryCount = 1;
                                        retryTracker.put(retryKey, new RetryTrackerEntry(retryCount));
                                    } else {
                                        retryCount = entry.retryCount + 1;
                                        entry.retryCount = retryCount;
                                        entry.lastAttemptTime = System.currentTimeMillis();
                                    }

                                    if (retryCount >= maxRetries) {
                                        // Poison pill — skip after max retries to prevent partition blockage
                                        log.error("Tag denorm DLQ entry at offset {} partition {} failed {} times (max retries reached). " +
                                                        "SKIPPING this message to prevent partition blockage. Error: {}",
                                                record.offset(), record.partition(), retryCount, e.getMessage(), e);

                                        skippedCount.incrementAndGet();
                                        emitReplayMessageMetric("skipped", e.getClass().getSimpleName());
                                        retryTracker.remove(retryKey);
                                        resetExponentialBackoff(retryKey);

                                        // Commit offset to move past poison pill
                                        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                                                new TopicPartition(record.topic(), record.partition()),
                                                new OffsetAndMetadata(record.offset() + 1)
                                        );
                                        try {
                                            consumer.commitSync(offsets);
                                            log.warn("Committed offset {} to skip poison pill", record.offset() + 1);
                                        } catch (Exception commitEx) {
                                            log.error("Failed to commit offset after skipping poison pill", commitEx);
                                        }
                                        // Continue to next record after skipping poison pill
                                    } else {
                                        // Retry with exponential backoff
                                        long backoffDelay = calculateExponentialBackoff(retryKey);

                                        log.warn("Failed to replay tag denorm DLQ entry (offset: {}, partition: {}). " +
                                                        "Retry {}/{}. Will retry after {}ms backoff. Error: {}",
                                                record.offset(), record.partition(), retryCount, maxRetries,
                                                backoffDelay, e.getMessage());

                                        failedOffset = record.offset();
                                        failedPartition = new TopicPartition(record.topic(), record.partition());

                                        Thread.sleep(backoffDelay);
                                        break;
                                    }
                                }
                            } finally {
                                // Clear RequestContext and graph vertex cache between messages to prevent accumulation
                                // Same pattern as TaskConsumer.run() finally block (TaskExecutor.java:213)
                                RequestContext.get().clearCache();
                                GraphTransactionInterceptor.clearCache();
                            }
                        }
                    } finally {
                        // Seek back to retry the failed message
                        if (failedOffset != null && failedPartition != null) {
                            try {
                                consumer.seek(failedPartition, failedOffset);
                                log.info("Seeked back to offset {} on partition {} to retry failed message",
                                        failedOffset, failedPartition);
                            } catch (Exception seekEx) {
                                log.error("Failed to seek back to offset {} on partition {}. Message may be skipped!",
                                        failedOffset, failedPartition, seekEx);
                            }
                        }

                        // RESUME consumption - always do this even if processing failed
                        consumer.resume(pausedPartitions);
                        log.info("Resumed consumption on partitions: {}", pausedPartitions);
                    }

                } catch (WakeupException e) {
                    log.info("Kafka consumer wakeup called, exiting processing loop");
                    break;
                } catch (Exception e) {
                    log.error("Error in tag denorm DLQ replay processing loop", e);
                    try {
                        Thread.sleep(errorBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("Tag denorm DLQ replay thread interrupted during error recovery");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in tag denorm DLQ replay thread - marking service as unhealthy", e);
            isHealthy.set(false);
        } finally {
            boolean wasRunning = isRunning.get();
            isRunning.set(false);

            if (wasRunning && !isHealthy.get()) {
                log.error("Tag denorm DLQ replay thread terminated unexpectedly! Service is unhealthy. Pod should be restarted.");
            }

            log.info("Tag denorm DLQ replay thread finished. Total processed: {}, Total errors: {}, Total skipped: {}",
                    processedCount.get(), errorCount.get(), skippedCount.get());
        }
    }

    /**
     * Replays a single DLQ entry: extracts GUIDs, delegates to repairClassificationMappingsV2,
     * flushes deferred ES operations.
     *
     * DLQ message format: { "type": "TAG_DENORM_SYNC", "timestamp": <millis>, "vertices": { "vertexId": "guid", ... } }
     */
    private void replayTagDenormDLQEntry(String dlqJson) throws Exception {
        JsonNode root = MAPPER.readTree(dlqJson);

        // Validate message type
        String type = root.has("type") ? root.get("type").asText() : null;
        if (!MESSAGE_TYPE_TAG_DENORM_SYNC.equals(type)) {
            log.warn("Unexpected DLQ message type: {}, skipping", type);
            return;
        }

        // Parse vertices (vertexId → guid)
        JsonNode verticesNode = root.get("vertices");
        if (verticesNode == null || !verticesNode.isObject() || verticesNode.isEmpty()) {
            log.warn("Tag denorm DLQ message has no vertices, skipping");
            return;
        }

        // Extract unique GUIDs from the message
        Set<String> guids = new LinkedHashSet<>();
        Iterator<Map.Entry<String, JsonNode>> fields = verticesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String guid = field.getValue().asText();
            if (guid != null && !guid.isEmpty()) {
                guids.add(guid);
            } else {
                log.warn("Missing GUID for vertexId={} in tag denorm DLQ message, skipping vertex", field.getKey());
            }
        }

        if (guids.isEmpty()) {
            log.warn("No valid GUIDs in tag denorm DLQ message, skipping");
            return;
        }

        log.info("Repairing tag denorm for {} vertices from DLQ", guids.size());

        // Process in batches to bound memory (deferred ops accumulate per batch)
        // Same pattern as EntityMutationService:341-355
        List<String> guidList = new ArrayList<>(guids);
        Map<String, String> allErrors = new HashMap<>();
        int batchSize = REPAIR_BATCH_SIZE;

        for (int i = 0; i < guidList.size(); i += batchSize) {
            List<String> batch = guidList.subList(i, Math.min(i + batchSize, guidList.size()));
            try {
                Map<String, String> errors = entityStore.repairClassificationMappingsV2(batch);
                allErrors.putAll(errors);

                // Flush deferred ES operations after each batch (same pattern as EntityMutationService:353-354)
                List<ESDeferredOperation> deferredOps = RequestContext.get().getESDeferredOperations();
                if (!deferredOps.isEmpty()) {
                    postProcessor.executeESOperations(deferredOps);
                }
            } finally {
                // Always clear deferred ops to prevent leakage between batches
                RequestContext.get().getESDeferredOperations().clear();
            }
        }

        if (!allErrors.isEmpty()) {
            int repairedCount = guids.size() - allErrors.size();
            emitReplayVertexMetric("repaired", null, repairedCount);
            emitReplayVertexMetric("failed", "repair_error", allErrors.size());
            log.warn("Tag denorm DLQ repair completed with {} errors: {}", allErrors.size(), allErrors);
            throw new AtlasBaseException("Repair failed for " + allErrors.size() + " vertices: " + allErrors.keySet());
        }

        emitReplayVertexMetric("repaired", null, guids.size());
        log.info("Successfully repaired tag denorm for {} vertices from DLQ", guids.size());
    }

    /**
     * Periodic cleanup of stale tracker entries to prevent memory leak.
     */
    private void cleanupStaleTrackersIfNeeded() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTrackerCleanupTime > trackerCleanupIntervalMs) {
            int retryTrackerSizeBefore = retryTracker.size();
            int backoffTrackerSizeBefore = backoffTracker.size();

            long cutoffTime = currentTime - trackerMaxAgeMs;

            // Clean up stale retry tracker entries
            retryTracker.entrySet().removeIf(entry -> entry.getValue().lastAttemptTime < cutoffTime);

            // Clean up stale backoff tracker entries (best-effort safety net)
            if (backoffTracker.size() > retryTracker.size() * 2) {
                log.warn("Backoff tracker size ({}) is much larger than retry tracker size ({}), clearing orphaned entries",
                        backoffTracker.size(), retryTracker.size());
                Set<String> activeRetryKeys = retryTracker.keySet();
                backoffTracker.keySet().removeIf(key -> !activeRetryKeys.contains(key));
            }

            int retryTrackerCleaned = retryTrackerSizeBefore - retryTracker.size();
            int backoffTrackerCleaned = backoffTrackerSizeBefore - backoffTracker.size();

            if (retryTrackerCleaned > 0 || backoffTrackerCleaned > 0) {
                log.debug("Cleaned up stale tracker entries: retry tracker {} -> {} (removed {}), backoff tracker {} -> {} (removed {})",
                        retryTrackerSizeBefore, retryTracker.size(), retryTrackerCleaned,
                        backoffTrackerSizeBefore, backoffTracker.size(), backoffTrackerCleaned);
            }

            lastTrackerCleanupTime = currentTime;
        }
    }

    /**
     * Calculate exponential backoff delay for a failed message.
     */
    private long calculateExponentialBackoff(String retryKey) {
        long currentDelay = backoffTracker.getOrDefault(retryKey, (long) exponentialBackoffBaseDelayMs);
        long nextDelay = (long) (currentDelay * exponentialBackoffMultiplier);

        // Cap at maximum delay
        nextDelay = Math.min(nextDelay, exponentialBackoffMaxDelayMs);

        // Store for next time
        backoffTracker.put(retryKey, nextDelay);

        return currentDelay;
    }

    /**
     * Reset exponential backoff for a successfully processed message.
     */
    private void resetExponentialBackoff(String retryKey) {
        backoffTracker.remove(retryKey);
    }

    /**
     * Health check for liveness/readiness probes.
     */
    public boolean isHealthy() {
        boolean threadAlive = replayThread != null && replayThread.isAlive();
        return isHealthy.get() && threadAlive;
    }

    /**
     * Get replay status.
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("enabled", enabled);
        status.put("isRunning", isRunning.get());
        status.put("isHealthy", isHealthy());
        status.put("threadAlive", replayThread != null && replayThread.isAlive());
        status.put("processedCount", processedCount.get());
        status.put("errorCount", errorCount.get());
        status.put("skippedCount", skippedCount.get());
        status.put("topic", dlqTopic);
        status.put("consumerGroup", consumerGroupId);
        status.put("maxRetries", maxRetries);
        status.put("activeRetries", retryTracker.size());
        status.put("activeBackoffs", backoffTracker.size());

        Map<String, Object> backoffConfig = new HashMap<>();
        backoffConfig.put("baseDelayMs", exponentialBackoffBaseDelayMs);
        backoffConfig.put("maxDelayMs", exponentialBackoffMaxDelayMs);
        backoffConfig.put("multiplier", exponentialBackoffMultiplier);
        status.put("exponentialBackoffConfig", backoffConfig);

        return status;
    }

    /**
     * Emits a Prometheus counter for DLQ replay message outcomes.
     * Labels: outcome (success/failed/skipped), reason (why it failed).
     */
    private void emitReplayMessageMetric(String outcome, String reason) {
        if (meterRegistry == null) {
            return;
        }
        try {
            Counter.builder("tag.denorm.dlq.replay.messages")
                    .description("Tag denorm DLQ replay message outcomes")
                    .tag("outcome", outcome)
                    .tag("reason", reason != null ? reason : "none")
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            log.warn("Failed to emit DLQ replay message metric", e);
        }
    }

    /**
     * Emits a Prometheus counter for DLQ replay vertex outcomes.
     * Labels: outcome (repaired/failed), reason (failure detail).
     */
    private void emitReplayVertexMetric(String outcome, String reason, int count) {
        if (meterRegistry == null || count <= 0) {
            return;
        }
        try {
            Counter.builder("tag.denorm.dlq.replay.vertices")
                    .description("Tag denorm DLQ replay vertex outcomes")
                    .tag("outcome", outcome)
                    .tag("reason", reason != null ? reason : "none")
                    .register(meterRegistry)
                    .increment(count);
        } catch (Exception e) {
            log.warn("Failed to emit DLQ replay vertex metric", e);
        }
    }
}
