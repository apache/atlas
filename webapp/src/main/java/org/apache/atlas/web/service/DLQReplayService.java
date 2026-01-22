package org.apache.atlas.web.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.RankFeatureUtils;
import org.apache.atlas.util.RepairIndex;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.dlq.DLQEntry;
import org.janusgraph.diskstorage.dlq.SerializableIndexMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.type.Constants.INDEX_NAME_VERTEX_INDEX;

/**
 * Service for replaying DLQ messages back to Elasticsearch.
 * Uses Kafka consumer groups to ensure only one instance processes messages.
 */
@Service
public class DLQReplayService {

    private static final Logger log = LoggerFactory.getLogger(DLQReplayService.class);

    private String bootstrapServers;
    @Value("${atlas.kafka.dlq.topic:ATLAS_ES_DLQ}")
    private String dlqTopic="ATLAS_ES_DLQ";

    @Value("${atlas.kafka.dlq.consumerGroupId:atlas_dq_replay_group}")
    private String consumerGroupId= "atlas_dq_replay_group";

    @Value("${atlas.kafka.dlq.maxRetries:3}")
    private int maxRetries = 3;

    // Kafka consumer configuration
    @Value("${atlas.kafka.dlq.maxPollRecords:10}")
    private int maxPollRecords = 10;

    @Value("${atlas.kafka.dlq.maxPollIntervalMs:600000}")
    private int maxPollIntervalMs = 600000; // 10 minutes

    @Value("${atlas.kafka.dlq.sessionTimeoutMs:90000}")
    private int sessionTimeoutMs = 90000; // 90 seconds

    @Value("${atlas.kafka.dlq.heartbeatIntervalMs:30000}")
    private int heartbeatIntervalMs = 30000; // 30 seconds

    // Timing configuration
    @Value("${atlas.kafka.dlq.pollTimeoutSeconds:15}")
    private int pollTimeoutSeconds = 15;

    @Value("${atlas.kafka.dlq.shutdownWaitSeconds:60}")
    private int shutdownWaitSeconds = 60;

    @Value("${atlas.kafka.dlq.consumerCloseTimeoutSeconds:30}")
    private int consumerCloseTimeoutSeconds = 30;

    @Value("${atlas.kafka.dlq.errorBackoffMs:10000}")
    private int errorBackoffMs = 10000; // 10 seconds (for permanent exceptions)

    // Exponential backoff configuration for temporary exceptions
    @Value("${atlas.kafka.dlq.exponentialBackoff.baseDelayMs:1000}")
    private int exponentialBackoffBaseDelayMs = 1000; // 1 second

    @Value("${atlas.kafka.dlq.exponentialBackoff.maxDelayMs:60000}")
    private int exponentialBackoffMaxDelayMs = 60000; // 60 seconds

    @Value("${atlas.kafka.dlq.exponentialBackoff.multiplier:2.0}")
    private double exponentialBackoffMultiplier = 2.0;

    @Value("${atlas.kafka.dlq.trackerCleanupIntervalMs:300000}")
    private long trackerCleanupIntervalMs = 300000; // 5 minutes

    @Value("${atlas.kafka.dlq.trackerMaxAgeMs:3600000}")
    private long trackerMaxAgeMs = 3600000; // 1 hour

    @Value("${atlas.kafka.dlq.retryDelayMs:5000}")
    private int retryDelayMs = 5000; // 5 seconds

    //Track retry attempts with timestamps for cleanup
    private static class RetryTrackerEntry {
        int retryCount;
        long lastAttemptTime;

        RetryTrackerEntry(int retryCount) {
            this.retryCount = retryCount;
            this.lastAttemptTime = System.currentTimeMillis();
        }
    }

    private final Map<String, RetryTrackerEntry> retryTracker = new ConcurrentHashMap<>();

    // Track exponential backoff delay per partition-offset for temporary exceptions (in-memory)
    private final Map<String, Long> backoffTracker = new ConcurrentHashMap<>();

    // P1 Fix: Track last cleanup time
    private volatile long lastTrackerCleanupTime = System.currentTimeMillis();

    private volatile KafkaConsumer<String, String> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isHealthy = new AtomicBoolean(true);
    private volatile Thread replayThread;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicInteger skippedCount = new AtomicInteger(0);

    private ObjectMapper mapper;
    private RepairIndex repairIndex;
    private AtlasTypeRegistry typeRegistry;

    private final static String INDEX_NAME = "search";
    private static final String ASSET_TYPE_NAME = "Asset";

    public DLQReplayService(RepairIndex repairIndex, AtlasTypeRegistry typeRegistry) throws AtlasException {
        this.mapper = configureMapper();
        this.repairIndex = repairIndex;
        this.typeRegistry = typeRegistry;
        this.bootstrapServers = ApplicationProperties.get().getString("atlas.graph.kafka.bootstrap.servers");
    }

    /**
     * Start replaying DLQ messages
     */
    @PostConstruct
    public synchronized void startReplay() {
        if (isRunning.get()) {
            log.warn("DLQ replay is already running");
            return;
        }

        log.info("Starting DLQ replay service for topic: {} with consumer group: {}", dlqTopic, consumerGroupId);

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

                // Log offset information for each partition
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

        // Start processing in a separate thread
        replayThread = new Thread(this::processMessages, "DLQ-Replay-Thread");
        replayThread.setDaemon(false);
        replayThread.start();

        log.info("DLQ replay service started successfully");
    }

    /**
     * Gracefully shutdown the DLQ replay service with proper thread join
     */
    @PreDestroy
    public synchronized void shutdown() {
        if (!isRunning.get()) {
            log.info("DLQ replay service is not running, nothing to shutdown");
            return;
        }

        log.info("Shutting down DLQ replay service...");
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
                // P0 Fix: Calculate appropriate timeout (consumer close timeout + buffer for processing)
                long shutdownTimeoutMs = TimeUnit.SECONDS.toMillis(shutdownWaitSeconds);
                log.info("Waiting up to {}ms for replay thread to complete current work...", shutdownTimeoutMs);

                replayThread.join(shutdownTimeoutMs);

                if (replayThread.isAlive()) {
                    log.error("Replay thread did not terminate within timeout of {}ms! " +
                                    "Current message processing may be interrupted. Thread state: {}",
                            shutdownTimeoutMs, replayThread.getState());
                    // Thread will be forcefully terminated when JVM exits
                } else {
                    log.info("Replay thread terminated gracefully");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for replay thread to finish");
            }
        }

        // Step 3: Close consumer (thread should be done by now)
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(consumerCloseTimeoutSeconds));
                log.info("Kafka consumer closed successfully");
            } catch (Exception e) {
                log.error("Error closing Kafka consumer during shutdown", e);
            }
        }

        log.info("DLQ replay service shutdown complete. Total processed: {}, Total errors: {}, Total skipped: {}",
                processedCount.get(), errorCount.get(), skippedCount.get());
    }

    /**
     * Process messages from the DLQ topic using pause/resume pattern
     */
    private void processMessages() {
        log.info("DLQ replay thread started, polling for messages...");

        try {
            while (isRunning.get()) {
                try {
                    // P1 Fix: Periodic cleanup of stale tracker entries
                    cleanupStaleTrackersIfNeeded();

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));

                    if (records.isEmpty()) {
                        // Log why we got no records
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

                    log.debug("Received {} DLQ messages to replay", records.count());

                    // PAUSE consumption immediately to prevent timeout during processing
                    Set<TopicPartition> pausedPartitions = consumer.assignment();
                    consumer.pause(pausedPartitions);
                    log.info("Paused consumption on partitions: {} to process messages", pausedPartitions);

                    Long failedOffset = null;  // Track if we need to seek back
                    TopicPartition failedPartition = null;

                    try {
                        // Now process without time pressure - heartbeats continue automatically
                        for (ConsumerRecord<String, String> record : records) {
                            String retryKey = record.partition() + "-" + record.offset();

                            try {
                                log.info("Processing DLQ entry at offset: {} from partition: {}",
                                        record.offset(), record.partition());

                                long processingStartTime = System.currentTimeMillis();
                                replayDLQEntry(record.value());
                                long processingTime = System.currentTimeMillis() - processingStartTime;

                                processedCount.incrementAndGet();

                                // Success - remove from retry tracker, reset backoff, and commit offset
                                retryTracker.remove(retryKey);
                                resetExponentialBackoff(retryKey);

                                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                                        new TopicPartition(record.topic(), record.partition()),
                                        new OffsetAndMetadata(record.offset() + 1)
                                );
                                consumer.commitSync(offsets);

                                log.debug("Successfully replayed DLQ entry (offset: {}, partition: {}) in {}ms",
                                        record.offset(), record.partition(), processingTime);

                            } catch (TemporaryBackendException temporaryBackendException) {
                                // Treat temporary backend exceptions as transient - will retry with exponential backoff
                                errorCount.incrementAndGet();

                                // Calculate exponential backoff delay
                                long backoffDelay = calculateExponentialBackoff(retryKey);

                                log.warn("Temporary backend exception while replaying DLQ entry (offset: {}, partition: {}). " +
                                                "Will retry on next poll after {}ms backoff (exponential). STOPPING batch processing to prevent skipping this message. Error: {}",
                                        record.offset(), record.partition(), backoffDelay, temporaryBackendException.getMessage());

                                // Mark for seek-back - consumer position has already advanced past this offset
                                failedOffset = record.offset();
                                failedPartition = new TopicPartition(record.topic(), record.partition());

                                Thread.sleep(backoffDelay);
                                break;
                            } catch (Exception e) {
                                errorCount.incrementAndGet();

                                //Track retry attempts with timestamp for this specific offset
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
                                    //Clean up both trackers for poison pill
                                    log.error("DLQ entry at offset {} partition {} failed {} times (max retries reached). " +
                                                    "SKIPPING this message to prevent partition blockage. Error: {}",
                                            record.offset(), record.partition(), retryCount, e.getMessage(), e);

                                    skippedCount.incrementAndGet();
                                    retryTracker.remove(retryKey);
                                    resetExponentialBackoff(retryKey); // P0 Fix: Clean up backoff tracker too

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
                                    //Add delay before retrying to avoid tight retry loop
                                    log.warn("Failed to replay DLQ entry (offset: {}, partition: {}). Retry {}/{}. " +
                                                    "STOPPING batch processing to prevent skipping this message. Will retry on next poll after {}ms delay. Error: {}",
                                            record.offset(), record.partition(), retryCount, maxRetries, retryDelayMs, e.getMessage());

                                    // Mark for seek-back - consumer position has already advanced past this offset
                                    failedOffset = record.offset();
                                    failedPartition = new TopicPartition(record.topic(), record.partition());

                                    //Sleep before breaking to avoid tight retry loop
                                    Thread.sleep(retryDelayMs);
                                    break;
                                }
                            }
                        }
                    } finally {
                        // The consumer's position advances when records are polled, not when committed
                        // We need to rewind to retry the failed message
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
                    // Expected during shutdown - exit gracefully
                    log.info("Kafka consumer wakeup called, exiting processing loop");
                    break;
                } catch (Exception e) {
                    log.error("Error in DLQ replay processing loop", e);
                    try {
                        // Back off before retry
                        Thread.sleep(errorBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("DLQ replay thread interrupted during error recovery");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in DLQ replay thread - marking service as unhealthy", e);
            isHealthy.set(false);
        } finally {
            boolean wasRunning = isRunning.get();
            isRunning.set(false);

            if (wasRunning && !isHealthy.get()) {
                log.error("DLQ replay thread terminated unexpectedly! Service is unhealthy. Pod should be restarted.");
            }

            log.info("DLQ replay thread finished. Total processed: {}, Total errors: {}, Total skipped: {}",
                    processedCount.get(), errorCount.get(), skippedCount.get());
        }
    }

    /**
     * P1 Fix: Periodic cleanup of stale tracker entries to prevent memory leak
     */
    private void cleanupStaleTrackersIfNeeded() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTrackerCleanupTime > trackerCleanupIntervalMs) {
            int retryTrackerSizeBefore = retryTracker.size();
            int backoffTrackerSizeBefore = backoffTracker.size();

            long cutoffTime = currentTime - trackerMaxAgeMs;

            // Clean up stale retry tracker entries
            retryTracker.entrySet().removeIf(entry -> entry.getValue().lastAttemptTime < cutoffTime);

            // Clean up stale backoff tracker entries (we don't have timestamps, so this is best-effort)
            // In practice, backoff entries get cleaned on success or poison pill, so this is just safety net
            if (backoffTracker.size() > retryTracker.size() * 2) {
                log.warn("Backoff tracker size ({}) is much larger than retry tracker size ({}), clearing orphaned entries",
                        backoffTracker.size(), retryTracker.size());
                // Keep only entries that have corresponding retry tracker entries
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
     * Replay a single DLQ entry
     */
    private void replayDLQEntry(String dlqJson) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            DLQEntry entry = mapper.readValue(dlqJson, DLQEntry.class);
            log.info("Replaying DLQ entry for index: {}, store: {}", entry.getIndexName(), entry.getStoreName());

            Map<String, SerializableIndexMutation> vertexIndex = entry.getMutations().get("vertex_index");
            if (MapUtils.isNotEmpty(vertexIndex)) {
                Set<String> vertexIds = new HashSet<>();
                for (Map.Entry<String, SerializableIndexMutation> ve : vertexIndex.entrySet()) {
                    log.debug("DLQ Entry Vertex Index Mutation - DocID: {}, Additions: {}, Deletions: {}",
                            ve.getKey(), ve.getValue().getAdditions().size(), ve.getValue().getDeletions().size());

                    // Check if any addition has an invalid rank_feature value using typedef-aware validation
                    if (hasInvalidRankFeatureValue(ve.getValue())) {
                        log.warn("Skipping vertex ID {} due to invalid rank_feature value", ve.getKey());
                    } else {
                        vertexIds.add(AtlasGraphUtilsV2.getVertexIdForDocId(ve.getKey()));
                    }
                }
                repairIndex.reindexVerticesByIds(INDEX_NAME_VERTEX_INDEX, vertexIds);
                log.debug("Replayed vertex index mutations for {} vertices", vertexIds.size());
            }
            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Successfully replayed mutation for index: {}. Total time: {}ms", entry.getIndexName(), totalTime);
        } catch (TemporaryBackendException e) {
            // Already a TemporaryBackendException from JanusGraph - rethrow as-is
            log.warn("Temporary backend exception replaying DLQ entry: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            // Other exceptions - might be permanent (bad data, schema issues, etc.)
            log.error("Error replaying DLQ entry - treating as permanent failure", e);
            throw e;
        }
    }

    /**
     * Checks if a vertex mutation contains any invalid rank_feature values.
     * Uses the typedef registry to determine which fields are rank_feature fields
     * and validates their values against the minimum threshold.
     *
     * @param mutation the index mutation to check
     * @return true if any addition has an invalid rank_feature value
     */
    private boolean hasInvalidRankFeatureValue(SerializableIndexMutation mutation) {
        if (mutation == null || mutation.getAdditions() == null) {
            return false;
        }

        AtlasEntityType assetType = typeRegistry.getEntityTypeByName(ASSET_TYPE_NAME);
        if (assetType == null) {
            log.debug("Asset type not found in registry, skipping rank_feature validation");
            return false;
        }

        for (SerializableIndexMutation.SerializableIndexEntry addition : mutation.getAdditions()) {
            String fieldName = addition.getField();
            Object value = addition.getValue();

            if (value instanceof Number) {
                AtlasAttributeDef attrDef = assetType.getAttributeDef(fieldName);
                if (attrDef != null && RankFeatureUtils.isRankFeatureField(attrDef)) {
                    if (!RankFeatureUtils.isValidRankFeatureValue((Number) value, attrDef)) {
                        log.debug("Invalid rank_feature value for field '{}': {} (minimum: {})",
                                fieldName, value, RankFeatureUtils.getMinimumValue(attrDef));
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Calculate exponential backoff delay for a failed message
     * @param retryKey The partition-offset key
     * @return The delay in milliseconds
     */
    private long calculateExponentialBackoff(String retryKey) {
        long currentDelay = backoffTracker.getOrDefault(retryKey, (long) exponentialBackoffBaseDelayMs);
        long nextDelay = (long) (currentDelay * exponentialBackoffMultiplier);

        // Cap at maximum delay
        nextDelay = Math.min(nextDelay, exponentialBackoffMaxDelayMs);

        // Store for next time
        backoffTracker.put(retryKey, nextDelay);

        return currentDelay; // Return current delay, store next delay for future use
    }

    /**
     * Reset exponential backoff for a successfully processed message
     * @param retryKey The partition-offset key
     */
    private void resetExponentialBackoff(String retryKey) {
        backoffTracker.remove(retryKey);
    }

    /**
     * Health check for liveness/readiness probes
     * @return true if the replay thread is healthy and running
     */
    public boolean isHealthy() {
        // Check if thread is alive and healthy
        boolean threadAlive = replayThread != null && replayThread.isAlive();
        return isHealthy.get() && threadAlive;
    }

    /**
     * Get replay status
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
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

        // Exponential backoff configuration
        Map<String, Object> backoffConfig = new HashMap<>();
        backoffConfig.put("baseDelayMs", exponentialBackoffBaseDelayMs);
        backoffConfig.put("maxDelayMs", exponentialBackoffMaxDelayMs);
        backoffConfig.put("multiplier", exponentialBackoffMultiplier);
        status.put("exponentialBackoffConfig", backoffConfig);

        return status;
    }

    private ObjectMapper configureMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Configure to handle property name differences
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Add custom deserializer for SerializableIndexMutation
        SimpleModule module = new SimpleModule();
        module.addDeserializer(SerializableIndexMutation.class, new JsonDeserializer<>() {
            @Override
            public SerializableIndexMutation deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode node = p.getCodec().readTree(p);

                // Handle both "new" and "isNew" fields
                boolean isNew = node.has("new") ? node.get("new").asBoolean() :
                        node.has("isNew") ? node.get("isNew").asBoolean() : false;

                boolean isDeleted = node.has("isDeleted") ? node.get("isDeleted").asBoolean() : false;

                List<SerializableIndexMutation.SerializableIndexEntry> additions = new ArrayList<>();
                List<SerializableIndexMutation.SerializableIndexEntry> deletions = new ArrayList<>();

                if (node.has("additions") && node.get("additions").isArray()) {
                    for (JsonNode entry : node.get("additions")) {
                        additions.add(new SerializableIndexMutation.SerializableIndexEntry(
                                entry.get("field").asText(),
                                mapper.treeToValue(entry.get("value"), Object.class)
                        ));
                    }
                }

                if (node.has("deletions") && node.get("deletions").isArray()) {
                    for (JsonNode entry : node.get("deletions")) {
                        deletions.add(new SerializableIndexMutation.SerializableIndexEntry(
                                entry.get("field").asText(),
                                mapper.treeToValue(entry.get("value"), Object.class)
                        ));
                    }
                }

                return new SerializableIndexMutation(isNew, isDeleted, additions, deletions);
            }
        });
        mapper.registerModule(module);
        return mapper;
    }

}