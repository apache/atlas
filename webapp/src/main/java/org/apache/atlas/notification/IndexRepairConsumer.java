/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.notification.AtlasDistributedTaskNotification;
import org.apache.atlas.model.notification.AtlasDistributedTaskNotification.AtlasTaskType;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.Service;
import org.apache.atlas.util.RepairIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;

/**
 * Consumes REINDEX_REPAIRED_ATTRIBUTES messages from the ATLAS_DISTRIBUTED_TASKS Kafka topic
 * and triggers controlled reindex of existing entities whose mixed index property keys were
 * self-healed by Phase 2a.
 *
 * Key design decisions:
 * - Kafka consumer group ensures only ONE pod receives each message (single-partition topic)
 * - Reindexing is idempotent (restoreByIds re-writes same data to ES) — no dedup needed
 * - Batch-and-throttle pattern prevents ES overload (configurable batch size + delay)
 * - Fire-and-forget from producer side: if consumer can't process, repairs are logged
 *   at WARN level in GraphBackedSearchIndexer for manual intervention
 */
@Component
@Order(6)
@DependsOn(value = {"atlasTypeDefStoreInitializer", "atlasTypeDefGraphStoreV2"})
public class IndexRepairConsumer implements Service, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IndexRepairConsumer.class);

    private static final String METRIC_PREFIX = "atlas_index_repair";

    private final NotificationInterface notificationInterface;
    private final RepairIndex repairIndex;
    private final AtlasGraph graph;

    private ExecutorService executorService;
    private volatile NotificationConsumer<AtlasDistributedTaskNotification> kafkaConsumer;
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    // Metrics
    private final Counter jobsStarted;
    private final Counter jobsCompleted;
    private final Counter jobsFailed;
    private final Counter entitiesReindexed;

    @Inject
    public IndexRepairConsumer(NotificationInterface notificationInterface,
                               RepairIndex repairIndex,
                               AtlasGraph graph) {
        this.notificationInterface = notificationInterface;
        this.repairIndex = repairIndex;
        this.graph = graph;

        MeterRegistry registry = getMeterRegistry();
        Tags tags = Tags.of("tenant", System.getenv().getOrDefault("DOMAIN_NAME", "default"));

        this.jobsStarted = Counter.builder(METRIC_PREFIX + "_jobs_total")
                .description("Index repair jobs by status")
                .tags(tags).tag("status", "started").register(registry);
        this.jobsCompleted = Counter.builder(METRIC_PREFIX + "_jobs_total")
                .tags(tags).tag("status", "completed").register(registry);
        this.jobsFailed = Counter.builder(METRIC_PREFIX + "_jobs_total")
                .tags(tags).tag("status", "failed").register(registry);
        this.entitiesReindexed = Counter.builder(METRIC_PREFIX + "_entities_reindexed_total")
                .description("Total entities reindexed by index repair")
                .tags(tags).register(registry);
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("IndexRepairConsumer: Service.start() called");
        instanceIsActive();
    }

    @Override
    public void stop() {
        LOG.info("IndexRepairConsumer: Service.stop() called");
        shutdown();
    }

    @Override
    public void instanceIsActive() {
        if (!AtlasConfiguration.INDEX_REPAIR_CONSUMER_ENABLED.getBoolean()) {
            LOG.info("IndexRepairConsumer is disabled (atlas.index.repair.consumer.enabled=false)");
            return;
        }

        LOG.info("IndexRepairConsumer: Reacting to active state — starting consumer");
        shouldRun.set(true);
        startConsumer();
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("IndexRepairConsumer: Reacting to passive state — stopping consumer");
        shutdown();
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.INDEX_REPAIR_CONSUMER.getOrder();
    }

    private void startConsumer() {
        if (executorService != null && !executorService.isShutdown()) {
            LOG.warn("IndexRepairConsumer: Consumer already running");
            return;
        }
        executorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "index-repair-consumer");
            t.setDaemon(true);
            return t;
        });
        executorService.submit(this::consumerLoop);
    }

    private void shutdown() {
        shouldRun.set(false);

        // Close Kafka consumer first — unblocks the poll() call and triggers
        // immediate rebalance so the other pod gets the partition without waiting
        // for session timeout (30s).
        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close();
            } catch (Exception e) {
                LOG.warn("IndexRepairConsumer: Error closing Kafka consumer", e);
            }
        }

        if (executorService != null) {
            executorService.shutdownNow();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void consumerLoop() {
        LOG.info("IndexRepairConsumer: Consumer loop started on pod {}",
                System.getenv().getOrDefault("HOSTNAME", "unknown-pod"));

        List<NotificationConsumer<AtlasDistributedTaskNotification>> consumers = null;

        try {
            consumers = notificationInterface.createConsumers(
                    NotificationType.ATLAS_DISTRIBUTED_TASKS, 1);
        } catch (Exception e) {
            LOG.error("IndexRepairConsumer: Failed to create Kafka consumers", e);
            return;
        }

        if (consumers == null || consumers.isEmpty()) {
            LOG.error("IndexRepairConsumer: No consumers created for ATLAS_DISTRIBUTED_TASKS");
            return;
        }

        NotificationConsumer<AtlasDistributedTaskNotification> consumer = consumers.get(0);
        this.kafkaConsumer = consumer; // Store ref for shutdown()
        long pollTimeoutMs = 5000L; // 5s Kafka poll

        while (shouldRun.get()) {
            try {
                List<AtlasKafkaMessage<AtlasDistributedTaskNotification>> messages = consumer.receive(pollTimeoutMs);

                if (messages == null || messages.isEmpty()) {
                    continue;
                }

                for (AtlasKafkaMessage<AtlasDistributedTaskNotification> message : messages) {
                    AtlasDistributedTaskNotification notification = message.getMessage();
                    if (notification == null) {
                        continue;
                    }

                    if (AtlasTaskType.REINDEX_REPAIRED_ATTRIBUTES.equals(notification.getTaskType())) {
                        processReindexMessage(notification);
                    }
                    // Commit offset after processing each message
                    consumer.commit(message.getTopicPartition(), message.getOffset() + 1);
                }
            } catch (IllegalStateException e) {
                LOG.info("IndexRepairConsumer: Consumer closed, exiting loop");
                break;
            } catch (Exception e) {
                LOG.error("IndexRepairConsumer: Error in consumer loop", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        LOG.info("IndexRepairConsumer: Consumer loop exited");
    }

    @SuppressWarnings("unchecked")
    private void processReindexMessage(AtlasDistributedTaskNotification notification) {
        Map<String, Object> params = notification.getParameters();
        if (params == null) {
            LOG.warn("IndexRepairConsumer: Received REINDEX_REPAIRED_ATTRIBUTES with null parameters");
            return;
        }

        List<Map<String, Object>> repairs = (List<Map<String, Object>>) params.get("repairs");
        if (repairs == null || repairs.isEmpty()) {
            LOG.warn("IndexRepairConsumer: Received REINDEX_REPAIRED_ATTRIBUTES with empty repairs list");
            return;
        }

        String sourcePod = (String) params.getOrDefault("podId", "unknown");
        LOG.info("IndexRepairConsumer: Processing reindex message from pod {} with {} repair(s)",
                sourcePod, repairs.size());

        // No Redis lock needed — Kafka consumer group already guarantees single-partition
        // delivery to one consumer. Deduplication is handled per-repair in processOneRepair().
        for (Map<String, Object> repair : repairs) {
            processOneRepair(repair);
        }
    }

    @SuppressWarnings("unchecked")
    private void processOneRepair(Map<String, Object> repair) {
        String vertexPropertyName = (String) repair.get("vertexPropertyName");
        List<String> affectedEntityTypes = (List<String>) repair.get("affectedEntityTypes");
        Object tsObj = repair.get("repairTimestamp");
        long repairTimestamp = (tsObj instanceof Number) ? ((Number) tsObj).longValue() : 0L;
        String attributeName = (String) repair.getOrDefault("attributeName", "unknown");

        if (vertexPropertyName == null || affectedEntityTypes == null || affectedEntityTypes.isEmpty()) {
            LOG.warn("IndexRepairConsumer: Skipping incomplete repair record: {}", repair);
            return;
        }

        LOG.info("INDEX REPAIR STARTED: attribute={}, vertexPropertyName={}, affectedTypes={}, repairTimestamp={}",
                attributeName, vertexPropertyName, affectedEntityTypes, repairTimestamp);
        jobsStarted.increment();

        int batchSize = AtlasConfiguration.INDEX_REPAIR_BATCH_SIZE.getInt();
        long batchDelayMs = AtlasConfiguration.INDEX_REPAIR_BATCH_DELAY_MS.getLong();
        int totalReindexed = 0;

        try {
            for (String typeName : affectedEntityTypes) {
                int reindexedForType = reindexEntitiesByType(typeName, batchSize, batchDelayMs);
                totalReindexed += reindexedForType;
            }

            entitiesReindexed.increment(totalReindexed);
            jobsCompleted.increment();

            LOG.info("INDEX REPAIR COMPLETED: attribute={}, vertexPropertyName={}, "
                    + "totalEntitiesReindexed={}, affectedTypes={}",
                    attributeName, vertexPropertyName, totalReindexed, affectedEntityTypes);
        } catch (Exception e) {
            jobsFailed.increment();
            LOG.error("INDEX REPAIR FAILED: attribute={}, vertexPropertyName={}, "
                    + "reindexedSoFar={}, error={}",
                    attributeName, vertexPropertyName, totalReindexed, e.getMessage(), e);
        }
    }

    /**
     * Streams entity vertices from Cassandra and reindexes in batches.
     * Does NOT load all GUIDs into memory — iterates the graph query result on-the-fly.
     */
    private int reindexEntitiesByType(String typeName, int batchSize, long batchDelayMs) throws Exception {
        LOG.info("IndexRepairConsumer: Reindexing entities of type {}", typeName);

        Set<String> guidBatch = new LinkedHashSet<>();
        int totalReindexed = 0;
        int batchCount = 0;

        try {
            // Use .vertices() directly — avoids a second Cassandra read that .vertexIds() + getVertex() would cause
            Iterable<AtlasVertex> vertices = graph.query()
                    .has(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName)
                    .vertices();

            for (AtlasVertex vertex : vertices) {
                try {
                    String guid = vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
                    if (guid == null) continue;

                    guidBatch.add(guid);
                } catch (Exception e) {
                    LOG.warn("IndexRepairConsumer: Failed to get GUID from vertex: {}",
                            e.getMessage());
                    continue;
                }

                if (guidBatch.size() >= batchSize) {
                    repairIndex.restoreByIds(guidBatch);
                    totalReindexed += guidBatch.size();
                    batchCount++;
                    guidBatch.clear();

                    if (batchCount % 100 == 0) {
                        LOG.info("IndexRepairConsumer: Progress for type {}: {} entities reindexed ({} batches)",
                                typeName, totalReindexed, batchCount);
                    }

                    if (batchDelayMs > 0) {
                        Thread.sleep(batchDelayMs);
                    }

                    if (!shouldRun.get()) {
                        LOG.warn("IndexRepairConsumer: Shutdown requested during reindex of type {}. "
                                + "Processed {} entities so far.", typeName, totalReindexed);
                        return totalReindexed;
                    }
                }
            }

            // Process remaining batch
            if (!guidBatch.isEmpty() && shouldRun.get()) {
                repairIndex.restoreByIds(guidBatch);
                totalReindexed += guidBatch.size();
                guidBatch.clear();
            }

            LOG.info("IndexRepairConsumer: Completed reindex for type {}: {} entities in {} batches",
                    typeName, totalReindexed, batchCount + 1);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("IndexRepairConsumer: Interrupted during reindex of type {}", typeName);
            throw e;
        }

        return totalReindexed;
    }

}
