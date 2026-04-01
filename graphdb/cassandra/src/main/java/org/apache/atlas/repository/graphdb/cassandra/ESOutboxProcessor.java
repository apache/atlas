package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lease-guarded background processor that polls the es_outbox PENDING partition
 * and retries ES sync. Only one pod runs this at a time (via JobLeaseManager).
 *
 * Adaptive polling:
 * - Idle mode (PENDING empty):  polls every 30s — minimal Cassandra I/O
 * - Drain mode (PENDING has entries): polls every 2s with batch size 500 — fast recovery
 *
 * With 500/batch at 2s intervals, drain throughput is ~15,000 entries/min.
 * This handles 8 pods each generating 1,000 failures/min during an ES outage.
 *
 * Poison-pill safe: each entry's JSON is validated individually before being added
 * to the bulk request. Malformed entries are marked FAILED immediately.
 *
 * Lifecycle: call {@link #start()} once at application startup, {@link #stop()} at shutdown.
 */
public class ESOutboxProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ESOutboxProcessor.class);

    // Adaptive polling: idle vs drain mode
    private static final int IDLE_POLL_SECONDS = 30;
    private static final int DRAIN_POLL_SECONDS = 2;
    private static final int IDLE_BATCH_SIZE = 100;
    private static final int DRAIN_BATCH_SIZE = 500;

    // Lease TTL must exceed the longest possible poll+process cycle.
    // Drain mode: 2s poll + up to ~5s for a 500-entry bulk request = ~7s.
    // Use 60s for safety — if the holder crashes, another pod takes over within 60s.
    private static final int LEASE_TTL_SECONDS = 60;

    // Number of consecutive empty polls before switching from drain → idle
    private static final int EMPTY_POLLS_BEFORE_IDLE = 3;

    private final ESOutboxRepository outboxRepository;
    private final JobLeaseManager leaseManager;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Adaptive state
    private volatile boolean drainMode = false;
    private volatile int consecutiveEmptyPolls = 0;
    private volatile ScheduledFuture<?> currentTask;

    public ESOutboxProcessor(ESOutboxRepository outboxRepository, JobLeaseManager leaseManager) {
        this.outboxRepository = outboxRepository;
        this.leaseManager = leaseManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "es-outbox-processor");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduleNext(IDLE_POLL_SECONDS);
            LOG.info("ESOutboxProcessor started (idle={}s, drain={}s, idle_batch={}, drain_batch={})",
                    IDLE_POLL_SECONDS, DRAIN_POLL_SECONDS, IDLE_BATCH_SIZE, DRAIN_BATCH_SIZE);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            ScheduledFuture<?> task = currentTask;
            if (task != null) {
                task.cancel(false);
            }
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("ESOutboxProcessor stopped");
        }
    }

    private void scheduleNext(int delaySeconds) {
        if (running.get()) {
            currentTask = scheduler.schedule(this::pollCycle, delaySeconds, TimeUnit.SECONDS);
        }
    }

    private void pollCycle() {
        if (!running.get()) return;

        try {
            processPendingEntries();
        } finally {
            // Schedule the next poll based on current mode
            if (running.get()) {
                scheduleNext(drainMode ? DRAIN_POLL_SECONDS : IDLE_POLL_SECONDS);
            }
        }
    }

    private void processPendingEntries() {
        if (!leaseManager.tryAcquire("es-outbox-processor", LEASE_TTL_SECONDS)) {
            return; // Another pod holds the lease
        }
        try {
            int batchSize = drainMode ? DRAIN_BATCH_SIZE : IDLE_BATCH_SIZE;
            List<ESOutboxRepository.OutboxEntry> entries = outboxRepository.getPendingEntries(batchSize);

            if (entries.isEmpty()) {
                consecutiveEmptyPolls++;
                if (drainMode && consecutiveEmptyPolls >= EMPTY_POLLS_BEFORE_IDLE) {
                    drainMode = false;
                    LOG.info("ESOutboxProcessor: PENDING drained, switching to idle mode (poll every {}s)",
                            IDLE_POLL_SECONDS);
                }
                return;
            }

            // Entries found — switch to drain mode
            consecutiveEmptyPolls = 0;
            if (!drainMode) {
                drainMode = true;
                LOG.info("ESOutboxProcessor: PENDING entries detected, switching to drain mode " +
                        "(poll every {}s, batch size {})", DRAIN_POLL_SECONDS, DRAIN_BATCH_SIZE);
            }

            LOG.info("ESOutboxProcessor: processing {} pending entries (drain mode)", entries.size());

            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                LOG.warn("ESOutboxProcessor: ES client not available, will retry next cycle");
                return;
            }

            String indexName = Constants.VERTEX_INDEX_NAME;

            // Phase 1: Triage — separate exhausted, poison, and valid entries
            StringBuilder bulkBody = new StringBuilder();
            Map<String, ESOutboxRepository.OutboxEntry> entryMap = new LinkedHashMap<>();

            for (ESOutboxRepository.OutboxEntry entry : entries) {
                // Exhausted: hit max attempts → mark FAILED, remove from batch
                if (entry.attemptCount >= ESOutboxRepository.MAX_ATTEMPTS) {
                    outboxRepository.markFailed(entry.vertexId, entry.attemptCount);
                    LOG.error("ESOutboxProcessor: marking vertex '{}' as FAILED after {} attempts",
                            entry.vertexId, entry.attemptCount);
                    continue;
                }

                // Validate entry before adding to bulk body — catch poison pills
                if (!appendToBulk(bulkBody, entry, indexName)) {
                    // Poison pill: malformed JSON or missing data — mark FAILED immediately
                    outboxRepository.markFailed(entry.vertexId, entry.attemptCount);
                    LOG.error("ESOutboxProcessor: marking vertex '{}' as FAILED (poison pill: invalid entry data)",
                            entry.vertexId);
                    continue;
                }

                entryMap.put(entry.vertexId, entry);
            }

            if (entryMap.isEmpty()) {
                return;
            }

            // Phase 2: Send bulk request to ES
            try {
                Request bulkReq = new Request("POST", "/_bulk");
                bulkReq.setEntity(new StringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON));
                Response resp = client.performRequest(bulkReq);
                int status = resp.getStatusLine().getStatusCode();
                String respBody = EntityUtils.toString(resp.getEntity());

                if (status >= 200 && status < 300) {
                    if (respBody != null && respBody.contains("\"errors\":true")) {
                        // Partial success — parse per-item results
                        Set<String> failedIds = parseFailedIds(respBody);
                        List<String> succeededIds = new ArrayList<>();
                        for (String id : entryMap.keySet()) {
                            if (!failedIds.contains(id)) {
                                succeededIds.add(id);
                            }
                        }
                        outboxRepository.batchMarkDone(succeededIds);
                        for (String failedId : failedIds) {
                            ESOutboxRepository.OutboxEntry entry = entryMap.get(failedId);
                            if (entry != null) {
                                outboxRepository.incrementAttempt(failedId, entry.attemptCount + 1);
                            }
                        }
                        LOG.info("ESOutboxProcessor: {} succeeded, {} failed (will retry)",
                                succeededIds.size(), failedIds.size());
                    } else {
                        // All succeeded
                        outboxRepository.batchMarkDone(new ArrayList<>(entryMap.keySet()));
                        LOG.info("ESOutboxProcessor: all {} entries synced to ES", entryMap.size());
                    }
                } else {
                    // Entire request failed — increment attempt count for all
                    for (ESOutboxRepository.OutboxEntry entry : entryMap.values()) {
                        outboxRepository.incrementAttempt(entry.vertexId, entry.attemptCount + 1);
                    }
                    LOG.warn("ESOutboxProcessor: bulk request failed with status {}, will retry {} entries",
                            status, entryMap.size());
                }
            } catch (Exception e) {
                // Connection/IO error — increment attempt count for all
                for (ESOutboxRepository.OutboxEntry entry : entryMap.values()) {
                    outboxRepository.incrementAttempt(entry.vertexId, entry.attemptCount + 1);
                }
                LOG.warn("ESOutboxProcessor: bulk request error, will retry {} entries: {}",
                        entryMap.size(), e.getMessage());
            }
        } catch (Exception e) {
            LOG.error("ESOutboxProcessor: unexpected error during processing", e);
        } finally {
            leaseManager.release("es-outbox-processor");
        }
    }

    /**
     * Validates and appends a single entry to the bulk NDJSON body.
     * Returns false if the entry is malformed (poison pill) — caller should mark it FAILED.
     */
    private boolean appendToBulk(StringBuilder bulkBody, ESOutboxRepository.OutboxEntry entry, String indexName) {
        try {
            if (entry.vertexId == null || entry.vertexId.isEmpty()) {
                return false;
            }

            if (ESOutboxRepository.ACTION_DELETE.equals(entry.action)) {
                bulkBody.append("{\"delete\":{\"_index\":\"").append(indexName)
                        .append("\",\"_id\":\"").append(entry.vertexId).append("\"}}\n");
                return true;
            }

            // Index action — validate JSON payload
            String json = entry.propertiesJson;
            if (json == null || json.isEmpty()) {
                return false;
            }

            // Verify it's parseable JSON (catches truncated/corrupt payloads)
            if (AtlasType.fromJson(json, Map.class) == null) {
                return false;
            }

            // Ensure no raw newlines that would break NDJSON format
            String safeJson = json.indexOf('\n') >= 0 ? json.replace('\n', ' ') : json;

            bulkBody.append("{\"index\":{\"_index\":\"").append(indexName)
                    .append("\",\"_id\":\"").append(entry.vertexId).append("\"}}\n");
            bulkBody.append(safeJson).append("\n");
            return true;
        } catch (Exception e) {
            LOG.warn("ESOutboxProcessor: invalid entry for vertex '{}': {}", entry.vertexId, e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> parseFailedIds(String respBody) {
        Set<String> failedIds = new LinkedHashSet<>();
        try {
            Map<String, Object> bulkResp = AtlasType.fromJson(respBody, Map.class);
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResp.get("items");
            if (items == null) return failedIds;

            for (Map<String, Object> item : items) {
                Map<String, Object> action = (Map<String, Object>) item.values().iterator().next();
                if (action != null && action.containsKey("error")) {
                    failedIds.add(String.valueOf(action.get("_id")));
                }
            }
        } catch (Exception e) {
            LOG.warn("ESOutboxProcessor: failed to parse bulk response: {}", e.getMessage());
        }
        return failedIds;
    }
}
