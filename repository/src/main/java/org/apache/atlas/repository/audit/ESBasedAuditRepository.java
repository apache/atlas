/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.EntityAuditEvent;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.atlas.repository.Constants.CATALOG_DATASET_GUID_ATTR;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_BACKOFF_BASE_MS;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_BACKOFF_MAX_MS;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_ENABLED;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_MAX_RETRIES;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_PUBLISH_TO_KAFKA_ENABLED;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_QUEUE_CAPACITY;
import static org.apache.atlas.AtlasConfiguration.ENTITY_AUDIT_DLQ_TOPIC;
import static org.apache.atlas.repository.Constants.DOMAIN_GUIDS;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.INDEX_BACKEND_CONF;
import static org.springframework.util.StreamUtils.copyToString;

/**
 * This class provides cassandra support as the backend for audit storage support.
 */
@Singleton
@Component
@Order(8)
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepositorySearch.impl")
public class ESBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ESBasedAuditRepository.class);
    public static final String INDEX_WRITE_BACKEND_CONF = "atlas.graph.index.search.write.hostname";
    private static final String TOTAL_FIELD_LIMIT = "atlas.index.audit.elasticsearch.total_field_limit";
    public static final String INDEX_NAME = "entity_audits";
    private static final String ENTITYID = "entityId";
    private static final String TYPE_NAME = "typeName";
    private static final String ENTITY_QUALIFIED_NAME = "entityQualifiedName";
    private static final String CREATED = "created";
    private static final String TIMESTAMP = "timestamp";
    private static final String EVENT_KEY = "eventKey";
    private static final String ACTION = "action";
    private static final String USER = "user";
    private static final String DETAIL = "detail";
    private static final String ENTITY = "entity";
    private static final String bulkMetadata = String.format("{ \"index\" : { \"_index\" : \"%s\" } }%n", INDEX_NAME);
    private static final Set<String> ALLOWED_LINKED_ATTRIBUTES = new HashSet<>(Arrays.asList(DOMAIN_GUIDS, CATALOG_DATASET_GUID_ATTR));
    private static final String ENTITY_AUDITS_INDEX = "entity_audits";
    private static final String NIOFS_MIGRATION_MARKER_ID = "entity_audits_niofs_migrated";
    private static final int DLQ_POLL_TIMEOUT_SECONDS = 5;

    /**
     * ES error types that are non-retriable (mapping/parsing/index). Sending these to DLQ would create poison pills.
     * Aligned with AtlanElasticSearchIndex permanent-error list and DLQReplayService poison-pill handling.
     * Includes: mapping/parsing (same payload will always fail), index_not_found / invalid_index_name (config/setup;
     * retry won't fix without human intervention).
     */
    private static final Set<String> MAPPING_OR_PERMANENT_ES_ERROR_TYPES = Set.of(
            "mapper_parsing_exception",
            "illegal_argument_exception",
            "parsing_exception",
            "strict_dynamic_mapping_exception",
            "version_conflict",
            "index_not_found_exception",
            "invalid_index_name_exception");
    private static final List<String> SOURCE_FIELDS = Arrays.asList(
            ENTITYID, ACTION, DETAIL, USER, CREATED, TIMESTAMP,
            TYPE_NAME, ENTITY_QUALIFIED_NAME, EVENT_KEY, "headers"
    );

    /*
    *    created   → event creation time
         timestamp → entity modified timestamp
         eventKey  → entityId:timestamp
    * */

    /** Metric name for entity audit DLQ outcomes (Victoria Metrics / Prometheus). */
    private static final String METRIC_ENTITY_AUDIT_DLQ = "atlas.entity.audit.dlq.events";

    /** Holder for failed audit events enqueued to async DLQ for retry. */
    private static record EntityAuditDLQEntry(List<EntityAuditEventV2> events, int retryCount) {}

    /**
     * Record audit DLQ failure metric to the existing Micrometer/Prometheus registry (scraped by Victoria Metrics).
     * Does not throw; failures are logged and ignored.
     */
    private static void recordAuditDlqMetric(String outcome, int eventCount) {
        if (eventCount <= 0) {
            return;
        }
        try {
            MeterRegistry registry = MetricUtils.getMeterRegistry();
            if (registry != null) {
                Counter.builder(METRIC_ENTITY_AUDIT_DLQ)
                        .description("Entity audit events in DLQ flow (failures, enqueued, dropped, published)")
                        .tag("outcome", outcome)
                        .register(registry)
                        .increment(eventCount);
            }
        } catch (Exception e) {
            LOG.warn("Failed to record entity audit DLQ metric outcome={} count={}", outcome, eventCount, e);
        }
    }

    /**
     * Returns true if the exception indicates a mapping or other permanent ES error.
     * Such errors must not be sent to the DLQ (poison pill: replay would fail every time).
     * Walks the cause chain (same pattern as AtlanElasticSearchIndex and AtlasEntityStoreV2.isPermanentBackendException)
     * so we detect permanent errors whether on the top-level exception or a wrapped cause.
     * Package-private for testability (ESBasedAuditRepositoryDLQTest verifies poison-pill detection).
     */
    static boolean isMappingOrPermanentException(Throwable t) {
        Throwable current = t;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null) {
                String msgLower = msg.toLowerCase();
                for (String type : MAPPING_OR_PERMANENT_ES_ERROR_TYPES) {
                    if (msgLower.contains(type)) {
                        return true;
                    }
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private RestClient lowLevelClient;
    private final Configuration configuration;
    private EntityGraphRetriever entityGraphRetriever;
    private BlockingQueue<EntityAuditDLQEntry> auditDlqQueue;
    private Thread auditDlqReplayThread;
    private volatile boolean auditDlqReplayRunning;
    private volatile KafkaProducer<String, String> auditDlqKafkaProducer;

    @Inject
    public ESBasedAuditRepository(Configuration configuration, EntityGraphRetriever entityGraphRetriever) {
        this.configuration = configuration;
        this.entityGraphRetriever = entityGraphRetriever;
    }

    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {

    }

    @Override
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        return null;
    }

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("pushInES");

        try {
            putEventsV2Internal(events);
        } catch (Exception e) {
            // Do not fail the main request; pass failed request into async DLQ for retry (MS-642).
            int eventCount = (events != null) ? events.size() : 0;
            boolean isMappingOrPermanent = isMappingOrPermanentException(e);
            if (isMappingOrPermanent) {
                LOG.error("Entity audit write to ES failed with mapping/permanent error (will not enqueue to DLQ to avoid poison pill). eventCount={}, error={}",
                        eventCount, e.getMessage(), e);
                recordAuditDlqMetric("dropped_mapping_permanent", eventCount);
            } else if (ENTITY_AUDIT_DLQ_ENABLED.getBoolean() && auditDlqQueue != null && eventCount > 0) {
                boolean offered = auditDlqQueue.offer(new EntityAuditDLQEntry(new ArrayList<>(events), 0));
                if (offered) {
                    LOG.warn("Entity audit write to ES failed; enqueued to async DLQ for retry. eventCount={}, error={}",
                            eventCount, e.getMessage());
                    recordAuditDlqMetric("enqueued", eventCount);
                } else {
                    LOG.error("Entity audit DLQ full; dropping {} events. error={}. Consider increasing atlas.entity.audit.dlq.queue.capacity",
                            eventCount, e.getMessage(), e);
                    recordAuditDlqMetric("dropped_queue_full", eventCount);
                }
            } else {
                LOG.error("Entity audit write to ES failed; main request will not fail. eventCount={}, error={}. " +
                        "Check ES health/circuit breaker if this recurs.", eventCount, e.getMessage(), e);
                recordAuditDlqMetric("dropped_dlq_disabled", eventCount);
            }
        } finally {
            try {
                RequestContext.get().endMetricRecord(metric);
            } catch (Exception metricEx) {
                LOG.warn("Failed to end metric record for pushInES", metricEx);
            }
        }
    }

    /**
     * Performs the actual ES bulk write for audit events. Throws on failure; callers are expected to
     * catch and handle gracefully so the main request does not fail (see putEventsV2).
     */
    private void putEventsV2Internal(List<EntityAuditEventV2> events) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(events)) {
            return;
        }

        Map<String, String> requestContextHeaders = RequestContext.get().getRequestContextHeaders();
        String entityPayloadTemplate = getQueryTemplate(requestContextHeaders);

        StringBuilder bulkRequestBody = new StringBuilder();
        for (EntityAuditEventV2 event : events) {
            String created = String.format("%s", event.getTimestamp());
            String auditDetailPrefix = EntityAuditListenerV2.getV2AuditPrefix(event.getAction());
            String detailsRaw = event.getDetails();
            String details = (detailsRaw != null && detailsRaw.length() >= auditDetailPrefix.length())
                    ? detailsRaw.substring(auditDetailPrefix.length())
                    : (detailsRaw != null ? detailsRaw : "{}");

            AtlasEntity auditEntity = event.getEntity();

            if (auditEntity == null) {
                LOG.warn("Audit entity is null for event (entityId={}, action={}); skipping ES audit record",
                        event.getEntityId(), event.getAction());
                continue;
            }

            String typeName = auditEntity.getTypeName();
            long   updateTimestamp;

            if (auditEntity.getUpdateTime() != null) {
                updateTimestamp = auditEntity.getUpdateTime().getTime();
            } else {
                updateTimestamp = event.getTimestamp();
                LOG.warn("Entity updateTime is null for audit event (entityId={}, type={}); using event timestamp as fallback",
                        event.getEntityId(), typeName);
            }

            String bulkItem = MessageFormat.format(entityPayloadTemplate,
                    event.getEntityId(),
                    event.getAction(),
                    details,
                    event.getUser(),
                    event.getEntityId() + ":" + updateTimestamp,
                    event.getEntityQualifiedName(),
                    typeName,
                    created,
                    "" + updateTimestamp);

            bulkRequestBody.append(bulkMetadata);
            bulkRequestBody.append(bulkItem);
            bulkRequestBody.append("\n");
        }
        if (bulkRequestBody.length() == 0) {
            return;
        }
        String endpoint = INDEX_NAME + "/_bulk";
        HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
        Request request = new Request("POST", endpoint);
        request.setEntity(entity);

        int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();
        long initialRetryDelay = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();

        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            Response response = null;
            try {
                response = lowLevelClient.performRequest(request);
                int statusCode = response.getStatusLine().getStatusCode();

                // Accept any 2xx status code as a success.
                if (statusCode >= 200 && statusCode < 300) {
                    String responseString = EntityUtils.toString(response.getEntity());
                    Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);

                    if ((boolean) responseMap.get("errors")) {
                        LOG.error("Elasticsearch returned errors for bulk audit event request. Full response: {}", responseString);
                        List<String> errors = new ArrayList<>();
                        List<Map<String, Object>> resultItems = (List<Map<String, Object>>) responseMap.get("items");
                        for (Map<String, Object> resultItem : resultItems) {
                            if (resultItem.get("index") != null) {
                                Map<String, Object> resultIndex = (Map<String, Object>) resultItem.get("index");
                                if (resultIndex.get("error") != null) {
                                    errors.add(resultIndex.get("error").toString());
                                }
                            }
                        }
                        throw new AtlasBaseException("Error pushing entity audits to ES: " + errors);
                    }
                    return; // Success, exit the method.
                }

                String responseBody = EntityUtils.toString(response.getEntity());

                if ((statusCode >= 500 && statusCode < 600) || statusCode==429) {
                    LOG.warn("Failed to push entity audits to ES due to server error ({}). Retrying... ({}/{}) Response: {}",
                            statusCode, retryCount + 1, maxRetries, responseBody);
                } else {
                    throw new AtlasBaseException("Unable to push entity audits to ES. Status code: " + statusCode + ", Response: " + responseBody);
                }

            } catch (IOException e) {
                LOG.warn("Failed to push entity audits to ES due to IOException. Retrying... ({}/{})", retryCount + 1, maxRetries, e);
            }

            if (retryCount < maxRetries - 1) {
                try {
                    long exponentialBackoffDelay = initialRetryDelay * (long) Math.pow(2, retryCount);
                    Thread.sleep(exponentialBackoffDelay);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new AtlasBaseException("ES audit push interrupted during retry delay", interruptedException);
                }
            }
        }

        LOG.error("Failed to push entity audits to ES after {} retries", maxRetries);
        throw new AtlasBaseException("Unable to push entity audits to ES after " + maxRetries + " retries");
    }

    private String getQueryTemplate(Map<String, String> requestContextHeaders) {
        StringBuilder template = new StringBuilder();

        template.append("'{'\"entityId\":\"{0}\",\"action\":\"{1}\",\"detail\":{2},\"user\":\"{3}\", \"eventKey\":\"{4}\", " +
                        "\"entityQualifiedName\": {5}, \"typeName\": \"{6}\",\"created\":{7}, \"timestamp\":{8}");

        if (MapUtils.isNotEmpty(requestContextHeaders)) {
            template.append(",")
                    .append("\"").append("headers").append("\"")
                    .append(":")
                    .append(AtlasType.toJson(requestContextHeaders).replaceAll("\\{", "'{").replaceAll("\\}", "'}"));

        }

        template.append("'}'");

        return template.toString();
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResultCount) throws AtlasBaseException {
        List<EntityAuditEventV2> ret;
        String queryTemplate = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"entityId\":\"%s\"}},{\"term\":{\"action\":\"%s\"}}]}}}";
        String queryWithEntityFilter = String.format(queryTemplate, entityId, auditAction);
        ret = searchEvents(queryWithEntityFilter).getEntityAudits();

        return ret;
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityAuditSearchResult searchEvents(String queryString) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ESAuditRepo.searchEvents");
        try {
            String optimizedQuery = addSourceFieldsIfAbsent(queryString);
            String response = performSearchOnIndex(optimizedQuery);
            return getResultFromResponse(response);
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private String addSourceFieldsIfAbsent(String queryString) {
        if (StringUtils.isEmpty(queryString)) {
            return queryString;
        }

        try {
            Map<String, Object> dsl = AtlasType.fromJson(queryString, Map.class);

            if (dsl != null && !dsl.containsKey("_source")) {
                dsl.put("_source", SOURCE_FIELDS);
                return AtlasType.toJson(dsl);
            }
        } catch (Exception e) {
            LOG.warn("Failed to inject _source fields into audit query, using original query", e);
        }

        return queryString;
    }

    private EntityAuditSearchResult getResultFromResponse(String responseString) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ESAuditRepo.getResultFromResponse");

        EntityAuditSearchResult searchResult = new EntityAuditSearchResult();

        try {
            List<EntityAuditEventV2> entityAudits = new ArrayList<>();
            Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
            Map<String, Object> hits_0 = (Map<String, Object>) responseMap.get("hits");
            List<LinkedHashMap> hits_1 = (List<LinkedHashMap>) hits_0.get("hits");
            Map<String, AtlasEntityHeader> existingLinkedEntities = searchResult.getLinkedEntities();

            for (LinkedHashMap hit : hits_1) {
                Map source = (Map) hit.get("_source");
                String entityGuid = (String) source.get(ENTITYID);
                EntityAuditEventV2 event = new EntityAuditEventV2();
                event.setEntityId(entityGuid);
                event.setAction(EntityAuditEventV2.EntityAuditActionV2.fromString((String) source.get(ACTION)));
                event.setDetail((Map<String, Object>) source.get(DETAIL));
                event.setUser((String) source.get(USER));
                event.setCreated((long) source.get(CREATED));
                if (source.get(TIMESTAMP) != null) {
                    event.setTimestamp((long) source.get(TIMESTAMP));
                }
                if (source.get(TYPE_NAME) != null) {
                    event.setTypeName((String) source.get(TYPE_NAME));
                }

                event.setEntityQualifiedName((String) source.get(ENTITY_QUALIFIED_NAME));

                String eventKey = (String) source.get(EVENT_KEY);
                if (StringUtils.isEmpty(eventKey)) {
                    eventKey = event.getEntityId() + ":" + event.getTimestamp();
                }

                AtlasPerfMetrics.MetricRecorder recorder_0 = RequestContext.get().startMetricRecord("ESAuditRepo.getResultFromResponse.attributes");
                Map<String, Object> detail = event.getDetail();
                if (detail != null && detail.containsKey("attributes")) {
                    Map<String, Object> attributes = (Map<String, Object>) detail.get("attributes");

                    for (Map.Entry<String, Object> entry: attributes.entrySet()) {
                        if (ALLOWED_LINKED_ATTRIBUTES.contains(entry.getKey())) {
                            Object attrValue = entry.getValue();
                            List<String> guids = new ArrayList<>();

                            // Handle both single GUID and list of GUIDs
                            if (attrValue instanceof List) {
                                guids = (List<String>) attrValue;
                            } else if (attrValue instanceof String) {
                                guids.add((String) attrValue);
                            }

                            if (guids != null && !guids.isEmpty()){
                                for (String guid: guids){
                                    if(!existingLinkedEntities.containsKey(guid)){
                                        try {
                                            AtlasEntityHeader entityHeader = fetchAtlasEntityHeader(guid);
                                            if (entityHeader != null) {
                                                existingLinkedEntities.put(guid, entityHeader);
                                            }
                                        } catch (AtlasBaseException e) {
                                            LOG.error("Error while fetching entity header for guid: {}", guid, e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                RequestContext.get().endMetricRecord(recorder_0);

                event.setHeaders((Map<String, String>) source.get("headers"));

                event.setEventKey(eventKey);
                entityAudits.add(event);
            }
            Map<String, Object> aggregationsMap = (Map<String, Object>) responseMap.get("aggregations");
            Map<String, Object> countObject = (Map<String, Object>) hits_0.get("total");
            int totalCount = (int) countObject.get("value");
            searchResult.setEntityAudits(entityAudits);
            searchResult.setLinkedEntities(existingLinkedEntities);
            searchResult.setAggregations(aggregationsMap);
            searchResult.setTotalCount(totalCount);
            searchResult.setCount(entityAudits.size());
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return searchResult;
    }

    private AtlasEntityHeader fetchAtlasEntityHeader(String domainGUID) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ESAuditRepo.getResultFromResponse");
        try {
            AtlasEntityHeader entityHeader = entityGraphRetriever.toAtlasEntityHeader(domainGUID);
            return entityHeader;
        } catch (AtlasBaseException e) {
            throw new AtlasBaseException(e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private String performSearchOnIndex(String queryString) throws IOException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ESAuditRepo.performSearchOnIndex");

        try {
            HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);
            String endPoint = INDEX_NAME + "/_search";
            Request request = new Request("GET", endPoint);
            request.setEntity(entity);
            Response response = lowLevelClient.performRequest(request);
            return EntityUtils.toString(response.getEntity());
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        throw new NotImplementedException();
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("ESBasedAuditRepo - start!");
        initApplicationProperties();
        startInternal();
        if (ENTITY_AUDIT_DLQ_ENABLED.getBoolean()) {
            int capacity = ENTITY_AUDIT_DLQ_QUEUE_CAPACITY.getInt();
            auditDlqQueue = new LinkedBlockingQueue<>(capacity);
            auditDlqReplayRunning = true;
            auditDlqReplayThread = new Thread(this::runAuditDlqReplayLoop, "EntityAudit-DLQ-Replay");
            auditDlqReplayThread.setDaemon(true);
            auditDlqReplayThread.start();
            LOG.info("Entity audit async DLQ started; queue capacity={}", capacity);
        }
    }

    /**
     * Background loop: drain retry queue, retry push to ES with backoff. After max retries, publish to Kafka DLQ (inspect later).
     */
    private void runAuditDlqReplayLoop() {
        int maxRetries = ENTITY_AUDIT_DLQ_MAX_RETRIES.getInt();
        long backoffBaseMs = ENTITY_AUDIT_DLQ_BACKOFF_BASE_MS.getLong();
        long backoffMaxMs = ENTITY_AUDIT_DLQ_BACKOFF_MAX_MS.getLong();
        while (auditDlqReplayRunning && auditDlqQueue != null) {
            try {
                EntityAuditDLQEntry entry = auditDlqQueue.poll(DLQ_POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (entry == null) {
                    continue;
                }
                try {
                    putEventsV2Internal(entry.events());
                } catch (Exception e) {
                    if (isMappingOrPermanentException(e)) {
                        LOG.error("Entity audit retry failed with mapping/permanent error (dropping, will not re-queue). eventCount={}. error={}",
                                entry.events().size(), e.getMessage(), e);
                        recordAuditDlqMetric("retry_dropped_mapping_permanent", entry.events().size());
                    } else if (entry.retryCount() < maxRetries) {
                        // Backoff before re-queue so we don't process immediately; during the failure period retries would likely fail again.
                        long backoffMs = Math.min(backoffMaxMs, backoffBaseMs * (long) Math.pow(2, entry.retryCount()));
                        try {
                            Thread.sleep(backoffMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        boolean reOffered = auditDlqQueue.offer(new EntityAuditDLQEntry(entry.events(), entry.retryCount() + 1));
                        if (reOffered) {
                            LOG.warn("Entity audit retry failed (retry {}/{}); backoff {}ms, re-queued. error={}",
                                    entry.retryCount() + 1, maxRetries, backoffMs, e.getMessage());
                            recordAuditDlqMetric("retry_requeued", entry.events().size());
                        } else {
                            LOG.error("Entity audit retry queue full; dropping {} events after {} retries",
                                    entry.events().size(), entry.retryCount() + 1, e);
                            recordAuditDlqMetric("retry_dropped_queue_full", entry.events().size());
                        }
                    } else {
                        if (ENTITY_AUDIT_DLQ_PUBLISH_TO_KAFKA_ENABLED.getBoolean()) {
                            publishToKafkaDLQ(entry.events(), e.getMessage());
                        } else {
                            LOG.error("Entity audit retry failed after {} retries; dropping {} events (Kafka DLQ publish disabled). error={}",
                                    maxRetries, entry.events().size(), e.getMessage(), e);
                            recordAuditDlqMetric("dropped_kafka_disabled", entry.events().size());
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Entity audit retry thread interrupted");
                break;
            }
        }
    }

    /**
     * Publish failed audit events to Kafka DLQ topic so they can be inspected/replayed later. Does not throw.
     */
    private void publishToKafkaDLQ(List<EntityAuditEventV2> events, String failureReason) {
        try {
            KafkaProducer<String, String> producer = getOrCreateAuditDlqKafkaProducer();
            if (producer == null) {
                LOG.error("Entity audit retry failed after max retries; cannot publish to DLQ (producer not available). eventCount={}. error={}",
                        events.size(), failureReason);
                recordAuditDlqMetric("dropped_producer_unavailable", events.size());
                return;
            }
            String topic = ENTITY_AUDIT_DLQ_TOPIC.getString();
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("failureReason", failureReason);
            payload.put("eventCount", events.size());
            payload.put("timestamp", System.currentTimeMillis());
            payload.put("events", events);
            String json = AtlasType.toJson(payload);
            producer.send(new ProducerRecord<>(topic, null, json));
            LOG.warn("Entity audit retry failed after max retries; published {} events to DLQ topic {} for inspection. error={}",
                    events.size(), topic, failureReason);
            recordAuditDlqMetric("published_kafka", events.size());
        } catch (Exception ex) {
            LOG.error("Entity audit: failed to publish {} events to Kafka DLQ; events will be lost. error={}", events.size(), ex.getMessage(), ex);
            recordAuditDlqMetric("dropped_kafka_publish_failed", events.size());
        }
    }

    private KafkaProducer<String, String> getOrCreateAuditDlqKafkaProducer() {
        if (auditDlqKafkaProducer != null) {
            return auditDlqKafkaProducer;
        }
        synchronized (this) {
            if (auditDlqKafkaProducer != null) {
                return auditDlqKafkaProducer;
            }
            try {
                String bootstrapServers = ApplicationProperties.get().getString("atlas.kafka.bootstrap.servers", "").trim();
                if (bootstrapServers.isEmpty()) {
                    LOG.warn("Entity audit DLQ Kafka publish enabled but atlas.kafka.bootstrap.servers not set; DLQ publish disabled");
                    return null;
                }
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                auditDlqKafkaProducer = new KafkaProducer<>(props);
                return auditDlqKafkaProducer;
            } catch (Exception e) {
                LOG.error("Entity audit: failed to create Kafka producer for DLQ", e);
                return null;
            }
        }
    }

    /**
     * Test hook: process at most one DLQ entry (poll with 0 timeout). Used to drive replay without
     * starting the background thread. Returns true if an entry was consumed (success or re-queued/dropped).
     */
    @VisibleForTesting
    boolean processOneDlqEntryForTest() {
        if (auditDlqQueue == null) {
            return false;
        }
        try {
            EntityAuditDLQEntry entry = auditDlqQueue.poll(0, TimeUnit.SECONDS);
            if (entry == null) {
                return false;
            }
            int maxRetries = ENTITY_AUDIT_DLQ_MAX_RETRIES.getInt();
            try {
                putEventsV2Internal(entry.events());
            } catch (Exception e) {
                if (!isMappingOrPermanentException(e) && entry.retryCount() < maxRetries) {
                    auditDlqQueue.offer(new EntityAuditDLQEntry(entry.events(), entry.retryCount() + 1));
                }
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @VisibleForTesting
    int getAuditDlqQueueSize() {
        return auditDlqQueue != null ? auditDlqQueue.size() : 0;
    }

    @VisibleForTesting
    void startInternal() throws AtlasException {
        createSession();
    }

    void createSession() throws AtlasException {
        LOG.info("Create ES Session in ES Based Audit Repo");
        setLowLevelClient();
        try {
            boolean indexExists = checkIfIndexExists();
            if (!indexExists) {
                LOG.info("Create ES index for entity audits in ES Based Audit Repo");
                createAuditIndex();
            }
            if (shouldUpdateFieldLimitSetting()) {
                LOG.info("Updating ES total field limit");
                updateFieldLimit();
            }
            updateMappingsIfChanged();
            ensureStoreTypeNiofs();
        } catch (IOException e) {
            LOG.error("error", e);
            throw new AtlasException(e);
        }

    }

    private boolean checkIfIndexExists() throws IOException {
        Request request = new Request("HEAD", INDEX_NAME);
        Response response = lowLevelClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            LOG.info("Entity audits index exists!");
            return true;
        }
        LOG.info("Entity audits index does not exist!");
        return false;
    }

    private boolean createAuditIndex() throws IOException {
        LOG.info("ESBasedAuditRepo - createAuditIndex!");
        String esMappingsString = getAuditIndexMappings();
        HttpEntity entity = new NStringEntity(esMappingsString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", INDEX_NAME);
        request.setEntity(entity);
        Response response = lowLevelClient.performRequest(request);
        return isSuccess(response);
    }

    private boolean shouldUpdateFieldLimitSetting() {
        JsonNode currentFieldLimit;
        try {
            currentFieldLimit = getIndexFieldLimit();
        } catch (IOException e) {
            LOG.error("Problem while retrieving the index field limit!", e);
            return false;
        }
        Integer fieldLimitFromConfigurationFile = configuration.getInt(TOTAL_FIELD_LIMIT, 0);
        return currentFieldLimit == null || fieldLimitFromConfigurationFile > currentFieldLimit.asInt();
    }

    private JsonNode getIndexFieldLimit() throws IOException {
        Request request = new Request("GET", INDEX_NAME + "/_settings");
        Response response = lowLevelClient.performRequest(request);
        ObjectMapper objectMapper = new ObjectMapper();
        String fieldPath = String.format("/%s/settings/index/mapping/total_fields/limit", INDEX_NAME);

        return objectMapper.readTree(copyToString(response.getEntity().getContent(), Charset.defaultCharset())).at(fieldPath);
    }

    private void updateFieldLimit() {
        Request request = new Request("PUT", INDEX_NAME + "/_settings");
        String requestBody = String.format("{\"index.mapping.total_fields.limit\": %d}", configuration.getInt(TOTAL_FIELD_LIMIT));
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        Response response;
        try {
            response = lowLevelClient.performRequest(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                LOG.error("Error while updating the Elasticsearch total field limits! Error: " + copyToString(response.getEntity().getContent(), defaultCharset()));
            } else {
                LOG.info("ES total field limit has been updated");
            }
        } catch (IOException e) {
            LOG.error("Error while updating the field limit", e);
        }
    }

    /**
     * One-time migration: switches entity_audits from default hybridfs/mmapfs to niofs store type.
     *
     * With hybridfs, ES memory-maps all audit segment files at index open time, consuming virtual
     * address space proportional to the full index size (often 19-400GB). These mmap'd pages compete
     * for the OS page cache with janusgraph_vertex_index, degrading search performance.
     *
     * niofs uses Java NIO FileChannel.read() instead of mmap. Audit pages only enter the page cache
     * during active queries and are easily evictable, freeing page cache for the vertex index.
     *
     * Uses a marker document to ensure the migration runs exactly once across all pods and deployments.
     * On every startup, each pod does a single cheap HEAD request to check for the marker.
     * Requires a brief close/open cycle (~2-3 seconds of audit write unavailability) on first run only.
     */
    private void ensureStoreTypeNiofs() {
        try {
            // Fast path: check if migration was already completed (cheap HEAD request)
            if (isNiofsMigrationDone()) {
                return;
            }

            // Migration not done yet — verify store type and migrate if needed
            Request getSettings = new Request("GET", INDEX_NAME + "/_settings");
            Response settingsResponse = lowLevelClient.performRequest(getSettings);
            String responseBody = copyToString(settingsResponse.getEntity().getContent(), defaultCharset());
            JsonNode storeType = new ObjectMapper().readTree(responseBody).at("/" + INDEX_NAME + "/settings/index/store/type");

            if (storeType != null && "niofs".equals(storeType.asText())) {
                // Already niofs (e.g. set manually) — just write the marker so we skip next time
                writeNiofsMigrationMarker();
                return;
            }

            String currentType = (storeType == null || storeType.isMissingNode()) ? "default" : storeType.asText();
            LOG.info("entity_audits index store type is '{}', migrating to niofs", currentType);

            // Close index
            Request closeRequest = new Request("POST", INDEX_NAME + "/_close");
            Response closeResponse = lowLevelClient.performRequest(closeRequest);
            if (!isSuccess(closeResponse)) {
                LOG.error("Failed to close entity_audits index for niofs migration");
                return;
            }

            boolean migrationSucceeded = false;
            try {
                Request putSettings = new Request("PUT", INDEX_NAME + "/_settings");
                String settingsBody = "{\"index.store.type\": \"niofs\"}";
                putSettings.setEntity(new NStringEntity(settingsBody, ContentType.APPLICATION_JSON));
                Response putResponse = lowLevelClient.performRequest(putSettings);
                if (isSuccess(putResponse)) {
                    LOG.info("entity_audits index store type set to niofs");
                    migrationSucceeded = true;
                } else {
                    LOG.error("Failed to set niofs store type on entity_audits");
                }
            } finally {
                Request openRequest = new Request("POST", INDEX_NAME + "/_open");
                Response openResponse = lowLevelClient.performRequest(openRequest);
                if (isSuccess(openResponse)) {
                    LOG.info("entity_audits index reopened after niofs migration");
                } else {
                    LOG.error("Failed to reopen entity_audits index after niofs migration");
                    migrationSucceeded = false;
                }

                if (migrationSucceeded) {
                    writeNiofsMigrationMarker();
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to ensure niofs store type on entity_audits, will retry on next startup", e);
        }
    }

    private boolean isNiofsMigrationDone() {
        try {
            Request request = new Request("HEAD", INDEX_NAME + "/_doc/" + NIOFS_MIGRATION_MARKER_ID);
            Response response = lowLevelClient.performRequest(request);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private void writeNiofsMigrationMarker() {
        try {
            Request request = new Request("PUT", INDEX_NAME + "/_doc/" + NIOFS_MIGRATION_MARKER_ID);
            String body = "{\"migration\":\"niofs\",\"timestamp\":" + System.currentTimeMillis() + "}";
            request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));
            lowLevelClient.performRequest(request);
            LOG.info("entity_audits niofs migration marker written");
        } catch (Exception e) {
            LOG.warn("Failed to write niofs migration marker, migration will re-check on next startup", e);
        }
    }

    private void updateMappingsIfChanged() throws IOException, AtlasException {
        LOG.info("ESBasedAuditRepo - updateMappings!");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, JsonNode> activeIndexMappings = getActiveIndexMappings(mapper);
        JsonNode indexInformationFromConfigurationFile = mapper.readTree(getAuditIndexMappings());
        for (String activeAuditIndex : activeIndexMappings.keySet()) {
            if (!areConfigurationsSame(activeIndexMappings.get(activeAuditIndex), indexInformationFromConfigurationFile)) {
                Response response = updateMappings(indexInformationFromConfigurationFile);
                if (isSuccess(response)) {
                    LOG.info("ESBasedAuditRepo - Elasticsearch mappings have been updated for index: {}", activeAuditIndex);
                } else {
                    LOG.error("Error while updating the Elasticsearch indexes for index: {}", activeAuditIndex);
                    throw new AtlasException(copyToString(response.getEntity().getContent(), Charset.defaultCharset()));
                }
            }
        }
    }

    private Map<String, JsonNode> getActiveIndexMappings(ObjectMapper mapper) throws IOException {
        Request request = new Request("GET", INDEX_NAME);
        Response response = lowLevelClient.performRequest(request);
        String responseString = copyToString(response.getEntity().getContent(), Charset.defaultCharset());
        Map<String, JsonNode> indexMappings = new TreeMap<>();
        JsonNode rootNode = mapper.readTree(responseString);

        // Iterate over the index names and get the mappings
        for (Iterator<String> it = rootNode.fieldNames(); it.hasNext(); ) {
            String indexName = it.next();
            if (indexName.startsWith(ENTITY_AUDITS_INDEX)) {
                indexMappings.put(indexName, rootNode.get(indexName).get("mappings"));
            }
        }
        return indexMappings;
    }

    private boolean areConfigurationsSame(JsonNode activeIndexInformation, JsonNode indexInformationFromConfigurationFile) {
        return indexInformationFromConfigurationFile.get("mappings").equals(activeIndexInformation);
    }

    private Response updateMappings(JsonNode indexInformationFromConfigurationFile) throws IOException {
        Request request = new Request("PUT", INDEX_NAME + "/_mapping");
        HttpEntity entity = new NStringEntity(indexInformationFromConfigurationFile.get("mappings").toString(), ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        return lowLevelClient.performRequest(request);
    }

    private String getAuditIndexMappings() throws IOException {
        String atlasHomeDir = System.getProperty("atlas.home");
        String atlasHome = StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir;
        File elasticsearchSettingsFile = Paths.get(atlasHome, "elasticsearch", "es-audit-mappings.json").toFile();
        return new String(Files.readAllBytes(elasticsearchSettingsFile.toPath()), StandardCharsets.UTF_8);
    }

    @Override
    public void stop() throws AtlasException {
        try {
            LOG.info("ESBasedAuditRepo - stop!");
            auditDlqReplayRunning = false;
            if (auditDlqReplayThread != null) {
                auditDlqReplayThread.interrupt();
                try {
                    auditDlqReplayThread.join(TimeUnit.SECONDS.toMillis(30));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for entity audit DLQ replay thread to stop");
                }
                auditDlqReplayThread = null;
            }
            auditDlqQueue = null;
            if (auditDlqKafkaProducer != null) {
                try {
                    auditDlqKafkaProducer.close();
                } catch (Exception e) {
                    LOG.warn("Error closing entity audit DLQ Kafka producer", e);
                }
                auditDlqKafkaProducer = null;
            }
            if (lowLevelClient != null) {
                lowLevelClient.close();
                lowLevelClient = null;
            }
        } catch (IOException e) {
            LOG.error("ESBasedAuditRepo - error while closing es lowlevel client", e);
            throw new AtlasException(e);
        }
    }

    private void setLowLevelClient() throws AtlasException {
        if (lowLevelClient == null) {
            try {
                LOG.info("ESBasedAuditRepo - setLowLevelClient!");
                List<HttpHost> httpHosts = getHttpHosts();

                RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
                builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                        .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

                lowLevelClient = builder.build();
            } catch (AtlasException e) {
                LOG.error("Failed to initialize low level rest client for ES");
                throw new AtlasException(e);
            }
        }
    }

    public static List<HttpHost> getHttpHosts() throws AtlasException {
        List<HttpHost> httpHosts = new ArrayList<>();
        String indexConf = getESHosts();
        String[] hosts = indexConf.split(",");
        for (String host : hosts) {
            host = host.trim();
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length == 1) {
                httpHosts.add(new HttpHost(hostAndPort[0]));
            } else if (hostAndPort.length == 2) {
                httpHosts.add(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                throw new AtlasException("Invalid config");
            }
        }
        return httpHosts;
    }

    public static String getESHosts() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        //get es write hosts if available (ES Isolation)
        String esHostNames = configuration.getString(INDEX_WRITE_BACKEND_CONF);
        if (StringUtils.isNotEmpty(esHostNames)) {
            return esHostNames;
        } else {
            return configuration.getString(INDEX_BACKEND_CONF);
        }
    }

    private boolean isSuccess(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }
}


