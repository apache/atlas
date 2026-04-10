package org.apache.atlas.repository.store.graph.v2;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;
import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;

public class ESConnector implements Closeable {
    private static final Logger LOG      = LoggerFactory.getLogger(ESConnector.class);

    private static RestClient lowLevelClient;

    private static Set<String> DENORM_ATTRS;
    private static String GET_DOCS_BY_ID = VERTEX_INDEX_NAME + "/_mget";

    static {
        try {
            lowLevelClient = initializeClient();
            DENORM_ATTRS = initializeDenormAttributes();
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to initialize ESConnector", e);
        }
    }

    private static RestClient initializeClient() throws AtlasException {
        try {
            List<HttpHost> httpHosts = getHttpHosts();
            RestClientBuilder builder = RestClient.builder(httpHosts.get(0))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                            .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

            return builder.build();
        } catch (Exception e) {
            throw new AtlasException("Failed to initialize Elasticsearch client", e);
        }
    }

    private static Set<String> initializeDenormAttributes() {
        Set<String> attrs = new HashSet<>();
        attrs.add(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        attrs.add(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        attrs.add(CLASSIFICATION_TEXT_KEY);
        attrs.add(TRAIT_NAMES_PROPERTY_KEY);
        attrs.add(CLASSIFICATION_NAMES_KEY);
        return Collections.unmodifiableSet(attrs);
    }

    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap) {
        writeTagProperties(entitiesMap, false);
    }

    /**
     * Builds an ES bulk request body for the given entities, optionally filtering to only pending doc IDs.
     *
     * @param entitiesMap    vertex ID → denorm attributes map
     * @param docIdToVertexId output map populated with docId → vertexId mappings
     * @param pendingDocIds  if non-null, only include entries whose docId is in this set
     * @param upsert         whether to include upsert clause
     * @return the bulk request body string
     */
    private static StringBuilder buildBulkBody(Map<String, Map<String, Object>> entitiesMap,
                                               Map<String, String> docIdToVertexId,
                                               Set<String> pendingDocIds,
                                               boolean upsert) {
        StringBuilder body = new StringBuilder();
        for (String assetVertexId : entitiesMap.keySet()) {
            String docId = LongEncodingUtil.vertexIdToDocId(assetVertexId);
            if (pendingDocIds != null && !pendingDocIds.contains(docId)) continue;

            docIdToVertexId.put(docId, assetVertexId);

            Map<String, Object> entry = entitiesMap.get(assetVertexId);
            Map<String, Object> toUpdate = new HashMap<>();
            DENORM_ATTRS.stream().filter(entry::containsKey).forEach(x -> toUpdate.put(x, entry.get(x)));

            body.append("{\"update\":{\"_index\":\"" + VERTEX_INDEX_NAME + "\",\"_id\":\"").append(docId).append("\" }}\n");
            body.append("{");
            String attrsToUpdate = AtlasType.toJson(toUpdate);
            body.append("\"doc\":").append(attrsToUpdate);
            if (upsert) {
                body.append(",\"upsert\":").append(attrsToUpdate);
            }
            body.append("}\n");
        }
        return body;
    }

    /**
     * Updates and writes tag properties for multiple entities to Elasticsearch index.
     *
     * This method processes the provided entities map to prepare an Elasticsearch bulk
     * request for updating tag properties and denormalized attributes. The modifications
     * include attributes specified in the {@code DENORM_ATTRS} field and a modification
     * timestamp. The bulk request is then executed using a low-level client.
     *
     * @param entitiesMap A map where the keys represent the entity vertex IDs (as strings),
     *                    and the values are maps containing the attributes to be updated
     *                    for each entity.
     * @param upsert A boolean flag that indicates whether the update operation should upsert
     *               (create new doc if not found) the document in the Elasticsearch index.
     */
    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap, boolean upsert) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");

        try {
            if (MapUtils.isEmpty(entitiesMap))
                return;

            // Track docId → vertexId mapping for failure reporting
            Map<String, String> docIdToVertexId = new LinkedHashMap<>();
            StringBuilder bulkRequestBody = buildBulkBody(entitiesMap, docIdToVertexId, null, upsert);

            int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();
            long initialRetryDelay = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();

            // Track which doc IDs still need to be retried
            Set<String> pendingDocIds = new LinkedHashSet<>(docIdToVertexId.keySet());

            for (int retryCount = 0; retryCount < maxRetries && !pendingDocIds.isEmpty(); retryCount++) {
                if (retryCount > 0) {
                    try {
                        long exponentialBackoffDelay = initialRetryDelay * (long) Math.pow(2, retryCount - 1);
                        Thread.sleep(exponentialBackoffDelay);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("ES update interrupted during retry delay", interruptedException);
                    }
                }

                // Rebuild bulk body for pending items only (on retry)
                StringBuilder currentBody = (retryCount == 0)
                        ? bulkRequestBody
                        : buildBulkBody(entitiesMap, docIdToVertexId, pendingDocIds, upsert);

                Request request = new Request("POST", "/_bulk");
                request.setEntity(new StringEntity(currentBody.toString(), ContentType.APPLICATION_JSON));

                try {
                    Response response = lowLevelClient.performRequest(request);
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        String responseBody = EntityUtils.toString(response.getEntity());

                        if (responseBody != null && responseBody.contains("\"errors\":true")) {
                            // Parse per-item results to detect partial failures
                            Set<String> retryableDocIds = new LinkedHashSet<>();
                            Set<String> permanentlyFailed = new LinkedHashSet<>();
                            parseBulkResponse(responseBody, retryableDocIds, permanentlyFailed);

                            if (!permanentlyFailed.isEmpty()) {
                                LOG.error("writeTagProperties: {} items permanently failed (4xx): {}",
                                        permanentlyFailed.size(), permanentlyFailed);
                            }

                            // Only retry items with transient failures (5xx/429)
                            pendingDocIds.retainAll(retryableDocIds);

                            if (pendingDocIds.isEmpty()) {
                                return; // All retryable items resolved
                            }

                            LOG.warn("writeTagProperties: {} items have retryable failures, will retry ({}/{})",
                                    pendingDocIds.size(), retryCount + 1, maxRetries);
                        } else {
                            return; // All items succeeded
                        }
                    } else if (statusCode >= 500) {
                        LOG.warn("Failed to update ES doc due to server error ({}). Retrying... ({}/{})",
                                statusCode, retryCount + 1, maxRetries);
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        throw new RuntimeException("Failed to update ES doc. Status: " + statusCode + ", Body: " + responseBody);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to update ES doc for denorm attributes. Retrying... ({}/{})", retryCount + 1, maxRetries, e);
                }
            }

            if (!pendingDocIds.isEmpty()) {
                throw new RuntimeException("Failed to update ES doc for denorm attributes after " + maxRetries +
                        " retries. " + pendingDocIds.size() + " items still pending.");
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /**
     * Updates tag properties in ES and returns detailed result with success/failure per doc.
     * Parses the ES bulk response to detect partial failures.
     * Used by the propagation flow (flushTagDenormToES) — NOT used by direct attachment paths.
     */
    public static TagDenormESWriteResult writeTagPropertiesWithResult(Map<String, Map<String, Object>> entitiesMap, boolean upsert) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");

        try {
            if (MapUtils.isEmpty(entitiesMap))
                return TagDenormESWriteResult.allSuccess(0);

            // Track docId → vertexId mapping for failure reporting
            Map<String, String> docIdToVertexId = new LinkedHashMap<>();
            StringBuilder bulkRequestBody = buildBulkBody(entitiesMap, docIdToVertexId, null, upsert);

            int maxRetries = AtlasConfiguration.ES_MAX_RETRIES.getInt();
            long initialRetryDelay = AtlasConfiguration.ES_RETRY_DELAY_MS.getLong();

            // Track which doc IDs still need to be retried and which permanently failed
            Set<String> pendingDocIds = new LinkedHashSet<>(docIdToVertexId.keySet());
            Set<String> permanentlyFailedDocIds = new LinkedHashSet<>();

            for (int retryCount = 0; retryCount < maxRetries && !pendingDocIds.isEmpty(); retryCount++) {
                if (retryCount > 0) {
                    try {
                        long exponentialBackoffDelay = initialRetryDelay * (long) Math.pow(2, retryCount - 1);
                        Thread.sleep(exponentialBackoffDelay);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("ES update interrupted during retry delay", interruptedException);
                    }
                }

                // Rebuild bulk body for pending items only (on retry)
                StringBuilder currentBody = (retryCount == 0)
                        ? bulkRequestBody
                        : buildBulkBody(entitiesMap, docIdToVertexId, pendingDocIds, upsert);

                Request request = new Request("POST", "/_bulk");
                request.setEntity(new StringEntity(currentBody.toString(), ContentType.APPLICATION_JSON));

                try {
                    Response response = lowLevelClient.performRequest(request);
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) {
                        String responseBody = EntityUtils.toString(response.getEntity());

                        if (responseBody != null && responseBody.contains("\"errors\":true")) {
                            Set<String> retryableDocIds = new LinkedHashSet<>();
                            Set<String> batchPermanentlyFailed = new LinkedHashSet<>();
                            parseBulkResponse(responseBody, retryableDocIds, batchPermanentlyFailed);

                            permanentlyFailedDocIds.addAll(batchPermanentlyFailed);

                            // Only retry items with transient failures (5xx/429)
                            pendingDocIds.retainAll(retryableDocIds);

                            if (pendingDocIds.isEmpty()) {
                                break; // No more retryable items
                            }

                            LOG.warn("writeTagPropertiesWithResult: {} items have retryable failures, will retry ({}/{})",
                                    pendingDocIds.size(), retryCount + 1, maxRetries);
                        } else {
                            pendingDocIds.clear(); // All items succeeded
                            break;
                        }
                    } else if (statusCode >= 500) {
                        LOG.warn("Failed to update ES doc due to server error ({}). Retrying... ({}/{})",
                                statusCode, retryCount + 1, maxRetries);
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        throw new RuntimeException("Failed to update ES doc. Status: " + statusCode + ", Body: " + responseBody);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to update ES doc for denorm attributes. Retrying... ({}/{})", retryCount + 1, maxRetries, e);
                }
            }

            // Collect all failed vertex IDs (permanently failed + retries exhausted)
            Set<String> allFailedDocIds = new LinkedHashSet<>(permanentlyFailedDocIds);
            allFailedDocIds.addAll(pendingDocIds);

            List<String> failedVertexIds = new ArrayList<>();
            for (String docId : allFailedDocIds) {
                String vertexId = docIdToVertexId.get(docId);
                if (vertexId != null) {
                    failedVertexIds.add(vertexId);
                }
            }

            int successCount = entitiesMap.size() - failedVertexIds.size();
            if (!failedVertexIds.isEmpty()) {
                LOG.error("writeTagPropertiesWithResult: {}/{} docs failed after retries ({} permanent, {} retries exhausted)",
                        failedVertexIds.size(), entitiesMap.size(),
                        permanentlyFailedDocIds.size(), pendingDocIds.size());
            }
            return new TagDenormESWriteResult(successCount, failedVertexIds);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /** Parses an ES bulk response to separate retryable (5xx/429) from permanently
     * failed (4xx) items.
     */
    @SuppressWarnings("unchecked")
    private static void parseBulkResponse(String respBody, Set<String> retryableIds, Set<String> permanentlyFailed) {
        try {
            Map<String, Object> bulkResp = AtlasType.fromJson(respBody, Map.class);
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResp.get("items");
            if (items == null) return;

            for (Map<String, Object> item : items) {
                Map<String, Object> action = (Map<String, Object>) item.values().iterator().next();
                if (action == null) continue;

                String docId = String.valueOf(action.get("_id"));
                Object statusObj = action.get("status");
                int itemStatus = (statusObj instanceof Number) ? ((Number) statusObj).intValue() : 0;

                if (action.containsKey("error")) {
                    if (itemStatus >= 500 || itemStatus == 429) {
                        retryableIds.add(docId);
                        LOG.warn("writeTagProperties: bulk item retryable failure: _id='{}', status={}", docId, itemStatus);
                    } else {
                        permanentlyFailed.add(docId);
                        LOG.error("writeTagProperties: bulk item FAILED (non-retryable): _id='{}', status={}, error={}",
                                docId, itemStatus, AtlasType.toJson(action.get("error")));
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("writeTagProperties: failed to parse bulk response: {}. Raw (truncated): {}",
                    e.getMessage(), respBody.substring(0, Math.min(4000, respBody.length())));
        }
    }

    @Override
    public void close() throws IOException {
        if (lowLevelClient != null) {
            lowLevelClient.close();
        }
    }

    /**
     * Result of a bulk ES write operation for tag denorm attributes.
     */
    public static class TagDenormESWriteResult {
        private final int successCount;
        private final List<String> failedVertexIds;

        public TagDenormESWriteResult(int successCount, List<String> failedVertexIds) {
            this.successCount = successCount;
            this.failedVertexIds = failedVertexIds;
        }

        public static TagDenormESWriteResult allSuccess(int count) {
            return new TagDenormESWriteResult(count, Collections.emptyList());
        }

        public static TagDenormESWriteResult allFailed(Collection<String> vertexIds) {
            return new TagDenormESWriteResult(0, new ArrayList<>(vertexIds));
        }

        public int getSuccessCount()          { return successCount; }
        public List<String> getFailedVertexIds() { return failedVertexIds; }
        public boolean hasFailures()           { return !failedVertexIds.isEmpty(); }
    }
}