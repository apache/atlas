/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.semantic;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Component
public class OpenSearchSemanticStore implements SemanticVectorStore {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSemanticStore.class);

    static final String SEMANTIC_TEXT_FIELD           = "atlas_semantic_text";
    static final String SEMANTIC_EMBEDDING_FIELD      = "atlas_semantic_embedding";
    static final String SEMANTIC_INGEST_PIPELINE_NAME = "atlas-semantic-ingest";
    private static final String GRAPH_INDEX_NAME_CONF = "atlas.graph.index.search.index-name";

    private static final int SEARCH_SOCKET_TIMEOUT_MS = 10_000;
    private static final int SEARCH_MAX_ATTEMPTS      = 2;
    // Make sure to allow JanusGraph finish its work(write to the index) before retrying the update embedding
    private static final int UPDATE_EMBEDDING_MAX_ATTEMPTS = 6;
    private static final String SCAN_KEEP_ALIVE       = "10m"; // max time between two pages of scanEntities (point in time)
    // JanusGraph maps __typeName and __state as text; exact filters use their keyword subfield
    static final String TYPE_NAME_KEYWORD_FIELD = Constants.ENTITY_TYPE_PROPERTY_KEY + ".keyword";
    static final String STATE_KEYWORD_FIELD     = Constants.STATE_PROPERTY_KEY + ".keyword";

    private volatile SemanticHttpClient httpClient;
    private volatile boolean                      searchable;

    @PreDestroy
    public synchronized void shutdown() {
        if (httpClient == null) {
            return;
        }

        try {
            httpClient.close();
        } catch (IOException e) {
            LOG.warn("Failed to close OpenSearch HTTP client", e);
        } finally {
            httpClient = null;
        }
    }

    /**
     * The JanusGraph vertex index: index-name, else opensearch.index-name (which Atlas sets at startup for the
     * OpenSearch backend), plus "_vertex_index".
     */
    static String getVertexIndexName() {
        String indexName = ApplicationProperties.DEFAULT_INDEX_NAME;

        try {
            Configuration config = ApplicationProperties.get();

            indexName = config.getString(GRAPH_INDEX_NAME_CONF,
                    config.getString(ApplicationProperties.OPENSEARCH_INDEX_NAME_CONF, ApplicationProperties.DEFAULT_INDEX_NAME));
        } catch (AtlasException e) {
            // keep the default
        }

        return indexName + "_" + Constants.VERTEX_INDEX;
    }

    // created lazily: this bean is instantiated even when semantic search is disabled
    private SemanticHttpClient httpClient() throws SemanticSearchException {
        SemanticHttpClient ret = httpClient;

        if (ret == null) {
            synchronized (this) {
                ret = httpClient;
                if (ret == null) {
                    try {
                        ret = OpenSearchClientFactory.create(ApplicationProperties.get());
                    } catch (AtlasException e) {
                        throw new SemanticSearchException("Failed to read OpenSearch connection settings", e);
                    }
                    httpClient = ret;
                }
            }
        }

        return ret;
    }

    @Override
    public void initialize() throws SemanticSearchException {
        new SemanticIndexSetup(httpClient()).run();
    }

    @Override
    public boolean isAvailable() {
        try {
            httpClient().sendForBody(new HttpGet("/_cluster/health?wait_for_status=yellow&timeout=1s"), SEARCH_SOCKET_TIMEOUT_MS);

            String modelId = AtlasConfiguration.SEMANTIC_MODEL_ID.getString().trim();
            String model   = httpClient().sendForBody(new HttpGet("/_plugins/_ml/models/" + modelId), SEARCH_SOCKET_TIMEOUT_MS);
            if (!isModelDeployed(model)) {
                LOG.debug("Embedding model {} is not deployed", modelId);
                return false;
            }
            return true;
        } catch (SemanticSearchException e) {
            LOG.debug("OpenSearch is not available: {}", e.getMessage());
            return false;
        }
    }

    // PARTIALLY_DEPLOYED: running on some of its nodes, which still serve inference
    static boolean isModelDeployed(String modelJson) {
        Map<String, Object> model = AtlasJson.fromJson(modelJson, Map.class);
        Object              state = model != null ? model.get("model_state") : null;

        return "DEPLOYED".equals(state) || "PARTIALLY_DEPLOYED".equals(state);
    }

    /**
     * Computes an embedding and patches {@link #SEMANTIC_EMBEDDING_FIELD} via
     * {@code _update_by_query} on the Atlas guid (no separate document-id lookup).
     */
    @Override
    public void updateEmbedding(String guid, String semanticText) throws SemanticSearchException {
        if (StringUtils.isBlank(guid) || StringUtils.isBlank(semanticText)) {
            return;
        }

        // OpenSearch 3.x no longer accepts pipeline/conflicts on _update; embed via pipeline simulate, then patch vector.
        // Embed once: only the update is retried, so a retry never repeats the ML inference.
        String body = AtlasJson.toJson(buildUpdateEmbeddingByQueryBody(guid, computeEmbedding(semanticText)));

        // Right after a create, _update_by_query (a search) doesn't see the vertex doc until the next index refresh
        // (1 s by default), then can conflict with JanusGraph's follow-up writes to it: more attempts than other calls.
        SemanticRetry.run("OpenSearch update embedding (guid=" + guid + ")", UPDATE_EMBEDDING_MAX_ATTEMPTS,
                () -> {
                    executeUpdateEmbeddingByGuid(guid, body);
                    return null;
                });
    }

    private void executeUpdateEmbeddingByGuid(String guid, String body) throws SemanticSearchException {
        String indexName = getVertexIndexName();

        // conflicts=proceed: JanusGraph may bump the vertex doc seqNo on the same Kafka event; abort returns HTTP 409.
        HttpPost post = new HttpPost("/" + indexName + "/_update_by_query?conflicts=proceed");
        post.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));

        String responseBody = httpClient().sendForBody(post); // single attempt: the caller already retries
        int updated          = parseUpdateByQueryUpdatedCount(responseBody);
        int versionConflicts = parseUpdateByQueryVersionConflicts(responseBody);
        if (updated == 1) {
            return;
        }

        if (updated == 0 && versionConflicts > 0) {
            throw new SemanticSearchException(
                    "OpenSearch version conflict updating embedding (guid=" + guid
                            + ", versionConflicts=" + versionConflicts + ")", true);
        }

        if (updated == 0) {
            throw new SemanticSearchException(
                    "OpenSearch document not found for update (guid=" + guid + ")", true);
        }

        // updated > 1 → multiple documents matched the query → should not happen
        throw new SemanticSearchException(
                "OpenSearch update_by_query matched " + updated + " documents for guid=" + guid);
    }

    static Map<String, Object> buildUpdateEmbeddingByQueryBody(String guid, List<?> embedding) {
        Map<String, Object> params = new HashMap<>();
        params.put("embedding", embedding);

        Map<String, Object> script = new HashMap<>();
        script.put("lang", "painless");
        script.put("source", "ctx._source." + SEMANTIC_EMBEDDING_FIELD + " = params.embedding");
        script.put("params", params);

        Map<String, Object> body = new HashMap<>();
        body.put("query", guidClause(guid));
        body.put("script", script);
        return body;
    }

    static int parseUpdateByQueryUpdatedCount(String responseJson) {
        Map<String, Object> root = AtlasJson.fromJson(responseJson, Map.class);
        if (root == null) {
            return -1;
        }

        Object updated = root.get("updated");
        if (updated instanceof Number) {
            return ((Number) updated).intValue();
        }

        return -1;
    }

    static int parseUpdateByQueryVersionConflicts(String responseJson) {
        Map<String, Object> root = AtlasJson.fromJson(responseJson, Map.class);
        if (root == null) {
            return 0;
        }

        Object versionConflicts = root.get("version_conflicts");
        if (versionConflicts instanceof Number) {
            return ((Number) versionConflicts).intValue();
        }

        return 0;
    }

    @SuppressWarnings("unchecked")
    private List<?> computeEmbedding(String semanticText) throws SemanticSearchException {
        Map<String, Object> source = new HashMap<>();
        source.put(SEMANTIC_TEXT_FIELD, semanticText);

        Map<String, Object> doc = new HashMap<>();
        doc.put("_source", source);

        Map<String, Object> body = new HashMap<>();
        body.put("docs", Collections.singletonList(doc));

        String url = "/_ingest/pipeline/" + SEMANTIC_INGEST_PIPELINE_NAME + "/_simulate";
        HttpPost post = new HttpPost(url);
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        String response = executeRequestForBody(post);
        Map<String, Object> root = AtlasJson.fromJson(response, Map.class);
        if (root == null || !(root.get("docs") instanceof List) || ((List<?>) root.get("docs")).isEmpty()) {
            throw new SemanticSearchException("Pipeline simulate returned no documents for semantic embedding");
        }

        Object firstDoc = ((List<?>) root.get("docs")).get(0);
        if (!(firstDoc instanceof Map)) {
            throw new SemanticSearchException("Pipeline simulate returned invalid document payload");
        }

        // a failed processor is reported in the document, with HTTP 200
        Object error = ((Map<String, Object>) firstDoc).get("error");
        if (error != null) {
            Object reason = error instanceof Map ? ((Map<?, ?>) error).get("reason") : error;
            throw new SemanticSearchException("Embedding failed in pipeline " + SEMANTIC_INGEST_PIPELINE_NAME + ": " + reason);
        }

        Object docObj = ((Map<String, Object>) firstDoc).get("doc");
        if (!(docObj instanceof Map)) {
            throw new SemanticSearchException("Pipeline simulate returned no doc for semantic embedding");
        }

        Object docSource = ((Map<String, Object>) docObj).get("_source");
        if (!(docSource instanceof Map)) {
            throw new SemanticSearchException("Pipeline simulate returned no _source for semantic embedding");
        }

        Object embedding = ((Map<String, Object>) docSource).get(SEMANTIC_EMBEDDING_FIELD);
        if (!(embedding instanceof List) || ((List<?>) embedding).isEmpty()) {
            throw new SemanticSearchException("Pipeline simulate did not produce " + SEMANTIC_EMBEDDING_FIELD);
        }

        return (List<?>) embedding;
    }

    @Override
    public List<VectorSearchHit> searchByText(String queryText, int topK, VectorSearchFilter filter) throws SemanticSearchException {
        if (StringUtils.isBlank(queryText)) {
            return Collections.emptyList();
        }

        Map<String, Object> neuralField = new HashMap<>();
        neuralField.put("query_text", queryText);
        neuralField.put("model_id", AtlasConfiguration.SEMANTIC_MODEL_ID.getString().trim());

        return vectorSearch("neural", neuralField, topK, filter);
    }

    @Override
    public List<VectorSearchHit> searchByVector(List<?> queryVector, int topK, VectorSearchFilter filter)
            throws SemanticSearchException {
        if (queryVector == null || queryVector.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, Object> knnField = new HashMap<>();
        knnField.put("vector", queryVector);

        return vectorSearch("knn", knnField, topK, filter);
    }

    @Override
    public List<?> getStoredEmbedding(String guid) throws SemanticSearchException {
        if (StringUtils.isBlank(guid)) {
            return null;
        }

        Map<String, Object> body = new HashMap<>();
        body.put("size", 1);
        body.put("query", guidClause(guid));
        body.put("_source", Collections.singletonList(SEMANTIC_EMBEDDING_FIELD));

        ensureSearchable();

        HttpPost post = new HttpPost("/" + getVertexIndexName() + "/_search");
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        return parseStoredEmbedding(executeSearchForBody(post));
    }

    @SuppressWarnings("unchecked")
    static List<?> parseStoredEmbedding(String responseJson) {
        Map<String, Object> root = AtlasJson.fromJson(responseJson, Map.class);
        if (root == null || !(root.get("hits") instanceof Map)) {
            return null;
        }

        Object hitsArray = ((Map<String, Object>) root.get("hits")).get("hits");
        if (!(hitsArray instanceof List) || ((List<?>) hitsArray).isEmpty()) {
            return null;
        }

        Object firstHit = ((List<?>) hitsArray).get(0);
        Object source   = firstHit instanceof Map ? ((Map<String, Object>) firstHit).get("_source") : null;
        Object embedding = source instanceof Map ? ((Map<String, Object>) source).get(SEMANTIC_EMBEDDING_FIELD) : null;

        return embedding instanceof List && !((List<?>) embedding).isEmpty() ? (List<?>) embedding : null;
    }

    /**
     * Pages with {@code search_after} on {@code _shard_doc} in a point in time: the index is read as it was when the
     * scan started, sorting costs nothing, and each page request carries its own cursor, so it is safely retried
     * (a scroll can't be: a retry after a lost response returns the next page). Entities embedded meanwhile are
     * still returned; re-embedding them is idempotent. Needs OpenSearch 2.4+.
     */
    @Override
    public void scanEntities(boolean missingEmbeddingOnly, int pageSize, Consumer<List<String>> onPage)
            throws SemanticSearchException {
        HttpPost open = new HttpPost("/" + getVertexIndexName() + "/_search/point_in_time?keep_alive=" + SCAN_KEEP_ALIVE);
        String   pitId = parsePitId(executeRequestForBody(open));

        try {
            List<?> searchAfter = null;

            while (true) {
                HttpPost post = new HttpPost("/_search"); // a point-in-time search names no index
                post.setEntity(new StringEntity(AtlasJson.toJson(buildEntityScanBody(missingEmbeddingOnly, pageSize, pitId, searchAfter)),
                        ContentType.APPLICATION_JSON));

                EntityScanPage page = SemanticRetry.run("OpenSearch entity scan", () -> parseEntityScanPage(httpClient().sendForBody(post)));
                if (page.hitCount == 0) {
                    return;
                }

                if (!page.guids.isEmpty()) {
                    onPage.accept(page.guids);
                }

                searchAfter = page.lastSort;
                pitId       = page.pitId != null ? page.pitId : pitId;
            }
        } finally {
            closePointInTime(pitId);
        }
    }

    private void closePointInTime(String pitId) {
        // the point-in-time delete API takes its ids in the body, which HttpDelete doesn't support
        HttpUriRequest delete = RequestBuilder.delete("/_search/point_in_time")
                .setEntity(new StringEntity(AtlasJson.toJson(Collections.singletonMap("pit_id", Collections.singletonList(pitId))), ContentType.APPLICATION_JSON))
                .build();

        try {
            httpClient().sendForBody(delete);
        } catch (SemanticSearchException e) {
            LOG.warn("Failed to close the OpenSearch point in time; it expires after {}: {}", SCAN_KEEP_ALIVE, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    static String parsePitId(String responseJson) throws SemanticSearchException {
        Map<String, Object> root  = AtlasJson.fromJson(responseJson, Map.class);
        Object              pitId = root != null ? root.get("pit_id") : null;
        if (!(pitId instanceof String)) {
            throw new SemanticSearchException("OpenSearch returned no point in time id: " + responseJson);
        }
        return (String) pitId;
    }

    static Map<String, Object> buildEntityScanBody(boolean missingEmbeddingOnly, int pageSize, String pitId, List<?> searchAfter) {
        // entity vertices are the only ones with __guid + __typeName + __state (typedefs have no __state,
        // classification/struct vertices have no __guid)
        List<Map<String, Object>> filter = new ArrayList<>();
        filter.add(Collections.singletonMap("exists", Collections.singletonMap("field", Constants.GUID_PROPERTY_KEY)));
        filter.add(Collections.singletonMap("exists", Collections.singletonMap("field", Constants.ENTITY_TYPE_PROPERTY_KEY)));
        filter.add(Collections.singletonMap("term", Collections.singletonMap(STATE_KEYWORD_FIELD, AtlasEntity.Status.ACTIVE.name())));

        List<Map<String, Object>> mustNot = new ArrayList<>();
        mustNot.add(Collections.singletonMap("prefix", Collections.singletonMap(TYPE_NAME_KEYWORD_FIELD, Constants.INTERNAL_PROPERTY_KEY_PREFIX)));
        mustNot.add(Collections.singletonMap("terms", Collections.singletonMap(TYPE_NAME_KEYWORD_FIELD,
                new ArrayList<>(SemanticEntityEmbedder.getNonEmbeddableEntityTypes()))));
        if (missingEmbeddingOnly) {
            mustNot.add(Collections.singletonMap("exists", Collections.singletonMap("field", SEMANTIC_EMBEDDING_FIELD)));
        }

        Map<String, Object> bool = new HashMap<>();
        bool.put("filter", filter);
        bool.put("must_not", mustNot);

        Map<String, Object> body = new HashMap<>();
        body.put("size", pageSize);
        body.put("query", Collections.singletonMap("bool", bool));
        body.put("_source", Collections.singletonList(Constants.GUID_PROPERTY_KEY));
        body.put("track_total_hits", false);
        body.put("sort", Collections.singletonList(Collections.singletonMap("_shard_doc", "asc")));

        Map<String, Object> pit = new HashMap<>();
        pit.put("id", pitId);
        pit.put("keep_alive", SCAN_KEEP_ALIVE);
        body.put("pit", pit);

        if (searchAfter != null) {
            body.put("search_after", searchAfter);
        }
        return body;
    }

    /**
     * Fails (retryable) when a shard failed: OpenSearch still answers 200 with the other shards' hits, and the scan
     * would silently miss the failed shard's entities.
     */
    @SuppressWarnings("unchecked")
    static EntityScanPage parseEntityScanPage(String responseJson) throws SemanticSearchException {
        Map<String, Object> root    = AtlasJson.fromJson(responseJson, Map.class);
        Object              shards  = root != null ? root.get("_shards") : null;
        Object              failed  = shards instanceof Map ? ((Map<String, Object>) shards).get("failed") : null;
        Object              hitsObj = root != null ? root.get("hits") : null;
        Object              hits    = hitsObj instanceof Map ? ((Map<String, Object>) hitsObj).get("hits") : null;
        String              pitId   = root != null && root.get("pit_id") instanceof String ? (String) root.get("pit_id") : null;

        if (failed instanceof Number && ((Number) failed).intValue() > 0) {
            throw new SemanticSearchException("OpenSearch entity scan failed on " + failed + " shard(s): " + shards, true);
        }

        if (!(hits instanceof List) || ((List<?>) hits).isEmpty()) {
            return new EntityScanPage(Collections.emptyList(), 0, null, pitId);
        }

        List<?>      hitList = (List<?>) hits;
        List<String> guids   = new ArrayList<>();

        for (Object hitObj : hitList) {
            String guid = hitObj instanceof Map ? extractGuid((Map<String, Object>) hitObj) : null;
            if (StringUtils.isNotBlank(guid)) {
                guids.add(guid);
            }
        }

        // the next page starts after the last hit's sort value (search_after)
        Object lastHit  = hitList.get(hitList.size() - 1);
        Object lastSort = lastHit instanceof Map ? ((Map<String, Object>) lastHit).get("sort") : null;
        if (!(lastSort instanceof List)) {
            throw new SemanticSearchException("OpenSearch entity scan returned hits without sort values");
        }

        return new EntityScanPage(guids, hitList.size(), (List<?>) lastSort, pitId);
    }

    static final class EntityScanPage {
        final List<String> guids;
        final int          hitCount;
        final List<?>      lastSort;
        final String       pitId;

        EntityScanPage(List<String> guids, int hitCount, List<?> lastSort, String pitId) {
            this.guids    = guids;
            this.hitCount = hitCount;
            this.lastSort = lastSort;
            this.pitId    = pitId;
        }
    }

    private List<VectorSearchHit> vectorSearch(String queryType, Map<String, Object> vectorField, int topK,
                                               VectorSearchFilter filter) throws SemanticSearchException {
        vectorField.put("k", topK);

        // The filter goes inside the knn/neural clause: OpenSearch then returns the k nearest documents that match it.
        // A bool filter around the clause would only filter the global top k, and can return nothing.
        Map<String, Object> vectorFilter = buildVectorFilter(filter);
        if (vectorFilter != null) {
            vectorField.put("filter", vectorFilter);
        }

        Map<String, Object> body = new HashMap<>();
        body.put("size", topK);
        body.put("query", Collections.singletonMap(queryType, Collections.singletonMap(SEMANTIC_EMBEDDING_FIELD, vectorField)));
        body.put("_source", Collections.singletonList(Constants.GUID_PROPERTY_KEY));

        ensureSearchable();

        HttpPost post = new HttpPost("/" + getVertexIndexName() + "/_search");
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        return parseSearchHits(executeSearchForBody(post));
    }

    static Map<String, Object> buildVectorFilter(VectorSearchFilter filter) {
        Map<String, Object>       bool    = new HashMap<>();
        List<Map<String, Object>> filters = new ArrayList<>();

        if (!filter.getTypeNames().isEmpty()) {
            filters.add(Collections.singletonMap("terms", Collections.singletonMap(TYPE_NAME_KEYWORD_FIELD, new ArrayList<>(filter.getTypeNames()))));
        }

        if (filter.isActiveOnly()) {
            filters.add(Collections.singletonMap("term", Collections.singletonMap(STATE_KEYWORD_FIELD, AtlasEntity.Status.ACTIVE.name())));
        }

        if (filter.getEntityFilterClause() != null) {
            filters.add(filter.getEntityFilterClause());
        }

        if (!filters.isEmpty()) {
            bool.put("filter", filters);
        }

        if (!filter.getExcludeGuids().isEmpty()) {
            List<Map<String, Object>> excluded = new ArrayList<>();
            for (String guid : filter.getExcludeGuids()) {
                excluded.add(guidClause(guid));
            }
            bool.put("must_not", excluded);
        }

        return bool.isEmpty() ? null : Collections.singletonMap("bool", bool);
    }

    // JanusGraph maps __guid as analyzed text without a keyword subfield: the phrase of all its tokens matches one guid
    private static Map<String, Object> guidClause(String guid) {
        return Collections.singletonMap("match_phrase", Collections.singletonMap(Constants.GUID_PROPERTY_KEY, guid));
    }

    @SuppressWarnings("unchecked")
    static List<VectorSearchHit> parseSearchHits(String responseJson) {
        Map<String, Object> response = AtlasJson.fromJson(responseJson, Map.class);
        Object              hitsObj  = response != null ? response.get("hits") : null;

        if (!(hitsObj instanceof Map)) {
            return Collections.emptyList();
        }

        Object hitsArray = ((Map<String, Object>) hitsObj).get("hits");
        if (!(hitsArray instanceof List)) {
            return Collections.emptyList();
        }

        List<VectorSearchHit> ret = new ArrayList<>();
        for (Object hitObj : (List<?>) hitsArray) {
            if (!(hitObj instanceof Map)) {
                continue;
            }

            Map<String, Object> hit = (Map<String, Object>) hitObj;
            String guid = extractGuid(hit);
            if (StringUtils.isBlank(guid)) {
                continue;
            }

            double score = 0.0;
            Object scoreObj = hit.get("_score");
            if (scoreObj instanceof Number) {
                score = ((Number) scoreObj).doubleValue();
            }

            ret.add(new VectorSearchHit(guid, score));
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private static String extractGuid(Map<String, Object> hit) {
        Object source = hit.get("_source");
        if (source instanceof Map) {
            Object guid = ((Map<String, Object>) source).get(Constants.GUID_PROPERTY_KEY);
            if (guid != null) {
                return guid.toString();
            }
        }

        return null; // _id is the JanusGraph vertex doc id, not an Atlas guid
    }

    private String executeRequestForBody(HttpUriRequest request) throws SemanticSearchException {
        return SemanticRetry.run("OpenSearch " + request.getMethod(), () -> httpClient().sendForBody(request));
    }

    private void ensureSearchable() throws SemanticSearchException {
        if (!searchable) {
            new SemanticIndexSetup(httpClient()).checkSearchable(SEARCH_SOCKET_TIMEOUT_MS);
            searchable = true;
        }
    }

    private String executeSearchForBody(HttpPost request) throws SemanticSearchException {
        return SemanticRetry.run("OpenSearch search", SEARCH_MAX_ATTEMPTS,
                () -> httpClient().sendForBody(request, SEARCH_SOCKET_TIMEOUT_MS));
    }
}
