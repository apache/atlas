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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            return true;
        } catch (SemanticSearchException e) {
            LOG.debug("OpenSearch is not available: {}", e.getMessage());
            return false;
        }
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

        // _update_by_query is a search: a vertex doc written by JanusGraph is not visible until the next index
        // refresh (1s by default), so "not found" is retryable.
        SemanticRetry.run("OpenSearch update embedding (guid=" + guid + ")",
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
        body.put("query", Collections.singletonMap("term", Collections.singletonMap(guidFilterField(), guid)));
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
        body.put("query", Collections.singletonMap("term", Collections.singletonMap(guidFilterField(), guid)));
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
     * Pages with {@code search_after} on the guid: documents embedded while scanning with
     * {@code missingEmbeddingOnly} simply drop out, which the guid cursor tolerates.
     */
    @Override
    public void scanEntities(boolean missingEmbeddingOnly, int pageSize, Consumer<List<String>> onPage)
            throws SemanticSearchException {
        String indexName   = getVertexIndexName();
        Object searchAfter = null;

        while (true) {
            HttpPost post = new HttpPost("/" + indexName + "/_search");
            post.setEntity(new StringEntity(AtlasJson.toJson(buildEntityScanBody(missingEmbeddingOnly, pageSize, searchAfter)),
                    ContentType.APPLICATION_JSON));

            EntityScanPage page = parseEntityScanPage(executeRequestForBody(post));
            if (!page.guids.isEmpty()) {
                onPage.accept(page.guids);
            }

            if (page.hitCount < pageSize || page.lastSortValue == null) {
                return;
            }

            searchAfter = page.lastSortValue;
        }
    }

    static Map<String, Object> buildEntityScanBody(boolean missingEmbeddingOnly, int pageSize, Object searchAfter) {
        // entity vertices are the only ones with __guid + __typeName + __state (typedefs have no __state,
        // classification/struct vertices have no __guid)
        List<Map<String, Object>> filter = new ArrayList<>();
        filter.add(Collections.singletonMap("exists", Collections.singletonMap("field", Constants.GUID_PROPERTY_KEY)));
        filter.add(Collections.singletonMap("exists", Collections.singletonMap("field", Constants.ENTITY_TYPE_PROPERTY_KEY)));
        // match (not term) works whether __state is mapped as keyword (legacy) or text with a keyword subfield
        filter.add(Collections.singletonMap("match", Collections.singletonMap(Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name())));

        List<Map<String, Object>> mustNot = new ArrayList<>();
        mustNot.add(Collections.singletonMap("prefix", Collections.singletonMap(typeNameFilterField(), Constants.INTERNAL_PROPERTY_KEY_PREFIX)));
        mustNot.add(Collections.singletonMap("terms", Collections.singletonMap(typeNameFilterField(),
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
        body.put("sort", Collections.singletonList(Collections.singletonMap(guidFilterField(), "asc")));
        if (searchAfter != null) {
            body.put("search_after", Collections.singletonList(searchAfter));
        }
        return body;
    }

    @SuppressWarnings("unchecked")
    static EntityScanPage parseEntityScanPage(String responseJson) {
        Map<String, Object> root    = AtlasJson.fromJson(responseJson, Map.class);
        Object              hitsObj = root != null ? root.get("hits") : null;
        Object              hits    = hitsObj instanceof Map ? ((Map<String, Object>) hitsObj).get("hits") : null;

        if (!(hits instanceof List) || ((List<?>) hits).isEmpty()) {
            return new EntityScanPage(Collections.emptyList(), 0, null);
        }

        List<String> guids     = new ArrayList<>();
        Object       lastSort  = null;

        for (Object hitObj : (List<?>) hits) {
            if (!(hitObj instanceof Map)) {
                continue;
            }

            Map<String, Object> hit  = (Map<String, Object>) hitObj;
            String              guid = extractGuid(hit);
            if (StringUtils.isNotBlank(guid)) {
                guids.add(guid);
            }

            Object sort = hit.get("sort");
            if (sort instanceof List && !((List<?>) sort).isEmpty()) {
                lastSort = ((List<?>) sort).get(0);
            }
        }

        return new EntityScanPage(guids, ((List<?>) hits).size(), lastSort);
    }

    static final class EntityScanPage {
        final List<String> guids;
        final int          hitCount;
        final Object       lastSortValue;

        EntityScanPage(List<String> guids, int hitCount, Object lastSortValue) {
            this.guids         = guids;
            this.hitCount      = hitCount;
            this.lastSortValue = lastSortValue;
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
        Map<String, Object> bool = new HashMap<>();

        if (!filter.getTypeNames().isEmpty()) {
            // JanusGraph maps __typeName as text; exact type filtering requires the keyword subfield.
            bool.put("filter", Collections.singletonList(termsFilter(typeNameFilterField(), filter.getTypeNames())));
        }

        if (!filter.getExcludeGuids().isEmpty()) {
            bool.put("must_not", Collections.singletonList(termsFilter(guidFilterField(), filter.getExcludeGuids())));
        }

        return bool.isEmpty() ? null : Collections.singletonMap("bool", bool);
    }

    private static String typeNameFilterField() {
        return Constants.ENTITY_TYPE_PROPERTY_KEY + ".keyword";
    }

    private static String guidFilterField() {
        return Constants.GUID_PROPERTY_KEY + ".keyword";
    }

    private static Map<String, Object> termsFilter(String field, Set<String> values) {
        return Collections.singletonMap("terms", Collections.singletonMap(field, new ArrayList<>(values)));
    }

    @SuppressWarnings("unchecked")
    private static List<VectorSearchHit> parseSearchHits(String responseJson) {
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
