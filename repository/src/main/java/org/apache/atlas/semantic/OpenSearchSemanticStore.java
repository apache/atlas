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
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.lang3.StringUtils;
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

import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_EMBEDDING_FIELD;
import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_INGEST_PIPELINE_NAME;
import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_TEXT_FIELD;

@Component
public class OpenSearchSemanticStore {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSemanticStore.class);

    private volatile SemanticOpenSearchHttpClient httpClient;

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

    // created lazily: this bean is instantiated even when semantic search is disabled
    SemanticOpenSearchHttpClient httpClient() throws SemanticSearchException {
        SemanticOpenSearchHttpClient ret = httpClient;

        if (ret == null) {
            synchronized (this) {
                ret = httpClient;
                if (ret == null) {
                    try {
                        ret = new SemanticOpenSearchHttpClient(ApplicationProperties.get());
                    } catch (AtlasException e) {
                        throw new SemanticSearchException("Failed to read OpenSearch connection settings", e);
                    }
                    httpClient = ret;
                }
            }
        }

        return ret;
    }

    /**
     * Computes an embedding and patches {@link SemanticSearchConfiguration#SEMANTIC_EMBEDDING_FIELD} via
     * {@code _update_by_query} on the Atlas guid (no separate document-id lookup).
     */
    public void updateEmbeddingByGuid(String guid, String semanticText) throws SemanticSearchException {
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
        String indexName = SemanticSearchConfiguration.getVertexIndexName();

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

    public List<VectorSearchHit> neuralSearch(String queryText, int topK, VectorSearchFilter filter) throws SemanticSearchException {
        if (StringUtils.isBlank(queryText)) {
            return Collections.emptyList();
        }

        String indexName = SemanticSearchConfiguration.getVertexIndexName();
        String modelId   = SemanticSearchConfiguration.getOpenSearchModelId();

        Map<String, Object> neuralField = new HashMap<>();
        neuralField.put("query_text", queryText);
        neuralField.put("model_id", modelId);
        neuralField.put("k", topK);
        putVectorFilter(neuralField, filter);

        Map<String, Object> query = Collections.singletonMap("neural",
                Collections.singletonMap(SEMANTIC_EMBEDDING_FIELD, neuralField));

        return executeSearch(indexName, topK, query, filter);
    }

    /**
     * k-NN search using a pre-computed embedding (used by similar-entities to avoid query-time ML inference).
     */
    public List<VectorSearchHit> knnSearch(List<?> queryVector, int topK, VectorSearchFilter filter)
            throws SemanticSearchException {
        if (queryVector == null || queryVector.isEmpty()) {
            return Collections.emptyList();
        }

        String indexName = SemanticSearchConfiguration.getVertexIndexName();

        Map<String, Object> knnField = new HashMap<>();
        knnField.put("vector", queryVector);
        knnField.put("k", topK);
        putVectorFilter(knnField, filter);

        Map<String, Object> query = Collections.singletonMap("knn",
                Collections.singletonMap(SEMANTIC_EMBEDDING_FIELD, knnField));

        return executeSearch(indexName, topK, query, filter);
    }

    /**
     * Reads the stored embedding for an entity by Atlas guid from the JanusGraph vertex index.
     */
    @SuppressWarnings("unchecked")
    public List<?> getStoredEmbeddingByGuid(String guid) throws SemanticSearchException {
        VertexIndexDocument document = findVertexIndexDocumentByGuid(guid,
                Collections.singletonList(SEMANTIC_EMBEDDING_FIELD));
        if (document == null) {
            return null;
        }

        Object embedding = document.getSource().get(SEMANTIC_EMBEDDING_FIELD);
        if (!(embedding instanceof List) || ((List<?>) embedding).isEmpty()) {
            return null;
        }

        return (List<?>) embedding;
    }

    private VertexIndexDocument findVertexIndexDocumentByGuid(String guid, List<String> sourceFields)
            throws SemanticSearchException {
        if (StringUtils.isBlank(guid)) {
            return null;
        }

        String indexName = SemanticSearchConfiguration.getVertexIndexName();

        Map<String, Object> body = new HashMap<>();
        body.put("size", 1);
        body.put("query", Collections.singletonMap("term", Collections.singletonMap(guidFilterField(), guid)));
        if (sourceFields == null || sourceFields.isEmpty()) {
            body.put("_source", false);
        } else {
            body.put("_source", sourceFields);
        }

        HttpPost post = new HttpPost("/" + indexName + "/_search");
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        return parseVertexIndexDocumentFromSearchResponse(executeRequestForBody(post));
    }

    @SuppressWarnings("unchecked")
    static VertexIndexDocument parseVertexIndexDocumentFromSearchResponse(String responseJson) {
        Map<String, Object> root = AtlasJson.fromJson(responseJson, Map.class);
        if (root == null || !(root.get("hits") instanceof Map)) {
            return null;
        }

        Object hitsArray = ((Map<String, Object>) root.get("hits")).get("hits");
        if (!(hitsArray instanceof List) || ((List<?>) hitsArray).isEmpty()) {
            return null;
        }

        Object firstHit = ((List<?>) hitsArray).get(0);
        if (!(firstHit instanceof Map)) {
            return null;
        }

        Map<String, Object> hit = (Map<String, Object>) firstHit;
        Object documentId = hit.get("_id");
        if (documentId == null || StringUtils.isBlank(documentId.toString())) {
            return null;
        }

        Map<String, Object> source = Collections.emptyMap();
        Object sourceObj = hit.get("_source");
        if (sourceObj instanceof Map) {
            source = (Map<String, Object>) sourceObj;
        }

        return new VertexIndexDocument(documentId.toString(), source);
    }

    static final class VertexIndexDocument {
        private final String              documentId;
        private final Map<String, Object> source;

        VertexIndexDocument(String documentId, Map<String, Object> source) {
            this.documentId = documentId;
            this.source     = source != null ? source : Collections.emptyMap();
        }

        String getDocumentId() {
            return documentId;
        }

        Map<String, Object> getSource() {
            return source;
        }
    }

    /**
     * Pages through active entity documents of the vertex index in guid order ({@code search_after}), passing each
     * page of guids to {@code onPage}. With {@code missingEmbeddingOnly}, only documents without an embedding are
     * returned; documents embedded while scanning simply drop out, which the guid cursor tolerates.
     * The graph stays the source of truth: callers must still check state/type on the vertex.
     */
    public void scanActiveEntityGuids(boolean missingEmbeddingOnly, int pageSize, Consumer<List<String>> onPage)
            throws SemanticSearchException {
        String indexName   = SemanticSearchConfiguration.getVertexIndexName();
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
        filter.add(Collections.singletonMap("match", Collections.singletonMap(Constants.STATE_PROPERTY_KEY, "ACTIVE")));

        List<Map<String, Object>> mustNot = new ArrayList<>();
        mustNot.add(Collections.singletonMap("prefix", Collections.singletonMap(typeNameFilterField(), "__")));
        mustNot.add(Collections.singletonMap("terms", Collections.singletonMap(typeNameFilterField(),
                new ArrayList<>(SemanticNotificationGuidExpander.getNonEmbeddableEntityTypes()))));
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

    private List<VectorSearchHit> executeSearch(String indexName,
                                                int topK,
                                                Map<String, Object> query,
                                                VectorSearchFilter filter) throws SemanticSearchException {
        Map<String, Object> body = new HashMap<>();
        body.put("size", topK);
        body.put("query", query);
        body.put("_source", Collections.singletonList(Constants.GUID_PROPERTY_KEY));

        HttpPost post = new HttpPost("/" + indexName + "/_search");
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        String response = executeRequestForBody(post);
        return parseSearchHits(response, filter);
    }

    // The filter goes inside the knn/neural clause: OpenSearch then returns the k nearest documents that match it.
    // A bool filter around the clause would only filter the global top k, and can return nothing.
    private static void putVectorFilter(Map<String, Object> vectorField, VectorSearchFilter filter) {
        Map<String, Object> vectorFilter = buildVectorFilter(filter);
        if (vectorFilter != null) {
            vectorField.put("filter", vectorFilter);
        }
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
    private List<VectorSearchHit> parseSearchHits(String responseJson, VectorSearchFilter filter) {
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
            if (StringUtils.isBlank(guid) || filter.getExcludeGuids().contains(guid)) {
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
}
