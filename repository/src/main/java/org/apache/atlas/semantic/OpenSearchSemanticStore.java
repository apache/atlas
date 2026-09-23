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

import org.apache.atlas.repository.Constants;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_EMBEDDING_FIELD;
import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_INGEST_PIPELINE_NAME;
import static org.apache.atlas.semantic.SemanticSearchConfiguration.SEMANTIC_TEXT_FIELD;

@Component
public class OpenSearchSemanticStore {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSemanticStore.class);

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    @PreDestroy
    public void shutdown() {
        try {
            httpClient.close();
        } catch (IOException e) {
            LOG.warn("Failed to close OpenSearch HTTP client", e);
        }
    }

    /**
     * Ensures ingest pipeline, knn settings, and semantic field mappings on the JanusGraph vertex index.
     */
    public void initialize() throws SemanticSearchException {
        validateCluster();
        ensureIngestPipeline();
        ensureVertexIndexSemanticFields();
    }

    /**
     * Computes an embedding and patches {@link SemanticSearchConfiguration#SEMANTIC_EMBEDDING_FIELD} via
     * {@code _update_by_query} on the Atlas guid (no separate document-id lookup).
     */
    public void updateEmbeddingByGuid(String guid, String semanticText) throws SemanticSearchException {
        if (StringUtils.isBlank(guid) || StringUtils.isBlank(semanticText)) {
            return;
        }

        SemanticRetry.run("OpenSearch update embedding (guid=" + guid + ")",
                () -> {
                    executeUpdateEmbeddingByGuid(guid, semanticText);
                    return null;
                });
    }

    private void executeUpdateEmbeddingByGuid(String guid, String semanticText) throws SemanticSearchException {
        String indexName = SemanticSearchConfiguration.getVertexIndexName();
        // OpenSearch 3.x no longer accepts pipeline/conflicts on _update; embed via pipeline simulate, then patch vector.
        List<?> embedding = computeEmbedding(semanticText);
        Map<String, Object> body = buildUpdateEmbeddingByQueryBody(guid, embedding);

        // conflicts=proceed: JanusGraph may bump the vertex doc seqNo on the same Kafka event; abort returns HTTP 409.
        HttpPost post = new HttpPost(baseUrl() + "/" + indexName + "/_update_by_query?conflicts=proceed");
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        String responseBody = executeRequestForBody(post);
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

        String url = baseUrl() + "/_ingest/pipeline/" + SEMANTIC_INGEST_PIPELINE_NAME + "/_simulate";
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

        Map<String, Object> neuralClause = new HashMap<>();
        neuralClause.put(SEMANTIC_EMBEDDING_FIELD, neuralField);

        List<Map<String, Object>> filters = buildFilters(filter);

        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", Collections.singletonList(Collections.singletonMap("neural", neuralClause)));
        if (!filters.isEmpty()) {
            boolQuery.put("filter", filters);
        }

        Map<String, Object> query = new HashMap<>();
        query.put("bool", boolQuery);

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

        Map<String, Object> knnClause = new HashMap<>();
        knnClause.put(SEMANTIC_EMBEDDING_FIELD, knnField);

        List<Map<String, Object>> filters = buildFilters(filter);

        Map<String, Object> query;
        if (filters.isEmpty()) {
            query = Collections.singletonMap("knn", knnClause);
        } else {
            Map<String, Object> boolQuery = new HashMap<>();
            boolQuery.put("must", Collections.singletonList(Collections.singletonMap("knn", knnClause)));
            boolQuery.put("filter", filters);
            query = Collections.singletonMap("bool", boolQuery);
        }

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

        HttpPost post = new HttpPost(baseUrl() + "/" + indexName + "/_search");
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

    private List<VectorSearchHit> executeSearch(String indexName,
                                                int topK,
                                                Map<String, Object> query,
                                                VectorSearchFilter filter) throws SemanticSearchException {
        Map<String, Object> body = new HashMap<>();
        body.put("size", topK);
        body.put("query", query);
        body.put("_source", Collections.singletonList(Constants.GUID_PROPERTY_KEY));

        HttpPost post = new HttpPost(baseUrl() + "/" + indexName + "/_search");
        post.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        String response = executeRequestForBody(post);
        return parseSearchHits(response, filter);
    }

    void ensureIngestPipeline() throws SemanticSearchException {
        String modelId = SemanticSearchConfiguration.getOpenSearchModelId();
        String url     = baseUrl() + "/_ingest/pipeline/" + SEMANTIC_INGEST_PIPELINE_NAME;

        HttpPut put = new HttpPut(url);
        put.setEntity(new StringEntity(AtlasJson.toJson(buildIngestPipelineBody(modelId)), ContentType.APPLICATION_JSON));
        executeRequest(put);
        LOG.info("Ensured OpenSearch ingest pipeline '{}' for model id {}", SEMANTIC_INGEST_PIPELINE_NAME, modelId);
    }

    static Map<String, Object> buildIngestPipelineBody(String modelId) {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put(SEMANTIC_TEXT_FIELD, SEMANTIC_EMBEDDING_FIELD);

        Map<String, Object> textEmbedding = new HashMap<>();
        textEmbedding.put("model_id", modelId);
        textEmbedding.put("field_map", fieldMap);

        Map<String, Object> textEmbeddingProcessor = new HashMap<>();
        textEmbeddingProcessor.put("text_embedding", textEmbedding);

        Map<String, Object> removeProcessor = new HashMap<>();
        removeProcessor.put("field", SEMANTIC_TEXT_FIELD);
        removeProcessor.put("ignore_missing", true);

        Map<String, Object> remove = new HashMap<>();
        remove.put("remove", removeProcessor);

        Map<String, Object> body = new HashMap<>();
        body.put("description", "Atlas semantic search: embed text at ingest on JanusGraph vertex index");
        body.put("processors", Arrays.asList(textEmbeddingProcessor, remove));
        return body;
    }

    private void ensureVertexIndexSemanticFields() throws SemanticSearchException {
        String indexName  = SemanticSearchConfiguration.getVertexIndexName();
        int    dimensions = SemanticSearchConfiguration.getOpenSearchEmbeddingDimension();

        if (!indexExists(indexName)) {
            throw new SemanticSearchException("Vertex index '" + indexName + "' does not exist in OpenSearch");
        }

        ensureKnnEnabled(indexName);
        patchSemanticMapping(indexName, dimensions);
        validateSemanticMapping(indexName, dimensions);
    }

    private void ensureKnnEnabled(String indexName) throws SemanticSearchException {
        if (isKnnEnabled(indexName)) {
            LOG.debug("knn already enabled on index '{}'", indexName);
            return;
        }

        Map<String, Object> indexSettings = new HashMap<>();
        indexSettings.put("knn", true);

        Map<String, Object> settings = new HashMap<>();
        settings.put("index", indexSettings);

        Map<String, Object> body = new HashMap<>();
        body.put("settings", settings);

        HttpPut put = new HttpPut(baseUrl() + "/" + indexName + "/_settings");
        put.setEntity(new StringEntity(AtlasJson.toJson(body), ContentType.APPLICATION_JSON));

        try {
            executeRequest(put);
        } catch (SemanticSearchException e) {
            if (isKnnAlreadyConfiguredError(e)) {
                LOG.info("knn already enabled on index '{}' (non-dynamic index setting)", indexName);
                return;
            }
            throw e;
        }
    }

    private boolean isKnnEnabled(String indexName) throws SemanticSearchException {
        // OpenSearch default (nested) settings response; include_defaults for implicit plugin defaults.
        HttpGet get = new HttpGet(baseUrl() + "/" + indexName + "/_settings/index.knn?include_defaults=true");
        String response = executeRequestForBody(get);

        try {
            Map<String, Object> root = AtlasJson.fromJson(response, Map.class);
            return parseKnnEnabledFromSettingsResponse(root);
        } catch (Exception e) {
            LOG.warn("Unable to read knn setting for index '{}'", indexName, e);
        }

        return false;
    }

    /**
     * Parses the OpenSearch default (nested) Get Index Settings response for {@code index.knn=true}.
     * Expects {@code settings.index.knn} and, when requested, {@code defaults.index.knn}.
     */
    @SuppressWarnings("unchecked")
    static boolean parseKnnEnabledFromSettingsResponse(Map<String, Object> root) {
        if (root == null || root.isEmpty()) {
            return false;
        }

        for (Object indexPayload : root.values()) {
            if (!(indexPayload instanceof Map)) {
                continue;
            }

            Map<String, Object> payload = (Map<String, Object>) indexPayload;
            if (parseNestedKnnFromSettingsSection(payload.get("settings"))) {
                return true;
            }
            if (parseNestedKnnFromSettingsSection(payload.get("defaults"))) {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private static boolean parseNestedKnnFromSettingsSection(Object settingsObj) {
        if (!(settingsObj instanceof Map)) {
            return false;
        }

        Object index = ((Map<String, Object>) settingsObj).get("index");
        if (!(index instanceof Map)) {
            return false;
        }

        Object knn = ((Map<?, ?>) index).get("knn");
        return isTruthyKnnSetting(knn);
    }

    private static boolean isTruthyKnnSetting(Object value) {
        return Boolean.TRUE.equals(value) || "true".equalsIgnoreCase(String.valueOf(value));
    }

    private static boolean isKnnAlreadyConfiguredError(SemanticSearchException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        return message.contains("index.knn")
                && (message.contains("non dynamic settings") || message.contains("non-dynamic settings"));
    }

    private void patchSemanticMapping(String indexName, int dimensions) throws SemanticSearchException {
        Map<String, Object> embeddingField = new HashMap<>();
        embeddingField.put("type", "knn_vector");
        embeddingField.put("dimension", dimensions);

        Map<String, Object> method = new HashMap<>();
        method.put("name", "hnsw");
        method.put("space_type", "cosinesimil");
        method.put("engine", "lucene");
        embeddingField.put("method", method);

        // Only persist the vector on the JanusGraph vertex index; semantic text is transient (pipeline simulate).
        Map<String, Object> properties = new HashMap<>();
        properties.put(SEMANTIC_EMBEDDING_FIELD, embeddingField);

        Map<String, Object> mappings = new HashMap<>();
        mappings.put("properties", properties);

        HttpPut put = new HttpPut(baseUrl() + "/" + indexName + "/_mapping");
        put.setEntity(new StringEntity(AtlasJson.toJson(mappings), ContentType.APPLICATION_JSON));
        executeRequest(put);
        LOG.info("Patched semantic fields on OpenSearch index '{}'", indexName);
    }

    private void validateSemanticMapping(String indexName, int configuredDimensions) throws SemanticSearchException {
        Integer indexDimension = getIndexEmbeddingDimension(indexName);
        if (indexDimension == null) {
            throw new SemanticSearchException(
                    "Vertex index '" + indexName + "' is missing knn_vector field '" + SEMANTIC_EMBEDDING_FIELD + "'");
        }

        if (indexDimension != configuredDimensions) {
            throw new SemanticSearchException(
                    "Vertex index '" + indexName + "' embedding dimension is " + indexDimension
                            + " but " + SemanticSearchConfiguration.SEMANTIC_OPENSEARCH_EMBEDDING_DIMENSION_CONF
                            + " is " + configuredDimensions);
        }
    }

    private Integer getIndexEmbeddingDimension(String indexName) throws SemanticSearchException {
        String url      = baseUrl() + "/" + indexName + "/_mapping";
        String response = executeRequestForBody(new HttpGet(url));
        return parseEmbeddingDimensionFromMapping(AtlasJson.fromJson(response, Map.class), indexName);
    }

    @SuppressWarnings("unchecked")
    static Integer parseEmbeddingDimensionFromMapping(Map<String, Object> mappingResponse, String indexName) {
        Map<String, Object> properties = parseIndexProperties(mappingResponse, indexName);
        if (properties == null) {
            return null;
        }

        Object embedding = properties.get(SEMANTIC_EMBEDDING_FIELD);
        if (!(embedding instanceof Map)) {
            return null;
        }

        Object dimension = ((Map<String, Object>) embedding).get("dimension");
        if (dimension instanceof Number) {
            return ((Number) dimension).intValue();
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseIndexProperties(Map<String, Object> mappingResponse, String indexName) {
        if (mappingResponse == null || mappingResponse.isEmpty()) {
            return null;
        }

        Object indexMappings = mappingResponse.get(indexName);
        if (!(indexMappings instanceof Map)) {
            indexMappings = mappingResponse.values().iterator().next();
        }

        if (!(indexMappings instanceof Map)) {
            return null;
        }

        Object mappings = ((Map<String, Object>) indexMappings).get("mappings");
        if (!(mappings instanceof Map)) {
            return null;
        }

        Object properties = ((Map<String, Object>) mappings).get("properties");
        if (!(properties instanceof Map)) {
            return null;
        }

        return (Map<String, Object>) properties;
    }

    private boolean indexExists(String indexName) throws SemanticSearchException {
        HttpHead request = new HttpHead(baseUrl() + "/" + indexName);
        addAuthHeader(request);
        HttpResponse response = executeHttp(request);
        if (response.statusCode == 404) {
            return false;
        }
        if (response.statusCode < 200 || response.statusCode >= 300) {
            throw httpFailure(response);
        }
        return true;
    }

    private List<Map<String, Object>> buildFilters(VectorSearchFilter filter) {
        List<Map<String, Object>> filters = new ArrayList<>();

        if (!filter.getTypeNames().isEmpty()) {
            // JanusGraph maps __typeName as text; exact type filtering requires the keyword subfield.
            filters.add(termsFilter(typeNameFilterField(), filter.getTypeNames()));
        }

        if (!filter.getExcludeGuids().isEmpty()) {
            Map<String, Object> mustNot = new HashMap<>();
            mustNot.put("terms", Collections.singletonMap(guidFilterField(), new ArrayList<>(filter.getExcludeGuids())));

            Map<String, Object> bool = new HashMap<>();
            bool.put("must_not", Collections.singletonList(mustNot));
            filters.add(Collections.singletonMap("bool", bool));
        }

        return filters;
    }

    private static String typeNameFilterField() {
        return Constants.ENTITY_TYPE_PROPERTY_KEY + ".keyword";
    }

    private static String guidFilterField() {
        return Constants.GUID_PROPERTY_KEY + ".keyword";
    }

    private Map<String, Object> termsFilter(String field, Set<String> values) {
        Map<String, Object> termsValue = new HashMap<>();
        termsValue.put(field, new ArrayList<>(values));

        Map<String, Object> terms = new HashMap<>();
        terms.put("terms", termsValue);
        return terms;
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

        Object id = hit.get("_id");
        return id != null ? id.toString() : null;
    }

    private String baseUrl() throws SemanticSearchException {
        return SemanticSearchConfiguration.getOpenSearchBaseUrl();
    }

    void validateCluster() throws SemanticSearchException {
        String body = executeRequestForBody(new HttpGet(baseUrl() + "/"));
        Map<String, Object> root = AtlasJson.fromJson(body, Map.class);

        if (root == null || !(root.get("version") instanceof Map)) {
            throw new SemanticSearchException(
                    SemanticSearchConfiguration.GRAPH_INDEX_HOSTNAME_CONF
                            + " does not point to OpenSearch (missing cluster version in response)");
        }

        Map<String, Object> version = (Map<String, Object>) root.get("version");
        String distribution = parseOpenSearchDistribution(version);

        if (!"opensearch".equalsIgnoreCase(distribution)) {
            throw new SemanticSearchException(
                    SemanticSearchConfiguration.GRAPH_INDEX_HOSTNAME_CONF
                            + " must point to an OpenSearch cluster with neural search support (got distribution="
                            + distribution + ")");
        }
    }

    @SuppressWarnings("unchecked")
    static String parseOpenSearchDistribution(Map<String, Object> version) {
        if (version == null || version.get("distribution") == null) {
            return null;
        }

        return version.get("distribution").toString();
    }

    private String executeRequestForBody(HttpUriRequest request) throws SemanticSearchException {
        return executeRequest(request).body;
    }

    private HttpResponse executeRequest(HttpUriRequest request) throws SemanticSearchException {
        addAuthHeader(request);
        return SemanticRetry.run("OpenSearch " + request.getMethod(), () -> {
            HttpResponse response = executeHttp(request);
            if (isSuccess(response.statusCode)) {
                return response;
            }
            throw httpFailure(response);
        });
    }

    private static boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private HttpResponse executeHttp(HttpUriRequest request) throws SemanticSearchException {
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int    statusCode   = response.getStatusLine().getStatusCode();
            String responseBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "";
            return new HttpResponse(statusCode, responseBody);
        } catch (IOException e) {
            throw new SemanticSearchException("HTTP request failed", e, true);
        }
    }

    private static SemanticSearchException httpFailure(HttpResponse response) {
        String message = "HTTP request failed with status " + response.statusCode;
        if (StringUtils.isNotEmpty(response.body)) {
            message += ": " + response.body;
        }
        boolean retryable = isTransientHttpStatus(response.statusCode) || isVersionConflictResponse(response);
        return new SemanticSearchException(message, retryable);
    }

    private static boolean isTransientHttpStatus(int status) {
        return status == 409 || status == 429 || status == 502 || status == 503 || status == 504;
    }

    private static boolean isVersionConflictResponse(HttpResponse response) {
        return response.statusCode == 409
                && response.body != null
                && response.body.contains("version_conflict_engine_exception");
    }

    private static final class HttpResponse {
        private final int    statusCode;
        private final String body;

        private HttpResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body       = body;
        }
    }

    private void addAuthHeader(HttpUriRequest request) {
        String username = SemanticSearchConfiguration.getOpenSearchUsername();
        String password = SemanticSearchConfiguration.getOpenSearchPassword();
        if (StringUtils.isBlank(username)) {
            return;
        }

        String credentials = username + ":" + StringUtils.defaultString(password);
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + encoded);
    }
}
