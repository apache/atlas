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
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.semantic.SemanticHttpClient.Response;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.semantic.OpenSearchSemanticStore.SEMANTIC_EMBEDDING_FIELD;
import static org.apache.atlas.semantic.OpenSearchSemanticStore.SEMANTIC_INGEST_PIPELINE_NAME;
import static org.apache.atlas.semantic.OpenSearchSemanticStore.SEMANTIC_TEXT_FIELD;

/**
 * OpenSearch setup for semantic search. {@link #run()}, behind {@link OpenSearchSemanticStore#initialize()}, checks
 * the cluster and {@code index.knn} on the JanusGraph vertex index, then ensures the ingest pipeline and the
 * embedding field mapping. {@link #validateConfiguration()} runs in the Semantic Indexer and the repair tool before
 * they open the graph. The Atlas server only runs the read-only {@link #checkSearchable(int)} on the search path.
 */
public final class SemanticIndexSetup {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticIndexSetup.class);

    private static final String GRAPH_INDEX_CREATE_KNN_CONF = "atlas.graph.index.search.opensearch.create.ext.knn";

    private final SemanticHttpClient httpClient;

    SemanticIndexSetup(SemanticHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * Validates the configuration regardless of atlas.semantic.enabled (which only gates the REST search). Must run
     * before the graph is opened: JanusGraph creates the vertex index on first open, and index.knn can't be added
     * to it afterwards.
     */
    public static void validateConfiguration() throws SemanticSearchException {
        Configuration config;

        try {
            config = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new SemanticSearchException("Failed to read semantic search configuration", e);
        }

        if (StringUtils.isBlank(config.getString(OpenSearchClientFactory.HOSTNAME_CONF, ""))) {
            throw new SemanticSearchException(OpenSearchClientFactory.HOSTNAME_CONF + " must be set for semantic search");
        }

        if (!config.getBoolean(GRAPH_INDEX_CREATE_KNN_CONF, false)) {
            throw new SemanticSearchException(GRAPH_INDEX_CREATE_KNN_CONF + "=true is required for semantic search:"
                    + " index.knn can only be set when JanusGraph creates the vertex index"
                    + " (an existing index must be cloned with index.knn=true, see SemanticSearch.md)");
        }

        if (StringUtils.isBlank(AtlasConfiguration.SEMANTIC_MODEL_ID.getString())) {
            throw new SemanticSearchException(AtlasConfiguration.SEMANTIC_MODEL_ID.getPropertyName() + " must be set for semantic search");
        }

        if (AtlasConfiguration.SEMANTIC_EMBEDDING_DIMENSION.getInt() <= 0) {
            throw new SemanticSearchException(AtlasConfiguration.SEMANTIC_EMBEDDING_DIMENSION.getPropertyName() + " must be a positive integer");
        }
    }

    void run() throws SemanticSearchException {
        validateCluster();
        ensureIngestPipeline();
        ensureVertexIndexSemanticFields();
    }

    /**
     * Read-only, single attempt per request: checks that the vertex index has index.knn and the embedding field.
     */
    void checkSearchable(int socketTimeoutMs) throws SemanticSearchException {
        String indexName = OpenSearchSemanticStore.getVertexIndexName();

        String settings = httpClient.sendForBody(new HttpGet("/" + indexName + "/_settings/index.knn?include_defaults=true"), socketTimeoutMs);
        if (!parseKnnEnabledFromSettingsResponse(AtlasJson.fromJson(settings, Map.class))) {
            LOG.error("Semantic search unavailable: index.knn is not enabled on vertex index '{}' (see SemanticSearch.md)", indexName);
            throw new SemanticSearchException(AtlasErrorCode.SEMANTIC_SEARCH_NOT_READY, "the OpenSearch vertex index was created without index.knn");
        }

        String mapping = httpClient.sendForBody(new HttpGet("/" + indexName + "/_mapping"), socketTimeoutMs);
        if (parseEmbeddingDimensionFromMapping(AtlasJson.fromJson(mapping, Map.class), indexName) == null) {
            LOG.error("Semantic search unavailable: vertex index '{}' has no '{}' field; start the Semantic Indexer or run the repair tool once",
                    indexName, SEMANTIC_EMBEDDING_FIELD);
            throw new SemanticSearchException(AtlasErrorCode.SEMANTIC_SEARCH_NOT_READY, "the embedding field is not set up yet");
        }
    }

    @SuppressWarnings("unchecked")
    private void validateCluster() throws SemanticSearchException {
        Map<String, Object> root = AtlasJson.fromJson(sendWithRetry(new HttpGet("/")), Map.class);

        if (root == null || !(root.get("version") instanceof Map)) {
            throw new SemanticSearchException(
                    OpenSearchClientFactory.HOSTNAME_CONF
                            + " does not point to OpenSearch (missing cluster version in response)");
        }

        String distribution = parseOpenSearchDistribution((Map<String, Object>) root.get("version"));

        if (!"opensearch".equalsIgnoreCase(distribution)) {
            throw new SemanticSearchException(
                    OpenSearchClientFactory.HOSTNAME_CONF
                            + " must point to an OpenSearch cluster with neural search support (got distribution="
                            + distribution + ")");
        }
    }

    static String parseOpenSearchDistribution(Map<String, Object> version) {
        if (version == null || version.get("distribution") == null) {
            return null;
        }

        return version.get("distribution").toString();
    }

    private void ensureIngestPipeline() throws SemanticSearchException {
        String modelId = AtlasConfiguration.SEMANTIC_MODEL_ID.getString().trim();

        HttpPut put = new HttpPut("/_ingest/pipeline/" + SEMANTIC_INGEST_PIPELINE_NAME);
        put.setEntity(new StringEntity(AtlasJson.toJson(buildIngestPipelineBody(modelId)), ContentType.APPLICATION_JSON));
        sendWithRetry(put);
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
        String indexName  = OpenSearchSemanticStore.getVertexIndexName();
        int    dimensions = AtlasConfiguration.SEMANTIC_EMBEDDING_DIMENSION.getInt();

        if (!indexExists(indexName)) {
            throw new SemanticSearchException("Vertex index '" + indexName + "' does not exist in OpenSearch");
        }

        ensureKnnEnabled(indexName);
        ensureSemanticMapping(indexName, dimensions);
    }

    private boolean indexExists(String indexName) throws SemanticSearchException {
        Response response = httpClient.send(new HttpHead("/" + indexName));
        if (response.statusCode == 404) {
            return false;
        }
        if (!SemanticHttpClient.isSuccess(response.statusCode)) {
            throw SemanticHttpClient.httpFailure(response);
        }
        return true;
    }

    // index.knn is a final setting: OpenSearch rejects changing it on an existing index (open or closed)
    private void ensureKnnEnabled(String indexName) throws SemanticSearchException {
        if (!isKnnEnabled(indexName)) {
            throw new SemanticSearchException("index.knn is not enabled on vertex index '" + indexName
                    + "'; set " + GRAPH_INDEX_CREATE_KNN_CONF + "=true before JanusGraph creates the index"
                    + " (existing index: clone it with index.knn=true, see SemanticSearch.md)");
        }
    }

    private boolean isKnnEnabled(String indexName) throws SemanticSearchException {
        // OpenSearch default (nested) settings response; include_defaults for implicit plugin defaults.
        String response = sendWithRetry(new HttpGet("/" + indexName + "/_settings/index.knn?include_defaults=true"));

        try {
            return parseKnnEnabledFromSettingsResponse(AtlasJson.fromJson(response, Map.class));
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
        return Boolean.TRUE.equals(knn) || "true".equalsIgnoreCase(String.valueOf(knn));
    }

    // a knn_vector mapping can't be changed once added, so only add it when missing and never overwrite it
    private void ensureSemanticMapping(String indexName, int configuredDimensions) throws SemanticSearchException {
        Integer indexDimension = getIndexEmbeddingDimension(indexName);

        if (indexDimension == null) {
            addSemanticMapping(indexName, configuredDimensions);
            indexDimension = getIndexEmbeddingDimension(indexName);
        } else {
            LOG.info("Semantic field '{}' already mapped on index '{}' (dimension={})", SEMANTIC_EMBEDDING_FIELD, indexName, indexDimension);
        }

        if (indexDimension == null) {
            throw new SemanticSearchException(
                    "Vertex index '" + indexName + "' is missing knn_vector field '" + SEMANTIC_EMBEDDING_FIELD + "'");
        }

        if (indexDimension != configuredDimensions) {
            throw new SemanticSearchException(
                    "Vertex index '" + indexName + "' embedding dimension is " + indexDimension
                            + " but " + AtlasConfiguration.SEMANTIC_EMBEDDING_DIMENSION.getPropertyName()
                            + " is " + configuredDimensions);
        }
    }

    private void addSemanticMapping(String indexName, int dimensions) throws SemanticSearchException {
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

        HttpPut put = new HttpPut("/" + indexName + "/_mapping");
        put.setEntity(new StringEntity(AtlasJson.toJson(mappings), ContentType.APPLICATION_JSON));
        sendWithRetry(put);
        LOG.info("Added semantic field '{}' (dimension={}) to OpenSearch index '{}'", SEMANTIC_EMBEDDING_FIELD, dimensions, indexName);
    }

    private Integer getIndexEmbeddingDimension(String indexName) throws SemanticSearchException {
        String response = sendWithRetry(new HttpGet("/" + indexName + "/_mapping"));
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

    private String sendWithRetry(HttpUriRequest request) throws SemanticSearchException {
        return SemanticRetry.run("OpenSearch " + request.getMethod(), () -> httpClient.sendForBody(request));
    }
}
