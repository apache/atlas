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
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration for semantic search read path, Semantic Indexer service, and repair tool.
 */
public final class SemanticSearchConfiguration {
    public static final String SEMANTIC_SEARCH_ENABLE_CONF                      = "atlas.search.semantic.enable";
    public static final String SEMANTIC_OPENSEARCH_MODEL_ID_CONF                = "atlas.search.semantic.opensearch.model.id";
    public static final String SEMANTIC_OPENSEARCH_EMBEDDING_DIMENSION_CONF     = "atlas.search.semantic.opensearch.embedding.dimension";
    public static final String SEMANTIC_DEFAULT_TOP_K_CONF                      = "atlas.search.semantic.default.topK";
    public static final String SEMANTIC_MIN_SCORE_CONF                          = "atlas.search.semantic.min.score";
    public static final String SEMANTIC_RETRY_MAX_ATTEMPTS_CONF                 = "atlas.search.semantic.retry.max.attempts";
    public static final String SEMANTIC_RETRY_SLEEP_MS_CONF                     = "atlas.search.semantic.retry.sleep.ms";

    public static final String SEMANTIC_INDEXER_KAFKA_GROUP_ID_CONF             = "atlas.semantic.indexer.kafka.group.id";
    public static final String SEMANTIC_INDEXER_BATCH_SIZE_CONF                 = "atlas.semantic.indexer.batch.size";
    public static final String SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS_CONF      = "atlas.semantic.indexer.kafka.poll.timeout.ms";
    public static final String SEMANTIC_INDEXER_HEALTH_ENABLED_CONF             = "atlas.semantic.indexer.health.enabled";
    public static final String SEMANTIC_INDEXER_HEALTH_PORT_CONF                = "atlas.semantic.indexer.health.port";
    public static final String SEMANTIC_INDEXER_HEALTH_PATH_CONF                = "atlas.semantic.indexer.health.path";

    public static final String GRAPH_INDEX_HOSTNAME_CONF                        = "atlas.graph.index.search.hostname";
    public static final String GRAPH_INDEX_PORT_CONF                            = "atlas.graph.index.search.port";
    public static final String GRAPH_INDEX_NAME_CONF                            = "atlas.graph.index.search.index-name";
    public static final String GRAPH_INDEX_USERNAME_CONF                        = "atlas.graph.index.search.opensearch.username";
    public static final String GRAPH_INDEX_PASSWORD_CONF                        = "atlas.graph.index.search.opensearch.password";
    public static final String GRAPH_INDEX_USE_HTTPS_CONF                       = "atlas.graph.index.search.opensearch.use.https";

    public static final String SEMANTIC_TEXT_FIELD                              = "atlas_semantic_text";
    public static final String SEMANTIC_EMBEDDING_FIELD                         = "atlas_semantic_embedding";
    public static final String SEMANTIC_INGEST_PIPELINE_NAME                    = "atlas-semantic-ingest";
    public static final String VERTEX_INDEX_SUFFIX                              = "_vertex_index";

    private static final int    DEFAULT_SEMANTIC_TOP_K                          = 25;
    private static final double DEFAULT_SEMANTIC_MIN_SCORE                      = 0.0;
    private static final int    DEFAULT_SEMANTIC_INDEXER_BATCH_SIZE             = 25;
    private static final int    DEFAULT_SEMANTIC_RETRY_MAX_ATTEMPTS             = 3;
    private static final long   DEFAULT_SEMANTIC_RETRY_SLEEP_MS                 = 500L;
    private static final String DEFAULT_SEMANTIC_INDEXER_KAFKA_GROUP_ID         = "atlas_semantic_indexer";
    private static final long   DEFAULT_SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS  = 5000L;
    private static final int    DEFAULT_SEMANTIC_INDEXER_HEALTH_PORT            = 8089;
    private static final String DEFAULT_SEMANTIC_INDEXER_HEALTH_PATH            = "/health";
    private static final String DEFAULT_GRAPH_INDEX_NAME                        = "janusgraph";

    private SemanticSearchConfiguration() {
    }

    public static boolean isSemanticSearchEnabled() {
        try {
            return ApplicationProperties.get().getBoolean(SEMANTIC_SEARCH_ENABLE_CONF, false);
        } catch (AtlasException e) {
            return false;
        }
    }

    public static String getOpenSearchModelId() {
        try {
            return ApplicationProperties.get().getString(SEMANTIC_OPENSEARCH_MODEL_ID_CONF, "").trim();
        } catch (AtlasException e) {
            return "";
        }
    }

    public static int getOpenSearchEmbeddingDimension() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_OPENSEARCH_EMBEDDING_DIMENSION_CONF, 384);
        } catch (AtlasException e) {
            return 384;
        }
    }

    public static int getDefaultTopK() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_DEFAULT_TOP_K_CONF, DEFAULT_SEMANTIC_TOP_K);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_TOP_K;
        }
    }

    public static double getMinScore() {
        try {
            return ApplicationProperties.get().getDouble(SEMANTIC_MIN_SCORE_CONF, DEFAULT_SEMANTIC_MIN_SCORE);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_MIN_SCORE;
        }
    }

    public static int getRetryMaxAttempts() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_RETRY_MAX_ATTEMPTS_CONF, DEFAULT_SEMANTIC_RETRY_MAX_ATTEMPTS);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_RETRY_MAX_ATTEMPTS;
        }
    }

    public static long getRetrySleepMs() {
        try {
            return ApplicationProperties.get().getLong(SEMANTIC_RETRY_SLEEP_MS_CONF, DEFAULT_SEMANTIC_RETRY_SLEEP_MS);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_RETRY_SLEEP_MS;
        }
    }

    public static int getSemanticIndexerBatchSize() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_INDEXER_BATCH_SIZE_CONF, DEFAULT_SEMANTIC_INDEXER_BATCH_SIZE);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_BATCH_SIZE;
        }
    }

    public static String getSemanticIndexerKafkaGroupId() {
        try {
            return ApplicationProperties.get().getString(SEMANTIC_INDEXER_KAFKA_GROUP_ID_CONF, DEFAULT_SEMANTIC_INDEXER_KAFKA_GROUP_ID);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_KAFKA_GROUP_ID;
        }
    }

    public static long getSemanticIndexerKafkaPollTimeoutMs() {
        try {
            return ApplicationProperties.get().getLong(SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS_CONF,
                    DEFAULT_SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS;
        }
    }

    public static boolean isSemanticIndexerHealthEnabled() {
        try {
            return ApplicationProperties.get().getBoolean(SEMANTIC_INDEXER_HEALTH_ENABLED_CONF, true);
        } catch (AtlasException e) {
            return true;
        }
    }

    public static int getSemanticIndexerHealthPort() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_INDEXER_HEALTH_PORT_CONF,
                    DEFAULT_SEMANTIC_INDEXER_HEALTH_PORT);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_HEALTH_PORT;
        }
    }

    public static String getSemanticIndexerHealthPath() {
        try {
            return ApplicationProperties.get().getString(SEMANTIC_INDEXER_HEALTH_PATH_CONF,
                    DEFAULT_SEMANTIC_INDEXER_HEALTH_PATH);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_HEALTH_PATH;
        }
    }

    public static String getGraphIndexName() {
        try {
            return ApplicationProperties.get().getString(GRAPH_INDEX_NAME_CONF, DEFAULT_GRAPH_INDEX_NAME);
        } catch (AtlasException e) {
            return DEFAULT_GRAPH_INDEX_NAME;
        }
    }

    public static String getVertexIndexName() {
        return getGraphIndexName() + VERTEX_INDEX_SUFFIX;
    }

    public static String getOpenSearchBaseUrl() throws SemanticSearchException {
        try {
            Configuration config = ApplicationProperties.get();
            String        host   = config.getString(GRAPH_INDEX_HOSTNAME_CONF, "").trim();

            if (StringUtils.isBlank(host)) {
                throw new SemanticSearchException(GRAPH_INDEX_HOSTNAME_CONF + " must be set for semantic search");
            }

            boolean https = config.getBoolean(GRAPH_INDEX_USE_HTTPS_CONF, false);
            int     port  = config.getInt(GRAPH_INDEX_PORT_CONF, https ? 443 : 9200);
            String  scheme = https ? "https" : "http";

            return scheme + "://" + host + ":" + port;
        } catch (AtlasException e) {
            throw new SemanticSearchException("Failed to read OpenSearch connection settings", e);
        }
    }

    public static String getOpenSearchUsername() {
        try {
            return ApplicationProperties.get().getString(GRAPH_INDEX_USERNAME_CONF, "");
        } catch (AtlasException e) {
            return "";
        }
    }

    public static String getOpenSearchPassword() {
        try {
            return ApplicationProperties.get().getString(GRAPH_INDEX_PASSWORD_CONF, "");
        } catch (AtlasException e) {
            return "";
        }
    }

    public static void validateWhenEnabled() throws SemanticSearchException {
        if (!isSemanticSearchEnabled()) {
            return;
        }

        try {
            Configuration config = ApplicationProperties.get();

            if (StringUtils.isBlank(config.getString(GRAPH_INDEX_HOSTNAME_CONF, ""))) {
                throw new SemanticSearchException(GRAPH_INDEX_HOSTNAME_CONF + " must be set when semantic search is enabled");
            }

            if (StringUtils.isBlank(config.getString(SEMANTIC_OPENSEARCH_MODEL_ID_CONF, ""))) {
                throw new SemanticSearchException(SEMANTIC_OPENSEARCH_MODEL_ID_CONF + " must be set when semantic search is enabled");
            }

            int dimensions = config.getInt(SEMANTIC_OPENSEARCH_EMBEDDING_DIMENSION_CONF, 0);
            if (dimensions <= 0) {
                throw new SemanticSearchException(SEMANTIC_OPENSEARCH_EMBEDDING_DIMENSION_CONF + " must be a positive integer");
            }
        } catch (AtlasException e) {
            throw new SemanticSearchException("Failed to read semantic search configuration", e);
        }
    }
}
