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
    public static final String SEMANTIC_ENABLED_CONF                            = "atlas.semantic.enabled";
    public static final String SEMANTIC_MODEL_ID_CONF                           = "atlas.semantic.model.id";
    public static final String SEMANTIC_EMBEDDING_DIMENSION_CONF                = "atlas.semantic.embedding.dimension";
    public static final String SEMANTIC_RETRY_MAX_ATTEMPTS_CONF                 = "atlas.semantic.retry.max.attempts";
    public static final String SEMANTIC_RETRY_SLEEP_MS_CONF                     = "atlas.semantic.retry.sleep.ms";

    public static final String SEMANTIC_SEARCH_DEFAULT_TOP_K_CONF               = "atlas.semantic.search.default.topK";
    public static final String SEMANTIC_SEARCH_MIN_SCORE_CONF                   = "atlas.semantic.search.min.score";

    public static final String SEMANTIC_INDEXER_KAFKA_GROUP_ID_CONF             = "atlas.semantic.indexer.kafka.group.id";
    public static final String SEMANTIC_INDEXER_BATCH_SIZE_CONF                 = "atlas.semantic.indexer.batch.size";
    public static final String SEMANTIC_INDEXER_KAFKA_POLL_TIMEOUT_MS_CONF      = "atlas.semantic.indexer.kafka.poll.timeout.ms";
    public static final String SEMANTIC_INDEXER_KAFKA_MAX_POLL_INTERVAL_MS_CONF = "atlas.semantic.indexer.kafka.max.poll.interval.ms";
    public static final String SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS_CONF     = "atlas.semantic.indexer.kafka.max.poll.records";
    public static final String SEMANTIC_INDEXER_MAX_TERM_ENTITIES_CONF          = "atlas.semantic.indexer.max.term.entities";
    public static final String SEMANTIC_INDEXER_HEALTH_ENABLED_CONF             = "atlas.semantic.indexer.health.enabled";
    public static final String SEMANTIC_INDEXER_HEALTH_PORT_CONF                = "atlas.semantic.indexer.health.port";
    public static final String SEMANTIC_INDEXER_HEALTH_PATH_CONF                = "atlas.semantic.indexer.health.path";

    public static final String GRAPH_INDEX_HOSTNAME_CONF                        = SemanticOpenSearchHttpClient.HOSTNAME_CONF;
    public static final String GRAPH_INDEX_NAME_CONF                            = "atlas.graph.index.search.index-name";
    public static final String GRAPH_INDEX_CREATE_KNN_CONF                      = "atlas.graph.index.search.opensearch.create.ext.knn";

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
    private static final int    DEFAULT_SEMANTIC_INDEXER_KAFKA_MAX_POLL_INTERVAL_MS = 30 * 60 * 1000;
    private static final int    DEFAULT_SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS = 25;
    private static final int    DEFAULT_SEMANTIC_INDEXER_MAX_TERM_ENTITIES      = 100;
    private static final int    DEFAULT_SEMANTIC_INDEXER_HEALTH_PORT            = 8089;
    private static final String DEFAULT_SEMANTIC_INDEXER_HEALTH_PATH            = "/health";
    private static final String DEFAULT_GRAPH_INDEX_NAME                        = "janusgraph";

    private SemanticSearchConfiguration() {
    }

    public static boolean isSemanticSearchEnabled() {
        try {
            return ApplicationProperties.get().getBoolean(SEMANTIC_ENABLED_CONF, false);
        } catch (AtlasException e) {
            return false;
        }
    }

    public static String getOpenSearchModelId() {
        try {
            return ApplicationProperties.get().getString(SEMANTIC_MODEL_ID_CONF, "").trim();
        } catch (AtlasException e) {
            return "";
        }
    }

    public static int getOpenSearchEmbeddingDimension() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_EMBEDDING_DIMENSION_CONF, 384);
        } catch (AtlasException e) {
            return 384;
        }
    }

    public static int getDefaultTopK() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_SEARCH_DEFAULT_TOP_K_CONF, DEFAULT_SEMANTIC_TOP_K);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_TOP_K;
        }
    }

    public static double getMinScore() {
        try {
            return ApplicationProperties.get().getDouble(SEMANTIC_SEARCH_MIN_SCORE_CONF, DEFAULT_SEMANTIC_MIN_SCORE);
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

    public static int getSemanticIndexerKafkaMaxPollIntervalMs() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_INDEXER_KAFKA_MAX_POLL_INTERVAL_MS_CONF,
                    DEFAULT_SEMANTIC_INDEXER_KAFKA_MAX_POLL_INTERVAL_MS);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_KAFKA_MAX_POLL_INTERVAL_MS;
        }
    }

    public static int getSemanticIndexerKafkaMaxPollRecords() {
        try {
            return ApplicationProperties.get().getInt(SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS_CONF,
                    DEFAULT_SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS);
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_KAFKA_MAX_POLL_RECORDS;
        }
    }

    /**
     * Max entities re-embedded when one glossary term changes (the term's text is part of each entity's text);
     * 0 disables this fan-out.
     */
    public static int getSemanticIndexerMaxTermEntities() {
        try {
            return Math.max(0, ApplicationProperties.get().getInt(SEMANTIC_INDEXER_MAX_TERM_ENTITIES_CONF,
                    DEFAULT_SEMANTIC_INDEXER_MAX_TERM_ENTITIES));
        } catch (AtlasException e) {
            return DEFAULT_SEMANTIC_INDEXER_MAX_TERM_ENTITIES;
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

    /**
     * Validates the configuration when {@link #SEMANTIC_ENABLED_CONF} is true. Must run before the graph is opened:
     * JanusGraph creates the vertex index on first open, and index.knn can't be added to it afterwards.
     */
    public static void validateWhenEnabled() throws SemanticSearchException {
        if (isSemanticSearchEnabled()) {
            validate();
        }
    }

    /**
     * Validates the configuration regardless of {@link #SEMANTIC_ENABLED_CONF} (which only gates the REST search):
     * used by the Semantic Indexer and repair tool, before they open the graph.
     */
    public static void validate() throws SemanticSearchException {
        try {
            Configuration config = ApplicationProperties.get();

            if (StringUtils.isBlank(config.getString(GRAPH_INDEX_HOSTNAME_CONF, ""))) {
                throw new SemanticSearchException(GRAPH_INDEX_HOSTNAME_CONF + " must be set for semantic search");
            }

            if (!config.getBoolean(GRAPH_INDEX_CREATE_KNN_CONF, false)) {
                throw new SemanticSearchException(GRAPH_INDEX_CREATE_KNN_CONF + "=true is required for semantic search:"
                        + " index.knn can only be set when JanusGraph creates the vertex index"
                        + " (an existing index must be cloned with index.knn=true, see SemanticSearch.md)");
            }

            if (StringUtils.isBlank(config.getString(SEMANTIC_MODEL_ID_CONF, ""))) {
                throw new SemanticSearchException(SEMANTIC_MODEL_ID_CONF + " must be set for semantic search");
            }

            int dimensions = config.getInt(SEMANTIC_EMBEDDING_DIMENSION_CONF, 0);
            if (dimensions <= 0) {
                throw new SemanticSearchException(SEMANTIC_EMBEDDING_DIMENSION_CONF + " must be a positive integer");
            }
        } catch (AtlasException e) {
            throw new SemanticSearchException("Failed to read semantic search configuration", e);
        }
    }
}
