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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.SemanticSearchParameters;
import org.apache.atlas.model.discovery.SimilarEntitySearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Component
public class SemanticSearchService {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticSearchService.class);

    private static final int MAX_TOP_K               = 100;
    // hard ceiling on text sent to the ML model, regardless of atlas.query.param.max.length
    private static final int MAX_QUERY_LENGTH        = 30_000;
    // OpenSearch error bodies name indexes and internals: they go to the server log, not to API clients
    private static final String BACKEND_ERROR_MESSAGE   = "(semantic search backend error, see Atlas server log)";
    private static final int SEARCH_OVERFETCH_FACTOR = 2;
    private static final int SEARCH_OVERFETCH_MAX    = MAX_TOP_K * SEARCH_OVERFETCH_FACTOR;

    private final SemanticVectorStore   semanticStore;
    private final EntityGraphRetriever  entityRetriever;
    private final SemanticTextBuilder   textBuilder;
    private final AtlasTypeRegistry     typeRegistry;

    @Inject
    public SemanticSearchService(SemanticVectorStore semanticStore,
                                 AtlasGraph graph,
                                 AtlasTypeRegistry typeRegistry,
                                 SemanticTextBuilder textBuilder) {
        this.semanticStore   = semanticStore;
        this.typeRegistry    = typeRegistry;
        this.textBuilder     = textBuilder;
        this.entityRetriever = new EntityGraphRetriever(graph, typeRegistry);
    }

    public AtlasSearchResult semanticSearch(SemanticSearchParameters parameters) throws AtlasBaseException {
        ensureEnabled();

        if (parameters == null || StringUtils.isBlank(parameters.getQuery())) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "query");
        }

        validateQueryLength(parameters.getQuery());
        int    topK     = resolveTopK(parameters.getTopK());
        double minScore = parameters.getMinScore() > 0 ? parameters.getMinScore() : AtlasConfiguration.SEMANTIC_SEARCH_MIN_SCORE.getDouble();

        try {
            Set<String> typeNames = resolveTypeNames(parameters.getTypeName(), parameters.getIncludeSubTypes());
            VectorSearchFilter filter = new VectorSearchFilter(typeNames, Collections.emptySet());
            int fetchSize = overfetchSize(topK);
            LOG.debug("semanticSearch query='{}' topK={} minScore={} typeName={}",
                    parameters.getQuery(), topK, minScore, parameters.getTypeName());
            List<VectorSearchHit> hits = semanticStore.searchByText(parameters.getQuery(), fetchSize, filter);

            return buildSearchResult(parameters.getQuery(), hits, minScore, topK,
                    parameters.getAttributes(), parameters.getExcludeDeletedEntities());
        } catch (SemanticSearchException e) {
            if (e.getErrorCode() != null) {
                throw new AtlasBaseException(e.getErrorCode(), e, e.getMessage());
            }
            LOG.error("Semantic search failed for query={}", parameters.getQuery(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e, BACKEND_ERROR_MESSAGE);
        }
    }

    public AtlasSearchResult similarEntities(String guid, SimilarEntitySearchParameters parameters) throws AtlasBaseException {
        ensureEnabled();

        if (StringUtils.isBlank(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid");
        }

        int    topK     = resolveTopK(parameters.getTopK());
        double minScore = parameters.getMinScore() > 0 ? parameters.getMinScore() : AtlasConfiguration.SEMANTIC_SEARCH_MIN_SCORE.getDouble();

        try {
            AtlasVertex       vertex = entityRetriever.getEntityVertex(guid);
            AtlasEntityHeader header = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, header), "read similar entities: guid=", guid);

            if (header.getStatus() != AtlasEntity.Status.ACTIVE) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            Set<String> typeNames = resolveTypeNames(parameters.getTypeName(), parameters.getIncludeSubTypes());
            VectorSearchFilter filter = new VectorSearchFilter(typeNames, Collections.singleton(guid));
            int fetchSize = overfetchSize(topK);

            List<VectorSearchHit> hits = similarSearchHits(guid, vertex, fetchSize, filter);
            LOG.debug("similarEntities guid={} topK={} minScore={} typeName={}", guid, topK, minScore, parameters.getTypeName());

            return buildSearchResult("similar:" + guid, hits, minScore, topK, parameters.getAttributes(), parameters.getExcludeDeletedEntities());
        } catch (SemanticSearchException e) {
            if (e.getErrorCode() != null) {
                throw new AtlasBaseException(e.getErrorCode(), e, e.getMessage());
            }
            LOG.error("Similar entity search failed for guid={}", guid, e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e, BACKEND_ERROR_MESSAGE);
        }
    }

    private AtlasSearchResult buildSearchResult(String queryText,
                                                List<VectorSearchHit> hits,
                                                double minScore,
                                                int topK,
                                                Set<String> attributes,
                                                boolean excludeDeletedEntities) throws AtlasBaseException {
        AtlasSearchResult         result  = new AtlasSearchResult(queryText, AtlasQueryType.SEMANTIC);
        List<AtlasFullTextResult> results = new ArrayList<>();

        for (VectorSearchHit hit : hits) {
            if (hit.getScore() < minScore) {
                continue;
            }

            if (results.size() >= topK) {
                break;
            }

            AtlasEntityHeader entityHeader = loadHeaderFromGraph(hit, attributes, excludeDeletedEntities);
            if (entityHeader == null) {
                continue;
            }

            results.add(new AtlasFullTextResult(entityHeader, hit.getScore()));
        }

        result.setFullTextResult(results);
        result.setApproximateCount(results.size());

        LOG.debug("semantic search completed queryText='{}' returned={} hits={}",
                queryText, results.size(), hits.size());

        return result;
    }

    private AtlasEntityHeader loadHeaderFromGraph(VectorSearchHit hit,
                                                  Set<String> attributes,
                                                  boolean excludeDeletedEntities) throws AtlasBaseException {
        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(hit.getGuid());
        if (vertex == null) {
            return null;
        }

        if (excludeDeletedEntities && AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
            return null;
        }

        try {
            return entityRetriever.toAtlasEntityHeader(vertex, attributes);
        } catch (AtlasBaseException e) {
            LOG.warn("Skipping semantic search hit guid={} because entity could not be loaded from graph", hit.getGuid(), e);
            return null;
        }
    }

    private List<VectorSearchHit> similarSearchHits(String guid,
                                                    AtlasVertex vertex,
                                                    int fetchSize,
                                                    VectorSearchFilter filter) throws AtlasBaseException, SemanticSearchException {
        List<?> storedEmbedding = semanticStore.getStoredEmbedding(guid);
        if (storedEmbedding != null && !storedEmbedding.isEmpty()) {
            LOG.debug("Similar search for guid={} using stored embedding", guid);
            return semanticStore.searchByVector(storedEmbedding, fetchSize, filter);
        }

        String sourceText = textBuilder.buildText(vertex);
        if (StringUtils.isBlank(sourceText)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    "Entity has no semantic index text: " + guid);
        }

        LOG.debug("Similar search for guid={} falling back to neural query (no stored embedding)", guid);
        return semanticStore.searchByText(sourceText, fetchSize, filter);
    }

    private int resolveTopK(int topK) throws AtlasBaseException {
        int resolved = topK > 0 ? topK : AtlasConfiguration.SEMANTIC_SEARCH_DEFAULT_TOP_K.getInt();

        if (resolved > MAX_TOP_K) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                    "topK (" + resolved + ") exceeds maximum allowed (" + MAX_TOP_K + ")");
        }

        return resolved;
    }

    private static void validateQueryLength(String query) throws AtlasBaseException {
        if (query != null && query.length() > MAX_QUERY_LENGTH) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                    "query length (" + query.length() + ") exceeds maximum allowed (" + MAX_QUERY_LENGTH + ")");
        }
    }

    private static int overfetchSize(int minimumHits) {
        return Math.min(Math.max(minimumHits * SEARCH_OVERFETCH_FACTOR, minimumHits), SEARCH_OVERFETCH_MAX);
    }

    private Set<String> resolveTypeNames(String typeName, boolean includeSubTypes) throws AtlasBaseException {
        if (StringUtils.isBlank(typeName)) {
            return Collections.emptySet();
        }

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, typeName);
        }

        return includeSubTypes ? entityType.getTypeAndAllSubTypes() : Collections.singleton(typeName);
    }

    private void ensureEnabled() throws AtlasBaseException {
        if (!AtlasConfiguration.SEMANTIC_ENABLED.getBoolean()) {
            throw new AtlasBaseException(AtlasErrorCode.SEMANTIC_SEARCH_DISABLED, AtlasConfiguration.SEMANTIC_ENABLED.getPropertyName());
        }
    }
}
