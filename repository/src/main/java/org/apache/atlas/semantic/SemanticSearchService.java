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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class SemanticSearchService {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticSearchService.class);

    private static final int SEARCH_OVERFETCH_FACTOR = 2;
    private static final int SEARCH_OVERFETCH_MAX      = 100;

    private final OpenSearchSemanticStore semanticStore;
    private final EntityGraphRetriever    entityRetriever;
    private final SemanticTextBuilder     textBuilder;
    private final AtlasTypeRegistry       typeRegistry;

    @Inject
    public SemanticSearchService(OpenSearchSemanticStore semanticStore,
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

        int topK      = resolveTopK(parameters.getTopK());
        double minScore = parameters.getMinScore() > 0 ? parameters.getMinScore() : SemanticSearchConfiguration.getMinScore();

        try {
            Set<String> typeNames = resolveTypeNames(parameters.getTypeName(), parameters.getIncludeSubTypes());
            VectorSearchFilter filter = new VectorSearchFilter(typeNames, Collections.emptySet());
            int fetchSize = overfetchSize(topK);
            LOG.debug("semanticSearch query='{}' topK={} minScore={} typeName={}",
                    parameters.getQuery(), topK, minScore, parameters.getTypeName());
            List<VectorSearchHit> hits = semanticStore.neuralSearch(parameters.getQuery(), fetchSize, filter);

            return buildSearchResult(parameters.getQuery(), hits, minScore, topK,
                    parameters.getAttributes(), parameters.getExcludeDeletedEntities());
        } catch (SemanticSearchException e) {
            LOG.error("Semantic search failed for query={}", parameters.getQuery(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    public AtlasSearchResult similarEntities(String guid, SimilarEntitySearchParameters parameters) throws AtlasBaseException {
        ensureEnabled();

        if (StringUtils.isBlank(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid");
        }

        int topK        = resolveTopK(parameters != null ? parameters.getTopK() : 0);
        double minScore = parameters != null && parameters.getMinScore() > 0 ? parameters.getMinScore() : SemanticSearchConfiguration.getMinScore();
        boolean excludeDeleted = parameters == null || parameters.getExcludeDeletedEntities();
        Set<String> attributes = parameters != null ? parameters.getAttributes() : null;
        String typeName = parameters != null ? parameters.getTypeName() : null;
        boolean includeSubTypes = parameters == null || parameters.getIncludeSubTypes();

        try {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(guid);
            if (vertex == null || AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            Set<String> typeNames = resolveTypeNames(typeName, includeSubTypes);
            VectorSearchFilter filter = new VectorSearchFilter(typeNames, Collections.singleton(guid));
            int fetchSize = overfetchSize(topK + 1);

            List<VectorSearchHit> hits = similarSearchHits(guid, vertex, fetchSize, filter);
            LOG.debug("similarEntities guid={} topK={} minScore={} typeName={}", guid, topK, minScore, typeName);

            return buildSearchResult("similar:" + guid, hits, minScore, topK, attributes, excludeDeleted);
        } catch (SemanticSearchException e) {
            LOG.error("Similar entity search failed for guid={}", guid, e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private AtlasSearchResult buildSearchResult(String queryText,
                                                List<VectorSearchHit> hits,
                                                double minScore,
                                                int topK,
                                                Set<String> attributes,
                                                boolean excludeDeletedEntities) throws AtlasBaseException {
        AtlasSearchResult result = new AtlasSearchResult(queryText, AtlasQueryType.SEMANTIC);
        result.setEntities(new ArrayList<>());

        Map<String, Double> scores = new LinkedHashMap<>();

        for (VectorSearchHit hit : hits) {
            if (hit.getScore() < minScore) {
                continue;
            }

            if (result.getEntities().size() >= topK) {
                break;
            }

            AtlasEntityHeader entityHeader = loadHeaderFromGraph(hit, attributes, excludeDeletedEntities);
            if (entityHeader == null) {
                continue;
            }

            result.getEntities().add(entityHeader);
            scores.put(hit.getGuid(), hit.getScore());
        }

        if (!scores.isEmpty()) {
            result.setSimilarityScores(scores);
        }

        result.setApproximateCount(result.getEntities().size());

        LOG.debug("semantic search completed queryText='{}' returned={} hits={}",
                queryText, result.getEntities().size(), hits.size());

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
        List<?> storedEmbedding = semanticStore.getStoredEmbeddingByGuid(guid);
        if (storedEmbedding != null && !storedEmbedding.isEmpty()) {
            LOG.debug("Similar search for guid={} using stored embedding", guid);
            return semanticStore.knnSearch(storedEmbedding, fetchSize, filter);
        }

        String sourceText = textBuilder.buildText(vertex);
        if (StringUtils.isBlank(sourceText)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    "Entity has no semantic index text: " + guid);
        }

        LOG.debug("Similar search for guid={} falling back to neural query (no stored embedding)", guid);
        return semanticStore.neuralSearch(sourceText, fetchSize, filter);
    }

    private int resolveTopK(int topK) {
        return topK > 0 ? topK : SemanticSearchConfiguration.getDefaultTopK();
    }

    private static int overfetchSize(int minimumHits) {
        return Math.min(Math.max(minimumHits * SEARCH_OVERFETCH_FACTOR, minimumHits), SEARCH_OVERFETCH_MAX);
    }

    private Set<String> resolveTypeNames(String typeName, boolean includeSubTypes) {
        if (StringUtils.isBlank(typeName)) {
            return Collections.emptySet();
        }

        Set<String> ret = new HashSet<>();
        ret.add(typeName);

        if (includeSubTypes) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
            if (entityType != null && CollectionUtils.isNotEmpty(entityType.getAllSubTypes())) {
                ret.addAll(entityType.getAllSubTypes());
            }
        }

        return ret;
    }

    private void ensureEnabled() throws AtlasBaseException {
        if (!SemanticSearchConfiguration.isSemanticSearchEnabled()) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Semantic search is disabled");
        }
    }
}
