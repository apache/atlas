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
package org.apache.atlas.repository.graphdb.janus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Emits OpenSearch Query DSL (and related request fragments) for matrix documentation.
 * Output format: one JSON object per line with keys testId, atlasQuery, opensearchQuery, notes.
 */
public final class OpenSearchQueryCaptureDriver {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private OpenSearchQueryCaptureDriver() {
    }

    public static void main(String[] args) throws Exception {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasEntityDef datasetDef = new AtlasEntityDef();
        datasetDef.setName("test_dataset");
        AtlasAttributeDef ownerAttr = new AtlasAttributeDef("owner", "string");
        ownerAttr.setIndexType(AtlasAttributeDef.IndexType.STRING);
        datasetDef.setAttributeDefs(List.of(ownerAttr));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(datasetDef);
        typeRegistry.updateTypes(typesDef);
        AtlasEntityType datasetType = typeRegistry.getEntityTypeByName("test_dataset");
        datasetType.getAttribute("owner").setIndexFieldName("owner_index");

        Map<String, String> indexFieldNameCache = new HashMap<>();
        indexFieldNameCache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, "__typeName");
        indexFieldNameCache.put(Constants.STATE_PROPERTY_KEY, "__state");
        indexFieldNameCache.put("owner", "owner_index");
        indexFieldNameCache.put("name", "name_index");
        indexFieldNameCache.put("labels", "labels_index");

        Map<String, Integer> searchWeights = new LinkedHashMap<>();
        searchWeights.put("name_index", 10);
        searchWeights.put("owner_index", 3);
        searchWeights.put("labels_index", 10);

        Set<AtlasEntityType> entityTypes = new HashSet<>(Collections.singletonList(datasetType));

        captureQuickSearch("TC4-01", "atlas", indexFieldNameCache, searchWeights, entityTypes);
        captureQuickSearch("TC7-01", "customer*", indexFieldNameCache, searchWeights, entityTypes);
        captureQuickSearch("TC6-14", "custo*", indexFieldNameCache, searchWeights, entityTypes);
        captureQuickSearch("TC8-14", "custo*", indexFieldNameCache, searchWeights, entityTypes);
        captureQuickSearch("TC10-049", "A:B", indexFieldNameCache, searchWeights, entityTypes);
        captureQuickSearch("SC-013", "A:B", indexFieldNameCache, searchWeights, entityTypes);

        Map<String, Object> aggQuery = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("atlas")
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(searchWeights)
                .withExcludedDeletedEntities(true)
                .buildDiscoveryQuery();
        Map<String, Object> aggBody = new LinkedHashMap<>();
        aggBody.put("size", 0);
        aggBody.put("query", aggQuery);
        aggBody.put("aggs", Map.of("agg_0", Map.of("terms", Map.of("field", "__typeName", "size", 100))));
        emit("TC8-15", "atlas + owner=team-alpha filter", aggBody, "POST /search/quick aggregation on __typeName");

        Map<String, Object> suggestBody = new LinkedHashMap<>();
        suggestBody.put("query", AtlasOpenSearchDiscoveryClient.buildSuggestionsFilterQuery());
        suggestBody.put("aggs", AtlasOpenSearchDiscoveryClient.buildSuggestionsTermsAggs(
                List.of("owner_index", "name_index"), "cust"));
        emit("TC5-01", "cust", suggestBody, "POST /search/suggestions prefixString=cust");

        emitTermsPattern("TC9-05", "cust_", AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust_"));
    }

    private static void captureQuickSearch(String testId, String atlasQuery,
                                           Map<String, String> indexFieldNameCache,
                                           Map<String, Integer> searchWeights,
                                           Set<AtlasEntityType> entityTypes) throws Exception {
        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString(atlasQuery)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(searchWeights)
                .withExcludedDeletedEntities(true)
                .buildDiscoveryQuery();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", query);
        body.put("from", 0);
        body.put("size", 10);
        body.put("track_total_hits", true);

        emit(testId, atlasQuery, body, "GET /search/quick weighted quick-search");
    }

    private static void emitTermsPattern(String testId, String prefix, String pattern) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("testId", testId);
        row.put("atlasQuery", prefix);
        row.put("opensearchQuery", Map.of("terms.include", pattern));
        row.put("notes", "terms aggregation include regex for suggestions");
        System.out.println(MAPPER.writeValueAsString(row));
    }

    private static void emit(String testId, String atlasQuery, Object opensearchBody, String notes) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("testId", testId);
        row.put("atlasQuery", atlasQuery);
        row.put("opensearchQuery", opensearchBody);
        row.put("notes", notes);
        System.out.println(MAPPER.writeValueAsString(row));
    }
}
