/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.QuickSearchContext;
import org.apache.atlas.repository.graphdb.QuickSearchResult;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.opensearch.AtlasOpenSearchIndex;
import org.janusgraph.diskstorage.opensearch.OpenSearchClient;
import org.janusgraph.diskstorage.opensearch.rest.RestSearchHit;
import org.janusgraph.diskstorage.opensearch.rest.RestSearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OpenSearch-native Atlas discovery helper for suggestions and aggregations. Uses
 * {@link OpenSearchClient#search} with terms aggregations and keeps suggestion-field configuration
 * in memory rather than reproducing Solr request-handler setup.
 */
public final class AtlasOpenSearchDiscoveryClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasOpenSearchDiscoveryClient.class);

    private static final String ATLAS_INDEX_NAME_CONF = "atlas.graph.index.search.index-name";

    private static final int SUGGESTION_BUCKET_MULTIPLE = 4;
    private static final int MIN_AGG_DOC_COUNT          = 1;
    private static final int MAX_AGG_BUCKET_SIZE        = 1000;

    /** OpenSearch {@code terms.include} regex metacharacters (Lucence regex dialect). */
    private static final String TERMS_INCLUDE_REGEX_METACHARACTERS = "\\.[]{}()*+-?^$|_";

    private static volatile Set<String> suggestionIndexFields = Collections.emptySet();
    private static volatile Map<String, Integer> searchWeightByIndexField = Collections.emptyMap();
    private static volatile Set<String> keywordSubfieldIndexFields = Collections.emptySet();

    private AtlasOpenSearchDiscoveryClient() {
    }

    public static void applySearchWeight(Map<String, Integer> indexFieldName2SearchWeightMap) {
        if (indexFieldName2SearchWeightMap == null || indexFieldName2SearchWeightMap.isEmpty()) {
            searchWeightByIndexField = Collections.emptyMap();
        } else {
            searchWeightByIndexField = Collections.unmodifiableMap(new HashMap<>(indexFieldName2SearchWeightMap));
        }

        LOG.info("Applied OpenSearch search weights (count={}).", searchWeightByIndexField.size());
    }

    static Map<String, Integer> getSearchWeightByIndexField() {
        return searchWeightByIndexField;
    }

    public static QuickSearchResult quickSearch(QuickSearchContext quickSearchContext, Configuration configuration) {
        OpenSearchClient client = AtlasOpenSearchIndex.getOpenSearchClient();

        if (client == null) {
            LOG.warn("The indexing system is not OpenSearch based. Will return empty quick-search results.");

            return new QuickSearchResult(Collections.emptyList(), 0L);
        }

        try {
            Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                    .withEntityTypes(quickSearchContext.getEntityTypes())
                    .withQueryString(quickSearchContext.getQueryString())
                    .withCriteria(quickSearchContext.getFilterCriteria())
                    .withExcludedDeletedEntities(quickSearchContext.isExcludeDeletedEntities())
                    .withIncludeSubTypes(quickSearchContext.isIncludeSubTypes())
                    .withCommonIndexFieldNames(quickSearchContext.getIndexFieldNameCache())
                    .withSearchWeights(searchWeightByIndexField)
                    .withClassificationTypeNames(quickSearchContext.getClassificationTypeNames())
                    .buildDiscoveryQuery();

            Map<String, Object> requestBody = new HashMap<>();

            requestBody.put("query", query);
            requestBody.put("from", quickSearchContext.getOffset());
            requestBody.put("size", quickSearchContext.getLimit());
            requestBody.put("track_total_hits", true);

            String physicalIndex = resolveVertexIndexName(configuration);

            LOG.debug("OpenSearch weighted quick-search: index={}, from={}, size={}", physicalIndex,
                    quickSearchContext.getOffset(), quickSearchContext.getLimit());

            org.janusgraph.diskstorage.opensearch.OpenSearchResponse osResponse =
                    client.search(physicalIndex, requestBody, false);

            if (!(osResponse instanceof RestSearchResponse)) {
                LOG.warn("OpenSearch quick-search response type {}", osResponse.getClass().getName());

                return new QuickSearchResult(Collections.emptyList(), 0L);
            }

            RestSearchResponse response = (RestSearchResponse) osResponse;
            long               total    = response.getTotalHitCount();
            List<String>       guids    = new ArrayList<>();

            if (response.getHits() != null && CollectionUtils.isNotEmpty(response.getHits().getHits())) {
                for (RestSearchHit hit : response.getHits().getHits()) {
                    String guid = resolveEntityGuid(hit);

                    if (StringUtils.isNotEmpty(guid)) {
                        guids.add(guid);
                    }
                }
            }

            return new QuickSearchResult(guids, total);
        } catch (AtlasBaseException | IOException e) {
            LOG.error("Error encountered in OpenSearch weighted quick search. Will return empty results.", e);
        }

        return new QuickSearchResult(Collections.emptyList(), 0L);
    }

    static String resolveEntityGuid(RestSearchHit hit) {
        if (hit == null) {
            return null;
        }

        Map<String, Object> source = hit.getSource();

        if (source != null) {
            Object guid = source.get("__guid");

            if (guid != null) {
                return String.valueOf(guid);
            }
        }

        return hit.getId();
    }

    public static void applySuggestionFields(List<String> suggestionProperties) {
        if (CollectionUtils.isEmpty(suggestionProperties)) {
            suggestionIndexFields = Collections.emptySet();
        } else {
            suggestionIndexFields = Collections.unmodifiableSet(new LinkedHashSet<>(suggestionProperties));
        }

        LOG.info("Applied OpenSearch suggestion fields (count={}).", suggestionIndexFields.size());
    }

    public static void registerKeywordSubfieldField(String indexFieldName) {
        if (StringUtils.isEmpty(indexFieldName)) {
            return;
        }

        Set<String> updated = new LinkedHashSet<>(keywordSubfieldIndexFields);

        updated.add(indexFieldName);
        keywordSubfieldIndexFields = Collections.unmodifiableSet(updated);

        LOG.debug("Registered OpenSearch keyword subfield for index field {}", indexFieldName);
    }

    public static void setKeywordSubfieldIndexFields(Set<String> indexFieldNames) {
        if (CollectionUtils.isEmpty(indexFieldNames)) {
            keywordSubfieldIndexFields = Collections.emptySet();
        } else {
            keywordSubfieldIndexFields = Collections.unmodifiableSet(new LinkedHashSet<>(indexFieldNames));
        }

        LOG.info("Applied OpenSearch keyword subfield fields (count={}).", keywordSubfieldIndexFields.size());
    }

    static Set<String> getKeywordSubfieldIndexFields() {
        return keywordSubfieldIndexFields;
    }

    static void clearKeywordSubfieldFieldsForTests() {
        keywordSubfieldIndexFields = Collections.emptySet();
    }

    static boolean usesKeywordSubfield(String indexFieldName) {
        return StringUtils.isNotEmpty(indexFieldName) && keywordSubfieldIndexFields.contains(indexFieldName);
    }

    static Set<String> getSuggestionIndexFields() {
        return suggestionIndexFields;
    }

    public static Map<String, List<AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext,
                                                                               Configuration configuration) {
        OpenSearchClient client = AtlasOpenSearchIndex.getOpenSearchClient();

        if (client == null) {
            LOG.warn("The indexing system is not OpenSearch based. Will return empty Aggregation metrics.");

            return Collections.emptyMap();
        }

        Set<String>         aggregationCommonFields = aggregationContext.getAggregationFieldNames();
        Set<AtlasAttribute> aggregationAttributes   = aggregationContext.getAggregationAttributes();
        Map<String, String> indexFieldNameCache     = aggregationContext.getIndexFieldNameCache();

        if (CollectionUtils.isEmpty(aggregationCommonFields) && CollectionUtils.isEmpty(aggregationAttributes)) {
            LOG.warn("There are no aggregation fields or attributes provided. Will return empty metrics.");

            return Collections.emptyMap();
        }

        try {
            Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                    .withEntityTypes(aggregationContext.getSearchForEntityTypes())
                    .withQueryString(aggregationContext.getQueryString())
                    .withCriteria(aggregationContext.getFilterCriteria())
                    .withExcludedDeletedEntities(aggregationContext.isExcludeDeletedEntities())
                    .withIncludeSubTypes(aggregationContext.isIncludeSubTypes())
                    .withCommonIndexFieldNames(indexFieldNameCache)
                    .withSearchWeights(searchWeightByIndexField)
                    .withClassificationTypeNames(aggregationContext.getClassificationTypeNames())
                    .buildDiscoveryQuery();

            Map<String, Object> aggs                     = new HashMap<>();
            Map<String, String> aggNameToPropertyKeyName = new HashMap<>();
            int                 aggIdx                   = 0;

            if (CollectionUtils.isNotEmpty(aggregationCommonFields)) {
                for (String propertyName : aggregationCommonFields) {
                    String indexFieldName = indexFieldNameCache.get(propertyName);

                    if (StringUtils.isEmpty(indexFieldName)) {
                        continue;
                    }

                    String aggName = "agg_" + aggIdx++;

                    aggNameToPropertyKeyName.put(aggName, propertyName);
                    aggs.put(aggName, termsAgg(indexFieldName));
                }
            }

            if (CollectionUtils.isNotEmpty(aggregationAttributes)) {
                for (AtlasAttribute attribute : aggregationAttributes) {
                    String indexFieldName = attribute.getIndexFieldName();

                    if (StringUtils.isEmpty(indexFieldName)) {
                        indexFieldName = attribute.getQualifiedName();
                    }

                    if (StringUtils.isEmpty(indexFieldName)) {
                        continue;
                    }

                    String aggName = "agg_" + aggIdx++;

                    aggNameToPropertyKeyName.put(aggName, attribute.getQualifiedName());
                    aggs.put(aggName, termsAgg(indexFieldName));
                }
            }

            if (aggs.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<String, Object> requestBody = new HashMap<>();

            requestBody.put("size", 0);
            requestBody.put("query", query);
            requestBody.put("aggs", aggs);

            String physicalIndex = resolveVertexIndexName(configuration);

            LOG.debug("OpenSearch aggregation query: index={}, aggs={}", physicalIndex, aggNameToPropertyKeyName.values());

            org.janusgraph.diskstorage.opensearch.OpenSearchResponse osResponse =
                    client.search(physicalIndex, requestBody, false);

            if (!(osResponse instanceof RestSearchResponse)) {
                LOG.warn("OpenSearch aggregation response type {}", osResponse.getClass().getName());

                return Collections.emptyMap();
            }

            RestSearchResponse response    = (RestSearchResponse) osResponse;
            Map<String, Object> aggResponse = response.getAggregations();

            if (aggResponse == null) {
                return Collections.emptyMap();
            }

            Map<String, List<AtlasAggregationEntry>> ret = new HashMap<>();

            for (Map.Entry<String, String> entry : aggNameToPropertyKeyName.entrySet()) {
                Map<String, Object>       aggResult = (Map<String, Object>) aggResponse.get(entry.getKey());
                List<Map<String, Object>> buckets   = aggResult == null ? null : (List<Map<String, Object>>) aggResult.get("buckets");
                List<AtlasAggregationEntry> entries = new ArrayList<>();

                if (CollectionUtils.isNotEmpty(buckets)) {
                    for (Map<String, Object> bucket : buckets) {
                        Object key      = bucket.get("key");
                        Object docCount = bucket.get("doc_count");

                        if (key != null) {
                            entries.add(new AtlasAggregationEntry(String.valueOf(key),
                                    docCount == null ? 0L : ((Number) docCount).longValue()));
                        }
                    }
                }

                ret.put(entry.getValue(), entries);
            }

            return ret;
        } catch (AtlasBaseException | IOException e) {
            LOG.error("Error encountered in getting the aggregation metrics. Will return empty aggregation.", e);
        }

        return Collections.emptyMap();
    }

    public static List<String> getSuggestions(String prefixString, String indexFieldName, Configuration configuration) {
        OpenSearchClient client = AtlasOpenSearchIndex.getOpenSearchClient();

        if (client == null) {
            LOG.warn("The indexing system is not OpenSearch based. Suggestions feature will not be available.");

            return Collections.emptyList();
        }

        List<String> fieldsToQuery = resolveFieldsToQuery(indexFieldName);

        if (fieldsToQuery.isEmpty()) {
            LOG.info("No index field name or suggestion fields configured for OpenSearch suggestions.");

            return Collections.emptyList();
        }

        List<String> aggregationFields = resolveAggregationCompatibleFields(fieldsToQuery);

        if (aggregationFields.isEmpty()) {
            LOG.info("No aggregation-compatible suggestion fields available for OpenSearch suggestions.");

            return Collections.emptyList();
        }

        try {
            Map<String, Object> aggs = buildSuggestionsTermsAggs(aggregationFields, prefixString);

            if (aggs.isEmpty()) {
                return Collections.emptyList();
            }

            Map<String, Object> requestBody = new HashMap<>();

            requestBody.put("size", 0);
            requestBody.put("query", buildSuggestionsFilterQuery());
            requestBody.put("aggs", aggs);

            String physicalIndex = resolveVertexIndexName(configuration);

            LOG.debug("OpenSearch suggestions query: index={}, prefix={}, fields={}, aggs={}",
                    physicalIndex, prefixString, aggregationFields, aggs.keySet());

            org.janusgraph.diskstorage.opensearch.OpenSearchResponse osResponse =
                    client.search(physicalIndex, requestBody, false);

            if (!(osResponse instanceof RestSearchResponse)) {
                LOG.warn("OpenSearch suggestions response type {}", osResponse.getClass().getName());

                return Collections.emptyList();
            }

            RestSearchResponse response = (RestSearchResponse) osResponse;
            Map<String, AtlasJanusGraphIndexClient.TermFreq> termsMap =
                    collectTermsFromAggregations(response.getAggregations(), aggs.keySet());

            return AtlasJanusGraphIndexClient.getTopTerms(termsMap);
        } catch (IOException e) {
            LOG.error("Error encountered in generating the suggestions. Ignoring the error", e);
        }

        return Collections.emptyList();
    }

    private static List<String> resolveFieldsToQuery(String indexFieldName) {
        if (StringUtils.isNotEmpty(indexFieldName)) {
            return Collections.singletonList(indexFieldName);
        }

        if (CollectionUtils.isNotEmpty(suggestionIndexFields)) {
            return new ArrayList<>(suggestionIndexFields);
        }

        return Collections.emptyList();
    }

    /**
     * Resolves Atlas index field names to OpenSearch {@code terms} aggregation targets.
     * TEXT fields without a registered {@code .keyword} subfield are skipped to avoid fielddata errors.
     */
    static List<String> resolveAggregationCompatibleFields(List<String> indexFieldNames) {
        List<String> ret = new ArrayList<>();

        for (String indexFieldName : indexFieldNames) {
            if (StringUtils.isEmpty(indexFieldName)) {
                continue;
            }

            if (resolveTermsAggregationFieldName(indexFieldName) != null) {
                ret.add(indexFieldName);
            } else {
                LOG.warn("Skipping OpenSearch suggestion field '{}' — not aggregation-compatible "
                        + "(TEXT field without keyword subfield).", indexFieldName);
            }
        }

        return ret;
    }

    /**
     * @return OpenSearch physical field for {@code terms} aggregation, or {@code null} if the field must be skipped.
     */
    static String resolveTermsAggregationFieldName(String indexFieldName) {
        if (StringUtils.isEmpty(indexFieldName)) {
            return null;
        }

        if (usesKeywordSubfield(indexFieldName)) {
            return toOpenSearchFieldName(indexFieldName) + ".keyword";
        }

        // Legacy JanusGraph mapping: system fields as native keyword (no .keyword subfield).
        if (Constants.STATE_PROPERTY_KEY.equals(indexFieldName)
                || Constants.ENTITY_TYPE_PROPERTY_KEY.equals(indexFieldName)
                || Constants.LABELS_PROPERTY_KEY.equals(indexFieldName)
                || Constants.CLASSIFICATION_TEXT_KEY.equals(indexFieldName)) {
            return toOpenSearchFieldName(indexFieldName);
        }

        if (isLikelyTextFieldWithoutKeywordSubfield(indexFieldName)) {
            return null;
        }

        return toOpenSearchFieldName(indexFieldName);
    }

    /**
     * Heuristic for misconfigured TEXT suggestion fields: high-weight entity attributes that are not STRING
     * mappings should always have a {@code .keyword} subfield registered. When missing, skip rather than
     * aggregating on a raw {@code text} field.
     */
    private static boolean isLikelyTextFieldWithoutKeywordSubfield(String indexFieldName) {
        if (usesKeywordSubfield(indexFieldName)) {
            return false;
        }

        if (Constants.STATE_PROPERTY_KEY.equals(indexFieldName)
                || Constants.ENTITY_TYPE_PROPERTY_KEY.equals(indexFieldName)
                || Constants.LABELS_PROPERTY_KEY.equals(indexFieldName)
                || Constants.CLASSIFICATION_TEXT_KEY.equals(indexFieldName)) {
            return keywordSubfieldIndexFields.isEmpty();
        }

        return false;
    }

    /**
     * JanusGraph mixed-index documents use {@code \u2022} as the type/attribute delimiter in OpenSearch field names.
     * Atlas callers may supply either the encoded or dot-separated property form.
     */
    static String toOpenSearchFieldName(String indexFieldName) {
        if (StringUtils.isEmpty(indexFieldName) || indexFieldName.indexOf('\u2022') >= 0) {
            return indexFieldName;
        }

        int dotIdx = indexFieldName.indexOf('.');

        if (dotIdx > 0) {
            return indexFieldName.substring(0, dotIdx) + '\u2022' + indexFieldName.substring(dotIdx + 1);
        }

        return indexFieldName;
    }

    static String toOpenSearchTermsFieldName(String indexFieldName) {
        String resolved = resolveTermsAggregationFieldName(indexFieldName);

        if (resolved != null) {
            return resolved;
        }

        return toOpenSearchFieldName(indexFieldName);
    }

    /**
     * Converts a user prefix to an OpenSearch {@code terms.include} regex (prefix match).
     * Case-sensitive — keyword fields preserve indexed term casing, matching Solr /terms behavior.
     */
    static String toTermsIncludePattern(String prefixString) {
        if (StringUtils.isEmpty(prefixString)) {
            return ".*";
        }

        StringBuilder sb = new StringBuilder(prefixString.length() + 2);

        for (int i = 0; i < prefixString.length(); i++) {
            char c = prefixString.charAt(i);

            if (TERMS_INCLUDE_REGEX_METACHARACTERS.indexOf(c) >= 0) {
                sb.append('\\');
            }

            sb.append(c);
        }

        sb.append(".*");

        return sb.toString();
    }

    static Map<String, Object> buildSuggestionsFilterQuery() {
        List<Map<String, Object>> mustNotClauses = new ArrayList<>();
        String stateTermsField = resolveTermsAggregationFieldName(Constants.STATE_PROPERTY_KEY);

        if (StringUtils.isEmpty(stateTermsField)) {
            stateTermsField = toOpenSearchFieldName(Constants.STATE_PROPERTY_KEY);
        }

        mustNotClauses.add(singleKeyMap("term",
                singleKeyMap(stateTermsField, AtlasEntity.Status.DELETED.name())));

        return singleKeyMap("bool", singleKeyMap("must_not", mustNotClauses));
    }

    static Map<String, Object> buildSuggestionsTermsAggs(List<String> indexFieldNames, String prefixString) {
        Map<String, Object> aggs = new HashMap<>();
        int                 idx  = 0;

        for (String indexFieldName : indexFieldNames) {
            String osField = resolveTermsAggregationFieldName(indexFieldName);

            if (StringUtils.isEmpty(osField)) {
                continue;
            }

            aggs.put("sugg_" + idx++, suggestionsTermsAgg(osField, prefixString));
        }

        return aggs;
    }

    private static Map<String, Object> suggestionsTermsAgg(String openSearchFieldName, String prefixString) {
        Map<String, Object> termsSpec = new HashMap<>();

        termsSpec.put("field", openSearchFieldName);
        termsSpec.put("size", AtlasJanusGraphIndexClient.DEFAULT_SUGGESTION_COUNT * SUGGESTION_BUCKET_MULTIPLE);
        termsSpec.put("min_doc_count", MIN_AGG_DOC_COUNT);

        if (StringUtils.isNotEmpty(prefixString)) {
            // Solr /terms uses prefix matching on indexed terms; terms.include expects a regex.
            termsSpec.put("include", toTermsIncludePattern(prefixString));
        }

        return Collections.singletonMap("terms", termsSpec);
    }

    static Map<String, AtlasJanusGraphIndexClient.TermFreq> collectTermsFromAggregations(Map<String, Object> aggregations,
                                                                                         Set<String> aggNames) {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> termsMap = new HashMap<>();

        if (aggregations == null || CollectionUtils.isEmpty(aggNames)) {
            return termsMap;
        }

        for (String aggName : aggNames) {
            Map<String, Object>       aggResult = (Map<String, Object>) aggregations.get(aggName);
            List<Map<String, Object>> buckets   = aggResult == null ? null : (List<Map<String, Object>>) aggResult.get("buckets");

            mergeTermBuckets(termsMap, buckets);
        }

        return termsMap;
    }

    static void mergeTermBuckets(Map<String, AtlasJanusGraphIndexClient.TermFreq> termsMap,
                                 List<Map<String, Object>> buckets) {
        if (CollectionUtils.isEmpty(buckets)) {
            return;
        }

        for (Map<String, Object> bucket : buckets) {
            Object key      = bucket.get("key");
            Object docCount = bucket.get("doc_count");

            if (key == null) {
                continue;
            }

            String term = String.valueOf(key);
            long   freq = docCount == null ? 0L : ((Number) docCount).longValue();
            AtlasJanusGraphIndexClient.TermFreq existing = termsMap.get(term);

            if (existing == null) {
                termsMap.put(term, new AtlasJanusGraphIndexClient.TermFreq(term, freq));
            } else {
                existing.addFreq(freq);
            }
        }
    }

    private static Map<String, Object> singleKeyMap(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(key, value);

        return ret;
    }

    static String resolveVertexIndexName(Configuration configuration) {
        String indexName = ApplicationProperties.DEFAULT_INDEX_NAME;

        if (configuration != null) {
            indexName = configuration.getString(ATLAS_INDEX_NAME_CONF,
                    configuration.getString(ApplicationProperties.OPENSEARCH_INDEX_NAME_CONF, ApplicationProperties.DEFAULT_INDEX_NAME));
        }

        return indexName + "_" + Constants.VERTEX_INDEX;
    }

    private static Map<String, Object> termsAgg(String indexFieldName) {
        Map<String, Object> terms = new HashMap<>();

        terms.put("field", toOpenSearchTermsFieldName(indexFieldName));
        terms.put("size", MAX_AGG_BUCKET_SIZE);
        terms.put("min_doc_count", MIN_AGG_DOC_COUNT);

        return Collections.singletonMap("terms", terms);
    }
}
