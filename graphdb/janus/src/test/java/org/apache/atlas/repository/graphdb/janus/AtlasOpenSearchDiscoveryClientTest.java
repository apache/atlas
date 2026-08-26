/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.janusgraph.diskstorage.opensearch.AtlasOpenSearchIndex;
import org.janusgraph.diskstorage.opensearch.OpenSearchClient;
import org.janusgraph.diskstorage.opensearch.rest.RestSearchResponse;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AtlasOpenSearchDiscoveryClientTest {

    @BeforeMethod
    public void setUp() {
        AtlasOpenSearchDiscoveryClient.clearKeywordSubfieldFieldsForTests();
        AtlasOpenSearchDiscoveryClient.applySuggestionFields(Collections.emptyList());
    }

    @AfterMethod
    public void tearDown() {
        AtlasOpenSearchDiscoveryClient.clearKeywordSubfieldFieldsForTests();
        AtlasOpenSearchDiscoveryClient.applySuggestionFields(Collections.emptyList());
    }

    @Test
    public void toTermsIncludePatternEscapesRegexMetacharacters() {
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust"), "cust.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust-"), "cust\\-.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust_"), "cust\\_.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust."), "cust\\..*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust+"), "cust\\+.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust*"), "cust\\*.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust?"), "cust\\?.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust("), "cust\\(.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust["), "cust\\[.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("cust\\"), "cust\\\\.*");
    }

    @Test
    public void toTermsIncludePatternPreservesCase() {
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("Customer"), "Customer.*");
        assertEquals(AtlasOpenSearchDiscoveryClient.toTermsIncludePattern("CUST"), "CUST.*");
    }

    @Test
    public void resolveTermsAggregationFieldUsesKeywordSubfieldWhenRegistered() {
        AtlasOpenSearchDiscoveryClient.registerKeywordSubfieldField("storm_node.description");

        assertEquals(AtlasOpenSearchDiscoveryClient.resolveTermsAggregationFieldName("storm_node.description"),
                "storm_node\u2022description.keyword");
    }

    @Test
    public void resolveTermsAggregationFieldUsesBaseFieldForStringMapping() {
        assertEquals(AtlasOpenSearchDiscoveryClient.resolveTermsAggregationFieldName("c55_asset\u2022__s_owner"),
                "c55_asset\u2022__s_owner");
    }

    @Test
    public void resolveTermsAggregationFieldUsesKeywordSubfieldWhenRegisteredForEntityType() {
        AtlasOpenSearchDiscoveryClient.registerKeywordSubfieldField(Constants.ENTITY_TYPE_PROPERTY_KEY);

        assertEquals(AtlasOpenSearchDiscoveryClient.resolveTermsAggregationFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY),
                Constants.ENTITY_TYPE_PROPERTY_KEY + ".keyword");
    }

    @Test
    public void resolveTermsAggregationFieldUsesNativeKeywordWhenSubfieldNotRegistered() {
        AtlasOpenSearchDiscoveryClient.clearKeywordSubfieldFieldsForTests();

        assertEquals(AtlasOpenSearchDiscoveryClient.resolveTermsAggregationFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY),
                Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    @Test
    public void buildSuggestionsTermsAggsCreatesOneAggPerField() {
        List<String> fields = Arrays.asList("field_a", "field_b", "field_c");
        Map<String, Object> aggs = AtlasOpenSearchDiscoveryClient.buildSuggestionsTermsAggs(fields, "cust");

        assertEquals(aggs.size(), 3);
        assertTrue(aggs.containsKey("sugg_0"));
        assertTrue(aggs.containsKey("sugg_1"));
        assertTrue(aggs.containsKey("sugg_2"));

        Map<String, Object> firstTerms = (Map<String, Object>) aggs.get("sugg_0");
        Map<String, Object> termsSpec  = (Map<String, Object>) firstTerms.get("terms");

        assertEquals(termsSpec.get("include"), "cust.*");
        assertEquals(termsSpec.get("size"), AtlasJanusGraphIndexClient.DEFAULT_SUGGESTION_COUNT * 4);
    }

    @Test
    public void buildSuggestionsFilterQueryExcludesDeletedEntities() {
        AtlasOpenSearchDiscoveryClient.registerKeywordSubfieldField(Constants.STATE_PROPERTY_KEY);

        Map<String, Object> query = AtlasOpenSearchDiscoveryClient.buildSuggestionsFilterQuery();
        Map<String, Object> bool  = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> mustNot = (List<Map<String, Object>>) bool.get("must_not");
        Map<String, Object> termClause = mustNot.get(0);
        Map<String, Object> term       = (Map<String, Object>) termClause.get("term");

        assertEquals(term.get("__state.keyword"), AtlasEntity.Status.DELETED.name());
    }

    @Test
    public void mergeTermBucketsDeduplicatesAndSumsFrequencies() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> termsMap = new HashMap<>();

        List<Map<String, Object>> nameBuckets = Arrays.asList(
                bucket("customer", 10L),
                bucket("customer_data", 7L));
        List<Map<String, Object>> ownerBuckets = Arrays.asList(
                bucket("customer", 5L),
                bucket("customer_team", 3L));

        AtlasOpenSearchDiscoveryClient.mergeTermBuckets(termsMap, nameBuckets);
        AtlasOpenSearchDiscoveryClient.mergeTermBuckets(termsMap, ownerBuckets);

        List<String> top = AtlasJanusGraphIndexClient.getTopTerms(termsMap);

        assertEquals(top.size(), 3);
        assertEquals(top.get(0), "customer");
        assertEquals(termsMap.get("customer").getFreq(), 15L);
        assertEquals(termsMap.get("customer_data").getFreq(), 7L);
        assertEquals(termsMap.get("customer_team").getFreq(), 3L);
    }

    @Test
    public void collectTermsFromAggregationsMergesAcrossNamedAggs() {
        Map<String, Object> aggregations = new HashMap<>();

        aggregations.put("sugg_0", aggResult(bucket("alpha", 3L), bucket("beta", 1L)));
        aggregations.put("sugg_1", aggResult(bucket("alpha", 2L), bucket("gamma", 4L)));

        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms =
                AtlasOpenSearchDiscoveryClient.collectTermsFromAggregations(
                        aggregations, new LinkedHashSet<>(Arrays.asList("sugg_0", "sugg_1")));

        assertEquals(terms.get("alpha").getFreq(), 5L);
        assertEquals(terms.get("gamma").getFreq(), 4L);
    }

    @Test
    public void getSuggestionsIssuesSingleOpenSearchRequestForMultipleFields() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);
        RestSearchResponse mockResponse = mock(RestSearchResponse.class);

        Map<String, Object> aggregations = new HashMap<>();
        aggregations.put("sugg_0", aggResult(bucket("team-alpha", 3L)));
        aggregations.put("sugg_1", aggResult(bucket("team-beta", 2L)));

        when(mockClient.search(any(), any(), eq(false))).thenReturn(mockResponse);
        when(mockResponse.getAggregations()).thenReturn(aggregations);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            AtlasOpenSearchDiscoveryClient.applySuggestionFields(
                    Arrays.asList("owner_field", "name_field"));

            List<String> result = AtlasOpenSearchDiscoveryClient.getSuggestions("team", null, null);

            verify(mockClient, times(1)).search(any(), any(), eq(false));
            assertFalse(result.isEmpty());
        }
    }

    @Test
    public void getTopTermsReturnsAtMostFiveSuggestions() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            terms.put("term-" + i, new AtlasJanusGraphIndexClient.TermFreq("term-" + i, 100 - i));
        }

        List<String> top = AtlasJanusGraphIndexClient.getTopTerms(terms);

        assertEquals(top.size(), AtlasJanusGraphIndexClient.DEFAULT_SUGGESTION_COUNT);
    }

    private static Map<String, Object> bucket(String key, long docCount) {
        Map<String, Object> bucket = new HashMap<>();

        bucket.put("key", key);
        bucket.put("doc_count", docCount);

        return bucket;
    }

    private static Map<String, Object> aggResult(Map<String, Object>... buckets) {
        Map<String, Object> agg = new HashMap<>();

        agg.put("buckets", Arrays.asList(buckets));

        return agg;
    }
}
