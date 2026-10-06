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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.QuickSearchContext;
import org.janusgraph.diskstorage.opensearch.AtlasOpenSearchIndex;
import org.janusgraph.diskstorage.opensearch.OpenSearchClient;
import org.janusgraph.diskstorage.opensearch.mapping.IndexMapping;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasOpenSearchIndexClientTest {
    @BeforeMethod
    public void setUp() {
        AtlasOpenSearchIndexClient.clearKeywordSubfieldFieldsForTests();
        AtlasOpenSearchIndexClient.applySuggestionFields(Collections.emptyList());
    }

    @AfterMethod
    public void tearDown() {
        AtlasOpenSearchIndexClient.clearKeywordSubfieldFieldsForTests();
        AtlasOpenSearchIndexClient.applySuggestionFields(Collections.emptyList());
    }

    @Test
    public void toTermsIncludePatternEscapesRegexMetacharacters() {
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust"), "cust.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust-"), "cust\\-.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust_"), "cust\\_.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust."), "cust\\..*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust+"), "cust\\+.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust*"), "cust\\*.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust?"), "cust\\?.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust("), "cust\\(.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust["), "cust\\[.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("cust\\"), "cust\\\\.*");
    }

    @Test
    public void toTermsIncludePatternPreservesCase() {
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("Customer"), "Customer.*");
        assertEquals(AtlasOpenSearchIndexClient.toTermsIncludePattern("CUST"), "CUST.*");
    }

    @Test
    public void resolveTermsAggregationFieldUsesKeywordSubfieldWhenRegistered() {
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("storm_node.description");

        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("storm_node.description"),
                "storm_node\u2022description.keyword");
    }

    @Test
    public void resolveTermsAggregationFieldUsesBaseFieldForStringMapping() {
        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("c55_asset\u2022__s_owner"),
                "c55_asset\u2022__s_owner");
    }

    @Test
    public void resolveTermsAggregationFieldUsesKeywordSubfieldWhenRegisteredForEntityType() {
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField(Constants.ENTITY_TYPE_PROPERTY_KEY);

        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY),
                Constants.ENTITY_TYPE_PROPERTY_KEY + ".keyword");
    }

    @Test
    public void resolveTermsAggregationFieldUsesNativeKeywordWhenSubfieldNotRegistered() {
        AtlasOpenSearchIndexClient.clearKeywordSubfieldFieldsForTests();

        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY),
                Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    @Test
    public void resolveTermsAggregationFieldSkipsFieldWithNoKeywordRegistrationOrNativeStringMarker() {
        // Neither registered in keywordSubfieldIndexFields, nor a legacy system field, nor an IndexType.STRING
        // ("__s_") field -- e.g. a TEXT attribute whose keyword subfield was never pushed to the physical
        // mapping (searchWeight/indexType raised by a type patch after the mixed index already existed).
        // Must be skipped, not assumed aggregation-safe.
        assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("Referenceable\u2022qualifiedName"));
    }

    @Test
    public void resolveAggregationCompatibleFieldsExcludesUnsafeFieldButKeepsSafeOnes() {
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("Asset\u2022description");

        List<String> candidateFields = Arrays.asList(
                "Asset\u2022__s_owner",             // native STRING keyword -- safe
                "Asset\u2022description",           // registered keyword subfield -- safe
                "Referenceable\u2022qualifiedName"); // neither signal -- unsafe, must be skipped

        List<String> compatible = AtlasOpenSearchIndexClient.resolveAggregationCompatibleFields(candidateFields);

        assertEquals(compatible, Arrays.asList("Asset\u2022__s_owner", "Asset\u2022description"));
    }

    @Test
    public void buildSuggestionsTermsAggsOmitsAggForUnsafeField() {
        Map<String, Object> aggs = AtlasOpenSearchIndexClient.buildSuggestionsTermsAggs(
                Arrays.asList("Asset\u2022__s_owner", "Referenceable\u2022qualifiedName"), "inv");

        // Only the native-keyword field produces an aggregation; the unsafe field is silently omitted.
        assertEquals(aggs.size(), 1);
    }

    @Test
    public void buildSuggestionsTermsAggsCreatesOneAggPerField() {
        List<String> fields = Arrays.asList("field_a", "field_b", "field_c");

        fields.forEach(AtlasOpenSearchIndexClient::registerKeywordSubfieldField);

        Map<String, Object> aggs = AtlasOpenSearchIndexClient.buildSuggestionsTermsAggs(fields, "cust");

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
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField(Constants.STATE_PROPERTY_KEY);

        Map<String, Object> query = AtlasOpenSearchIndexClient.buildSuggestionsFilterQuery();
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

        AtlasOpenSearchIndexClient.mergeTermBuckets(termsMap, nameBuckets);
        AtlasOpenSearchIndexClient.mergeTermBuckets(termsMap, ownerBuckets);

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
                AtlasOpenSearchIndexClient.collectTermsFromAggregations(
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

            // Both fields need an aggregation-safe representation to reach the backend: register one via the
            // explicit keyword-subfield path, use the native "__s_" STRING-index marker for the other.
            AtlasOpenSearchIndexClient.registerKeywordSubfieldField("owner_field");

            AtlasOpenSearchIndexClient.applySuggestionFields(
                    Arrays.asList("owner_field", "Asset\u2022__s_name"));

            List<String> result = AtlasOpenSearchIndexClient.getSuggestions("team", null, null);

            verify(mockClient, times(1)).search(any(), any(), eq(false));
            assertFalse(result.isEmpty());
        }
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void quickSearchPropagatesBackendFailureInsteadOfReturningEmpty() throws Exception {
        // An OpenSearch outage must surface as an exception, not be converted into a 0-result search.
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        when(mockClient.search(any(), any(), eq(false))).thenThrow(new java.io.IOException("OpenSearch unavailable"));

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            QuickSearchContext ctx = new QuickSearchContext("atlas", null, Collections.emptySet(),
                    Collections.emptySet(), new HashMap<>(), false, false, 0, 10);

            AtlasOpenSearchIndexClient.quickSearch(ctx, null);
        }
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void quickSearchPropagatesQueryBuildFailureInsteadOfReturningEmpty() throws Exception {
        // A query-builder failure (here: exclude-deleted requested but no __state index field mapped)
        // must propagate rather than silently becoming an empty result.
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            QuickSearchContext ctx = new QuickSearchContext("atlas", null, Collections.emptySet(),
                    Collections.emptySet(), new HashMap<>(), true, false, 0, 10);

            AtlasOpenSearchIndexClient.quickSearch(ctx, null);
        }
    }

    @Test
    public void quickSearchReturnsEmptyWhenBackendIsNotOpenSearch() throws Exception {
        // Documented contract: when the deployment is not OpenSearch-backed (null client), an empty result is
        // returned — this is NOT a backend failure and must not throw.
        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(null);

            QuickSearchContext ctx = new QuickSearchContext("atlas", null, Collections.emptySet(),
                    Collections.emptySet(), new HashMap<>(), false, false, 0, 10);

            assertTrue(AtlasOpenSearchIndexClient.quickSearch(ctx, null).getEntityGuids().isEmpty());
        }
    }

    @Test
    public void getSuggestionsReturnsEmptyOnBackendFailure() throws Exception {
        // Suggestions intentionally return empty on backend errors (parity with Solr), and must not throw.
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        when(mockClient.search(any(), any(), eq(false))).thenThrow(new java.io.IOException("OpenSearch unavailable"));

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            // Field must be aggregation-safe to actually reach the (failing) backend call being tested.
            AtlasOpenSearchIndexClient.registerKeywordSubfieldField("owner_field");
            AtlasOpenSearchIndexClient.applySuggestionFields(Arrays.asList("owner_field"));

            List<String> result = AtlasOpenSearchIndexClient.getSuggestions("team", null, null);

            verify(mockClient, times(1)).search(any(), any(), eq(false));
            assertTrue(result.isEmpty());
        }
    }

    // -------------------------------------------------------------------------------------------------------
    // setKeywordSubfieldIndexFields(): searchWeight-crosses-threshold-via-patch mapping reconciliation.
    // -------------------------------------------------------------------------------------------------------

    /**
     * Case 1 -- native STRING (IndexType.STRING) field: searchWeight rising past the threshold must NOT
     * result in a {@code .keyword} multi-field being added (the field is already a native keyword field),
     * must NOT enter the {@code keywordSubfieldIndexFields} registry at all (that registry specifically
     * means "has a .keyword multi-field"), no OpenSearch mapping call of any kind should happen, and the
     * field must still resolve correctly for suggestions/aggregation via the native-keyword path (bare
     * field name, existing mapping unchanged).
     */
    @Test
    public void setKeywordSubfieldIndexFieldsSkipsNativeStringFieldWithoutTouchingMapping() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            // searchWeight < 8: field not in the candidate set at all.
            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(Collections.emptySet(), null);
            assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield("Asset\u2022__s_owner"));

            // searchWeight >= 8, but this is a native STRING field -- must be excluded from the registry
            // entirely, with zero OpenSearch mapping calls, yet still resolve correctly (bare field, no
            // .keyword) since resolveTermsAggregationFieldName() recognizes native fields independently.
            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(
                    new LinkedHashSet<>(Arrays.asList("Asset\u2022__s_owner")), null);

            assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield("Asset\u2022__s_owner"));
            assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("Asset\u2022__s_owner"),
                    "Asset\u2022__s_owner");

            verify(mockClient, times(0)).getMapping(any(), any());
            verify(mockClient, times(0)).createMapping(any(), any(), any());
        }
    }

    /**
     * Case 2 -- plain TEXT string field (no keyword subfield yet): searchWeight rising past the threshold
     * must add a {@code .keyword} multi-field via {@link OpenSearchClient#createMapping}, preserving the
     * field's existing definition, and only THEN register the field as keyword-subfield-safe.
     */
    @Test
    public void setKeywordSubfieldIndexFieldsAddsKeywordToExistingTextFieldOnSearchWeightPatch() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        Map<String, Object> existingTextDefinition = new HashMap<>();
        existingTextDefinition.put("type", "text");
        existingTextDefinition.put("copy_to", Arrays.asList("all"));

        IndexMapping mapping = new IndexMapping();
        Map<String, Object> properties = new HashMap<>();
        properties.put("Referenceable\u2022qualifiedName", existingTextDefinition);
        mapping.setProperties(properties);

        when(mockClient.getMapping(any(), any())).thenReturn(mapping);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            // Before: searchWeight < 8 -- field is not suggestion/keyword-subfield eligible.
            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(Collections.emptySet(), null);
            assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield("Referenceable\u2022qualifiedName"));
            assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("Referenceable\u2022qualifiedName"));

            // Patch raises searchWeight >= 8.
            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(
                    new LinkedHashSet<>(Arrays.asList("Referenceable\u2022qualifiedName")), null);

            // Field remains TEXT (we never re-declare "type"), keyword subfield is added additively.
            @SuppressWarnings("unchecked")
            ArgumentCaptorHolder<Map<String, Object>> captured = captureCreateMappingPayload(mockClient);
            Map<String, Object> pushedProperties = (Map<String, Object>) captured.value.get("properties");
            Map<String, Object> pushedField       = (Map<String, Object>) pushedProperties.get("Referenceable\u2022qualifiedName");

            assertEquals(pushedField.get("type"), "text");
            assertEquals(pushedField.get("copy_to"), Arrays.asList("all"));
            Map<String, Object> pushedFields = (Map<String, Object>) pushedField.get("fields");
            assertTrue(pushedFields.containsKey("keyword"));

            // Registry now contains the field, and suggestion aggregation resolves to <field>.keyword.
            assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield("Referenceable\u2022qualifiedName"));
            assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("Referenceable\u2022qualifiedName"),
                    "Referenceable\u2022qualifiedName.keyword");
        }
    }

    /**
     * Case 3 -- field already has a {@code .keyword} multi-field (e.g. registered on an earlier patch, or
     * present since creation): re-running the reconciliation for a field NOT already in the in-memory
     * registry, but whose PHYSICAL mapping already has {@code .keyword}, must not re-create/duplicate the
     * mapping -- only {@link OpenSearchClient#createMapping} must be skipped -- and the registry must still
     * end up correct.
     */
    @Test
    public void setKeywordSubfieldIndexFieldsDoesNotRecreateMappingWhenKeywordAlreadyPresent() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        Map<String, Object> keywordSubfield = new HashMap<>();
        keywordSubfield.put("type", "keyword");

        Map<String, Object> fields = new HashMap<>();
        fields.put("keyword", keywordSubfield);

        Map<String, Object> existingTextWithKeyword = new HashMap<>();
        existingTextWithKeyword.put("type", "text");
        existingTextWithKeyword.put("fields", fields);

        IndexMapping mapping = new IndexMapping();
        Map<String, Object> properties = new HashMap<>();
        properties.put("hive_table\u2022comment", existingTextWithKeyword);
        mapping.setProperties(properties);

        when(mockClient.getMapping(any(), any())).thenReturn(mapping);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(
                    new LinkedHashSet<>(Arrays.asList("hive_table\u2022comment")), null);

            assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield("hive_table\u2022comment"));
            verify(mockClient, times(1)).getMapping(any(), any());
            verify(mockClient, times(0)).createMapping(any(), any(), any());
        }
    }

    /**
     * Case 4 -- mapping update failure (OpenSearch unreachable / rejects the PUT): the field must NOT be
     * added to {@code keywordSubfieldIndexFields}, leaving no inconsistent state where Atlas believes
     * {@code <field>.keyword} exists when it does not.
     */
    @Test
    public void setKeywordSubfieldIndexFieldsDoesNotRegisterFieldWhenMappingUpdateFails() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        Map<String, Object> existingTextDefinition = new HashMap<>();
        existingTextDefinition.put("type", "text");

        IndexMapping mapping = new IndexMapping();
        Map<String, Object> properties = new HashMap<>();
        properties.put("hive_table\u2022viewOriginalText", existingTextDefinition);
        mapping.setProperties(properties);

        when(mockClient.getMapping(any(), any())).thenReturn(mapping);
        Mockito.doThrow(new java.io.IOException("OpenSearch rejected the mapping update"))
                .when(mockClient).createMapping(any(), any(), any());

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(
                    new LinkedHashSet<>(Arrays.asList("hive_table\u2022viewOriginalText")), null);

            assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield("hive_table\u2022viewOriginalText"));
            assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("hive_table\u2022viewOriginalText"));
        }
    }

    /**
     * Case 5 -- field exists physically but is NOT a {@code text} field (e.g. mapped as something other
     * than what {@code needsKeywordSubfield()}'s TEXT-string assumption expects). Must not blindly add a
     * keyword subfield; field is excluded from the registry rather than guessed at.
     */
    @Test
    public void setKeywordSubfieldIndexFieldsRefusesToModifyNonTextField() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);

        Map<String, Object> unexpectedDefinition = new HashMap<>();
        unexpectedDefinition.put("type", "keyword"); // already keyword-typed at the top level, unexpectedly

        IndexMapping mapping = new IndexMapping();
        Map<String, Object> properties = new HashMap<>();
        properties.put("odd_field", unexpectedDefinition);
        mapping.setProperties(properties);

        when(mockClient.getMapping(any(), any())).thenReturn(mapping);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(
                    new LinkedHashSet<>(Arrays.asList("odd_field")), null);

            assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield("odd_field"));
            verify(mockClient, times(0)).createMapping(any(), any(), any());
        }
    }

    /**
     * Full regression scenario for the reported bug: an attribute created with searchWeight &lt; 8 (not
     * suggestion-eligible), later patched to searchWeight &gt;= 8. Verifies every step: not eligible before,
     * mapping updated on patch, registry updated, suggestion aggregation resolves to {@code <field>.keyword},
     * and the resolved field is usable end-to-end by {@code buildSuggestionsTermsAggs}.
     */
    @Test
    public void searchWeightPatchRegressionMakesFieldSuggestionEligibleEndToEnd() throws Exception {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);
        String            field     = "rdbms_table\u2022comment";

        Map<String, Object> existingTextDefinition = new HashMap<>();
        existingTextDefinition.put("type", "text");

        IndexMapping mapping = new IndexMapping();
        Map<String, Object> properties = new HashMap<>();
        properties.put(field, existingTextDefinition);
        mapping.setProperties(properties);

        when(mockClient.getMapping(any(), any())).thenReturn(mapping);

        try (MockedStatic<AtlasOpenSearchIndex> mockedIndex = Mockito.mockStatic(AtlasOpenSearchIndex.class)) {
            mockedIndex.when(AtlasOpenSearchIndex::getOpenSearchClient).thenReturn(mockClient);

            // 1 & 2: attribute created with searchWeight < 8 -- not in the keyword-subfield candidate set,
            // not suggestion eligible.
            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(Collections.emptySet(), null);
            AtlasOpenSearchIndexClient.applySuggestionFields(Collections.emptyList());

            assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield(field));
            assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName(field));

            // 3: typedef patch raises searchWeight to >= 8 -- SolrIndexHelper recomputes both sets.
            AtlasOpenSearchIndexClient.setKeywordSubfieldIndexFields(new LinkedHashSet<>(Arrays.asList(field)), null);
            AtlasOpenSearchIndexClient.applySuggestionFields(Arrays.asList(field));

            // 4: physical mapping now has the required .keyword (verify the PUT payload shape).
            @SuppressWarnings("unchecked")
            ArgumentCaptorHolder<Map<String, Object>> captured = captureCreateMappingPayload(mockClient);
            Map<String, Object> pushedProperties = (Map<String, Object>) captured.value.get("properties");
            Map<String, Object> pushedField       = (Map<String, Object>) pushedProperties.get(field);
            Map<String, Object> pushedFields      = (Map<String, Object>) pushedField.get("fields");

            assertTrue(pushedFields.containsKey("keyword"));

            // 5: registry contains the field.
            assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield(field));

            // 6: suggestions terms-agg resolves to <field>.keyword.
            Map<String, Object> aggs = AtlasOpenSearchIndexClient.buildSuggestionsTermsAggs(
                    Arrays.asList(field), "cust");

            assertEquals(aggs.size(), 1);

            // 7: the suggestions request-building path succeeds (no exception, no fields silently dropped).
            List<String> compatibleFields = AtlasOpenSearchIndexClient.resolveAggregationCompatibleFields(
                    Arrays.asList(field));

            assertEquals(compatibleFields, Arrays.asList(field));
        }
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptorHolder<Map<String, Object>> captureCreateMappingPayload(OpenSearchClient mockClient)
            throws Exception {
        org.mockito.ArgumentCaptor<Map<String, Object>> captor = org.mockito.ArgumentCaptor.forClass(Map.class);

        verify(mockClient, times(1)).createMapping(any(), any(), captor.capture());

        ArgumentCaptorHolder<Map<String, Object>> holder = new ArgumentCaptorHolder<>();
        holder.value = captor.getValue();

        return holder;
    }

    private static final class ArgumentCaptorHolder<T> {
        private T value;
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
