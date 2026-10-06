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
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AtlasOpenSearchQueryBuilderTest {
    private static final String TYPE_DATASET = "test_dataset";

    private AtlasTypeRegistry typeRegistry;
    private Map<String, String> indexFieldNameCache;
    private Map<String, Integer> searchWeights;
    private Set<AtlasEntityType> entityTypes;

    @org.testng.annotations.AfterMethod
    public void tearDownKeywordFields() {
        AtlasOpenSearchIndexClient.clearKeywordSubfieldFieldsForTests();
    }

    @BeforeMethod
    public void setUp() throws org.apache.atlas.exception.AtlasBaseException {
        AtlasOpenSearchIndexClient.clearKeywordSubfieldFieldsForTests();

        typeRegistry = new AtlasTypeRegistry();

        AtlasEntityDef datasetDef = new AtlasEntityDef();
        datasetDef.setName(TYPE_DATASET);

        List<AtlasAttributeDef> attrs = new java.util.ArrayList<>();
        AtlasAttributeDef ownerAttr = new AtlasAttributeDef("owner", "string");
        ownerAttr.setIndexType(AtlasAttributeDef.IndexType.STRING);
        attrs.add(ownerAttr);
        AtlasAttributeDef departmentAttr = new AtlasAttributeDef("department", "string");
        departmentAttr.setIndexType(AtlasAttributeDef.IndexType.STRING);
        attrs.add(departmentAttr);
        // Defined on the type but not indexed in the mixed index (no indexFieldName) — mirrors hive_table.temporary.
        attrs.add(new AtlasAttributeDef("temporary", "boolean"));
        attrs.add(new AtlasAttributeDef("retentionPolicy", "string"));
        attrs.add(new AtlasAttributeDef("createTime", "date"));
        datasetDef.setAttributeDefs(attrs);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(datasetDef);
        typeRegistry.updateTypes(typesDef);

        AtlasEntityType datasetType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        datasetType.getAttribute("owner").setIndexFieldName("owner_index");
        datasetType.getAttribute("department").setIndexFieldName("department_index");
        datasetType.getAttribute("createTime").setIndexFieldName("createTime_index");
        datasetType.getAttribute(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY).setIndexFieldName("__customAttributes");

        indexFieldNameCache = new HashMap<>();
        indexFieldNameCache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, "__typeName");
        indexFieldNameCache.put(Constants.STATE_PROPERTY_KEY, "__state");
        indexFieldNameCache.put("owner", "owner_index");
        indexFieldNameCache.put("department", "department_index");

        searchWeights = new HashMap<>();
        searchWeights.put("name_index", 10);
        searchWeights.put("comment_index", 5);

        entityTypes = new HashSet<>();
        entityTypes.add(datasetType);

        // High-weight entity string attrs use text+keyword in OpenSearch; owner exercises that path in filter tests.
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("owner_index");
    }

    @Test
    public void plainTermUsesSingleMultiMatchWhenWeightsConfigured() throws AtlasBaseException {
        // Plain word, no trailing/embedded wildcard -- a single multi_match clause (OpenSearch's default type,
        // best_fields), not a per-field fan-out. best_fields ORs the query's terms per field and takes the
        // best-scoring field, matching Solr's default eDisMax behavior (no mm/pf/ps configured).
        Map<String, Object> query = builder("atlas").buildDiscoveryQuery();

        Map<String, Object> multiMatch = singleMultiMatchClause(query);

        assertEquals(multiMatch.get("query"), "atlas");
        assertFalse(multiMatch.containsKey("type"), "best_fields is the implicit default; no explicit type expected");

        List<String> fields = (List<String>) multiMatch.get("fields");
        assertNotNull(fields);
        assertTrue(fields.contains("all"), "catch-all 'all' field must always be included: " + fields);
        assertTrue(fields.contains("name_index^10"), "expected boost preserved verbatim: " + fields);
        assertTrue(fields.contains("comment_index^5"), "expected boost preserved verbatim: " + fields);
    }

    @Test
    public void trailingWildcardUsesSingleMultiMatchWithBoolPrefixType() throws AtlasBaseException {
        // "custo*" -- the exact shape EntityDiscoveryService.quickSearch() produces implicitly for any bare-word
        // quick search. Must use multi_match/bool_prefix (analyzed, case-correct "starts with" semantics), NOT
        // the raw "prefix" query (unanalyzed) and NOT match_phrase_prefix (stricter, adjacent-terms-only semantics
        // than Atlas's actual/Solr eDisMax OR-of-terms behavior).
        Map<String, Object> query = builder("custo*").buildDiscoveryQuery();

        Map<String, Object> multiMatch = singleMultiMatchClause(query);

        assertEquals(multiMatch.get("type"), "bool_prefix");
        assertEquals(multiMatch.get("query"), "custo", "the trailing '*' must be stripped before querying");

        List<String> fields = (List<String>) multiMatch.get("fields");
        assertTrue(fields.contains("all"));
        assertTrue(fields.contains("name_index^10"));
        assertTrue(fields.contains("comment_index^5"));
    }

    @Test
    public void trailingWildcardCaseIsDelegatedToOpenSearchAnalyzerNotAtlas() throws AtlasBaseException {
        // Regression test for Issue 11: quick search implicitly appends '*' to a bare word, so "ENTITYDESCRIPTION"
        // arrives here as "ENTITYDESCRIPTION*". Atlas must not attempt any client-side case normalization -- the
        // query text is passed through to multi_match/bool_prefix verbatim, and OpenSearch's own field analyzer
        // (the same analyzer used at index time) performs case-folding at query time.
        Map<String, Object> upperMultiMatch = singleMultiMatchClause(builder("ENTITYDESCRIPTION*").buildDiscoveryQuery());
        Map<String, Object> lowerMultiMatch = singleMultiMatchClause(builder("entitydescription*").buildDiscoveryQuery());

        assertEquals(upperMultiMatch.get("type"), "bool_prefix");
        assertEquals(lowerMultiMatch.get("type"), "bool_prefix");
        assertEquals(upperMultiMatch.get("query"), "ENTITYDESCRIPTION");
        assertEquals(lowerMultiMatch.get("query"), "entitydescription");
    }

    @Test
    public void multiWordQueryStaysAsSingleMultiMatchWithNoPhraseRequirement() throws AtlasBaseException {
        // Multi-word queries never receive the implicit trailing '*' (AtlasStructType.AtlasAttribute#hastokenizeChar
        // treats whitespace as a tokenize character), so "customer purchase" reaches the builder unmodified. It
        // must remain a single multi_match/best_fields clause -- no phrase/adjacency requirement is imposed, which
        // matches Solr's actual eDisMax configuration (no mm/pf/ps set) that ORs individual terms independent of
        // order.
        Map<String, Object> query = builder("customer purchase").buildDiscoveryQuery();
        Map<String, Object> multiMatch = singleMultiMatchClause(query);

        assertEquals(multiMatch.get("query"), "customer purchase");
        assertFalse(multiMatch.containsKey("type"));
    }

    @Test
    public void explicitMidStringWildcardUsesSingleQueryStringWithFullFieldList() throws AtlasBaseException {
        // "cus*tomer" -- a wildcard the USER typed explicitly (not the implicit trailing '*'). multi_match never
        // interprets '*'/'?' as wildcards (its query text is always literal analyzed text), so genuine
        // wildcard-pattern matching requires query_string -- but still as a SINGLE clause carrying the full
        // weighted fields list, not one query_string object per field (which is what caused the original
        // too_many_nested_clauses failure).
        Map<String, Object> query = builder("cus*tomer").buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        assertNotNull(must);
        assertEquals(must.size(), 1, "expected exactly one top-level free-text clause: " + must);
        assertTrue(must.get(0).containsKey("query_string"), "expected query_string for an explicit mid-string wildcard: " + must.get(0));

        Map<String, Object> queryString = (Map<String, Object>) must.get(0).get("query_string");
        assertEquals(queryString.get("query"), "cus*tomer");

        List<String> fields = (List<String>) queryString.get("fields");
        assertNotNull(fields, "query_string must carry the full weighted fields list, not per-field clauses");
        assertTrue(fields.contains("all"));
        assertTrue(fields.contains("name_index^10"));
        assertTrue(fields.contains("comment_index^5"));
    }

    @Test
    public void punctuationWithoutWildcardUsesPlainMultiMatchNotQueryString() throws AtlasBaseException {
        // "A:B" contains a Lucene metacharacter but no '*'/'?' -- since multi_match never parses query syntax, it
        // is safe to pass straight through as a plain multi_match (no escaping/metacharacter-detection needed at
        // all, unlike the old query_string-based fallback).
        Map<String, Object> query = builder("A:B").buildDiscoveryQuery();

        Map<String, Object> multiMatch = singleMultiMatchClause(query);

        assertEquals(multiMatch.get("query"), "A:B", "no escaping should be applied for a plain multi_match query");
        assertFalse(multiMatch.containsKey("type"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> singleMultiMatchClause(Map<String, Object> query) {
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        assertNotNull(must);
        assertEquals(must.size(), 1, "expected exactly one top-level free-text clause: " + must);
        assertTrue(must.get(0).containsKey("multi_match"), "expected multi_match, got: " + must.get(0));

        return (Map<String, Object>) must.get(0).get("multi_match");
    }

    @Test
    public void nonIndexedAttributeFilterIsSkippedWithoutException() throws AtlasBaseException {
        Map<String, Object> query = builder("atlas").withCriteria(leaf("temporary", Operator.EQ, "true")).buildDiscoveryQuery();

        assertNotNull(query.get("bool"));
        assertFalse(hasEntityCriteriaFilterClause(query),
                "non-indexed filter must not be pushed into OpenSearch filter clause");
    }

    @Test
    public void nonIndexedAttributeFilterGenericAttributeIsSkipped() throws AtlasBaseException {
        for (Operator op : new Operator[] {Operator.EQ, Operator.NEQ, Operator.CONTAINS, Operator.STARTS_WITH}) {
            Map<String, Object> query = builder("atlas")
                    .withCriteria(leaf("retentionPolicy", op, "archive"))
                    .buildDiscoveryQuery();

            assertFalse(hasEntityCriteriaFilterClause(query),
                    "non-indexed attribute must be skipped for operator " + op);
        }
    }

    @Test
    public void mixedIndexedAndNonIndexedAndFilterRetainsIndexedClauseOnly() throws AtlasBaseException {
        FilterCriteria and = new FilterCriteria();
        and.setCondition(FilterCriteria.Condition.AND);
        and.setCriterion(java.util.Arrays.asList(
                leaf("owner", Operator.EQ, "team-alpha"),
                leaf("temporary", Operator.EQ, "true")));

        Map<String, Object> query  = builder("atlas").withCriteria(and).buildDiscoveryQuery();
        Map<String, Object> clause = entityCriteriaFilterClause(query);

        assertNotNull(clause, "indexed part of AND filter must remain in OpenSearch query");

        Map<String, Object> criteriaBool = (Map<String, Object>) clause.get("bool");
        assertNotNull(criteriaBool, "compound AND criteria are wrapped in bool/must: " + clause);

        List<Map<String, Object>> must = (List<Map<String, Object>>) criteriaBool.get("must");
        assertEquals(must.size(), 1, "only the indexed leaf should be in the OpenSearch AND filter: " + must);

        Map<String, Object> term = (Map<String, Object>) must.get(0).get("term");
        assertNotNull(term);
        assertTrue(term.containsKey("owner_index") || term.containsKey("owner_index.keyword"),
                "expected owner term filter, got: " + term);
    }

    @Test
    public void orWithNonIndexedAttributeDefersEntireEntityFilter() throws AtlasBaseException {
        FilterCriteria or = new FilterCriteria();
        or.setCondition(FilterCriteria.Condition.OR);
        or.setCriterion(java.util.Arrays.asList(
                leaf("owner", Operator.EQ, "team-alpha"),
                leaf("temporary", Operator.EQ, "true")));

        Map<String, Object> query = builder("atlas").withCriteria(or).buildDiscoveryQuery();

        assertFalse(hasEntityCriteriaFilterClause(query),
                "OR mixing indexed and non-indexed attributes must not be partially pushed to OpenSearch");
    }

    @Test
    public void multipleNonIndexedAndFiltersProduceNoCriteriaClause() throws AtlasBaseException {
        FilterCriteria and = new FilterCriteria();
        and.setCondition(FilterCriteria.Condition.AND);
        and.setCriterion(java.util.Arrays.asList(
                leaf("temporary", Operator.EQ, "true"),
                leaf("retentionPolicy", Operator.EQ, "cold")));

        Map<String, Object> query = builder("atlas").withCriteria(and).buildDiscoveryQuery();

        assertFalse(hasEntityCriteriaFilterClause(query));
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void unknownAttributeFilterStillThrows() throws AtlasBaseException {
        builder("atlas").withCriteria(leaf("noSuchAttribute", Operator.EQ, "x")).buildDiscoveryQuery();
    }

    @Test
    public void timerangeFilterIsDeferredWithoutException() throws AtlasBaseException {
        Map<String, Object> query = builder("atlas").withCriteria(
                leaf("createTime", Operator.TIME_RANGE, "TODAY")).buildDiscoveryQuery();

        assertNotNull(query.get("bool"));
        assertFalse(hasEntityCriteriaFilterClause(query),
                "TIME_RANGE must not be pushed into OpenSearch; EntitySearchProcessor applies timerange");
    }

    @Test
    public void andOwnerAndTimerangePushesOwnerOnlyInOpenSearch() throws AtlasBaseException {
        FilterCriteria and = compound(FilterCriteria.Condition.AND,
                leaf("owner", Operator.EQ, "team-alpha"),
                leaf("createTime", Operator.TIME_RANGE, "TODAY"));

        Map<String, Object> query  = builder("atlas").withCriteria(and).buildDiscoveryQuery();
        Map<String, Object> clause = entityCriteriaFilterClause(query);

        assertNotNull(clause, "indexed AND sibling may still be pushed when TIME_RANGE is deferred");

        Map<String, Object> criteriaBool = (Map<String, Object>) clause.get("bool");
        List<Map<String, Object>> must   = (List<Map<String, Object>>) criteriaBool.get("must");
        assertEquals(must.size(), 1, "only owner should be in OpenSearch AND when TIME_RANGE is deferred: " + must);

        Map<String, Object> term = (Map<String, Object>) must.get(0).get("term");
        assertTrue(term.containsKey("owner_index") || term.containsKey("owner_index.keyword"), "expected owner term: " + term);
    }

    @Test
    public void orOwnerOrTimerangeDefersEntireEntityFilter() throws AtlasBaseException {
        FilterCriteria or = compound(FilterCriteria.Condition.OR,
                leaf("owner", Operator.EQ, "team-alpha"),
                leaf("createTime", Operator.TIME_RANGE, "TODAY"));

        Map<String, Object> query = builder("atlas").withCriteria(or).buildDiscoveryQuery();

        assertFalse(hasEntityCriteriaFilterClause(query),
                "OR with TIME_RANGE must not partially push the indexed arm alone");
    }

    @Test
    public void nestedIndexedAndNonIndexedOrIndexedSiblingDefersEntireEntityFilter() throws AtlasBaseException {
        // (owner=A AND temporary) OR department=D — non-indexed B under OR poisons pushdown for the whole OR.
        FilterCriteria criteria = compound(FilterCriteria.Condition.OR,
                compound(FilterCriteria.Condition.AND,
                        leaf("owner", Operator.EQ, "team-alpha"),
                        leaf("temporary", Operator.EQ, "true")),
                leaf("department", Operator.EQ, "finance"));

        Map<String, Object> query = builder("atlas").withCriteria(criteria).buildDiscoveryQuery();

        assertFalse(hasEntityCriteriaFilterClause(query),
                "must not push only department=C when (A AND nonIndexed) OR C is the full expression");
    }

    @Test
    public void nestedIndexedOrNonIndexedAndIndexedSiblingDefersEntireEntityFilter() throws AtlasBaseException {
        // (owner=A OR temporary) AND department=D — OR with non-indexed defers entire filter (SearchProcessor parity).
        FilterCriteria criteria = compound(FilterCriteria.Condition.AND,
                compound(FilterCriteria.Condition.OR,
                        leaf("owner", Operator.EQ, "team-alpha"),
                        leaf("temporary", Operator.EQ, "true")),
                leaf("department", Operator.EQ, "finance"));

        Map<String, Object> query = builder("atlas").withCriteria(criteria).buildDiscoveryQuery();

        assertFalse(hasEntityCriteriaFilterClause(query),
                "must not push department=D alone when (A OR nonIndexed) AND D is the full expression");
    }

    @Test
    public void indexedOrIndexedAndNonIndexedPushesOnlyIndexedOrBranch() throws AtlasBaseException {
        // (owner=A OR department=D) AND temporary — safe partial push: indexed OR only, non-indexed deferred.
        FilterCriteria criteria = compound(FilterCriteria.Condition.AND,
                compound(FilterCriteria.Condition.OR,
                        leaf("owner", Operator.EQ, "team-alpha"),
                        leaf("department", Operator.EQ, "finance")),
                leaf("temporary", Operator.EQ, "true"));

        Map<String, Object> query  = builder("atlas").withCriteria(criteria).buildDiscoveryQuery();
        Map<String, Object> clause = entityCriteriaFilterClause(query);

        assertNotNull(clause, "indexed OR branch may be pushed under AND with non-indexed sibling");

        Map<String, Object> criteriaBool = (Map<String, Object>) clause.get("bool");
        List<Map<String, Object>> must   = (List<Map<String, Object>>) criteriaBool.get("must");
        assertEquals(must.size(), 1, "non-indexed AND sibling must be omitted, not fail the query: " + must);

        Map<String, Object> orBool = (Map<String, Object>) must.get(0).get("bool");
        List<Map<String, Object>> should = (List<Map<String, Object>>) orBool.get("should");
        assertEquals(should.size(), 2, "OpenSearch should retain both indexed OR arms: " + should);
    }

    @Test
    public void deeplyNestedOrWithNonIndexedDefersDespiteIndexedAndElsewhere() throws AtlasBaseException {
        // ((owner=A AND temporary) OR department=D) AND retentionPolicy=R
        FilterCriteria criteria = compound(FilterCriteria.Condition.AND,
                compound(FilterCriteria.Condition.OR,
                        compound(FilterCriteria.Condition.AND,
                                leaf("owner", Operator.EQ, "team-alpha"),
                                leaf("temporary", Operator.EQ, "true")),
                        leaf("department", Operator.EQ, "finance")),
                leaf("retentionPolicy", Operator.EQ, "cold"));

        Map<String, Object> query = builder("atlas").withCriteria(criteria).buildDiscoveryQuery();

        assertFalse(hasEntityCriteriaFilterClause(query),
                "inner OR contains non-indexed under AND; must not partially push outer AND including department");
    }

    @Test
    public void entityFilterUsesFilterClause() throws AtlasBaseException {
        FilterCriteria filter = new FilterCriteria();
        filter.setAttributeName("owner");
        filter.setOperator(Operator.EQ);
        filter.setAttributeValue("team-alpha");

        Map<String, Object> query = builder("atlas").withCriteria(filter).buildDiscoveryQuery();
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");

        assertNotNull(bool.get("filter"));
        assertNotNull(bool.get("must_not"));
    }

    @Test
    public void excludeDeletedUsesMustNotOnState() throws AtlasBaseException {
        Map<String, Object> query = builder("atlas").buildDiscoveryQuery();
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> mustNot = (List<Map<String, Object>>) bool.get("must_not");

        assertNotNull(mustNot);
        assertFalse(mustNot.isEmpty());
    }

    @Test
    public void excludeDeletedUsesKeywordSubfieldWhenStateRegistered() throws AtlasBaseException {
        // __state mapped as text+keyword: exact-match term must target __state.keyword, otherwise the analyzed
        // (lower-cased) text field would not match the "DELETED" term and deleted entities would leak into results.
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("__state");

        Map<String, Object> query   = builder("atlas").buildDiscoveryQuery();
        Map<String, Object> bool    = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> mustNot = (List<Map<String, Object>>) bool.get("must_not");
        Map<String, Object> term    = (Map<String, Object>) mustNot.get(0).get("term");

        assertTrue(term.containsKey("__state.keyword"), "expected __state.keyword, got: " + term);
        assertEquals(term.get("__state.keyword"), "DELETED");
    }

    @Test
    public void excludeDeletedUsesBareFieldWhenStateNotRegistered() throws AtlasBaseException {
        // Native-keyword/string mapping (not registered): the bare field is correct; behavior must be unchanged.
        Map<String, Object> query   = builder("atlas").buildDiscoveryQuery();
        Map<String, Object> bool    = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> mustNot = (List<Map<String, Object>>) bool.get("must_not");
        Map<String, Object> term    = (Map<String, Object>) mustNot.get(0).get("term");

        assertTrue(term.containsKey("__state"), "expected bare __state, got: " + term);
        assertFalse(term.containsKey("__state.keyword"));
    }

    @Test
    public void entityTypeFilterUsesKeywordSubfieldWhenTypeNameRegistered() throws AtlasBaseException {
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("__typeName");

        Map<String, Object> query  = builder("atlas").buildDiscoveryQuery();
        Map<String, Object> bool   = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> filter = (List<Map<String, Object>>) bool.get("filter");
        Map<String, Object> terms  = (Map<String, Object>) filter.get(0).get("terms");

        assertTrue(terms.containsKey("__typeName.keyword"), "expected __typeName.keyword, got: " + terms);
    }

    @Test
    public void businessMetadataEqUsesMatchPhraseOnAnalyzedTextWithoutKeywordSubfield() throws AtlasBaseException {
        // Business Metadata string attrs are mixed-indexed as analyzed text without .keyword (IndexType.STRING).
        AtlasEntityType datasetType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        datasetType.getAttribute("department").setIndexFieldName("OSIssue1Metadata.criticality");

        FilterCriteria bmEq = leaf("department", Operator.EQ, "High");
        Map<String, Object> clause = leafClause(builder("atlas").withCriteria(bmEq).buildDiscoveryQuery());

        Map<String, Object> matchPhrase = (Map<String, Object>) clause.get("match_phrase");
        assertNotNull(matchPhrase, "BM EQ on analyzed text should use match_phrase: " + clause);
        assertTrue(matchPhrase.containsKey("OSIssue1Metadata\u2022criticality"), matchPhrase.toString());

        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) matchPhrase.get("OSIssue1Metadata\u2022criticality");
        assertEquals(params.get("query"), "High");
    }

    @Test
    public void eqAndNeqUseKeywordSubfieldWhileWildcardAndExistsUseAnalyzedField() throws AtlasBaseException {
        // owner_index mapped as text+keyword.
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("owner_index");

        // EQ -> term on owner_index.keyword
        FilterCriteria eq = leaf("owner", Operator.EQ, "team-alpha");
        Map<String, Object> eqClause = leafClause(builder("atlas").withCriteria(eq).buildDiscoveryQuery());
        Map<String, Object> term = (Map<String, Object>) eqClause.get("term");
        assertNotNull(term, "EQ should produce a term clause: " + eqClause);
        assertTrue(term.containsKey("owner_index.keyword"), "EQ must use keyword field, got: " + term);

        // NEQ -> must_not term on owner_index.keyword
        FilterCriteria neq = leaf("owner", Operator.NEQ, "team-alpha");
        Map<String, Object> neqClause = leafClause(builder("atlas").withCriteria(neq).buildDiscoveryQuery());
        Map<String, Object> neqBool = (Map<String, Object>) neqClause.get("bool");
        Map<String, Object> neqTerm = (Map<String, Object>) ((Map<String, Object>) neqBool.get("must_not")).get("term");
        assertTrue(neqTerm.containsKey("owner_index.keyword"), "NEQ must use keyword field, got: " + neqTerm);

        // STARTS_WITH -> wildcard on the analyzed field (NOT .keyword)
        FilterCriteria sw = leaf("owner", Operator.STARTS_WITH, "team");
        Map<String, Object> swClause = leafClause(builder("atlas").withCriteria(sw).buildDiscoveryQuery());
        Map<String, Object> wildcard = (Map<String, Object>) swClause.get("wildcard");
        assertNotNull(wildcard, "STARTS_WITH should produce a wildcard clause: " + swClause);
        assertTrue(wildcard.containsKey("owner_index"), "wildcard must use analyzed field, got: " + wildcard);
        assertFalse(wildcard.containsKey("owner_index.keyword"));

        // NOT_NULL -> exists on the analyzed field (NOT .keyword)
        FilterCriteria nn = leaf("owner", Operator.NOT_NULL, null);
        Map<String, Object> nnClause = leafClause(builder("atlas").withCriteria(nn).buildDiscoveryQuery());
        Map<String, Object> exists = (Map<String, Object>) nnClause.get("exists");
        assertNotNull(exists, "NOT_NULL should produce an exists clause: " + nnClause);
        assertEquals(exists.get("field"), "owner_index");
    }

    @Test
    public void containsStartsWithEndsWithUseCaseInsensitiveWildcardOnAnalyzedField() throws AtlasBaseException {
        // OpenSearch's default analyzer lowercases stored tokens (same normalization Solr's LowerCaseFilterFactory
        // applies to wildcard/prefix query text via MultiTermAwareComponent -- see AtlasSolrQueryBuilder#withContains
        // et al., which build the same +field:*value*-style wildcard Solr itself lowercases before matching).
        // Without case_insensitive, a mixed-case attributeValue would never match the lowercased token and
        // OpenSearch would drop the true candidate before EntitySearchProcessor's downstream case-sensitive
        // in-memory predicate (always chained after FreeTextSearchProcessor -- see SearchContext) gets to see it.
        FilterCriteria contains = leaf("owner", Operator.CONTAINS, "Team");
        Map<String, Object> containsClause = leafClause(builder("atlas").withCriteria(contains).buildDiscoveryQuery());
        assertWildcardCaseInsensitive(containsClause, "owner_index", "*Team*");

        FilterCriteria startsWith = leaf("owner", Operator.STARTS_WITH, "Team");
        Map<String, Object> startsWithClause = leafClause(builder("atlas").withCriteria(startsWith).buildDiscoveryQuery());
        assertWildcardCaseInsensitive(startsWithClause, "owner_index", "Team*");

        FilterCriteria endsWith = leaf("owner", Operator.ENDS_WITH, "Team");
        Map<String, Object> endsWithClause = leafClause(builder("atlas").withCriteria(endsWith).buildDiscoveryQuery());
        assertWildcardCaseInsensitive(endsWithClause, "owner_index", "*Team");
    }

    @Test
    public void startsWithAndEndsWithPatternsAreNotSwapped() throws AtlasBaseException {
        // Regression guard: STARTS_WITH must produce a SUFFIX wildcard ("value*") and ENDS_WITH a PREFIX wildcard
        // ("*value"). Live E2E against atlas-b confirmed these were previously swapped in this builder: querying
        // "owner startsWith 'bo'" against owner="bob" (with a free-text query present) returned zero results while
        // "owner startsWith 'ob'" incorrectly matched -- and the no-free-text EntitySearchProcessor-only path
        // (SearchPredicateUtil#getStartsWithPredicate / getEndsWithPredicate, unaffected by this builder) proved
        // "bo" is the correct STARTS_WITH match and "ob" the correct ENDS_WITH match for "bob".
        FilterCriteria startsWith = leaf("owner", Operator.STARTS_WITH, "bo");
        Map<String, Object> swWildcard = (Map<String, Object>) leafClause(
                builder("atlas").withCriteria(startsWith).buildDiscoveryQuery()).get("wildcard");
        Map<String, Object> swParams = (Map<String, Object>) swWildcard.get("owner_index");
        assertEquals(swParams.get("value"), "bo*", "STARTS_WITH must be a suffix wildcard (value*): " + swWildcard);

        FilterCriteria endsWith = leaf("owner", Operator.ENDS_WITH, "ob");
        Map<String, Object> ewWildcard = (Map<String, Object>) leafClause(
                builder("atlas").withCriteria(endsWith).buildDiscoveryQuery()).get("wildcard");
        Map<String, Object> ewParams = (Map<String, Object>) ewWildcard.get("owner_index");
        assertEquals(ewParams.get("value"), "*ob", "ENDS_WITH must be a prefix wildcard (*value): " + ewWildcard);
    }

    @SuppressWarnings("unchecked")
    private static void assertWildcardCaseInsensitive(Map<String, Object> clause, String expectedField, String expectedPattern) {
        Map<String, Object> wildcard = (Map<String, Object>) clause.get("wildcard");
        assertNotNull(wildcard, "expected a wildcard clause: " + clause);

        Map<String, Object> params = (Map<String, Object>) wildcard.get(expectedField);
        assertNotNull(params, "expected wildcard on '" + expectedField + "', got: " + wildcard);
        assertEquals(params.get("value"), expectedPattern);
        assertEquals(params.get("case_insensitive"), true, "inclusion wildcard must widen case-insensitively: " + wildcard);
    }

    @Test
    public void notContainsWildcardStaysCaseSensitiveToAvoidOverExclusion() throws AtlasBaseException {
        // NOT_CONTAINS is negated (must_not). Widening it case-insensitively would make must_not exclude entities
        // whose raw value differs only in case from attributeValue -- entities that Atlas's case-sensitive
        // NOT_CONTAINS contract (SearchPredicateUtil#getNotContainsPredicate) says should be INCLUDED -- and
        // EntitySearchProcessor's in-memory predicate can only narrow further, never restore a candidate that
        // OpenSearch's must_not already dropped. So, unlike CONTAINS/STARTS_WITH/ENDS_WITH, this must remain
        // case-sensitive (unchanged from the pre-existing behavior).
        FilterCriteria notContains = leaf("owner", Operator.NOT_CONTAINS, "Team");
        Map<String, Object> clause = leafClause(builder("atlas").withCriteria(notContains).buildDiscoveryQuery());

        Map<String, Object> bool = (Map<String, Object>) clause.get("bool");
        Map<String, Object> mustNot = (Map<String, Object>) bool.get("must_not");
        Map<String, Object> wildcard = (Map<String, Object>) mustNot.get("wildcard");

        assertNotNull(wildcard, "NOT_CONTAINS should produce a must_not/wildcard clause: " + clause);
        assertEquals(wildcard.get("owner_index"), "*Team*",
                "NOT_CONTAINS wildcard must stay a plain case-sensitive pattern (no case_insensitive widening), got: " + wildcard);
    }

    @Test
    public void notContainsOnAnalyzedOnlyFieldIsDeferredToAvoidFalsePositiveExclusion() throws AtlasBaseException {
        // Live E2E against atlas-b proved this false-positive-exclusion risk is real: with a free-text query,
        // "criticality not_contains 'high'" (lowercase) against a raw stored value of "High" wrongly returned
        // zero OpenSearch candidates (approximateCount=0) even though "High" does NOT case-sensitively contain
        // "high" -- a case-sensitive must_not/wildcard against the analyzed (lowercased) field coincidentally
        // matched, excluding the entity before EntitySearchProcessor's correct in-memory NOT_CONTAINS predicate
        // ever ran. Deferring this leaf (like TIME_RANGE) fixes it with zero risk, since EntitySearchProcessor
        // (unconditionally chained whenever typeName is present) still applies the full, correct filter.
        AtlasEntityType datasetType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        datasetType.getAttribute("department").setIndexFieldName("OSIssue1Metadata.criticality");

        // AND: leaf omitted, sibling (keyword-backed owner) still pushed.
        FilterCriteria and = compound(FilterCriteria.Condition.AND,
                leaf("owner", Operator.EQ, "team-alpha"),
                leaf("department", Operator.NOT_CONTAINS, "high"));

        Map<String, Object> andQuery  = builder("atlas").withCriteria(and).buildDiscoveryQuery();
        Map<String, Object> andClause = entityCriteriaFilterClause(andQuery);
        assertNotNull(andClause, "keyword-backed sibling must still be pushed when NOT_CONTAINS is deferred");

        Map<String, Object> andBool = (Map<String, Object>) andClause.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) andBool.get("must");
        assertEquals(must.size(), 1, "only owner should be in the OpenSearch AND filter: " + must);

        // OR: NOT_CONTAINS on an analyzed-only field must defer the WHOLE group (same hazard as TIME_RANGE/OR).
        FilterCriteria or = compound(FilterCriteria.Condition.OR,
                leaf("owner", Operator.EQ, "team-alpha"),
                leaf("department", Operator.NOT_CONTAINS, "high"));

        Map<String, Object> orQuery = builder("atlas").withCriteria(or).buildDiscoveryQuery();
        assertFalse(hasEntityCriteriaFilterClause(orQuery),
                "OR with NOT_CONTAINS on an analyzed-only field must not partially push the other arm alone");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void unsupportedOperatorThrowsForParityWithSolr() throws AtlasBaseException {
        // IN/LIKE/CONTAINS_ANY/CONTAINS_ALL are unsupported by AtlasSolrQueryBuilder too; OpenSearch must not invent.
        FilterCriteria in = leaf("owner", Operator.IN, "a,b");

        builder("atlas").withCriteria(in).buildDiscoveryQuery();
    }

    @Test
    public void classificationFilterUsesShouldOverBothClassificationFieldsWithKeywordSubfield() throws AtlasBaseException {
        indexFieldNameCache.put(Constants.CLASSIFICATION_NAMES_KEY, "__classificationNames");
        indexFieldNameCache.put(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, "__propagatedClassificationNames");

        // Common case: classification-name fields are registered as text+keyword (see
        // AtlasJanusGraphManagement#registerKeywordSubfieldField), so the exact-match "terms" filter must resolve
        // to the .keyword subfield, not the analyzed text field.
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("__classificationNames");
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("__propagatedClassificationNames");

        Set<String> classifications = new HashSet<>();
        classifications.add("PII");

        Map<String, Object> query = builder("atlas").withClassificationTypeNames(classifications).buildDiscoveryQuery();

        assertClassificationShouldClause(query, "__classificationNames.keyword", "__propagatedClassificationNames.keyword");
    }

    @Test
    public void classificationFilterFallsBackToBareFieldWhenNotKeywordRegistered() throws AtlasBaseException {
        indexFieldNameCache.put(Constants.CLASSIFICATION_NAMES_KEY, "__classificationNames");
        indexFieldNameCache.put(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, "__propagatedClassificationNames");

        // Deliberately do NOT register a .keyword subfield for these fields — exercises the "native"/legacy
        // fallback path (e.g. classification fields mapped without a keyword subfield), preserving prior behavior.
        Set<String> classifications = new HashSet<>();
        classifications.add("PII");

        Map<String, Object> query = builder("atlas").withClassificationTypeNames(classifications).buildDiscoveryQuery();

        assertClassificationShouldClause(query, "__classificationNames", "__propagatedClassificationNames");
    }

    @SuppressWarnings("unchecked")
    private static void assertClassificationShouldClause(Map<String, Object> query, String expectedClassField,
                                                           String expectedPropagatedField) {
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> filter = (List<Map<String, Object>>) bool.get("filter");

        Map<String, Object> classificationBool = filter.stream()
                .map(f -> (Map<String, Object>) f.get("bool"))
                .filter(b -> b != null && b.containsKey("should"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected a classification bool/should clause: " + filter));

        List<Map<String, Object>> shouldClauses = (List<Map<String, Object>>) classificationBool.get("should");
        assertEquals(shouldClauses.size(), 2,
                "classification filter must OR the direct and propagated classification fields: " + shouldClauses);

        Map<String, Object> classTerms      = (Map<String, Object>) shouldClauses.get(0).get("terms");
        Map<String, Object> propagatedTerms = (Map<String, Object>) shouldClauses.get(1).get("terms");

        assertNotNull(classTerms, "expected first should-clause to be a terms filter: " + shouldClauses);
        assertNotNull(propagatedTerms, "expected second should-clause to be a terms filter: " + shouldClauses);

        assertTrue(classTerms.containsKey(expectedClassField),
                "expected terms filter keyed on '" + expectedClassField + "' but got: " + classTerms.keySet());
        assertTrue(propagatedTerms.containsKey(expectedPropagatedField),
                "expected terms filter keyed on '" + expectedPropagatedField + "' but got: " + propagatedTerms.keySet());

        assertEquals(classTerms.get(expectedClassField), Collections.singletonList("PII"),
                "expected the classification value PII to be passed through to the terms filter");
        assertEquals(propagatedTerms.get(expectedPropagatedField), Collections.singletonList("PII"),
                "expected the classification value PII to be passed through to the propagated terms filter");
    }

    private static FilterCriteria leaf(String attr, Operator op, String value) {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName(attr);
        criteria.setOperator(op);
        criteria.setAttributeValue(value);
        return criteria;
    }

    private static FilterCriteria compound(FilterCriteria.Condition condition, FilterCriteria... children) {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setCondition(condition);
        criteria.setCriterion(java.util.Arrays.asList(children));
        return criteria;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> leafClause(Map<String, Object> query) {
        Map<String, Object> clause = entityCriteriaFilterClause(query);
        assertNotNull(clause, "expected an entity-criteria filter clause");
        return clause;
    }

    @SuppressWarnings("unchecked")
    private static boolean hasEntityCriteriaFilterClause(Map<String, Object> query) {
        return entityCriteriaFilterClause(query) != null;
    }

    /**
     * Entity-type restriction is always the first {@code filter} entry; user entity filters (when indexable) follow.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> entityCriteriaFilterClause(Map<String, Object> query) {
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        if (bool == null || bool.get("filter") == null) {
            return null;
        }

        List<Map<String, Object>> filter = (List<Map<String, Object>>) bool.get("filter");
        if (filter.size() <= 1) {
            return null;
        }

        return filter.get(filter.size() - 1);
    }

    @Test
    public void containsWildcardDetectsUserWildcards() {
        assertTrue(AtlasOpenSearchQueryBuilder.containsWildcard("custo*"));
        assertTrue(AtlasOpenSearchQueryBuilder.containsWildcard("atlas?"));
        assertFalse(AtlasOpenSearchQueryBuilder.containsWildcard("atlas"));
        assertFalse(AtlasOpenSearchQueryBuilder.containsWildcard("atlas\\*"));
    }

    /**
     * __customAttributes is indexed as analyzed JSON text in OpenSearch. Solr-style {@code term} filters with
     * {@code "\"key\":\"value\""} tokens do not match; quick search must use an analyzed-text clause instead.
     */
    @Test
    public void customAttributesContainsUsesMatchPhraseOnAnalyzedField() throws AtlasBaseException {
        FilterCriteria filter = leaf(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY, Operator.CONTAINS, "env=production");

        Map<String, Object> clause = leafClause(builder("filterdemo").withCriteria(filter).buildDiscoveryQuery());

        assertFalse(clause.containsKey("term"),
                "custom attributes filter must not use Solr-style term query: " + clause);

        Map<String, Object> matchPhrase = (Map<String, Object>) clause.get("match_phrase");
        assertNotNull(matchPhrase, "expected match_phrase on analyzed __customAttributes: " + clause);
        assertTrue(matchPhrase.containsKey("__customAttributes"), matchPhrase.toString());

        Map<String, Object> params = (Map<String, Object>) matchPhrase.get("__customAttributes");
        assertEquals(params.get("query"), "\"env\":\"production\"");
    }

    @Test
    public void customAttributesWithFreeTextQueryWithoutSearchWeightsStillUsesMultiMatchAndMatchPhrase()
            throws AtlasBaseException {
        // Even with an empty searchWeights map (e.g. before SolrIndexHelper has registered any indexable string
        // attribute), buildWeightedFieldList() always includes the catch-all "all" field, so the fields list is
        // never empty and the free-text clause is still a single multi_match/bool_prefix -- not a query_string
        // fallback.
        FilterCriteria filter = leaf(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY, Operator.CONTAINS, "env=production");

        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("filterdemo*")
                .withCriteria(filter)
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(Collections.emptyMap())
                .buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        assertNotNull(bool.get("must"));
        assertNotNull(bool.get("filter"));

        Map<String, Object> multiMatch = singleMultiMatchClause(query);
        assertEquals(multiMatch.get("type"), "bool_prefix");
        assertEquals((List<String>) multiMatch.get("fields"), Collections.singletonList("all"));

        Map<String, Object> customClause = leafClause(query);
        assertTrue(customClause.containsKey("match_phrase"), customClause.toString());
    }

    @Test
    public void customAttributesWithFreeTextQueryComposesMustMultiMatchAndFilter() throws AtlasBaseException {
        FilterCriteria filter = leaf(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY, Operator.CONTAINS, "env=production");

        Map<String, Object> query = builder("filterdemo").withCriteria(filter).buildDiscoveryQuery();
        Map<String, Object> bool  = (Map<String, Object>) query.get("bool");

        assertNotNull(bool.get("must"), "free-text query must stay in must clause: " + bool);
        assertNotNull(bool.get("filter"), "entity filter must stay in filter clause: " + bool);

        singleMultiMatchClause(query); // asserts exactly one multi_match must-clause

        Map<String, Object> customClause = leafClause(query);
        assertTrue(customClause.containsKey("match_phrase"),
                "custom attributes filter must survive alongside the free-text multi_match: " + customClause);
    }

    @Test
    public void customAttributesNotContainsUsesMustNotMatchPhrase() throws AtlasBaseException {
        FilterCriteria filter = leaf(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY, Operator.NOT_CONTAINS, "env=staging");

        Map<String, Object> clause = leafClause(builder("filterdemo").withCriteria(filter).buildDiscoveryQuery());
        Map<String, Object> bool   = (Map<String, Object>) clause.get("bool");
        assertNotNull(bool);

        Map<String, Object> mustNot     = (Map<String, Object>) bool.get("must_not");
        Map<String, Object> matchPhrase = (Map<String, Object>) mustNot.get("match_phrase");
        assertNotNull(matchPhrase);
        assertTrue(matchPhrase.containsKey("__customAttributes"));

        Map<String, Object> params = (Map<String, Object>) matchPhrase.get("__customAttributes");
        assertEquals(params.get("query"), "\"env\":\"staging\"");
    }

    // ------------------------------------------------------------------------------------------------------------
    // Issue 5 regression tests: searchWeights carries one entry per indexable string attribute SYSTEM-WIDE (see
    // SolrIndexHelper#geIndexFieldNamesWithSearchWeights), independent of the current typeName restriction, with
    // no upper bound. The previous implementation manually created one explicit OpenSearch query object per
    // weighted field and wrapped them in a client-side dis_max, which scaled past OpenSearch/Lucene's default
    // clause budget (too_many_nested_clauses; default maxClauseCount=1024) once the type system grew large enough.
    // The current implementation instead emits a SINGLE multi_match (or, for explicit wildcards, a single
    // query_string) clause whose "fields" parameter lists every weighted field; OpenSearch performs the per-field
    // fan-out internally. These tests assert there is no field-count cap and no truncation of any kind: EVERY
    // searchWeights entry, however many there are, must appear in the fields list, with its exact boost preserved.
    // ------------------------------------------------------------------------------------------------------------

    @Test
    public void allWeightedFieldsAreRetainedWithNoCapAtLargeScale() throws AtlasBaseException {
        // 3000 fields -- well beyond the old 900-field cap and the 1024-clause budget that a per-field fan-out
        // would have hit. A single multi_match with a 3000-entry "fields" array does not construct one query
        // object per field, so there is nothing here for OpenSearch's clause-count limit to reject.
        final int             fieldCount  = 3000;
        Map<String, Integer>  manyWeights = syntheticWeights(fieldCount, 3);

        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("customer")
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(manyWeights)
                .buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        // Exactly one top-level free-text clause regardless of field count -- no per-field fan-out at all.
        assertEquals(must.size(), 1, "expected a single free-text clause, not one per weighted field: " + must);

        Map<String, Object> multiMatch = singleMultiMatchClause(query);
        List<String>         fields    = (List<String>) multiMatch.get("fields");

        assertNotNull(fields);
        // +1 for the always-included catch-all "all" field.
        assertEquals(fields.size(), fieldCount + 1,
                "every one of the " + fieldCount + " weighted fields must be present -- no cap, no truncation");

        for (int i = 0; i < fieldCount; i++) {
            assertTrue(fields.contains(String.format("synthetic_field_%04d^3", i)),
                    "field " + i + " must be present with its boost preserved");
        }
    }

    @Test
    public void allWeightedFieldsAreRetainedWithNoCapAtLargeScaleWithTrailingWildcard() throws AtlasBaseException {
        // Same as above but through the bool_prefix (trailing-wildcard) branch -- this is the exact branch that
        // reproduced the original too_many_nested_clauses failure end-to-end, since EntityDiscoveryService
        // appends '*' to every bare-word quick search.
        final int             fieldCount  = 3000;
        Map<String, Integer>  manyWeights = syntheticWeights(fieldCount, 3);

        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("ENTITYDESCRIPTION*")
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(manyWeights)
                .buildDiscoveryQuery();

        Map<String, Object> multiMatch = singleMultiMatchClause(query);

        assertEquals(multiMatch.get("type"), "bool_prefix");

        List<String> fields = (List<String>) multiMatch.get("fields");
        assertEquals(fields.size(), fieldCount + 1, "no cap/truncation expected on the bool_prefix branch either");
    }

    @Test
    public void allWeightedFieldsAreRetainedWithNoCapAtLargeScaleWithExplicitWildcard() throws AtlasBaseException {
        // Same guarantee for the explicit-wildcard (query_string) branch: it must carry the FULL fields list as a
        // single clause, not one query_string object per field.
        final int             fieldCount  = 3000;
        Map<String, Integer>  manyWeights = syntheticWeights(fieldCount, 3);

        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("cus*tomer")
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(manyWeights)
                .buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        assertEquals(must.size(), 1, "expected a single free-text clause, not one per weighted field: " + must);
        assertTrue(must.get(0).containsKey("query_string"));

        Map<String, Object> queryString = (Map<String, Object>) must.get(0).get("query_string");
        List<String>        fields      = (List<String>) queryString.get("fields");

        assertEquals(fields.size(), fieldCount + 1, "no cap/truncation expected on the query_string branch either");
    }

    @Test
    public void lowWeightFieldBeyondOldCapBoundaryIsStillSearchable() throws AtlasBaseException {
        // Proves the new implementation did not simply MOVE the old 900-field cutoff: a field with a rank/index
        // position far beyond the old MAX_WEIGHTED_FIELD_CLAUSES=900 boundary must still be present in the query.
        Map<String, Integer> weights = syntheticWeights(1200, 1);
        weights.put("field_beyond_old_cap_boundary", 1);

        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("customer")
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(weights)
                .buildDiscoveryQuery();

        List<String> fields = (List<String>) singleMultiMatchClause(query).get("fields");

        assertTrue(fields.contains("field_beyond_old_cap_boundary^1"),
                "a low-weight field must not be dropped just because the total field count is large: " + fields.size());
    }

    @Test
    public void allBoostValuesArePreservedExactlyRegardlessOfMagnitude() throws AtlasBaseException {
        // No boost-tier grouping, no rounding, no truncation of low-boost entries -- every configured boost value
        // must be rendered verbatim in the "fields" list.
        Map<String, Integer> weights = new HashMap<>();
        weights.put("high_boost_index", 50);
        weights.put("mid_boost_index", 5);
        weights.put("low_boost_index", 1);

        Map<String, Object> query = new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString("customer")
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(weights)
                .buildDiscoveryQuery();

        List<String> fields = (List<String>) singleMultiMatchClause(query).get("fields");

        assertTrue(fields.contains("high_boost_index^50"));
        assertTrue(fields.contains("mid_boost_index^5"));
        assertTrue(fields.contains("low_boost_index^1"));
    }

    @Test
    public void normalSizedWeightMapIncludesEveryFieldPlusAllField() throws AtlasBaseException {
        // Existing/typical deployments (a handful to a few hundred weighted fields) must see every field
        // represented, with the catch-all "all" field always present too.
        Map<String, Object> query  = builder("atlas").buildDiscoveryQuery();
        List<String>         fields = (List<String>) singleMultiMatchClause(query).get("fields");

        assertEquals(fields.size(), searchWeights.size() + 1, "expected every weighted field plus 'all': " + fields);
    }

    private static Map<String, Integer> syntheticWeights(int count, int boost) {
        Map<String, Integer> weights = new HashMap<>();

        for (int i = 0; i < count; i++) {
            weights.put(String.format("synthetic_field_%04d", i), boost);
        }

        return weights;
    }

    private AtlasOpenSearchQueryBuilder builder(String queryString) {
        return new AtlasOpenSearchQueryBuilder()
                .withEntityTypes(entityTypes)
                .withQueryString(queryString)
                .withExcludedDeletedEntities(true)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNameCache)
                .withSearchWeights(searchWeights);
    }
}
