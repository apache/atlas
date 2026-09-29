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
        datasetDef.setAttributeDefs(attrs);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(datasetDef);
        typeRegistry.updateTypes(typesDef);

        AtlasEntityType datasetType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        datasetType.getAttribute("owner").setIndexFieldName("owner_index");

        indexFieldNameCache = new HashMap<>();
        indexFieldNameCache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, "__typeName");
        indexFieldNameCache.put(Constants.STATE_PROPERTY_KEY, "__state");
        indexFieldNameCache.put("owner", "owner_index");

        searchWeights = new HashMap<>();
        searchWeights.put("name_index", 10);
        searchWeights.put("comment_index", 5);

        entityTypes = new HashSet<>();
        entityTypes.add(datasetType);
    }

    @Test
    public void plainTermUsesDisMaxWhenWeightsConfigured() throws AtlasBaseException {
        Map<String, Object> query = builder("atlas").buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        assertNotNull(must);
        assertTrue(must.get(0).containsKey("dis_max"));

        Map<String, Object> disMax = (Map<String, Object>) must.get(0).get("dis_max");
        List<Map<String, Object>> queries = (List<Map<String, Object>>) disMax.get("queries");

        assertNotNull(queries);
        assertFalse(queries.isEmpty());
        assertTrue(queries.get(0).containsKey("match"));
        assertTrue(queries.stream().anyMatch(q -> q.containsKey("match")
                && ((Map<?, ?>) q.get("match")).containsKey("all")));
    }

    @Test
    public void wildcardQueryUsesDisMaxWithQueryStringSubQueries() throws AtlasBaseException {
        Map<String, Object> query = builder("custo*").buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        assertNotNull(must);
        assertTrue(must.get(0).containsKey("dis_max"));

        Map<String, Object> disMax = (Map<String, Object>) must.get(0).get("dis_max");
        List<Map<String, Object>> queries = (List<Map<String, Object>>) disMax.get("queries");

        assertNotNull(queries);
        assertTrue(queries.get(0).containsKey("prefix"));

        Map<String, Object> prefix = (Map<String, Object>) queries.get(0).get("prefix");
        assertNotNull(prefix);
    }

    @Test
    public void colonQueryUsesQuotedQueryStringSubQueries() throws AtlasBaseException {
        Map<String, Object> query = builder("A:B").buildDiscoveryQuery();

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> must = (List<Map<String, Object>>) bool.get("must");

        assertNotNull(must);
        assertTrue(must.get(0).containsKey("dis_max"));

        Map<String, Object> disMax = (Map<String, Object>) must.get(0).get("dis_max");
        List<Map<String, Object>> queries = (List<Map<String, Object>>) disMax.get("queries");

        assertNotNull(queries);
        assertTrue(queries.get(0).containsKey("query_string"));

        Map<String, Object> queryString = (Map<String, Object>) queries.get(0).get("query_string");
        assertEquals(queryString.get("query"), "\"A\\:B\"");
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> leafClause(Map<String, Object> query) {
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> filter = (List<Map<String, Object>>) bool.get("filter");
        // The criteria clause is the last filter entry (after entity-type filter).
        return filter.get(filter.size() - 1);
    }

    @Test
    public void containsWildcardDetectsUserWildcards() {
        assertTrue(AtlasOpenSearchQueryBuilder.containsWildcard("custo*"));
        assertTrue(AtlasOpenSearchQueryBuilder.containsWildcard("atlas?"));
        assertFalse(AtlasOpenSearchQueryBuilder.containsWildcard("atlas"));
        assertFalse(AtlasOpenSearchQueryBuilder.containsWildcard("atlas\\*"));
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
