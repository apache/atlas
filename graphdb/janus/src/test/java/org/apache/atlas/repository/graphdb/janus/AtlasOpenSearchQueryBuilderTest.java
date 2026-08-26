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
import org.apache.atlas.model.instance.AtlasEntity;
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

    @BeforeMethod
    public void setUp() throws org.apache.atlas.exception.AtlasBaseException {
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
    public void discoveryQueryAndWeightedQuickSearchAreEquivalent() throws AtlasBaseException {
        AtlasOpenSearchQueryBuilder builder = builder("atlas*");

        assertEquals(builder.buildDiscoveryQuery(), builder.buildWeightedQuickSearch());
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
