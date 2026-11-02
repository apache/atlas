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
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.lang3.StringUtils;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static org.apache.atlas.semantic.SemanticSearchServiceTest.filter;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class SemanticEntityFilterTest {
    private final AtlasTypeRegistry          typeRegistry = SemanticSearchServiceTest.typeRegistryWithTable();
    private       MockedStatic<AtlasGraphUtilsV2> graphUtils;

    @BeforeMethod
    public void setUp() {
        graphUtils = mockStatic(AtlasGraphUtilsV2.class);
    }

    @AfterMethod
    public void tearDown() {
        graphUtils.close();
    }

    @Test
    public void keywordNumberAndDateFiltersBecomeExactClauses() throws Exception {
        assertIndexClause("test_table", filter("qualifiedName", Operator.STARTS_WITH, "sales_db."), "{\"prefix\":{\"test_table•qualifiedName\":\"sales_db.\"}}");
        assertIndexClause(null, filter("Governance.domain", Operator.EQ, "finance"), "{\"term\":{\"Governance•domain\":\"finance\"}}");
        assertIndexClause(null, filter("__typeName", Operator.IN, "test_table, test_term,"), "{\"terms\":{\"__typeName.keyword\":[\"test_table\",\"test_term\"]}}");
        assertIndexClause("test_table", filter("createTime", Operator.GTE, "1700000000000"), "{\"range\":{\"test_table•createTime\":{\"gte\":\"1700000000000\"}}}");
        assertIndexClause("test_table", filter("createTime", Operator.TIME_RANGE, "1,2"), "{\"range\":{\"test_table•createTime\":{\"gte\":\"1\",\"lte\":\"2\"}}}");

        String lastWeek = clause("test_table", filter("createTime", Operator.TIME_RANGE, "LAST_7_DAYS"));
        assertTrue(lastWeek.matches("\\{\"range\":\\{\"test_table•createTime\":\\{\"(gte|lte)\":\"\\d+\",\"(gte|lte)\":\"\\d+\"}}}"), lastWeek);

        FilterCriteria domains = new FilterCriteria();
        domains.setCondition(FilterCriteria.Condition.OR);
        domains.setCriterion(Arrays.asList(filter("Governance.domain", Operator.IN, "sales,hr"), filter("Governance.domain", Operator.NEQ, "finance")));
        FilterCriteria criteria = new FilterCriteria();
        criteria.setCriterion(Arrays.asList(filter("qualifiedName", Operator.STARTS_WITH, "sales_db."), domains));

        assertIndexClause("test_table", criteria, "{\"bool\":{\"filter\":["
                + "{\"prefix\":{\"test_table•qualifiedName\":\"sales_db.\"}},"
                + "{\"bool\":{\"should\":["
                + "{\"terms\":{\"Governance•domain\":[\"sales\",\"hr\"]}},"
                + "{\"bool\":{\"must_not\":[{\"term\":{\"Governance•domain\":\"finance\"}}]}}],"
                + "\"minimum_should_match\":1}}]}}");
    }

    @Test
    public void analyzedTextFiltersMatchEveryExactValue() throws Exception {
        assertIndexClause(null, filter("Governance.dataOwner", Operator.EQ, "finance team"), "{\"match_phrase\":{\"Governance•dataOwner\":\"finance team\"}}");
        assertIndexClause(null, filter("__labels", Operator.EQ, "finance-deck"), "{\"match_phrase\":{\"__labels\":\"finance-deck\"}}");
        assertIndexClause(null, filter("__classificationNames", Operator.IN, "PII,Certified"), "{\"bool\":{\"should\":["
                + "{\"match_phrase\":{\"__classificationNames\":\"PII\"}},{\"match_phrase\":{\"__classificationNames\":\"Certified\"}}],\"minimum_should_match\":1}}");
        // "edw.gx_117@udf-retail" is indexed as the words edw.gx_117, udf, retail
        assertIndexClause(null, filter("Governance.dataOwner", Operator.STARTS_WITH, "EDW.gx_117@u"), "{\"prefix\":{\"Governance•dataOwner\":\"edw\"}}");
        assertIndexClause(null, filter("Governance.dataOwner", Operator.STARTS_WITH, "@edw"), "{\"match_all\":{}}");
        assertIndexClause(null, filter("Governance.dataOwner", Operator.STARTS_WITH, StringUtils.repeat('a', 300)),
                "{\"prefix\":{\"Governance•dataOwner\":\"" + StringUtils.repeat('a', 255) + "\"}}");
        // must_not of the phrase would also drop longer values containing it
        assertIndexClause(null, filter("__labels", Operator.NEQ, "finance-deck"), "{\"match_all\":{}}");
    }

    @Test
    public void rejectsFiltersSemanticSearchCannotApply() {
        assertRejected("test_table", filter("qualifiedName", Operator.CONTAINS, "x"), AtlasErrorCode.INVALID_PARAMETERS);
        assertRejected("test_table", filter("qualifiedName", Operator.IN, " , "), AtlasErrorCode.INVALID_PARAMETERS);
        assertRejected("test_table", filter("qualifiedName", Operator.GT, "5"), AtlasErrorCode.INVALID_PARAMETERS);
        assertRejected("test_table", filter("createTime", Operator.GT, "yesterday"), AtlasErrorCode.INVALID_PARAMETERS);
        assertRejected("test_table", filter("createTime", Operator.TIME_RANGE, "SOMETIME"), AtlasErrorCode.INVALID_PARAMETERS);
        assertRejected("test_table", filter("__labels", Operator.STARTS_WITH, "fin"), AtlasErrorCode.INVALID_PARAMETERS);
        assertRejected("test_table", filter("no_such_attribute", Operator.EQ, "x"), AtlasErrorCode.UNKNOWN_ATTRIBUTE);
        // without typeName, only system attributes and business metadata
        assertRejected(null, filter("qualifiedName", Operator.EQ, "x"), AtlasErrorCode.INVALID_PARAMETERS);
    }

    @Test
    public void matchesExactValuesOnTheVertex() throws Exception {
        AtlasVertex finance = vertex("Governance.dataOwner", "finance team");
        AtlasVertex emea    = vertex("Governance.dataOwner", "Finance Team - EMEA");
        AtlasVertex hr      = vertex("Governance.dataOwner", "hr team");
        property(hr, "Governance.__s_domain", "hr");

        SemanticEntityFilter equalsFilter = resolve(null, filter("Governance.dataOwner", Operator.EQ, "finance team"));
        assertTrue(equalsFilter.matches(finance));
        assertFalse(equalsFilter.matches(emea));

        SemanticEntityFilter notEquals = resolve(null, filter("Governance.dataOwner", Operator.NEQ, "finance team"));
        assertFalse(notEquals.matches(finance));
        assertTrue(notEquals.matches(emea));

        SemanticEntityFilter in = resolve(null, filter("Governance.dataOwner", Operator.IN, "hr team, finance team"));
        assertTrue(in.matches(finance));
        assertFalse(in.matches(emea));

        FilterCriteria prefixOrDomain = new FilterCriteria();
        prefixOrDomain.setCondition(FilterCriteria.Condition.OR);
        prefixOrDomain.setCriterion(Arrays.asList(filter("Governance.dataOwner", Operator.STARTS_WITH, "fin"), filter("Governance.domain", Operator.EQ, "hr")));
        SemanticEntityFilter or = resolve(null, prefixOrDomain);
        assertTrue(or.matches(finance));
        assertFalse(or.matches(emea));
        assertTrue(or.matches(hr));
    }

    @Test
    public void matchesOneNameOfClassificationsAndLabels() throws Exception {
        AtlasVertex labeled = vertex("__labels", "|finance|finance-deck|");

        assertTrue(resolve(null, filter("__labels", Operator.EQ, "finance-deck")).matches(labeled));
        assertFalse(resolve(null, filter("__labels", Operator.EQ, "deck")).matches(labeled));
        assertFalse(resolve(null, filter("__labels", Operator.NEQ, "finance")).matches(labeled));
        assertTrue(resolve(null, filter("__labels", Operator.IN, "hr,finance")).matches(labeled));
    }

    @Test
    public void matchesDatesOnTheVertex() throws Exception {
        AtlasVertex table = mock(AtlasVertex.class);
        graphUtils.when(() -> AtlasGraphUtilsV2.getProperty(table, "test_table.createTime", Long.class)).thenReturn(1500L);

        assertTrue(resolve("test_table", filter("createTime", Operator.GT, "1000")).matches(table));
        assertFalse(resolve("test_table", filter("createTime", Operator.TIME_RANGE, "1,1000")).matches(table));
        // a decimal never matches an integer value, as in OpenSearch
        assertFalse(resolve("test_table", filter("createTime", Operator.EQ, "1500.5")).matches(table));
    }

    private SemanticEntityFilter resolve(String typeName, FilterCriteria criteria) throws AtlasBaseException {
        return SemanticEntityFilter.resolve(typeRegistry, typeName, criteria);
    }

    private String clause(String typeName, FilterCriteria criteria) throws AtlasBaseException {
        return AtlasJson.toJson(resolve(typeName, criteria).getIndexClause());
    }

    private void assertIndexClause(String typeName, FilterCriteria criteria, String expectedJson) throws AtlasBaseException {
        assertEquals(AtlasJson.fromJson(clause(typeName, criteria), Map.class), AtlasJson.fromJson(expectedJson, Map.class));
    }

    private void assertRejected(String typeName, FilterCriteria criteria, AtlasErrorCode errorCode) {
        assertEquals(expectThrows(AtlasBaseException.class, () -> resolve(typeName, criteria)).getAtlasErrorCode(), errorCode);
    }

    private AtlasVertex vertex(String property, String value) {
        AtlasVertex ret = mock(AtlasVertex.class);
        property(ret, property, value);
        return ret;
    }

    private void property(AtlasVertex vertex, String name, String value) {
        graphUtils.when(() -> AtlasGraphUtilsV2.getProperty(vertex, name, String.class)).thenReturn(value);
    }
}
