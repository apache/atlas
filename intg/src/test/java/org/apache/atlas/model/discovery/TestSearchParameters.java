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
package org.apache.atlas.model.discovery;

import org.apache.atlas.SortOrder;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestSearchParameters {
    private SearchParameters searchParameters;

    @BeforeMethod
    public void setUp() {
        searchParameters = new SearchParameters();
    }

    @Test
    public void testDefaultConstructor() {
        SearchParameters params = new SearchParameters();

        assertNull(params.getQuery());
        assertNull(params.getTypeName());
        assertNull(params.getClassification());
        assertNull(params.getRelationshipName());
        assertNull(params.getTermName());
        assertNull(params.getSortBy());
        assertFalse(params.getExcludeDeletedEntities());
        assertFalse(params.getIncludeClassificationAttributes());
        assertTrue(params.getIncludeSubTypes());
        assertTrue(params.getIncludeSubClassifications());
        assertFalse(params.getExcludeHeaderAttributes());
        assertEquals(params.getLimit(), 0);
        assertEquals(params.getOffset(), 0);
        assertNull(params.getMarker());
        assertNull(params.getEntityFilters());
        assertNull(params.getTagFilters());
        assertNull(params.getRelationshipFilters());
        assertNull(params.getAttributes());
        assertNull(params.getSortOrder());
    }

    @Test
    public void testConstants() {
        assertEquals(SearchParameters.WILDCARD_CLASSIFICATIONS, "*");
        assertEquals(SearchParameters.ALL_CLASSIFICATIONS, "_CLASSIFIED");
        assertEquals(SearchParameters.NO_CLASSIFICATIONS, "_NOT_CLASSIFIED");
        assertEquals(SearchParameters.ALL_ENTITY_TYPES, "_ALL_ENTITY_TYPES");
        assertEquals(SearchParameters.ALL_CLASSIFICATION_TYPES, "_ALL_CLASSIFICATION_TYPES");
    }

    @Test
    public void testQueryGetterSetter() {
        assertNull(searchParameters.getQuery());

        String query = "name:testEntity";
        searchParameters.setQuery(query);
        assertEquals(searchParameters.getQuery(), query);

        searchParameters.setQuery("");
        assertEquals(searchParameters.getQuery(), "");

        searchParameters.setQuery(null);
        assertNull(searchParameters.getQuery());
    }

    @Test
    public void testTypeNameGetterSetter() {
        assertNull(searchParameters.getTypeName());

        String typeName = "DataSet";
        searchParameters.setTypeName(typeName);
        assertEquals(searchParameters.getTypeName(), typeName);

        searchParameters.setTypeName("");
        assertEquals(searchParameters.getTypeName(), "");

        searchParameters.setTypeName(null);
        assertNull(searchParameters.getTypeName());
    }

    @Test
    public void testClassificationGetterSetter() {
        assertNull(searchParameters.getClassification());

        String classification = "PII";
        searchParameters.setClassification(classification);
        assertEquals(searchParameters.getClassification(), classification);

        searchParameters.setClassification("");
        assertEquals(searchParameters.getClassification(), "");

        searchParameters.setClassification(null);
        assertNull(searchParameters.getClassification());
    }

    @Test
    public void testRelationshipNameGetterSetter() {
        assertNull(searchParameters.getRelationshipName());

        String relationshipName = "owns";
        searchParameters.setRelationshipName(relationshipName);
        assertEquals(searchParameters.getRelationshipName(), relationshipName);

        searchParameters.setRelationshipName("");
        assertEquals(searchParameters.getRelationshipName(), "");

        searchParameters.setRelationshipName(null);
        assertNull(searchParameters.getRelationshipName());
    }

    @Test
    public void testTermNameGetterSetter() {
        assertNull(searchParameters.getTermName());

        String termName = "CustomerData";
        searchParameters.setTermName(termName);
        assertEquals(searchParameters.getTermName(), termName);

        searchParameters.setTermName("");
        assertEquals(searchParameters.getTermName(), "");

        searchParameters.setTermName(null);
        assertNull(searchParameters.getTermName());
    }

    @Test
    public void testExcludeDeletedEntitiesGetterSetter() {
        assertFalse(searchParameters.getExcludeDeletedEntities());

        searchParameters.setExcludeDeletedEntities(true);
        assertTrue(searchParameters.getExcludeDeletedEntities());

        searchParameters.setExcludeDeletedEntities(false);
        assertFalse(searchParameters.getExcludeDeletedEntities());
    }

    @Test
    public void testIncludeClassificationAttributesGetterSetter() {
        assertFalse(searchParameters.getIncludeClassificationAttributes());

        searchParameters.setIncludeClassificationAttributes(true);
        assertTrue(searchParameters.getIncludeClassificationAttributes());

        searchParameters.setIncludeClassificationAttributes(false);
        assertFalse(searchParameters.getIncludeClassificationAttributes());
    }

    @Test
    public void testIncludeSubTypesGetterSetter() {
        assertTrue(searchParameters.getIncludeSubTypes());

        searchParameters.setIncludeSubTypes(false);
        assertFalse(searchParameters.getIncludeSubTypes());

        searchParameters.setIncludeSubTypes(true);
        assertTrue(searchParameters.getIncludeSubTypes());
    }

    @Test
    public void testIncludeSubClassificationsGetterSetter() {
        assertTrue(searchParameters.getIncludeSubClassifications());

        searchParameters.setIncludeSubClassifications(false);
        assertFalse(searchParameters.getIncludeSubClassifications());

        searchParameters.setIncludeSubClassifications(true);
        assertTrue(searchParameters.getIncludeSubClassifications());
    }

    @Test
    public void testExcludeHeaderAttributesGetterSetter() {
        assertFalse(searchParameters.getExcludeHeaderAttributes());

        searchParameters.setExcludeHeaderAttributes(true);
        assertTrue(searchParameters.getExcludeHeaderAttributes());

        searchParameters.setExcludeHeaderAttributes(false);
        assertFalse(searchParameters.getExcludeHeaderAttributes());
    }

    @Test
    public void testLimitGetterSetter() {
        assertEquals(searchParameters.getLimit(), 0);

        searchParameters.setLimit(100);
        assertEquals(searchParameters.getLimit(), 100);

        searchParameters.setLimit(-1);
        assertEquals(searchParameters.getLimit(), -1);

        searchParameters.setLimit(Integer.MAX_VALUE);
        assertEquals(searchParameters.getLimit(), Integer.MAX_VALUE);
    }

    @Test
    public void testOffsetGetterSetter() {
        assertEquals(searchParameters.getOffset(), 0);

        searchParameters.setOffset(50);
        assertEquals(searchParameters.getOffset(), 50);

        searchParameters.setOffset(-1);
        assertEquals(searchParameters.getOffset(), -1);

        searchParameters.setOffset(Integer.MAX_VALUE);
        assertEquals(searchParameters.getOffset(), Integer.MAX_VALUE);
    }

    @Test
    public void testMarkerGetterSetter() {
        assertNull(searchParameters.getMarker());

        String marker = "next-page-marker";
        searchParameters.setMarker(marker);
        assertEquals(searchParameters.getMarker(), marker);

        searchParameters.setMarker("");
        assertEquals(searchParameters.getMarker(), "");

        searchParameters.setMarker(null);
        assertNull(searchParameters.getMarker());
    }

    @Test
    public void testEntityFiltersGetterSetter() {
        assertNull(searchParameters.getEntityFilters());

        FilterCriteria filters = new FilterCriteria();
        searchParameters.setEntityFilters(filters);
        assertSame(searchParameters.getEntityFilters(), filters);

        searchParameters.setEntityFilters(null);
        assertNull(searchParameters.getEntityFilters());
    }

    @Test
    public void testTagFiltersGetterSetter() {
        assertNull(searchParameters.getTagFilters());

        FilterCriteria filters = new FilterCriteria();
        searchParameters.setTagFilters(filters);
        assertSame(searchParameters.getTagFilters(), filters);

        searchParameters.setTagFilters(null);
        assertNull(searchParameters.getTagFilters());
    }

    @Test
    public void testRelationshipFiltersGetterSetter() {
        assertNull(searchParameters.getRelationshipFilters());

        FilterCriteria filters = new FilterCriteria();
        searchParameters.setRelationshipFilters(filters);
        assertSame(searchParameters.getRelationshipFilters(), filters);

        searchParameters.setRelationshipFilters(null);
        assertNull(searchParameters.getRelationshipFilters());
    }

    @Test
    public void testAttributesGetterSetter() {
        assertNull(searchParameters.getAttributes());

        Set<String> attributes = new HashSet<>();
        attributes.add("name");
        attributes.add("description");
        searchParameters.setAttributes(attributes);
        assertSame(searchParameters.getAttributes(), attributes);

        searchParameters.setAttributes(null);
        assertNull(searchParameters.getAttributes());
    }

    @Test
    public void testSortByGetterSetter() {
        assertNull(searchParameters.getSortBy());

        String sortBy = "createTime";
        searchParameters.setSortBy(sortBy);
        assertEquals(searchParameters.getSortBy(), sortBy);

        searchParameters.setSortBy("");
        assertEquals(searchParameters.getSortBy(), "");

        searchParameters.setSortBy(null);
        assertNull(searchParameters.getSortBy());
    }

    @Test
    public void testSortOrderGetterSetter() {
        assertNull(searchParameters.getSortOrder());

        searchParameters.setSortOrder(SortOrder.ASCENDING);
        assertEquals(searchParameters.getSortOrder(), SortOrder.ASCENDING);

        searchParameters.setSortOrder(SortOrder.DESCENDING);
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);

        searchParameters.setSortOrder(null);
        assertNull(searchParameters.getSortOrder());
    }

    @Test
    public void testHashCodeConsistency() {
        int hashCode1 = searchParameters.hashCode();
        int hashCode2 = searchParameters.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        SearchParameters params1 = new SearchParameters();
        SearchParameters params2 = new SearchParameters();

        params1.setQuery("test");
        params1.setTypeName("DataSet");
        params1.setLimit(100);

        params2.setQuery("test");
        params2.setTypeName("DataSet");
        params2.setLimit(100);

        assertEquals(params1.hashCode(), params2.hashCode());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(searchParameters.equals(searchParameters));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(searchParameters.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(searchParameters.equals("not a search parameters"));
    }

    @Test
    public void testEqualsWithSameValues() {
        SearchParameters params1 = new SearchParameters();
        SearchParameters params2 = new SearchParameters();

        params1.setQuery("test");
        params1.setTypeName("DataSet");
        params1.setLimit(100);

        params2.setQuery("test");
        params2.setTypeName("DataSet");
        params2.setLimit(100);

        assertTrue(params1.equals(params2));
        assertTrue(params2.equals(params1));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        SearchParameters params1 = new SearchParameters();
        SearchParameters params2 = new SearchParameters();

        params1.setQuery("test1");
        params2.setQuery("test2");

        assertFalse(params1.equals(params2));
        assertFalse(params2.equals(params1));
    }

    @Test
    public void testToString() {
        searchParameters.setQuery("test query");
        searchParameters.setTypeName("DataSet");
        searchParameters.setLimit(50);

        String result = searchParameters.toString();

        assertNotNull(result);
        assertTrue(result.contains("test query"));
        assertTrue(result.contains("DataSet"));
        assertTrue(result.contains("50"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        searchParameters.setQuery("test");
        searchParameters.setTypeName("Table");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = searchParameters.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("test"));
        assertTrue(result.toString().contains("Table"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        searchParameters.setQuery("test");

        StringBuilder result = searchParameters.toString(null);

        assertNotNull(result);
        assertTrue(result.toString().contains("test"));
    }

    @Test
    public void testSerializable() {
        assertNotNull(searchParameters);
    }

    @Test
    public void testSerialVersionUID() throws Exception {
        java.lang.reflect.Field serialVersionUIDField = SearchParameters.class.getDeclaredField("serialVersionUID");
        serialVersionUIDField.setAccessible(true);
        long serialVersionUID = (Long) serialVersionUIDField.get(null);

        assertEquals(serialVersionUID, 1L);
    }

    @Test
    public void testOperatorValues() {
        Operator[] operators = Operator.values();
        assertTrue(operators.length > 0);

        boolean foundEQ = false;
        boolean foundLIKE = false;
        boolean foundIN = false;

        for (Operator op : operators) {
            if (op == Operator.EQ) {
                foundEQ = true;
            } else if (op == Operator.LIKE) {
                foundLIKE = true;
            } else if (op == Operator.IN) {
                foundIN = true;
            }
        }

        assertTrue(foundEQ);
        assertTrue(foundLIKE);
        assertTrue(foundIN);
    }

    @Test
    public void testOperatorFromString() {
        assertEquals(Operator.fromString("="), Operator.EQ);
        assertEquals(Operator.fromString("eq"), Operator.EQ);
        assertEquals(Operator.fromString("like"), Operator.LIKE);
        assertEquals(Operator.fromString("LIKE"), Operator.LIKE);
        assertEquals(Operator.fromString("in"), Operator.IN);
        assertEquals(Operator.fromString("IN"), Operator.IN);
        assertEquals(Operator.fromString(">"), Operator.GT);
        assertEquals(Operator.fromString("gt"), Operator.GT);
    }

    @Test
    public void testOperatorFromStringInvalid() {
        assertNull(Operator.fromString("INVALID_OPERATOR"));
        assertNull(Operator.fromString(""));
        assertNull(Operator.fromString(null));
    }

    @Test
    public void testOperatorGetSymbol() {
        assertEquals(Operator.EQ.getSymbol(), "=");
        assertEquals(Operator.GT.getSymbol(), ">");
        assertEquals(Operator.LT.getSymbol(), "<");
        assertEquals(Operator.LIKE.getSymbol(), "like");
        assertEquals(Operator.IN.getSymbol(), "in");
    }

    @Test
    public void testOperatorGetSymbols() {
        String[] eqSymbols = Operator.EQ.getSymbols();
        assertTrue(eqSymbols.length >= 2);
        assertEquals(eqSymbols[0], "=");
        assertEquals(eqSymbols[1], "eq");

        String[] likeSymbols = Operator.LIKE.getSymbols();
        assertTrue(likeSymbols.length >= 2);
        assertEquals(likeSymbols[0], "like");
        assertEquals(likeSymbols[1], "LIKE");
    }

    @Test
    public void testOperatorToString() {
        assertEquals(Operator.EQ.toString(), "=");
        assertEquals(Operator.LIKE.toString(), "like");
        assertEquals(Operator.GT.toString(), ">");
    }

    @Test
    public void testFilterCriteriaDefaultConstructor() {
        FilterCriteria criteria = new FilterCriteria();

        assertNull(criteria.getAttributeName());
        assertNull(criteria.getOperator());
        assertNull(criteria.getAttributeValue());
        assertNull(criteria.getCondition());
        assertNull(criteria.getCriterion());
    }

    @Test
    public void testFilterCriteriaAttributeNameGetterSetter() {
        FilterCriteria criteria = new FilterCriteria();

        assertNull(criteria.getAttributeName());

        String attributeName = "name";
        criteria.setAttributeName(attributeName);
        assertEquals(criteria.getAttributeName(), attributeName);

        criteria.setAttributeName(null);
        assertNull(criteria.getAttributeName());
    }

    @Test
    public void testFilterCriteriaOperatorGetterSetter() {
        FilterCriteria criteria = new FilterCriteria();

        assertNull(criteria.getOperator());

        criteria.setOperator(Operator.EQ);
        assertEquals(criteria.getOperator(), Operator.EQ);

        criteria.setOperator(null);
        assertNull(criteria.getOperator());
    }

    @Test
    public void testFilterCriteriaAttributeValueGetterSetter() {
        FilterCriteria criteria = new FilterCriteria();

        assertNull(criteria.getAttributeValue());

        String attributeValue = "testValue";
        criteria.setAttributeValue(attributeValue);
        assertEquals(criteria.getAttributeValue(), attributeValue);

        criteria.setAttributeValue(null);
        assertNull(criteria.getAttributeValue());
    }

    @Test
    public void testFilterCriteriaConditionGetterSetter() {
        FilterCriteria criteria = new FilterCriteria();

        assertNull(criteria.getCondition());

        criteria.setCondition(FilterCriteria.Condition.AND);
        assertEquals(criteria.getCondition(), FilterCriteria.Condition.AND);

        criteria.setCondition(FilterCriteria.Condition.OR);
        assertEquals(criteria.getCondition(), FilterCriteria.Condition.OR);

        criteria.setCondition(null);
        assertNull(criteria.getCondition());
    }

    @Test
    public void testFilterCriteriaCriterionGetterSetter() {
        FilterCriteria criteria = new FilterCriteria();

        assertNull(criteria.getCriterion());

        List<FilterCriteria> criterion = new ArrayList<>();
        FilterCriteria subCriteria = new FilterCriteria();
        criterion.add(subCriteria);

        criteria.setCriterion(criterion);
        assertSame(criteria.getCriterion(), criterion);

        criteria.setCriterion(null);
        assertNull(criteria.getCriterion());
    }

    @Test
    public void testFilterCriteriaHashCodeConsistency() {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setOperator(Operator.EQ);
        criteria.setAttributeValue("test");

        int hashCode1 = criteria.hashCode();
        int hashCode2 = criteria.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testFilterCriteriaEquals() {
        FilterCriteria criteria1 = new FilterCriteria();
        criteria1.setAttributeName("name");
        criteria1.setOperator(Operator.EQ);
        criteria1.setAttributeValue("test");

        FilterCriteria criteria2 = new FilterCriteria();
        criteria2.setAttributeName("name");
        criteria2.setOperator(Operator.EQ);
        criteria2.setAttributeValue("test");

        assertTrue(criteria1.equals(criteria2));
        assertTrue(criteria2.equals(criteria1));
        assertEquals(criteria1.hashCode(), criteria2.hashCode());
    }

    @Test
    public void testFilterCriteriaToString() {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setOperator(Operator.LIKE);
        criteria.setAttributeValue("test*");

        String result = criteria.toString();

        assertNotNull(result);
        assertTrue(result.contains("name"));
        assertTrue(result.contains("test*"));
    }

    @Test
    public void testFilterCriteriaConditionEnum() {
        FilterCriteria.Condition[] conditions = FilterCriteria.Condition.values();
        assertEquals(conditions.length, 2);

        assertEquals(conditions[0], FilterCriteria.Condition.AND);
        assertEquals(conditions[1], FilterCriteria.Condition.OR);

        assertEquals(FilterCriteria.Condition.valueOf("AND"), FilterCriteria.Condition.AND);
        assertEquals(FilterCriteria.Condition.valueOf("OR"), FilterCriteria.Condition.OR);
    }

    @Test
    public void testComplexFilterCriteria() {
        // Create a complex filter with nested criteria
        FilterCriteria mainCriteria = new FilterCriteria();
        mainCriteria.setCondition(FilterCriteria.Condition.AND);

        List<FilterCriteria> subCriteria = new ArrayList<>();

        FilterCriteria criteria1 = new FilterCriteria();
        criteria1.setAttributeName("name");
        criteria1.setOperator(Operator.LIKE);
        criteria1.setAttributeValue("test*");

        FilterCriteria criteria2 = new FilterCriteria();
        criteria2.setAttributeName("owner");
        criteria2.setOperator(Operator.EQ);
        criteria2.setAttributeValue("admin");

        subCriteria.add(criteria1);
        subCriteria.add(criteria2);

        mainCriteria.setCriterion(subCriteria);

        // Verify the complex structure
        assertEquals(mainCriteria.getCondition(), FilterCriteria.Condition.AND);
        assertEquals(mainCriteria.getCriterion().size(), 2);
        assertEquals(mainCriteria.getCriterion().get(0).getAttributeName(), "name");
        assertEquals(mainCriteria.getCriterion().get(1).getAttributeName(), "owner");
    }

    @Test
    public void testCompleteSearchParametersConfiguration() {
        // Configure all parameters
        searchParameters.setQuery("name:\"production*\" AND classification:PII");
        searchParameters.setTypeName("DataSet");
        searchParameters.setClassification("PII");
        searchParameters.setRelationshipName("owns");
        searchParameters.setTermName("CustomerData");
        searchParameters.setExcludeDeletedEntities(true);
        searchParameters.setIncludeClassificationAttributes(true);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);
        searchParameters.setExcludeHeaderAttributes(true);
        searchParameters.setLimit(100);
        searchParameters.setOffset(50);
        searchParameters.setMarker("page-2");
        searchParameters.setSortBy("createTime");
        searchParameters.setSortOrder(SortOrder.DESCENDING);

        Set<String> attributes = new HashSet<>();
        attributes.add("qualifiedName");
        attributes.add("owner");
        searchParameters.setAttributes(attributes);

        FilterCriteria entityFilters = new FilterCriteria();
        entityFilters.setAttributeName("owner");
        entityFilters.setOperator(Operator.EQ);
        entityFilters.setAttributeValue("dataTeam");
        searchParameters.setEntityFilters(entityFilters);

        // Verify all parameters
        assertTrue(searchParameters.getQuery().contains("production*"));
        assertEquals(searchParameters.getTypeName(), "DataSet");
        assertEquals(searchParameters.getClassification(), "PII");
        assertEquals(searchParameters.getRelationshipName(), "owns");
        assertEquals(searchParameters.getTermName(), "CustomerData");
        assertTrue(searchParameters.getExcludeDeletedEntities());
        assertTrue(searchParameters.getIncludeClassificationAttributes());
        assertFalse(searchParameters.getIncludeSubTypes());
        assertFalse(searchParameters.getIncludeSubClassifications());
        assertTrue(searchParameters.getExcludeHeaderAttributes());
        assertEquals(searchParameters.getLimit(), 100);
        assertEquals(searchParameters.getOffset(), 50);
        assertEquals(searchParameters.getMarker(), "page-2");
        assertEquals(searchParameters.getSortBy(), "createTime");
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);
        assertEquals(searchParameters.getAttributes().size(), 2);
        assertNotNull(searchParameters.getEntityFilters());
    }
}
