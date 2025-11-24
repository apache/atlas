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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestQuickSearchParameters {
    private QuickSearchParameters searchParameters;

    @BeforeMethod
    public void setUp() {
        searchParameters = new QuickSearchParameters();
    }

    @Test
    public void testDefaultConstructor() {
        QuickSearchParameters params = new QuickSearchParameters();

        assertNull(params.getQuery());
        assertNull(params.getTypeName());
        assertNull(params.getEntityFilters());
        assertFalse(params.getIncludeSubTypes());
        assertFalse(params.getExcludeDeletedEntities());
        assertEquals(params.getOffset(), 0);
        assertEquals(params.getLimit(), 0);
        assertNull(params.getAttributes());
        assertNull(params.getSortBy());
        assertNull(params.getSortOrder());
        assertFalse(params.getExcludeHeaderAttributes());
    }

    @Test
    public void testParameterizedConstructor() {
        String query = "test query";
        String typeName = "TestType";
        FilterCriteria entityFilters = new FilterCriteria();
        boolean includeSubTypes = true;
        boolean excludeDeletedEntities = true;
        int offset = 10;
        int limit = 100;
        Set<String> attributes = new HashSet<>();
        attributes.add("attr1");
        String sortBy = "name";
        SortOrder sortOrder = SortOrder.ASCENDING;

        QuickSearchParameters params = new QuickSearchParameters(
                query, typeName, entityFilters, includeSubTypes, excludeDeletedEntities,
                offset, limit, attributes, sortBy, sortOrder);

        assertEquals(params.getQuery(), query);
        assertEquals(params.getTypeName(), typeName);
        assertSame(params.getEntityFilters(), entityFilters);
        assertTrue(params.getIncludeSubTypes());
        assertTrue(params.getExcludeDeletedEntities());
        assertEquals(params.getOffset(), offset);
        assertEquals(params.getLimit(), limit);
        assertSame(params.getAttributes(), attributes);
        assertEquals(params.getSortBy(), sortBy);
        assertEquals(params.getSortOrder(), sortOrder);
    }

    @Test
    public void testQueryGetterSetter() {
        assertNull(searchParameters.getQuery());

        String query = "test search query";
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
    public void testEntityFiltersGetterSetter() {
        assertNull(searchParameters.getEntityFilters());

        FilterCriteria filters = new FilterCriteria();
        searchParameters.setEntityFilters(filters);
        assertSame(searchParameters.getEntityFilters(), filters);

        searchParameters.setEntityFilters(null);
        assertNull(searchParameters.getEntityFilters());
    }

    @Test
    public void testIncludeSubTypesGetterSetter() {
        assertFalse(searchParameters.getIncludeSubTypes());

        searchParameters.setIncludeSubTypes(true);
        assertTrue(searchParameters.getIncludeSubTypes());

        searchParameters.setIncludeSubTypes(false);
        assertFalse(searchParameters.getIncludeSubTypes());
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
    public void testLimitGetterSetter() {
        assertEquals(searchParameters.getLimit(), 0);

        searchParameters.setLimit(200);
        assertEquals(searchParameters.getLimit(), 200);

        searchParameters.setLimit(-1);
        assertEquals(searchParameters.getLimit(), -1);

        searchParameters.setLimit(Integer.MAX_VALUE);
        assertEquals(searchParameters.getLimit(), Integer.MAX_VALUE);
    }

    @Test
    public void testAttributesGetterSetter() {
        assertNull(searchParameters.getAttributes());

        Set<String> attributes = new HashSet<>();
        attributes.add("name");
        attributes.add("description");
        attributes.add("owner");

        searchParameters.setAttributes(attributes);
        assertSame(searchParameters.getAttributes(), attributes);

        searchParameters.setAttributes(null);
        assertNull(searchParameters.getAttributes());
    }

    @Test
    public void testSortByGetterSetter() {
        assertNull(searchParameters.getSortBy());

        String sortBy = "createdTime";
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
    public void testExcludeHeaderAttributesGetterSetter() {
        assertFalse(searchParameters.getExcludeHeaderAttributes());

        searchParameters.setExcludeHeaderAttributes(true);
        assertTrue(searchParameters.getExcludeHeaderAttributes());

        searchParameters.setExcludeHeaderAttributes(false);
        assertFalse(searchParameters.getExcludeHeaderAttributes());
    }

    @Test
    public void testCompleteParameterConfiguration() {
        String query = "comprehensive test query";
        String typeName = "CompleteTestType";
        FilterCriteria entityFilters = new FilterCriteria();
        boolean includeSubTypes = true;
        boolean excludeDeletedEntities = true;
        int offset = 25;
        int limit = 500;
        Set<String> attributes = new HashSet<>();
        attributes.add("name");
        attributes.add("qualifiedName");
        attributes.add("owner");
        String sortBy = "modifiedTime";
        SortOrder sortOrder = SortOrder.DESCENDING;
        boolean excludeHeaderAttributes = true;

        searchParameters.setQuery(query);
        searchParameters.setTypeName(typeName);
        searchParameters.setEntityFilters(entityFilters);
        searchParameters.setIncludeSubTypes(includeSubTypes);
        searchParameters.setExcludeDeletedEntities(excludeDeletedEntities);
        searchParameters.setOffset(offset);
        searchParameters.setLimit(limit);
        searchParameters.setAttributes(attributes);
        searchParameters.setSortBy(sortBy);
        searchParameters.setSortOrder(sortOrder);
        searchParameters.setExcludeHeaderAttributes(excludeHeaderAttributes);

        assertEquals(searchParameters.getQuery(), query);
        assertEquals(searchParameters.getTypeName(), typeName);
        assertSame(searchParameters.getEntityFilters(), entityFilters);
        assertTrue(searchParameters.getIncludeSubTypes());
        assertTrue(searchParameters.getExcludeDeletedEntities());
        assertEquals(searchParameters.getOffset(), offset);
        assertEquals(searchParameters.getLimit(), limit);
        assertSame(searchParameters.getAttributes(), attributes);
        assertEquals(searchParameters.getSortBy(), sortBy);
        assertEquals(searchParameters.getSortOrder(), sortOrder);
        assertTrue(searchParameters.getExcludeHeaderAttributes());
    }

    @Test
    public void testParameterizedConstructorWithNullValues() {
        QuickSearchParameters params = new QuickSearchParameters(
                null, null, null, false, false, 0, 0, null, null, null);

        assertNull(params.getQuery());
        assertNull(params.getTypeName());
        assertNull(params.getEntityFilters());
        assertFalse(params.getIncludeSubTypes());
        assertFalse(params.getExcludeDeletedEntities());
        assertEquals(params.getOffset(), 0);
        assertEquals(params.getLimit(), 0);
        assertNull(params.getAttributes());
        assertNull(params.getSortBy());
        assertNull(params.getSortOrder());
    }

    @Test
    public void testEmptyAttributesSet() {
        Set<String> emptyAttributes = new HashSet<>();
        searchParameters.setAttributes(emptyAttributes);

        assertNotNull(searchParameters.getAttributes());
        assertEquals(searchParameters.getAttributes().size(), 0);
        assertTrue(searchParameters.getAttributes().isEmpty());
    }

    @Test
    public void testAttributesSetModification() {
        Set<String> attributes = new HashSet<>();
        attributes.add("initialAttribute");
        searchParameters.setAttributes(attributes);

        attributes.add("additionalAttribute");

        assertEquals(searchParameters.getAttributes().size(), 2);
        assertTrue(searchParameters.getAttributes().contains("initialAttribute"));
        assertTrue(searchParameters.getAttributes().contains("additionalAttribute"));
    }

    @Test
    public void testQueryWithSpecialCharacters() {
        String specialQuery = "name:\"test entity\" AND owner:user@domain.com";
        searchParameters.setQuery(specialQuery);
        assertEquals(searchParameters.getQuery(), specialQuery);

        String unicodeQuery = "名称:测试实体";
        searchParameters.setQuery(unicodeQuery);
        assertEquals(searchParameters.getQuery(), unicodeQuery);
    }

    @Test
    public void testTypeNameWithSpecialCharacters() {
        String specialTypeName = "Custom_Type-With.Special@Characters";
        searchParameters.setTypeName(specialTypeName);
        assertEquals(searchParameters.getTypeName(), specialTypeName);

        String unicodeTypeName = "unicodeString";
        searchParameters.setTypeName(unicodeTypeName);
        assertEquals(searchParameters.getTypeName(), unicodeTypeName);
    }

    @Test
    public void testSortByWithSpecialCharacters() {
        String specialSortBy = "custom.attribute_name";
        searchParameters.setSortBy(specialSortBy);
        assertEquals(searchParameters.getSortBy(), specialSortBy);

        String unicodeSortBy = "unicodeString";
        searchParameters.setSortBy(unicodeSortBy);
        assertEquals(searchParameters.getSortBy(), unicodeSortBy);
    }

    @Test
    public void testOffsetAndLimitEdgeCases() {
        // Test with zero values
        searchParameters.setOffset(0);
        searchParameters.setLimit(0);
        assertEquals(searchParameters.getOffset(), 0);
        assertEquals(searchParameters.getLimit(), 0);

        // Test with negative values
        searchParameters.setOffset(-100);
        searchParameters.setLimit(-50);
        assertEquals(searchParameters.getOffset(), -100);
        assertEquals(searchParameters.getLimit(), -50);

        // Test with large values
        searchParameters.setOffset(1000000);
        searchParameters.setLimit(999999);
        assertEquals(searchParameters.getOffset(), 1000000);
        assertEquals(searchParameters.getLimit(), 999999);
    }

    @Test
    public void testBooleanFieldsIndependence() {
        // Test that boolean fields can be set independently
        searchParameters.setIncludeSubTypes(true);
        assertFalse(searchParameters.getExcludeDeletedEntities());
        assertFalse(searchParameters.getExcludeHeaderAttributes());

        searchParameters.setExcludeDeletedEntities(true);
        assertTrue(searchParameters.getIncludeSubTypes());
        assertFalse(searchParameters.getExcludeHeaderAttributes());

        searchParameters.setExcludeHeaderAttributes(true);
        assertTrue(searchParameters.getIncludeSubTypes());
        assertTrue(searchParameters.getExcludeDeletedEntities());
    }

    @Test
    public void testSerializable() {
        assertNotNull(searchParameters);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(searchParameters);
    }

    @Test
    public void testComplexFilterCriteria() {
        FilterCriteria filters = new FilterCriteria();
        filters.setAttributeName("name");
        filters.setOperator(SearchParameters.Operator.LIKE);
        filters.setAttributeValue("test*");

        searchParameters.setEntityFilters(filters);

        assertEquals(searchParameters.getEntityFilters().getAttributeName(), "name");
        assertEquals(searchParameters.getEntityFilters().getOperator(), SearchParameters.Operator.LIKE);
        assertEquals(searchParameters.getEntityFilters().getAttributeValue(), "test*");
    }

    @Test
    public void testAttributesWithVariousTypes() {
        Set<String> attributes = new HashSet<>();
        attributes.add("stringAttribute");
        attributes.add("numericAttribute");
        attributes.add("dateAttribute");
        attributes.add("booleanAttribute");
        attributes.add("arrayAttribute");
        attributes.add("objectAttribute");

        searchParameters.setAttributes(attributes);

        assertEquals(searchParameters.getAttributes().size(), 6);
        assertTrue(searchParameters.getAttributes().contains("stringAttribute"));
        assertTrue(searchParameters.getAttributes().contains("numericAttribute"));
        assertTrue(searchParameters.getAttributes().contains("dateAttribute"));
        assertTrue(searchParameters.getAttributes().contains("booleanAttribute"));
        assertTrue(searchParameters.getAttributes().contains("arrayAttribute"));
        assertTrue(searchParameters.getAttributes().contains("objectAttribute"));
    }

    @Test
    public void testSortOrderValues() {
        searchParameters.setSortOrder(SortOrder.ASCENDING);
        assertEquals(searchParameters.getSortOrder(), SortOrder.ASCENDING);

        searchParameters.setSortOrder(SortOrder.DESCENDING);
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);
    }

    @Test
    public void testSerialVersionUID() throws Exception {
        java.lang.reflect.Field serialVersionUIDField = QuickSearchParameters.class.getDeclaredField("serialVersionUID");
        serialVersionUIDField.setAccessible(true);
        long serialVersionUID = (Long) serialVersionUIDField.get(null);

        assertEquals(serialVersionUID, 1L);
    }

    @Test
    public void testPaginationScenario() {
        searchParameters.setOffset(100);
        searchParameters.setLimit(25);

        assertEquals(searchParameters.getOffset(), 100);
        assertEquals(searchParameters.getLimit(), 25);

        searchParameters.setOffset(125);
        assertEquals(searchParameters.getOffset(), 125);
        assertEquals(searchParameters.getLimit(), 25);
    }

    @Test
    public void testSearchScenarioWithAllParameters() {
        searchParameters.setQuery("name:\"production*\" AND owner:dataTeam");
        searchParameters.setTypeName("DataSet");
        searchParameters.setIncludeSubTypes(true);
        searchParameters.setExcludeDeletedEntities(true);
        searchParameters.setOffset(0);
        searchParameters.setLimit(50);

        Set<String> attributes = new HashSet<>();
        attributes.add("qualifiedName");
        attributes.add("owner");
        attributes.add("createTime");
        searchParameters.setAttributes(attributes);

        searchParameters.setSortBy("createTime");
        searchParameters.setSortOrder(SortOrder.DESCENDING);
        searchParameters.setExcludeHeaderAttributes(false);

        FilterCriteria filters = new FilterCriteria();
        filters.setAttributeName("owner");
        filters.setOperator(SearchParameters.Operator.EQ);
        filters.setAttributeValue("dataTeam");
        searchParameters.setEntityFilters(filters);

        assertTrue(searchParameters.getQuery().contains("production*"));
        assertEquals(searchParameters.getTypeName(), "DataSet");
        assertTrue(searchParameters.getIncludeSubTypes());
        assertTrue(searchParameters.getExcludeDeletedEntities());
        assertEquals(searchParameters.getOffset(), 0);
        assertEquals(searchParameters.getLimit(), 50);
        assertEquals(searchParameters.getAttributes().size(), 3);
        assertEquals(searchParameters.getSortBy(), "createTime");
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);
        assertFalse(searchParameters.getExcludeHeaderAttributes());
        assertNotNull(searchParameters.getEntityFilters());
    }
}
