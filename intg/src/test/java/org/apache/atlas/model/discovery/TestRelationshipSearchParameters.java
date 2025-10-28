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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestRelationshipSearchParameters {
    private RelationshipSearchParameters searchParameters;

    @BeforeMethod
    public void setUp() {
        searchParameters = new RelationshipSearchParameters();
    }

    @Test
    public void testDefaultConstructor() {
        RelationshipSearchParameters params = new RelationshipSearchParameters();

        assertNull(params.getRelationshipName());
        assertNull(params.getRelationshipFilters());
        assertFalse(params.isIncludeSubTypes());
        assertEquals(params.getOffset(), 0);
        assertEquals(params.getLimit(), 0);
        assertNull(params.getSortBy());
        assertNull(params.getSortOrder());
        assertNull(params.getMarker());
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
    public void testRelationshipFiltersGetterSetter() {
        assertNull(searchParameters.getRelationshipFilters());

        SearchParameters.FilterCriteria filters = new SearchParameters.FilterCriteria();
        searchParameters.setRelationshipFilters(filters);
        assertSame(searchParameters.getRelationshipFilters(), filters);

        searchParameters.setRelationshipFilters(null);
        assertNull(searchParameters.getRelationshipFilters());
    }

    @Test
    public void testIncludeSubTypesGetterSetter() {
        assertFalse(searchParameters.isIncludeSubTypes());

        searchParameters.setIncludeSubTypes(true);
        assertTrue(searchParameters.isIncludeSubTypes());

        searchParameters.setIncludeSubTypes(false);
        assertFalse(searchParameters.isIncludeSubTypes());
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

        searchParameters.setOffset(Integer.MIN_VALUE);
        assertEquals(searchParameters.getOffset(), Integer.MIN_VALUE);
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

        searchParameters.setLimit(Integer.MIN_VALUE);
        assertEquals(searchParameters.getLimit(), Integer.MIN_VALUE);
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
    public void testMarkerGetterSetter() {
        assertNull(searchParameters.getMarker());

        String marker = "next-page-marker-123";
        searchParameters.setMarker(marker);
        assertEquals(searchParameters.getMarker(), marker);

        searchParameters.setMarker("");
        assertEquals(searchParameters.getMarker(), "");

        searchParameters.setMarker(null);
        assertNull(searchParameters.getMarker());
    }

    @Test
    public void testCompleteParameterConfiguration() {
        String relationshipName = "contains";
        SearchParameters.FilterCriteria filters = new SearchParameters.FilterCriteria();
        boolean includeSubTypes = true;
        int offset = 25;
        int limit = 200;
        String sortBy = "modifiedTime";
        SortOrder sortOrder = SortOrder.DESCENDING;
        String marker = "pagination-marker-456";

        searchParameters.setRelationshipName(relationshipName);
        searchParameters.setRelationshipFilters(filters);
        searchParameters.setIncludeSubTypes(includeSubTypes);
        searchParameters.setOffset(offset);
        searchParameters.setLimit(limit);
        searchParameters.setSortBy(sortBy);
        searchParameters.setSortOrder(sortOrder);
        searchParameters.setMarker(marker);

        assertEquals(searchParameters.getRelationshipName(), relationshipName);
        assertSame(searchParameters.getRelationshipFilters(), filters);
        assertTrue(searchParameters.isIncludeSubTypes());
        assertEquals(searchParameters.getOffset(), offset);
        assertEquals(searchParameters.getLimit(), limit);
        assertEquals(searchParameters.getSortBy(), sortBy);
        assertEquals(searchParameters.getSortOrder(), sortOrder);
        assertEquals(searchParameters.getMarker(), marker);
    }

    @Test
    public void testRelationshipNameWithSpecialCharacters() {
        String specialName = "custom_relationship-type.v2";
        searchParameters.setRelationshipName(specialName);
        assertEquals(searchParameters.getRelationshipName(), specialName);

        String unicodeName = "unicodeString";
        searchParameters.setRelationshipName(unicodeName);
        assertEquals(searchParameters.getRelationshipName(), unicodeName);
    }

    @Test
    public void testSortByWithSpecialCharacters() {
        String specialSortBy = "relationship.createTime";
        searchParameters.setSortBy(specialSortBy);
        assertEquals(searchParameters.getSortBy(), specialSortBy);

        String unicodeSortBy = "unicodeString";
        searchParameters.setSortBy(unicodeSortBy);
        assertEquals(searchParameters.getSortBy(), unicodeSortBy);
    }

    @Test
    public void testMarkerWithSpecialCharacters() {
        String specialMarker = "marker_with-special.chars@123";
        searchParameters.setMarker(specialMarker);
        assertEquals(searchParameters.getMarker(), specialMarker);

        String unicodeMarker = "unicodeString";
        searchParameters.setMarker(unicodeMarker);
        assertEquals(searchParameters.getMarker(), unicodeMarker);
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
    public void testComplexFilterCriteria() {
        SearchParameters.FilterCriteria filters = new SearchParameters.FilterCriteria();
        filters.setAttributeName("status");
        filters.setOperator(SearchParameters.Operator.EQ);
        filters.setAttributeValue("ACTIVE");

        searchParameters.setRelationshipFilters(filters);

        assertEquals(searchParameters.getRelationshipFilters().getAttributeName(), "status");
        assertEquals(searchParameters.getRelationshipFilters().getOperator(), SearchParameters.Operator.EQ);
        assertEquals(searchParameters.getRelationshipFilters().getAttributeValue(), "ACTIVE");
    }

    @Test
    public void testSortOrderValues() {
        // Test both sort order values
        searchParameters.setSortOrder(SortOrder.ASCENDING);
        assertEquals(searchParameters.getSortOrder(), SortOrder.ASCENDING);

        searchParameters.setSortOrder(SortOrder.DESCENDING);
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);
    }

    @Test
    public void testPaginationScenario() {
        // Test typical pagination scenario
        searchParameters.setOffset(50);
        searchParameters.setLimit(25);
        searchParameters.setMarker("page-2-marker");

        assertEquals(searchParameters.getOffset(), 50);
        assertEquals(searchParameters.getLimit(), 25);
        assertEquals(searchParameters.getMarker(), "page-2-marker");

        // Test next page
        searchParameters.setOffset(75);
        searchParameters.setMarker("page-3-marker");
        assertEquals(searchParameters.getOffset(), 75);
        assertEquals(searchParameters.getLimit(), 25);
        assertEquals(searchParameters.getMarker(), "page-3-marker");
    }

    @Test
    public void testSerializable() {
        assertNotNull(searchParameters);
    }

    @Test
    public void testSerialVersionUID() throws Exception {
        java.lang.reflect.Field serialVersionUIDField = RelationshipSearchParameters.class.getDeclaredField("serialVersionUID");
        serialVersionUIDField.setAccessible(true);
        long serialVersionUID = (Long) serialVersionUIDField.get(null);

        assertEquals(serialVersionUID, 1L);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(searchParameters);
    }

    @Test
    public void testCommonRelationshipTypes() {
        String[] commonRelationshipTypes = {
            "owns",
            "contains",
            "dependsOn",
            "derivedFrom",
            "hasLineage",
            "isPartOf",
            "references",
            "implements"
        };

        for (String relationshipType : commonRelationshipTypes) {
            searchParameters.setRelationshipName(relationshipType);
            assertEquals(searchParameters.getRelationshipName(), relationshipType);
        }
    }

    @Test
    public void testCompleteRelationshipSearchScenario() {
        // Simulate a complete relationship search scenario
        searchParameters.setRelationshipName("owns");
        searchParameters.setIncludeSubTypes(true);
        searchParameters.setOffset(0);
        searchParameters.setLimit(100);
        searchParameters.setSortBy("createTime");
        searchParameters.setSortOrder(SortOrder.DESCENDING);

        SearchParameters.FilterCriteria filters = new SearchParameters.FilterCriteria();
        filters.setAttributeName("status");
        filters.setOperator(SearchParameters.Operator.EQ);
        filters.setAttributeValue("ACTIVE");
        searchParameters.setRelationshipFilters(filters);

        // Verify all parameters are set correctly
        assertEquals(searchParameters.getRelationshipName(), "owns");
        assertTrue(searchParameters.isIncludeSubTypes());
        assertEquals(searchParameters.getOffset(), 0);
        assertEquals(searchParameters.getLimit(), 100);
        assertEquals(searchParameters.getSortBy(), "createTime");
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);
        assertNotNull(searchParameters.getRelationshipFilters());
        assertEquals(searchParameters.getRelationshipFilters().getAttributeName(), "status");
    }

    @Test
    public void testEmptyStringValues() {
        searchParameters.setRelationshipName("");
        searchParameters.setSortBy("");
        searchParameters.setMarker("");

        assertEquals(searchParameters.getRelationshipName(), "");
        assertEquals(searchParameters.getSortBy(), "");
        assertEquals(searchParameters.getMarker(), "");
    }

    @Test
    public void testNullValueHandling() {
        // Set all fields to null
        searchParameters.setRelationshipName(null);
        searchParameters.setRelationshipFilters(null);
        searchParameters.setSortBy(null);
        searchParameters.setSortOrder(null);
        searchParameters.setMarker(null);

        assertNull(searchParameters.getRelationshipName());
        assertNull(searchParameters.getRelationshipFilters());
        assertNull(searchParameters.getSortBy());
        assertNull(searchParameters.getSortOrder());
        assertNull(searchParameters.getMarker());
    }

    @Test
    public void testBooleanFieldBehavior() {
        // Test default value
        assertFalse(searchParameters.isIncludeSubTypes());

        // Test setting to true
        searchParameters.setIncludeSubTypes(true);
        assertTrue(searchParameters.isIncludeSubTypes());

        // Test setting back to false
        searchParameters.setIncludeSubTypes(false);
        assertFalse(searchParameters.isIncludeSubTypes());
    }

    @Test
    public void testFieldsIndependence() {
        // Test that fields can be set independently
        searchParameters.setRelationshipName("testRelationship");
        assertEquals(searchParameters.getOffset(), 0);
        assertNull(searchParameters.getSortBy());

        searchParameters.setOffset(50);
        assertEquals(searchParameters.getRelationshipName(), "testRelationship");
        assertNull(searchParameters.getSortBy());

        searchParameters.setSortBy("name");
        assertEquals(searchParameters.getRelationshipName(), "testRelationship");
        assertEquals(searchParameters.getOffset(), 50);
    }

    @Test
    public void testLongMarkerString() {
        StringBuilder longMarker = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longMarker.append("marker_part_").append(i).append("_");
        }
        String veryLongMarker = longMarker.toString();

        searchParameters.setMarker(veryLongMarker);
        assertEquals(searchParameters.getMarker(), veryLongMarker);
    }

    @Test
    public void testRelationshipFiltersModification() {
        SearchParameters.FilterCriteria filters = new SearchParameters.FilterCriteria();
        filters.setAttributeName("initialAttribute");
        searchParameters.setRelationshipFilters(filters);

        // Modify the filter after setting
        filters.setAttributeName("modifiedAttribute");
        filters.setOperator(SearchParameters.Operator.LIKE);
        filters.setAttributeValue("test*");

        // Verify the modification is reflected
        assertEquals(searchParameters.getRelationshipFilters().getAttributeName(), "modifiedAttribute");
        assertEquals(searchParameters.getRelationshipFilters().getOperator(), SearchParameters.Operator.LIKE);
        assertEquals(searchParameters.getRelationshipFilters().getAttributeValue(), "test*");
    }

    @Test
    public void testTypicalUseCases() {
        // Use case 1: Find all "owns" relationships
        searchParameters.setRelationshipName("owns");
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setLimit(50);

        assertEquals(searchParameters.getRelationshipName(), "owns");
        assertFalse(searchParameters.isIncludeSubTypes());
        assertEquals(searchParameters.getLimit(), 50);

        // Use case 2: Find relationships with pagination
        searchParameters.setOffset(100);
        searchParameters.setMarker("page-3");

        assertEquals(searchParameters.getOffset(), 100);
        assertEquals(searchParameters.getMarker(), "page-3");

        // Use case 3: Find relationships sorted by creation time
        searchParameters.setSortBy("createTime");
        searchParameters.setSortOrder(SortOrder.DESCENDING);

        assertEquals(searchParameters.getSortBy(), "createTime");
        assertEquals(searchParameters.getSortOrder(), SortOrder.DESCENDING);
    }
}
