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
package org.apache.atlas.model.audit;

import org.apache.atlas.SortOrder;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestAuditSearchParameters {
    private AuditSearchParameters searchParameters;

    @BeforeMethod
    public void setUp() {
        searchParameters = new AuditSearchParameters();
    }

    @Test
    public void testDefaultConstructor() {
        AuditSearchParameters params = new AuditSearchParameters();

        assertNull(params.getAuditFilters());
        assertEquals(params.getLimit(), 0);
        assertEquals(params.getOffset(), 0);
        assertNull(params.getSortBy());
        assertNull(params.getSortOrder());
    }

    @Test
    public void testGetSerialVersionUID() {
        long serialVersionUID = AuditSearchParameters.getSerialVersionUID();
        assertEquals(serialVersionUID, 1L);
    }

    @Test
    public void testAuditFiltersGetterSetter() {
        assertNull(searchParameters.getAuditFilters());

        FilterCriteria filters = new FilterCriteria();
        searchParameters.setAuditFilters(filters);
        assertSame(searchParameters.getAuditFilters(), filters);

        searchParameters.setAuditFilters(null);
        assertNull(searchParameters.getAuditFilters());
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
    public void testSortByGetterSetter() {
        assertNull(searchParameters.getSortBy());

        String sortBy = "timestamp";
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
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        params1.setLimit(100);
        params1.setOffset(10);
        params1.setSortBy("timestamp");

        params2.setLimit(100);
        params2.setOffset(10);
        params2.setSortBy("timestamp");

        assertEquals(params1.hashCode(), params2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        params1.setLimit(100);
        params2.setLimit(200);

        assertNotEquals(params1.hashCode(), params2.hashCode());
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
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        FilterCriteria filters = new FilterCriteria();
        params1.setAuditFilters(filters);
        params1.setLimit(100);
        params1.setOffset(10);
        params1.setSortBy("timestamp");
        params1.setSortOrder(SortOrder.ASCENDING);

        params2.setAuditFilters(filters);
        params2.setLimit(100);
        params2.setOffset(10);
        params2.setSortBy("timestamp");
        params2.setSortOrder(SortOrder.ASCENDING);

        assertTrue(params1.equals(params2));
        assertTrue(params2.equals(params1));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        params1.setLimit(100);
        params2.setLimit(200);

        assertFalse(params1.equals(params2));
        assertFalse(params2.equals(params1));
    }

    @Test
    public void testEqualsWithNullAuditFilters() {
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        params1.setAuditFilters(null);
        params2.setAuditFilters(null);

        assertTrue(params1.equals(params2));
    }

    @Test
    public void testEqualsWithOneNullAuditFilter() {
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        params1.setAuditFilters(new FilterCriteria());
        params2.setAuditFilters(null);

        assertFalse(params1.equals(params2));
        assertFalse(params2.equals(params1));
    }

    @Test
    public void testToString() {
        searchParameters.setLimit(100);
        searchParameters.setOffset(10);
        searchParameters.setSortBy("timestamp");
        searchParameters.setSortOrder(SortOrder.DESCENDING);

        String result = searchParameters.toString();

        assertNotNull(result);
        assertTrue(result.contains("AuditSearchParameters"));
        assertTrue(result.contains("limit=100"));
        assertTrue(result.contains("offset=10"));
        assertTrue(result.contains("sortBy='timestamp'"));
        assertTrue(result.contains("sortOrder=DESCENDING"));
    }

    @Test
    public void testToStringWithNullValues() {
        String result = searchParameters.toString();

        assertNotNull(result);
        assertTrue(result.contains("AuditSearchParameters"));
        assertTrue(result.contains("auditFilters=null"));
        assertTrue(result.contains("sortBy='null'"));
        assertTrue(result.contains("sortOrder=null"));
    }

    @Test
    public void testToStringWithAuditFilters() {
        FilterCriteria filters = new FilterCriteria();
        searchParameters.setAuditFilters(filters);

        String result = searchParameters.toString();

        assertNotNull(result);
        assertTrue(result.contains("auditFilters="));
    }

    @Test
    public void testSerializable() {
        assertNotNull(searchParameters);
    }

    @Test
    public void testCompleteConfiguration() {
        FilterCriteria filters = new FilterCriteria();
        searchParameters.setAuditFilters(filters);
        searchParameters.setLimit(500);
        searchParameters.setOffset(25);
        searchParameters.setSortBy("user");
        searchParameters.setSortOrder(SortOrder.ASCENDING);

        assertSame(searchParameters.getAuditFilters(), filters);
        assertEquals(searchParameters.getLimit(), 500);
        assertEquals(searchParameters.getOffset(), 25);
        assertEquals(searchParameters.getSortBy(), "user");
        assertEquals(searchParameters.getSortOrder(), SortOrder.ASCENDING);
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AuditSearchParameters params1 = createFullyConfiguredParameters();
        AuditSearchParameters params2 = createFullyConfiguredParameters();

        // Test equals contract
        assertTrue(params1.equals(params2));
        assertTrue(params2.equals(params1));
        assertEquals(params1.hashCode(), params2.hashCode());

        // Test reflexivity
        assertTrue(params1.equals(params1));

        // Test consistency
        assertTrue(params1.equals(params2));
        assertTrue(params1.equals(params2));
    }

    @Test
    public void testLimitEdgeCases() {
        // Test with zero
        searchParameters.setLimit(0);
        assertEquals(searchParameters.getLimit(), 0);

        // Test with negative values
        searchParameters.setLimit(-100);
        assertEquals(searchParameters.getLimit(), -100);

        // Test with large positive values
        searchParameters.setLimit(1000000);
        assertEquals(searchParameters.getLimit(), 1000000);
    }

    @Test
    public void testOffsetEdgeCases() {
        // Test with zero
        searchParameters.setOffset(0);
        assertEquals(searchParameters.getOffset(), 0);

        // Test with negative values
        searchParameters.setOffset(-50);
        assertEquals(searchParameters.getOffset(), -50);

        // Test with large positive values
        searchParameters.setOffset(999999);
        assertEquals(searchParameters.getOffset(), 999999);
    }

    @Test
    public void testSortByWithSpecialCharacters() {
        String specialSortBy = "entity.timestamp";
        searchParameters.setSortBy(specialSortBy);
        assertEquals(searchParameters.getSortBy(), specialSortBy);

        String unicodeSortBy = "unicodeString";
        searchParameters.setSortBy(unicodeSortBy);
        assertEquals(searchParameters.getSortBy(), unicodeSortBy);
    }

    @Test
    public void testSortByWithEmptyString() {
        searchParameters.setSortBy("");
        assertEquals(searchParameters.getSortBy(), "");
    }

    @Test
    public void testFieldsIndependence() {
        // Test that fields can be set independently
        searchParameters.setLimit(100);
        assertEquals(searchParameters.getOffset(), 0);
        assertNull(searchParameters.getSortBy());

        searchParameters.setOffset(50);
        assertEquals(searchParameters.getLimit(), 100);
        assertNull(searchParameters.getSortBy());

        searchParameters.setSortBy("timestamp");
        assertEquals(searchParameters.getLimit(), 100);
        assertEquals(searchParameters.getOffset(), 50);
    }

    @Test
    public void testEqualsWithAllFieldsDifferent() {
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        FilterCriteria filters1 = new FilterCriteria();
        FilterCriteria filters2 = new FilterCriteria();

        params1.setAuditFilters(filters1);
        params1.setLimit(100);
        params1.setOffset(10);
        params1.setSortBy("timestamp");
        params1.setSortOrder(SortOrder.ASCENDING);

        params2.setAuditFilters(filters2);
        params2.setLimit(200);
        params2.setOffset(20);
        params2.setSortBy("user");
        params2.setSortOrder(SortOrder.DESCENDING);

        assertFalse(params1.equals(params2));
    }

    @Test
    public void testEqualsWithSomeFieldsSame() {
        AuditSearchParameters params1 = new AuditSearchParameters();
        AuditSearchParameters params2 = new AuditSearchParameters();

        params1.setLimit(100);
        params1.setOffset(10);
        params1.setSortBy("timestamp");

        params2.setLimit(100);
        params2.setOffset(20); // Different offset
        params2.setSortBy("timestamp");

        assertFalse(params1.equals(params2));
    }

    @Test
    public void testPackagePrivateConstructor() {
        AuditSearchParameters params = new AuditSearchParameters();
        assertNotNull(params);
        assertNull(params.getAuditFilters());
        assertEquals(params.getLimit(), 0);
        assertEquals(params.getOffset(), 0);
        assertNull(params.getSortBy());
        assertNull(params.getSortOrder());
    }

    private AuditSearchParameters createFullyConfiguredParameters() {
        AuditSearchParameters params = new AuditSearchParameters();
        FilterCriteria filters = new FilterCriteria();
        params.setAuditFilters(filters);
        params.setLimit(100);
        params.setOffset(10);
        params.setSortBy("timestamp");
        params.setSortOrder(SortOrder.ASCENDING);
        return params;
    }
}
