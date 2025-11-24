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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAuditReductionCriteria {
    private AuditReductionCriteria criteria;

    @BeforeMethod
    public void setUp() {
        criteria = new AuditReductionCriteria();
    }

    @Test
    public void testDefaultConstructor() {
        AuditReductionCriteria newCriteria = new AuditReductionCriteria();

        assertFalse(newCriteria.isAuditAgingEnabled());
        assertFalse(newCriteria.isDefaultAgeoutEnabled());
        assertFalse(newCriteria.isAuditSweepoutEnabled());
        assertFalse(newCriteria.isCreateEventsAgeoutAllowed());
        assertFalse(newCriteria.isSubTypesIncluded());
        assertFalse(newCriteria.ignoreDefaultAgeoutTTL());
        assertEquals(newCriteria.getDefaultAgeoutAuditCount(), 0);
        assertEquals(newCriteria.getDefaultAgeoutTTLInDays(), 0);
        assertEquals(newCriteria.getCustomAgeoutAuditCount(), 0);
        assertEquals(newCriteria.getCustomAgeoutTTLInDays(), 0);
        assertNull(newCriteria.getCustomAgeoutEntityTypes());
        assertNull(newCriteria.getCustomAgeoutActionTypes());
        assertNull(newCriteria.getSweepoutEntityTypes());
        assertNull(newCriteria.getSweepoutActionTypes());
    }

    @Test
    public void testAuditAgingEnabledGetterSetter() {
        assertFalse(criteria.isAuditAgingEnabled());

        criteria.setAuditAgingEnabled(true);
        assertTrue(criteria.isAuditAgingEnabled());

        criteria.setAuditAgingEnabled(false);
        assertFalse(criteria.isAuditAgingEnabled());
    }

    @Test
    public void testDefaultAgeoutEnabledGetterSetter() {
        assertFalse(criteria.isDefaultAgeoutEnabled());

        criteria.setDefaultAgeoutEnabled(true);
        assertTrue(criteria.isDefaultAgeoutEnabled());

        criteria.setDefaultAgeoutEnabled(false);
        assertFalse(criteria.isDefaultAgeoutEnabled());
    }

    @Test
    public void testAuditSweepoutEnabledGetterSetter() {
        assertFalse(criteria.isAuditSweepoutEnabled());

        criteria.setAuditSweepoutEnabled(true);
        assertTrue(criteria.isAuditSweepoutEnabled());

        criteria.setAuditSweepoutEnabled(false);
        assertFalse(criteria.isAuditSweepoutEnabled());
    }

    @Test
    public void testCreateEventsAgeoutAllowedGetterSetter() {
        assertFalse(criteria.isCreateEventsAgeoutAllowed());

        criteria.setCreateEventsAgeoutAllowed(true);
        assertTrue(criteria.isCreateEventsAgeoutAllowed());

        criteria.setCreateEventsAgeoutAllowed(false);
        assertFalse(criteria.isCreateEventsAgeoutAllowed());
    }

    @Test
    public void testSubTypesIncludedGetterSetter() {
        assertFalse(criteria.isSubTypesIncluded());

        criteria.setSubTypesIncluded(true);
        assertTrue(criteria.isSubTypesIncluded());

        criteria.setSubTypesIncluded(false);
        assertFalse(criteria.isSubTypesIncluded());
    }

    @Test
    public void testIgnoreDefaultAgeoutTTLGetterSetter() {
        assertFalse(criteria.ignoreDefaultAgeoutTTL());

        criteria.setIgnoreDefaultAgeoutTTL(true);
        assertTrue(criteria.ignoreDefaultAgeoutTTL());

        criteria.setIgnoreDefaultAgeoutTTL(false);
        assertFalse(criteria.ignoreDefaultAgeoutTTL());
    }

    @Test
    public void testDefaultAgeoutTTLInDaysGetterSetter() {
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), 0);

        criteria.setDefaultAgeoutTTLInDays(30);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), 30);

        criteria.setDefaultAgeoutTTLInDays(-1);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), -1);

        criteria.setDefaultAgeoutTTLInDays(Integer.MAX_VALUE);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), Integer.MAX_VALUE);
    }

    @Test
    public void testDefaultAgeoutAuditCountGetterSetter() {
        assertEquals(criteria.getDefaultAgeoutAuditCount(), 0);

        criteria.setDefaultAgeoutAuditCount(1000);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), 1000);

        criteria.setDefaultAgeoutAuditCount(-1);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), -1);

        criteria.setDefaultAgeoutAuditCount(Integer.MAX_VALUE);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), Integer.MAX_VALUE);
    }

    @Test
    public void testCustomAgeoutTTLInDaysGetterSetter() {
        assertEquals(criteria.getCustomAgeoutTTLInDays(), 0);

        criteria.setCustomAgeoutTTLInDays(60);
        assertEquals(criteria.getCustomAgeoutTTLInDays(), 60);

        criteria.setCustomAgeoutTTLInDays(-1);
        assertEquals(criteria.getCustomAgeoutTTLInDays(), -1);

        criteria.setCustomAgeoutTTLInDays(Integer.MAX_VALUE);
        assertEquals(criteria.getCustomAgeoutTTLInDays(), Integer.MAX_VALUE);
    }

    @Test
    public void testCustomAgeoutAuditCountGetterSetter() {
        assertEquals(criteria.getCustomAgeoutAuditCount(), 0);

        criteria.setCustomAgeoutAuditCount(500);
        assertEquals(criteria.getCustomAgeoutAuditCount(), 500);

        criteria.setCustomAgeoutAuditCount(-1);
        assertEquals(criteria.getCustomAgeoutAuditCount(), -1);

        criteria.setCustomAgeoutAuditCount(Integer.MAX_VALUE);
        assertEquals(criteria.getCustomAgeoutAuditCount(), Integer.MAX_VALUE);
    }

    @Test
    public void testCustomAgeoutEntityTypesGetterSetter() {
        assertNull(criteria.getCustomAgeoutEntityTypes());

        String entityTypes = "DataSet,Table,Database";
        criteria.setCustomAgeoutEntityTypes(entityTypes);
        assertEquals(criteria.getCustomAgeoutEntityTypes(), entityTypes);

        criteria.setCustomAgeoutEntityTypes("");
        assertEquals(criteria.getCustomAgeoutEntityTypes(), "");

        criteria.setCustomAgeoutEntityTypes(null);
        assertNull(criteria.getCustomAgeoutEntityTypes());
    }

    @Test
    public void testCustomAgeoutActionTypesGetterSetter() {
        assertNull(criteria.getCustomAgeoutActionTypes());

        String actionTypes = "CREATE,UPDATE,DELETE";
        criteria.setCustomAgeoutActionTypes(actionTypes);
        assertEquals(criteria.getCustomAgeoutActionTypes(), actionTypes);

        criteria.setCustomAgeoutActionTypes("");
        assertEquals(criteria.getCustomAgeoutActionTypes(), "");

        criteria.setCustomAgeoutActionTypes(null);
        assertNull(criteria.getCustomAgeoutActionTypes());
    }

    @Test
    public void testSweepoutEntityTypesGetterSetter() {
        assertNull(criteria.getSweepoutEntityTypes());

        String entityTypes = "Process,Pipeline";
        criteria.setSweepoutEntityTypes(entityTypes);
        assertEquals(criteria.getSweepoutEntityTypes(), entityTypes);

        criteria.setSweepoutEntityTypes("");
        assertEquals(criteria.getSweepoutEntityTypes(), "");

        criteria.setSweepoutEntityTypes(null);
        assertNull(criteria.getSweepoutEntityTypes());
    }

    @Test
    public void testSweepoutActionTypesGetterSetter() {
        assertNull(criteria.getSweepoutActionTypes());

        String actionTypes = "PURGE,EXPORT";
        criteria.setSweepoutActionTypes(actionTypes);
        assertEquals(criteria.getSweepoutActionTypes(), actionTypes);

        criteria.setSweepoutActionTypes("");
        assertEquals(criteria.getSweepoutActionTypes(), "");

        criteria.setSweepoutActionTypes(null);
        assertNull(criteria.getSweepoutActionTypes());
    }

    @Test
    public void testHashCodeConsistency() {
        int hashCode1 = criteria.hashCode();
        int hashCode2 = criteria.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AuditReductionCriteria criteria1 = new AuditReductionCriteria();
        AuditReductionCriteria criteria2 = new AuditReductionCriteria();

        criteria1.setAuditAgingEnabled(true);
        criteria1.setDefaultAgeoutTTLInDays(30);

        criteria2.setAuditAgingEnabled(true);
        criteria2.setDefaultAgeoutTTLInDays(30);

        assertEquals(criteria1.hashCode(), criteria2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AuditReductionCriteria criteria1 = new AuditReductionCriteria();
        AuditReductionCriteria criteria2 = new AuditReductionCriteria();

        criteria1.setAuditAgingEnabled(true);
        criteria2.setAuditAgingEnabled(false);

        assertNotEquals(criteria1.hashCode(), criteria2.hashCode());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(criteria.equals(criteria));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(criteria.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(criteria.equals("not a criteria"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AuditReductionCriteria criteria1 = new AuditReductionCriteria();
        AuditReductionCriteria criteria2 = new AuditReductionCriteria();

        criteria1.setAuditAgingEnabled(true);
        criteria1.setDefaultAgeoutTTLInDays(30);
        criteria1.setCustomAgeoutEntityTypes("Table");

        criteria2.setAuditAgingEnabled(true);
        criteria2.setDefaultAgeoutTTLInDays(30);
        criteria2.setCustomAgeoutEntityTypes("Table");

        assertTrue(criteria1.equals(criteria2));
        assertTrue(criteria2.equals(criteria1));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        AuditReductionCriteria criteria1 = new AuditReductionCriteria();
        AuditReductionCriteria criteria2 = new AuditReductionCriteria();

        criteria1.setAuditAgingEnabled(true);
        criteria2.setAuditAgingEnabled(false);

        assertFalse(criteria1.equals(criteria2));
        assertFalse(criteria2.equals(criteria1));
    }

    @Test
    public void testEqualsWithAllFields() {
        AuditReductionCriteria criteria1 = createFullyConfiguredCriteria();
        AuditReductionCriteria criteria2 = createFullyConfiguredCriteria();

        assertTrue(criteria1.equals(criteria2));
        assertTrue(criteria2.equals(criteria1));
    }

    @Test
    public void testToString() {
        criteria.setAuditAgingEnabled(true);
        criteria.setDefaultAgeoutTTLInDays(30);
        criteria.setCustomAgeoutEntityTypes("Table,Database");

        String result = criteria.toString();

        assertNotNull(result);
        assertTrue(result.contains("auditAgingEnabled"));
        assertTrue(result.contains("true"));
        assertTrue(result.contains("30"));
        assertTrue(result.contains("Table,Database"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        criteria.setAuditAgingEnabled(true);
        criteria.setDefaultAgeoutTTLInDays(30);

        StringBuilder sb = new StringBuilder();
        StringBuilder result = criteria.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("auditAgingEnabled"));
        assertTrue(result.toString().contains("true"));
        assertTrue(result.toString().contains("30"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        criteria.setAuditAgingEnabled(true);

        StringBuilder result = criteria.toString(null);

        assertNotNull(result);
        assertTrue(result.toString().contains("auditAgingEnabled"));
        assertTrue(result.toString().contains("true"));
    }

    @Test
    public void testToStringWithAllNullValues() {
        String result = criteria.toString();

        assertNotNull(result);
        assertTrue(result.contains("auditAgingEnabled"));
        assertTrue(result.contains("false"));
        assertTrue(result.contains("null"));
    }

    @Test
    public void testSerializable() {
        assertNotNull(criteria);
    }

    @Test
    public void testCompleteConfiguration() {
        AuditReductionCriteria fullCriteria = createFullyConfiguredCriteria();

        assertTrue(fullCriteria.isAuditAgingEnabled());
        assertTrue(fullCriteria.isDefaultAgeoutEnabled());
        assertTrue(fullCriteria.isAuditSweepoutEnabled());
        assertTrue(fullCriteria.isCreateEventsAgeoutAllowed());
        assertTrue(fullCriteria.isSubTypesIncluded());
        assertTrue(fullCriteria.ignoreDefaultAgeoutTTL());
        assertEquals(fullCriteria.getDefaultAgeoutAuditCount(), 1000);
        assertEquals(fullCriteria.getDefaultAgeoutTTLInDays(), 30);
        assertEquals(fullCriteria.getCustomAgeoutAuditCount(), 500);
        assertEquals(fullCriteria.getCustomAgeoutTTLInDays(), 60);
        assertEquals(fullCriteria.getCustomAgeoutEntityTypes(), "Table,Database");
        assertEquals(fullCriteria.getCustomAgeoutActionTypes(), "CREATE,UPDATE");
        assertEquals(fullCriteria.getSweepoutEntityTypes(), "Process");
        assertEquals(fullCriteria.getSweepoutActionTypes(), "PURGE");
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AuditReductionCriteria criteria1 = createFullyConfiguredCriteria();
        AuditReductionCriteria criteria2 = createFullyConfiguredCriteria();

        // Test equals contract
        assertTrue(criteria1.equals(criteria2));
        assertTrue(criteria2.equals(criteria1));
        assertEquals(criteria1.hashCode(), criteria2.hashCode());

        // Test reflexivity
        assertTrue(criteria1.equals(criteria1));

        // Test consistency
        assertTrue(criteria1.equals(criteria2));
        assertTrue(criteria1.equals(criteria2));
    }

    @Test
    public void testBooleanFieldsIndependence() {
        // Test that boolean fields can be set independently
        criteria.setAuditAgingEnabled(true);
        assertFalse(criteria.isDefaultAgeoutEnabled());
        assertFalse(criteria.isAuditSweepoutEnabled());

        criteria.setDefaultAgeoutEnabled(true);
        assertTrue(criteria.isAuditAgingEnabled());
        assertFalse(criteria.isAuditSweepoutEnabled());

        criteria.setAuditSweepoutEnabled(true);
        assertTrue(criteria.isAuditAgingEnabled());
        assertTrue(criteria.isDefaultAgeoutEnabled());
    }

    @Test
    public void testIntegerFieldsEdgeCases() {
        // Test with zero values
        criteria.setDefaultAgeoutTTLInDays(0);
        criteria.setDefaultAgeoutAuditCount(0);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), 0);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), 0);

        // Test with negative values
        criteria.setDefaultAgeoutTTLInDays(-100);
        criteria.setDefaultAgeoutAuditCount(-500);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), -100);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), -500);

        // Test with maximum values
        criteria.setDefaultAgeoutTTLInDays(Integer.MAX_VALUE);
        criteria.setDefaultAgeoutAuditCount(Integer.MAX_VALUE);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), Integer.MAX_VALUE);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), Integer.MAX_VALUE);

        // Test with minimum values
        criteria.setDefaultAgeoutTTLInDays(Integer.MIN_VALUE);
        criteria.setDefaultAgeoutAuditCount(Integer.MIN_VALUE);
        assertEquals(criteria.getDefaultAgeoutTTLInDays(), Integer.MIN_VALUE);
        assertEquals(criteria.getDefaultAgeoutAuditCount(), Integer.MIN_VALUE);
    }

    @Test
    public void testStringFieldsWithSpecialCharacters() {
        String specialChars = "Table,Database;Process|Pipeline:Entity*Type";
        criteria.setCustomAgeoutEntityTypes(specialChars);
        assertEquals(criteria.getCustomAgeoutEntityTypes(), specialChars);

        String unicodeChars = "表格,数据库";
        criteria.setCustomAgeoutActionTypes(unicodeChars);
        assertEquals(criteria.getCustomAgeoutActionTypes(), unicodeChars);
    }

    private AuditReductionCriteria createFullyConfiguredCriteria() {
        AuditReductionCriteria fullCriteria = new AuditReductionCriteria();
        fullCriteria.setAuditAgingEnabled(true);
        fullCriteria.setDefaultAgeoutEnabled(true);
        fullCriteria.setAuditSweepoutEnabled(true);
        fullCriteria.setCreateEventsAgeoutAllowed(true);
        fullCriteria.setSubTypesIncluded(true);
        fullCriteria.setIgnoreDefaultAgeoutTTL(true);
        fullCriteria.setDefaultAgeoutAuditCount(1000);
        fullCriteria.setDefaultAgeoutTTLInDays(30);
        fullCriteria.setCustomAgeoutAuditCount(500);
        fullCriteria.setCustomAgeoutTTLInDays(60);
        fullCriteria.setCustomAgeoutEntityTypes("Table,Database");
        fullCriteria.setCustomAgeoutActionTypes("CREATE,UPDATE");
        fullCriteria.setSweepoutEntityTypes("Process");
        fullCriteria.setSweepoutActionTypes("PURGE");
        return fullCriteria;
    }
}
