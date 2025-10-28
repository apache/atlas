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
package org.apache.atlas.model.instance;

import org.apache.atlas.model.instance.AtlasCheckStateResult.AtlasEntityState;
import org.apache.atlas.model.instance.AtlasCheckStateResult.State;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

public class TestAtlasCheckStateResult {
    @Test
    public void testConstructor() {
        AtlasCheckStateResult result = new AtlasCheckStateResult();
        assertNotNull(result);
        assertEquals(0, result.getEntitiesScanned());
        assertEquals(0, result.getEntitiesOk());
        assertEquals(0, result.getEntitiesFixed());
        assertEquals(0, result.getEntitiesPartiallyFixed());
        assertEquals(0, result.getEntitiesNotFixed());
        assertEquals(State.OK, result.getState()); // default state
        assertNull(result.getEntities());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        result.setEntitiesScanned(100);
        assertEquals(100, result.getEntitiesScanned());

        result.setEntitiesOk(80);
        assertEquals(80, result.getEntitiesOk());

        result.setEntitiesFixed(10);
        assertEquals(10, result.getEntitiesFixed());

        result.setEntitiesPartiallyFixed(5);
        assertEquals(5, result.getEntitiesPartiallyFixed());

        result.setEntitiesNotFixed(5);
        assertEquals(5, result.getEntitiesNotFixed());

        result.setState(State.FIXED);
        assertEquals(State.FIXED, result.getState());

        Map<String, AtlasEntityState> entities = new HashMap<>();
        AtlasEntityState entityState = new AtlasEntityState();
        entityState.setGuid("guid1");
        entities.put("guid1", entityState);
        result.setEntities(entities);
        assertEquals(entities, result.getEntities());
    }

    @Test
    public void testIncrementMethods() {
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        result.incrEntitiesScanned();
        assertEquals(1, result.getEntitiesScanned());
        result.incrEntitiesScanned();
        result.incrEntitiesScanned();
        assertEquals(3, result.getEntitiesScanned());

        result.incrEntitiesOk();
        assertEquals(1, result.getEntitiesOk());
        result.incrEntitiesOk();
        assertEquals(2, result.getEntitiesOk());

        result.incrEntitiesFixed();
        assertEquals(1, result.getEntitiesFixed());
        result.incrEntitiesFixed();
        assertEquals(2, result.getEntitiesFixed());

        result.incrEntitiesPartiallyFixed();
        assertEquals(1, result.getEntitiesPartiallyFixed());
        result.incrEntitiesPartiallyFixed();
        assertEquals(2, result.getEntitiesPartiallyFixed());

        result.incrEntitiesNotFixed();
        assertEquals(1, result.getEntitiesNotFixed());
        result.incrEntitiesNotFixed();
        assertEquals(2, result.getEntitiesNotFixed());
    }

    @Test
    public void testStateEnum() {
        assertEquals(4, State.values().length);
        assertTrue(Arrays.asList(State.values()).contains(State.OK));
        assertTrue(Arrays.asList(State.values()).contains(State.FIXED));
        assertTrue(Arrays.asList(State.values()).contains(State.PARTIALLY_FIXED));
        assertTrue(Arrays.asList(State.values()).contains(State.NOT_FIXED));

        assertEquals("OK", State.OK.name());
        assertEquals("FIXED", State.FIXED.name());
        assertEquals("PARTIALLY_FIXED", State.PARTIALLY_FIXED.name());
        assertEquals("NOT_FIXED", State.NOT_FIXED.name());
    }

    @Test
    public void testToString() {
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        String toStringDefault = result.toString();
        assertNotNull(toStringDefault);
        assertTrue(toStringDefault.contains("AtlasCheckStateResult"));
        assertTrue(toStringDefault.contains("entitiesScanned='0"));
        assertTrue(toStringDefault.contains("entitiesFixed=0"));
        assertTrue(toStringDefault.contains("entitiesPartiallyFixed=0"));
        assertTrue(toStringDefault.contains("entitiesNotFixed=0"));
        assertTrue(toStringDefault.contains("state=OK"));
        assertTrue(toStringDefault.contains("entities=[]"));

        result.setEntitiesScanned(100);
        result.setEntitiesOk(80);
        result.setEntitiesFixed(10);
        result.setEntitiesPartiallyFixed(5);
        result.setEntitiesNotFixed(5);
        result.setState(State.PARTIALLY_FIXED);

        Map<String, AtlasEntityState> entities = new HashMap<>();
        AtlasEntityState entityState1 = new AtlasEntityState();
        entityState1.setGuid("guid1");
        entityState1.setTypeName("Table");
        entityState1.setState(State.FIXED);
        entities.put("guid1", entityState1);

        AtlasEntityState entityState2 = new AtlasEntityState();
        entityState2.setGuid("guid2");
        entityState2.setTypeName("Column");
        entityState2.setState(State.NOT_FIXED);
        entities.put("guid2", entityState2);

        result.setEntities(entities);

        String toStringWithData = result.toString();
        assertNotNull(toStringWithData);
        assertTrue(toStringWithData.contains("AtlasCheckStateResult"));
        assertTrue(toStringWithData.contains("entitiesScanned='100"));
        assertTrue(toStringWithData.contains("entitiesFixed=10"));
        assertTrue(toStringWithData.contains("entitiesPartiallyFixed=5"));
        assertTrue(toStringWithData.contains("entitiesNotFixed=5"));
        assertTrue(toStringWithData.contains("state=PARTIALLY_FIXED"));
        assertTrue(toStringWithData.contains("guid1") || toStringWithData.contains("guid2"));

        StringBuilder sb = result.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("AtlasCheckStateResult"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        result.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testAtlasEntityStateNestedClass() {
        AtlasEntityState entityState = new AtlasEntityState();
        assertNotNull(entityState);
        assertNull(entityState.getGuid());
        assertNull(entityState.getTypeName());
        assertNull(entityState.getName());
        assertNull(entityState.getStatus());
        assertEquals(State.OK, entityState.getState()); // default state
        assertNull(entityState.getIssues());

        entityState.setGuid("testGuid");
        assertEquals("testGuid", entityState.getGuid());

        entityState.setTypeName("testType");
        assertEquals("testType", entityState.getTypeName());

        entityState.setName("testName");
        assertEquals("testName", entityState.getName());

        entityState.setStatus(AtlasEntity.Status.ACTIVE);
        assertEquals(AtlasEntity.Status.ACTIVE, entityState.getStatus());

        entityState.setState(State.FIXED);
        assertEquals(State.FIXED, entityState.getState());

        List<String> issues = new ArrayList<>();
        issues.add("Issue 1");
        issues.add("Issue 2");
        entityState.setIssues(issues);
        assertEquals(issues, entityState.getIssues());
    }

    @Test
    public void testAtlasEntityStateToString() {
        AtlasEntityState entityState = new AtlasEntityState();
        entityState.setGuid("testGuid");
        entityState.setTypeName("Table");
        entityState.setName("customer_table");
        entityState.setStatus(AtlasEntity.Status.ACTIVE);
        entityState.setState(State.PARTIALLY_FIXED);

        List<String> issues = new ArrayList<>();
        issues.add("Missing required attribute");
        issues.add("Invalid relationship");
        entityState.setIssues(issues);

        String toString = entityState.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasEntityState"));
        assertTrue(toString.contains("testGuid"));
        assertTrue(toString.contains("Table"));
        assertTrue(toString.contains("customer_table"));
        assertTrue(toString.contains("ACTIVE"));
        assertTrue(toString.contains("PARTIALLY_FIXED"));
        assertTrue(toString.contains("Missing required attribute"));

        StringBuilder sb = entityState.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("AtlasEntityState"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        entityState.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testNullHandling() {
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        // Test setting null entities
        result.setEntities(null);
        assertNull(result.getEntities());

        String toString = result.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("entities=[]"));

        AtlasEntityState entityState = new AtlasEntityState();
        entityState.setGuid(null);
        entityState.setTypeName(null);
        entityState.setName(null);
        entityState.setStatus(null);
        entityState.setIssues(null);

        String entityStateToString = entityState.toString();
        assertNotNull(entityStateToString);
        assertTrue(entityStateToString.contains("AtlasEntityState"));
    }

    @Test
    public void testComplexScenario() {
        // Simulate a complex state check result
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        result.setEntitiesScanned(1000);
        result.setEntitiesOk(800);
        result.setEntitiesFixed(150);
        result.setEntitiesPartiallyFixed(30);
        result.setEntitiesNotFixed(20);
        result.setState(State.PARTIALLY_FIXED);

        // Create detailed entity states
        Map<String, AtlasEntityState> entities = new HashMap<>();

        // Entity that was completely fixed
        AtlasEntityState fixedEntity = new AtlasEntityState();
        fixedEntity.setGuid("table-guid-1");
        fixedEntity.setTypeName("hive_table");
        fixedEntity.setName("customer_data");
        fixedEntity.setStatus(AtlasEntity.Status.ACTIVE);
        fixedEntity.setState(State.FIXED);
        List<String> fixedIssues = new ArrayList<>();
        fixedIssues.add("Missing owner attribute - FIXED");
        fixedEntity.setIssues(fixedIssues);
        entities.put("table-guid-1", fixedEntity);

        // Entity that was partially fixed
        AtlasEntityState partiallyFixedEntity = new AtlasEntityState();
        partiallyFixedEntity.setGuid("table-guid-2");
        partiallyFixedEntity.setTypeName("hive_table");
        partiallyFixedEntity.setName("order_data");
        partiallyFixedEntity.setStatus(AtlasEntity.Status.ACTIVE);
        partiallyFixedEntity.setState(State.PARTIALLY_FIXED);
        List<String> partialIssues = new ArrayList<>();
        partialIssues.add("Missing owner attribute - FIXED");
        partialIssues.add("Invalid schema reference - NOT FIXED");
        partiallyFixedEntity.setIssues(partialIssues);
        entities.put("table-guid-2", partiallyFixedEntity);

        // Entity that could not be fixed
        AtlasEntityState notFixedEntity = new AtlasEntityState();
        notFixedEntity.setGuid("table-guid-3");
        notFixedEntity.setTypeName("hive_table");
        notFixedEntity.setName("legacy_data");
        notFixedEntity.setStatus(AtlasEntity.Status.DELETED);
        notFixedEntity.setState(State.NOT_FIXED);
        List<String> notFixedIssues = new ArrayList<>();
        notFixedIssues.add("Entity marked as deleted but still referenced");
        notFixedIssues.add("Orphaned relationships detected");
        notFixedEntity.setIssues(notFixedIssues);
        entities.put("table-guid-3", notFixedEntity);

        result.setEntities(entities);

        // Verify the complete result
        assertEquals(1000, result.getEntitiesScanned());
        assertEquals(State.PARTIALLY_FIXED, result.getState());
        assertEquals(3, result.getEntities().size());

        String toString = result.toString();
        assertTrue(toString.contains("customer_data"));
        assertTrue(toString.contains("order_data"));
        assertTrue(toString.contains("legacy_data"));
        assertTrue(toString.contains("FIXED"));
        assertTrue(toString.contains("PARTIALLY_FIXED"));
        assertTrue(toString.contains("NOT_FIXED"));
    }

    @Test
    public void testIncrementMethodsCombination() {
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        // Simulate processing 100 entities
        for (int i = 0; i < 100; i++) {
            result.incrEntitiesScanned();

            if (i < 70) {
                result.incrEntitiesOk();
            } else if (i < 80) {
                result.incrEntitiesFixed();
            } else if (i < 90) {
                result.incrEntitiesPartiallyFixed();
            } else {
                result.incrEntitiesNotFixed();
            }
        }

        assertEquals(100, result.getEntitiesScanned());
        assertEquals(70, result.getEntitiesOk());
        assertEquals(10, result.getEntitiesFixed());
        assertEquals(10, result.getEntitiesPartiallyFixed());
        assertEquals(10, result.getEntitiesNotFixed());
    }

    @Test
    public void testEntityStateWithEmptyIssues() {
        AtlasEntityState entityState = new AtlasEntityState();
        entityState.setGuid("guid1");
        entityState.setTypeName("Table");
        entityState.setState(State.OK);

        // Set empty issues list
        entityState.setIssues(new ArrayList<>());
        assertNotNull(entityState.getIssues());
        assertTrue(entityState.getIssues().isEmpty());

        String toString = entityState.toString();
        assertTrue(toString.contains("issues=[]"));
    }

    @Test
    public void testEntityStateWithMultipleIssues() {
        AtlasEntityState entityState = new AtlasEntityState();
        entityState.setGuid("problematic-entity");
        entityState.setTypeName("hive_table");
        entityState.setState(State.NOT_FIXED);

        List<String> issues = new ArrayList<>();
        issues.add("Relationship to deleted entity");
        issues.add("Missing required classification");
        issues.add("Invalid attribute value for 'owner'");
        issues.add("Duplicate entity detected");
        issues.add("Schema drift detected");
        entityState.setIssues(issues);

        assertEquals(5, entityState.getIssues().size());
        String toString = entityState.toString();
        assertTrue(toString.contains("Relationship to deleted entity"));
        assertTrue(toString.contains("Schema drift detected"));
    }

    @Test
    public void testRealWorldResultScenario() {
        // Simulate a real Atlas state check result after running maintenance
        AtlasCheckStateResult result = new AtlasCheckStateResult();

        // Overall statistics from a production environment
        result.setEntitiesScanned(5000);
        result.setEntitiesOk(4500);
        result.setEntitiesFixed(300);
        result.setEntitiesPartiallyFixed(150);
        result.setEntitiesNotFixed(50);
        result.setState(State.PARTIALLY_FIXED);

        // Sample of problematic entities
        Map<String, AtlasEntityState> entities = new HashMap<>();

        // Critical table that was successfully repaired
        AtlasEntityState criticalTable = new AtlasEntityState();
        criticalTable.setGuid("critical-customer-table-123");
        criticalTable.setTypeName("hive_table");
        criticalTable.setName("prod.customer_pii");
        criticalTable.setStatus(AtlasEntity.Status.ACTIVE);
        criticalTable.setState(State.FIXED);
        List<String> criticalIssues = new ArrayList<>();
        criticalIssues.add("Missing PII classification - FIXED");
        criticalIssues.add("Ownership attribute was null - FIXED");
        criticalTable.setIssues(criticalIssues);
        entities.put("critical-customer-table-123", criticalTable);

        result.setEntities(entities);

        // Verify the result makes sense
        assertTrue(result.getEntitiesScanned() > 0);
        assertTrue(result.getEntitiesOk() > result.getEntitiesFixed());
        assertTrue(result.getEntitiesFixed() > result.getEntitiesPartiallyFixed());
        assertTrue(result.getEntitiesPartiallyFixed() > result.getEntitiesNotFixed());
        assertEquals(State.PARTIALLY_FIXED, result.getState());

        String toString = result.toString();
        assertTrue(toString.contains("5000"));
        assertTrue(toString.contains("PARTIALLY_FIXED"));
        assertTrue(toString.contains("critical-customer-table-123"));
    }
}
