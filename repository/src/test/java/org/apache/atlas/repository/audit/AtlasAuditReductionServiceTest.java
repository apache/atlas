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
package org.apache.atlas.repository.audit;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.model.audit.AuditReductionCriteria;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.AUDIT_AGING_ACTION_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_COUNT_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_ENTITY_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_SUBTYPES_INCLUDED_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TTL_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TYPE_KEY;
import static org.apache.atlas.repository.Constants.CREATE_EVENTS_AGEOUT_ALLOWED_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasAuditReductionServiceTest {
    @Mock
    private Configuration mockConfiguration;

    @Mock
    private AtlasGraph mockGraph;

    @Mock
    private AtlasDiscoveryService mockDiscoveryService;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    private AtlasAuditReductionService auditReductionService;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        auditReductionService = new AtlasAuditReductionService(mockConfiguration, mockGraph, mockDiscoveryService, mockTypeRegistry);
    }

    @Test
    public void testConstructor() {
        assertNotNull(auditReductionService);
    }

    @Test
    public void testStartAuditAgingByConfigWithEnabledAging() throws Exception {
        // Given
        setupMockConfiguration();
        AtlasTask mockTask = mock(AtlasTask.class);
        when(mockDiscoveryService.createAndQueueAuditReductionTask(any(Map.class), anyString())).thenReturn(mockTask);
    }

    @Test
    public void testStartAuditAgingByConfigWithException() throws Exception {
        // Given
        setupMockConfiguration();
        when(mockDiscoveryService.createAndQueueAuditReductionTask(any(Map.class), anyString()))
                .thenThrow(new RuntimeException("Test exception"));

        // When
        List<AtlasTask> result = auditReductionService.startAuditAgingByConfig();

        // Then
        assertNull(result);
    }

    @Test
    public void testStartAuditAgingByCriteriaWithValidCriteria() throws Exception {
        // Given
        Map<String, Object> criteria1 = createTestCriteria(AtlasAuditAgingType.DEFAULT);
        Map<String, Object> criteria2 = createTestCriteria(AtlasAuditAgingType.CUSTOM);
        List<Map<String, Object>> criteriaList = Arrays.asList(criteria1, criteria2);

        AtlasTask mockTask1 = mock(AtlasTask.class);
        AtlasTask mockTask2 = mock(AtlasTask.class);
        when(mockDiscoveryService.createAndQueueAuditReductionTask(eq(criteria1), anyString())).thenReturn(mockTask1);
        when(mockDiscoveryService.createAndQueueAuditReductionTask(eq(criteria2), anyString())).thenReturn(mockTask2);

        // When
        List<AtlasTask> result = auditReductionService.startAuditAgingByCriteria(criteriaList);

        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertTrue(result.contains(mockTask1));
        assertTrue(result.contains(mockTask2));
    }

    @Test
    public void testStartAuditAgingByCriteriaWithEmptyList() {
        // When
        List<AtlasTask> result = auditReductionService.startAuditAgingByCriteria(null);

        // Then
        assertNull(result);
    }

    @Test
    public void testStartAuditAgingByCriteriaWithException() throws Exception {
        // Given
        Map<String, Object> criteria = createTestCriteria(AtlasAuditAgingType.DEFAULT);
        List<Map<String, Object>> criteriaList = Arrays.asList(criteria);
        when(mockDiscoveryService.createAndQueueAuditReductionTask(any(Map.class), anyString()))
                .thenThrow(new RuntimeException("Test exception"));

        // When
        List<AtlasTask> result = auditReductionService.startAuditAgingByCriteria(criteriaList);

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildAgeoutCriteriaForAllAgingTypesWithNullCriteria() {
        // When
        List<Map<String, Object>> result = auditReductionService.buildAgeoutCriteriaForAllAgingTypes(null);

        // Then
        assertNull(result);
    }

    @Test
    public void testBuildAgeoutCriteriaForAllAgingTypesWithDisabledAging() {
        // Given
        AuditReductionCriteria criteria = new AuditReductionCriteria();
        criteria.setAuditAgingEnabled(false);

        // When
        List<Map<String, Object>> result = auditReductionService.buildAgeoutCriteriaForAllAgingTypes(criteria);

        // Then
        assertNull(result);
    }

    @Test
    public void testBuildAgeoutCriteriaForAllAgingTypesWithDefaultAging() {
        // Given
        AuditReductionCriteria criteria = createValidAuditReductionCriteria();
        criteria.setDefaultAgeoutEnabled(true);
        criteria.setDefaultAgeoutTTLInDays(30);
        criteria.setDefaultAgeoutAuditCount(100);

        // When
        List<Map<String, Object>> result = auditReductionService.buildAgeoutCriteriaForAllAgingTypes(criteria);

        // Then
        assertNotNull(result);
        assertFalse(result.isEmpty());

        // Find default aging criteria
        Map<String, Object> defaultCriteria = result.stream()
                .filter(map -> AtlasAuditAgingType.DEFAULT.equals(map.get(AUDIT_AGING_TYPE_KEY)))
                .findFirst()
                .orElse(null);

        assertNotNull(defaultCriteria);
        assertEquals(defaultCriteria.get(AUDIT_AGING_TTL_KEY), 30);
        assertEquals(defaultCriteria.get(AUDIT_AGING_COUNT_KEY), 100);
    }

    @Test
    public void testBuildAgeoutCriteriaForAllAgingTypesWithCustomAging() {
        // Given
        AuditReductionCriteria criteria = createValidAuditReductionCriteria();
        criteria.setCustomAgeoutTTLInDays(15);
        criteria.setCustomAgeoutAuditCount(50);
        criteria.setCustomAgeoutEntityTypes("DataSet,Table");
        criteria.setCustomAgeoutActionTypes("ENTITY_CREATE,ENTITY_UPDATE");

        // When
        List<Map<String, Object>> result = auditReductionService.buildAgeoutCriteriaForAllAgingTypes(criteria);

        // Then
        assertNotNull(result);
        assertFalse(result.isEmpty());

        // Find custom aging criteria
        Map<String, Object> customCriteria = result.stream()
                .filter(map -> AtlasAuditAgingType.CUSTOM.equals(map.get(AUDIT_AGING_TYPE_KEY)))
                .findFirst()
                .orElse(null);

        assertNotNull(customCriteria);
        assertEquals(customCriteria.get(AUDIT_AGING_TTL_KEY), 15);
        assertEquals(customCriteria.get(AUDIT_AGING_COUNT_KEY), 50);
    }

    @Test
    public void testBuildAgeoutCriteriaForAllAgingTypesWithSweepOut() {
        // Given
        AuditReductionCriteria criteria = createValidAuditReductionCriteria();
        criteria.setAuditSweepoutEnabled(true);
        criteria.setSweepoutEntityTypes("TempTable");
        criteria.setSweepoutActionTypes("ENTITY_DELETE");

        // When
        List<Map<String, Object>> result = auditReductionService.buildAgeoutCriteriaForAllAgingTypes(criteria);

        // Then
        assertNotNull(result);
        assertFalse(result.isEmpty());

        // Find sweep aging criteria
        Map<String, Object> sweepCriteria = result.stream()
                .filter(map -> AtlasAuditAgingType.SWEEP.equals(map.get(AUDIT_AGING_TYPE_KEY)))
                .findFirst()
                .orElse(null);

        assertNotNull(sweepCriteria);
        assertEquals(sweepCriteria.get(AUDIT_AGING_TTL_KEY), 0);
        assertEquals(sweepCriteria.get(AUDIT_AGING_COUNT_KEY), 0);
    }

    @Test
    public void testBuildAgeoutCriteriaWithIgnoreDefaultTTL() {
        // Given
        AuditReductionCriteria criteria = createValidAuditReductionCriteria();
        criteria.setDefaultAgeoutEnabled(true);
        criteria.setIgnoreDefaultAgeoutTTL(true);
        criteria.setDefaultAgeoutAuditCount(100);

        // When
        List<Map<String, Object>> result = auditReductionService.buildAgeoutCriteriaForAllAgingTypes(criteria);

        // Then
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    public void testConfigureTasksWithDisabledAging() throws Exception {
        // Given
        ScheduledTaskRegistrar mockRegistrar = mock(ScheduledTaskRegistrar.class);

        // When
        auditReductionService.configureTasks(mockRegistrar);
    }

    @Test
    public void testConvertConfigToAuditReductionCriteria() throws Exception {
        // Given
        setupMockConfigurationForConversion();
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("convertConfigToAuditReductionCriteria");
        method.setAccessible(true);

        // When
        AuditReductionCriteria result = (AuditReductionCriteria) method.invoke(auditReductionService);

        // Then
        assertNotNull(result);
    }

    @Test
    public void testGetAgeoutCriteriaMap() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getAgeoutCriteriaMap",
                AtlasAuditAgingType.class, int.class, int.class, Set.class, Set.class, boolean.class, boolean.class);
        method.setAccessible(true);

        // When
        Map<String, Object> result = (Map<String, Object>) method.invoke(auditReductionService, AtlasAuditAgingType.DEFAULT, 30, 100, java.util.Collections.singleton("DataSet"), java.util.Collections.singleton("ENTITY_CREATE"), true, false);

        // Then
        assertNotNull(result);
        assertEquals(result.get(AUDIT_AGING_TYPE_KEY), AtlasAuditAgingType.DEFAULT);
        assertEquals(result.get(AUDIT_AGING_TTL_KEY), 30);
        assertEquals(result.get(AUDIT_AGING_COUNT_KEY), 100);
        assertEquals(result.get(CREATE_EVENTS_AGEOUT_ALLOWED_KEY), true);
        assertEquals(result.get(AUDIT_AGING_SUBTYPES_INCLUDED_KEY), false);
    }

    @Test
    public void testGetGuaranteedMinValueOf() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getGuaranteedMinValueOf",
                AtlasConfiguration.class, int.class, int.class);
        method.setAccessible(true);

        // When - value less than minimum
        int result1 = (Integer) method.invoke(auditReductionService,
                AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_TTL, 5, 10);

        // When - value greater than minimum
        int result2 = (Integer) method.invoke(auditReductionService,
                AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_TTL, 15, 10);

        // Then
        assertEquals(result1, 10); // Should return minimum value
        assertEquals(result2, 15); // Should return configured value
    }

    @Test
    public void testGetStringOf() throws Exception {
        // Given
        when(mockConfiguration.getList("test.property")).thenReturn(Arrays.asList("value1", "value2", "value3"));

        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getStringOf", String.class);
        method.setAccessible(true);

        // When
        String result = (String) method.invoke(auditReductionService, "test.property");

        // Then
        assertNotNull(result);
        assertEquals(result, "value1,value2,value3");
    }

    @Test
    public void testGetStringOfWithEmptyProperty() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getStringOf", String.class);
        method.setAccessible(true);

        // When
        String result = (String) method.invoke(auditReductionService, "");

        // Then
        assertNull(result);
    }

    @Test
    public void testGetUniqueListOf() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getUniqueListOf", String.class);
        method.setAccessible(true);

        // When
        Set<String> result = (Set<String>) method.invoke(auditReductionService, "value1,value2,value1,value3");

        // Then
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertTrue(result.contains("value1"));
        assertTrue(result.contains("value2"));
        assertTrue(result.contains("value3"));
    }

    @Test
    public void testGetUniqueListOfWithEmptyValue() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getUniqueListOf", String.class);
        method.setAccessible(true);

        // When
        Set<String> result = (Set<String>) method.invoke(auditReductionService, "");

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetValidActionTypes() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getValidActionTypes",
                AtlasAuditAgingType.class, Set.class);
        method.setAccessible(true);

        Set<String> inputActionTypes = java.util.Collections.singleton("ENTITY_CREATE");

        // When
        Set<String> result = (Set<String>) method.invoke(auditReductionService,
                AtlasAuditAgingType.DEFAULT, inputActionTypes);

        // Then
        assertNotNull(result);
        assertTrue(result.contains("ENTITY_CREATE"));
    }

    @Test
    public void testGetValidActionTypesWithWildcard() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getValidActionTypes",
                AtlasAuditAgingType.class, Set.class);
        method.setAccessible(true);

        Set<String> inputActionTypes = java.util.Collections.singleton("ENTITY*");

        // When
        Set<String> result = (Set<String>) method.invoke(auditReductionService,
                AtlasAuditAgingType.DEFAULT, inputActionTypes);

        // Then
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    public void testGetValidActionTypesWithInvalidAction() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getValidActionTypes",
                AtlasAuditAgingType.class, Set.class);
        method.setAccessible(true);

        Set<String> inputActionTypes = java.util.Collections.singleton("INVALID_ACTION");

        try {
            method.invoke(auditReductionService, AtlasAuditAgingType.DEFAULT, inputActionTypes);
            assertTrue(false, "Expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testGetValidActionTypesWithEmptySet() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getValidActionTypes",
                AtlasAuditAgingType.class, Set.class);
        method.setAccessible(true);

        // When
        Set<String> result = (Set<String>) method.invoke(auditReductionService,
                AtlasAuditAgingType.DEFAULT, java.util.Collections.emptySet());

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetAuditAgingFrequencyInMillis() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getAuditAgingFrequencyInMillis");
        method.setAccessible(true);

        // When
        long result = (Long) method.invoke(auditReductionService);

        // Then
        assertTrue(result > 0);
    }

    @Test
    public void testGetAuditAgingInitialDelayInMillis() throws Exception {
        // Given
        Method method = AtlasAuditReductionService.class.getDeclaredMethod("getAuditAgingInitialDelayInMillis");
        method.setAccessible(true);

        // When
        long result = (Long) method.invoke(auditReductionService);

        // Then
        assertTrue(result > 0);
    }

    private void setupMockConfiguration() {
        // Mock configuration values needed for aging
        when(mockConfiguration.getList(ArgumentMatchers.anyString())).thenReturn(Arrays.asList("value1", "value2"));
    }

    private void setupMockConfigurationForConversion() {
        when(mockConfiguration.getList("atlas.audit.custom.ageout.entity.types")).thenReturn(Arrays.asList("DataSet"));
        when(mockConfiguration.getList("atlas.audit.custom.ageout.action.types")).thenReturn(Arrays.asList("ENTITY_CREATE"));
        when(mockConfiguration.getList("atlas.audit.sweep.out.entity.types")).thenReturn(Arrays.asList("TempTable"));
        when(mockConfiguration.getList("atlas.audit.sweep.out.action.types")).thenReturn(Arrays.asList("ENTITY_DELETE"));
    }

    private Map<String, Object> createTestCriteria(AtlasAuditAgingType agingType) {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put(AUDIT_AGING_TYPE_KEY, agingType);
        criteria.put(AUDIT_AGING_TTL_KEY, 30);
        criteria.put(AUDIT_AGING_COUNT_KEY, 100);
        criteria.put(AUDIT_AGING_ENTITY_TYPES_KEY, java.util.Collections.singleton("DataSet"));
        criteria.put(AUDIT_AGING_ACTION_TYPES_KEY, java.util.Collections.singleton("ENTITY_CREATE"));
        criteria.put(CREATE_EVENTS_AGEOUT_ALLOWED_KEY, true);
        criteria.put(AUDIT_AGING_SUBTYPES_INCLUDED_KEY, false);
        return criteria;
    }

    private AuditReductionCriteria createValidAuditReductionCriteria() {
        AuditReductionCriteria criteria = new AuditReductionCriteria();
        criteria.setAuditAgingEnabled(true);
        criteria.setCreateEventsAgeoutAllowed(true);
        criteria.setSubTypesIncluded(false);
        criteria.setIgnoreDefaultAgeoutTTL(false);
        criteria.setDefaultAgeoutEnabled(false);
        criteria.setDefaultAgeoutTTLInDays(0);
        criteria.setDefaultAgeoutAuditCount(0);
        criteria.setCustomAgeoutTTLInDays(0);
        criteria.setCustomAgeoutAuditCount(0);
        criteria.setAuditSweepoutEnabled(false);
        return criteria;
    }
}
