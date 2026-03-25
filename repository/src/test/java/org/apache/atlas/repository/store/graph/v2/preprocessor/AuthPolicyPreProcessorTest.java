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
package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for AuthPolicyPreProcessor duplicate policy name validation.
 */
public class AuthPolicyPreProcessorTest {

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private EntityDiscoveryService discoveryService;

    @Mock
    private AtlasVertex mockVertex;

    private AuthPolicyPreProcessor preProcessor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get().setUser("testUser", null);

        // Create preprocessor instance using reflection to avoid constructor initialization issues
        preProcessor = createPreprocessorInstance();

        // Inject mocked discovery service via reflection
        Field discoveryField = AuthPolicyPreProcessor.class.getDeclaredField("discovery");
        discoveryField.setAccessible(true);
        discoveryField.set(preProcessor, discoveryService);
    }

    /**
     * Create AuthPolicyPreProcessor instance via reflection without running the full constructor
     * (avoids EntityDiscoveryService initialization which requires JanusGraph)
     */
    private AuthPolicyPreProcessor createPreprocessorInstance() throws Exception {
        // Use Objenesis (bundled with Mockito) to create instance without calling constructor
        org.objenesis.Objenesis objenesis = new org.objenesis.ObjenesisStd();
        AuthPolicyPreProcessor instance = objenesis.newInstance(AuthPolicyPreProcessor.class);

        // Manually inject required fields
        setField(instance, "graph", graph);
        setField(instance, "typeRegistry", typeRegistry);
        setField(instance, "entityRetriever", entityRetriever);

        return instance;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = AuthPolicyPreProcessor.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) {
            closeable.close();
        }
    }

    // =====================================================================================
    // Test Group 1: Duplicate Policy Name Validation - Core Functionality
    // =====================================================================================

    @Test
    public void testValidateDuplicatePolicyName_Duplicate_ThrowsException() throws Exception {
        // Setup
        AtlasEntity policy = createPolicy("duplicate-policy");
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        // Mock discovery service to return existing policy (simulating duplicate)
        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.singletonList(mockVertex));

        // Execute & Verify
        try {
            preProcessor.validateDuplicatePolicyName(policy, persona);
            fail("Should have thrown AtlasBaseException for duplicate policy name");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Policy with name 'duplicate-policy' already exists"));
            assertTrue(e.getMessage().contains("Persona"));
            assertTrue(e.getMessage().contains("TestPersona"));
        }

        // Verify discovery service was called
        verify(discoveryService).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    @Test
    public void testValidateDuplicatePolicyName_Unique_Success() throws Exception {
        // Setup
        AtlasEntity policy = createPolicy("unique-policy");
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        // Mock discovery service to return empty list (no duplicates)
        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.emptyList());

        // Execute - should not throw exception
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify discovery service was called
        verify(discoveryService).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    @Test
    public void testValidateDuplicatePolicyName_NullName_SkipsValidation() throws Exception {
        // Setup
        AtlasEntity policy = createPolicy(null);
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        // Execute - should not throw exception
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify discovery service was NOT called
        verify(discoveryService, never()).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    @Test
    public void testValidateDuplicatePolicyName_EmptyName_SkipsValidation() throws Exception {
        // Setup
        AtlasEntity policy = createPolicy("");
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        // Execute - should not throw exception
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify discovery service was NOT called
        verify(discoveryService, never()).directVerticesIndexSearch(any(IndexSearchParams.class));
    }

    @Test
    public void testValidateDuplicatePolicyName_DiscoveryServiceNull_SkipsValidation() throws Exception {
        // Setup - set discovery service to null
        Field discoveryField = AuthPolicyPreProcessor.class.getDeclaredField("discovery");
        discoveryField.setAccessible(true);
        discoveryField.set(preProcessor, null);

        AtlasEntity policy = createPolicy("any-policy");
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        // Execute - should not throw exception, just log warning
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify - no exception thrown means graceful degradation worked
        // (cannot verify the warning log in unit test, but method completes successfully)
    }

    // =====================================================================================
    // Test Group 2: Elasticsearch Query Structure Validation
    // =====================================================================================

    @Test
    public void testValidateDuplicatePolicyName_VerifyESQueryStructure() throws Exception {
        // Setup
        String policyName = "test-policy";
        String personaQN = "default/persona123";

        AtlasEntity policy = createPolicy(policyName);
        AtlasEntity persona = createPersona("TestPersona", personaQN);

        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.emptyList());

        ArgumentCaptor<IndexSearchParams> paramsCaptor = ArgumentCaptor.forClass(IndexSearchParams.class);

        // Execute
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Capture and verify the ES query
        verify(discoveryService).directVerticesIndexSearch(paramsCaptor.capture());

        IndexSearchParams capturedParams = paramsCaptor.getValue();
        Map<String, Object> dsl = capturedParams.getDsl();

        // Verify DSL structure
        assertNotNull(dsl, "DSL should not be null");
        assertEquals(dsl.get("size"), 1, "Query size should be 1 for existence check");

        Map<String, Object> query = (Map<String, Object>) dsl.get("query");
        assertNotNull(query, "Query should not be null");

        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        assertNotNull(bool, "Bool clause should not be null");

        List<Map<String, Object>> filters = (List<Map<String, Object>>) bool.get("filter");
        assertEquals(filters.size(), 5, "Should have exactly 5 filter clauses");

        // Verify individual filters
        verifyFilterExists(filters, "term", "__state", "ACTIVE");
        verifyFilterExists(filters, "term", "__typeName.keyword", POLICY_ENTITY_TYPE);
        verifyFilterExists(filters, "term", "policyCategory", "persona");
        verifyFilterExists(filters, "term", "name.keyword", policyName);
        verifyPrefixFilterExists(filters, QUALIFIED_NAME, personaQN);
    }

    @Test
    public void testValidateDuplicatePolicyName_QueryUsesExactNameMatch() throws Exception {
        // Setup - verify .keyword is used for exact match
        AtlasEntity policy = createPolicy("Test-Policy");  // Mixed case
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.emptyList());

        ArgumentCaptor<IndexSearchParams> paramsCaptor = ArgumentCaptor.forClass(IndexSearchParams.class);

        // Execute
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify
        verify(discoveryService).directVerticesIndexSearch(paramsCaptor.capture());

        Map<String, Object> dsl = paramsCaptor.getValue().getDsl();
        List<Map<String, Object>> filters = getFilters(dsl);

        // Find the name filter and verify it uses .keyword
        Map<String, Object> nameFilter = filters.stream()
            .filter(f -> f.containsKey("term") && ((Map<String, Object>) f.get("term")).containsKey("name.keyword"))
            .findFirst()
            .orElse(null);

        assertNotNull(nameFilter, "Should have name.keyword filter for exact match");
        Map<String, Object> term = (Map<String, Object>) nameFilter.get("term");
        assertEquals(term.get("name.keyword"), "Test-Policy", "Should preserve case for exact match");
    }

    @Test
    public void testValidateDuplicatePolicyName_QueryScopedToPersona() throws Exception {
        // Setup - verify prefix filter scopes to specific persona
        AtlasEntity policy = createPolicy("my-policy");
        AtlasEntity persona = createPersona("Persona1", "default/persona123");

        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.emptyList());

        ArgumentCaptor<IndexSearchParams> paramsCaptor = ArgumentCaptor.forClass(IndexSearchParams.class);

        // Execute
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify
        verify(discoveryService).directVerticesIndexSearch(paramsCaptor.capture());

        Map<String, Object> dsl = paramsCaptor.getValue().getDsl();
        List<Map<String, Object>> filters = getFilters(dsl);

        // Find the prefix filter
        Map<String, Object> prefixFilter = filters.stream()
            .filter(f -> f.containsKey("prefix"))
            .findFirst()
            .orElse(null);

        assertNotNull(prefixFilter, "Should have prefix filter");
        Map<String, Object> prefix = (Map<String, Object>) prefixFilter.get("prefix");
        assertEquals(prefix.get(QUALIFIED_NAME), "default/persona123",
            "Should filter by parent persona's qualifiedName");
    }

    @Test
    public void testValidateDuplicatePolicyName_QueryFiltersPersonaCategory() throws Exception {
        // Setup - verify policyCategory filter is set to "persona"
        AtlasEntity policy = createPolicy("policy1");
        AtlasEntity persona = createPersona("TestPersona", "default/persona123");

        when(discoveryService.directVerticesIndexSearch(any(IndexSearchParams.class)))
            .thenReturn(Collections.emptyList());

        ArgumentCaptor<IndexSearchParams> paramsCaptor = ArgumentCaptor.forClass(IndexSearchParams.class);

        // Execute
        preProcessor.validateDuplicatePolicyName(policy, persona);

        // Verify
        verify(discoveryService).directVerticesIndexSearch(paramsCaptor.capture());

        Map<String, Object> dsl = paramsCaptor.getValue().getDsl();
        List<Map<String, Object>> filters = getFilters(dsl);

        // Find the policyCategory filter
        Map<String, Object> categoryFilter = filters.stream()
            .filter(f -> f.containsKey("term") && ((Map<String, Object>) f.get("term")).containsKey("policyCategory"))
            .findFirst()
            .orElse(null);

        assertNotNull(categoryFilter, "Should have policyCategory filter");
        Map<String, Object> term = (Map<String, Object>) categoryFilter.get("term");
        assertEquals(term.get("policyCategory"), "persona",
            "Should only search for Persona policies");
    }

    // =====================================================================================
    // Helper Methods
    // =====================================================================================

    private AtlasEntity createPolicy(String name) {
        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setAttribute(NAME, name);
        policy.setGuid("policy-guid-" + System.nanoTime());
        return policy;
    }

    private AtlasEntity createPersona(String name, String qualifiedName) {
        AtlasEntity persona = new AtlasEntity();
        persona.setTypeName(PERSONA_ENTITY_TYPE);
        persona.setAttribute(NAME, name);
        persona.setAttribute(QUALIFIED_NAME, qualifiedName);
        persona.setGuid("persona-guid-" + System.nanoTime());
        persona.setStatus(AtlasEntity.Status.ACTIVE);
        return persona;
    }

    private void verifyFilterExists(List<Map<String, Object>> filters, String filterType, String field, Object value) {
        boolean found = filters.stream()
            .anyMatch(f -> {
                if (!f.containsKey(filterType)) return false;
                Map<String, Object> filterContent = (Map<String, Object>) f.get(filterType);
                return value.equals(filterContent.get(field));
            });
        assertTrue(found, "Should have " + filterType + " filter for " + field + "=" + value);
    }

    private void verifyPrefixFilterExists(List<Map<String, Object>> filters, String field, String prefix) {
        boolean found = filters.stream()
            .anyMatch(f -> {
                if (!f.containsKey("prefix")) return false;
                Map<String, Object> prefixContent = (Map<String, Object>) f.get("prefix");
                return prefix.equals(prefixContent.get(field));
            });
        assertTrue(found, "Should have prefix filter for " + field + " starting with " + prefix);
    }

    private List<Map<String, Object>> getFilters(Map<String, Object> dsl) {
        Map<String, Object> query = (Map<String, Object>) dsl.get("query");
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        return (List<Map<String, Object>>) bool.get("filter");
    }
}
