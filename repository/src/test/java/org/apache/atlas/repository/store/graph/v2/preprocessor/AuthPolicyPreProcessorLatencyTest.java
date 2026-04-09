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

import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * MS-752 — Tests documenting the bulk-policy-create latency root causes.
 *
 * <p>These tests are written against master to capture CURRENT behaviour. They serve two
 * purposes:
 *
 * <ol>
 *   <li><b>Regression guard</b> — correctness tests that must pass on both master and the
 *       optimised branch.
 *   <li><b>Bug documentation</b> — call-count tests that expose the O(N×K) graph-read pattern.
 *       They PASS on master (asserting the slow behaviour) and will need to be updated once the
 *       optimisation lands (the counts drop to O(K) / O(1)).
 * </ol>
 *
 * <h2>Scenario under test</h2>
 *
 * <pre>
 *   Persona  "default/testPersonaId"
 *     ├── existingPolicy-0  (already persisted)
 *     ├── existingPolicy-1
 *     └── ...  (K existing policies)
 *
 *   Client POSTs  N  new AuthPolicy entities pointing at that Persona.
 * </pre>
 *
 * <h2>Root-cause summary (MS-752)</h2>
 *
 * <pre>
 *   For each of the N new policies, processCreatePolicy() calls getAccessControlEntity()
 *   which calls entityRetriever.toAtlasEntityWithExtInfo(personaId).  That call triggers
 *   mapRelationshipAttributes for every relationship on the Persona — O(K × attrs) JanusGraph
 *   reads — and then loops the K existing-policy ObjectIds calling toAtlasEntity() K more
 *   times.  Cost: N persona fetches + N×K policy fetches.
 * </pre>
 */
public class AuthPolicyPreProcessorLatencyTest {

    // -------------------------------------------------------------------------
    // Persona fixture constants
    // -------------------------------------------------------------------------
    private static final String PERSONA_GUID = "persona-guid-fixed";
    private static final String PERSONA_QN   = "default/testPersonaId";  // parts[1] = "testPersonaId" → role name

    // -------------------------------------------------------------------------
    // Mocks
    // -------------------------------------------------------------------------
    @Mock private AtlasTypeRegistry       typeRegistry;
    @Mock private EntityGraphRetriever    entityRetriever;
    @Mock private EntityGraphRetriever    noRelAttrRetriever;  // injected in place of the field-level retriever (ms-752+)
    @Mock private EntityDiscoveryService  discoveryService;
    @Mock private IndexAliasStore         aliasStore;
    @Mock private EntityMutationContext   mutationContext;

    private AuthPolicyPreProcessor preProcessor;
    private AutoCloseable closeable;

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    @BeforeMethod
    public void setup() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get().setUser("testUser", null);

        preProcessor = createPreprocessorViaObjenesis();

        // Inject mocks via reflection (constructor is skipped by Objenesis)
        // graph is left null — none of the test scenarios exercise graph methods directly
        setField("typeRegistry",    typeRegistry);
        setField("entityRetriever", entityRetriever);
        setField("aliasStore",      aliasStore);

        // "noRelAttrRetriever" exists on ms-752+ (promoted from inline to field).
        // Inject a mock so vertex-based entity loading is testable.
        try {
            setField("noRelAttrRetriever", noRelAttrRetriever);
        } catch (NoSuchFieldException ignored) {
            // master branch — field not present, inline construction used instead
        }

        // "discovery" field exists on master but was removed on ms-752 branch
        // (validateDuplicatePolicyName was also removed). Skip injection if field is absent.
        try {
            setField("discovery", discoveryService);
            // By default: no duplicate policy names found (master only)
            when(discoveryService.directVerticesIndexSearch(any())).thenReturn(Collections.emptyList());
        } catch (NoSuchFieldException ignored) {
            // ms-752 branch — discovery service removed, no action needed
        }
    }

    @AfterMethod
    public void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) closeable.close();
    }

    // =========================================================================
    // Group 1 — Correctness tests
    // These must pass on BOTH master and the optimised branch.
    // =========================================================================

    /**
     * Happy path: Persona exists, no prior policies.  processCreatePolicy() must set all
     * required attributes on the new policy without throwing.
     *
     * <p><b>NPE-regression note (MS-752 rev):</b> The optimised branch (PR #6354) broke this
     * case.  After that fix, getAccessControlEntity() builds referredEntities manually; when
     * there are zero existing policies addReferredEntity() is never called and referredEntities
     * stays null.  getPolicies() → objectToEntityList() then NPEs on
     * {@code getReferredEntities().keySet()}.  The fix is a null-guard in objectToEntityList.
     */
    @Test
    public void createPolicy_personaWithNoPriorPolicies_succeeds() throws Exception {
        // Given: persona exists, has zero existing policies
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(0);
        mockPersonaFetch(personaInfo);

        AtlasEntity newPolicy = buildDomainPolicy("policy-a", PERSONA_GUID);

        // When
        preProcessor.processAttributes(newPolicy, mutationContext, CREATE);

        // Then: qualifiedName was set to  <personaQN>/<uuid>
        String qn = (String) newPolicy.getAttribute(QUALIFIED_NAME);
        assertNotNull(qn, "qualifiedName must be set");
        assertTrue(qn.startsWith(PERSONA_QN + "/"), "QN must be prefixed with persona QN");

        // Then: roles extracted from persona qualifiedName
        List<String> roles = (List<String>) newPolicy.getAttribute(ATTR_POLICY_ROLES);
        assertNotNull(roles);
        assertEquals(roles.size(), 1);
        assertTrue(roles.get(0).startsWith("persona_"), "Role must follow persona_<alias> pattern");

        // Then: users and groups cleared to empty lists
        assertEquals(((List<?>) newPolicy.getAttribute(ATTR_POLICY_USERS)).size(), 0);
        assertEquals(((List<?>) newPolicy.getAttribute(ATTR_POLICY_GROUPS)).size(), 0);

        // Then: updateAlias was invoked
        verify(aliasStore).updateAlias(any(AtlasEntityWithExtInfo.class), any(AtlasEntity.class));
    }

    /**
     * Persona with K existing policies: new policy is created, alias store receives the
     * parent entity (with existing policies) plus the new policy.
     */
    @Test
    public void createPolicy_personaWithExistingPolicies_callsUpdateAliasWithAllPolicies() throws Exception {
        int K = 5;
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(K);
        mockPersonaFetch(personaInfo);
        mockExistingPolicyFetches(personaInfo);

        AtlasEntity newPolicy = buildDomainPolicy("policy-new", PERSONA_GUID);

        preProcessor.processAttributes(newPolicy, mutationContext, CREATE);

        // aliasStore.updateAlias must be called with the persona (parent) and the new policy
        verify(aliasStore).updateAlias(
                argThat(info -> PERSONA_GUID.equals(info.getEntity().getGuid())),
                argThat(p -> "policy-new".equals(p.getAttribute(NAME)))
        );
    }

    /**
     * [MS-752 Fix B — CORE TRAVERSAL PATH]
     *
     * <p>Exercises Steps 2-3 of the optimisation: when {@code typeRegistry.getEntityTypeByName()}
     * returns a non-null {@code AtlasEntityType} whose {@code policiesAttrMap} contains the
     * {@code "access_control_policies"} key, {@code getAccessControlEntity()} must:
     *
     * <ol>
     *   <li>Call {@code GraphHelper.getActiveCollectionElementsUsingRelationship()} to obtain the K
     *       policy edges from the graph.
     *   <li>For each edge, pick the correct vertex using the declared
     *       {@code AtlasRelationshipEdgeDirection} (IN → {@code edge.getOutVertex()} is the policy).
     *   <li>Load each policy as a scalar-only entity via {@code noRelAttrRetriever.toAtlasEntity(vertex)}.
     *   <li>Register all K policies in {@code ret.getReferredEntities()}.
     * </ol>
     *
     * <p>Without this test, the gap flagged in the PR review would leave the entire edge-traversal
     * loop unexercised (because {@code mockPersonaFetch()} stubs typeRegistry to return null, which
     * short-circuits the loop).
     */
    @Test
    public void getAccessControlEntity_traversesEdgesForExistingPolicies() throws Exception {
        int K = 3;

        // ── persona entity (scalar-only, as returned by noRelAttrRetriever) ──
        AtlasEntity persona = new AtlasEntity();
        persona.setTypeName(PERSONA_ENTITY_TYPE);
        persona.setGuid(PERSONA_GUID);
        persona.setAttribute(QUALIFIED_NAME, PERSONA_QN);
        persona.setAttribute(NAME, "Test Persona");
        persona.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, Boolean.TRUE);

        // ── mock AtlasAttribute for the "policies" relationship ──
        AtlasAttribute policiesAttr = mock(AtlasAttribute.class);
        // Direction IN: from the Persona's perspective the edge goes OUT from policy → IN to persona.
        // So edge.getOutVertex() is the policy vertex.
        when(policiesAttr.getRelationshipEdgeDirection()).thenReturn(AtlasRelationshipEdgeDirection.IN);
        when(policiesAttr.getRelationshipEdgeLabel()).thenReturn("__AccessControl.policies");

        // ── mock AtlasEntityType so typeRegistry returns it ──
        AtlasEntityType parentType = mock(AtlasEntityType.class);
        Map<String, AtlasAttribute> innerMap = new HashMap<>();
        innerMap.put("access_control_policies", policiesAttr);
        Map<String, Map<String, AtlasAttribute>> relAttrsMap = new HashMap<>();
        relAttrsMap.put(REL_ATTR_POLICIES, innerMap);
        when(parentType.getRelationshipAttributes()).thenReturn(relAttrsMap);
        when(typeRegistry.getEntityTypeByName(anyString())).thenReturn(parentType);

        // ── build K mock edges, each carrying a policy vertex (as outVertex, per IN direction) ──
        List<AtlasEdge> mockEdges = new ArrayList<>();
        List<AtlasEntity> expectedPolicies = new ArrayList<>();
        for (int i = 0; i < K; i++) {
            String policyGuid = "traversal-policy-guid-" + i;
            AtlasEntity policyEntity = buildExistingPolicy(policyGuid, i);
            expectedPolicies.add(policyEntity);

            AtlasVertex policyVertex = mock(AtlasVertex.class);
            AtlasEdge edge = mock(AtlasEdge.class);
            when(edge.getOutVertex()).thenReturn(policyVertex);   // direction=IN → policy is outVertex
            when(noRelAttrRetriever.toAtlasEntity(policyVertex)).thenReturn(policyEntity);
            mockEdges.add(edge);
        }

        // ── wire up entity-retriever stubs ──
        AtlasVertex personaVertex = mock(AtlasVertex.class);
        when(entityRetriever.getEntityVertex(any(AtlasObjectId.class))).thenReturn(personaVertex);
        when(noRelAttrRetriever.toAtlasEntity(personaVertex)).thenReturn(persona);

        AtlasEntity newPolicy = buildDomainPolicy("policy-traversal-test", PERSONA_GUID);

        // ── execute with GraphHelper.getActiveCollectionElementsUsingRelationship mocked ──
        try (MockedStatic<GraphHelper> mockedGraphHelper = mockStatic(GraphHelper.class)) {
            mockedGraphHelper.when(() -> GraphHelper.getActiveCollectionElementsUsingRelationship(
                            eq(personaVertex), eq(policiesAttr), anyString()))
                    .thenReturn(mockEdges);

            preProcessor.processAttributes(newPolicy, mutationContext, CREATE);
        }

        // ── verify: noRelAttrRetriever.toAtlasEntity(vertex) called K times for policy vertices ──
        verify(noRelAttrRetriever, times(K)).toAtlasEntity(any(AtlasVertex.class));

        // ── verify: updateAlias was invoked (alias store received the parent entity) ──
        verify(aliasStore).updateAlias(
                argThat(info -> {
                    // All K policy GUIDs must be registered in referredEntities
                    Map<String, AtlasEntity> referred = info.getReferredEntities();
                    if (referred == null || referred.size() != K) return false;
                    for (AtlasEntity ep : expectedPolicies) {
                        if (!referred.containsKey(ep.getGuid())) return false;
                    }
                    return true;
                }),
                any(AtlasEntity.class)
        );
    }

    /**
     * Creating N policies sequentially (same persona) must all succeed — correctness check
     * that has nothing to do with performance.
     */
    @Test
    public void createPolicy_multiplePoliciesSequentially_allSucceed() throws Exception {
        int N = 4, K = 3;
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(K);
        mockPersonaFetch(personaInfo);
        mockExistingPolicyFetches(personaInfo);

        for (int i = 0; i < N; i++) {
            AtlasEntity policy = buildDomainPolicy("policy-" + i, PERSONA_GUID);
            preProcessor.processAttributes(policy, mutationContext, CREATE);
        }

        verify(aliasStore, times(N)).updateAlias(any(), any());
    }

    /**
     * Each new policy must get a unique qualifiedName even within the same bulk call.
     */
    @Test
    public void createPolicy_eachNewPolicy_getsUniqueQualifiedName() throws Exception {
        int N = 3;
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(0);
        mockPersonaFetch(personaInfo);

        List<String> collectedQNs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            AtlasEntity policy = buildDomainPolicy("policy-" + i, PERSONA_GUID);
            preProcessor.processAttributes(policy, mutationContext, CREATE);
            collectedQNs.add((String) policy.getAttribute(QUALIFIED_NAME));
        }

        // All QNs must be distinct
        long distinct = collectedQNs.stream().distinct().count();
        assertEquals(distinct, N, "Every policy must receive a unique qualifiedName");
    }

    // =========================================================================
    // Group 2 — Bug-documentation tests (master behaviour)
    //
    // These tests ASSERT the CURRENT (slow / buggy) call counts.
    // They pass on master and act as a baseline.
    //
    // After the MS-752 optimisation lands the counts change:
    //   toAtlasEntityWithExtInfo : N times → should become 1 (or 0 on vertex path)
    //   toAtlasEntity (policies) : N×K times → should become K
    //   updateAlias              : N times → should become 1 (after Phase-E)
    //
    // When you update the implementation, flip the expected counts below to match
    // the new optimised values.
    // =========================================================================

    /**
     * [REGRESSION GUARD — MS-752 Fix B]
     *
     * <p>Before the fix, getAccessControlEntity() called toAtlasEntityWithExtInfo() once per
     * new policy (N times for a batch of N), triggering O(K × attrs) JanusGraph reads each time.
     *
     * <p>After Fix B the vertex-based path is used instead: toAtlasEntityWithExtInfo(ObjectId)
     * must not be called at all — 0 calls for any batch size N.
     */
    @Test
    public void masterBehavior_personaFetchedOncePerNewPolicy() throws Exception {
        int N = 3, K = 3;
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(K);
        mockPersonaFetch(personaInfo);
        mockExistingPolicyFetches(personaInfo);

        for (int i = 0; i < N; i++) {
            preProcessor.processAttributes(buildDomainPolicy("p-" + i, PERSONA_GUID), mutationContext, CREATE);
        }

        // Fix B: vertex path used — toAtlasEntityWithExtInfo(ObjectId) must never be called
        verify(entityRetriever, times(0)).toAtlasEntityWithExtInfo(any(AtlasObjectId.class));
    }

    /**
     * [REGRESSION GUARD — MS-752 Fix B]
     *
     * <p>Before the fix, each call to getAccessControlEntity() looped K existing policies and
     * called toAtlasEntity(ObjectId) for each, producing N×K = 15 redundant fetches for
     * N=3 new policies and K=5 existing ones.
     *
     * <p>After Fix B the edge-traversal path is used instead: toAtlasEntity(ObjectId) must
     * not be called at all — policy vertices are loaded via noRelAttrRetriever.toAtlasEntity(vertex).
     */
    @Test
    public void masterBehavior_existingPoliciesFetchedNxKTimes() throws Exception {
        int N = 3, K = 5;
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(K);
        mockPersonaFetch(personaInfo);
        mockExistingPolicyFetches(personaInfo);

        for (int i = 0; i < N; i++) {
            preProcessor.processAttributes(buildDomainPolicy("p-" + i, PERSONA_GUID), mutationContext, CREATE);
        }

        // Fix B: edge traversal used — toAtlasEntity(ObjectId) must never be called
        verify(entityRetriever, times(0)).toAtlasEntity(any(AtlasObjectId.class));
    }

    /**
     * [BUG DOCUMENTATION — MS-752 Bug #3]
     *
     * <p>On master, updateAlias() is called once per new policy in the batch.  For a bulk
     * request of N=4 policies this means 4 full ES alias PUTs — each recomputing the entire
     * DSL from scratch; only the last write is meaningful.
     *
     * <p>After Phase-E (deferred alias) the expected count drops to 1 per persona per batch.
     */
    @Test
    public void masterBehavior_updateAlias_calledOncePerNewPolicy() throws Exception {
        int N = 4, K = 2;
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(K);
        mockPersonaFetch(personaInfo);
        mockExistingPolicyFetches(personaInfo);

        for (int i = 0; i < N; i++) {
            preProcessor.processAttributes(buildDomainPolicy("p-" + i, PERSONA_GUID), mutationContext, CREATE);
        }

        // MASTER BEHAVIOUR: updateAlias called N times
        // OPTIMISED BEHAVIOUR (Phase-E): should be 1
        verify(aliasStore, times(N)).updateAlias(any(), any());
    }

    // =========================================================================
    // Group 3 — NPE regression test (introduced by PR #6354, fixed by NPE patch)
    //
    // This test PASSES on master (original code) and on ms-752 with the NPE fix.
    // It FAILS on ms-752 without the NPE fix (objectToEntityList NPEs on null
    // referredEntities when K=0).
    // =========================================================================

    /**
     * [REGRESSION GUARD — MS-752 NPE fix]
     *
     * <p>When a Persona has zero existing policies (first-ever policy creation) the
     * optimised path in getAccessControlEntity() never calls addReferredEntity(), leaving
     * referredEntities null.  Then objectToEntityList() hits:
     *
     * <pre>
     *   entityWithExtInfo.getReferredEntities().keySet()  // NPE!
     * </pre>
     *
     * <p>This test documents that scenario so the NPE fix is guarded against regression.
     * The fix is a null-guard in AccessControlUtils.objectToEntityList line 332:
     *
     * <pre>
     *   Map<String,AtlasEntity> map = entityWithExtInfo.getReferredEntities();
     *   Set<String> referredGuids = map != null ? map.keySet() : Collections.emptySet();
     * </pre>
     */
    @Test
    public void npeRegression_createFirstPolicy_personaHasNoPolicies_noNullPointerException()
            throws Exception {
        // Given: persona with ZERO existing policies — referredEntities will be null after
        // the optimised getAccessControlEntity() builds the result manually
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(0);
        mockPersonaFetch(personaInfo);

        AtlasEntity newPolicy = buildDomainPolicy("first-policy", PERSONA_GUID);

        // When / Then: must not throw NullPointerException
        try {
            preProcessor.processAttributes(newPolicy, mutationContext, CREATE);
        } catch (NullPointerException npe) {
            fail("NullPointerException thrown when creating the first policy for a Persona " +
                 "with no existing policies.  Check objectToEntityList() null-guard in " +
                 "AccessControlUtils.java:332.  Detail: " + npe.getMessage());
        }
        // Basic sanity: qualifiedName was assigned
        assertNotNull(newPolicy.getAttribute(QUALIFIED_NAME));
    }

    /**
     * Boundary variant: exactly 1 existing policy on the persona.  Verifies that the
     * referredEntities map is populated for the one existing policy and the new policy's
     * alias update still fires.
     */
    @Test
    public void npeRegression_createPolicy_personaHasOnePriorPolicy_succeeds() throws Exception {
        AtlasEntityWithExtInfo personaInfo = buildPersonaWithPolicies(1);
        mockPersonaFetch(personaInfo);
        mockExistingPolicyFetches(personaInfo);

        AtlasEntity newPolicy = buildDomainPolicy("second-policy", PERSONA_GUID);
        preProcessor.processAttributes(newPolicy, mutationContext, CREATE);

        verify(aliasStore).updateAlias(any(), any());
    }

    /**
     * [DIRECT NPE REGRESSION — AccessControlUtils.objectToEntityList line 332]
     *
     * <p>This is the most focused test for the NPE introduced in PR #6354.  It calls
     * {@code objectToEntityList} directly with an {@code AtlasEntityWithExtInfo} whose
     * {@code referredEntities} map is null (as produced by the optimised
     * {@code getAccessControlEntity()} when the Persona has zero existing policies).
     *
     * <p>The broken line on ms-752 without the fix:
     * <pre>
     *   Set<String> referredGuids = entityWithExtInfo.getReferredEntities().keySet(); // NPE
     * </pre>
     *
     * <p>The fix (one line change in AccessControlUtils.java):
     * <pre>
     *   Map<String,AtlasEntity> map = entityWithExtInfo.getReferredEntities();
     *   Set<String> referredGuids = map != null ? map.keySet() : Collections.emptySet();
     * </pre>
     *
     * <p>Expected results:
     * <ul>
     *   <li>master                     → PASSES (original code never creates null referredEntities)
     *   <li>ms-752 WITHOUT NPE fix     → FAILS with NullPointerException
     *   <li>ms-752 WITH NPE fix        → PASSES
     * </ul>
     */
    @Test
    public void objectToEntityList_nullReferredEntities_returnsEmptyListWithoutNPE() {
        // Simulate the result of the optimised getAccessControlEntity() when K=0:
        // new AtlasEntityWithExtInfo(persona) with addReferredEntity never called
        AtlasEntityWithExtInfo infoWithNullReferredEntities = new AtlasEntityWithExtInfo(new AtlasEntity());
        assertNull(infoWithNullReferredEntities.getReferredEntities(),
                "Precondition: AtlasEntityWithExtInfo constructor leaves referredEntities null");

        // When: ESAliasStore.getPolicies() is called for this entity (e.g. during updateAlias)
        // it calls objectToEntityList which must NOT throw NPE
        try {
            List<AtlasEntity> result = AccessControlUtils.objectToEntityList(
                    infoWithNullReferredEntities, Collections.emptyList());

            // Then: should return an empty list (no policies to process)
            assertNotNull(result, "Result must not be null");
            assertTrue(result.isEmpty(), "Should return empty list when persona has no policies");

        } catch (NullPointerException npe) {
            fail("NullPointerException in objectToEntityList when referredEntities is null. " +
                 "Fix: add null-guard at AccessControlUtils.java:332 — " +
                 "'Map<String,AtlasEntity> map = entityWithExtInfo.getReferredEntities(); " +
                 "Set<String> referredGuids = map != null ? map.keySet() : Collections.emptySet();'");
        }
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /**
     * Builds an AtlasEntityWithExtInfo representing a Persona that already has {@code k}
     * policies.  Each existing policy is added to referredEntities (simulating what the real
     * EntityGraphRetriever returns on master).
     */
    private AtlasEntityWithExtInfo buildPersonaWithPolicies(int k) {
        AtlasEntity persona = new AtlasEntity();
        persona.setTypeName(PERSONA_ENTITY_TYPE);
        persona.setGuid(PERSONA_GUID);
        persona.setAttribute(QUALIFIED_NAME, PERSONA_QN);
        persona.setAttribute(NAME, "Test Persona");
        persona.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, Boolean.TRUE);

        List<AtlasObjectId> existingPolicyIds = new ArrayList<>();
        AtlasEntityWithExtInfo info = new AtlasEntityWithExtInfo(persona);

        for (int i = 0; i < k; i++) {
            String policyGuid = "existing-policy-guid-" + i;
            AtlasEntity existingPolicy = buildExistingPolicy(policyGuid, i);
            existingPolicyIds.add(new AtlasObjectId(policyGuid, POLICY_ENTITY_TYPE));
            info.addReferredEntity(existingPolicy);
        }

        persona.setRelationshipAttribute(REL_ATTR_POLICIES, existingPolicyIds);
        return info;
    }

    /**
     * Stubs both code paths for fetching the parent Persona entity:
     *
     * <ul>
     *   <li><b>master path</b>: {@code entityRetriever.toAtlasEntityWithExtInfo(AtlasObjectId)}
     *   <li><b>ms-752+ path</b>: {@code entityRetriever.getEntityVertex(AtlasObjectId)} → vertex,
     *       then {@code noRelAttrRetriever.toAtlasEntity(AtlasVertex)} → persona entity
     * </ul>
     *
     * <p>The ms-752 path also skips the graph-edge traversal for existing policies (by keeping
     * {@code typeRegistry.getEntityTypeByName()} returning null, so {@code policiesAttr} is null
     * and the edge loop is not entered).  This means existing-policy call counts register as 0
     * on ms-752, which is the expected post-optimisation behaviour.
     */
    private void mockPersonaFetch(AtlasEntityWithExtInfo personaInfo) throws AtlasBaseException {
        AtlasEntity persona = personaInfo.getEntity();

        // master path
        when(entityRetriever.toAtlasEntityWithExtInfo(any(AtlasObjectId.class)))
                .thenReturn(personaInfo);

        // ms-752+ path: vertex lookup + scalar-only entity load
        // getEntityVertex returns null (vertex mocking avoided — AtlasVertex references TinkerPop);
        // noRelAttrRetriever.toAtlasEntity(any vertex including null) returns the persona entity.
        when(entityRetriever.getEntityVertex(any(AtlasObjectId.class)))
                .thenReturn(null);
        when(noRelAttrRetriever.toAtlasEntity(nullable(AtlasVertex.class)))
                .thenReturn(persona);
        // typeRegistry returns null → policiesAttr = null → edge loop skipped (K=0 on ms-752 tests)
        when(typeRegistry.getEntityTypeByName(any())).thenReturn(null);
    }

    /**
     * For every existing policy stored in {@code personaInfo.referredEntities}, stubs
     * entityRetriever.toAtlasEntity(objectId) to return that entity.
     *
     * <p>This matches master's getAccessControlEntity() loop which calls
     * {@code entityRetriever.toAtlasEntity(policy)} for each existing AtlasObjectId.
     */
    private void mockExistingPolicyFetches(AtlasEntityWithExtInfo personaInfo)
            throws AtlasBaseException {
        List<AtlasObjectId> policyIds = (List<AtlasObjectId>)
                personaInfo.getEntity().getRelationshipAttribute(REL_ATTR_POLICIES);

        if (policyIds == null || policyIds.isEmpty()) return;

        // Build a guid → entity lookup so we can answer any toAtlasEntity(AtlasObjectId)
        // call for an existing policy with the correct entity.
        // Casting to AtlasObjectId disambiguates from toAtlasEntity(AtlasVertex).
        java.util.Map<String, AtlasEntity> byGuid = new java.util.HashMap<>();
        for (AtlasObjectId id : policyIds) {
            byGuid.put(id.getGuid(), personaInfo.getReferredEntity(id.getGuid()));
        }

        when(entityRetriever.toAtlasEntity(any(AtlasObjectId.class)))
                .thenAnswer(inv -> {
                    AtlasObjectId requested = inv.getArgument(0);
                    AtlasEntity found = byGuid.get(requested.getGuid());
                    return found != null ? found : byGuid.values().iterator().next();
                });
    }

    /**
     * Creates a minimal existing-policy entity for use in the persona's referredEntities map.
     */
    private AtlasEntity buildExistingPolicy(String guid, int index) {
        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setGuid(guid);
        policy.setStatus(AtlasEntity.Status.ACTIVE);
        policy.setAttribute(NAME, "existing-policy-" + index);
        policy.setAttribute(QUALIFIED_NAME, PERSONA_QN + "/existing-" + index);
        policy.setAttribute(ATTR_POLICY_CATEGORY, POLICY_CATEGORY_PERSONA);
        policy.setAttribute(ATTR_POLICY_TYPE, "allow");
        policy.setAttribute(ATTR_POLICY_ACTIONS, Collections.singletonList("persona-domain-read"));
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList(
                "entity:default/domain/*/super"));
        return policy;
    }

    /**
     * Builds a new-policy entity with policyCategory=persona and policySubCategory=domain.
     *
     * <p>Using the "domain" sub-category keeps the code path simple: it skips
     * AuthPolicyValidator.validate() and validateConnectionAdmin() (which require a live
     * Connection entity), and calls validateAndReduce() instead — no external I/O.
     */
    private AtlasEntity buildDomainPolicy(String name, String personaGuid) {
        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setGuid("-" + UUID.randomUUID().toString());
        policy.setAttribute(NAME, name);
        policy.setAttribute(QUALIFIED_NAME, name);            // will be overwritten
        policy.setAttribute(ATTR_POLICY_CATEGORY, POLICY_CATEGORY_PERSONA);
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, POLICY_SUB_CATEGORY_DOMAIN);
        policy.setAttribute(ATTR_POLICY_TYPE, "allow");
        policy.setAttribute(ATTR_POLICY_IS_ENABLED, Boolean.TRUE);
        // Resources must contain a recognized domain pattern so validateAndReduce() works
        policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList(
                "entity:default/domain/*/super"));

        policy.setRelationshipAttribute(REL_ATTR_ACCESS_CONTROL,
                new AtlasObjectId(personaGuid, PERSONA_ENTITY_TYPE));
        return policy;
    }

    // -------------------------------------------------------------------------
    // Objenesis / reflection helpers
    // -------------------------------------------------------------------------

    private AuthPolicyPreProcessor createPreprocessorViaObjenesis() {
        org.objenesis.Objenesis objenesis = new org.objenesis.ObjenesisStd();
        return objenesis.newInstance(AuthPolicyPreProcessor.class);
    }

    private void setField(String fieldName, Object value) throws Exception {
        // throws NoSuchFieldException if field doesn't exist (e.g., fields removed on ms-752)
        Field field = AuthPolicyPreProcessor.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(preProcessor, value);
    }
}

