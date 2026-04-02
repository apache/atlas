package org.apache.atlas.authorizer.store;

import org.apache.atlas.RequestContext;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.junit.jupiter.api.Assertions.*;

public class PoliciesStoreTest {

    private static final String SERVICE_ABAC = "atlas_abac";
    private static final String SERVICE_RESOURCE = "atlas";
    private static final String SERVICE_TAG = "atlas_tag";

    @BeforeEach
    void setUp() {
        RequestContext.clear();
        // Seed a minimal user store so getRelevantPolicies can resolve the current user
        UsersStore usersStore = UsersStore.getInstance();

        RangerUserStore userStore = new RangerUserStore();
        Map<String, Set<String>> mapping = new HashMap<>();
        mapping.put("alice", new HashSet<>(Arrays.asList("data-eng", "analysts")));
        mapping.put("bob", new HashSet<>(Collections.singletonList("data-eng")));
        userStore.setUserGroupMapping(mapping);
        usersStore.setUserStore(userStore);

        RangerRoles roles = new RangerRoles();
        roles.setServiceName("atlas");
        roles.setRoleVersion(1L);
        roles.setRoleUpdateTime(new Date());

        Set<RangerRole> rangerRoles = new HashSet<>();
        RangerRole adminRole = new RangerRole();
        adminRole.setName("admin-role");
        adminRole.setUsers(Collections.singletonList(new RangerRole.RoleMember("alice", false)));
        adminRole.setGroups(Collections.emptyList());
        adminRole.setRoles(Collections.emptyList());
        rangerRoles.add(adminRole);

        RangerRole viewerRole = new RangerRole();
        viewerRole.setName("viewer-role");
        viewerRole.setGroups(Collections.singletonList(new RangerRole.RoleMember("data-eng", false)));
        viewerRole.setUsers(Collections.emptyList());
        viewerRole.setRoles(Collections.emptyList());
        rangerRoles.add(viewerRole);

        roles.setRangerRoles(rangerRoles);
        usersStore.setAllRoles(roles);
    }

    @AfterEach
    void tearDown() {
        PoliciesStore.getInstance().setAbacPolicies(Collections.emptyList());
        PoliciesStore.getInstance().setResourcePolicies(Collections.emptyList());
        PoliciesStore.getInstance().setTagPolicies(Collections.emptyList());
        SecurityContextHolder.clearContext();
        RequestContext.clear();
    }

    @Test
    void testActionFilteringReturnsMatchingPolicies() {
        RangerPolicy readPolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy writePolicy = makePolicy("p2", "entity-update", POLICY_TYPE_ALLOW, "alice", null, null);

        PoliciesStore.getInstance().setAbacPolicies(Arrays.asList(readPolicy, writePolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertEquals(1, result.size(), "Should match only the read policy");
        assertSame(readPolicy, result.get(0));
    }

    @Test
    void testMultipleActionsReturnUnionOfPolicies() {
        RangerPolicy readPolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy writePolicy = makePolicy("p2", "entity-update", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy deletePolicy = makePolicy("p3", "entity-delete", POLICY_TYPE_ALLOW, "alice", null, null);

        PoliciesStore.getInstance().setAbacPolicies(Arrays.asList(readPolicy, writePolicy, deletePolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Arrays.asList("entity-read", "entity-delete"), POLICY_TYPE_ALLOW);

        assertEquals(2, result.size(), "Should match read and delete policies");
        assertTrue(result.contains(readPolicy));
        assertTrue(result.contains(deletePolicy));
    }

    @Test
    void testPrincipalFilteringByUser() {
        RangerPolicy alicePolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy bobPolicy = makePolicy("p2", "entity-read", POLICY_TYPE_ALLOW, "bob", null, null);

        PoliciesStore.getInstance().setAbacPolicies(Arrays.asList(alicePolicy, bobPolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertTrue(result.contains(alicePolicy), "Alice should see her own policy");
        assertFalse(result.contains(bobPolicy), "Alice should NOT see Bob's policy");
    }

    @Test
    void testPrincipalFilteringByGroup() {
        RangerPolicy groupPolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, null, "data-eng", null);

        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(groupPolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertEquals(1, result.size(), "Alice (member of data-eng) should see the group policy");
        assertSame(groupPolicy, result.get(0));
    }

    @Test
    void testPrincipalFilteringByRole() {
        RangerPolicy rolePolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, null, null, "admin-role");

        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(rolePolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertEquals(1, result.size(), "Alice (member of admin-role) should see the role policy");
        assertSame(rolePolicy, result.get(0));
    }

    @Test
    void testPublicGroupAlwaysIncluded() {
        RangerPolicy publicPolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, null, "public", null);

        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(publicPolicy));
        setCurrentUser("unknown-user");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertEquals(1, result.size(), "Public group policy should be returned for any user");
    }

    @Test
    void testDeduplication() {
        RangerPolicy policy = makePolicyMultiPrincipal("p1", "entity-read", POLICY_TYPE_ALLOW,
                Collections.singletonList("alice"),
                Collections.singletonList("data-eng"),
                Collections.emptyList());

        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(policy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertEquals(1, result.size(), "Duplicate policy should appear only once");
    }

    @Test
    void testDenyPolicyRouting() {
        RangerPolicy denyPolicy = makeDenyPolicy("p1", "entity-read", "alice");

        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(denyPolicy));
        setCurrentUser("alice");

        List<RangerPolicy> denyResult = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_DENY);
        assertEquals(1, denyResult.size(), "Deny policy should be returned for deny type query");

        List<RangerPolicy> allowResult = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);
        assertEquals(0, allowResult.size(), "Deny-only policy should NOT appear in allow query");
    }

    @Test
    void testQualifiedNamePrefixFilter() {
        RangerPolicy personaPolicy = makePolicy("persona1/policy-read", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy otherPolicy = makePolicy("persona2/policy-read", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);

        PoliciesStore.getInstance().setAbacPolicies(Arrays.asList(personaPolicy, otherPolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                "persona1", null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertEquals(1, result.size(), "Should only return policies matching persona1 prefix");
        assertSame(personaPolicy, result.get(0));
    }

    @Test
    void testEmptyPolicies() {
        PoliciesStore.getInstance().setAbacPolicies(Collections.emptyList());
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testNullPolicies() {
        PoliciesStore.getInstance().setAbacPolicies(null);
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testIndexRebuildOnSetter() {
        RangerPolicy policyV1 = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(policyV1));
        setCurrentUser("alice");

        List<RangerPolicy> resultV1 = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);
        assertEquals(1, resultV1.size());

        RangerPolicy policyV2 = makePolicy("p2", "entity-read", POLICY_TYPE_ALLOW, "charlie", null, null);
        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(policyV2));

        List<RangerPolicy> resultV2 = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);
        assertEquals(0, resultV2.size(), "Alice should no longer see policies after rebuild");
    }

    @Test
    void testSnapshotAtomicity() throws Exception {
        int iterations = 1000;
        AtomicBoolean failed = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);

        RangerPolicy policyA = makePolicy("pA", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy policyB = makePolicy("pB", "entity-update", POLICY_TYPE_ALLOW, "alice", null, null);

        Thread writer = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    if (i % 2 == 0) {
                        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(policyA));
                    } else {
                        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(policyB));
                    }
                }
            } catch (Exception e) {
                failed.set(true);
            }
        });

        Thread reader = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    List<RangerPolicy> policies = PoliciesStore.getInstance().getAbacPolicies();
                    if (policies != null && !policies.isEmpty() && policies.size() != 1) {
                        failed.set(true);
                    }
                }
            } catch (Exception e) {
                failed.set(true);
            }
        });

        writer.start();
        reader.start();
        writer.join(10_000);
        reader.join(10_000);

        assertFalse(failed.get(), "Reader saw inconsistent state during concurrent writes");
    }

    @Test
    void testServiceNameRouting() {
        RangerPolicy resourcePolicy = makePolicy("rp", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy tagPolicy = makePolicy("tp", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy abacPolicy = makePolicy("ap", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);

        PoliciesStore.getInstance().setResourcePolicies(Collections.singletonList(resourcePolicy));
        PoliciesStore.getInstance().setTagPolicies(Collections.singletonList(tagPolicy));
        PoliciesStore.getInstance().setAbacPolicies(Collections.singletonList(abacPolicy));
        setCurrentUser("alice");

        List<RangerPolicy> resourceResult = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_RESOURCE, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);
        assertEquals(1, resourceResult.size());
        assertSame(resourcePolicy, resourceResult.get(0));

        List<RangerPolicy> tagResult = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_TAG, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);
        assertEquals(1, tagResult.size());
        assertSame(tagPolicy, tagResult.get(0));

        List<RangerPolicy> abacResult = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW);
        assertEquals(1, abacResult.size());
        assertSame(abacPolicy, abacResult.get(0));
    }

    @Test
    void testIgnoreUserReturnsAllMatchingPolicies() {
        RangerPolicy alicePolicy = makePolicy("p1", "entity-read", POLICY_TYPE_ALLOW, "alice", null, null);
        RangerPolicy bobPolicy = makePolicy("p2", "entity-read", POLICY_TYPE_ALLOW, "bob", null, null);

        PoliciesStore.getInstance().setAbacPolicies(Arrays.asList(alicePolicy, bobPolicy));
        setCurrentUser("alice");

        List<RangerPolicy> result = PoliciesStore.getInstance().getRelevantPolicies(
                null, null, SERVICE_ABAC, Collections.singletonList("entity-read"), POLICY_TYPE_ALLOW, true);

        assertEquals(2, result.size(), "ignoreUser should return all action-matched policies");
    }

    // --- Helper methods ---

    private static void setCurrentUser(String user) {
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(new UsernamePasswordAuthenticationToken(user, ""));
        SecurityContextHolder.setContext(context);
        RequestContext.clear();
        RequestContext.get().setUser(user, null);
    }

    private static RangerPolicy makePolicy(String name, String action, String policyType, String user, String group, String role) {
        List<String> users = user != null ? Collections.singletonList(user) : Collections.emptyList();
        List<String> groups = group != null ? Collections.singletonList(group) : Collections.emptyList();
        List<String> roles = role != null ? Collections.singletonList(role) : Collections.emptyList();
        return makePolicyMultiPrincipal(name, action, policyType, users, groups, roles);
    }

    private static RangerPolicy makePolicyMultiPrincipal(String name, String action, String policyType,
                                                         List<String> users, List<String> groups, List<String> roles) {
        RangerPolicy policy = new RangerPolicy();
        policy.setGuid("guid-" + name);
        policy.setName(name);
        policy.setPolicyPriority(RangerPolicy.POLICY_PRIORITY_NORMAL);

        RangerPolicyItem policyItem = new RangerPolicyItem();
        policyItem.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(action, true)));
        policyItem.setUsers(new ArrayList<>(users));
        policyItem.setGroups(new ArrayList<>(groups));
        policyItem.setRoles(new ArrayList<>(roles));

        if (POLICY_TYPE_DENY.equals(policyType)) {
            policy.setPolicyItems(Collections.emptyList());
            policy.setDenyPolicyItems(Collections.singletonList(policyItem));
        } else {
            policy.setPolicyItems(Collections.singletonList(policyItem));
            policy.setDenyPolicyItems(Collections.emptyList());
        }

        return policy;
    }

    private static RangerPolicy makeDenyPolicy(String name, String action, String user) {
        return makePolicy(name, action, POLICY_TYPE_DENY, user, null, null);
    }
}
