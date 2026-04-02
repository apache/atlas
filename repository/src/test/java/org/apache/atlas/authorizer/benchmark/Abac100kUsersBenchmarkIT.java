package org.apache.atlas.authorizer.benchmark;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.authorizer.store.UsersStore;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;

/**
 * Opt-in benchmark-style test.
 *
 * Default surefire patterns do not include *IT classes, so this only runs when explicitly requested:
 * mvn -pl repository -Dtest=Abac100kUsersBenchmarkIT test
 */
public class Abac100kUsersBenchmarkIT {
    private static final int USER_COUNT = 100_000;
    private static final int GROUP_COUNT = 4_000;
    private static final int ROLE_COUNT = 1_200;
    private static final int POLICY_COUNT = 12_000;
    private static final int GROUPS_PER_USER = 3;

    private static final String ACTION = "entity-read";
    private static final String SERVICE_NAME = "atlas_abac";

    private static final int THREAD_COUNT = Math.max(4, Runtime.getRuntime().availableProcessors());
    private static final int REQUESTS_PER_THREAD = 10_000;
    private static final int WARMUP_REQUESTS = 15_000;

    @Test
    public void benchmarkRelevantPoliciesLookupFor100kUsers() throws Exception {
        seedUsersAndRoles();
        seedAbacPolicies();
        warmup();

        CountDownLatch startGate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        List<Callable<Integer>> tasks = new ArrayList<>(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            tasks.add(() -> {
                startGate.await();
                int matchedPolicies = 0;

                for (int j = 0; j < REQUESTS_PER_THREAD; j++) {
                    String user = "user-" + ThreadLocalRandom.current().nextInt(USER_COUNT);
                    setCurrentUser(user);

                    List<RangerPolicy> policies = PoliciesStore.getInstance().getRelevantPolicies(
                            null,
                            null,
                            SERVICE_NAME,
                            Collections.singletonList(ACTION),
                            POLICY_TYPE_ALLOW);
                    matchedPolicies += policies.size();
                }

                SecurityContextHolder.clearContext();
                RequestContext.clear();
                return matchedPolicies;
            });
        }

        List<Future<Integer>> futures = new ArrayList<>(THREAD_COUNT);
        for (Callable<Integer> task : tasks) {
            futures.add(executor.submit(task));
        }

        startGate.countDown();

        int totalMatchedPolicies = 0;
        for (Future<Integer> future : futures) {
            totalMatchedPolicies += future.get();
        }

        executor.shutdown();
        Assert.assertTrue("Executor did not terminate in time", executor.awaitTermination(10, TimeUnit.MINUTES));

        Assert.assertTrue("Benchmark returned no policy matches", totalMatchedPolicies > 0);
    }

    private static void warmup() {
        for (int i = 0; i < WARMUP_REQUESTS; i++) {
            String user = "user-" + (i % USER_COUNT);
            setCurrentUser(user);
            PoliciesStore.getInstance().getRelevantPolicies(
                    null,
                    null,
                    SERVICE_NAME,
                    Collections.singletonList(ACTION),
                    POLICY_TYPE_ALLOW);
        }
        SecurityContextHolder.clearContext();
        RequestContext.clear();
    }

    private static void seedUsersAndRoles() {
        Map<String, Set<String>> userGroups = new HashMap<>(USER_COUNT);
        for (int i = 0; i < USER_COUNT; i++) {
            Set<String> groups = new HashSet<>(GROUPS_PER_USER);
            for (int g = 0; g < GROUPS_PER_USER; g++) {
                groups.add("group-" + ((i + g) % GROUP_COUNT));
            }
            userGroups.put("user-" + i, groups);
        }

        RangerUserStore userStore = new RangerUserStore();
        userStore.setServiceName("atlas");
        userStore.setUserStoreVersion(1L);
        userStore.setUserStoreUpdateTime(new Date());
        userStore.setUserGroupMapping(userGroups);
        userStore.setUserAttrMapping(Collections.emptyMap());
        userStore.setGroupAttrMapping(Collections.emptyMap());
        UsersStore.getInstance().setUserStore(userStore);

        Set<RangerRole> rangerRoles = new HashSet<>(ROLE_COUNT);
        for (int r = 0; r < ROLE_COUNT; r++) {
            RangerRole role = new RangerRole();
            role.setName("role-" + r);
            role.setUsers(Collections.singletonList(new RangerRole.RoleMember("user-" + r, false)));
            role.setGroups(Collections.singletonList(new RangerRole.RoleMember("group-" + (r % GROUP_COUNT), false)));
            if (r > 0 && r % 3 == 0) {
                role.setRoles(Collections.singletonList(new RangerRole.RoleMember("role-" + (r - 1), false)));
            }
            rangerRoles.add(role);
        }

        RangerRoles roles = new RangerRoles();
        roles.setServiceName("atlas");
        roles.setRoleVersion(1L);
        roles.setRoleUpdateTime(new Date());
        roles.setRangerRoles(rangerRoles);
        UsersStore.getInstance().setAllRoles(roles);
    }

    private static void seedAbacPolicies() {
        List<RangerPolicy> policies = new ArrayList<>(POLICY_COUNT);

        for (int i = 0; i < POLICY_COUNT; i++) {
            RangerPolicy policy = new RangerPolicy();
            policy.setGuid("policy-" + i);
            policy.setName("abac-policy-" + i);
            policy.setPolicyPriority(i % 100 == 0 ? RangerPolicy.POLICY_PRIORITY_OVERRIDE : RangerPolicy.POLICY_PRIORITY_NORMAL);

            RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
            policyItem.setAccesses(Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess(ACTION, true)));
            policyItem.setUsers(Collections.singletonList("user-" + (i % USER_COUNT)));
            policyItem.setGroups(new ArrayList<>(Collections.singletonList("group-" + (i % GROUP_COUNT))));
            policyItem.setRoles(Collections.singletonList("role-" + (i % ROLE_COUNT)));

            // Keep some broad-match policies in the mix.
            if (i % 20 == 0) {
                policyItem.getGroups().add("public");
            }

            policy.setPolicyItems(Collections.singletonList(policyItem));
            policy.setDenyPolicyItems(Collections.emptyList());
            policies.add(policy);
        }

        PoliciesStore store = PoliciesStore.getInstance();
        store.setAbacPolicies(policies);
    }

    private static void setCurrentUser(String user) {
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(new UsernamePasswordAuthenticationToken(user, ""));
        SecurityContextHolder.setContext(context);
    }

}
