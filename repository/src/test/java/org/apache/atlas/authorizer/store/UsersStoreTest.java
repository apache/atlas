package org.apache.atlas.authorizer.store;

import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

import static org.junit.jupiter.api.Assertions.*;

public class UsersStoreTest {

    private UsersStore store;

    @BeforeEach
    void setUp() {
        store = UsersStore.getInstance();
    }

    @AfterEach
    void tearDown() {
        store.setUserStore(null);
        store.setAllRoles(new RangerRoles());
    }

    @Test
    void testGetRolesForUserDirect() {
        RangerRoles roles = buildRoles(
                role("admin-role", users("alice"), groups(), nestedRoles()),
                role("viewer-role", users("bob"), groups(), nestedRoles())
        );
        store.setAllRoles(roles);

        List<String> aliceRoles = store.getRolesForUser("alice", Collections.emptyList(), roles);
        assertEquals(1, aliceRoles.size());
        assertTrue(aliceRoles.contains("admin-role"));

        List<String> bobRoles = store.getRolesForUser("bob", Collections.emptyList(), roles);
        assertEquals(1, bobRoles.size());
        assertTrue(bobRoles.contains("viewer-role"));
    }

    @Test
    void testGetRolesForUserViaGroup() {
        RangerRoles roles = buildRoles(
                role("eng-role", users(), groups("engineering"), nestedRoles())
        );
        store.setAllRoles(roles);

        List<String> userRoles = store.getRolesForUser("alice", Collections.singletonList("engineering"), roles);
        assertEquals(1, userRoles.size());
        assertTrue(userRoles.contains("eng-role"));
    }

    @Test
    void testGetRolesForUserViaMultipleGroups() {
        RangerRoles roles = buildRoles(
                role("eng-role", users(), groups("engineering"), nestedRoles()),
                role("analytics-role", users(), groups("analytics"), nestedRoles())
        );
        store.setAllRoles(roles);

        List<String> userRoles = store.getRolesForUser("alice", Arrays.asList("engineering", "analytics"), roles);
        assertEquals(2, userRoles.size());
        assertTrue(userRoles.contains("eng-role"));
        assertTrue(userRoles.contains("analytics-role"));
    }

    @Test
    void testGetRolesDeduplicatesUserAndGroupMatch() {
        RangerRoles roles = buildRoles(
                role("shared-role", users("alice"), groups("engineering"), nestedRoles())
        );
        store.setAllRoles(roles);

        List<String> userRoles = store.getRolesForUser("alice", Collections.singletonList("engineering"), roles);
        assertEquals(1, userRoles.size(), "Should deduplicate when user matches via both user and group");
        assertTrue(userRoles.contains("shared-role"));
    }

    @Test
    void testGetNestedRoles() {
        RangerRoles roles = buildRoles(
                role("child-role", users("alice"), groups(), nestedRoles()),
                role("parent-role", users(), groups(), nestedRoles("child-role"))
        );
        store.setAllRoles(roles);

        List<String> directRoles = store.getRolesForUser("alice", Collections.emptyList(), roles);
        assertEquals(1, directRoles.size());
        assertTrue(directRoles.contains("child-role"));

        List<String> nested = store.getNestedRolesForUser(directRoles, roles);
        assertEquals(1, nested.size());
        assertTrue(nested.contains("parent-role"));
    }

    @Test
    void testGetNestedRolesExcludesDirectRoles() {
        RangerRoles roles = buildRoles(
                role("child-role", users("alice"), groups(), nestedRoles()),
                role("parent-role", users(), groups(), nestedRoles("child-role"))
        );
        store.setAllRoles(roles);

        List<String> directRoles = store.getRolesForUser("alice", Collections.emptyList(), roles);
        List<String> nested = store.getNestedRolesForUser(directRoles, roles);

        assertFalse(nested.contains("child-role"), "Nested roles should not include the direct roles themselves");
    }

    @Test
    void testGetGroupsForUser() {
        RangerUserStore userStore = new RangerUserStore();
        Map<String, Set<String>> mapping = new HashMap<>();
        mapping.put("alice", new HashSet<>(Arrays.asList("eng", "analytics")));
        mapping.put("bob", new HashSet<>(Collections.singletonList("eng")));
        userStore.setUserGroupMapping(mapping);
        store.setUserStore(userStore);

        List<String> aliceGroups = store.getGroupsForUser("alice", userStore);
        assertEquals(2, aliceGroups.size());
        assertTrue(aliceGroups.contains("eng"));
        assertTrue(aliceGroups.contains("analytics"));

        List<String> bobGroups = store.getGroupsForUser("bob", userStore);
        assertEquals(1, bobGroups.size());
        assertTrue(bobGroups.contains("eng"));
    }

    @Test
    void testGetGroupsForUnknownUser() {
        RangerUserStore userStore = new RangerUserStore();
        Map<String, Set<String>> mapping = new HashMap<>();
        mapping.put("alice", new HashSet<>(Collections.singletonList("eng")));
        userStore.setUserGroupMapping(mapping);
        store.setUserStore(userStore);

        List<String> groups = store.getGroupsForUser("unknown", userStore);
        assertNotNull(groups);
        assertTrue(groups.isEmpty());
    }

    @Test
    void testNullRolesReturnsEmpty() {
        List<String> roles = store.getRolesForUser("alice", Collections.emptyList(), null);
        assertNotNull(roles);
        assertTrue(roles.isEmpty());
    }

    @Test
    void testEmptyRolesReturnsEmpty() {
        RangerRoles emptyRoles = new RangerRoles();
        store.setAllRoles(emptyRoles);

        List<String> roles = store.getRolesForUser("alice", Collections.emptyList(), emptyRoles);
        assertNotNull(roles);
        assertTrue(roles.isEmpty());
    }

    @Test
    void testNullUserGroupsHandled() {
        RangerRoles roles = buildRoles(
                role("admin-role", users("alice"), groups(), nestedRoles())
        );
        store.setAllRoles(roles);

        List<String> result = store.getRolesForUser("alice", null, roles);
        assertEquals(1, result.size());
    }

    @Test
    void testNullUserStoreReturnsEmptyGroups() {
        List<String> groups = store.getGroupsForUser("alice", null);
        assertNotNull(groups);
        assertTrue(groups.isEmpty());
    }

    @Test
    void testGetNestedRolesForNullInput() {
        RangerRoles roles = buildRoles();
        store.setAllRoles(roles);

        List<String> nested = store.getNestedRolesForUser(null, roles);
        assertNotNull(nested);
        assertTrue(nested.isEmpty());
    }

    @Test
    void testGetNestedRolesForEmptyInput() {
        RangerRoles roles = buildRoles();
        store.setAllRoles(roles);

        List<String> nested = store.getNestedRolesForUser(Collections.emptyList(), roles);
        assertNotNull(nested);
        assertTrue(nested.isEmpty());
    }

    @Test
    void testRolesSnapshotAtomicity() throws Exception {
        int iterations = 1000;
        AtomicBoolean failed = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);

        RangerRoles rolesV1 = buildRoles(role("role-v1", users("alice"), groups(), nestedRoles()));
        RangerRoles rolesV2 = buildRoles(role("role-v2", users("bob"), groups(), nestedRoles()));

        Thread writer = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    store.setAllRoles(i % 2 == 0 ? rolesV1 : rolesV2);
                }
            } catch (Exception e) {
                failed.set(true);
            }
        });

        Thread reader = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    RangerRoles roles = store.getAllRoles();
                    if (roles != null) {
                        store.getRolesForUser("alice", Collections.emptyList(), roles);
                        store.getRolesForUser("bob", Collections.emptyList(), roles);
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

        assertFalse(failed.get(), "Reader encountered an error during concurrent writes");
    }

    // --- Helper methods ---

    private static RangerRoles buildRoles(RangerRole... roles) {
        RangerRoles rangerRoles = new RangerRoles();
        rangerRoles.setServiceName("atlas");
        rangerRoles.setRoleVersion(1L);
        rangerRoles.setRoleUpdateTime(new Date());
        rangerRoles.setRangerRoles(new HashSet<>(Arrays.asList(roles)));
        return rangerRoles;
    }

    private static RangerRole role(String name, List<RangerRole.RoleMember> users,
                                   List<RangerRole.RoleMember> groups,
                                   List<RangerRole.RoleMember> nestedRoles) {
        RangerRole role = new RangerRole();
        role.setName(name);
        role.setUsers(users);
        role.setGroups(groups);
        role.setRoles(nestedRoles);
        return role;
    }

    private static List<RangerRole.RoleMember> users(String... names) {
        List<RangerRole.RoleMember> members = new ArrayList<>();
        for (String name : names) {
            members.add(new RangerRole.RoleMember(name, false));
        }
        return members;
    }

    private static List<RangerRole.RoleMember> groups(String... names) {
        return users(names);
    }

    private static List<RangerRole.RoleMember> nestedRoles(String... names) {
        return users(names);
    }
}
