package org.apache.atlas.authorizer.store;

import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UsersStore {
    private static final UsersStore INSTANCE = new UsersStore();

    private static final RoleIndex EMPTY_ROLE_INDEX = new RoleIndex(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    private volatile RangerUserStore userStore;
    private volatile RolesSnapshot rolesSnapshot = RolesSnapshot.EMPTY;

    private UsersStore() {} // private constructor

    public static UsersStore getInstance() {
        return INSTANCE;
    }

    public void setUserStore(RangerUserStore userStore) {
        this.userStore = userStore;
    }

    public RangerUserStore getUserStore() {
        return userStore;
    }

    public void setAllRoles(RangerRoles allRoles) {
        this.rolesSnapshot = new RolesSnapshot(allRoles, buildRoleIndex(allRoles));
    }

    public RangerRoles getAllRoles() {
        return rolesSnapshot.roles;
    }

    public List<String> getGroupsForUser(String user, RangerUserStore userStore) {
        RangerUserStore effectiveUserStore = userStore != null ? userStore : this.userStore;
        if (effectiveUserStore == null || effectiveUserStore.getUserGroupMapping() == null) {
            return new ArrayList<>(0);
        }

        Set<String> groupsSet = effectiveUserStore.getUserGroupMapping().get(user);
        return groupsSet == null || groupsSet.isEmpty() ? new ArrayList<>(0) : new ArrayList<>(groupsSet);
    }

    public List<String> getRolesForUser(String user, List<String> userGroups, RangerRoles allRoles) {
        RoleIndex index = resolveRoleIndex(allRoles);
        LinkedHashSet<String> roles = new LinkedHashSet<>();

        if (user != null) {
            List<String> userRoles = index.rolesByUser.get(user);
            if (userRoles != null) {
                roles.addAll(userRoles);
            }
        }

        if (userGroups != null && !userGroups.isEmpty()) {
            for (String group : userGroups) {
                List<String> groupRoles = index.rolesByGroup.get(group);
                if (groupRoles != null) {
                    roles.addAll(groupRoles);
                }
            }
        }

        return new ArrayList<>(roles);
    }

    public List<String> getNestedRolesForUser(List<String> userRoles, RangerRoles allRoles) {
        if (userRoles == null || userRoles.isEmpty()) {
            return new ArrayList<>(0);
        }

        RoleIndex index = resolveRoleIndex(allRoles);
        LinkedHashSet<String> ret = new LinkedHashSet<>();

        for (String userRole : userRoles) {
            List<String> parentRoles = index.parentRolesByNestedRole.get(userRole);
            if (parentRoles != null) {
                ret.addAll(parentRoles);
            }
        }

        ret.removeAll(userRoles);

        return new ArrayList<>(ret);
    }

    private RoleIndex resolveRoleIndex(RangerRoles roles) {
        if (roles == null) {
            return EMPTY_ROLE_INDEX;
        }

        RolesSnapshot snapshot = this.rolesSnapshot;
        return roles == snapshot.roles ? snapshot.index : buildRoleIndex(roles);
    }

    private static RoleIndex buildRoleIndex(RangerRoles roles) {
        Set<RangerRole> rangerRoles = roles != null ? roles.getRangerRoles() : null;
        if (rangerRoles == null || rangerRoles.isEmpty()) {
            return EMPTY_ROLE_INDEX;
        }

        Map<String, LinkedHashSet<String>> rolesByUser = new HashMap<>();
        Map<String, LinkedHashSet<String>> rolesByGroup = new HashMap<>();
        Map<String, LinkedHashSet<String>> parentRolesByNestedRole = new HashMap<>();

        for (RangerRole role : rangerRoles) {
            if (role == null || role.getName() == null) {
                continue;
            }

            String roleName = role.getName();

            addMembers(rolesByUser, role.getUsers(), roleName);
            addMembers(rolesByGroup, role.getGroups(), roleName);
            addMembers(parentRolesByNestedRole, role.getRoles(), roleName);
        }

        return new RoleIndex(
                toImmutableValueLists(rolesByUser),
                toImmutableValueLists(rolesByGroup),
                toImmutableValueLists(parentRolesByNestedRole));
    }

    private static void addMembers(Map<String, LinkedHashSet<String>> index, List<RangerRole.RoleMember> members, String roleName) {
        if (members == null || members.isEmpty()) {
            return;
        }

        for (RangerRole.RoleMember member : members) {
            if (member != null && member.getName() != null) {
                index.computeIfAbsent(member.getName(), key -> new LinkedHashSet<>()).add(roleName);
            }
        }
    }

    private static Map<String, List<String>> toImmutableValueLists(Map<String, LinkedHashSet<String>> source) {
        if (source.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, List<String>> ret = new HashMap<>(source.size());
        for (Map.Entry<String, LinkedHashSet<String>> entry : source.entrySet()) {
            ret.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(ret);
    }

    private static final class RoleIndex {
        private final Map<String, List<String>> rolesByUser;
        private final Map<String, List<String>> rolesByGroup;
        private final Map<String, List<String>> parentRolesByNestedRole;

        private RoleIndex(Map<String, List<String>> rolesByUser, Map<String, List<String>> rolesByGroup, Map<String, List<String>> parentRolesByNestedRole) {
            this.rolesByUser = rolesByUser;
            this.rolesByGroup = rolesByGroup;
            this.parentRolesByNestedRole = parentRolesByNestedRole;
        }
    }

    /**
     * Immutable holder that groups roles with their pre-built index.
     * Swapped via a single volatile write so readers always see a consistent pair.
     */
    static final class RolesSnapshot {
        static final RolesSnapshot EMPTY = new RolesSnapshot(null, EMPTY_ROLE_INDEX);

        final RangerRoles roles;
        final RoleIndex index;

        RolesSnapshot(RangerRoles roles, RoleIndex index) {
            this.roles = roles;
            this.index = index;
        }
    }
}
