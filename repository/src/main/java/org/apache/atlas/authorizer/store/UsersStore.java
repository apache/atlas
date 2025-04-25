package org.apache.atlas.authorizer.store;

import org.apache.atlas.authorizer.authorizers.AuthorizerCommon;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UsersStore {

    private static RangerUserStore userStore;
    private static RangerRoles allRoles;

    public static void setUserStore(RangerUserStore userStore) {
        UsersStore.userStore = userStore;
    }

    public static RangerUserStore getUserStore() {
        return userStore;
    }

    public static void setAllRoles(RangerRoles allRoles) {
        UsersStore.allRoles = allRoles;
    }

    public static RangerRoles getAllRoles() {
        return allRoles;
    }

    public static List<String> getGroupsForUser(String user, RangerUserStore userStore) {
        Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
        List<String> groups = new ArrayList<>();
        Set<String> groupsSet = userGroupMapping.get(user);
        if (groupsSet != null && !groupsSet.isEmpty()) {
            groups.addAll(groupsSet);
        }
        return groups;
    }

    public static List<String> getRolesForUser(String user, RangerRoles allRoles) {
        List<String> roles = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            List<RangerRole.RoleMember> users = role.getUsers();
            for (RangerRole.RoleMember roleUser: users) {
                if (roleUser.getName().equals(user)) {
                    roles.add(role.getName());
                }
            }
        }
        return roles;
    }

    public static List<String> getNestedRolesForUser(List<String> userRoles, RangerRoles allRoles) {
        List<String> ret = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            List<RangerRole.RoleMember> nestedRoles = role.getRoles();
            List<String> nestedRolesName = new ArrayList<>();
            for (RangerRole.RoleMember nestedRole : nestedRoles) {
                nestedRolesName.add(nestedRole.getName());
            }
            if (AuthorizerCommon.arrayListContains(userRoles, nestedRolesName)) {
                ret.add(role.getName());
            }
        }
        return ret;
    }

}
