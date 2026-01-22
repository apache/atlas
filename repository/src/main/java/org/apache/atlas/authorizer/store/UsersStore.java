package org.apache.atlas.authorizer.store;

import org.apache.atlas.authorizer.authorizers.AuthorizerCommonUtil;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UsersStore {
    private static final UsersStore INSTANCE = new UsersStore();
    
    private RangerUserStore userStore;
    private RangerRoles allRoles;

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
        this.allRoles = allRoles;
    }

    public RangerRoles getAllRoles() {
        return allRoles;
    }

    public List<String> getGroupsForUser(String user, RangerUserStore userStore) {
        Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
        List<String> groups = new ArrayList<>();
        Set<String> groupsSet = userGroupMapping.get(user);
        if (groupsSet != null && !groupsSet.isEmpty()) {
            groups.addAll(groupsSet);
        }
        return groups;
    }

    public List<String> getRolesForUser(String user, List<String> userGroups, RangerRoles allRoles) {
        List<String> roles = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            boolean userInRole = false;

            // Check if user is directly in role's user list
            List<RangerRole.RoleMember> users = role.getUsers();
            for (RangerRole.RoleMember roleUser: users) {
                if (roleUser.getName().equals(user)) {
                    userInRole = true;
                    break;
                }
            }

            // If user not found directly, check if user's groups are in role's group list
            if (!userInRole && userGroups != null && !userGroups.isEmpty()) {
                List<RangerRole.RoleMember> roleGroups = role.getGroups();
                if (roleGroups != null) {
                    for (RangerRole.RoleMember roleGroup : roleGroups) {
                        if (userGroups.contains(roleGroup.getName())) {
                            userInRole = true;
                            break;
                        }
                    }
                }
            }

            if (userInRole) {
                roles.add(role.getName());
            }
        }
        return roles;
    }

    public List<String> getNestedRolesForUser(List<String> userRoles, RangerRoles allRoles) {
        List<String> ret = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            List<RangerRole.RoleMember> nestedRoles = role.getRoles();
            List<String> nestedRolesName = new ArrayList<>();
            for (RangerRole.RoleMember nestedRole : nestedRoles) {
                nestedRolesName.add(nestedRole.getName());
            }
            if (AuthorizerCommonUtil.arrayListContains(userRoles, nestedRolesName)) {
                ret.add(role.getName());
            }
        }
        return ret;
    }

}
