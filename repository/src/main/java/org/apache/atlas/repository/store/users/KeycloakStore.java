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

package org.apache.atlas.repository.store.users;

import org.apache.atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.GroupsResource;
import org.keycloak.admin.client.resource.RoleByIdResource;
import org.keycloak.admin.client.resource.RoleResource;
import org.keycloak.admin.client.resource.RolesResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.RESOURCE_NOT_FOUND;

public class KeycloakStore {
    private static final Logger LOG = LoggerFactory.getLogger(KeycloakStore.class);

    private boolean saveUsersToAttributes = false;
    private boolean saveGroupsToAttributes = false;

    public KeycloakStore() {

    }

    public KeycloakStore(boolean saveUsersToAttributes, boolean saveGroupsToAttributes) {
        this.saveUsersToAttributes  = saveUsersToAttributes;
        this.saveGroupsToAttributes  = saveGroupsToAttributes;
    }

    public RoleRepresentation createRole(String name) throws AtlasBaseException {
        return createRole(name, false, null, null, null, null);
    }

    public RoleRepresentation createRole(String name,
                                         List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {
        return createRole(name, false, users, groups, roles, null);
    }

    public RoleRepresentation createRoleForConnection(String name, boolean isComposite,
                                                      List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {

        List<UserRepresentation> roleUsers = new ArrayList<>();
        UsersResource usersResource = null;

        if (CollectionUtils.isNotEmpty(users)) {
            usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

            for (String userName : users) {
                List<UserRepresentation> matchedUsers = usersResource.search(userName);
                Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

                if (keyUserOptional.isPresent()) {
                    roleUsers.add(keyUserOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak user not found with userName " + userName);
                }
            }
        }

        List<GroupRepresentation> roleGroups = new ArrayList<>();
        GroupsResource groupsResource = null;

        if (CollectionUtils.isNotEmpty(groups)) {
            groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

            for (String groupName : groups) {
                List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
                Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

                if (keyGroupOptional.isPresent()) {
                    roleGroups.add(keyGroupOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak group not found with name " + groupName);
                }
            }
        }

        List<RoleRepresentation> roleRoles = new ArrayList<>();
        RolesResource rolesResource      = KeycloakClient.getKeycloakClient().getRealm().roles();
        RoleByIdResource rolesIdResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();

        if (CollectionUtils.isNotEmpty(roles)) {
            for (String roleId : roles) {
                RoleRepresentation role = getRoleById(rolesIdResource, roleId);
                roleRoles.add(role);
            }
        }

        RoleRepresentation role = new RoleRepresentation();
        role.setName(name);
        role.setComposite(isComposite);

        RoleRepresentation createdRole = createRole(role);
        if (createdRole == null) {
            throw new AtlasBaseException("Failed to create a keycloak role " + name);
        }
        LOG.info("Created keycloak role with name {}", name);

        //add realm role into users
        if (CollectionUtils.isNotEmpty(roleUsers)) {
            for (UserRepresentation kUser : roleUsers) {
                UserResource userResource = usersResource.get(kUser.getId());

                userResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                userResource.update(kUser);
            }
        }

        //add realm role into groups
        if (CollectionUtils.isNotEmpty(roleGroups)) {
            for (GroupRepresentation kGroup : roleGroups) {
                GroupResource groupResource = groupsResource.group(kGroup.getId());

                groupResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                groupResource.update(kGroup);
            }
        }

        //add realm role into roles
        if (CollectionUtils.isNotEmpty(roleRoles)) {
            RoleResource connectionRoleResource = rolesResource.get(createdRole.getName());

            for (RoleRepresentation kRole : roleRoles) {
                RoleResource roleResource = rolesResource.get(kRole.getName());

                connectionRoleResource.addComposites(Collections.singletonList(roleResource.toRepresentation()));
                connectionRoleResource.update(connectionRoleResource.toRepresentation());
            }
        }

        return createdRole;
    }

    public RoleRepresentation createRole(String name, boolean isComposite,
                                         List<String> users, List<String> groups, List<String> roles,
                                         Map<String, List<String>> attributes) throws AtlasBaseException {

        RolesResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().roles();

        List<UserRepresentation> roleUsers = new ArrayList<>();
        UsersResource usersResource = null;

        if (CollectionUtils.isNotEmpty(users)) {
            usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

            for (String userName : users) {
                List<UserRepresentation> matchedUsers = usersResource.search(userName);
                Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

                if (keyUserOptional.isPresent()) {
                    roleUsers.add(keyUserOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak user not found with userName " + userName);
                }
            }
        }

        List<GroupRepresentation> roleGroups = new ArrayList<>();
        GroupsResource groupsResource = null;

        if (CollectionUtils.isNotEmpty(groups)) {
            groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

            for (String groupName : groups) {
                List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
                Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

                if (keyGroupOptional.isPresent()) {
                    roleGroups.add(keyGroupOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak group not found with name " + groupName);
                }
            }
        }

        List<RoleRepresentation> roleRoles = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(roles)) {
            for (String roleName : roles) {
                LOG.info("Searching role {}", roleName);
                RoleRepresentation roleRepresentation = rolesResource.get(roleName).toRepresentation();

                if (roleRepresentation != null) {
                    roleRoles.add(roleRepresentation);
                } else {
                    throw new AtlasBaseException("Keycloak role not found with name " + roleName);
                }
            }
        }

        RoleRepresentation role = new RoleRepresentation();
        role.setName(name);
        role.setComposite(isComposite);

        if (attributes == null) {
            attributes = new HashMap<>();
        }

        if (saveUsersToAttributes) {
            attributes.put("users", Collections.singletonList(AtlasType.toJson(roleUsers.stream().map(x -> x.getId()).collect(Collectors.toList()))));
        }

        if (saveGroupsToAttributes) {
            attributes.put("groups", Collections.singletonList(AtlasType.toJson(roleGroups.stream().map(x -> x.getId()).collect(Collectors.toList()))));
        }

        if (MapUtils.isNotEmpty(attributes)) {
            role.setAttributes(attributes);
        }

        RoleRepresentation createdRole = createRole(role);
        if (createdRole == null) {
            throw new AtlasBaseException("Failed to create a keycloak role " + name);
        }
        LOG.info("Created keycloak role with name {}", name);

        //add realm role into users
        if (CollectionUtils.isNotEmpty(roleUsers)) {
            for (UserRepresentation kUser : roleUsers) {
                UserResource userResource = usersResource.get(kUser.getId());

                userResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                userResource.update(kUser);
            }
        }

        //add realm role into groups
        if (CollectionUtils.isNotEmpty(roleGroups)) {
            for (GroupRepresentation kGroup : roleGroups) {
                GroupResource groupResource = groupsResource.group(kGroup.getId());

                groupResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                groupResource.update(kGroup);
            }
        }

        //add realm role into roles
        if (CollectionUtils.isNotEmpty(roleRoles)) {
            RoleResource connectionRoleResource = rolesResource.get(createdRole.getName());

            for (RoleRepresentation kRole : roleRoles) {
                RoleResource roleResource = rolesResource.get(kRole.getName());

                connectionRoleResource.addComposites(Collections.singletonList(roleResource.toRepresentation()));
                connectionRoleResource.update(connectionRoleResource.toRepresentation());
            }
        }

        return createdRole;
    }

    public RoleRepresentation createRole(RoleRepresentation role) throws AtlasBaseException {
        KeycloakClient.getKeycloakClient().getRealm().roles().create(role);

        return KeycloakClient.getKeycloakClient().getRealm()
                .roles()
                .get(role.getName())
                .toRepresentation();
    }

    public RoleRepresentation getRole(String roleName) throws AtlasBaseException {
        RoleRepresentation roleRepresentation = null;
        try{
            roleRepresentation =  KeycloakClient.getKeycloakClient().getRealm()
                    .roles()
                    .get(roleName)
                    .toRepresentation();
        } catch (NotFoundException e) {
            return null;
        }
        return roleRepresentation;
    }

    public void updateRoleUsers(String roleName,
                                List<String> existingUsers, List<String> newUsers,
                                RoleRepresentation roleRepresentation) throws AtlasBaseException {

        if (roleRepresentation == null) {
            throw new AtlasBaseException("Failed to updateRoleUsers as roleRepresentation is null");
        }

        if (newUsers == null) {
            newUsers = new ArrayList<>();
        }

        if (existingUsers == null) {
            existingUsers = new ArrayList<>();
        }

        List<String> usersToAdd     = (List<String>) CollectionUtils.removeAll(newUsers, existingUsers);
        List<String> usersToRemove  = (List<String>) CollectionUtils.removeAll(existingUsers, newUsers);

        UsersResource usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

        for (String userName : usersToAdd) {
            LOG.info("Adding user {} to role {}", userName, roleName);
            List<UserRepresentation> matchedUsers = usersResource.search(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

            if (keyUserOptional.isPresent()) {
                final UserResource userResource = usersResource.get(keyUserOptional.get().getId());

                userResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));
                userResource.update(keyUserOptional.get());
            } else {
                throw new AtlasBaseException("Keycloak user not found with userName " + userName);
            }
        }

        for (String userName : usersToRemove) {
            LOG.info("Removing user {} from role {}", userName, roleName);
            List<UserRepresentation> matchedUsers = usersResource.search(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

            if (keyUserOptional.isPresent()) {
                final UserResource userResource = usersResource.get(keyUserOptional.get().getId());

                userResource.roles().realmLevel().remove(Collections.singletonList(roleRepresentation));
                userResource.update(keyUserOptional.get());
            } else {
                LOG.warn("Keycloak user not found with userName " + userName);
            }
        }
    }

    public void updateRoleGroups(String roleName,
                                 List<String> existingGroups, List<String> newGroups,
                                 RoleRepresentation roleRepresentation) throws AtlasBaseException {

        if (roleRepresentation == null) {
            throw new AtlasBaseException("Failed to updateRoleGroups as roleRepresentation is null");
        }

        GroupsResource groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

        if (newGroups == null) {
            newGroups = new ArrayList<>();
        }

        if (existingGroups == null) {
            existingGroups = new ArrayList<>();
        }

        List<String> groupsToAdd    = (List<String>) CollectionUtils.removeAll(newGroups, existingGroups);
        List<String> groupsToRemove = (List<String>) CollectionUtils.removeAll(existingGroups, newGroups);

        for (String groupName : groupsToAdd) {
            LOG.info("Adding group {} to role {}", groupName, roleName);
            List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

            if (keyGroupOptional.isPresent()) {
                final GroupResource groupResource = groupsResource.group(keyGroupOptional.get().getId());

                groupResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));
                groupResource.update(keyGroupOptional.get());
            } else {
                throw new AtlasBaseException("Keycloak group not found with userName " + groupName);
            }
        }

        for (String groupName : groupsToRemove) {
            LOG.info("removing group {} from role {}", groupName, roleName);
            List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

            if (keyGroupOptional.isPresent()) {
                final GroupResource groupResource = groupsResource.group(keyGroupOptional.get().getId());

                groupResource.roles().realmLevel().remove(Collections.singletonList(roleRepresentation));
                groupResource.update(keyGroupOptional.get());
            } else {
                LOG.warn("Keycloak group not found with userName " + groupName);
            }
        }
    }

    public void updateRoleRoles(String roleName,
                                List<String> existingRoles, List<String> newRoles,
                                RoleResource connRoleResource, RoleRepresentation roleRepresentation) throws AtlasBaseException {

        if (roleRepresentation == null) {
            throw new AtlasBaseException("Failed to updateRoleRoles as roleRepresentation is null");
        }

        RolesResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().roles();
        RoleByIdResource rolesIdResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();

        if (newRoles == null) {
            newRoles = new ArrayList<>();
        }

        if (existingRoles == null) {
            existingRoles = new ArrayList<>();
        }

        List<String> rolesToAdd    = (List<String>) CollectionUtils.removeAll(newRoles, existingRoles);
        List<String> rolesToRemove = (List<String>) CollectionUtils.removeAll(existingRoles, newRoles);

        for (String subRoleId : rolesToAdd) {
            LOG.info("Adding role {} to role {}", subRoleId, roleName);
            RoleRepresentation keyRole = getRoleById(rolesIdResource, subRoleId);
            RoleResource subrRoleResource = rolesResource.get(keyRole.getName());

            connRoleResource.addComposites(Collections.singletonList(subrRoleResource.toRepresentation()));
            connRoleResource.update(connRoleResource.toRepresentation());
        }

        for (String subRoleId : rolesToRemove) {
            LOG.info("removing role {} from role {}", subRoleId, roleName);
            RoleRepresentation keyRole = getRoleById(rolesIdResource, subRoleId);
            RoleResource subrRoleResource = rolesResource.get(keyRole.getName());

            connRoleResource.deleteComposites(Collections.singletonList(subrRoleResource.toRepresentation()));
            connRoleResource.update(connRoleResource.toRepresentation());
        }
    }

    public void removeRole(String roleId) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(roleId)) {
            RoleByIdResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();

            rolesResource.deleteRole(roleId);
            LOG.info("Removed keycloak role with id {}", roleId);
        }
    }
    public void removeRoleByName(String roleName) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(roleName)) {
            RoleResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().roles().get(roleName);

            rolesResource.remove();
            LOG.info("Removed keycloak role with name {}", roleName);
        }
    }

    private RoleRepresentation getRoleById(RoleByIdResource idResource, String roleId) throws AtlasBaseException {

        try {
            return idResource.getRole(roleId);
        } catch (NotFoundException nfe) {
            LOG.error("Role not found with id {}", roleId);
            throw new AtlasBaseException(RESOURCE_NOT_FOUND, "Role with id " + roleId);
        } catch (Exception e) {
            LOG.error("Role not found with id/name {}: {}", roleId, e.getMessage());
            throw new AtlasBaseException(e);
        }
    }
}
