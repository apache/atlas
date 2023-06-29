package org.apache.atlas.keycloak.client;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.config.KeycloakConfig;
import org.keycloak.representations.idm.*;
import retrofit2.Response;

import java.util.List;
import java.util.Set;

/**
 * Keycloak Rest client wrapper used in atlas metastore
 */
public final class KeycloakRestClient extends AbstractKeycloakClient {

    public KeycloakRestClient(final KeycloakConfig keycloakConfig) {
        super(keycloakConfig);
    }

    public Response<List<UserRepresentation>> searchUserByUserName(String username) throws AtlasBaseException {
        return processResponse(this.retrofit.searchUserByUserName(this.keycloakConfig.getRealmId(), username));
    }

    public Response<List<UserRepresentation>> getAllUsers(int start, int size) throws AtlasBaseException {
        return processResponse(this.retrofit.getAllUsers(this.keycloakConfig.getRealmId(), start, size));
    }

    public Response<List<UserRepresentation>> getRoleUserMembers(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleUserMembers(this.keycloakConfig.getRealmId(), roleName));
    }

    public Response<Set<UserRepresentation>> getRoleUserMembers(String roleName, Integer start, Integer size) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleUserMembers(this.keycloakConfig.getRealmId(), roleName, start, size));
    }

    public Response<List<GroupRepresentation>> searchGroupByName(String groupName, Integer start, Integer size) throws AtlasBaseException {
        return processResponse(this.retrofit.searchGroupByName(this.keycloakConfig.getRealmId(), groupName, start, size));
    }

    public Response<List<GroupRepresentation>> getRoleGroupMembers(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleGroupMembers(this.keycloakConfig.getRealmId(), roleName));
    }

    public Response<Set<GroupRepresentation>> getRoleGroupMembers(String roleName, Integer first, Integer size) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleGroupMembers(this.keycloakConfig.getRealmId(), roleName, first, size));
    }

    public Response<List<GroupRepresentation>> getGroupsForUserById(String userId) throws AtlasBaseException {
        return processResponse(this.retrofit.getGroupsForUserById(this.keycloakConfig.getRealmId(), userId));
    }

    public Response<List<GroupRepresentation>> getAllGroups(int start, int size) throws AtlasBaseException {
        return processResponse(this.retrofit.getAllGroups(this.keycloakConfig.getRealmId(), start, size));
    }

    public void addRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofit.addRealmLevelRoleMappingsForGroup(this.keycloakConfig.getRealmId(), groupId, roles));
    }

    public void deleteRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofit.deleteRealmLevelRoleMappingsForGroup(this.keycloakConfig.getRealmId(), groupId, roles));
    }

    public Response<List<RoleRepresentation>> getAllRoles(int start, int size) throws AtlasBaseException {
        return processResponse(this.retrofit.getAllRoles(this.keycloakConfig.getRealmId(), start, size));
    }

    public void deleteRoleById(String roleId) throws AtlasBaseException {
        processResponse(this.retrofit.deleteRoleById(this.keycloakConfig.getRealmId(), roleId));
    }

    public void deleteRoleByName(String roleName) throws AtlasBaseException {
        processResponse(this.retrofit.deleteRoleByName(this.keycloakConfig.getRealmId(), roleName));
    }

    public Response<List<RoleRepresentation>> addRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        return processResponse(this.retrofit.addRealmLevelRoleMappingsForUser(this.keycloakConfig.getRealmId(), userId, roles));
    }

    public void deleteRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofit.deleteRealmLevelRoleMappingsForUser(this.keycloakConfig.getRealmId(), userId, roles));
    }

    public void createRole(RoleRepresentation roleRepresentation) throws AtlasBaseException {
        processResponse(this.retrofit.createRole(this.keycloakConfig.getRealmId(), roleRepresentation));
    }

    public void updateRole(String roleId, RoleRepresentation roleRepresentation) throws AtlasBaseException {
        processResponse(this.retrofit.updateRole(this.keycloakConfig.getRealmId(), roleId, roleRepresentation));
    }

    public Response<RoleRepresentation> getRoleById(String roleId) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleById(this.keycloakConfig.getRealmId(), roleId));
    }

    public Response<RoleRepresentation> getRoleByName(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleByName(this.keycloakConfig.getRealmId(), roleName));
    }

    public Response<List<RoleRepresentation>> getAllRoles(Integer first, Integer max) throws AtlasBaseException {
        return processResponse(this.retrofit.getAllRoles(this.keycloakConfig.getRealmId(), first, max));
    }

    public Response<Set<RoleRepresentation>> getRoleComposites(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofit.getRoleComposites(this.keycloakConfig.getRealmId(), roleName));
    }

    public void addComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofit.addComposites(this.keycloakConfig.getRealmId(), roleName, roles));
    }

    public void deleteComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofit.deleteComposites(this.keycloakConfig.getRealmId(), roleName, roles));
    }

    public Response<List<AdminEventRepresentation>> getAdminEvents(List<String> operationTypes, String authRealm,
                                                                   String authClient, String authUser, String authIpAddress,
                                                                   String resourcePath, String dateFrom, String dateTo,
                                                                   Integer first, Integer max) throws AtlasBaseException {
        return processResponse(this.retrofit.getAdminEvents(this.keycloakConfig.getRealmId(), operationTypes,
                authRealm, authClient, authUser, authIpAddress, resourcePath, dateFrom, dateTo, first, max));
    }

    public Response<List<EventRepresentation>> getEvents(List<String> type, String client, String user, String dateFrom,
                                                         String dateTo, String ipAddress, Integer first, Integer max) throws AtlasBaseException {
        return processResponse(this.retrofit.getEvents(this.keycloakConfig.getRealmId(), type, client, user, dateFrom, dateTo, ipAddress, first, max));
    }
}
