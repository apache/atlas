package org.apache.atlas.auth.client.keycloak;

import org.apache.atlas.auth.client.auth.AbstractAuthClient;
import okhttp3.FormBody;
import okhttp3.RequestBody;
import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.exception.AtlasBaseException;
import org.keycloak.representations.idm.*;
import org.keycloak.representations.oidc.TokenMetadataRepresentation;
import retrofit2.Response;

import java.util.List;
import java.util.Set;

/**
 * Keycloak Rest client wrapper used in atlas metastore
 */
public final class KeycloakRestClient extends AbstractAuthClient {

    private static final String TOKEN = "token";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    public KeycloakRestClient(final AuthConfig authConfig) {
        super(authConfig);
    }

    public Response<List<UserRepresentation>> searchUserByUserName(String username) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.searchUserByUserName(this.authConfig.getRealmId(), username));
    }

    public Response<List<UserRepresentation>> getAllUsers(int start, int size) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getAllUsers(this.authConfig.getRealmId(), start, size));
    }

    public Response<List<UserRepresentation>> getRoleUserMembers(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleUserMembers(this.authConfig.getRealmId(), roleName));
    }

    public Response<Set<UserRepresentation>> getRoleUserMembers(String roleName, Integer start, Integer size) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleUserMembers(this.authConfig.getRealmId(), roleName, start, size));
    }

    public Response<List<GroupRepresentation>> searchGroupByName(String groupName, Integer start, Integer size) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.searchGroupByName(this.authConfig.getRealmId(), groupName, start, size));
    }

    public Response<List<GroupRepresentation>> getRoleGroupMembers(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleGroupMembers(this.authConfig.getRealmId(), roleName));
    }

    public Response<Set<GroupRepresentation>> getRoleGroupMembers(String roleName, Integer first, Integer size) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleGroupMembers(this.authConfig.getRealmId(), roleName, first, size));
    }

    public Response<List<GroupRepresentation>> getGroupsForUserById(String userId) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getGroupsForUserById(this.authConfig.getRealmId(), userId));
    }

    public void addRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.addRealmLevelRoleMappingsForGroup(this.authConfig.getRealmId(), groupId, roles));
    }

    public void deleteRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.deleteRealmLevelRoleMappingsForGroup(this.authConfig.getRealmId(), groupId, roles));
    }

    public Response<List<RoleRepresentation>> getAllRoles(int start, int size) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getAllRoles(this.authConfig.getRealmId(), start, size));
    }

    public void deleteRoleById(String roleId) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.deleteRoleById(this.authConfig.getRealmId(), roleId));
    }

    public void deleteRoleByName(String roleName) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.deleteRoleByName(this.authConfig.getRealmId(), roleName));
    }

    public Response<List<RoleRepresentation>> addRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.addRealmLevelRoleMappingsForUser(this.authConfig.getRealmId(), userId, roles));
    }

    public void deleteRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.deleteRealmLevelRoleMappingsForUser(this.authConfig.getRealmId(), userId, roles));
    }

    public void createRole(RoleRepresentation roleRepresentation) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.createRole(this.authConfig.getRealmId(), roleRepresentation));
    }

    public void updateRole(String roleId, RoleRepresentation roleRepresentation) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.updateRole(this.authConfig.getRealmId(), roleId, roleRepresentation));
    }

    public Response<RoleRepresentation> getRoleById(String roleId) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleById(this.authConfig.getRealmId(), roleId));
    }

    public Response<RoleRepresentation> getRoleByName(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleByName(this.authConfig.getRealmId(), roleName));
    }

    public Response<List<RoleRepresentation>> getAllRoles(Integer first, Integer max) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getAllRoles(this.authConfig.getRealmId(), first, max));
    }

    public Response<Set<RoleRepresentation>> getRoleComposites(String roleName) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getRoleComposites(this.authConfig.getRealmId(), roleName));
    }

    public void addComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.addComposites(this.authConfig.getRealmId(), roleName, roles));
    }

    public void deleteComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        processResponse(this.retrofitKeycloakClient.deleteComposites(this.authConfig.getRealmId(), roleName, roles));
    }

    public Response<List<AdminEventRepresentation>> getAdminEvents(List<String> operationTypes, String authRealm,
                                                                   String authClient, String authUser, String authIpAddress,
                                                                   String resourcePath, String dateFrom, String dateTo,
                                                                   Integer first, Integer max) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getAdminEvents(this.authConfig.getRealmId(), operationTypes,
                authRealm, authClient, authUser, authIpAddress, resourcePath, dateFrom, dateTo, first, max));
    }

    public Response<List<EventRepresentation>> getEvents(List<String> type, String client, String user, String dateFrom,
                                                         String dateTo, String ipAddress, Integer first, Integer max) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.getEvents(this.authConfig.getRealmId(), type, client, user, dateFrom, dateTo, ipAddress, first, max));
    }

    public Response<TokenMetadataRepresentation> introspectToken(String token) throws AtlasBaseException {
        return processResponse(this.retrofitKeycloakClient.introspectToken(this.authConfig.getRealmId(), getIntrospectTokenRequest(token)));
    }

    private RequestBody getIntrospectTokenRequest(String token) {
        return new FormBody.Builder()
                .addEncoded(TOKEN, token)
                .addEncoded(CLIENT_ID, this.authConfig.getClientId())
                .addEncoded(CLIENT_SECRET, this.authConfig.getClientSecret())
                .build();
    }
}
