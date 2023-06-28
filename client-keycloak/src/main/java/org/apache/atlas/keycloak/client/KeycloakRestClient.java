package org.apache.atlas.keycloak.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.config.KeycloakConfig;
import org.apache.http.HttpStatus;
import org.keycloak.representations.idm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.atlas.AtlasErrorCode.RESOURCE_NOT_FOUND;

final class KeycloakRestClient {

    public static final Logger LOG = LoggerFactory.getLogger(KeycloakRestClient.class);

    private final KeycloakConfig keycloakConfig;
    private final OkHttpClient okHttpClient;
    private final RetrofitKeycloakClient retrofit;

    public KeycloakRestClient(final KeycloakConfig keycloakConfig) {
        this.keycloakConfig = keycloakConfig;
        this.okHttpClient = new OkHttpClient.Builder().addInterceptor(noCacheInterceptor).addInterceptor(errorHandlingInterceptor).build();
        this.retrofit = new Retrofit.Builder().client(okHttpClient).baseUrl(this.keycloakConfig.getAuthServerUrl()).addConverterFactory(JacksonConverterFactory.create(new ObjectMapper())).build().create(RetrofitKeycloakClient.class);
    }

    public KeycloakConfig getConfig() {
        return this.keycloakConfig;
    }

    public void close() {
        this.okHttpClient.dispatcher().executorService().shutdownNow();
    }

    Interceptor noCacheInterceptor = chain -> {
        Request request = chain.request();
        Request.Builder requestBuilder = request.newBuilder().addHeader("Cache-Control", "no-store").addHeader("Cache-Control", "no-cache");
        return chain.proceed(requestBuilder.build());
    };

    Interceptor errorHandlingInterceptor = chain -> {
        Request request = chain.request();
        okhttp3.Response response = chain.proceed(request);
        if (!response.isSuccessful()) {
            LOG.error("Request for url {} failed:, {}", request.url(), valueOf(response.body()));
        }
        return response;
    };

    public Response<List<UserRepresentation>> searchUserByUserName(String username) throws AtlasBaseException {
        try {
            return this.retrofit.searchUserByUserName(this.keycloakConfig.getRealmId(), username).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'searchUserByUserName'", e);
            throw new AtlasBaseException("Request for url failed: 'searchUserByUserName' request");
        }
    }

    public Response<UserRepresentation> getUserById(String userId) throws AtlasBaseException {
        try {
            return this.retrofit.getUserById(this.keycloakConfig.getRealmId(), userId).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getUserById'", e);
            throw new AtlasBaseException("Request for url failed: 'getUserById' request");
        }
    }

    public Response<List<UserRepresentation>> getAllUsers(int start, int size) throws AtlasBaseException {
        try {
            return this.retrofit.getAllUsers(this.keycloakConfig.getRealmId(), start, size).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getAllUsers'", e);
            throw new AtlasBaseException("Request for url failed: 'getAllUsers' request");
        }
    }

    public Response<List<UserRepresentation>> getRoleUserMembers(String roleName) throws AtlasBaseException {
        try {
            return this.retrofit.getRoleUserMembers(this.keycloakConfig.getRealmId(), roleName).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleUserMembers'", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleUserMembers' request");
        }
    }

    public Response<Set<UserRepresentation>> getRoleUserMembers(String roleName, Integer start, Integer size) throws AtlasBaseException {
        try {
            return this.retrofit.getRoleUserMembers(this.keycloakConfig.getRealmId(), roleName, start, size).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleUserMembers' set", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleUserMembers' set request");
        }
    }

    public Response<List<GroupRepresentation>> searchGroupByName(String groupName, Integer start, Integer size) throws AtlasBaseException {
        try {
            return this.retrofit.searchGroupByName(this.keycloakConfig.getRealmId(), groupName, start, size).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'searchGroupByName'", e);
            throw new AtlasBaseException("Request for url failed: 'searchGroupByName' request");
        }
    }

    public Response<List<GroupRepresentation>> getRoleGroupMembers(String roleName) throws AtlasBaseException {
        try {
            return this.retrofit.getRoleGroupMembers(this.keycloakConfig.getRealmId(), roleName).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleGroupMembers'", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleGroupMembers' request");
        }
    }

    public Response<Set<GroupRepresentation>> getRoleGroupMembers(String roleName, Integer first, Integer size) throws AtlasBaseException {
        try {
            return this.retrofit.getRoleGroupMembers(this.keycloakConfig.getRealmId(), roleName, first, size).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleGroupMembers set'", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleGroupMembers set' request");
        }
    }

    public Response<List<GroupRepresentation>> getGroupsForUserById(String userId) throws AtlasBaseException {
        try {
            return this.retrofit.getGroupsForUserById(this.keycloakConfig.getRealmId(), userId).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getGroupsForUserById set'", e);
            throw new AtlasBaseException("Request for url failed: 'getGroupsForUserById set' request");
        }
    }

    public Response<List<GroupRepresentation>> getAllGroups(int start, int size) throws AtlasBaseException {
        try {
            return this.retrofit.getAllGroups(this.keycloakConfig.getRealmId(), start, size).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getAllGroups'", e);
            throw new AtlasBaseException("Request for url failed: 'getAllGroups' request");
        }
    }

    public void addRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        try {
            this.retrofit.addRealmLevelRoleMappingsForGroup(this.keycloakConfig.getRealmId(), groupId, roles).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'addRealmLevelRoleMappingsForGroup'", e);
            throw new AtlasBaseException("Request for url failed: 'addRealmLevelRoleMappingsForGroup' request");
        }
    }

    public void deleteRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        try {
            this.retrofit.deleteRealmLevelRoleMappingsForGroup(this.keycloakConfig.getRealmId(), groupId, roles).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'deleteRealmLevelRoleMappingsForGroup'", e);
            throw new AtlasBaseException("Request for url failed: 'deleteRealmLevelRoleMappingsForGroup' request");
        }
    }

    public Response<List<RoleRepresentation>> getAllRoles(int start, int size) throws AtlasBaseException {
        try {
            return this.retrofit.getAllRoles(this.keycloakConfig.getRealmId(), start, size).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getAllRoles'", e);
            throw new AtlasBaseException("Request for url failed: 'getAllRoles' request");
        }
    }

    public void deleteRoleById(String roleId) throws AtlasBaseException {
        try {
            this.retrofit.deleteRoleById(this.keycloakConfig.getRealmId(), roleId).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'deleteRoleById'", e);
            throw new AtlasBaseException("Request for url failed: 'deleteRoleById' request");
        }
    }

    public void deleteRoleByName(String roleName) throws AtlasBaseException {
        try {
            this.retrofit.deleteRoleByName(this.keycloakConfig.getRealmId(), roleName).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'deleteRoleByName'", e);
            throw new AtlasBaseException("Request for url failed: 'deleteRoleByName' request");
        }
    }

    public Response<List<RoleRepresentation>> addRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        try {
            return this.retrofit.addRealmLevelRoleMappingsForUser(this.keycloakConfig.getRealmId(), userId, roles).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRealmLevelRoleMappingsForUser'", e);
            throw new AtlasBaseException("Request for url failed: 'getRealmLevelRoleMappingsForUser' request");
        }
    }

    public void deleteRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        try {
            this.retrofit.deleteRealmLevelRoleMappingsForUser(this.keycloakConfig.getRealmId(), userId, roles).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'deleteRealmLevelRoleMappingsForUser'", e);
            throw new AtlasBaseException("Request for url failed: 'deleteRealmLevelRoleMappingsForUser' request");
        }
    }

    public void createRole(RoleRepresentation roleRepresentation) throws AtlasBaseException {
        try {
            this.retrofit.createRole(this.keycloakConfig.getRealmId(), roleRepresentation).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'createRole'", e);
            throw new AtlasBaseException("Request for url failed: 'createRole' request");
        }
    }

    public void updateRole(String roleId, RoleRepresentation roleRepresentation) throws AtlasBaseException {
        try {
            this.retrofit.updateRole(this.keycloakConfig.getRealmId(), roleId, roleRepresentation).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'updateRole'", e);
            throw new AtlasBaseException("Request for url failed: 'updateRole' request");
        }
    }

    public Response<RoleRepresentation> getRoleById(String roleId) throws AtlasBaseException {
        try {
            return this.retrofit.getRoleById(this.keycloakConfig.getRealmId(), roleId).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleById'", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleById' request");
        }
    }

    public Response<RoleRepresentation> getRoleByName(String roleName) throws AtlasBaseException {
        Response response = null;
        try {
            response = this.retrofit.getRoleByName(this.keycloakConfig.getRealmId(), roleName).execute();
            if (response.code() == HttpStatus.SC_NOT_FOUND) {
                throw new AtlasBaseException(RESOURCE_NOT_FOUND);
            }
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleByName'", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleByName' request");
        }
        return response;
    }

    public Response<List<RoleRepresentation>> getAllRoles(Integer first, Integer max) throws AtlasBaseException {
        try {
            return this.retrofit.getAllRoles(this.keycloakConfig.getRealmId(), first, max).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getAllRoles'", e);
            throw new AtlasBaseException("Request for url failed: 'getAllRoles' request");
        }
    }

    public Response<Set<RoleRepresentation>> getRoleComposites(String roleName) throws AtlasBaseException {
        try {
            return this.retrofit.getRoleComposites(this.keycloakConfig.getRealmId(), roleName).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getRoleComposites'", e);
            throw new AtlasBaseException("Request for url failed: 'getRoleComposites' request");
        }
    }

    public void addComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        try {
            this.retrofit.addComposites(this.keycloakConfig.getRealmId(), roleName, roles).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'addComposites'", e);
            throw new AtlasBaseException("Request for url failed: 'addComposites' request");
        }
    }

    public void deleteComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        try {
            this.retrofit.deleteComposites(this.keycloakConfig.getRealmId(), roleName, roles).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'deleteComposites'", e);
            throw new AtlasBaseException("Request for url failed: 'deleteComposites' request");
        }
    }

    public Response<List<AdminEventRepresentation>> getAdminEvents(List<String> operationTypes, String authRealm, String authClient, String authUser, String authIpAddress, String resourcePath, String dateFrom, String dateTo, Integer first, Integer max) throws AtlasBaseException {
        try {
            return this.retrofit.getAdminEvents(this.keycloakConfig.getRealmId(), operationTypes, authRealm, authClient, authUser, authIpAddress, resourcePath, dateFrom, dateTo, first, max).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getAdminEvents'", e);
            throw new AtlasBaseException("Request for url failed: 'getAdminEvents' request");
        }
    }

    public Response<List<EventRepresentation>> getEvents(List<String> type, String client, String user, String dateFrom, String dateTo, String ipAddress, Integer first, Integer max) throws AtlasBaseException {
        try {
            return this.retrofit.getEvents(this.keycloakConfig.getRealmId(), type, client, user, dateFrom, dateTo, ipAddress, first, max).execute();
        } catch (IOException e) {
            LOG.error("Request for url failed: 'getAdminEvents'", e);
            throw new AtlasBaseException("Request for url failed: 'getAdminEvents' request");
        }
    }
}
