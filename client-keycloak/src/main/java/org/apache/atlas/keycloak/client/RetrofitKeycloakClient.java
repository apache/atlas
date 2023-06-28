package org.apache.atlas.keycloak.client;

import org.keycloak.representations.idm.*;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.List;
import java.util.Set;

public interface RetrofitKeycloakClient {

    /* Keycloak Users */

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/users")
    Call<List<UserRepresentation>> searchUserByUserName(@Path("realmId") String realmId, @Query("username") String username);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/users/{id}")
    Call<UserRepresentation> getUserById(@Path("realmId") String realmId, @Query("id") String id);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/users")
    Call<List<UserRepresentation>> getAllUsers(@Path("realmId") String realmId, @Query("first") Integer first, @Query("max") Integer max);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles/{role-name}/users")
    Call<List<UserRepresentation>> getRoleUserMembers(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles/{role-name}/users")
    Call<Set<UserRepresentation>> getRoleUserMembers(@Path("realmId") String realmId, @Path("role-name") String roleName, @Query("first") Integer first, @Query("max") Integer max);


    /* Keycloak Groups */

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/groups")
    Call<List<GroupRepresentation>> searchGroupByName(@Path("realmId") String realmId, @Query("search") String groupName, @Query("first") Integer first, @Query("max") Integer max);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles/{role-name}/groups")
    Call<List<GroupRepresentation>> getRoleGroupMembers(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles/{role-name}/groups")
    Call<Set<GroupRepresentation>> getRoleGroupMembers(@Path("realmId") String realmId, @Path("role-name") String roleName, @Query("first") Integer first, @Query("max") Integer max);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/users/{id}/groups")
    Call<List<GroupRepresentation>> getGroupsForUserById(@Path("realmId") String realmId, @Path("id") String userId);


    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/groups")
    Call<List<GroupRepresentation>> getAllGroups(@Path("realmId") String realmId, @Query("first") Integer first, @Query("max") Integer max);

    @Headers("Accept: application/json")
    @POST("admin/realms/{realmId}/groups/{id}/role-mappings/realm")
    Call<Void> addRealmLevelRoleMappingsForGroup(@Path("realmId") String realmId, @Path("id") String groupId, @Body List<RoleRepresentation> roles);

    @Headers("Accept: application/json")
    @DELETE("admin/realms/{realmId}/groups/{id}/role-mappings/realm")
    Call<Void> deleteRealmLevelRoleMappingsForGroup(@Path("realmId") String realmId, @Path("id") String groupId, @Body List<RoleRepresentation> roles);


    /* Keycloak Roles */

    @Headers("Accept: application/json")
    @POST("admin/realms/{realmId}/users/{id}/role-mappings/realm")
    Call<List<RoleRepresentation>> addRealmLevelRoleMappingsForUser(@Path("realmId") String realmId, @Path("id") String userId, @Body List<RoleRepresentation> roles);

    @Headers("Accept: application/json")
    @DELETE("admin/realms/{realmId}/users/{id}/role-mappings/realm")
    Call<Void> deleteRealmLevelRoleMappingsForUser(@Path("realmId") String realmId, @Path("id") String userId, @Body List<RoleRepresentation> roles);


    @Headers("Accept: application/json")
    @POST("admin/realms/{realmId}/roles")
    Call<Void> createRole(@Path("realmId") String realmId, @Body RoleRepresentation role);

    @Headers("Accept: application/json")
    @POST("admin/realms/{realmId}/roles/{role-id}")
    Call<Void> updateRole(@Path("realmId") String realmId, @Path("role-id") String roleId, @Body RoleRepresentation role);


    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles-by-id/{role-id}")
    Call<RoleRepresentation> getRoleById(@Path("realmId") String realmId, @Path("role-id") String roleId);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles/{role-name}")
    Call<RoleRepresentation> getRoleByName(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles")
    Call<List<RoleRepresentation>> getAllRoles(@Path("realmId") String realmId, @Query("first") Integer first, @Query("max") Integer max);


    @Headers("Accept: application/json")
    @DELETE("admin/realms/{realmId}/roles-by-id/{role-id}")
    Call<Void> deleteRoleById(@Path("realmId") String realmId, @Query("role-id") String roleId);

    @Headers("Accept: application/json")
    @DELETE("admin/realms/{realmId}/roles/{role-name}}")
    Call<Void> deleteRoleByName(@Path("realmId") String realmId, @Query("role-name") String roleName);

    /* Keycloak composites */

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/roles/{role-name}/composites")
    Call<Set<RoleRepresentation>> getRoleComposites(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers("Accept: application/json")
    @POST("admin/realms/{realmId}/roles/{role-name}/composites")
    Call<Void> addComposites(@Path("realmId") String realmId, @Path("role-name") String roleName, @Body List<RoleRepresentation> roles);

    @Headers("Accept: application/json")
    @DELETE("admin/realms/{realmId}/roles/{role-name}/composites")
    Call<Void> deleteComposites(@Path("realmId") String realmId, @Path("role-name") String roleName, @Body List<RoleRepresentation> roles);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/admin-events")
    Call<List<AdminEventRepresentation>> getAdminEvents(@Path("realmId") String realmId, @Query("operationTypes") List<String> var1, @Query("authRealm") String var2, @Query("authClient") String var3, @Query("authUser") String var4, @Query("authIpAddress") String var5, @Query("resourcePath") String var6, @Query("dateFrom") String var7, @Query("dateTo") String var8, @Query("first") Integer var9, @Query("max") Integer var10);

    @Headers("Accept: application/json")
    @GET("admin/realms/{realmId}/events")
    Call<List<EventRepresentation>> getEvents(@Path("realmId") String realmId, @Query("type") List<String> var1, @Query("client") String var2, @Query("user") String var3, @Query("dateFrom") String var4, @Query("dateTo") String var5, @Query("ipAddress") String var6, @Query("first") Integer var7, @Query("max") Integer var8);

}
