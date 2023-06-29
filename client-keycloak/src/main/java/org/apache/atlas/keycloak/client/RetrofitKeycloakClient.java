package org.apache.atlas.keycloak.client;

import okhttp3.RequestBody;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.representations.idm.*;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.List;
import java.util.Set;

public interface RetrofitKeycloakClient {

    /* Keycloak Users */

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/users")
    Call<List<UserRepresentation>> searchUserByUserName(@Path("realmId") String realmId, @Query("username") String username);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/users")
    Call<List<UserRepresentation>> getAllUsers(@Path("realmId") String realmId, @Query("first") Integer first,
                                               @Query("max") Integer max);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles/{role-name}/users")
    Call<List<UserRepresentation>> getRoleUserMembers(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles/{role-name}/users")
    Call<Set<UserRepresentation>> getRoleUserMembers(@Path("realmId") String realmId, @Path("role-name") String roleName,
                                                     @Query("first") Integer first, @Query("max") Integer max);


    /* Keycloak Groups */

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/groups")
    Call<List<GroupRepresentation>> searchGroupByName(@Path("realmId") String realmId, @Query("search") String groupName,
                                                      @Query("first") Integer first, @Query("max") Integer max);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles/{role-name}/groups")
    Call<List<GroupRepresentation>> getRoleGroupMembers(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles/{role-name}/groups")
    Call<Set<GroupRepresentation>> getRoleGroupMembers(@Path("realmId") String realmId, @Path("role-name") String roleName,
                                                       @Query("first") Integer first, @Query("max") Integer max);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/users/{id}/groups")
    Call<List<GroupRepresentation>> getGroupsForUserById(@Path("realmId") String realmId, @Path("id") String userId);


    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/groups")
    Call<List<GroupRepresentation>> getAllGroups(@Path("realmId") String realmId, @Query("first") Integer first,
                                                 @Query("max") Integer max);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @POST("admin/realms/{realmId}/groups/{id}/role-mappings/realm")
    Call<Void> addRealmLevelRoleMappingsForGroup(@Path("realmId") String realmId, @Path("id") String groupId, @Body List<RoleRepresentation> roles);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @DELETE("admin/realms/{realmId}/groups/{id}/role-mappings/realm")
    Call<Void> deleteRealmLevelRoleMappingsForGroup(@Path("realmId") String realmId, @Path("id") String groupId, @Body List<RoleRepresentation> roles);


    /* Keycloak Roles */

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @POST("admin/realms/{realmId}/users/{id}/role-mappings/realm")
    Call<List<RoleRepresentation>> addRealmLevelRoleMappingsForUser(@Path("realmId") String realmId, @Path("id") String userId,
                                                                    @Body List<RoleRepresentation> roles);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @DELETE("admin/realms/{realmId}/users/{id}/role-mappings/realm")
    Call<Void> deleteRealmLevelRoleMappingsForUser(@Path("realmId") String realmId, @Path("id") String userId,
                                                   @Body List<RoleRepresentation> roles);


    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @POST("admin/realms/{realmId}/roles")
    Call<Void> createRole(@Path("realmId") String realmId, @Body RoleRepresentation role);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @POST("admin/realms/{realmId}/roles/{role-id}")
    Call<Void> updateRole(@Path("realmId") String realmId, @Path("role-id") String roleId, @Body RoleRepresentation role);


    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles-by-id/{role-id}")
    Call<RoleRepresentation> getRoleById(@Path("realmId") String realmId, @Path("role-id") String roleId);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles/{role-name}")
    Call<RoleRepresentation> getRoleByName(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles")
    Call<List<RoleRepresentation>> getAllRoles(@Path("realmId") String realmId, @Query("first") Integer first,
                                               @Query("max") Integer max);


    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @DELETE("admin/realms/{realmId}/roles-by-id/{role-id}")
    Call<Void> deleteRoleById(@Path("realmId") String realmId, @Query("role-id") String roleId);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @DELETE("admin/realms/{realmId}/roles/{role-name}}")
    Call<Void> deleteRoleByName(@Path("realmId") String realmId, @Query("role-name") String roleName);

    /* Keycloak composites */

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/roles/{role-name}/composites")
    Call<Set<RoleRepresentation>> getRoleComposites(@Path("realmId") String realmId, @Path("role-name") String roleName);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @POST("admin/realms/{realmId}/roles/{role-name}/composites")
    Call<Void> addComposites(@Path("realmId") String realmId, @Path("role-name") String roleName, @Body List<RoleRepresentation> roles);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @DELETE("admin/realms/{realmId}/roles/{role-name}/composites")
    Call<Void> deleteComposites(@Path("realmId") String realmId, @Path("role-name") String roleName, @Body List<RoleRepresentation> roles);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/admin-events")
    Call<List<AdminEventRepresentation>> getAdminEvents(@Path("realmId") String realmId, @Query("operationTypes") List<String> operationTypes,
                                                        @Query("authRealm") String authRealm, @Query("authClient") String authClient,
                                                        @Query("authUser") String authUser, @Query("authIpAddress") String authIpAddress,
                                                        @Query("resourcePath") String resourcePath, @Query("dateFrom") String dateFrom,
                                                        @Query("dateTo") String dateTo, @Query("first") Integer first, @Query("max") Integer max);

    @Headers({"Accept: application/json", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("admin/realms/{realmId}/events")
    Call<List<EventRepresentation>> getEvents(@Path("realmId") String realmId, @Query("type") List<String> types,
                                              @Query("client") String client, @Query("user") String user,
                                              @Query("dateFrom") String dateFrom, @Query("dateTo") String dateTo,
                                              @Query("ipAddress") String ipAddress, @Query("first") Integer first,
                                              @Query("max") Integer max);

    /* Access token */

    @Headers({"Accept: application/json", "Content-Type: application/x-www-form-urlencoded", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @POST("realms/{realmId}/protocol/openid-connect/token")
    Call<AccessTokenResponse> grantToken(@Path("realmId") String realmId, @Body RequestBody request);

}
