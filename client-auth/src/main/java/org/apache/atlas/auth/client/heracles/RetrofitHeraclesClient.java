package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.heracles.models.HeraclesGroupViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesGroupsResponse;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Query;

import java.util.List;

public interface RetrofitHeraclesClient {
    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/users/mappings")
    Call<List<HeraclesUserViewRepresentation>> getUsersMapping(@Query("offset") Integer offset, @Query("limit") Integer limit, @Query("sort") String sort,
                                                               @Query("columns") String[] columns);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/roles/mappings")
    Call<List<HeraclesRoleViewRepresentation>> getRolesMapping(@Query("offset") Integer offset, @Query("limit") Integer limit, @Query("sort") String sort,
                                                               @Query("columns") String[] columns);

    /**
     * List groups from Heracles API (v2) with relation lookups
     * API returns a wrapped response with records array
     * 
     * @param offset Offset for pagination
     * @param limit The numbers of items to return  
     * @param sort Column names for sorting (+/-)
     * @param columns Column names to project
     * @param filter Filter string
     * @param count Whether to process count
     * @param relations Column names to lookup
     * @return Wrapped response containing list of groups
     */
    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/v2/groups")
    Call<HeraclesGroupsResponse> getGroupsV2(@Query("offset") Integer offset, 
                                             @Query("limit") Integer limit, 
                                             @Query("sort") String[] sort,
                                             @Query("columns") String[] columns,
                                             @Query("filter") String filter,
                                             @Query("count") Boolean count,
                                             @Query("relations") String[] relations);
}
