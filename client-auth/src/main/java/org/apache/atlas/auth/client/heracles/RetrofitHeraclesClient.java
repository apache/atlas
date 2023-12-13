package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUsersRepresentation;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Query;

import java.util.List;

public interface RetrofitHeraclesClient {
    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/users")
    Call<HeraclesUsersRepresentation> getUsers(@Query("offset") Integer offset, @Query("columns") String columns,
                                                  @Query(value = "filter", encoded = true) String filter, @Query("limit") Integer limit,
                                                  @Query("sort") String sort);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/users/view")
    Call<List<HeraclesUserViewRepresentation>> getUsersView(@Query("offset") Integer offset, @Query("limit") Integer limit, @Query("sort") String sort);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/roles/view")
    Call<List<HeraclesRoleViewRepresentation>> getRolesView(@Query("offset") Integer offset, @Query("limit") Integer limit, @Query("sort") String sort);

}
