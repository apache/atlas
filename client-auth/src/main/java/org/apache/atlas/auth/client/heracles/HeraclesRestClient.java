package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.auth.client.auth.AbstractAuthClient;
import org.apache.atlas.auth.client.heracles.models.HeraclesGroupViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesGroupsResponse;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import org.apache.atlas.exception.AtlasBaseException;
import retrofit2.Response;

import java.util.List;

public class HeraclesRestClient extends AbstractAuthClient {

    public HeraclesRestClient(final AuthConfig authConfig) {
        super(authConfig);
    }
    
    public Response<List<HeraclesUserViewRepresentation>> getUsersMappings(int offset, int limit, String sort, String[] columns) throws AtlasBaseException {
        return processResponse(this.retrofitHeraclesClient.getUsersMapping(offset, limit, sort, columns));
    }

    public Response<List<HeraclesRoleViewRepresentation>> getRolesMappings(int offset, int limit, String sort, String[] columns) throws AtlasBaseException {
        return processResponse(this.retrofitHeraclesClient.getRolesMapping(offset, limit, sort, columns));
    }

    /**
     * Fetch groups from Heracles API (v2) with relation lookups
     * 
     * @param offset Offset for pagination
     * @param limit The numbers of items to return
     * @param sort Column names for sorting (+/-)
     * @param columns Column names to project
     * @param filter Filter string (optional)
     * @param count Whether to process count (optional)
     * @param relations Column names to lookup (e.g., users, roles)
     * @return Response containing wrapped groups response
     */
    public Response<HeraclesGroupsResponse> getGroupsV2(int offset, int limit, String[] sort, String[] columns, String filter, Boolean count, String[] relations) throws AtlasBaseException {
        return processResponse(this.retrofitHeraclesClient.getGroupsV2(offset, limit, sort, columns, filter, count, relations));
    }
}