package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.auth.client.auth.AbstractAuthClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.auth.client.heracles.models.HeraclesUsersRepresentation;
import retrofit2.Response;

public class HeraclesRestClient extends AbstractAuthClient {

    public HeraclesRestClient(final AuthConfig authConfig) {
        super(authConfig);
    }
    public Response<HeraclesUsersRepresentation> getUsers(int offset,int limit, String columns, String filter, String sort) throws AtlasBaseException {
        return processResponse(this.retrofitHeraclesClient.getUsers(offset, columns, filter, limit,sort));
    }

}
