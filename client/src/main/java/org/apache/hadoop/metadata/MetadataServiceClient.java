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

package org.apache.hadoop.metadata;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Client for metadata.
 */
public class MetadataServiceClient {
    public static final String REQUEST_ID = "requestId";
    public static final String RESULTS = "results";
    public static final String TOTAL_SIZE = "totalSize";


    private final WebResource service;

    public MetadataServiceClient(String baseUrl) {
        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }

    static enum API {
        //Type operations
        CREATE_TYPE("api/metadata/types/submit", HttpMethod.POST),
        GET_TYPE("api/metadata/types/definition", HttpMethod.GET),
        LIST_TYPES("api/metadata/types/list", HttpMethod.GET),
        LIST_TRAIT_TYPES("api/metadata/types/traits/list", HttpMethod.GET),

        //Entity operations
        CREATE_ENTITY("api/metadata/entities/submit", HttpMethod.POST),
        GET_ENTITY("api/metadata/entities/definition", HttpMethod.GET),
        UPDATE_ENTITY("api/metadata/entities/update", HttpMethod.PUT),
        LIST_ENTITY("api/metadata/entities/list", HttpMethod.GET),

        //Trait operations
        ADD_TRAITS("api/metadata/traits/add", HttpMethod.POST),
        DELETE_TRAITS("api/metadata/traits/delete", HttpMethod.PUT),
        LIST_TRAITS("api/metadata/traits/list", HttpMethod.GET);

        private final String method;
        private final String path;

        API(String path, String method) {
            this.path = path;
            this.method = method;
        }

        public String getMethod() {
            return method;
        }

        public String getPath() {
            return path;
        }
    }

    public JSONObject createType(String typeAsJson) throws MetadataServiceException {
        return callAPI(API.CREATE_TYPE, typeAsJson);
    }

    public List<String> listTypes() throws MetadataServiceException {
        try {
            final JSONObject jsonObject = callAPI(API.LIST_TYPES, null);
            final JSONArray list = jsonObject.getJSONArray(MetadataServiceClient.RESULTS);
            ArrayList<String> types = new ArrayList<>();
            for (int index = 0; index < list.length(); index++) {
                types.add(list.getString(index));
            }

            return types;
        } catch (JSONException e) {
            throw new MetadataServiceException(API.LIST_TYPES, e);
        }
    }

    public JSONObject createEntity(String entityAsJson) throws MetadataServiceException {
        return callAPI(API.CREATE_ENTITY, entityAsJson);
    }

    public String getRequestId(JSONObject json) throws MetadataServiceException {
        try {
            return json.getString(REQUEST_ID);
        } catch (JSONException e) {
            throw new MetadataServiceException(e);
        }
    }

    private JSONObject callAPI(API api, Object requestObject, String... pathParams) throws MetadataServiceException {
        WebResource resource = service.path(api.getPath());
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(api.getMethod(), ClientResponse.class, requestObject);

        if (clientResponse.getStatus() == Response.Status.OK.getStatusCode()) {
            String responseAsString = clientResponse.getEntity(String.class);
            try {
                return new JSONObject(responseAsString);
            } catch (JSONException e) {
                throw new MetadataServiceException(api, e);
            }
        }
        throw new MetadataServiceException(api, clientResponse.getClientResponseStatus());
    }
}
