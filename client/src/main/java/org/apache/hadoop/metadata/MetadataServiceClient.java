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
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.security.SecureClientUtils;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.json.InstanceSerialization;
import org.apache.hadoop.metadata.typesystem.json.Serialization;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.metadata.security.SecurityProperties.TLS_ENABLED;

/**
 * Client for metadata.
 */
public class MetadataServiceClient {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceClient.class);
    public static final String REQUEST_ID = "requestId";
    public static final String RESULTS = "results";
    public static final String COUNT = "count";
    public static final String ROWS = "rows";

    private static final String BASE_URI = "api/metadata/";
    private static final String URI_TYPES = "types";
    private static final String URI_ENTITIES = "entities";
    private static final String URI_TRAITS = "traits";
    private static final String URI_SEARCH = "discovery/search";


    private WebResource service;

    public MetadataServiceClient(String baseUrl) {
        DefaultClientConfig config = new DefaultClientConfig();
        PropertiesConfiguration clientConfig = null;
        try {
            clientConfig = getClientProperties();
            if (clientConfig.getBoolean(TLS_ENABLED)  || clientConfig.getString("metadata.http.authentication.type") != null) {
                // create an SSL properties configuration if one doesn't exist.  SSLFactory expects a file, so forced to create a
                // configuration object, persist it, then subsequently pass in an empty configuration to SSLFactory
                SecureClientUtils.persistSSLClientConfiguration(clientConfig);
            }
        } catch (Exception e) {
            LOG.info("Error processing client configuration.", e);
        }
        URLConnectionClientHandler handler = SecureClientUtils.getClientConnectionHandler(config, clientConfig);

        Client client = new Client(handler, config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }

    protected PropertiesConfiguration getClientProperties() throws MetadataException {
        return PropertiesUtil.getClientProperties();
    }

    static enum API {
        //Type operations
        CREATE_TYPE(BASE_URI + URI_TYPES, HttpMethod.POST),
        GET_TYPE(BASE_URI + URI_TYPES, HttpMethod.GET),
        LIST_TYPES(BASE_URI + URI_TYPES, HttpMethod.GET),
        LIST_TRAIT_TYPES(BASE_URI + URI_TYPES + "?type=trait", HttpMethod.GET),

        //Entity operations
        CREATE_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.POST),
        GET_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.GET),
        UPDATE_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.PUT),
        LIST_ENTITY(BASE_URI + URI_ENTITIES + "?type=", HttpMethod.GET),

        //Trait operations
        ADD_TRAITS(BASE_URI + URI_TRAITS, HttpMethod.POST),
        DELETE_TRAITS(BASE_URI + URI_TRAITS, HttpMethod.DELETE),
        LIST_TRAITS(BASE_URI + URI_TRAITS, HttpMethod.GET),

        //Search operations
        SEARCH(BASE_URI + URI_SEARCH, HttpMethod.GET),
        SEARCH_DSL(BASE_URI + URI_SEARCH + "/dsl", HttpMethod.GET),
        SEARCH_GREMLIN(BASE_URI + URI_SEARCH + "/gremlin", HttpMethod.GET),
        SEARCH_FULL_TEXT(BASE_URI + URI_SEARCH + "/fulltext", HttpMethod.GET);

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

    public String getType(String typeName) throws MetadataServiceException {
        WebResource resource = getResource(API.GET_TYPE, typeName);
        try {
            JSONObject response = callAPIWithResource(API.GET_TYPE, resource);
            return response.getString("definition");
        } catch (MetadataServiceException e) {
            if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }
            throw e;
        } catch (JSONException e) {
            throw new MetadataServiceException(e);
        }
    }

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws MetadataServiceException
     */
    public JSONObject createType(String typeAsJson) throws MetadataServiceException {
        return callAPI(API.CREATE_TYPE, typeAsJson);
    }

    /**
     * Create the given entity
     * @param entityAsJson entity(type instance) as json
     * @return result json object
     * @throws MetadataServiceException
     */
    public JSONObject createEntity(String entityAsJson) throws MetadataServiceException {
        return callAPI(API.CREATE_ENTITY, entityAsJson);
    }

    /**
     * Get an entity given the entity id
     * @param guid entity id
     * @return result json object
     * @throws MetadataServiceException
     */
    public Referenceable getEntity(String guid) throws MetadataServiceException {
        JSONObject jsonResponse = callAPI(API.GET_ENTITY, null, guid);
        try {
            String entityInstanceDefinition = jsonResponse.getString(MetadataServiceClient.RESULTS);
            return InstanceSerialization.fromJsonReferenceable(entityInstanceDefinition, true);
        } catch (JSONException e) {
            throw new MetadataServiceException(e);
        }
    }

    public JSONObject searchEntity(String searchQuery) throws MetadataServiceException {
        WebResource resource = getResource(API.SEARCH);
        resource = resource.queryParam("query", searchQuery);
        return callAPIWithResource(API.SEARCH, resource);
    }

    /**
     * Search given type name, an attribute and its value. Uses search dsl
     * @param typeName name of the entity type
     * @param attributeName attribute name
     * @param attributeValue attribute value
     * @return result json object
     * @throws MetadataServiceException
     */
    public JSONArray rawSearch(String typeName, String attributeName, Object attributeValue) throws
            MetadataServiceException {
//        String gremlinQuery = String.format(
//                "g.V.has(\"typeName\",\"%s\").and(_().has(\"%s.%s\", T.eq, \"%s\")).toList()",
//                typeName, typeName, attributeName, attributeValue);
//        return searchByGremlin(gremlinQuery);
        String dslQuery = String.format("%s where %s = \"%s\"", typeName, attributeName, attributeValue);
        return searchByDSL(dslQuery);
    }

    /**
     * Search given query DSL
     * @param query DSL query
     * @return result json object
     * @throws MetadataServiceException
     */
    public JSONArray searchByDSL(String query) throws MetadataServiceException {
        WebResource resource = getResource(API.SEARCH_DSL);
        resource = resource.queryParam("query", query);
        JSONObject result = callAPIWithResource(API.SEARCH_DSL, resource);
        try {
            return result.getJSONObject("results").getJSONArray("rows");
        } catch (JSONException e) {
            throw new MetadataServiceException(e);
        }
    }

    /**
     * Search given gremlin query
     * @param gremlinQuery Gremlin query
     * @return result json object
     * @throws MetadataServiceException
     */
    public JSONObject searchByGremlin(String gremlinQuery) throws MetadataServiceException {
        WebResource resource = getResource(API.SEARCH_GREMLIN);
        resource = resource.queryParam("query", gremlinQuery);
        return callAPIWithResource(API.SEARCH_GREMLIN, resource);
    }

    /**
     * Search given full text search
     * @param query Query
     * @return result json object
     * @throws MetadataServiceException
     */
    public JSONObject searchByFullText(String query) throws MetadataServiceException {
        WebResource resource = getResource(API.SEARCH_FULL_TEXT);
        resource = resource.queryParam("query", query);
        return callAPIWithResource(API.SEARCH_FULL_TEXT, resource);
    }

    public String getRequestId(JSONObject json) throws MetadataServiceException {
        try {
            return json.getString(REQUEST_ID);
        } catch (JSONException e) {
            throw new MetadataServiceException(e);
        }
    }

    private WebResource getResource(API api, String... pathParams) {
        WebResource resource = service.path(api.getPath());
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }
        return resource;
    }

    private JSONObject callAPIWithResource(API api, WebResource resource) throws MetadataServiceException {
        return callAPIWithResource(api, resource, null);
    }

    private JSONObject callAPIWithResource(API api, WebResource resource, Object requestObject)
            throws MetadataServiceException {
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

        throw new MetadataServiceException(api, clientResponse);
    }

    private JSONObject callAPI(API api, Object requestObject,
                               String... pathParams) throws MetadataServiceException {
        WebResource resource = getResource(api, pathParams);
        return callAPIWithResource(api, resource, requestObject);
    }
}
