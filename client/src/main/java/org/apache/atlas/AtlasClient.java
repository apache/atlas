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

package org.apache.atlas;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.atlas.security.SecureClientUtils;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
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

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;

/**
 * Client for metadata.
 */
public class AtlasClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClient.class);
    public static final String NAME = "name";
    public static final String GUID = "GUID";
    public static final String TYPENAME = "typeName";

    public static final String DEFINITION = "definition";
    public static final String ERROR = "error";
    public static final String STACKTRACE = "stackTrace";
    public static final String REQUEST_ID = "requestId";
    public static final String RESULTS = "results";
    public static final String COUNT = "count";
    public static final String ROWS = "rows";

    public static final String BASE_URI = "api/atlas/";
    public static final String TYPES = "types";
    public static final String URI_ENTITIES = "entities";
    public static final String URI_TRAITS = "traits";
    public static final String URI_SEARCH = "discovery/search";
    public static final String URI_LINEAGE = "lineage/hive/table";

    public static final String QUERY = "query";
    public static final String QUERY_TYPE = "queryType";
    public static final String ATTRIBUTE_NAME = "property";
    public static final String ATTRIBUTE_VALUE = "value";


    public static final String INFRASTRUCTURE_SUPER_TYPE = "Infrastructure";
    public static final String DATA_SET_SUPER_TYPE = "DataSet";
    public static final String PROCESS_SUPER_TYPE = "Process";

    public static final String JSON_MEDIA_TYPE = MediaType.APPLICATION_JSON + "; charset=UTF-8";

    private WebResource service;

    public AtlasClient(String baseUrl) {
        this(baseUrl, null, null);
    }

    public AtlasClient(String baseUrl, UserGroupInformation ugi, String doAsUser) {
        DefaultClientConfig config = new DefaultClientConfig();
        PropertiesConfiguration clientConfig = null;
        try {
            clientConfig = getClientProperties();
            if (clientConfig.getBoolean(TLS_ENABLED, false)) {
                // create an SSL properties configuration if one doesn't exist.  SSLFactory expects a file, so forced
                // to create a
                // configuration object, persist it, then subsequently pass in an empty configuration to SSLFactory
                SecureClientUtils.persistSSLClientConfiguration(clientConfig);
            }
        } catch (Exception e) {
            LOG.info("Error processing client configuration.", e);
        }

        URLConnectionClientHandler handler =
            SecureClientUtils.getClientConnectionHandler(config, clientConfig, doAsUser, ugi);

        Client client = new Client(handler, config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }

    protected PropertiesConfiguration getClientProperties() throws AtlasException {
        return PropertiesUtil.getClientProperties();
    }

    enum API {

        //Type operations
        CREATE_TYPE(BASE_URI + TYPES, HttpMethod.POST),
        GET_TYPE(BASE_URI + TYPES, HttpMethod.GET),
        LIST_TYPES(BASE_URI + TYPES, HttpMethod.GET),
        LIST_TRAIT_TYPES(BASE_URI + TYPES + "?type=trait", HttpMethod.GET),

        //Entity operations
        CREATE_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.POST),
        GET_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.GET),
        UPDATE_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.PUT),
        LIST_ENTITY(BASE_URI + URI_ENTITIES, HttpMethod.GET),

        //Trait operations
        ADD_TRAITS(BASE_URI + URI_TRAITS, HttpMethod.POST),
        DELETE_TRAITS(BASE_URI + URI_TRAITS, HttpMethod.DELETE),
        LIST_TRAITS(BASE_URI + URI_TRAITS, HttpMethod.GET),

        //Search operations
        SEARCH(BASE_URI + URI_SEARCH, HttpMethod.GET),
        SEARCH_DSL(BASE_URI + URI_SEARCH + "/dsl", HttpMethod.GET),
        SEARCH_GREMLIN(BASE_URI + URI_SEARCH + "/gremlin", HttpMethod.GET),
        SEARCH_FULL_TEXT(BASE_URI + URI_SEARCH + "/fulltext", HttpMethod.GET),

        //Lineage operations
        LINEAGE_INPUTS_GRAPH(BASE_URI + URI_LINEAGE, HttpMethod.GET),
        LINEAGE_OUTPUTS_GRAPH(BASE_URI + URI_LINEAGE, HttpMethod.GET),
        LINEAGE_SCHEMA(BASE_URI + URI_LINEAGE, HttpMethod.GET);

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

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject createType(String typeAsJson) throws AtlasServiceException {
        return callAPI(API.CREATE_TYPE, typeAsJson);
    }

    public List<String> listTypes() throws AtlasServiceException {
        try {
            final JSONObject jsonObject = callAPI(API.LIST_TYPES, null);
            final JSONArray list = jsonObject.getJSONArray(AtlasClient.RESULTS);
            ArrayList<String> types = new ArrayList<>();
            for (int index = 0; index < list.length(); index++) {
                types.add(list.getString(index));
            }

            return types;
        } catch (JSONException e) {
            throw new AtlasServiceException(API.LIST_TYPES, e);
        }
    }

    public String getType(String typeName) throws AtlasServiceException {
        WebResource resource = getResource(API.GET_TYPE, typeName);
        try {
            JSONObject response = callAPIWithResource(API.GET_TYPE, resource);
            return response.getString(DEFINITION);
        } catch (AtlasServiceException e) {
            if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }
            throw e;
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Create the given entity
     * @param entityAsJson entity(type instance) as json
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject createEntity(String entityAsJson) throws AtlasServiceException {
        return callAPI(API.CREATE_ENTITY, entityAsJson);
    }

    /**
     * Get an entity given the entity id
     * @param guid entity id
     * @return result json object
     * @throws AtlasServiceException
     */
    public Referenceable getEntity(String guid) throws AtlasServiceException {
        JSONObject jsonResponse = callAPI(API.GET_ENTITY, null, guid);
        try {
            String entityInstanceDefinition = jsonResponse.getString(AtlasClient.DEFINITION);
            return InstanceSerialization.fromJsonReferenceable(entityInstanceDefinition, true);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Updates property for the entity corresponding to guid
     * @param guid      guid
     * @param property  property key
     * @param value     property value
     */
    public JSONObject updateEntity(String guid, String property, String value) throws AtlasServiceException {
        WebResource resource = getResource(API.UPDATE_ENTITY, guid);
        resource = resource.queryParam(ATTRIBUTE_NAME, property);
        resource = resource.queryParam(ATTRIBUTE_VALUE, value);
        return callAPIWithResource(API.UPDATE_ENTITY, resource);
    }

    public JSONObject searchEntity(String searchQuery) throws AtlasServiceException {
        WebResource resource = getResource(API.SEARCH);
        resource = resource.queryParam(QUERY, searchQuery);
        return callAPIWithResource(API.SEARCH, resource);
    }

    /**
     * Search given type name, an attribute and its value. Uses search dsl
     * @param typeName name of the entity type
     * @param attributeName attribute name
     * @param attributeValue attribute value
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONArray rawSearch(String typeName, String attributeName, Object attributeValue)
            throws AtlasServiceException {
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
     * @throws AtlasServiceException
     */
    public JSONArray searchByDSL(String query) throws AtlasServiceException {
        LOG.debug("DSL query: {}", query);
        WebResource resource = getResource(API.SEARCH_DSL);
        resource = resource.queryParam(QUERY, query);
        JSONObject result = callAPIWithResource(API.SEARCH_DSL, resource);
        try {
            return result.getJSONObject(RESULTS).getJSONArray(ROWS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Search given gremlin query
     * @param gremlinQuery Gremlin query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject searchByGremlin(String gremlinQuery) throws AtlasServiceException {
        LOG.debug("Gremlin query: " + gremlinQuery);
        WebResource resource = getResource(API.SEARCH_GREMLIN);
        resource = resource.queryParam(QUERY, gremlinQuery);
        return callAPIWithResource(API.SEARCH_GREMLIN, resource);
    }

    /**
     * Search given full text search
     * @param query Query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject searchByFullText(String query) throws AtlasServiceException {
        WebResource resource = getResource(API.SEARCH_FULL_TEXT);
        resource = resource.queryParam(QUERY, query);
        return callAPIWithResource(API.SEARCH_FULL_TEXT, resource);
    }

    public JSONObject getInputGraph(String datasetName) throws AtlasServiceException {
        JSONObject response = callAPI(API.LINEAGE_INPUTS_GRAPH, null, datasetName, "/inputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public JSONObject getOutputGraph(String datasetName) throws AtlasServiceException {
        JSONObject response = callAPI(API.LINEAGE_OUTPUTS_GRAPH, null, datasetName, "/outputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public String getRequestId(JSONObject json) throws AtlasServiceException {
        try {
            return json.getString(REQUEST_ID);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
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

    private JSONObject callAPIWithResource(API api, WebResource resource) throws AtlasServiceException {
        return callAPIWithResource(api, resource, null);
    }

    private JSONObject callAPIWithResource(API api, WebResource resource, Object requestObject)
    throws AtlasServiceException {
        ClientResponse clientResponse = resource.accept(JSON_MEDIA_TYPE).type(JSON_MEDIA_TYPE)
                .method(api.getMethod(), ClientResponse.class, requestObject);

        Response.Status expectedStatus =
                HttpMethod.POST.equals(api.getMethod()) ? Response.Status.CREATED : Response.Status.OK;
        if (clientResponse.getStatus() == expectedStatus.getStatusCode()) {
            String responseAsString = clientResponse.getEntity(String.class);
            try {
                return new JSONObject(responseAsString);
            } catch (JSONException e) {
                throw new AtlasServiceException(api, e);
            }
        }

        throw new AtlasServiceException(api, clientResponse);
    }

    private JSONObject callAPI(API api, Object requestObject, String... pathParams) throws AtlasServiceException {
        WebResource resource = getResource(api, pathParams);
        return callAPIWithResource(api, resource, requestObject);
    }
}
