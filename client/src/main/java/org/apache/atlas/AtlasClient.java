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

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.atlas.security.SecureClientUtils;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
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
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;

/**
 * Client for metadata.
 */
public class AtlasClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClient.class);
    public static final String NAME = "name";
    public static final String GUID = "GUID";
    public static final String TYPE = "type";
    public static final String TYPENAME = "typeName";

    public static final String DEFINITION = "definition";
    public static final String ERROR = "error";
    public static final String STACKTRACE = "stackTrace";
    public static final String REQUEST_ID = "requestId";
    public static final String RESULTS = "results";
    public static final String COUNT = "count";
    public static final String ROWS = "rows";
    public static final String DATATYPE = "dataType";

    public static final String EVENTS = "events";
    public static final String START_KEY = "startKey";
    public static final String NUM_RESULTS = "count";

    public static final String BASE_URI = "api/atlas/";
    public static final String ADMIN_VERSION = "admin/version";
    public static final String ADMIN_STATUS = "admin/status";
    public static final String TYPES = "types";
    public static final String URI_ENTITY = "entities";
    public static final String URI_ENTITY_AUDIT = "audit";
    public static final String URI_SEARCH = "discovery/search";
    public static final String URI_LINEAGE = "lineage/hive/table";

    public static final String QUERY = "query";
    public static final String QUERY_TYPE = "queryType";
    public static final String ATTRIBUTE_NAME = "property";
    public static final String ATTRIBUTE_VALUE = "value";


    public static final String INFRASTRUCTURE_SUPER_TYPE = "Infrastructure";
    public static final String DATA_SET_SUPER_TYPE = "DataSet";
    public static final String PROCESS_SUPER_TYPE = "Process";
    public static final String REFERENCEABLE_SUPER_TYPE = "Referenceable";
    public static final String REFERENCEABLE_ATTRIBUTE_NAME = "qualifiedName";

    public static final String PROCESS_ATTRIBUTE_INPUTS = "inputs";
    public static final String PROCESS_ATTRIBUTE_OUTPUTS = "outputs";

    public static final String JSON_MEDIA_TYPE = MediaType.APPLICATION_JSON + "; charset=UTF-8";
    public static final String UNKNOWN_STATUS = "Unknown status";

    public static final String ATLAS_CLIENT_HA_RETRIES_KEY = "atlas.client.ha.retries";
    // Setting the default value based on testing failovers while client code like quickstart is running.
    public static final int DEFAULT_NUM_RETRIES = 4;
    public static final String ATLAS_CLIENT_HA_SLEEP_INTERVAL_MS_KEY = "atlas.client.ha.sleep.interval.ms";
    // Setting the default value based on testing failovers while client code like quickstart is running.
    // With number of retries, this gives a total time of about 20s for the server to start.
    public static final int DEFAULT_SLEEP_BETWEEN_RETRIES_MS = 5000;

    private WebResource service;
    private AtlasClientContext atlasClientContext;
    private Configuration configuration;

    /**
     * Create a new AtlasClient.
     *
     * @param baseUrl The URL of the Atlas server to connect to.
     */
    public AtlasClient(String baseUrl) {
        this(baseUrl, null, null);
    }

    /**
     * Create a new Atlas Client.
     * @param baseUrl The URL of the Atlas server to connect to.
     * @param ugi The {@link UserGroupInformation} of logged in user.
     * @param doAsUser The user on whose behalf queries will be executed.
     */
    public AtlasClient(String baseUrl, UserGroupInformation ugi, String doAsUser) {
        initializeState(new String[] {baseUrl}, ugi, doAsUser);
    }

    /**
     * Create a new Atlas client.
     * @param ugi The {@link UserGroupInformation} of logged in user, can be null in unsecure mode.
     * @param doAsUser The user on whose behalf queries will be executed, can be null in unsecure mode.
     * @param baseUrls A list of URLs that point to an ensemble of Atlas servers working in
     *                 High Availability mode. The client will automatically determine the
     *                 active instance on startup and also when there is a scenario of
     *                 failover.
     */
    public AtlasClient(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        initializeState(baseUrls, ugi, doAsUser);
    }

    private void initializeState(String[] baseUrls, UserGroupInformation ugi, String doAsUser) {
        configuration = getClientProperties();
        Client client = getClient(configuration, ugi, doAsUser);
        String activeServiceUrl = determineActiveServiceURL(baseUrls, client);
        atlasClientContext = new AtlasClientContext(baseUrls, client, ugi, doAsUser);
        service = client.resource(UriBuilder.fromUri(activeServiceUrl).build());
    }

    @VisibleForTesting
    protected Client getClient(Configuration configuration, UserGroupInformation ugi, String doAsUser) {
        DefaultClientConfig config = new DefaultClientConfig();
        Configuration clientConfig = null;
        int readTimeout = 60000;
        int connectTimeout = 60000;
        try {
            clientConfig = configuration;
            if (clientConfig.getBoolean(TLS_ENABLED, false)) {
                // create an SSL properties configuration if one doesn't exist.  SSLFactory expects a file, so forced
                // to create a
                // configuration object, persist it, then subsequently pass in an empty configuration to SSLFactory
                SecureClientUtils.persistSSLClientConfiguration(clientConfig);
            }
            readTimeout = clientConfig.getInt("atlas.client.readTimeoutMSecs", readTimeout);
            connectTimeout = clientConfig.getInt("atlas.client.connectTimeoutMSecs", connectTimeout);
        } catch (Exception e) {
            LOG.info("Error processing client configuration.", e);
        }

        URLConnectionClientHandler handler =
            SecureClientUtils.getClientConnectionHandler(config, clientConfig, doAsUser, ugi);

        Client client = new Client(handler, config);
        client.setReadTimeout(readTimeout);
        client.setConnectTimeout(connectTimeout);
        return client;
    }

    @VisibleForTesting
    protected String determineActiveServiceURL(String[] baseUrls, Client client) {
        if (baseUrls.length == 0) {
            throw new IllegalArgumentException("Base URLs cannot be null or empty");
        }
        String baseUrl;
        AtlasServerEnsemble atlasServerEnsemble = new AtlasServerEnsemble(baseUrls);
        if (atlasServerEnsemble.hasSingleInstance()) {
            baseUrl = atlasServerEnsemble.firstURL();
            LOG.info("Client has only one service URL, will use that for all actions: {}", baseUrl);
            return baseUrl;
        } else {
            try {
                baseUrl = selectActiveServerAddress(client, atlasServerEnsemble);
            } catch (AtlasServiceException e) {
                LOG.error("None of the passed URLs are active: {}", atlasServerEnsemble, e);
                throw new IllegalArgumentException("None of the passed URLs are active " + atlasServerEnsemble, e);
            }
        }
        return baseUrl;
    }

    private String selectActiveServerAddress(Client client, AtlasServerEnsemble serverEnsemble)
            throws AtlasServiceException {
        List<String> serverInstances = serverEnsemble.getMembers();
        String activeServerAddress = null;
        for (String serverInstance : serverInstances) {
            LOG.info("Trying with address {}", serverInstance);
            activeServerAddress = getAddressIfActive(client, serverInstance);
            if (activeServerAddress != null) {
                LOG.info("Found service {} as active service.", serverInstance);
                break;
            }
        }
        if (activeServerAddress != null)
            return activeServerAddress;
        else
            throw new AtlasServiceException(API.STATUS, new RuntimeException("Could not find any active instance"));
    }

    private String getAddressIfActive(Client client, String serverInstance) {
        String activeServerAddress = null;
        for (int i = 0; i < getNumberOfRetries(); i++) {
            try {
                WebResource service = client.resource(UriBuilder.fromUri(serverInstance).build());
                String adminStatus = getAdminStatus(service);
                if (adminStatus.equals("ACTIVE")) {
                    activeServerAddress = serverInstance;
                    break;
                } else {
                    LOG.info("Service {} is not active.. will retry.", serverInstance);
                }
            } catch (Exception e) {
                LOG.error("Could not get status from service {} after {} tries.", serverInstance, i, e);
            }
            sleepBetweenRetries();
            LOG.warn("Service {} is not active.", serverInstance);
        }
        return activeServerAddress;
    }

    private void sleepBetweenRetries(){
        try {
            Thread.sleep(getSleepBetweenRetriesMs());
        } catch (InterruptedException e) {
            LOG.error("Interrupted from sleeping between retries.", e);
        }
    }

    private int getSleepBetweenRetriesMs() {
        return configuration.getInt(ATLAS_CLIENT_HA_SLEEP_INTERVAL_MS_KEY, DEFAULT_SLEEP_BETWEEN_RETRIES_MS);
    }

    private int getNumberOfRetries() {
        return configuration.getInt(ATLAS_CLIENT_HA_RETRIES_KEY, DEFAULT_NUM_RETRIES);
    }

    @VisibleForTesting
    AtlasClient(WebResource service, Configuration configuration) {
        this.service = service;
        this.configuration = configuration;
    }

    protected Configuration getClientProperties() {
        try {
            if (configuration == null) {
                configuration = ApplicationProperties.get();
            }
        } catch (AtlasException e) {
            LOG.error("Exception while loading configuration.", e);
        }
        return configuration;
    }

    public boolean isServerReady() throws AtlasServiceException {
        WebResource resource = getResource(API.VERSION);
        try {
            callAPIWithResource(API.VERSION, resource, null);
            return true;
        } catch (ClientHandlerException che) {
            return false;
        } catch (AtlasServiceException ase) {
            if (ase.getStatus().equals(ClientResponse.Status.SERVICE_UNAVAILABLE)) {
                LOG.warn("Received SERVICE_UNAVAILABLE, server is not yet ready");
                return false;
            }
            throw ase;
        }
    }

    /**
     * Return status of the service instance the client is pointing to.
     *
     * @return One of the values in ServiceState.ServiceStateValue or {@link #UNKNOWN_STATUS} if there is a JSON parse
     * exception
     * @throws AtlasServiceException if there is a HTTP error.
     */
    public String getAdminStatus() throws AtlasServiceException {
        return getAdminStatus(service);
    }

    private void handleClientHandlerException(ClientHandlerException che) {
        if (isRetryableException(che)) {
            atlasClientContext.getClient().destroy();
            LOG.warn("Destroyed current context while handling ClientHandlerEception.");
            LOG.warn("Will retry and create new context.");
            sleepBetweenRetries();
            initializeState(atlasClientContext.getBaseUrls(), atlasClientContext.getUgi(),
                    atlasClientContext.getDoAsUser());
            return;
        }
        throw che;
    }

    private boolean isRetryableException(ClientHandlerException che) {
        return che.getCause().getClass().equals(IOException.class)
                || che.getCause().getClass().equals(ConnectException.class);
    }

    private String getAdminStatus(WebResource service) throws AtlasServiceException {
        String result = UNKNOWN_STATUS;
        WebResource resource = getResource(service, API.STATUS);
        JSONObject response = callAPIWithResource(API.STATUS, resource, null);
        try {
            result = response.getString("Status");
        } catch (JSONException e) {
            LOG.error("Exception while parsing admin status response. Returned response {}", response.toString(), e);
        }
        return result;
    }

    public enum API {

        //Admin operations
        VERSION(BASE_URI + ADMIN_VERSION, HttpMethod.GET, Response.Status.OK),
        STATUS(BASE_URI + ADMIN_STATUS, HttpMethod.GET, Response.Status.OK),

        //Type operations
        CREATE_TYPE(BASE_URI + TYPES, HttpMethod.POST, Response.Status.CREATED),
        UPDATE_TYPE(BASE_URI + TYPES, HttpMethod.PUT, Response.Status.OK),
        GET_TYPE(BASE_URI + TYPES, HttpMethod.GET, Response.Status.OK),
        LIST_TYPES(BASE_URI + TYPES, HttpMethod.GET, Response.Status.OK),
        LIST_TRAIT_TYPES(BASE_URI + TYPES + "?type=trait", HttpMethod.GET, Response.Status.OK),

        //Entity operations
        CREATE_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.POST, Response.Status.CREATED),
        GET_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.GET, Response.Status.OK),
        UPDATE_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.PUT, Response.Status.OK),
        UPDATE_ENTITY_PARTIAL(BASE_URI + URI_ENTITY, HttpMethod.POST, Response.Status.OK),
        LIST_ENTITIES(BASE_URI + URI_ENTITY, HttpMethod.GET, Response.Status.OK),
        DELETE_ENTITIES(BASE_URI + URI_ENTITY, HttpMethod.DELETE, Response.Status.OK),
        DELETE_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.DELETE, Response.Status.OK),

        //audit operation
        LIST_ENTITY_AUDIT(BASE_URI + URI_ENTITY, HttpMethod.GET, Response.Status.OK),

        //Trait operations
        ADD_TRAITS(BASE_URI + URI_ENTITY, HttpMethod.POST, Response.Status.CREATED),
        DELETE_TRAITS(BASE_URI + URI_ENTITY, HttpMethod.DELETE, Response.Status.OK),
        LIST_TRAITS(BASE_URI + URI_ENTITY, HttpMethod.GET, Response.Status.OK),

        //Search operations
        SEARCH(BASE_URI + URI_SEARCH, HttpMethod.GET, Response.Status.OK),
        SEARCH_DSL(BASE_URI + URI_SEARCH + "/dsl", HttpMethod.GET, Response.Status.OK),
        SEARCH_GREMLIN(BASE_URI + URI_SEARCH + "/gremlin", HttpMethod.GET, Response.Status.OK),
        SEARCH_FULL_TEXT(BASE_URI + URI_SEARCH + "/fulltext", HttpMethod.GET, Response.Status.OK),

        //Lineage operations
        LINEAGE_INPUTS_GRAPH(BASE_URI + URI_LINEAGE, HttpMethod.GET, Response.Status.OK),
        LINEAGE_OUTPUTS_GRAPH(BASE_URI + URI_LINEAGE, HttpMethod.GET, Response.Status.OK),
        LINEAGE_SCHEMA(BASE_URI + URI_LINEAGE, HttpMethod.GET, Response.Status.OK);

        private final String method;
        private final String path;
        private final Response.Status status;

        API(String path, String method, Response.Status status) {
            this.path = path;
            this.method = method;
            this.status = status;
        }

        public String getMethod() {
            return method;
        }

        public String getPath() {
            return path;
        }
        
        public Response.Status getExpectedStatus() { return status; }
    }

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws AtlasServiceException
     */
    public List<String> createType(String typeAsJson) throws AtlasServiceException {
        JSONObject response = callAPI(API.CREATE_TYPE, typeAsJson);
        return extractResults(response, AtlasClient.TYPES, new ExtractOperation<String, JSONObject>() {
            @Override
            String extractElement(JSONObject element) throws JSONException {
                return element.getString(AtlasClient.NAME);
            }
        });
    }

    /**
     * Register the given type(meta model)
     * @param typeDef type definition
     * @return result json object
     * @throws AtlasServiceException
     */
    public List<String> createType(TypesDef typeDef) throws AtlasServiceException {
        return createType(TypesSerialization.toJson(typeDef));
    }

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws AtlasServiceException
     */
    public List<String> updateType(String typeAsJson) throws AtlasServiceException {
        JSONObject response = callAPI(API.UPDATE_TYPE, typeAsJson);
        return extractResults(response, AtlasClient.TYPES, new ExtractOperation<String, JSONObject>() {
            @Override
            String extractElement(JSONObject element) throws JSONException {
                return element.getString(AtlasClient.NAME);
            }
        });
    }

    /**
     * Register the given type(meta model)
     * @param typeDef type definition
     * @return result json object
     * @throws AtlasServiceException
     */
    public List<String> updateType(TypesDef typeDef) throws AtlasServiceException {
        return updateType(TypesSerialization.toJson(typeDef));
    }

    public List<String> listTypes() throws AtlasServiceException {
        final JSONObject jsonObject = callAPI(API.LIST_TYPES, null);
        return extractResults(jsonObject, AtlasClient.RESULTS, new ExtractOperation<String, String>());
    }

    public String getType(String typeName) throws AtlasServiceException {
        try {
            JSONObject response = callAPI(API.GET_TYPE, null, typeName);;
            return response.getString(DEFINITION);
        } catch (AtlasServiceException e) {
            if (Response.Status.NOT_FOUND.equals(e.getStatus())) {
                return null;
            }
            throw e;
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Create the given entity
     * @param entities entity(type instance) as json
     * @return json array of guids
     * @throws AtlasServiceException
     */
    public JSONArray createEntity(JSONArray entities) throws AtlasServiceException {
        JSONObject response = callAPI(API.CREATE_ENTITY, entities.toString());
        try {
            return response.getJSONArray(GUID);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    /**
     * Create the given entity
     * @param entitiesAsJson entity(type instance) as json
     * @return json array of guids
     * @throws AtlasServiceException
     */
    public JSONArray createEntity(String... entitiesAsJson) throws AtlasServiceException {
        return createEntity(new JSONArray(Arrays.asList(entitiesAsJson)));
    }

    public JSONArray createEntity(Referenceable... entities) throws AtlasServiceException {
        return createEntity(Arrays.asList(entities));
    }

    public JSONArray createEntity(Collection<Referenceable> entities) throws AtlasServiceException {
        JSONArray entityArray = getEntitiesArray(entities);
        return createEntity(entityArray);
    }

    private JSONArray getEntitiesArray(Collection<Referenceable> entities) {
        JSONArray entityArray = new JSONArray(entities.size());
        for (Referenceable entity : entities) {
            entityArray.put(InstanceSerialization.toJson(entity, true));
        }
        return entityArray;
    }

    /**
     * Replaces entity definitions identified by their guid or unique attribute
     * Updates properties set in the definition for the entity corresponding to guid
     * @param entities entities to be updated
     * @return json array of guids which were updated/created
     * @throws AtlasServiceException
     */
    public JSONArray updateEntities(Referenceable... entities) throws AtlasServiceException {
        return updateEntities(Arrays.asList(entities));
    }

    public JSONArray updateEntities(Collection<Referenceable> entities) throws AtlasServiceException {
        JSONArray entitiesArray = getEntitiesArray(entities);
        JSONObject response = callAPI(API.UPDATE_ENTITY, entitiesArray.toString());
        try {
            return response.getJSONArray(GUID);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.UPDATE_ENTITY, e);
        }
    }

    /**
     * Supports Partial updates
     * Updates property for the entity corresponding to guid
     * @param guid      guid
     * @param attribute  property key
     * @param value     property value
     */
    public void updateEntityAttribute(final String guid, final String attribute, String value) throws AtlasServiceException {
        callAPIWithRetries(API.UPDATE_ENTITY_PARTIAL, value, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                API api = API.UPDATE_ENTITY_PARTIAL;
                WebResource resource = getResource(api, guid);
                resource = resource.queryParam(ATTRIBUTE_NAME, attribute);
                return resource;
            }
        });
    }

    @VisibleForTesting
    JSONObject callAPIWithRetries(API api, Object requestObject, ResourceCreator resourceCreator)
            throws AtlasServiceException {
        for (int i = 0; i < getNumberOfRetries(); i++) {
            WebResource resource = resourceCreator.createResource();
            try {
                LOG.info("using resource {} for {} times", resource.getURI(), i);
                JSONObject result = callAPIWithResource(api, resource, requestObject);
                return result;
            } catch (ClientHandlerException che) {
                if (i==(getNumberOfRetries()-1)) {
                    throw che;
                }
                LOG.warn("Handled exception in calling api {}", api.getPath(), che);
                LOG.warn("Exception's cause: {}", che.getCause().getClass());
                handleClientHandlerException(che);
            }
        }
        throw new AtlasServiceException(api, new RuntimeException("Could not get response after retries."));
    }

    /**
     * Supports Partial updates
     * Updates properties set in the definition for the entity corresponding to guid
     * @param guid      guid
     * @param entity entity definition
     */
    public void updateEntity(String guid, Referenceable entity) throws AtlasServiceException {
        String entityJson = InstanceSerialization.toJson(entity, true);
        callAPI(API.UPDATE_ENTITY_PARTIAL, entityJson, guid);
    }

    /**
     * Supports Partial updates
     * Updates properties set in the definition for the entity corresponding to guid
     * @param entityType Type of the entity being updated
     * @param uniqueAttributeName Attribute Name that uniquely identifies the entity
     * @param uniqueAttributeValue Attribute Value that uniquely identifies the entity
     * @param entity entity definition
     */
    public String updateEntity(final String entityType, final String uniqueAttributeName, final String uniqueAttributeValue,
                               Referenceable entity) throws AtlasServiceException {
        final API api = API.UPDATE_ENTITY_PARTIAL;
        String entityJson = InstanceSerialization.toJson(entity, true);
        JSONObject response = callAPIWithRetries(api, entityJson, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(api, "qualifiedName");
                resource = resource.queryParam(TYPE, entityType);
                resource = resource.queryParam(ATTRIBUTE_NAME, uniqueAttributeName);
                resource = resource.queryParam(ATTRIBUTE_VALUE, uniqueAttributeValue);
                return resource;
            }
        });
        try {
            return response.getString(GUID);
        } catch (JSONException e) {
            throw new AtlasServiceException(api, e);
        }
    }

    /**
     * Delete the specified entities from the repository
     * 
     * @param guids guids of entities to delete
     * @return List of deleted entity guids
     * @throws AtlasServiceException
     */
    public List<String> deleteEntities(final String ... guids) throws AtlasServiceException {
        JSONObject jsonResponse = callAPIWithRetries(API.DELETE_ENTITIES, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                API api = API.DELETE_ENTITIES;
                WebResource resource = getResource(api);
                for (String guid : guids) {
                    resource = resource.queryParam(GUID.toLowerCase(), guid);
                }
                return resource;
            }
        });
        return extractResults(jsonResponse, GUID, new ExtractOperation<String, String>());
    }

    /**
     * Supports Deletion of an entity identified by its unique attribute value
     * @param entityType Type of the entity being deleted
     * @param uniqueAttributeName Attribute Name that uniquely identifies the entity
     * @param uniqueAttributeValue Attribute Value that uniquely identifies the entity
     * @return List of deleted entity guids(including composite references from that entity)
     */
    public List<String> deleteEntity(String entityType, String uniqueAttributeName, String uniqueAttributeValue)
            throws AtlasServiceException {
        API api = API.DELETE_ENTITY;
        WebResource resource = getResource(api);
        resource = resource.queryParam(TYPE, entityType);
        resource = resource.queryParam(ATTRIBUTE_NAME, uniqueAttributeName);
        resource = resource.queryParam(ATTRIBUTE_VALUE, uniqueAttributeValue);
        JSONObject jsonResponse = callAPIWithResource(API.DELETE_ENTITIES, resource, null);
        return extractResults(jsonResponse, GUID, new ExtractOperation<String, String>());
    }

    /**
     * Get an entity given the entity id
     * @param guid entity id
     * @return result object
     * @throws AtlasServiceException
     */
    public Referenceable getEntity(String guid) throws AtlasServiceException {
        JSONObject jsonResponse = callAPI(API.GET_ENTITY, null, guid);
        try {
            String entityInstanceDefinition = jsonResponse.getString(AtlasClient.DEFINITION);
            return InstanceSerialization.fromJsonReferenceable(entityInstanceDefinition, true);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    public static String toString(JSONArray jsonArray) throws JSONException {
        ArrayList<String> resultsList = new ArrayList<>();
        for (int index = 0; index < jsonArray.length(); index++) {
            resultsList.add(jsonArray.getString(index));
        }
        return StringUtils.join(resultsList, ",");
    }

    /**
     * Get an entity given the entity id
     * @param entityType entity type name
     * @param attribute qualified name of the entity
     * @param value
     * @return result object
     * @throws AtlasServiceException
     */
    public Referenceable getEntity(final String entityType, final String attribute, final String value)
            throws AtlasServiceException {
        JSONObject jsonResponse = callAPIWithRetries(API.GET_ENTITY, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.GET_ENTITY);
                resource = resource.queryParam(TYPE, entityType);
                resource = resource.queryParam(ATTRIBUTE_NAME, attribute);
                resource = resource.queryParam(ATTRIBUTE_VALUE, value);
                return resource;
            }
        });
        try {
            String entityInstanceDefinition = jsonResponse.getString(AtlasClient.DEFINITION);
            return InstanceSerialization.fromJsonReferenceable(entityInstanceDefinition, true);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    /**
     * List entities for a given entity type
     * @param entityType
     * @return
     * @throws AtlasServiceException
     */
    public List<String> listEntities(final String entityType) throws AtlasServiceException {
        JSONObject jsonResponse = callAPIWithRetries(API.LIST_ENTITIES, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.LIST_ENTITIES);
                resource = resource.queryParam(TYPE, entityType);
                return resource;
            }
        });
        return extractResults(jsonResponse, AtlasClient.RESULTS, new ExtractOperation<String, String>());
    }

    private class ExtractOperation<T, U> {
        T extractElement(U element) throws JSONException {
            return (T) element;
        }
    }

    private <T, U> List<T> extractResults(JSONObject jsonResponse, String key, ExtractOperation<T, U> extractInterafce)
            throws AtlasServiceException {
        try {
            JSONArray results = jsonResponse.getJSONArray(key);
            ArrayList<T> resultsList = new ArrayList<>();
            for (int index = 0; index < results.length(); index++) {
                Object element = results.get(index);
                resultsList.add(extractInterafce.extractElement((U) element));
            }
            return resultsList;
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Get the latest numResults entity audit events in decreasing order of timestamp for the given entity id
     * @param entityId entity id
     * @param numResults number of results to be returned
     * @return list of audit events for the entity id
     * @throws AtlasServiceException
     */
    public List<EntityAuditEvent> getEntityAuditEvents(String entityId, short numResults)
            throws AtlasServiceException {
        return getEntityAuditEvents(entityId, null, numResults);
    }

    /**
     * Get the entity audit events in decreasing order of timestamp for the given entity id
     * @param entityId entity id
     * @param startKey key for the first event to be returned, used for pagination
     * @param numResults number of results to be returned
     * @return list of audit events for the entity id
     * @throws AtlasServiceException
     */
    public List<EntityAuditEvent> getEntityAuditEvents(String entityId, String startKey, short numResults)
            throws AtlasServiceException {
        WebResource resource = getResource(API.LIST_ENTITY_AUDIT, entityId, URI_ENTITY_AUDIT);
        if (StringUtils.isNotEmpty(startKey)) {
            resource = resource.queryParam(START_KEY, startKey);
        }
        resource = resource.queryParam(NUM_RESULTS, String.valueOf(numResults));

        JSONObject jsonResponse = callAPIWithResource(API.LIST_ENTITY_AUDIT, resource, null);
        return extractResults(jsonResponse, AtlasClient.EVENTS, new ExtractOperation<EntityAuditEvent, JSONObject>() {
            @Override
            EntityAuditEvent extractElement(JSONObject element) throws JSONException {
                return EntityAuditEvent.GSON.fromJson(element.toString(), EntityAuditEvent.class);
            }
        });

    }

    /**
     * Search using gremlin/dsl/full text
     * @param searchQuery
     * @return
     * @throws AtlasServiceException
     */
    public JSONArray search(final String searchQuery) throws AtlasServiceException {
        JSONObject result = callAPIWithRetries(API.SEARCH, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH);
                resource = resource.queryParam(QUERY, searchQuery);
                return resource;
            }
        });
        try {
            return result.getJSONArray(RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }

    }

    /**
     * Search given query DSL
     * @param query DSL query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONArray searchByDSL(final String query) throws AtlasServiceException {
        LOG.debug("DSL query: {}", query);
        JSONObject result = callAPIWithRetries(API.SEARCH_DSL, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH_DSL);
                resource = resource.queryParam(QUERY, query);
                return resource;
            }
        });
        try {
            return result.getJSONArray(RESULTS);
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
    public JSONArray searchByGremlin(final String gremlinQuery) throws AtlasServiceException {
        LOG.debug("Gremlin query: " + gremlinQuery);
        JSONObject result = callAPIWithRetries(API.SEARCH_GREMLIN, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH_GREMLIN);
                resource = resource.queryParam(QUERY, gremlinQuery);
                return resource;
            }
        });
        try {
            return result.getJSONArray(RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Search given full text search
     * @param query Query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject searchByFullText(final String query) throws AtlasServiceException {
        return callAPIWithRetries(API.SEARCH_FULL_TEXT, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH_FULL_TEXT);
                resource = resource.queryParam(QUERY, query);
                return resource;
            }
        });
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

    private WebResource getResource(API api, String... pathParams) {
        return getResource(service, api, pathParams);
    }

    private WebResource getResource(WebResource service, API api, String... pathParams) {
        WebResource resource = service.path(api.getPath());
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }
        return resource;
    }

    private JSONObject callAPIWithResource(API api, WebResource resource, Object requestObject)
        throws AtlasServiceException {
        ClientResponse clientResponse = null;
        for (int i = 0; i < getNumberOfRetries(); i++) {
            clientResponse = resource.accept(JSON_MEDIA_TYPE).type(JSON_MEDIA_TYPE)
                .method(api.getMethod(), ClientResponse.class, requestObject);

            if (clientResponse.getStatus() == api.getExpectedStatus().getStatusCode()) {
                String responseAsString = clientResponse.getEntity(String.class);
                try {
                    return new JSONObject(responseAsString);
                } catch (JSONException e) {
                    throw new AtlasServiceException(api, e);
                }
            } else if (clientResponse.getStatus() != ClientResponse.Status.SERVICE_UNAVAILABLE.getStatusCode()) {
                break;
            } else {
                LOG.error("Got a service unavailable when calling: {}, will retry..", resource);
                sleepBetweenRetries();
            }
        }

        throw new AtlasServiceException(api, clientResponse);
    }

    private JSONObject callAPI(final API api, Object requestObject, final String... pathParams)
            throws AtlasServiceException {
        return callAPIWithRetries(api, requestObject, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                return getResource(api, pathParams);
            }
        });
    }

    /**
     * A class to capture input state while creating the client.
     *
     * The information here will be reused when the client is re-initialized on switch-over
     * in case of High Availability.
     */
    private class AtlasClientContext {
        private String[] baseUrls;
        private Client client;
        private final UserGroupInformation ugi;
        private final String doAsUser;

        public AtlasClientContext(String[] baseUrls, Client client, UserGroupInformation ugi, String doAsUser) {
            this.baseUrls = baseUrls;
            this.client = client;
            this.ugi = ugi;
            this.doAsUser = doAsUser;
        }

        public UserGroupInformation getUgi() {
            return ugi;
        }

        public String getDoAsUser() {
            return doAsUser;
        }

        public Client getClient() {
            return client;
        }

        public String[] getBaseUrls() {
            return baseUrls;
        }
    }

}
