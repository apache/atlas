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
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Client for metadata.
 */
public class AtlasClient extends AtlasBaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClient.class);

    public static final String TYPE = "type";
    public static final String TYPENAME = "typeName";
    public static final String GUID = "GUID";
    public static final String ENTITIES = "entities";
    public static final String GUID_ASSIGNMENTS = "guidAssignments";

    public static final String DEFINITION = "definition";
    public static final String ERROR = "error";
    public static final String STACKTRACE = "stackTrace";
    public static final String REQUEST_ID = "requestId";
    public static final String RESULTS = "results";
    public static final String COUNT = "count";
    public static final String ROWS = "rows";
    public static final String DATATYPE = "dataType";
    public static final String STATUS = "Status";

    public static final String EVENTS = "events";
    public static final String START_KEY = "startKey";
    public static final String NUM_RESULTS = "count";

    public static final String URI_ENTITY = "entities";
    public static final String URI_ENTITY_AUDIT = "audit";
    public static final String URI_SEARCH = "discovery/search";
    public static final String URI_NAME_LINEAGE = "lineage/hive/table";
    public static final String URI_LINEAGE = "lineage/";
    public static final String URI_TRAITS = "traits";
    public static final String TRAITS = "traits";
    public static final String TRAIT_DEFINITIONS = "traitDefinitions";


    public static final String QUERY = "query";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    public static final String QUERY_TYPE = "queryType";
    public static final String ATTRIBUTE_NAME = "property";
    public static final String ATTRIBUTE_VALUE = "value";

    public static final String SUPERTYPE = "supertype";
    public static final String NOT_SUPERTYPE = "notsupertype";

    public static final String ASSET_TYPE = "Asset";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String OWNER = "owner";

    public static final String INFRASTRUCTURE_SUPER_TYPE = "Infrastructure";
    public static final String DATA_SET_SUPER_TYPE = "DataSet";
    public static final String PROCESS_SUPER_TYPE = "Process";
    public static final String PROCESS_ATTRIBUTE_INPUTS = "inputs";
    public static final String PROCESS_ATTRIBUTE_OUTPUTS = "outputs";

    public static final String REFERENCEABLE_SUPER_TYPE = "Referenceable";
    public static final String QUALIFIED_NAME = "qualifiedName";
    public static final String REFERENCEABLE_ATTRIBUTE_NAME = QUALIFIED_NAME;

    public static final String UNKNOWN_STATUS = "Unknown status";

    // New constuctor for Basic auth
    public AtlasClient(String[] baseUrl, String[] basicAuthUserNamepassword) {
        super(baseUrl, basicAuthUserNamepassword);
    }

    /**
     * Create a new Atlas client.
     * @param baseUrls A list of URLs that point to an ensemble of Atlas servers working in
     *                 High Availability mode. The client will automatically determine the
     *                 active instance on startup and also when there is a scenario of
     *                 failover.
     */
    public AtlasClient(String... baseUrls) throws AtlasException {
        this(getCurrentUGI(), baseUrls);
    }

    /**
     * Create a new Atlas client.
     * @param ugi UserGroupInformation
     * @param doAsUser
     * @param baseUrls A list of URLs that point to an ensemble of Atlas servers working in
     *                 High Availability mode. The client will automatically determine the
     *                 active instance on startup and also when there is a scenario of
     *                 failover.
     */
    public AtlasClient(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        initializeState(baseUrls, ugi, doAsUser);
    }

    private AtlasClient(UserGroupInformation ugi, String[] baseUrls) {
        this(ugi, ugi.getShortUserName(), baseUrls);
    }

    //Used by LocalAtlasClient
    protected AtlasClient() {
        //Do nothing
    }

    @VisibleForTesting
    public AtlasClient(Configuration configuration, String[] baseUrl, String[] basicAuthUserNamepassword) {
        super(configuration, baseUrl, basicAuthUserNamepassword);
    }

    @VisibleForTesting
    public AtlasClient(Configuration configuration, String... baseUrls) throws AtlasException {
        initializeState(configuration, baseUrls, getCurrentUGI(), getCurrentUGI().getShortUserName());
    }

    @VisibleForTesting
    AtlasClient(WebResource service, Configuration configuration) {
        super(service, configuration);
    }

    public WebResource getResource() {
        return service;
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
        GET_ALL_TRAIT_DEFINITIONS(BASE_URI + URI_ENTITY, HttpMethod.GET, Response.Status.OK),
        GET_TRAIT_DEFINITION(BASE_URI + URI_ENTITY, HttpMethod.GET, Response.Status.OK),

        //Search operations
        SEARCH(BASE_URI + URI_SEARCH, HttpMethod.GET, Response.Status.OK),
        SEARCH_DSL(BASE_URI + URI_SEARCH + "/dsl", HttpMethod.GET, Response.Status.OK),
        SEARCH_FULL_TEXT(BASE_URI + URI_SEARCH + "/fulltext", HttpMethod.GET, Response.Status.OK),

        GREMLIN_SEARCH(BASE_URI + URI_SEARCH + "/gremlin", HttpMethod.GET, Response.Status.OK),

        //Lineage operations based on dataset name
        NAME_LINEAGE_INPUTS_GRAPH(BASE_URI + URI_NAME_LINEAGE, HttpMethod.GET, Response.Status.OK),
        NAME_LINEAGE_OUTPUTS_GRAPH(BASE_URI + URI_NAME_LINEAGE, HttpMethod.GET, Response.Status.OK),
        NAME_LINEAGE_SCHEMA(BASE_URI + URI_NAME_LINEAGE, HttpMethod.GET, Response.Status.OK),

        //Lineage operations based on entity id of the dataset
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

        public Response.Status getExpectedStatus() {
            return status;
        }
    }

    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class EntityResult {
        private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

        public static final String OP_CREATED = "created";
        public static final String OP_UPDATED = "updated";
        public static final String OP_DELETED = "deleted";

        Map<String, List<String>> entities = new HashMap<>();

        public EntityResult() {
            //For gson
        }

        public EntityResult(List<String> created, List<String> updated, List<String> deleted) {
            set(OP_CREATED, created);
            set(OP_UPDATED, updated);
            set(OP_DELETED, deleted);
        }

        public void set(String type, List<String> list) {
            if (list != null && list.size() > 0) {
                entities.put(type, list);
            }
        }

        private List<String> get(String type) {
            List<String> list = entities.get(type);
            if (list == null) {
                list = new ArrayList<>();
            }
            return list;
        }

        public Map<String, List<String>> getEntities(){
            return entities;
        }

        public void setEntities(Map<String, List<String>> entities){
            this.entities = entities;
        }

        @JsonIgnore
        public List<String> getCreatedEntities() {
            return get(OP_CREATED);
        }

        @JsonIgnore
        public List<String> getUpdateEntities() {
            return get(OP_UPDATED);
        }


        @JsonIgnore
        public List<String> getDeletedEntities() {
            return get(OP_DELETED);
        }

        @Override
        public String toString() { return gson.toJson(this); }

        public static EntityResult fromString(String json) throws AtlasServiceException {
            return gson.fromJson(json, EntityResult.class);
        }
    }

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws AtlasServiceException
     */
    public List<String> createType(String typeAsJson) throws AtlasServiceException {
        LOG.debug("Creating type definition: {}", typeAsJson);
        JSONObject response = callAPIWithBody(API.CREATE_TYPE, typeAsJson);
        List<String> results = extractResults(response, AtlasClient.TYPES, new ExtractOperation<String, JSONObject>() {
            @Override
            String extractElement(JSONObject element) throws JSONException {
                return element.getString(AtlasClient.NAME);
            }
        });
        LOG.debug("Create type definition returned results: {}", results);
        return results;
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
     * Creates trait type with specifiedName, superTraits and attributes
     * @param traitName the name of the trait type
     * @param superTraits the list of super traits from which this trait type inherits attributes
     * @param attributeDefinitions the list of attributes of the trait type
     * @return the list of types created
     * @throws AtlasServiceException
     */
    public List<String> createTraitType(String traitName, ImmutableSet<String> superTraits, AttributeDefinition... attributeDefinitions) throws AtlasServiceException {
        HierarchicalTypeDefinition<TraitType> piiTrait =
            TypesUtil.createTraitTypeDef(traitName, superTraits, attributeDefinitions);

        String traitDefinitionAsJSON = TypesSerialization.toJson(piiTrait, true);
        LOG.debug("Creating trait type {} {}" , traitName, traitDefinitionAsJSON);
        return createType(traitDefinitionAsJSON);
    }

    /**
     * Creates simple trait type with specifiedName with no superTraits or attributes
     * @param traitName the name of the trait type
     * @return the list of types created
     * @throws AtlasServiceException
     */
    public List<String> createTraitType(String traitName) throws AtlasServiceException {
        return createTraitType(traitName, null);
    }

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws AtlasServiceException
     */
    public List<String> updateType(String typeAsJson) throws AtlasServiceException {
        LOG.debug("Updating type definition: {}", typeAsJson);
        JSONObject response = callAPIWithBody(API.UPDATE_TYPE, typeAsJson);
        List<String> results = extractResults(response, AtlasClient.TYPES, new ExtractOperation<String, JSONObject>() {
            @Override
            String extractElement(JSONObject element) throws JSONException {
                return element.getString(AtlasClient.NAME);
            }
        });
        LOG.debug("Update type definition returned results: {}", results);
        return results;
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

    /**
     * Returns all type names in the system
     * @return list of type names
     * @throws AtlasServiceException
     */
    public List<String> listTypes() throws AtlasServiceException {
        final JSONObject jsonObject = callAPIWithQueryParams(API.LIST_TYPES, null);
        return extractResults(jsonObject, AtlasClient.RESULTS, new ExtractOperation<String, String>());
    }

    /**
     * Returns all type names with the given category
     * @param category
     * @return list of type names
     * @throws AtlasServiceException
     */
    public List<String> listTypes(final DataTypes.TypeCategory category) throws AtlasServiceException {
        JSONObject response = callAPIWithRetries(API.LIST_TYPES, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.LIST_TYPES.getPath());
                resource = resource.queryParam(TYPE, category.name());
                return resource;
            }
        });
        return extractResults(response, AtlasClient.RESULTS, new ExtractOperation<String, String>());
    }

    /**
     * Return the list of type names in the type system which match the specified filter.
     *
     * @param category returns types whose category is the given typeCategory
     * @param superType returns types which contain the given supertype
     * @param notSupertype returns types which do not contain the given supertype
     *
     * Its possible to specify combination of these filters in one request and the conditions are combined with AND
     * For example, typeCategory = TRAIT && supertype contains 'X' && supertype !contains 'Y'
     * If there is no filter, all the types are returned
     * @return list of type names
     */
    public List<String> listTypes(final DataTypes.TypeCategory category, final String superType,
                                  final String notSupertype) throws AtlasServiceException {
        JSONObject response = callAPIWithRetries(API.LIST_TYPES, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.LIST_TYPES);
                resource = resource.queryParam(TYPE, category.name());
                resource = resource.queryParam(SUPERTYPE, superType);
                resource = resource.queryParam(NOT_SUPERTYPE, notSupertype);
                return resource;
            }
        });
        return extractResults(response, AtlasClient.RESULTS, new ExtractOperation<String, String>());
    }

    public TypesDef getType(String typeName) throws AtlasServiceException {
        try {
            JSONObject response = callAPIWithBodyAndParams(API.GET_TYPE, null, typeName);
            String typeJson = response.getString(DEFINITION);
            return TypesSerialization.fromJson(typeJson);
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
    protected List<String> createEntity(JSONArray entities) throws AtlasServiceException {
        LOG.debug("Creating entities: {}", entities);
        JSONObject response = callAPIWithBody(API.CREATE_ENTITY, entities.toString());
        List<String> results = extractEntityResult(response).getCreatedEntities();
        LOG.debug("Create entities returned results: {}", results);
        return results;
    }

    protected EntityResult extractEntityResult(JSONObject response) throws AtlasServiceException {
        return EntityResult.fromString(response.toString());
    }

    /**
     * Create the given entity
     * @param entitiesAsJson entity(type instance) as json
     * @return json array of guids
     * @throws AtlasServiceException
     */
    public List<String> createEntity(String... entitiesAsJson) throws AtlasServiceException {
        return createEntity(new JSONArray(Arrays.asList(entitiesAsJson)));
    }

    public List<String> createEntity(Referenceable... entities) throws AtlasServiceException {
        return createEntity(Arrays.asList(entities));
    }

    public List<String> createEntity(Collection<Referenceable> entities) throws AtlasServiceException {
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
    public EntityResult updateEntities(Referenceable... entities) throws AtlasServiceException {
        return updateEntities(Arrays.asList(entities));
    }

    protected EntityResult updateEntities(JSONArray entities) throws AtlasServiceException {
        LOG.debug("Updating entities: {}", entities);
        JSONObject response = callAPIWithBody(API.UPDATE_ENTITY, entities.toString());
        EntityResult results = extractEntityResult(response);
        LOG.debug("Update entities returned results: {}", results);
        return results;
    }

    public EntityResult updateEntities(Collection<Referenceable> entities) throws AtlasServiceException {
        JSONArray entitiesArray = getEntitiesArray(entities);
        return updateEntities(entitiesArray);
    }

    /**
     * Supports Partial updates
     * Updates property for the entity corresponding to guid
     * @param guid      guid
     * @param attribute  property key
     * @param value     property value
     */
    public EntityResult updateEntityAttribute(final String guid, final String attribute, String value)
            throws AtlasServiceException {
        LOG.debug("Updating entity id: {}, attribute name: {}, attribute value: {}", guid, attribute, value);
        JSONObject response = callAPIWithRetries(API.UPDATE_ENTITY_PARTIAL, value, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                API api = API.UPDATE_ENTITY_PARTIAL;
                WebResource resource = getResource(api, guid);
                resource = resource.queryParam(ATTRIBUTE_NAME, attribute);
                return resource;
            }
        });
        return extractEntityResult(response);
    }

    /**
     * Supports Partial updates
     * Updates properties set in the definition for the entity corresponding to guid
     * @param guid      guid
     * @param entity entity definition
     */
    public EntityResult updateEntity(String guid, Referenceable entity) throws AtlasServiceException {
        String entityJson = InstanceSerialization.toJson(entity, true);
        LOG.debug("Updating entity id {} with {}", guid, entityJson);
        JSONObject response = callAPIWithBodyAndParams(API.UPDATE_ENTITY_PARTIAL, entityJson, guid);
        return extractEntityResult(response);
    }

    /**
     * Associate trait to an entity
     *
     * @param guid      guid
     * @param traitDefinition trait definition
     */
    public void addTrait(String guid, Struct traitDefinition) throws AtlasServiceException {
        String traitJson = InstanceSerialization.toJson(traitDefinition, true);
        LOG.debug("Adding trait to entity with id {} {}", guid, traitJson);
        callAPIWithBodyAndParams(API.ADD_TRAITS, traitJson, guid, URI_TRAITS);
    }

    /**
     * Delete a trait from the given entity
     * @param guid guid of the entity
     * @param traitName trait to be deleted
     * @throws AtlasServiceException
     */
    public void deleteTrait(String guid, String traitName) throws AtlasServiceException {
        callAPIWithBodyAndParams(API.DELETE_TRAITS, null, guid, TRAITS, traitName);
    }

    /**
     * Supports Partial updates
     * Updates properties set in the definition for the entity corresponding to guid
     * @param entityType Type of the entity being updated
     * @param uniqueAttributeName Attribute Name that uniquely identifies the entity
     * @param uniqueAttributeValue Attribute Value that uniquely identifies the entity
     * @param entity entity definition
     */
    public EntityResult updateEntity(final String entityType, final String uniqueAttributeName,
                                     final String uniqueAttributeValue,
                                     Referenceable entity) throws AtlasServiceException {
        final API api = API.UPDATE_ENTITY_PARTIAL;
        String entityJson = InstanceSerialization.toJson(entity, true);
        LOG.debug("Updating entity type: {}, attributeName: {}, attributeValue: {}, entity: {}", entityType,
                uniqueAttributeName, uniqueAttributeValue, entityJson);
        JSONObject response = callAPIWithRetries(api, entityJson, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(api, QUALIFIED_NAME);
                resource = resource.queryParam(TYPE, entityType);
                resource = resource.queryParam(ATTRIBUTE_NAME, uniqueAttributeName);
                resource = resource.queryParam(ATTRIBUTE_VALUE, uniqueAttributeValue);
                return resource;
            }
        });
        EntityResult result = extractEntityResult(response);
        LOG.debug("Update entity returned result: {}", result);
        return result;
    }

    protected String getString(JSONObject jsonObject, String parameter) throws AtlasServiceException {
        try {
            return jsonObject.getString(parameter);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Delete the specified entities from the repository
     *
     * @param guids guids of entities to delete
     * @return List of entity ids updated/deleted
     * @throws AtlasServiceException
     */
    public EntityResult deleteEntities(final String ... guids) throws AtlasServiceException {
        LOG.debug("Deleting entities: {}", guids);
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
        EntityResult results = extractEntityResult(jsonResponse);
        LOG.debug("Delete entities returned results: {}", results);
        return results;
    }

    /**
     * Supports Deletion of an entity identified by its unique attribute value
     * @param entityType Type of the entity being deleted
     * @param uniqueAttributeName Attribute Name that uniquely identifies the entity
     * @param uniqueAttributeValue Attribute Value that uniquely identifies the entity
     * @return List of entity ids updated/deleted(including composite references from that entity)
     */
    public EntityResult deleteEntity(String entityType, String uniqueAttributeName, String uniqueAttributeValue)
            throws AtlasServiceException {
        LOG.debug("Deleting entity type: {}, attributeName: {}, attributeValue: {}", entityType, uniqueAttributeName,
                uniqueAttributeValue);
        API api = API.DELETE_ENTITY;
        WebResource resource = getResource(api);
        resource = resource.queryParam(TYPE, entityType);
        resource = resource.queryParam(ATTRIBUTE_NAME, uniqueAttributeName);
        resource = resource.queryParam(ATTRIBUTE_VALUE, uniqueAttributeValue);
        JSONObject jsonResponse = callAPIWithResource(API.DELETE_ENTITIES, resource);
        EntityResult results = extractEntityResult(jsonResponse);
        LOG.debug("Delete entities returned results: {}", results);
        return results;
    }

    /**
     * Get an entity given the entity id
     * @param guid entity id
     * @return result object
     * @throws AtlasServiceException
     */
    public Referenceable getEntity(String guid) throws AtlasServiceException {
        JSONObject jsonResponse = callAPIWithBodyAndParams(API.GET_ENTITY, null, guid);
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

    /**
     * List traits for a given entity identified by its GUID
     * @param guid GUID of the entity
     * @return List<String> - traitnames associated with entity
     * @throws AtlasServiceException
     */
    public List<String> listTraits(final String guid) throws AtlasServiceException {
        JSONObject jsonResponse = callAPIWithBodyAndParams(API.LIST_TRAITS, null, guid, URI_TRAITS);
        return extractResults(jsonResponse, AtlasClient.RESULTS, new ExtractOperation<String, String>());
    }

    /**
     * Get all trait definitions for an entity
     * @param guid GUID of the entity
     * @return List<String> trait definitions of the traits associated to the entity
     * @throws AtlasServiceException
     */
    public List<Struct> listTraitDefinitions(final String guid) throws AtlasServiceException{
        JSONObject jsonResponse = callAPIWithBodyAndParams(API.GET_ALL_TRAIT_DEFINITIONS, null, guid, TRAIT_DEFINITIONS);
        List<JSONObject> traitDefList = extractResults(jsonResponse, AtlasClient.RESULTS, new ExtractOperation<JSONObject, JSONObject>());
        ArrayList<Struct> traitStructList = new ArrayList<>();
        for(JSONObject traitDef:traitDefList){
            Struct traitStruct = InstanceSerialization.fromJsonStruct(traitDef.toString(), true);
            traitStructList.add(traitStruct);
        }
        return traitStructList;
    }

    /**
     * Get trait definition for a given entity and traitname
     * @param guid GUID of the entity
     * @param traitName
     * @return trait definition
     * @throws AtlasServiceException
     */
    public Struct getTraitDefinition(final String guid, final String traitName) throws AtlasServiceException{
        JSONObject jsonResponse = callAPIWithBodyAndParams(API.GET_TRAIT_DEFINITION, null, guid, TRAIT_DEFINITIONS, traitName);

        try {
            return InstanceSerialization.fromJsonStruct(jsonResponse.getString(AtlasClient.RESULTS), false);
        }catch (JSONException e){
            throw new AtlasServiceException(API.GET_TRAIT_DEFINITION, e);
        }
    }

    protected class ExtractOperation<T, U> {
        T extractElement(U element) throws JSONException {
            return (T) element;
        }
    }

    protected <T, U> List<T> extractResults(JSONObject jsonResponse, String key, ExtractOperation<T, U> extractInterafce)
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

        JSONObject jsonResponse = callAPIWithResource(API.LIST_ENTITY_AUDIT, resource);
        return extractResults(jsonResponse, AtlasClient.EVENTS, new ExtractOperation<EntityAuditEvent, JSONObject>() {
            @Override
            EntityAuditEvent extractElement(JSONObject element) throws JSONException {
                return SerDe.GSON.fromJson(element.toString(), EntityAuditEvent.class);
            }
        });

    }

    /**
     * Search using dsl/full text
     * @param searchQuery
     * @param limit number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return Query results
     * @throws AtlasServiceException
     */
    public JSONArray search(final String searchQuery, final int limit, final int offset) throws AtlasServiceException {
        JSONObject result = callAPIWithRetries(API.SEARCH, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH);
                resource = resource.queryParam(QUERY, searchQuery);
                resource = resource.queryParam(LIMIT, String.valueOf(limit));
                resource = resource.queryParam(OFFSET, String.valueOf(offset));
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
     * @param limit number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONArray searchByDSL(final String query, final int limit, final int offset) throws AtlasServiceException {
        LOG.debug("DSL query: {}", query);
        JSONObject result = callAPIWithRetries(API.SEARCH_DSL, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH_DSL);
                resource = resource.queryParam(QUERY, query);
                resource = resource.queryParam(LIMIT, String.valueOf(limit));
                resource = resource.queryParam(OFFSET, String.valueOf(offset));
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
     * @param limit number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject searchByFullText(final String query, final int limit, final int offset) throws AtlasServiceException {
        return callAPIWithRetries(API.SEARCH_FULL_TEXT, null, new ResourceCreator() {
            @Override
            public WebResource createResource() {
                WebResource resource = getResource(API.SEARCH_FULL_TEXT);
                resource = resource.queryParam(QUERY, query);
                resource = resource.queryParam(LIMIT, String.valueOf(limit));
                resource = resource.queryParam(OFFSET, String.valueOf(offset));
                return resource;
            }
        });
    }

    public JSONObject getInputGraph(String datasetName) throws AtlasServiceException {
        JSONObject response = callAPIWithBodyAndParams(API.NAME_LINEAGE_INPUTS_GRAPH, null, datasetName, "/inputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public JSONObject getOutputGraph(String datasetName) throws AtlasServiceException {
        JSONObject response = callAPIWithBodyAndParams(API.NAME_LINEAGE_OUTPUTS_GRAPH, null, datasetName, "/outputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public JSONObject getInputGraphForEntity(String entityId) throws AtlasServiceException {
        JSONObject response = callAPIWithBodyAndParams(API.LINEAGE_INPUTS_GRAPH, null, entityId, "/inputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public JSONObject getOutputGraphForEntity(String datasetId) throws AtlasServiceException {
        JSONObject response = callAPIWithBodyAndParams(API.LINEAGE_OUTPUTS_GRAPH, null, datasetId, "/outputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public JSONObject getSchemaForEntity(String datasetId) throws AtlasServiceException {
        JSONObject response = callAPIWithBodyAndParams(API.LINEAGE_OUTPUTS_GRAPH, null, datasetId, "/schema");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    // Wrapper methods for compatibility
    @VisibleForTesting
    public JSONObject callAPIWithResource(API api, WebResource resource) throws AtlasServiceException {
        return callAPIWithResource(toAPIInfo(api), resource, null, JSONObject.class);
    }

    @VisibleForTesting
    public WebResource getResource(API api, String ... params) {
        return getResource(toAPIInfo(api), params);
    }

    @VisibleForTesting
    public JSONObject callAPIWithBody(API api, Object requestObject) throws AtlasServiceException {
        return callAPI(toAPIInfo(api), JSONObject.class, requestObject, (String[]) null);
    }

    @VisibleForTesting
    public JSONObject callAPIWithBodyAndParams(API api, Object requestObject, String ... params) throws AtlasServiceException {
        return callAPI(toAPIInfo(api), JSONObject.class, requestObject, params);
    }

    @VisibleForTesting
    public JSONObject callAPIWithQueryParams(API api, MultivaluedMap<String, String> queryParams) throws AtlasServiceException {
        return callAPI(toAPIInfo(api), JSONObject.class, queryParams);
    }

    @VisibleForTesting
    JSONObject callAPIWithRetries(API api, Object requestObject, ResourceCreator resourceCreator) throws AtlasServiceException {
        return super.callAPIWithRetries(toAPIInfo(api), requestObject, resourceCreator);
    }

    private APIInfo toAPIInfo(API api){
        return new APIInfo(api.getPath(), api.getMethod(), api.getExpectedStatus());
    }


}
