/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.AtlasClassifications;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasClient.LIMIT;
import static org.apache.atlas.AtlasClient.OFFSET;
import static org.apache.atlas.AtlasClient.QUERY;

public class AtlasClientV2 extends AtlasBaseClient {

    // Type APIs
    public static final String TYPES_API = BASE_URI + "v2/types/";
    private static final String TYPEDEFS_API = TYPES_API + "typedefs/";
    private static final String TYPEDEF_BY_NAME = TYPES_API + "typedef/name/";
    private static final String TYPEDEF_BY_GUID = TYPES_API + "typedef/guid/";

    private static final String GET_BY_NAME_TEMPLATE = TYPES_API + "%s/name/%s";
    private static final String GET_BY_GUID_TEMPLATE = TYPES_API + "%s/guid/%s";

    private static final APIInfo GET_TYPEDEF_BY_NAME = new APIInfo(TYPEDEF_BY_NAME, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo GET_TYPEDEF_BY_GUID = new APIInfo(TYPEDEF_BY_GUID, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo GET_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_API, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo CREATE_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_API, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo UPDATE_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_API, HttpMethod.PUT, Response.Status.OK);
    private static final APIInfo DELETE_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_API, HttpMethod.DELETE, Response.Status.OK);

    // Entity APIs
    public static final String ENTITY_API = BASE_URI + "v2/entity/";
    private static final String ENTITY_BULK_API = ENTITY_API + "bulk/";

    private static final APIInfo GET_ENTITY_BY_GUID         = new APIInfo(ENTITY_API + "guid/", HttpMethod.GET, Response.Status.OK);
    private static final APIInfo GET_ENTITY_BY_ATTRIBUTE    = new APIInfo(ENTITY_API + "uniqueAttribute/type/", HttpMethod.GET, Response.Status.OK);
    public  static final APIInfo CREATE_ENTITY              = new APIInfo(ENTITY_API, HttpMethod.POST, Response.Status.OK);
    public  static final APIInfo UPDATE_ENTITY              = CREATE_ENTITY;
    public  static final APIInfo UPDATE_ENTITY_BY_ATTRIBUTE = new APIInfo(ENTITY_API + "uniqueAttribute/type/", HttpMethod.PUT, Response.Status.OK);
    private static final APIInfo DELETE_ENTITY_BY_GUID      = new APIInfo(ENTITY_API + "guid/", HttpMethod.DELETE, Response.Status.OK);
    public  static final APIInfo DELETE_ENTITY_BY_ATTRIBUTE = new APIInfo(ENTITY_API + "uniqueAttribute/type/", HttpMethod.DELETE, Response.Status.OK);

    private static final APIInfo GET_ENTITIES_BY_GUIDS    = new APIInfo(ENTITY_BULK_API, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo CREATE_ENTITIES          = new APIInfo(ENTITY_BULK_API, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo UPDATE_ENTITIES          = CREATE_ENTITIES;
    private static final APIInfo DELETE_ENTITIES_BY_GUIDS = new APIInfo(ENTITY_BULK_API, HttpMethod.DELETE, Response.Status.OK);

    private static final APIInfo GET_CLASSIFICATIONS    = new APIInfo(ENTITY_API + "guid/%s/classifications", HttpMethod.GET, Response.Status.OK);
    private static final APIInfo ADD_CLASSIFICATIONS    = new APIInfo(ENTITY_API + "guid/%s/classifications", HttpMethod.POST, Response.Status.NO_CONTENT);
    private static final APIInfo UPDATE_CLASSIFICATIONS = new APIInfo(ENTITY_API + "guid/%s/classifications", HttpMethod.PUT, Response.Status.OK);
    private static final APIInfo DELETE_CLASSIFICATION  = new APIInfo(ENTITY_API + "guid/%s/classification/%s", HttpMethod.DELETE, Response.Status.NO_CONTENT);

    public static final String PREFIX_ATTR = "attr:";

    // Lineage APIs
    private static final String  LINEAGE_URI  = BASE_URI + "v2/lineage/";
    private static final APIInfo LINEAGE_INFO = new APIInfo(LINEAGE_URI, HttpMethod.GET, Response.Status.OK);


    // Discovery APIs
    private static final String DISCOVERY_URI = BASE_URI + "v2/search";
    private static final String DSL_URI       = DISCOVERY_URI + "/dsl";
    private static final String FULL_TEXT_URI = DISCOVERY_URI + "/fulltext";
    private static final String BASIC_SEARCH_URI = DISCOVERY_URI + "/basic";
    private static final String FACETED_SEARCH_URI = BASIC_SEARCH_URI;

    private static final APIInfo DSL_SEARCH       = new APIInfo(DSL_URI, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo FULL_TEXT_SEARCH = new APIInfo(FULL_TEXT_URI, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo BASIC_SEARCH = new APIInfo(BASIC_SEARCH_URI, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo FACETED_SEARCH = new APIInfo(FACETED_SEARCH_URI, HttpMethod.POST, Response.Status.OK);


    public AtlasClientV2(String[] baseUrl, String[] basicAuthUserNamePassword) {
        super(baseUrl, basicAuthUserNamePassword);
    }

    public AtlasClientV2(String... baseUrls) throws AtlasException {
        super(baseUrls);
    }

    public AtlasClientV2(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        super(ugi, doAsUser, baseUrls);
    }

    /**
     * Constructor for AtlasClient with cookie params as header
     * @param baseUrl
     * @param cookieName
     * @param value
     * @param path
     * @param domain
     */
    public AtlasClientV2(String[] baseUrl, String cookieName, String value, String path, String domain) {
        super(baseUrl, new Cookie(cookieName, value, path, domain));
    }

    /**
     * Constructor for AtlasClient with cookie as header
     * @param baseUrl
     * @param cookie
     */
    public AtlasClientV2(String[] baseUrl, Cookie cookie) {
        super(baseUrl, cookie);
    }

    @VisibleForTesting
    AtlasClientV2(WebResource service, Configuration configuration) {
        super(service, configuration);
    }

    /**
     * Bulk retrieval API for retrieving all type definitions in Atlas
     *
     * @return A composite wrapper object with lists of all type definitions
     */
    public AtlasTypesDef getAllTypeDefs(SearchFilter searchFilter) throws AtlasServiceException {
        return callAPI(GET_ALL_TYPE_DEFS, AtlasTypesDef.class, searchFilter.getParams());
    }

    public boolean typeWithGuidExists(String guid) {
        try {
            callAPI(GET_TYPEDEF_BY_GUID, String.class, null, guid);
        } catch (AtlasServiceException e) {
            return false;
        }
        return true;
    }

    public boolean typeWithNameExists(String name) {
        try {
            callAPI(GET_TYPEDEF_BY_NAME, String.class, null, name);
        } catch (AtlasServiceException e) {
            return false;
        }
        return true;
    }

    public AtlasEnumDef getEnumDefByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasEnumDef.class);
    }

    public AtlasEnumDef getEnumDefByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasEnumDef.class);
    }

    public AtlasStructDef getStructDefByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasStructDef.class);
    }

    public AtlasStructDef getStructDefByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasStructDef.class);
    }

    public AtlasClassificationDef getClassificationDefByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasClassificationDef.class);
    }

    public AtlasClassificationDef getClassificationDefByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasClassificationDef.class);
    }

    public AtlasEntityDef getEntityDefByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasEntityDef.class);
    }

    public AtlasEntityDef getEntityDefByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasEntityDef.class);
    }

    @Deprecated
    public AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasServiceException {
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEnumDefs().add(enumDef);
        AtlasTypesDef created = createAtlasTypeDefs(atlasTypesDef);
        assert created != null;
        assert created.getEnumDefs() != null;
        return created.getEnumDefs().get(0);
    }

    @Deprecated
    public AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasServiceException {
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getStructDefs().add(structDef);
        AtlasTypesDef created = createAtlasTypeDefs(atlasTypesDef);
        assert created != null;
        assert created.getStructDefs() != null;
        return created.getStructDefs().get(0);
    }

    @Deprecated
    public AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws AtlasServiceException {
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEntityDefs().add(entityDef);
        AtlasTypesDef created = createAtlasTypeDefs(atlasTypesDef);
        assert created != null;
        assert created.getEntityDefs() != null;
        return created.getEntityDefs().get(0);
    }

    @Deprecated
    public AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef)
            throws AtlasServiceException {
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getClassificationDefs().add(classificationDef);
        AtlasTypesDef created = createAtlasTypeDefs(atlasTypesDef);
        assert created != null;
        assert created.getClassificationDefs() != null;
        return created.getClassificationDefs().get(0);
    }


    /**
     * Bulk create APIs for all atlas type definitions, only new definitions will be created.
     * Any changes to the existing definitions will be discarded
     *
     * @param typesDef A composite wrapper object with corresponding lists of the type definition
     * @return A composite wrapper object with lists of type definitions that were successfully
     * created
     */
    public AtlasTypesDef createAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasServiceException {
        return callAPI(CREATE_ALL_TYPE_DEFS, AtlasTypesDef.class, AtlasType.toJson(typesDef));
    }

    /**
     * Bulk update API for all types, changes detected in the type definitions would be persisted
     *
     * @param typesDef A composite object that captures all type definition changes
     * @return A composite object with lists of type definitions that were updated
     */
    public AtlasTypesDef updateAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasServiceException {
        return callAPI(UPDATE_ALL_TYPE_DEFS, AtlasTypesDef.class, AtlasType.toJson(typesDef));
    }

    /**
     * Bulk delete API for all types
     *
     * @param typesDef A composite object that captures all types to be deleted
     */
    public void deleteAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasServiceException {
        callAPI(DELETE_ALL_TYPE_DEFS, AtlasTypesDef.class, AtlasType.toJson(typesDef));
    }

    /* Lineage Calls  */

    public AtlasLineageInfo getLineageInfo(final String guid, final LineageDirection direction, final int depth) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("direction", direction.toString());
        queryParams.add("depth", String.valueOf(depth));

        return callAPI(LINEAGE_INFO, AtlasLineageInfo.class, queryParams, guid);
    }

    /* Entity Calls */

    public AtlasEntityWithExtInfo getEntityByGuid(String guid) throws AtlasServiceException {
        return callAPI(GET_ENTITY_BY_GUID, AtlasEntityWithExtInfo.class, (MultivaluedMap<String, String>) null, guid);
    }

    public AtlasEntityWithExtInfo getEntityByAttribute(String type, Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = attributesToQueryParams(attributes);

        return callAPI(GET_ENTITY_BY_ATTRIBUTE, AtlasEntityWithExtInfo.class, queryParams, type);
    }

    public EntityMutationResponse updateEntityByAttribute(String type, Map<String, String> attributes, AtlasEntityWithExtInfo entityInfo)
            throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = attributesToQueryParams(attributes);

        return callAPI(UPDATE_ENTITY_BY_ATTRIBUTE, EntityMutationResponse.class, entityInfo, queryParams, type);
    }

    public EntityMutationResponse deleteEntityByAttribute(String type, Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = attributesToQueryParams(attributes);

        return callAPI(DELETE_ENTITY_BY_ATTRIBUTE, EntityMutationResponse.class, queryParams, type);
    }

    public EntityMutationResponse createEntity(AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        return callAPI(CREATE_ENTITY, EntityMutationResponse.class, entity);
    }

    public EntityMutationResponse updateEntity(AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        return callAPI(UPDATE_ENTITY, EntityMutationResponse.class, entity);
    }

    public EntityMutationResponse deleteEntityByGuid(String guid) throws AtlasServiceException {
        return callAPI(DELETE_ENTITY_BY_GUID, EntityMutationResponse.class, null, guid);
    }

    public AtlasEntitiesWithExtInfo getEntitiesByGuids(List<String> guids) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();

        queryParams.put("guid", guids);

        return callAPI(GET_ENTITIES_BY_GUIDS, AtlasEntitiesWithExtInfo.class, queryParams);
    }

    public EntityMutationResponse createEntities(AtlasEntitiesWithExtInfo atlasEntities) throws AtlasServiceException {
        return callAPI(CREATE_ENTITIES, EntityMutationResponse.class, atlasEntities);
    }

    public EntityMutationResponse updateEntities(AtlasEntitiesWithExtInfo atlasEntities) throws AtlasServiceException {
        return callAPI(UPDATE_ENTITIES, EntityMutationResponse.class, atlasEntities);
    }

    public EntityMutationResponse deleteEntitiesByGuids(List<String> guids) throws AtlasServiceException {
        return callAPI(DELETE_ENTITIES_BY_GUIDS, EntityMutationResponse.class, "guid", guids);
    }

    public AtlasClassifications getClassifications(String guid) throws AtlasServiceException {
        return callAPI(updatePathParameters(GET_CLASSIFICATIONS, guid), AtlasClassifications.class, null);
    }

    public void addClassifications(String guid, List<AtlasClassification> classifications) throws AtlasServiceException {
        callAPI(updatePathParameters(ADD_CLASSIFICATIONS, guid), (Class<?>)null, classifications, (String[]) null);
    }

    public void updateClassifications(String guid, List<AtlasClassification> classifications) throws AtlasServiceException {
        callAPI(updatePathParameters(UPDATE_CLASSIFICATIONS, guid), AtlasClassifications.class, classifications);
    }

    public void deleteClassifications(String guid, List<AtlasClassification> classifications) throws AtlasServiceException {
        callAPI(updatePathParameters(GET_CLASSIFICATIONS, guid), AtlasClassifications.class, classifications);
    }

    public void deleteClassification(String guid, String classificationName) throws AtlasServiceException {
        callAPI(updatePathParameters(DELETE_CLASSIFICATION, guid, classificationName), null, null);
    }


    private MultivaluedMap<String, String> attributesToQueryParams(Map<String, String> attributes) {
        return attributesToQueryParams(attributes, null);
    }

    private MultivaluedMap<String, String> attributesToQueryParams(Map<String, String>            attributes,
                                                                   MultivaluedMap<String, String> queryParams) {
        if (queryParams == null) {
            queryParams = new MultivaluedMapImpl();
        }

        if (MapUtils.isNotEmpty(attributes)) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                queryParams.putSingle(PREFIX_ATTR + e.getKey(), e.getValue());
            }
        }

        return queryParams;
    }

    /* Discovery calls */
    public AtlasSearchResult dslSearch(final String query) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);

        return callAPI(DSL_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult dslSearchWithParams(final String query, final int limit, final int offset) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);
        queryParams.add(LIMIT, String.valueOf(limit));
        queryParams.add(OFFSET, String.valueOf(offset));

        return callAPI(DSL_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult fullTextSearch(final String query) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);

        return callAPI(FULL_TEXT_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult fullTextSearchWithParams(final String query, final int limit, final int offset) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add(QUERY, query);
        queryParams.add(LIMIT, String.valueOf(limit));
        queryParams.add(OFFSET, String.valueOf(offset));

        return callAPI(FULL_TEXT_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult basicSearch(final String typeName, final String classification, final String query,
                                         final boolean excludeDeletedEntities, final int limit, final int offset) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("typeName", typeName);
        queryParams.add("classification", classification);
        queryParams.add(QUERY, query);
        queryParams.add("excludeDeletedEntities", String.valueOf(excludeDeletedEntities));
        queryParams.add(LIMIT, String.valueOf(limit));
        queryParams.add(OFFSET, String.valueOf(offset));

        return callAPI(BASIC_SEARCH, AtlasSearchResult.class, queryParams);
    }

    public AtlasSearchResult facetedSearch(SearchParameters searchParameters) throws AtlasServiceException {
        return callAPI(FACETED_SEARCH, AtlasSearchResult.class, searchParameters);
    }

    private <T> T getTypeDefByName(final String name, Class<T> typeDefClass) throws AtlasServiceException {
        String atlasPath = getAtlasPath(typeDefClass);
        APIInfo apiInfo = new APIInfo(String.format(GET_BY_NAME_TEMPLATE, atlasPath, name), HttpMethod.GET, Response.Status.OK);
        return callAPI(apiInfo, typeDefClass, null);
    }

    private <T> T getTypeDefByGuid(final String guid, Class<T> typeDefClass) throws AtlasServiceException {
        String atlasPath = getAtlasPath(typeDefClass);
        APIInfo apiInfo = new APIInfo(String.format(GET_BY_GUID_TEMPLATE, atlasPath, guid), HttpMethod.GET, Response.Status.OK);
        return callAPI(apiInfo, typeDefClass, null);
    }

    private <T> String getAtlasPath(Class<T> typeDefClass) {
        if (AtlasEnumDef.class.isAssignableFrom(typeDefClass)) {
            return "enumdef";
        } else if (AtlasEntityDef.class.isAssignableFrom(typeDefClass)) {
            return "entitydef";
        } else if (AtlasClassificationDef.class.isAssignableFrom(typeDefClass)) {
            return "classificationdef";
        } else if (AtlasStructDef.class.isAssignableFrom(typeDefClass)) {
            return "structdef";
        }
        // Code should never reach this point
        return "";
    }
}
