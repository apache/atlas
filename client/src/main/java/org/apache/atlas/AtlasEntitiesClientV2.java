/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.AtlasClassifications;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class AtlasEntitiesClientV2 extends AtlasBaseClient {
    public static final String ENTITY_API = BASE_URI + "v2/entity/";

    private static final APIInfo GET_ENTITY_BY_GUID         = new APIInfo(ENTITY_API + "guid/%s", HttpMethod.GET, Response.Status.OK);
    private static final APIInfo GET_ENTITY_BY_ATTRIBUTE    = new APIInfo(ENTITY_API + "uniqueAttribute/type/%s", HttpMethod.GET, Response.Status.OK);
    private static final APIInfo CREATE_ENTITY              = new APIInfo(ENTITY_API, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo UPDATE_ENTITY              = CREATE_ENTITY;
    private static final APIInfo UPDATE_ENTITY_BY_ATTRIBUTE = new APIInfo(ENTITY_API + "uniqueAttribute/type/%s", HttpMethod.PUT, Response.Status.OK);
    private static final APIInfo DELETE_ENTITY_BY_GUID      = new APIInfo(ENTITY_API + "guid/%s", HttpMethod.DELETE, Response.Status.OK);
    private static final APIInfo DELETE_ENTITY_BY_ATTRIBUTE = new APIInfo(ENTITY_API + "uniqueAttribute/type/%s", HttpMethod.DELETE, Response.Status.OK);

    private static final APIInfo GET_ENTITIES_BY_GUIDS    = new APIInfo(ENTITY_API + "bulk/", HttpMethod.GET, Response.Status.OK);
    private static final APIInfo CREATE_ENTITIES          = new APIInfo(ENTITY_API + "bulk/", HttpMethod.POST, Response.Status.OK);
    private static final APIInfo UPDATE_ENTITIES          = CREATE_ENTITIES;
    private static final APIInfo DELETE_ENTITIES_BY_GUIDS = new APIInfo(ENTITY_API + "bulk/", HttpMethod.DELETE, Response.Status.OK);

    private static final APIInfo GET_CLASSIFICATIONS    = new APIInfo(ENTITY_API + "guid/%s/classifications", HttpMethod.GET, Response.Status.OK);
    private static final APIInfo ADD_CLASSIFICATIONS    = new APIInfo(ENTITY_API + "guid/%s/classifications", HttpMethod.POST, Response.Status.NO_CONTENT);
    private static final APIInfo UPDATE_CLASSIFICATIONS = new APIInfo(ENTITY_API + "guid/%s/classifications", HttpMethod.PUT, Response.Status.OK);
    private static final APIInfo DELETE_CLASSIFICATION  = new APIInfo(ENTITY_API + "guid/%s/classification/%s", HttpMethod.DELETE, Response.Status.NO_CONTENT);


    public static final String PREFIX_ATTR = "attr:";


    public AtlasEntitiesClientV2(String[] baseUrl, String[] basicAuthUserNamePassword) {
        super(baseUrl, basicAuthUserNamePassword);
    }

    public AtlasEntitiesClientV2(String... baseUrls) throws AtlasException {
        super(baseUrls);
    }

    public AtlasEntitiesClientV2(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        super(ugi, doAsUser, baseUrls);
    }

    protected AtlasEntitiesClientV2() {
        super();
    }

    @VisibleForTesting
    AtlasEntitiesClientV2(WebResource service, Configuration configuration) {
        super(service, configuration);
    }

    public AtlasEntityWithExtInfo getEntityByGuid(String guid) throws AtlasServiceException {
        return callAPI(formatPathForPathParams(GET_ENTITY_BY_GUID, guid), AtlasEntityWithExtInfo.class, null);
    }

    public AtlasEntityWithExtInfo getEntityByAttribute(String type, Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = attributesToQueryParams(attributes);

        return callAPI(formatPathForPathParams(GET_ENTITY_BY_ATTRIBUTE, type), AtlasEntityWithExtInfo.class, queryParams);
    }

    public EntityMutationResponse updateEntityByAttribute(String type, Map<String, String> attributes, AtlasEntity entity) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = attributesToQueryParams(attributes);

        return callAPI(formatPathForPathParams(UPDATE_ENTITY_BY_ATTRIBUTE, type), entity, EntityMutationResponse.class, queryParams);
    }

    public EntityMutationResponse deleteEntityByAttribute(String type, Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = attributesToQueryParams(attributes);

        return callAPI(formatPathForPathParams(DELETE_ENTITY_BY_ATTRIBUTE, type), null, EntityMutationResponse.class, queryParams);
    }

    public EntityMutationResponse createEntity(AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        return callAPI(CREATE_ENTITY, entity, EntityMutationResponse.class);
    }

    public EntityMutationResponse updateEntity(AtlasEntityWithExtInfo entity) throws AtlasServiceException {
        return callAPI(UPDATE_ENTITY, entity, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntityByGuid(String guid) throws AtlasServiceException {
        return callAPI(formatPathForPathParams(DELETE_ENTITY_BY_GUID, guid), EntityMutationResponse.class, null);
    }


    public AtlasEntitiesWithExtInfo getEntitiesByGuids(List<String> guids) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();

        queryParams.put("guid", guids);

        return callAPI(GET_ENTITIES_BY_GUIDS, AtlasEntitiesWithExtInfo.class, queryParams);
    }

    public EntityMutationResponse createEntities(AtlasEntitiesWithExtInfo atlasEntities) throws AtlasServiceException {
        return callAPI(CREATE_ENTITIES, atlasEntities, EntityMutationResponse.class);
    }

    public EntityMutationResponse updateEntities(AtlasEntitiesWithExtInfo atlasEntities) throws AtlasServiceException {
        return callAPI(UPDATE_ENTITIES, atlasEntities, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntitiesByGuids(List<String> guids) throws AtlasServiceException {
        return callAPI(DELETE_ENTITIES_BY_GUIDS, EntityMutationResponse.class, "guid", guids);
    }


    public AtlasClassifications getClassifications(String guid) throws AtlasServiceException {
        return callAPI(formatPathForPathParams(GET_CLASSIFICATIONS, guid), null, AtlasClassifications.class);
    }

    public void addClassifications(String guid, List<AtlasClassification> classifications) throws AtlasServiceException {
        callAPI(formatPathForPathParams(ADD_CLASSIFICATIONS, guid), classifications, (Class<?>)null, (String[]) null);
    }

    public void updateClassifications(String guid, List<AtlasClassification> classifications) throws AtlasServiceException {
        callAPI(formatPathForPathParams(UPDATE_CLASSIFICATIONS, guid), classifications, AtlasClassifications.class);
    }

    public void deleteClassifications(String guid, List<AtlasClassification> classifications) throws AtlasServiceException {
        callAPI(formatPathForPathParams(GET_CLASSIFICATIONS, guid), classifications, AtlasClassifications.class);
    }

    public void deleteClassification(String guid, String classificationName) throws AtlasServiceException {
        callAPI(formatPathForPathParams(DELETE_CLASSIFICATION, guid, classificationName), null, null);
    }

    // Entities operations
    public List<AtlasEntity> getEntities(List<String> entityIds) {
        // TODO Map the query params correctly
        return null;
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
}
