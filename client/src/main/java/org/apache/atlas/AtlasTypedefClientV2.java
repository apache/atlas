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

import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

public class AtlasTypedefClientV2 extends AtlasBaseClient {

    private static final String BASE_URI = "api/atlas/v2/types/";
    private static final String ENUMDEF_URI = BASE_URI + "enumdef/";
    private static final String STRUCTDEF_URI = BASE_URI + "structdef/";
    private static final String ENTITYDEF_URI = BASE_URI + "entitydef/";
    private static final String CLASSIFICATIONDEF_URI = BASE_URI + "classificationdef/";
    private static final String TYPEDEFS_PATH = BASE_URI + "typedefs/";
    private static final String GET_BY_NAME_TEMPLATE = BASE_URI + "%s/name/%s";
    private static final String GET_BY_GUID_TEMPLATE = BASE_URI + "%s/guid/%s";

    private static final APIInfo CREATE_ENUM_DEF = new APIInfo(ENUMDEF_URI, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo CREATE_STRUCT_DEF = new APIInfo(STRUCTDEF_URI, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo CREATE_ENTITY_DEF = new APIInfo(ENTITYDEF_URI, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo CREATE_CLASSIFICATION_DEF = new APIInfo(CLASSIFICATIONDEF_URI, HttpMethod.POST, Response.Status.OK);

    private static final APIInfo GET_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_PATH, HttpMethod.GET, Response.Status.OK);
    private static final APIInfo CREATE_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_PATH, HttpMethod.POST, Response.Status.OK);
    private static final APIInfo UPDATE_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_PATH, HttpMethod.PUT, Response.Status.OK);
    private static final APIInfo DELETE_ALL_TYPE_DEFS = new APIInfo(TYPEDEFS_PATH, HttpMethod.DELETE, Response.Status.OK);

    public AtlasTypedefClientV2(String[] baseUrl, String[] basicAuthUserNamePassword) {
        super(baseUrl, basicAuthUserNamePassword);
    }

    public AtlasTypedefClientV2(String... baseUrls) throws AtlasException {
        super(baseUrls);
    }

    public AtlasTypedefClientV2(UserGroupInformation ugi, String doAsUser, String... baseUrls) {
        super(ugi, doAsUser, baseUrls);
    }

    protected AtlasTypedefClientV2() {
        super();
    }

    @VisibleForTesting
    AtlasTypedefClientV2(WebResource service, Configuration configuration) {
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

    public AtlasEnumDef getEnumByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasEnumDef.class);
    }

    public AtlasEnumDef getEnumByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasEnumDef.class);
    }

    public AtlasStructDef getStructByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasStructDef.class);
    }

    public AtlasStructDef getStructByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasStructDef.class);
    }

    public AtlasClassificationDef getClassificationByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasClassificationDef.class);
    }

    public AtlasClassificationDef getClassificationByGuid(final String guid) throws AtlasServiceException {
        return getTypeDefByGuid(guid, AtlasClassificationDef.class);
    }

    public AtlasEntityDef getEntityByName(final String name) throws AtlasServiceException {
        return getTypeDefByName(name, AtlasEntityDef.class);
    }

    public AtlasEntityDef getEntityByGuid(final String guid) throws AtlasServiceException {
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
        return callAPI(CREATE_ALL_TYPE_DEFS, AtlasType.toJson(typesDef), AtlasTypesDef.class);
    }

    /**
     * Bulk update API for all types, changes detected in the type definitions would be persisted
     *
     * @param typesDef A composite object that captures all type definition changes
     * @return A composite object with lists of type definitions that were updated
     */
    public AtlasTypesDef updateAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasServiceException {
        return callAPI(UPDATE_ALL_TYPE_DEFS, AtlasType.toJson(typesDef), AtlasTypesDef.class);
    }

    /**
     * Bulk delete API for all types
     *
     * @param typesDef A composite object that captures all types to be deleted
     */
    public void deleteAtlasTypeDefs(final AtlasTypesDef typesDef) throws AtlasServiceException {
        callAPI(DELETE_ALL_TYPE_DEFS, AtlasType.toJson(typesDef), AtlasTypesDef.class);
    }

    private <T> T getTypeDefByName(final String name, Class<T> typeDefClass) throws AtlasServiceException {
        String atlasPath = getAtlasPath(typeDefClass);
        APIInfo apiInfo = new APIInfo(String.format(GET_BY_NAME_TEMPLATE, atlasPath, name), HttpMethod.GET, Response.Status.OK);
        return callAPI(apiInfo, null, typeDefClass);
    }

    private <T> T getTypeDefByGuid(final String guid, Class<T> typeDefClass) throws AtlasServiceException {
        String atlasPath = getAtlasPath(typeDefClass);
        APIInfo apiInfo = new APIInfo(String.format(GET_BY_GUID_TEMPLATE, atlasPath, guid), HttpMethod.GET, Response.Status.OK);
        return callAPI(apiInfo, null, typeDefClass);
    }

    private <T> String getAtlasPath(Class<T> typeDefClass) {
        if (typeDefClass.isAssignableFrom(AtlasEnumDef.class)) {
            return "enumdef";
        } else if (typeDefClass.isAssignableFrom(AtlasEntityDef.class)) {
            return "entitydef";
        } else if (typeDefClass.isAssignableFrom(AtlasClassificationDef.class)) {
            return "classificationdef";
        } else if (typeDefClass.isAssignableFrom(AtlasStructDef.class)) {
            return "structdef";
        }
        // Code should never reach this pion
        return "";
    }
}
