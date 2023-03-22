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
package org.apache.atlas.repository.util;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.CONNECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getListAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getStringAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class AccessControlUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AccessControlUtils.class);

    public static final String ATTR_ACCESS_CONTROL_ENABLED = "isAccessControlEnabled";
    public static final String ATTR_ACCESS_CONTROL_DENY_CM_GUIDS = "denyCustomMetadataGuids";
    public static final String ATTR_ACCESS_CONTROL_DENY_ASSET_TABS = "denyAssetTabs";

    public static final String ATTR_PERSONA_ROLE_ID = "roleId";
    public static final String ATTR_PERSONA_USERS   = "personaUsers";
    public static final String ATTR_PERSONA_GROUPS  = "personaGroups";

    public static final String ATTR_PURPOSE_CLASSIFICATIONS  = "purposeClassifications";

    public static final String ATTR_POLICY_TYPE  = "policyType";
    public static final String ATTR_POLICY_USERS  = "policyUsers";
    public static final String ATTR_POLICY_GROUPS  = "policyGroups";
    public static final String ATTR_POLICY_ROLES  = "policyRoles";
    public static final String ATTR_POLICY_ACTIONS  = "policyActions";
    public static final String ATTR_POLICY_CATEGORY  = "policyCategory";
    public static final String ATTR_POLICY_SUB_CATEGORY  = "policySubCategory";
    public static final String ATTR_POLICY_RESOURCES  = "policyResources";
    public static final String ATTR_POLICY_IS_ENABLED  = "isPolicyEnabled";
    public static final String ATTR_POLICY_RESOURCES_CATEGORY  = "policyResourceCategory";
    public static final String ATTR_POLICY_SERVICE_NAME  = "policyServiceName";

    public static final String REL_ATTR_ACCESS_CONTROL = "accessControl";
    public static final String REL_ATTR_POLICIES       = "policies";

    public static final String POLICY_TYPE_ALLOW  = "allow";
    public static final String POLICY_TYPE_DENY  = "deny";

    public static final String ACCESS_READ_PURPOSE_METADATA = "entity-read";
    public static final String ACCESS_READ_PERSONA_METADATA = "persona-asset-read";
    public static final String ACCESS_READ_PURPOSE_GLOSSARY = "persona-glossary-read";

    public static final String POLICY_CATEGORY_PERSONA  = "persona";
    public static final String POLICY_CATEGORY_PURPOSE  = "purpose";
    public static final String POLICY_CATEGORY_BOOTSTRAP  = "bootstrap";

    public static final String POLICY_SUB_CATEGORY_METADATA  = "metadata";
    public static final String POLICY_SUB_CATEGORY_GLOSSARY  = "glossary";
    public static final String POLICY_SUB_CATEGORY_DATA  = "data";

    public static final String RESOURCES_ENTITY = "entity:";
    public static final String RESOURCES_SPLITTER = ":";


    public static String getEntityName(AtlasEntity entity) {
        return (String) entity.getAttribute(NAME);
    }

    public static List<String> getPolicyAssets(AtlasEntity policyEntity) throws AtlasBaseException {
        List<String> resources = getPolicyResources(policyEntity);

        return getPolicyAssets(resources);
    }

    public static List<String> getPolicyAssets(List<String> resources) {
        return resources.stream()
                .filter(x -> x.startsWith(RESOURCES_ENTITY))
                .map(x -> x.split(RESOURCES_SPLITTER)[1])
                .collect(Collectors.toList());
    }

    public static List<String> getPolicyResources(AtlasEntity policyEntity) throws AtlasBaseException {
        return getListAttribute(policyEntity, ATTR_POLICY_RESOURCES);
    }

    public static List<String> getPolicyResources(AtlasEntityHeader policyEntity) {
        return getListAttribute(policyEntity, ATTR_POLICY_RESOURCES);
    }

    public static List<String> getPolicyActions(AtlasEntity policyEntity) {
        return getListAttribute(policyEntity, ATTR_POLICY_ACTIONS);
    }

    public static List<String> getPolicyActions(AtlasEntityHeader policyEntity) {
        return getListAttribute(policyEntity, ATTR_POLICY_ACTIONS);
    }

    public static String getPolicyCategory(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_CATEGORY);
    }

    public static String getPolicyCategory(AtlasEntityHeader policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_CATEGORY);
    }

    public static String getPolicySubCategory(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_SUB_CATEGORY);
    }

    public static String getPolicySubCategory(AtlasEntityHeader policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_SUB_CATEGORY);
    }

    public static AtlasEntity getConnectionEntity(EntityGraphRetriever entityRetriever, String connectionQualifiedName) throws AtlasBaseException {
        AtlasObjectId objectId = new AtlasObjectId(CONNECTION_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionQualifiedName));

        AtlasEntity entity = entityRetriever.toAtlasEntity(objectId);

        return entity;
    }
}
