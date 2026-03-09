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
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.DirectIndexQueryResult;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.ACCESS_CONTROL_ALREADY_EXISTS;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getListAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getQualifiedName;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getStringAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public final class AccessControlUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AccessControlUtils.class);

    public static final String ATTR_ACCESS_CONTROL_ENABLED = "isAccessControlEnabled";
    public static final String ATTR_ACCESS_CONTROL_DENY_CM_GUIDS = "denyCustomMetadataGuids";
    public static final String ATTR_ACCESS_CONTROL_DENY_ASSET_TABS = "denyAssetTabs";

    public static final String ATTR_PERSONA_ROLE_ID = "roleId";
    public static final String ATTR_PERSONA_USERS   = "personaUsers";
    public static final String ATTR_PERSONA_GROUPS  = "personaGroups";

    public static final String ATTR_PURPOSE_CLASSIFICATIONS  = "purposeClassifications";

    public static final String ATTR_SERVICE_SERVICE_TYPE = "authServiceType";
    public static final String ATTR_SERVICE_TAG_SERVICE  = "tagService";
    public static final String ATTR_SERVICE_IS_ENABLED   = "authServiceIsEnabled";
    public static final String ATTR_SERVICE_LAST_SYNC    = "authServicePolicyLastSync";
    public static final String ATTR_POLICY_TYPE  = "policyType";
    public static final String ATTR_POLICY_USERS  = "policyUsers";
    public static final String ATTR_POLICY_GROUPS  = "policyGroups";
    public static final String ATTR_POLICY_ROLES  = "policyRoles";
    public static final String ATTR_POLICY_ACTIONS  = "policyActions";
    public static final String ATTR_POLICY_CONDITIONS  = "policyConditions";
    public static final String ATTR_POLICY_CATEGORY  = "policyCategory";
    public static final String ATTR_POLICY_SUB_CATEGORY  = "policySubCategory";
    public static final String ATTR_POLICY_FILTER_CRITERIA = "policyFilterCriteria";
    public static final String ATTR_POLICY_RESOURCES  = "policyResources";
    public static final String ATTR_POLICY_IS_ENABLED  = "isPolicyEnabled";
    public static final String ATTR_POLICY_CONNECTION_QN  = "connectionQualifiedName";
    public static final String ATTR_POLICY_RESOURCES_CATEGORY  = "policyResourceCategory";
    public static final String ATTR_POLICY_SERVICE_NAME  = "policyServiceName";
    public static final String ATTR_POLICY_PRIORITY  = "policyPriority";
    public static final String ATTR_QUALIFIED_NAME  = "qualifiedName";

    public static final String REL_ATTR_ACCESS_CONTROL = "accessControl";
    public static final String REL_ATTR_POLICIES       = "policies";

    public static final String POLICY_TYPE_ALLOW  = "allow";
    public static final String POLICY_TYPE_DENY  = "deny";

    public static final String ATTR_CHANNEL_LINK = "channelLink";

    public static final String ACCESS_READ_PURPOSE_METADATA = "entity-read";
    public static final String ACCESS_READ_PERSONA_METADATA = "persona-asset-read";
    public static final String ACCESS_READ_PERSONA_GLOSSARY = "persona-glossary-read";
    public static final String ACCESS_READ_PERSONA_DOMAIN = "persona-domain-read";
    public static final String ACCESS_READ_PERSONA_SUB_DOMAIN = "persona-domain-sub-domain-read";
    public static final String ACCESS_READ_PERSONA_PRODUCT = "persona-domain-product-read";
    public static final String ACCESS_READ_PERSONA_AI_APP = "persona-ai-application-read";
    public static final String ACCESS_READ_PERSONA_AI_MODEL = "persona-ai-model-read";


    public static final String POLICY_CATEGORY_PERSONA  = "persona";
    public static final String POLICY_CATEGORY_PURPOSE  = "purpose";
    public static final String POLICY_CATEGORY_DATAMESH = "datamesh";
    public static final String POLICY_CATEGORY_BOOTSTRAP  = "bootstrap";

    public static final String POLICY_SUB_CATEGORY_COLLECTION = "collection";

    public static final String POLICY_RESOURCE_CATEGORY_PERSONA_CUSTOM  = "CUSTOM";
    public static final String POLICY_RESOURCE_CATEGORY_PERSONA_ENTITY  = "ENTITY";
    public static final String POLICY_RESOURCE_CATEGORY_PURPOSE  = "TAG";

    public static final String POLICY_SUB_CATEGORY_METADATA  = "metadata";
    public static final String POLICY_SUB_CATEGORY_GLOSSARY  = "glossary";
    public static final String POLICY_SUB_CATEGORY_DOMAIN  = "domain";
    public static final String POLICY_SUB_CATEGORY_DATA  = "data";
    public static final String POLICY_SUB_CATEGORY_PRODUCT  = "dataProduct";
    public static final String POLICY_SUB_CATEGORY_AI = "ai";

    public static final String RESOURCES_ENTITY = "entity:";
    public static final String RESOURCES_ENTITY_TYPE = "entity-type:";
    public static final String RESOURCES_TAG = "tag:";
    public static final String RESOURCES_SPLITTER = ":";

    private static final String CONNECTION_QN = "%s/%s/%s";
    public static final String CONN_NAME_PATTERN = "connection_admins_%s";
    public static final String ARGO_SERVICE_USER_NAME = "service-account-atlan-argo";
    public static final String BACKEND_SERVICE_USER_NAME = "service-account-atlan-backend";
    public static final String GOVERNANCE_WORKFLOWS_SERVICE_USER_NAME = "atlan-governance-workflows";

    public static final String INSTANCE_DOMAIN_KEY = "instance";

    public static final String POLICY_SERVICE_NAME_ABAC  = "atlas_abac";
    public static final String POLICY_SERVICE_NAME_ATLAS  = "atlas";
    public static final String POLICY_SERVICE_NAME_HEKA  = "heka";

    public static final String POLICY_FILTER_CRITERIA_ENTITY = "entity";
    public static final String POLICY_FILTER_CRITERIA_END1 = "endOneEntity";
    public static final String POLICY_FILTER_CRITERIA_END2 = "endTwoEntity";

    public static final String POLICY_FILTER_CRITERIA_CRITERION = "criterion";
    public static final String POLICY_FILTER_CRITERIA_CONDITION = "condition";
    public static final String POLICY_FILTER_CRITERIA_OPERATAOR = "operator";
    public static final String POLICY_FILTER_CRITERIA_AND = "AND";
    public static final String POLICY_FILTER_CRITERIA_OR = "OR";
    public static final String POLICY_FILTER_CRITERIA_EQUALS = "EQUALS";
    public static final String POLICY_FILTER_CRITERIA_NOT_EQUALS = "NOT_EQUALS";
    public static final String POLICY_FILTER_CRITERIA_IN = "IN";
    public static final String POLICY_FILTER_CRITERIA_NOT_IN = "NOT_IN";
    public static final String POLICY_FILTER_CRITERIA_STARTS_WITH = "STARTS_WITH";
    public static final String POLICY_FILTER_CRITERIA_ENDS_WITH = "ENDS_WITH";

    public static final String ATTR_TAG = "__traitNames";
    public static final String ATTR_PROPAGATED_TAG = "__propagatedTraitNames";
    public static final List<String> ATTR_TAGS = List.of(ATTR_TAG, ATTR_PROPAGATED_TAG);
    
    public static final Set<String> POLICY_FILTER_CRITERIA_VALID_OPS = Set.of(POLICY_FILTER_CRITERIA_EQUALS,
            POLICY_FILTER_CRITERIA_NOT_EQUALS, POLICY_FILTER_CRITERIA_IN, POLICY_FILTER_CRITERIA_NOT_IN,
            POLICY_FILTER_CRITERIA_STARTS_WITH, POLICY_FILTER_CRITERIA_ENDS_WITH);
    public static final Set<String> POLICY_FILTER_CRITERIA_NEGATIVE_OPS = Set.of(POLICY_FILTER_CRITERIA_NOT_EQUALS,
            POLICY_FILTER_CRITERIA_NOT_IN);


    private AccessControlUtils() {}

    public static String getEntityName(AtlasEntity entity) {
        return (String) entity.getAttribute(NAME);
    }

    public static String getEntityQualifiedName(AtlasEntity entity) {
        return getStringAttribute(entity, QUALIFIED_NAME);
    }

    public static List<String> getPolicyAssets(AtlasEntity policyEntity) throws AtlasBaseException {
        List<String> resources = getPolicyResources(policyEntity);

        return getFilteredPolicyResources(resources, RESOURCES_ENTITY);
    }

    public static List<String> getFilteredPolicyResources(List<String> resources, String resourcePrefix) {
        return resources.stream()
                .filter(x -> x.startsWith(resourcePrefix))
                .map(x -> x.substring(resourcePrefix.length()))
                .collect(Collectors.toList());
    }

    public static String getPolicyConnectionQN(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_CONNECTION_QN);
    }

    public static String getPolicyConnectionQN(AtlasEntityHeader policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_CONNECTION_QN);
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

    public static String getPolicyFilterCriteria(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_FILTER_CRITERIA);
    }

    public static String getPolicyFilterCriteria(AtlasEntityHeader policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_FILTER_CRITERIA);
    }

    public static String getPolicyResourceCategory(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_RESOURCES_CATEGORY);
    }

    public static String getPolicyResourceCategory(AtlasEntityHeader policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_RESOURCES_CATEGORY);
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

    public static String getPolicyServiceName(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_SERVICE_NAME);
    }

    public static String getPolicyServiceName(AtlasEntityHeader policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_SERVICE_NAME);
    }

    public static String getPolicyType(AtlasEntity policyEntity) {
        return getStringAttribute(policyEntity, ATTR_POLICY_TYPE);
    }

    public static List<String> getPolicyRoles(AtlasEntity policyEntity) {
        return getListAttribute(policyEntity, ATTR_POLICY_ROLES);
    }


    public static boolean getIsAllowPolicy(AtlasEntity policyEntity) throws AtlasBaseException {
        String policyType = (String) policyEntity.getAttribute(ATTR_POLICY_TYPE);

        if (POLICY_TYPE_ALLOW.equals(policyType)) {
            return true;
        } else if (POLICY_TYPE_DENY.equals(policyType)) {
            return false;
        } else {
            throw new AtlasBaseException("Unsupported policy type while creating index alias filters");
        }
    }

    public static AtlasEntity getEntityByQualifiedName(EntityGraphRetriever entityRetriever, String connectionQualifiedName) throws AtlasBaseException {
        AtlasObjectId objectId = new AtlasObjectId(CONNECTION_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionQualifiedName));

        AtlasEntity entity = entityRetriever.toAtlasEntity(objectId);

        return entity;
    }

    public static String getConnectionQualifiedNameFromPolicyAssets(EntityGraphRetriever entityRetriever, List<String> assets) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Policy assets could not be null");
        }

        AtlasEntity connection = extractConnectionFromResource(entityRetriever, assets.get(0));

        return getQualifiedName(connection);
    }

    public static AtlasEntity extractConnectionFromResource(EntityGraphRetriever entityRetriever, String assetQName) throws AtlasBaseException {
        AtlasEntity connection = null;

        String[] splitted = assetQName.split("/");
        String connectionQName;
        try {
            connectionQName = String.format(CONNECTION_QN, splitted[0], splitted[1], splitted[2]);
        } catch (ArrayIndexOutOfBoundsException aib) {
            LOG.error("Failed to extract qualifiedName of the connection: " + assetQName);
            return null;
        }

        connection = getEntityByQualifiedName(entityRetriever, connectionQName);

        return connection;
    }

    public static String getPersonaRoleName(AtlasEntity persona) {
        return "persona_" + getESAliasName(persona);
    }

    public static String getESAliasName(AtlasEntity entity) {
        String qualifiedName = getStringAttribute(entity, QUALIFIED_NAME);
        return getESAliasName(qualifiedName);
    }

    public static String getESAliasName(String qualifiedName) {
        String[] parts = qualifiedName.split("/");

        return parts[1];
    }

    public static List<AtlasEntity> getPolicies(AtlasEntity.AtlasEntityWithExtInfo accessControl) {
        List<AtlasObjectId> policies = (List<AtlasObjectId>) accessControl.getEntity().getRelationshipAttribute(REL_ATTR_POLICIES);

        return objectToEntityList(accessControl, policies);
    }

    public static List<AtlasEntity> objectToEntityList(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo, List<AtlasObjectId> policies) {
        List<AtlasEntity> ret = new ArrayList<>();

        Set<String> referredGuids =  entityWithExtInfo.getReferredEntities().keySet();
        if (policies != null) {
            ret = policies.stream()
                    .filter(x -> referredGuids.contains(x.getGuid()))
                    .map(x -> entityWithExtInfo.getReferredEntity(x.getGuid()))
                    .filter(x -> x.getStatus() == null || x.getStatus() == AtlasEntity.Status.ACTIVE)
                    .collect(Collectors.toList());
        }

        return ret;
    }

    public static List<String> getPurposeTags(AtlasStruct entity) {
        return getListAttribute(entity, ATTR_PURPOSE_CLASSIFICATIONS);
    }

    public static boolean getIsAccessControlEnabled(AtlasEntity entity) {
        return (boolean) entity.getAttribute(ATTR_ACCESS_CONTROL_ENABLED);
    }

    public static boolean getIsPolicyEnabled(AtlasEntityHeader entity)  {
        if (entity.hasAttribute(ATTR_POLICY_IS_ENABLED)) {
            return (boolean) entity.getAttribute(ATTR_POLICY_IS_ENABLED);
        }
        return true;
    }

    public static List<String> getPersonaUsers(AtlasStruct entity) {
        return getListAttribute(entity, ATTR_PERSONA_USERS);
    }

    public static List<String> getPersonaGroups(AtlasStruct entity) {
        return getListAttribute(entity, ATTR_PERSONA_GROUPS);
    }

    public static String getPersonaRoleId(AtlasEntity entity) {
        String roleId = (String) entity.getAttribute(ATTR_PERSONA_ROLE_ID);
        if (roleId == null) {
            LOG.warn("roleId not found for Persona with GUID " + entity.getGuid());
        }
        return roleId;
    }

    public static String getTenantId(AtlasStruct entity) {
        String ret = DEFAULT_TENANT_ID;

        Object tenantId = entity.getAttribute(ATTR_TENANT_ID);

        if (tenantId != null) {
            String tenantIdAsString = (String) tenantId;
            if (tenantIdAsString.length() > 0) {
                ret = tenantIdAsString;
            }
        }

        return ret;
    }

    public static void validateNoPoliciesAttached(AtlasEntity entity) throws AtlasBaseException {
        List<AtlasObjectId> policies = (List<AtlasObjectId>) entity.getRelationshipAttribute(REL_ATTR_POLICIES);
        if (CollectionUtils.isNotEmpty(policies)) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Can not attach a policy while creating/updating Persona/Purpose/Stakeholder");
        }
    }

    public static String getUUID(){
        return NanoIdUtils.randomNanoId(22);
    }

    public static void validateUniquenessByTags(AtlasGraph graph, List<String> tags, String typeName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("size", 1);

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustClauseList.add(mapOf("terms", mapOf(ATTR_PURPOSE_CLASSIFICATIONS, tags)));

        Map<String, Object> scriptMap = mapOf("inline", "doc['" + ATTR_PURPOSE_CLASSIFICATIONS + "'].length == params.list_length");
        scriptMap.put("lang", "painless");
        scriptMap.put("params", mapOf("list_length", tags.size()));

        mustClauseList.add(mapOf("script", mapOf("script", scriptMap)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);

        if (hasMatchingVertex(graph, tags, indexSearchParams)){
            throw new AtlasBaseException(String.format("Entity already exists, typeName:tags, %s:%s", typeName, tags));
        }
    }

    private static boolean hasMatchingVertex(AtlasGraph graph, List<String> newTags,
                                               IndexSearchParams indexSearchParams) throws AtlasBaseException {
        String vertexIndexName = VERTEX_INDEX_NAME;
        AtlasIndexQuery indexQuery = graph.elasticsearchQuery(vertexIndexName);

        DirectIndexQueryResult indexQueryResult = indexQuery.vertices(indexSearchParams);
        if (indexQueryResult == null || indexQueryResult.getIterator() == null) {
            return false;
        }
        Iterator<AtlasIndexQuery.Result> iterator = indexQueryResult.getIterator();

        while (iterator.hasNext()) {
            AtlasVertex vertex = iterator.next().getVertex();
            if (vertex != null) {
                List<String> tags = (List<String>) vertex.getPropertyValues(ATTR_PURPOSE_CLASSIFICATIONS, String.class);

                //TODO: handle via ES query if possible -> match exact tags list
                if (CollectionUtils.isEqualCollection(tags, newTags)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static void validateUniquenessByName(AtlasGraph graph, String name, String typeName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("size", 1);

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustClauseList.add(mapOf("term", mapOf("name.keyword", name)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);

        if (checkEntityExists(graph, indexSearchParams)){
            throw new AtlasBaseException(ACCESS_CONTROL_ALREADY_EXISTS, typeName, name);
        }
    }

    private static boolean checkEntityExists(AtlasGraph graph, IndexSearchParams indexSearchParams) throws AtlasBaseException {
        AtlasIndexQuery indexQuery = graph.elasticsearchQuery(VERTEX_INDEX_NAME);

        DirectIndexQueryResult indexQueryResult = indexQuery.vertices(indexSearchParams);
        if (indexQueryResult == null || indexQueryResult.getIterator() == null) {
            return false;
        }
        Iterator<AtlasIndexQuery.Result> iterator = indexQueryResult.getIterator();

        while (iterator.hasNext()) {
            AtlasVertex vertex = iterator.next().getVertex();
            if (vertex != null) {
                return true;
            }
        }

        return false;
    }

    public static boolean isABACPolicyService(AtlasEntity entity) {
        return POLICY_SERVICE_NAME_ABAC.equals(getPolicyServiceName(entity));
    }
}
