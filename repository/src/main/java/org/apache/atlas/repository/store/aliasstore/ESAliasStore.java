/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.aliasstore;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.ESAliasRequestBuilder.AliasAction;
import org.apache.atlas.authorizer.JsonToElasticsearchQuery;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.ADD;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_DOMAIN;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_GLOSSARY;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_PRODUCT;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_SUB_DOMAIN;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_AI_APP;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_AI_MODEL;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SERVICE_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SERVICE_NAME_ABAC;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_FILTER_CRITERIA;
import static org.apache.atlas.repository.util.AccessControlUtils.getConnectionQualifiedNameFromPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getESAliasName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsAllowPolicy;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicies;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getFilteredPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyConnectionQN;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicySubCategory;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.Constants.GLOSSARY_PROPERTY_KEY;


@Component
public class ESAliasStore implements IndexAliasStore {
    private static final Logger LOG = LoggerFactory.getLogger(ESAliasStore.class);
    public static final String NEW_WILDCARD_DOMAIN_SUPER = "default/domain/*/super";
    public static final String ENABLE_PERSONA_HIERARCHY_FILTER = "enable_persona_hierarchy_filter";

    private final AtlasGraph graph;
    private final EntityGraphRetriever entityRetriever;

    private final int assetsMaxLimit = AtlasConfiguration.PERSONA_POLICY_ASSET_MAX_LIMIT.getInt();

    @Inject
    public ESAliasStore(AtlasGraph graph,
                        EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.entityRetriever = entityRetriever;
    }

    @Override
    public boolean createAlias(AtlasEntity entity) throws AtlasBaseException {
        String aliasName = getAliasName(entity);

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();

        if (PERSONA_ENTITY_TYPE.equals(entity.getTypeName())) {
            requestBuilder.addAction(ADD, new AliasAction(getIndexNameFromAliasIfExists(VERTEX_INDEX_NAME), aliasName, getFilterForPersona(null, null)));
        } else {
            requestBuilder.addAction(ADD, new AliasAction(getIndexNameFromAliasIfExists(VERTEX_INDEX_NAME), aliasName, getFilterForPurpose(entity)));
        }

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    private String getIndexNameFromAliasIfExists(final String aliasIndexName) throws AtlasBaseException {
        try {
            RestHighLevelClient esClient = AtlasElasticsearchDatabase.getClient();
            GetAliasesRequest aliasesRequest = new GetAliasesRequest(aliasIndexName);
            GetAliasesResponse aliasesResponse = esClient.indices().getAlias(aliasesRequest, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetadata>> aliases = aliasesResponse.getAliases();
            for (Map.Entry<String, Set<AliasMetadata>> entry : aliases.entrySet()) {
                String indexName = entry.getKey();
                Set<AliasMetadata> aliasMetadataList = entry.getValue();
                for (AliasMetadata aliasMetadata : aliasMetadataList) {
                    if (aliasIndexName.equals(aliasMetadata.alias())) {
                        return indexName;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error while fetching index for alias index {}", aliasIndexName, e);
            throw new AtlasBaseException(e);
        }
        return aliasIndexName;
    }

    @Override
    public boolean updateAlias(AtlasEntity.AtlasEntityWithExtInfo accessControl, AtlasEntity policy) throws AtlasBaseException {
        String aliasName = getAliasName(accessControl.getEntity());

        Map<String, Object> filter;

        if (PERSONA_ENTITY_TYPE.equals(accessControl.getEntity().getTypeName())) {
            filter = getFilterForPersona(accessControl, policy);
            if (filter == null || filter.isEmpty()) {
                filter = getEmptyFilter();
            }
        } else {
            filter = getFilterForPurpose(accessControl.getEntity());
        }

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(getIndexNameFromAliasIfExists(VERTEX_INDEX_NAME), aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);

        return true;
    }

    @Override
    public boolean deleteAlias(String aliasName) throws AtlasBaseException {
        graph.deleteESAlias(getIndexNameFromAliasIfExists(VERTEX_INDEX_NAME), aliasName);
        return true;
    }

    private Map<String, Object> getFilterForPersona(AtlasEntity.AtlasEntityWithExtInfo persona, AtlasEntity policy) throws AtlasBaseException {
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();

        if (policy == null && persona == null){
            return getEmptyFilter();
        }

        List<AtlasEntity> policies = getPolicies(persona);
        if (policy != null) {
            policies.add(policy);
        }
        if (CollectionUtils.isNotEmpty(policies)) {
            boolean useHierarchicalQualifiedNameFilter = DynamicConfigStore.isPersonaHierarchyFilterEnabled();
            personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList, useHierarchicalQualifiedNameFilter);
        }

        return esClausesToFilter(allowClauseList, denyClauseList);
    }

    private Map<String, Object> getFilterForPurpose(AtlasEntity purpose) throws AtlasBaseException {

        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>(); // deny policies are not include for purpose

        List<String> tags = getPurposeTags(purpose);
        addPurposeMetadataFilterClauses(tags, allowClauseList);

        return esClausesToFilter(allowClauseList, denyClauseList);
    }

    private void personaPolicyToESDslClauses(List<AtlasEntity> policies,
                                             List<Map<String, Object>> allowClauseList, List<Map<String, Object>> denyClauseList, boolean useHierarchicalQualifiedNameFilter) throws AtlasBaseException {
        
        // Group related collections together
        TermCollections allowCollections = new TermCollections();
        TermCollections denyCollections = new TermCollections();
        
        for (AtlasEntity policy: policies) {

            if (policy.getStatus() == null || AtlasEntity.Status.ACTIVE.equals(policy.getStatus())) {
                boolean isAllowPolicy = getIsAllowPolicy(policy);
                
                // Select the appropriate terms and clause list based on policy type
                TermCollections terms = isAllowPolicy ? allowCollections : denyCollections;
                List<Map<String, Object>> clauseList = isAllowPolicy ? allowClauseList : denyClauseList;

                List<String> assets = getPolicyAssets(policy);
                List<String> policyActions = getPolicyActions(policy);
                String policyServiceName = (String) policy.getAttribute(ATTR_POLICY_SERVICE_NAME);


                // Handle ABAC policies
                if (POLICY_SERVICE_NAME_ABAC.equals(policyServiceName)) {
                    if (!policyActions.contains(ACCESS_READ_PERSONA_METADATA)) {
                        continue;
                    }
                    String policyFilterCriteria = (String) policy.getAttribute(ATTR_POLICY_FILTER_CRITERIA);
                    JsonNode entityFilterCriteriaNode = JsonToElasticsearchQuery.parseFilterJSON(policyFilterCriteria, "entity");

                    if (entityFilterCriteriaNode == null) continue;

                    try {
                        JsonNode dsl = JsonToElasticsearchQuery.convertJsonToQuery(entityFilterCriteriaNode);
                        clauseList.add(mapOf("bool", dsl.get("bool")));
                    } catch (Exception e) {
                        LOG.error("Error processing ABAC policy filter criteria for policy {}", policy.getGuid(), e);
                    }
                    continue;
                } else if (policyActions.contains(ACCESS_READ_PERSONA_METADATA)) {

                    if (!POLICY_SUB_CATEGORY_METADATA.equals(getPolicySubCategory(policy))) {
                        terms.qualifiedNames.addAll(assets);
                        continue;
                    }

                    String connectionQName = getPolicyConnectionQN(policy);
                    if (StringUtils.isEmpty(connectionQName)) {
                        connectionQName = getConnectionQualifiedNameFromPolicyAssets(entityRetriever, assets);
                    }

                    for (String asset : assets) {
                        /*
                        * We are introducing a hierarchical filter for qualifiedName.
                        * This requires a migration of existing data to have a hierarchical qualifiedName.
                        * So this will only work if migration is done, upon migration completion we will set the feature flag to true
                        * This will be dictated by the feature flag ENABLE_PERSONA_HIERARCHY_FILTER
                        */

                        // If asset resource ends with /* then add it in hierarchical filter
                        boolean isHierarchical = asset.endsWith("/*");
                        if (isHierarchical) {
                            asset = asset.substring(0, asset.length() - 2);
                        }
                        boolean isWildcard = asset.contains("*") || asset.contains("?");
                        if (isWildcard) {
                            clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset)));
                        } else if (useHierarchicalQualifiedNameFilter) {
                            terms.metadataPolicyQualifiedNames.add(asset);
                        } else {
                            terms.qualifiedNames.add(asset);
                        }

                        if (!useHierarchicalQualifiedNameFilter || isWildcard) {
                            clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*")));
                        }
                    }

                    terms.qualifiedNames.add(connectionQName);
                } else if (policyActions.contains(ACCESS_READ_PERSONA_GLOSSARY)) {
                    if (CollectionUtils.isNotEmpty(assets)) {
                        terms.qualifiedNames.addAll(assets);
                        terms.glossaryQualifiedNames.addAll(assets);
                    }
                } else if (policyActions.contains(ACCESS_READ_PERSONA_DOMAIN)) {
                    for (String asset : assets) {
                        if(!isAllDomain(asset)) {
                            terms.qualifiedNames.add(asset);
                            terms.qualifiedNames.addAll(getParentDomainPaths(asset)); // Add all parent domains in the hierarchy
                        } else {
                            asset = NEW_WILDCARD_DOMAIN_SUPER;
                        }
                        clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "*")));
                    }
                } else if (policyActions.contains(ACCESS_READ_PERSONA_SUB_DOMAIN)) {
                    for (String asset : assets) {
                        List<Map<String, Object>> mustMap = new ArrayList<>();
                        mustMap.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*domain/*")));
                        mustMap.add(mapOf("term", mapOf("__typeName.keyword", "DataDomain")));
                        clauseList.add(mapOf("bool", mapOf("must", mustMap)));
                    }
                } else if (policyActions.contains(ACCESS_READ_PERSONA_PRODUCT)) {
                    for (String asset : assets) {
                        List<Map<String, Object>> mustMap = new ArrayList<>();
                        mustMap.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*product/*")));
                        mustMap.add(mapOf("term", mapOf("__typeName.keyword", "DataProduct")));
                        clauseList.add(mapOf("bool", mapOf("must", mustMap)));
                    }
                } else if (policyActions.contains(ACCESS_READ_PERSONA_AI_APP) || policyActions.contains(ACCESS_READ_PERSONA_AI_MODEL)) {
                    // access is given across the resource as per entity-type for AI asset
                    List<String> resources = getPolicyResources(policy);
                    List<String> typeResources = getFilteredPolicyResources(resources, RESOURCES_ENTITY_TYPE);
                    List<Map<String, Object>> mustMap = new ArrayList<>();
                    if (CollectionUtils.isNotEmpty(typeResources)) {
                        mustMap.add(mapOf("terms", mapOf("__typeName.keyword", typeResources)));
                        clauseList.add(mapOf("bool", mapOf("must", mustMap)));
                    }
                }

                if (terms.qualifiedNames.size() > assetsMaxLimit) {
                    throw new AtlasBaseException(AtlasErrorCode.PERSONA_POLICY_ASSETS_LIMIT_EXCEEDED, String.valueOf(assetsMaxLimit), String.valueOf(terms.qualifiedNames.size()));
                }
            }
        }

        // Add allow terms to allow clauses
        addTermsClause(allowClauseList, allowCollections.qualifiedNames, QUALIFIED_NAME);
        addTermsClause(allowClauseList, allowCollections.metadataPolicyQualifiedNames, QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY);
        addTermsClause(allowClauseList, allowCollections.glossaryQualifiedNames, GLOSSARY_PROPERTY_KEY);

        // Add deny terms to deny clauses
        addTermsClause(denyClauseList, denyCollections.qualifiedNames, QUALIFIED_NAME);
        addTermsClause(denyClauseList, denyCollections.metadataPolicyQualifiedNames, QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY);
        addTermsClause(denyClauseList, denyCollections.glossaryQualifiedNames, GLOSSARY_PROPERTY_KEY);
    }

    // Inner class to group related collections
    private static class TermCollections {
        Set<String> qualifiedNames = new HashSet<>();
        Set<String> glossaryQualifiedNames = new HashSet<>();
        Set<String> metadataPolicyQualifiedNames = new HashSet<>();
    }

    // addTermsClause set the terms to the clauseList argument itself
    private void addTermsClause(List<Map<String, Object>> clauseList, Set<String> terms, String propertyKey) {
        if (!terms.isEmpty()) {
            clauseList.add(mapOf("terms", mapOf(propertyKey, new ArrayList<>(terms))));
        }
    }

    private List<String> getParentDomainPaths(String asset) {
        List<String> domainPaths = new ArrayList<>();
        String currentPath = asset;
        while (true) {
            int lastDomainIndex = currentPath.lastIndexOf("/domain/");
            int lastProductIndex = currentPath.lastIndexOf("/product/");
            int lastIndex = Math.max(lastDomainIndex, lastProductIndex);

            if (lastIndex == -1) {
                break;
            }

            currentPath = currentPath.substring(0, lastIndex);
            if (currentPath.endsWith("default")) {
                break;
            }
            domainPaths.add(currentPath);
        }
        return domainPaths;
    }

    private boolean isAllDomain(String asset) {
        return asset.equals("*/super") || asset.equals("*") || asset.equals(NEW_WILDCARD_DOMAIN_SUPER);
    }
    private Map<String, Object> esClausesToFilter(List<Map<String, Object>> allowClauseList, List<Map<String, Object>> denyClauseList) {
        Map<String, Object> boolQuery = new HashMap<>();
        
        if (CollectionUtils.isNotEmpty(allowClauseList)) {
            boolQuery.put("should", allowClauseList);
            boolQuery.put("minimum_should_match", 1);
        }

        if (CollectionUtils.isNotEmpty(denyClauseList)) {
            boolQuery.put("must_not", denyClauseList);
        }
        
        // If we have no allow clauses and no deny clauses, return null (no filter)
        if (boolQuery.isEmpty()) {
            return null;
        }
        
        return mapOf("bool", boolQuery);
    }

    private Map<String, Object> getEmptyFilter() {
        return mapOf("match_none", new HashMap<>());
    }

    private String getAliasName(AtlasEntity entity) {
        return getESAliasName(entity);
    }

    private void addPurposeMetadataFilterClauses(List<String> tags, List<Map<String, Object>> clauseList) {
        clauseList.add(mapOf("terms", mapOf(TRAIT_NAMES_PROPERTY_KEY, tags)));
        clauseList.add(mapOf("terms", mapOf(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, tags)));
    }
}
