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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.ESAliasRequestBuilder.AliasAction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
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
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_DOMAIN;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_GLOSSARY;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_PRODUCT;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_SUB_DOMAIN;
import static org.apache.atlas.repository.util.AccessControlUtils.getConnectionQualifiedNameFromPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getESAliasName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsAllowPolicy;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicies;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyConnectionQN;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;


@Component
public class ESAliasStore implements IndexAliasStore {
    private static final Logger LOG = LoggerFactory.getLogger(ESAliasStore.class);
    public static final String NEW_WILDCARD_DOMAIN_SUPER = "default/domain/*/super";

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

        if (policy == null && persona == null){
            return getEmptyFilter();
        }

        List<AtlasEntity> policies = getPolicies(persona);
        if (policy != null) {
            policies.add(policy);
        }
        if (CollectionUtils.isNotEmpty(policies)) {
            personaPolicyToESDslClauses(policies, allowClauseList);
        }

        return esClausesToFilter(allowClauseList);
    }

    private Map<String, Object> getFilterForPurpose(AtlasEntity purpose) throws AtlasBaseException {

        List<Map<String, Object>> allowClauseList = new ArrayList<>();

        List<String> tags = getPurposeTags(purpose);
        addPurposeMetadataFilterClauses(tags, allowClauseList);

        return esClausesToFilter(allowClauseList);
    }

    private void personaPolicyToESDslClauses(List<AtlasEntity> policies,
                                             List<Map<String, Object>> allowClauseList) throws AtlasBaseException {
        List<String> terms = new ArrayList<>();
        
        for (AtlasEntity policy: policies) {

            if (policy.getStatus() == null || AtlasEntity.Status.ACTIVE.equals(policy.getStatus())) {
                List<String> assets = getPolicyAssets(policy);

                if (!getIsAllowPolicy(policy)) {
                    continue;
                }
                
                if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_METADATA)) {

                    String connectionQName = getPolicyConnectionQN(policy);
                    if (StringUtils.isEmpty(connectionQName)) {
                        connectionQName = getConnectionQualifiedNameFromPolicyAssets(entityRetriever, assets);
                    }

                    for (String asset : assets) {
                        if (asset.contains("*") || asset.contains("?")) {
                            //DG-898 Bug fix
                            allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset)));
                        } else {
                            terms.add(asset);
                        }
                        allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*")));
                    }

                    terms.add(connectionQName);

                } else if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_GLOSSARY)) {

                    for (String glossaryQName : assets) {
                        terms.add(glossaryQName);
                        allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, "*@" + glossaryQName)));
                    }
                } else if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_DOMAIN)) {

                    for (String asset : assets) {
                        asset = validateAndConvertAsset(asset);
                        if(!asset.equals(NEW_WILDCARD_DOMAIN_SUPER)) {
                            terms.add(asset);
                        }
                        allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "*")));
                    }

                } else if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_SUB_DOMAIN)) {
                    for (String asset : assets) {
                        //terms.add(asset);
                        List<Map<String, Object>> mustMap = new ArrayList<>();
                        mustMap.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*domain/*")));
                        mustMap.add(mapOf("term", mapOf("__typeName.keyword", "DataDomain")));
                        allowClauseList.add(mapOf("bool", mapOf("must", mustMap)));
                    }

                } else if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_PRODUCT)) {
                    for (String asset : assets) {
                        //terms.add(asset);
                        List<Map<String, Object>> mustMap = new ArrayList<>();
                        mustMap.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*product/*")));
                        mustMap.add(mapOf("term", mapOf("__typeName.keyword", "DataProduct")));
                        allowClauseList.add(mapOf("bool", mapOf("must", mustMap)));
                    }
                }
            }

            if (terms.size() > assetsMaxLimit) {
                throw new AtlasBaseException(AtlasErrorCode.PERSONA_POLICY_ASSETS_LIMIT_EXCEEDED, String.valueOf(assetsMaxLimit), String.valueOf(terms.size()));
            }
        }

        allowClauseList.add(mapOf("terms", mapOf(QUALIFIED_NAME, terms)));
    }

    private String validateAndConvertAsset(String asset) {
        if(asset.equals("*/super") || asset.equals("*"))
            asset = NEW_WILDCARD_DOMAIN_SUPER;
        return asset;
    }
    private Map<String, Object> esClausesToFilter(List<Map<String, Object>> allowClauseList) {
        if (CollectionUtils.isNotEmpty(allowClauseList)) {
            return mapOf("bool", mapOf("should", allowClauseList));
        }
        return null;
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
