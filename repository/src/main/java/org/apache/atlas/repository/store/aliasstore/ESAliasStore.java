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

import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.ESAliasRequestBuilder.AliasAction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.ADD;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PERSONA_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PURPOSE_GLOSSARY;
import static org.apache.atlas.repository.util.AccessControlUtils.ACCESS_READ_PURPOSE_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.getConnectionQualifiedNameFromPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getESAliasName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsAllowPolicy;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicies;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;


@Component
public class ESAliasStore implements IndexAliasStore {
    private static final Logger LOG = LoggerFactory.getLogger(ESAliasStore.class);

    private final AtlasGraph graph;
    private final EntityGraphRetriever entityRetriever;

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
        requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName));

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    @Override
    public boolean updateAlias(AtlasEntity.AtlasEntityWithExtInfo accessControl, AtlasEntity policy) throws AtlasBaseException {
        String aliasName = getAliasName(accessControl.getEntity());

        Map<String, Object> filter;

        if (PERSONA_ENTITY_TYPE.equals(accessControl.getEntity().getTypeName())) {
            filter = getFilterForPersona(accessControl, policy);
        } else {
            filter = getFilterForPurpose(accessControl, policy);
        }

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);

        return true;
    }

    @Override
    public boolean deleteAlias(String aliasName) throws AtlasBaseException {
        graph.deleteESAlias(VERTEX_INDEX_NAME, aliasName);
        return true;
    }

    private Map<String, Object> getFilterForPersona(AtlasEntity.AtlasEntityWithExtInfo persona, AtlasEntity policy) throws AtlasBaseException {
        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = getPolicies(persona);
        if (policy != null) {
            policies.add(policy);
        }
        if (CollectionUtils.isNotEmpty(policies)) {
            personaPolicyToESDslClauses(policies, allowClauseList, denyClauseList);
        }

        return esClausesToFilter(allowClauseList, denyClauseList);
    }

    private Map<String, Object> getFilterForPurpose(AtlasEntity.AtlasEntityWithExtInfo purpose, AtlasEntity policy) throws AtlasBaseException {

        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = getPolicies(purpose);
        if (policy != null) {
            policies.add(policy);
        }
        List<String> tags = getPurposeTags(purpose.getEntity());

        if (CollectionUtils.isNotEmpty(policies)) {

            for (AtlasEntity entity: policies) {
                if (entity.getStatus() == null || AtlasEntity.Status.ACTIVE.equals(entity.getStatus())) {
                    if (getPolicyActions(entity).contains(ACCESS_READ_PURPOSE_METADATA)) {
                        boolean allow = getIsAllowPolicy(entity);

                        addPurposeMetadataFilterClauses(tags, allow ? allowClauseList : denyClauseList);
                    }
                }
            }
        }

        return esClausesToFilter(allowClauseList, denyClauseList);
    }

    private void personaPolicyToESDslClauses(List<AtlasEntity> policies,
                                             List<Map<String, Object>> allowClauseList,
                                             List<Map<String, Object>> denyClauseList) throws AtlasBaseException {
        for (AtlasEntity policy: policies) {

            if (policy.getStatus() == null || AtlasEntity.Status.ACTIVE.equals(policy.getStatus())) {
                List<Map<String, Object>> clauseList = getIsAllowPolicy(policy) ? allowClauseList : denyClauseList;
                List<String> assets = getPolicyAssets(policy);

                if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_METADATA)) {
                    String connectionQName = getConnectionQualifiedNameFromPolicyAssets(entityRetriever, assets);

                    for (String asset : assets) {
                        addPersonaMetadataFilterClauses(asset, clauseList);
                    }

                    addPersonaMetadataFilterConnectionClause(connectionQName, clauseList);

                } else if (getPolicyActions(policy).contains(ACCESS_READ_PURPOSE_GLOSSARY)) {
                    for (String glossaryQName : assets) {
                        addPersonaMetadataFilterClauses(glossaryQName, clauseList);
                    }
                }
            }
        }
    }

    private Map<String, Object> esClausesToFilter(List<Map<String, Object>> allowClauseList, List<Map<String, Object>> denyClauseList) {
        Map<String, Object> eSFilterBoolClause = new HashMap<>();
        if (CollectionUtils.isNotEmpty(allowClauseList)) {
            eSFilterBoolClause.put("should", allowClauseList);
        }

        if (CollectionUtils.isNotEmpty(denyClauseList)) {
            eSFilterBoolClause.put("must_not", denyClauseList);
        }

        return mapOf("bool", eSFilterBoolClause);
    }

    private String getAliasName(AtlasEntity entity) {
        return getESAliasName(entity);
    }

    private void addPersonaMetadataFilterClauses(String asset, List<Map<String, Object>> clauseList) {
        addPersonaFilterClauses(asset, clauseList);
    }

    private void addPersonaMetadataFilterConnectionClause(String connection, List<Map<String, Object>> clauseList) {
        clauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, connection)));
    }

    private void addPersonaFilterClauses(String asset, List<Map<String, Object>> clauseList) {
        clauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, asset)));
        clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*")));
        clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, "*@" + asset)));
    }

    private void addPurposeMetadataFilterClauses(List<String> tags, List<Map<String, Object>> clauseList) {
        clauseList.add(mapOf("terms", mapOf(TRAIT_NAMES_PROPERTY_KEY, tags)));
        clauseList.add(mapOf("terms", mapOf(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, tags)));
    }

}
