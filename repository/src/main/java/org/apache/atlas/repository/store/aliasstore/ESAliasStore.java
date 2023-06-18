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

        if (PERSONA_ENTITY_TYPE.equals(entity.getTypeName())) {
            requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName));
        } else {
            requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName, getFilterForPurpose(entity)));
        }

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
            filter = getFilterForPurpose(accessControl.getEntity());
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
        for (AtlasEntity policy: policies) {

            if (policy.getStatus() == null || AtlasEntity.Status.ACTIVE.equals(policy.getStatus())) {
                List<String> assets = getPolicyAssets(policy);

                if (getIsAllowPolicy(policy)) {
                    if (getPolicyActions(policy).contains(ACCESS_READ_PERSONA_METADATA)) {
                        String connectionQName = getPolicyConnectionQN(policy);

                        for (String asset : assets) {
                            addPersonaMetadataFilterClauses(asset, allowClauseList);
                        }

                        addPersonaMetadataFilterConnectionClause(connectionQName, allowClauseList);

                    } else if (getPolicyActions(policy).contains(ACCESS_READ_PURPOSE_GLOSSARY)) {
                        for (String glossaryQName : assets) {
                            addPersonaMetadataFilterClauses(glossaryQName, allowClauseList);
                        }
                    }
                }
            }
        }
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
