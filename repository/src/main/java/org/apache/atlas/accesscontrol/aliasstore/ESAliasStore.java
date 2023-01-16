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
package org.apache.atlas.accesscontrol.aliasstore;

import org.apache.atlas.ESAliasRequestBuilder;
import org.apache.atlas.ESAliasRequestBuilder.AliasAction;
import org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil;
import org.apache.atlas.accesscontrol.purpose.PurposeContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.ADD;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_ENTITY_READ;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getActions;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getESAliasName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getAssets;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getConnectionId;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getIsAllow;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getQualifiedName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.mapOf;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getTags;
import static org.apache.atlas.repository.Constants.POLICY_TYPE_GLOSSARY;
import static org.apache.atlas.repository.Constants.POLICY_TYPE_METADATA;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;

;

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
    public boolean createAlias(PersonaContext personaContext) throws AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName));

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    @Override
    public boolean updateAlias(PersonaContext personaContext) throws AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        Map<String, Object> filter = getFilter(personaContext);

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);

        return true;
    }

    @Override
    public boolean createAlias(PurposeContext purposeContext) throws AtlasBaseException {
        String aliasName = getAliasName(purposeContext.getPurpose());

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(VERTEX_INDEX_NAME, aliasName));

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    @Override
    public boolean updateAlias(PurposeContext purposeContext) throws AtlasBaseException {
        String aliasName = getAliasName(purposeContext.getPurpose());

        Map<String, Object> filter = getFilter(purposeContext);

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

    private Map<String, Object> getFilter(PersonaContext context) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo personaEntityWithExtInfo = context.getPersonaExtInfo();

        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = AtlasPersonaUtil.getMetadataPolicies(personaEntityWithExtInfo);
        if (CollectionUtils.isNotEmpty(policies)) {
            personaPolicyToESDslClauses(context, policies, POLICY_TYPE_METADATA, allowClauseList, denyClauseList);
        }

        policies = AtlasPersonaUtil.getGlossaryPolicies(personaEntityWithExtInfo);
        if (CollectionUtils.isNotEmpty(policies)) {
            personaPolicyToESDslClauses(context, policies, POLICY_TYPE_GLOSSARY, allowClauseList, denyClauseList);
        }

        return esClausesToFilter(allowClauseList, denyClauseList);
    }

    private void personaPolicyToESDslClauses(PersonaContext context, List<AtlasEntity> policies, String type,
                                             List<Map<String, Object>> allowClauseList, List<Map<String, Object>> denyClauseList) throws AtlasBaseException {
        for (AtlasEntity entity: policies) {
            if (!getActions(entity).contains(ACCESS_ENTITY_READ) || isDeletedPolicy(context, entity)) {
                continue;
            }

            List<Map<String, Object>> clauseList = getIsAllow(entity) ? allowClauseList : denyClauseList;
            List<String> assets = getAssets(entity);

            if (POLICY_TYPE_METADATA.equals(type)) {
                boolean addConnectionFilter = true;
                String connectionQName = getQualifiedName(getConnectionEntity(entity));

                for (String asset : assets) {
                    if (StringUtils.equals(connectionQName, asset)) {
                        addConnectionFilter = false;
                    }

                    addPersonaMetadataFilterClauses(asset, clauseList);
                }

                if (addConnectionFilter) {
                    addPersonaMetadataFilterConnectionClause(connectionQName, clauseList);
                }
            } else if (POLICY_TYPE_GLOSSARY.equals(type)) {
                for (String glossaryQName : assets) {
                    addPersonaGlossaryFilterClauses(glossaryQName, clauseList);
                }
            }
        }
    }

    private Map<String, Object> getFilter(PurposeContext context) {
        AtlasEntity.AtlasEntityWithExtInfo purposeEntityWithExtInfo = context.getPurposeExtInfo();

        List<Map<String, Object>> allowClauseList = new ArrayList<>();
        List<Map<String, Object>> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = AtlasPurposeUtil.getMetadataPolicies(purposeEntityWithExtInfo);
        List<String> tags = getTags(context.getPurpose());

        if (CollectionUtils.isNotEmpty(policies)) {

            for (AtlasEntity entity: policies) {
                if (isDeletedPolicy(context, entity)) {
                    continue;
                }

                if (getActions(entity).contains(ACCESS_ENTITY_READ)) {
                    boolean allow = getIsAllow(entity);

                    addPurposeMetadataFilterClauses(tags, allow ? allowClauseList : denyClauseList);
                }
            }
        }

        return esClausesToFilter(allowClauseList, denyClauseList);
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

    private boolean isDeletedPolicy(PersonaContext context, AtlasEntity policy) {
        return context.isDeletePersonaPolicy() && policy.getGuid().equals(context.getPersonaPolicy().getGuid());
    }

    private boolean isDeletedPolicy(PurposeContext context, AtlasEntity policy) {
        return context.isDeletePurposePolicy() && policy.getGuid().equals(context.getPurposePolicy().getGuid());
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

    private void addPersonaGlossaryFilterClauses(String asset, List<Map<String, Object>> clauseList) {
        addPersonaFilterClauses(asset, clauseList);
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

    private AtlasEntity getConnectionEntity(AtlasEntity personaPolicy) throws AtlasBaseException {
        String connectionId = getConnectionId(personaPolicy);

        return entityRetriever.toAtlasEntity(connectionId);
    }
}
