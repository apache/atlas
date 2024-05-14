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
package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.AuthPolicyPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol.StakeholderPreProcessor.ATTR_DOMAIN_QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh.StakeholderTitlePreProcessor.ATTR_DOMAIN_QUALIFIED_NAMES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getListAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public abstract class AbstractDomainPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDomainPreProcessor.class);


    protected final AtlasTypeRegistry typeRegistry;
    protected final EntityGraphRetriever entityRetriever;
    protected EntityGraphRetriever entityRetrieverNoRelations;
    private final PreProcessor preProcessor;
    protected EntityDiscoveryService discovery;

    private static final Set<String> POLICY_ATTRIBUTES_FOR_SEARCH = new HashSet<>(Arrays.asList(ATTR_POLICY_RESOURCES));
    private static final Set<String> STAKEHOLDER_ATTRIBUTES_FOR_SEARCH = new HashSet<>(Arrays.asList(ATTR_DOMAIN_QUALIFIED_NAMES, ATTR_DOMAIN_QUALIFIED_NAME));

    static final Set<String> PARENT_ATTRIBUTES            = new HashSet<>(Arrays.asList(SUPER_DOMAIN_QN_ATTR, PARENT_DOMAIN_QN_ATTR));

    static final Map<String, String> customAttributes = new HashMap<>();

    static {
        customAttributes.put(MIGRATION_CUSTOM_ATTRIBUTE, "true");
    }

    AbstractDomainPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.preProcessor = new AuthPolicyPreProcessor(graph, typeRegistry, entityRetriever);

        try {
            this.entityRetrieverNoRelations = new EntityGraphRetriever(graph, typeRegistry, true);
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    protected void isAuthorized(AtlasEntityHeader sourceDomain, AtlasEntityHeader targetDomain) throws AtlasBaseException {

       if(sourceDomain != null){
           // source -> CREATE + UPDATE + DELETE
           AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, sourceDomain),
                   "create on source Domain: ", sourceDomain.getAttribute(NAME));

           AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceDomain),
                   "update on source Domain: ", sourceDomain.getAttribute(NAME));
       }

       if(targetDomain != null){
           // target -> CREATE + UPDATE + DELETE
           AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, targetDomain),
                   "create on target Domain: ", targetDomain.getAttribute(NAME));

           AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, targetDomain),
                   "update on target Domain: ", targetDomain.getAttribute(NAME));
       }
    }

    protected void updatePolicies(Map<String, String> updatedPolicyResources, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePolicies");
        try {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(POLICY_ENTITY_TYPE);

            if (MapUtils.isEmpty(updatedPolicyResources)) {
                return;
            }

            List<AtlasEntityHeader> policies = getPolicies(updatedPolicyResources.keySet());
            LOG.info("Found {} policies to update", policies.size());

            if (CollectionUtils.isNotEmpty(policies)) {
                for (AtlasEntityHeader policy : policies) {
                    LOG.info("Updating Policy {}", policy.getGuid());
                    AtlasVertex policyVertex = entityRetriever.getEntityVertex(policy.getGuid());

                    AtlasEntity policyEntity = entityRetriever.toAtlasEntity(policyVertex);

                    if (policyEntity.hasRelationshipAttribute(REL_ATTR_ACCESS_CONTROL) && policyEntity.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL) != null) {
                        AtlasObjectId accessControlObjId = (AtlasObjectId) policyEntity.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
                        AtlasVertex accessControl = entityRetriever.getEntityVertex(accessControlObjId.getGuid());
                        context.getDiscoveryContext().addResolvedGuid(GraphHelper.getGuid(accessControl), accessControl);
                    }

                    List<String> policyResources = (List<String>) policyEntity.getAttribute(ATTR_POLICY_RESOURCES);

                    List<String> updatedPolicyResourcesList = new ArrayList<>();

                    for (String resource : policyResources) {
                        if (updatedPolicyResources.containsKey(resource)) {
                            updatedPolicyResourcesList.add(updatedPolicyResources.get(resource));
                        } else {
                            updatedPolicyResourcesList.add(resource);
                        }
                    }
                    Map<String, Object> updatedAttributes = new HashMap<>();
                    updatedAttributes.put(ATTR_POLICY_RESOURCES, updatedPolicyResourcesList);

                    //policyVertex.removeProperty(ATTR_POLICY_RESOURCES);
                    policyEntity.setAttribute(ATTR_POLICY_RESOURCES, updatedPolicyResourcesList);

                    context.addUpdated(policyEntity.getGuid(), policyEntity, entityType, policyVertex);
                    recordUpdatedChildEntities(policyVertex, updatedAttributes);
                    this.preProcessor.processAttributes(policyEntity, context, EntityMutations.EntityOperation.UPDATE);
                }
            }

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    protected void updateStakeholderTitlesAndStakeholders(Map<String, String> updatedDomainQualifiedNames, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateStakeholderTitlesAndStakeholders");
        try {

            if (MapUtils.isEmpty(updatedDomainQualifiedNames)) {
                return;
            }

            List<AtlasEntityHeader> assets = getStakeholderTitlesAndStakeholders(updatedDomainQualifiedNames.keySet());

            if (CollectionUtils.isNotEmpty(assets)) {
                for (AtlasEntityHeader asset : assets) {
                    AtlasVertex vertex = entityRetrieverNoRelations.getEntityVertex(asset.getGuid());
                    AtlasEntity entity = entityRetrieverNoRelations.toAtlasEntity(vertex);
                    Map<String, Object> updatedAttributes = new HashMap<>();
                    AtlasEntityType entityType = null;

                    if (entity.getTypeName().equals(STAKEHOLDER_ENTITY_TYPE)) {
                        entityType = typeRegistry.getEntityTypeByName(STAKEHOLDER_ENTITY_TYPE);

                        String currentDomainQualifiedName = (String) asset.getAttribute(ATTR_DOMAIN_QUALIFIED_NAME);

                        entity.setAttribute(ATTR_DOMAIN_QUALIFIED_NAME, updatedDomainQualifiedNames.get(currentDomainQualifiedName));
                        updatedAttributes.put(ATTR_DOMAIN_QUALIFIED_NAME, updatedDomainQualifiedNames.get(currentDomainQualifiedName));

                    } else if (entity.getTypeName().equals(STAKEHOLDER_TITLE_ENTITY_TYPE)) {
                        entityType = typeRegistry.getEntityTypeByName(STAKEHOLDER_TITLE_ENTITY_TYPE);

                        List<String> currentDomainQualifiedNames = getListAttribute(asset, ATTR_DOMAIN_QUALIFIED_NAMES);

                        List<String> newDomainQualifiedNames = new ArrayList<>();

                        for (String qualifiedName : currentDomainQualifiedNames) {
                            if (updatedDomainQualifiedNames.containsKey(qualifiedName)) {
                                newDomainQualifiedNames.add(updatedDomainQualifiedNames.get(qualifiedName));
                            } else {
                                newDomainQualifiedNames.add(qualifiedName);
                            }
                        }

                        entity.setAttribute(ATTR_DOMAIN_QUALIFIED_NAMES, newDomainQualifiedNames);
                        updatedAttributes.put(ATTR_DOMAIN_QUALIFIED_NAMES, newDomainQualifiedNames);
                    }

                    context.addUpdated(entity.getGuid(), entity, entityType, vertex);
                    recordUpdatedChildEntities(vertex, updatedAttributes);
                }
            }

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    protected void exists(String assetType, String assetName, String parentDomainQualifiedName, String guid) throws AtlasBaseException {
        boolean exists = false;

        List<Map<String, Object>> mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", assetType)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustClauseList.add(mapOf("term", mapOf("name.keyword", assetName)));
        List<Map<String, Object>> mustNotClauseList = new ArrayList();
        if(Objects.nonNull(guid) && StringUtils.isNotEmpty(guid)){
            mustNotClauseList.add(mapOf("term", mapOf("__guid", guid)));
        }

        Map<String, Object> bool = new HashMap<>();
        if (StringUtils.isNotEmpty(parentDomainQualifiedName)) {
            mustClauseList.add(mapOf("term", mapOf("parentDomainQualifiedName", parentDomainQualifiedName)));
        } else {
            mustNotClauseList.add(mapOf("exists", mapOf("field", "parentDomainQualifiedName")));
        }

        bool.put("must", mustClauseList);
        if(!mustNotClauseList.isEmpty()) {
            bool.put("must_not", mustNotClauseList);
        }
        Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

        List<AtlasEntityHeader> assets = indexSearchPaginated(dsl, null, this.discovery);

        if (CollectionUtils.isNotEmpty(assets)) {
            for (AtlasEntityHeader asset : assets) {
                String name = (String) asset.getAttribute(NAME);
                if (assetName.equals(name)) {
                    exists = true;
                    break;
                }
            }
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    String.format("%s with name %s already exists in the domain", assetType, assetName));
        }
    }

    protected List<AtlasEntityHeader> getPolicies(Set<String> resources) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getPolicies");
        try {
            List<Map<String, Object>> mustClauseList = new ArrayList<>();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", POLICY_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("terms", mapOf("policyResources", resources)));

            Map<String, Object> bool = new HashMap<>();
            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            return indexSearchPaginated(dsl, POLICY_ATTRIBUTES_FOR_SEARCH, discovery);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    protected List<AtlasEntityHeader> getStakeholderTitlesAndStakeholders(Set<String> qualifiedNames) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getStakeholderTitlesAndStakeholders");
        try {
            List<Map<String, Object>> mustClauseList = new ArrayList<>();
            mustClauseList.add(mapOf("terms", mapOf("__typeName.keyword", Arrays.asList(STAKEHOLDER_ENTITY_TYPE, STAKEHOLDER_TITLE_ENTITY_TYPE))));

            List<Map<String, Object>> shouldClauseList = new ArrayList<>();
            shouldClauseList.add(mapOf("terms", mapOf("stakeholderTitleDomainQualifiedNames", qualifiedNames)));
            shouldClauseList.add(mapOf("terms", mapOf("stakeholderDomainQualifiedName", qualifiedNames)));

            mustClauseList.add(mapOf("bool", mapOf("should", shouldClauseList)));

            Map<String, Object> bool = new HashMap<>();
            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            return indexSearchPaginated(dsl, STAKEHOLDER_ATTRIBUTES_FOR_SEARCH, discovery);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    /**
     * Record the updated child entities, it will be used to send notification and store audit logs
     * @param entityVertex Child entity vertex
     * @param updatedAttributes Updated attributes while updating required attributes on updating collection
     */
    protected void recordUpdatedChildEntities(AtlasVertex entityVertex, Map<String, Object> updatedAttributes) {
        RequestContext requestContext = RequestContext.get();
        AtlasPerfMetrics.MetricRecorder metricRecorder = requestContext.startMetricRecord("recordUpdatedChildEntities");
        AtlasEntity entity = new AtlasEntity();
        entity = entityRetriever.mapSystemAttributes(entityVertex, entity);
        entity.setAttributes(updatedAttributes);
        requestContext.cacheDifferentialEntity(new AtlasEntity(entity));

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        //Add the min info attributes to entity header to be sent as part of notification
        if(entityType != null) {
            AtlasEntity finalEntity = entity;
            entityType.getMinInfoAttributes().values().stream().filter(attribute -> !updatedAttributes.containsKey(attribute.getName())).forEach(attribute -> {
                Object attrValue = null;
                try {
                    attrValue = entityRetriever.getVertexAttribute(entityVertex, attribute);
                } catch (AtlasBaseException e) {
                    LOG.error("Error while getting vertex attribute", e);
                }
                if(attrValue != null) {
                    finalEntity.setAttribute(attribute.getName(), attrValue);
                }
            });
            requestContext.recordEntityUpdate(new AtlasEntityHeader(finalEntity));
        }

        requestContext.endMetricRecord(metricRecorder);
    }

    protected AtlasEntityHeader getParent(Object parentObject, Set<String> attributes) throws AtlasBaseException {
        if (parentObject == null) {
            return null;
        }

        AtlasObjectId objectId;
        if (parentObject instanceof Map) {
            objectId = getAtlasObjectIdFromMapObject(parentObject);
        } else {
            objectId = (AtlasObjectId) parentObject;
        }

        AtlasVertex parentVertex = entityRetriever.getEntityVertex(objectId);
        return entityRetriever.toAtlasEntityHeader(parentVertex, attributes);
    }

    public static AtlasObjectId getAtlasObjectIdFromMapObject(Object obj) {
        Map<String, Object> parentMap = (Map<String, Object>) obj;
        AtlasObjectId objectId = new AtlasObjectId();
        objectId.setTypeName((String) parentMap.get("typeName"));

        if (parentMap.containsKey("guid")) {
            objectId.setGuid((String) parentMap.get("guid"));
        } else {
            objectId.setUniqueAttributes((Map<String, Object>) parentMap.get("uniqueAttributes"));
        }

        return objectId;
    }
}
