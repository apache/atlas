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
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
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
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DomainPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DomainPreProcessor.class);
    private AtlasEntityHeader parentDomain;
    private EntityGraphMapper entityGraphMapper;
    private EntityMutationContext context;

    public DomainPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                              AtlasGraph graph, EntityGraphMapper entityGraphMapper) {
        super(typeRegistry, entityRetriever, graph);
        this.entityGraphMapper = entityGraphMapper;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("DomainPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity, context);

        switch (operation) {
            case CREATE:
                processCreateDomain(entity, vertex);
                break;
            case UPDATE:
                processUpdateDomain(entity, vertex, context);
                break;
        }
    }

    private void processCreateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateDomain");
        String domainName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = (String) entity.getAttribute(PARENT_DOMAIN_QN);

        domainExists(domainName, parentDomainQualifiedName);
        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName));

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName(String parentDomainQualifiedName) {
        if (StringUtils.isNotEmpty(parentDomainQualifiedName) && parentDomainQualifiedName !=null) {
            return parentDomainQualifiedName + "/domain/" + getUUID();
        } else{
            String prefixQN = "default/domain";
            return prefixQN + "/" + getUUID();
        }
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");
        String domainName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        AtlasEntityHeader currentParentDomainHeader = null;
        String currentParentDomainQualifiedName = "";

        AtlasEntity storedDomain = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentParentDomain = (AtlasRelatedObjectId) storedDomain.getRelationshipAttribute(PARENT_DOMAIN);

        String newParentDomainQualifiedName = "";
        String superDomainQualifiedName = "";

        if(currentParentDomain != null){
            currentParentDomainHeader = entityRetriever.toAtlasEntityHeader(currentParentDomain.getGuid());
            currentParentDomainQualifiedName = (String) currentParentDomainHeader.getAttribute(QUALIFIED_NAME);
        }

        if (parentDomain != null) {
            newParentDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);
            superDomainQualifiedName = (String) parentDomain.getAttribute(SUPER_DOMAIN_QN);
            if(superDomainQualifiedName == null){
                superDomainQualifiedName = newParentDomainQualifiedName;
            }
        }

        if (!currentParentDomainQualifiedName.equals(newParentDomainQualifiedName)  && entity.hasRelationshipAttribute(PARENT_DOMAIN)) {
            if(storedDomain.getRelationshipAttribute(PARENT_DOMAIN) == null){
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Cannot move Root Domain");
            }

            //Auth check
            if(parentDomain != null && currentParentDomainHeader != null){
                isAuthorized(currentParentDomainHeader, parentDomain);
            }

            processMoveSubDomainToAnotherDomain(entity, vertex, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, superDomainQualifiedName, context);

        } else {
            String vertexName = vertex.getProperty(NAME, String.class);
            if (!vertexName.equals(domainName)) {
                domainExists(domainName, newParentDomainQualifiedName);
            }

            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processMoveSubDomainToAnotherDomain(AtlasEntity domain,
                                                      AtlasVertex domainVertex,
                                                      String sourceDomainQualifiedName,
                                                      String targetDomainQualifiedName,
                                                      String currentSubDomainQualifiedName,
                                                      String superDomainQualifiedName,
                                                      EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveSubDomainToAnotherGlossary");

        try {
            String domainName = (String) domain.getAttribute(NAME);
            String updatedQualifiedName = "";

            LOG.info("Moving subdomain {} to Domain {}", domainName, targetDomainQualifiedName);

            domainExists(domainName, targetDomainQualifiedName);


            // Move Sub-Domain as root Domain
            if(targetDomainQualifiedName.isEmpty()){
                targetDomainQualifiedName = "default";
                updatedQualifiedName = currentSubDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
                domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
                domain.setAttribute(PARENT_DOMAIN_QN, null);
                domain.setAttribute(SUPER_DOMAIN_QN, null);
                superDomainQualifiedName = updatedQualifiedName ;
            }
            else{
                updatedQualifiedName = currentSubDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
                domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
                domain.setAttribute(PARENT_DOMAIN_QN, targetDomainQualifiedName);
                domain.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);
            }

            moveChildrenToAnotherDomain(domainVertex, superDomainQualifiedName, null, sourceDomainQualifiedName, targetDomainQualifiedName, context);

            LOG.info("Moved subDomain {} to Domain {}", domainName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void moveChildrenToAnotherDomain(AtlasVertex childDomainVertex,
                                               String superDomainQualifiedName,
                                               String parentDomainQualifiedName,
                                               String sourceDomainQualifiedName,
                                               String targetDomainQualifiedName,
                                               EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildrenToAnotherDomain");


        try {
            LOG.info("Moving child domain {} to Domain {}", childDomainVertex.getProperty(NAME, String.class), targetDomainQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentDomainQualifiedName = childDomainVertex.getProperty(QUALIFIED_NAME, String.class);
            String updatedDomainQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            // Change domain qualifiedName
            childDomainVertex.setProperty(QUALIFIED_NAME, updatedDomainQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedDomainQualifiedName);

            //change superDomainQN, parentDomainQN
            childDomainVertex.setProperty(SUPER_DOMAIN_QN, superDomainQualifiedName);
            childDomainVertex.setProperty(PARENT_DOMAIN_QN, parentDomainQualifiedName);

            //update policy
            updatePolicy(currentDomainQualifiedName, updatedDomainQualifiedName, context);

            //update system properties
            GraphHelper.setModifiedByAsString(childDomainVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(childDomainVertex, System.currentTimeMillis());

            // move products to target Domain
            Iterator<AtlasVertex> products = getActiveChildrenVertices(childDomainVertex, DATA_PRODUCT_EDGE_LABEL);

            while (products.hasNext()) {
                AtlasVertex productVertex = products.next();
                moveChildDataProductToAnotherDomain(productVertex, superDomainQualifiedName, updatedDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName, context);
            }

            // Get all children domains of current domain
            Iterator<AtlasVertex> childDomains = getActiveChildrenVertices(childDomainVertex, DOMAIN_PARENT_EDGE_LABEL);

            while (childDomains.hasNext()) {
                AtlasVertex childVertex = childDomains.next();
                moveChildrenToAnotherDomain(childVertex, superDomainQualifiedName, updatedDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName, context);
            }

            recordUpdatedChildEntities(childDomainVertex, updatedAttributes);

            LOG.info("Moved child domain {} to Domain {}", childDomainVertex.getProperty(NAME, String.class), targetDomainQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void moveChildDataProductToAnotherDomain(AtlasVertex productVertex,
                                                     String superDomainQualifiedName,
                                                     String parentDomainQualifiedName,
                                                     String sourceDomainQualifiedName,
                                                     String targetDomainQualifiedName,
                                                     EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildDataProductToAnotherDomain");

        try {
            String productName = productVertex.getProperty(NAME, String.class);
            LOG.info("Moving dataProduct {} to Domain {}", productName, targetDomainQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentQualifiedName = productVertex.getProperty(PARENT_DOMAIN_QN, String.class);
            String updatedQualifiedName = currentQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            productVertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);

            productVertex.setProperty(PARENT_DOMAIN_QN, parentDomainQualifiedName);
            productVertex.setProperty(SUPER_DOMAIN_QN, superDomainQualifiedName);

            //update policy
            updatePolicy(currentQualifiedName, updatedQualifiedName, context);

            //update system properties
            GraphHelper.setModifiedByAsString(productVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(productVertex, System.currentTimeMillis());

            recordUpdatedChildEntities(productVertex, updatedAttributes);

            LOG.info("Moved dataProduct {} to Domain {}", productName, targetDomainQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void updatePolicy(String currentQualifiedName, String updatedQualifiedName, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateDomainPolicy");
        try {
            LOG.info("Updating policy for entity {}", currentQualifiedName);
            Map<String, Object> updatedpolicyResources = new HashMap<>();

            String currentResource = "entity:"+ currentQualifiedName;
            String updatedResource = "entity:"+ updatedQualifiedName;

            updatedpolicyResources.put(currentResource, updatedResource);

            List<AtlasEntityHeader> policies = getPolicy(currentResource);
            if (CollectionUtils.isNotEmpty(policies)) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(POLICY_ENTITY_TYPE);
                for (AtlasEntityHeader policy : policies) {
                    AtlasEntity policyEntity = entityRetriever.toAtlasEntity(policy.getGuid());
                    List<String> policyResources = (List<String>) policyEntity.getAttribute(ATTR_POLICY_RESOURCES);
                    policyResources.remove(currentResource);
                    policyResources.add(updatedResource);
                    AtlasVertex policyVertex = context.getVertex(policy.getGuid());
                    policyVertex.removeProperty(ATTR_POLICY_RESOURCES);
                    policyEntity.setAttribute(ATTR_POLICY_RESOURCES, policyResources);
                    context.addUpdated(policyEntity.getGuid(), policyEntity, entityType, policyVertex);
                }
            }

        }finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

    }

    private List<AtlasEntityHeader> getPolicy(String resource) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getPolicy");
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", POLICY_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("terms", mapOf("policyResources", Arrays.asList(resource))));

            Map<String, Object> bool = new HashMap<>();
            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> policies = indexSearchPaginated(dsl, POLICY_ENTITY_TYPE);

            return policies;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void setParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DomainPreProcessor.setParent");
        if (parentDomain == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_DOMAIN);
            Set<String> attributes = new HashSet<>(Arrays.asList(QUALIFIED_NAME, SUPER_DOMAIN_QN, PARENT_DOMAIN_QN, "__typeName"));

            if (objectId != null) {
                if (StringUtils.isNotEmpty(objectId.getGuid())) {
                    AtlasVertex vertex = entityRetriever.getEntityVertex(objectId.getGuid());

                    if (vertex == null) {
                        parentDomain = entityRetriever.toAtlasEntityHeader(objectId.getGuid(), attributes);
                    } else {
                        parentDomain = entityRetriever.toAtlasEntityHeader(vertex, attributes);
                    }
                } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty((String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    AtlasVertex parentDomainVertex = entityRetriever.getEntityVertex(objectId);
                    parentDomain = entityRetriever.toAtlasEntityHeader(parentDomainVertex, attributes);
                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void domainExists(String domainName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        boolean exists = false;
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", DATA_DOMAIN_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", domainName)));


            Map<String, Object> bool = new HashMap<>();
            if (parentDomain != null) {
                mustClauseList.add(mapOf("term", mapOf("parentDomainQualifiedName", parentDomainQualifiedName)));
            } else {
                List mustNotClauseList = new ArrayList();
                mustNotClauseList.add(mapOf("exists", mapOf("field", "parentDomainQualifiedName")));
                bool.put("must_not", mustNotClauseList);
            }

            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> domains = indexSearchPaginated(dsl, DATA_DOMAIN_ENTITY_TYPE);

            if (CollectionUtils.isNotEmpty(domains)) {
                for (AtlasEntityHeader domain : domains) {
                    String name = (String) domain.getAttribute(NAME);
                    if (domainName.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, domainName+" already exists");
        }
    }
}


