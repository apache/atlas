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
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DataDomainPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataDomainPreProcessor.class);

    private EntityMutationContext context;
    private Map<String, String> updatedPolicyResources;
    private EntityGraphRetriever retrieverNoRelation = null;
    private Map<String, String> updatedDomainQualifiedNames;

    public DataDomainPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                  AtlasGraph graph, DynamicVertexService dynamicVertexService) {
        super(typeRegistry, entityRetriever, graph, dynamicVertexService);
        this.updatedPolicyResources = new HashMap<>();
        this.retrieverNoRelation = new EntityGraphRetriever(entityRetriever, true);
        this.updatedDomainQualifiedNames = new HashMap<>();
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("DataDomainPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateDomain(entity);
                break;
            case UPDATE:
                AtlasVertex vertex = context.getVertex(entity.getGuid());
                processUpdateDomain(entity, vertex);
                break;
        }
    }

    private void processCreateDomain(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateDomain");

        validateStakeholderRelationship(entity);

        String domainName = (String) entity.getAttribute(NAME);

        String parentDomainQualifiedName = "";
        AtlasObjectId parentDomainObject = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_DOMAIN_REL_TYPE);
        AtlasVertex parentDomain = null;

        if(parentDomainObject != null ){
            parentDomain = retrieverNoRelation.getEntityVertex(parentDomainObject);
            parentDomainQualifiedName = parentDomain.getProperty(QUALIFIED_NAME, String.class);
            if(StringUtils.isNotEmpty(parentDomainQualifiedName)) {
                entity.setAttribute(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);
                String superDomainQualifiedName = parentDomain.getProperty(SUPER_DOMAIN_QN_ATTR, String.class);
                if(StringUtils.isEmpty(parentDomain.getProperty(SUPER_DOMAIN_QN_ATTR, String.class))) {
                    superDomainQualifiedName = parentDomainQualifiedName;
                }
                entity.setAttribute(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
            }
        } else {
            entity.removeAttribute(PARENT_DOMAIN_QN_ATTR);
            entity.removeAttribute(SUPER_DOMAIN_QN_ATTR);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName));


        entity.setCustomAttributes(customAttributes);

        domainExists(domainName, parentDomainQualifiedName, null);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");

        // Validate Relationship
        if(entity.hasRelationshipAttribute(SUB_DOMAIN_REL_TYPE) || entity.hasRelationshipAttribute(DATA_PRODUCT_REL_TYPE)){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Cannot update Domain's subDomains or dataProducts relations");
        }

        validateStakeholderRelationship(entity);

        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        AtlasEntity storedDomain = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentParentDomainObjectId = (AtlasRelatedObjectId) storedDomain.getRelationshipAttribute(PARENT_DOMAIN_REL_TYPE);

        String newSuperDomainQualifiedName = "";
        String newParentDomainQualifiedName = "";
        String currentParentDomainQualifiedName = "";

        AtlasEntityHeader currentParentDomainHeader = null;

        if(currentParentDomainObjectId != null){
            currentParentDomainHeader = entityRetriever.toAtlasEntityHeader(currentParentDomainObjectId.getGuid());
            currentParentDomainQualifiedName = (String) currentParentDomainHeader.getAttribute(QUALIFIED_NAME);
        }

        AtlasEntityHeader newParentDomainHeader = getParent(entity);
        if (newParentDomainHeader != null) {
            newParentDomainQualifiedName = (String) newParentDomainHeader.getAttribute(QUALIFIED_NAME);

            newSuperDomainQualifiedName = (String) newParentDomainHeader.getAttribute(SUPER_DOMAIN_QN_ATTR);
            if(StringUtils.isEmpty(newSuperDomainQualifiedName)) {
                newSuperDomainQualifiedName = newParentDomainQualifiedName;
            }
        }

        if (!newParentDomainQualifiedName.equals(currentParentDomainQualifiedName) && entity.hasRelationshipAttribute(PARENT_DOMAIN_REL_TYPE)) {
            //Auth check
            isAuthorizedToMove(DATA_DOMAIN_ENTITY_TYPE, currentParentDomainHeader, newParentDomainHeader);

            processMoveSubDomainToAnotherDomain(entity, vertex, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, newSuperDomainQualifiedName);

        } else {
            String domainCurrentName = vertex.getProperty(NAME, String.class);
            String domainNewName = (String) entity.getAttribute(NAME);

            entity.removeAttribute(PARENT_DOMAIN_QN_ATTR);
            entity.removeAttribute(SUPER_DOMAIN_QN_ATTR);

            if (!domainCurrentName.equals(domainNewName)) {
                domainExists(domainNewName, currentParentDomainQualifiedName, storedDomain.getGuid());
            }
            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processMoveSubDomainToAnotherDomain(AtlasEntity domain,
                                                      AtlasVertex domainVertex,
                                                      String sourceDomainQualifiedName,
                                                      String targetDomainQualifiedName,
                                                      String currentDomainQualifiedName,
                                                      String superDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveSubDomainToAnotherDomain");

        try {
            String domainName = (String) domain.getAttribute(NAME);
            String updatedQualifiedName = "";
            LinkedHashMap<String, Object> updatedAttributes = new LinkedHashMap<>();

            LOG.info("Moving subdomain {} to Domain {}", domainName, targetDomainQualifiedName);

            domainExists(domainName, targetDomainQualifiedName, domain.getGuid());

            isParentDomainMovedToChild(targetDomainQualifiedName, currentDomainQualifiedName);

            if(targetDomainQualifiedName.isEmpty()){
                //Moving subDomain to make it Super Domain
                targetDomainQualifiedName = "default";
                updatedQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
                updatedQualifiedName = updatedQualifiedName + "/super";
                domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
                domain.setAttribute(PARENT_DOMAIN_QN_ATTR, null);
                domain.setAttribute(SUPER_DOMAIN_QN_ATTR, null);
                superDomainQualifiedName = updatedQualifiedName ;

                updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);
                updatedAttributes.put(PARENT_DOMAIN_QN_ATTR, null);
                updatedAttributes.put(SUPER_DOMAIN_QN_ATTR, null);
            }
            else{
                if(StringUtils.isEmpty(sourceDomainQualifiedName)){
                    updatedQualifiedName = createQualifiedName(targetDomainQualifiedName);
                }else {
                    updatedQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
                }

                domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
                domain.setAttribute(PARENT_DOMAIN_QN_ATTR, targetDomainQualifiedName);
                domain.setAttribute(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);

                updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);
                updatedAttributes.put(PARENT_DOMAIN_QN_ATTR, targetDomainQualifiedName);
                updatedAttributes.put(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
            }

            Iterator<AtlasEdge> existingParentEdges = domainVertex.getEdges(AtlasEdgeDirection.IN, DOMAIN_PARENT_EDGE_LABEL).iterator();
            if (existingParentEdges.hasNext()) {
                graph.removeEdge(existingParentEdges.next());
            }

            String currentQualifiedName = domainVertex.getProperty(QUALIFIED_NAME, String.class);
            this.updatedPolicyResources.put("entity:" + currentQualifiedName, "entity:" + updatedQualifiedName);
            this.updatedDomainQualifiedNames.put(currentQualifiedName, updatedQualifiedName);

            for (Map.Entry<String, Object> entry : updatedAttributes.entrySet()) {
                RequestContext.get().getDifferentialEntitiesMap().get(domain.getGuid()).setAttribute(entry.getKey(), entry.getValue());
            }

            moveChildren(domainVertex, superDomainQualifiedName, updatedQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
            updatePolicies(this.updatedPolicyResources, this.context);
            updateStakeholderTitlesAndStakeholders(this.updatedDomainQualifiedNames, this.context);

            LOG.info("Moved subDomain {} to Domain {}", domainName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void moveChildren(AtlasVertex domainVertex,
                              String superDomainQualifiedName,
                              String parentDomainQualifiedName,
                              String sourceDomainQualifiedName,
                              String targetDomainQualifiedName) throws AtlasBaseException {
        // move products to target Domain
        Iterator<AtlasVertex> products = getActiveChildrenVertices(domainVertex, DATA_PRODUCT_EDGE_LABEL);
        while (products.hasNext()) {
            AtlasVertex productVertex = products.next();
            moveChildDataProductToAnotherDomain(productVertex, superDomainQualifiedName, parentDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
        }
        // Get all children domains of current domain
        Iterator<AtlasVertex> childDomains = getActiveChildrenVertices(domainVertex, DOMAIN_PARENT_EDGE_LABEL);
        while (childDomains.hasNext()) {
            AtlasVertex childVertex = childDomains.next();
            moveChildrenToAnotherDomain(childVertex, superDomainQualifiedName, parentDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
        }
    }

    private void moveChildrenToAnotherDomain(AtlasVertex childDomainVertex,
                                               String superDomainQualifiedName,
                                               String parentDomainQualifiedName,
                                               String sourceDomainQualifiedName,
                                               String targetDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildrenToAnotherDomain");


        try {
            LOG.info("Moving child domain {} to Domain {}", childDomainVertex.getProperty(NAME, String.class), targetDomainQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentDomainQualifiedName = childDomainVertex.getProperty(QUALIFIED_NAME, String.class);
            String updatedDomainQualifiedName = parentDomainQualifiedName + getOwnQualifiedNameForChild(currentDomainQualifiedName);

            // Change domain qualifiedName
            childDomainVertex.setProperty(QUALIFIED_NAME, updatedDomainQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedDomainQualifiedName);

            // Change unique qualifiedName attribute
            childDomainVertex.setProperty(UNIQUE_QUALIFIED_NAME, updatedDomainQualifiedName);

            //change superDomainQN, parentDomainQN
            childDomainVertex.setProperty(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
            childDomainVertex.setProperty(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:"+ currentDomainQualifiedName;
            String updatedResource = "entity:"+ updatedDomainQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);
            this.updatedDomainQualifiedNames.put(currentDomainQualifiedName, updatedDomainQualifiedName);

            //update system properties
            GraphHelper.setModifiedByAsString(childDomainVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(childDomainVertex, System.currentTimeMillis());

            // move products to target Domain
            Iterator<AtlasVertex> products = getActiveChildrenVertices(childDomainVertex, DATA_PRODUCT_EDGE_LABEL);

            while (products.hasNext()) {
                AtlasVertex productVertex = products.next();
                moveChildDataProductToAnotherDomain(productVertex, superDomainQualifiedName, updatedDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
            }

            // Get all children domains of current domain
            Iterator<AtlasVertex> childDomains = getActiveChildrenVertices(childDomainVertex, DOMAIN_PARENT_EDGE_LABEL);

            while (childDomains.hasNext()) {
                AtlasVertex childVertex = childDomains.next();
                moveChildrenToAnotherDomain(childVertex, superDomainQualifiedName, updatedDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
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
                                                     String targetDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildDataProductToAnotherDomain");

        try {
            String productName = productVertex.getProperty(NAME, String.class);
            LOG.info("Moving dataProduct {} to Domain {}", productName, targetDomainQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentQualifiedName = productVertex.getProperty(QUALIFIED_NAME, String.class);
            String updatedQualifiedName = parentDomainQualifiedName + getOwnQualifiedNameForChild(currentQualifiedName);

            productVertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);
            productVertex.setProperty(UNIQUE_QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);

            productVertex.setProperty(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);
            productVertex.setProperty(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:"+ currentQualifiedName;
            String updatedResource = "entity:"+ updatedQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);

            //update system properties
            GraphHelper.setModifiedByAsString(productVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(productVertex, System.currentTimeMillis());

            recordUpdatedChildEntities(productVertex, updatedAttributes);

            LOG.info("Moved dataProduct {} to Domain {}", productName, targetDomainQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private AtlasEntityHeader getParent(AtlasEntity domainEntity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DataDomainPreProcessor.getParent");

        AtlasObjectId objectId = (AtlasObjectId) domainEntity.getRelationshipAttribute(PARENT_DOMAIN_REL_TYPE);

        RequestContext.get().endMetricRecord(metricRecorder);
        return getParent(objectId, PARENT_ATTRIBUTES);
    }

    private void domainExists(String domainName, String parentDomainQualifiedName,String guid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");
        try {
            exists(DATA_DOMAIN_ENTITY_TYPE, domainName, parentDomainQualifiedName, guid);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static String createQualifiedName(String parentDomainQualifiedName) {
        if (StringUtils.isNotEmpty(parentDomainQualifiedName)) {
            return parentDomainQualifiedName + "/domain/" + getUUID();
        } else{
            return "default/domain/" + getUUID() + "/super";
        }
    }

    private String getOwnQualifiedNameForChild(String childQualifiedName) {
        String[] splitted = childQualifiedName.split("/");
        return String.format("/%s/%s", splitted[splitted.length -2], splitted[splitted.length -1]);
    }

    private void validateStakeholderRelationship(AtlasEntity entity) throws AtlasBaseException {
        if(entity.hasRelationshipAttribute(STAKEHOLDER_REL_TYPE)){
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "Managing Stakeholders while creating/updating a domain");
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        String domainGuid = GraphHelper.getGuid(vertex);

        if(LOG.isDebugEnabled()) {
            LOG.debug("DataDomainPreProcessor.processDelete: pre processing {}", domainGuid);
        }

        if (hasLinkedAssets(domainGuid, DOMAIN_GUIDS)) {
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "Domain cannot be deleted because some assets are linked to this domain");
        }

        if (hasChildObjects(vertex)) {
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "Domain cannot be deleted because it has active child domains or products");
        }
    }

    private void isParentDomainMovedToChild(String targetDomainQualifiedName, String currentDomainQualifiedName) throws AtlasBaseException {
        if(targetDomainQualifiedName.startsWith(currentDomainQualifiedName)){
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "Cannot move a domain to its child domain");
        }
    }
}


