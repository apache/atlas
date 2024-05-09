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
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DataDomainPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataDomainPreProcessor.class);

    private EntityMutationContext context;
    private Map<String, String> updatedPolicyResources;

    public DataDomainPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                  AtlasGraph graph) {
        super(typeRegistry, entityRetriever, graph);
        this.updatedPolicyResources = new HashMap<>();
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
        String domainName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = (String) entity.getAttribute(PARENT_DOMAIN_QN_ATTR);

        AtlasEntityHeader parentDomain = getParent(entity);
        if(parentDomain != null ){
            parentDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);
        }

        domainExists(domainName, parentDomainQualifiedName);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");

        // Validate Relationship
        if(entity.hasRelationshipAttribute(SUB_DOMAIN_REL_TYPE) || entity.hasRelationshipAttribute(DATA_PRODUCT_REL_TYPE)){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Cannot update Domain's subDomains or dataProducts relations");
        }

        if(entity.hasRelationshipAttribute("stakeholders")){
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "Managing Stakeholders via Domain update");
        }

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
            if(storedDomain.getRelationshipAttribute(PARENT_DOMAIN_REL_TYPE) == null){
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Cannot move Super Domain inside another domain");
            }

            //Auth check
            isAuthorized(currentParentDomainHeader, newParentDomainHeader);

            processMoveSubDomainToAnotherDomain(entity, vertex, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, newSuperDomainQualifiedName);

        } else {
            String domainCurrentName = vertex.getProperty(NAME, String.class);
            String domainNewName = (String) entity.getAttribute(NAME);

            if (!domainCurrentName.equals(domainNewName)) {
                domainExists(domainNewName, currentParentDomainQualifiedName);
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

            LOG.info("Moving subdomain {} to Domain {}", domainName, targetDomainQualifiedName);

            domainExists(domainName, targetDomainQualifiedName);

            if(targetDomainQualifiedName.isEmpty()){
                //Moving subDomain to make it Super Domain
                targetDomainQualifiedName = "default";
                updatedQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
                domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
                domain.setAttribute(PARENT_DOMAIN_QN_ATTR, null);
                domain.setAttribute(SUPER_DOMAIN_QN_ATTR, null);
                superDomainQualifiedName = updatedQualifiedName ;
            }
            else{
                updatedQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
                domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
                domain.setAttribute(PARENT_DOMAIN_QN_ATTR, targetDomainQualifiedName);
                domain.setAttribute(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
            }

            moveChildrenToAnotherDomain(domainVertex, superDomainQualifiedName, null, sourceDomainQualifiedName, targetDomainQualifiedName);
            updatePolicies(this.updatedPolicyResources, this.context);

            LOG.info("Moved subDomain {} to Domain {}", domainName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
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
            String updatedDomainQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            // Change domain qualifiedName
            childDomainVertex.setProperty(QUALIFIED_NAME, updatedDomainQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedDomainQualifiedName);

            //change superDomainQN, parentDomainQN
            childDomainVertex.setProperty(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
            childDomainVertex.setProperty(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:"+ currentDomainQualifiedName;
            String updatedResource = "entity:"+ updatedDomainQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);

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
            String updatedQualifiedName = currentQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            productVertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);
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

    private void domainExists(String domainName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");
        try {
            exists(DATA_DOMAIN_ENTITY_TYPE, domainName, parentDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }
}


