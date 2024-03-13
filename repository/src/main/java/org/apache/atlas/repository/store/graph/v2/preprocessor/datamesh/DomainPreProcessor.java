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
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
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
        if (operation == EntityMutations.EntityOperation.UPDATE && LOG.isDebugEnabled()) {
            LOG.debug("DomainPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity, context);

        if (operation == EntityMutations.EntityOperation.UPDATE) {
            processUpdateDomain(entity, vertex);
        } else {
            LOG.error("DataProductPreProcessor.processAttributes: Operation not supported {}", operation);
        }
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");
        String domainName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        AtlasEntity storedDomain = entityRetriever.toAtlasEntity(vertex);
        if(storedDomain.getRelationshipAttribute(PARENT_DOMAIN) == null){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Cannot move Root Domain");
        }
        AtlasRelatedObjectId currentDomain = (AtlasRelatedObjectId) storedDomain.getRelationshipAttribute(PARENT_DOMAIN);
        AtlasEntityHeader currentDomainHeader = entityRetriever.toAtlasEntityHeader(currentDomain.getGuid());
        String currentDomainQualifiedName = (String) currentDomainHeader.getAttribute(QUALIFIED_NAME);

        String newDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);
        String superDomainQualifiedName = (String) parentDomain.getAttribute(SUPER_DOMAIN_QN);


        if (!currentDomainQualifiedName.equals(newDomainQualifiedName)) {
            //Auth check
            isAuthorized(currentDomainHeader, parentDomain);
            LOG.info("Edge Labels: {}", entity.getRelationshipAttributes());
            processMoveSubDomainToAnotherDomain(entity, vertex, currentDomainQualifiedName, newDomainQualifiedName, vertexQnName, superDomainQualifiedName);

        } else {
            String vertexName = vertex.getProperty(NAME, String.class);
            if (!vertexName.equals(domainName)) {
                domainExists(domainName, newDomainQualifiedName);
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
                                                      String superDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveSubDomainToAnotherGlossary");

        try {
            String domainName = (String) domain.getAttribute(NAME);

            LOG.info("Moving subdomain {} to Domain {}", domainName, targetDomainQualifiedName);

            domainExists(domainName, targetDomainQualifiedName);

            String updatedQualifiedName = currentSubDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            domain.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
            domain.setAttribute(PARENT_DOMAIN_QN, targetDomainQualifiedName);
            domain.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);

            moveChildrenToAnotherDomain(domainVertex, superDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);

            LOG.info("Moved subDomain {} to Domain {}", domainName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void moveChildrenToAnotherDomain(AtlasVertex childDomainVertex,
                                               String parentDomainQualifiedName,
                                               String sourceDomainQualifiedName,
                                               String targetDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildrenToAnotherDomain");


        try {
            LOG.info("Moving child domain {} to Domain {}", childDomainVertex.getProperty(NAME, String.class), targetDomainQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentDomainQualifiedName = childDomainVertex.getProperty(QUALIFIED_NAME, String.class);
            String updatedQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            // Change domain qualifiedName
            childDomainVertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);

            //change superDomainQN, parentDomainQN
            childDomainVertex.setProperty(SUPER_DOMAIN_QN, targetDomainQualifiedName);
            childDomainVertex.setProperty(PARENT_DOMAIN_QN, parentDomainQualifiedName);

            //update system properties
            GraphHelper.setModifiedByAsString(childDomainVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(childDomainVertex, System.currentTimeMillis());

            // move products to target Domain
            Iterator<AtlasVertex> products = getActiveChildrenVertices(childDomainVertex, DATA_PRODUCT_EDGE_LABEL);

            while (products.hasNext()) {
                AtlasVertex productVertex = products.next();
                moveChildDataProductToAnotherDomain(productVertex, parentDomainQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
            }

            // Get all children domains of current domain
            Iterator<AtlasVertex> childDomains = getActiveChildrenVertices(childDomainVertex, DOMAIN_PARENT_EDGE_LABEL);

            while (childDomains.hasNext()) {
                AtlasVertex childVertex = childDomains.next();
                moveChildrenToAnotherDomain(childVertex, updatedQualifiedName, sourceDomainQualifiedName, targetDomainQualifiedName);
            }

            recordUpdatedChildEntities(childDomainVertex, updatedAttributes);

            LOG.info("Moved child domain {} to Domain {}", childDomainVertex.getProperty(NAME, String.class), targetDomainQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void moveChildDataProductToAnotherDomain(AtlasVertex productVertex,
                                                     String parentDomainQualifiedName,
                                                     String sourceDomainQualifiedName,
                                                     String targetDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildDataProductToAnotherDomain");

        try {
            String productName = productVertex.getProperty(NAME, String.class);
            LOG.info("Moving dataProduct {} to Domain {}", productName, targetDomainQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentDomainQualifiedName = productVertex.getProperty(PARENT_DOMAIN_QN, String.class);
            String updatedQualifiedName = currentDomainQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            productVertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);

            productVertex.setProperty(PARENT_DOMAIN_QN, targetDomainQualifiedName);
            productVertex.setProperty(SUPER_DOMAIN_QN, parentDomainQualifiedName);

            //update system properties
            GraphHelper.setModifiedByAsString(productVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(productVertex, System.currentTimeMillis());

            recordUpdatedChildEntities(productVertex, updatedAttributes);

            LOG.info("Moved dataProduct {} to Domain {}", productName, targetDomainQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void setParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DomainPreProcessor.setParent");
        if (parentDomain == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_DOMAIN);

            if (objectId != null) {
                if (StringUtils.isNotEmpty(objectId.getGuid())) {
                    AtlasVertex vertex = context.getVertex(objectId.getGuid());

                    if (vertex == null) {
                        parentDomain = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                    } else {
                        parentDomain = entityRetriever.toAtlasEntityHeader(vertex);
                    }

                } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty((String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    parentDomain = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

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

            List<AtlasEntityHeader> domains = indexSearchPaginated(dsl);

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
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, domainName);
        }
    }
}


