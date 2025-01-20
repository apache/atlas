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

package org.apache.atlas.repository.businesslineage;


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.BusinessLineageRequest;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.updateModificationMetadata;

@Service
public class BusinessLineageService implements AtlasBusinessLineageService {
    private static final Logger LOG = LoggerFactory.getLogger(BusinessLineageService.class);

    private static final boolean LINEAGE_USING_GREMLIN = AtlasConfiguration.LINEAGE_USING_GREMLIN.getBoolean();
    private static final String TYPE_GLOSSARY= "AtlasGlossary";
    private static final String TYPE_CATEGORY= "AtlasGlossaryCategory";
    private static final String TYPE_TERM = "AtlasGlossaryTerm";
    private static final String TYPE_PRODUCT = "DataProduct";
    private static final String TYPE_DOMAIN = "DataDomain";

    private final AtlasGraph graph;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasTypeRegistry atlasTypeRegistry;
    private final TransactionInterceptHelper   transactionInterceptHelper;
    private final GraphHelper graphHelper;
    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(TYPE_GLOSSARY, TYPE_CATEGORY, TYPE_TERM, TYPE_PRODUCT, TYPE_DOMAIN));



    @Inject
    BusinessLineageService(AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph, TransactionInterceptHelper transactionInterceptHelper) {
        this.graph = atlasGraph;
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
        this.entityRetriever = new EntityGraphRetriever(atlasGraph, typeRegistry);
        this.atlasTypeRegistry = typeRegistry;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.graphHelper = new GraphHelper(atlasGraph);
    }

    @Override
    public void createLineage(BusinessLineageRequest request) throws AtlasBaseException, RepositoryException {

        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("BusinessLineageService.createLineage");

        try {
            List<BusinessLineageRequest.LineageOperation> lineageOperations = request.getLineageOperations();

            if (CollectionUtils.isEmpty(lineageOperations)) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Lineage operations are empty");
            }

            for (BusinessLineageRequest.LineageOperation lineageOperation : lineageOperations) {
                String workflowId = lineageOperation.getWorkflowId();
                String assetGuid = lineageOperation.getAssetGuid();
                String productGuid = lineageOperation.getProductGuid();
                BusinessLineageRequest.OperationType operation = lineageOperation.getOperation();
                String edgeLabel = lineageOperation.getEdgeLabel();

                if (StringUtils.isEmpty(assetGuid) || StringUtils.isEmpty(productGuid) || operation == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Invalid lineage operation");
                }

                if (StringUtils.isNotEmpty(workflowId)) {
                    LOG.info("Processing lineage operation for workflowId: {}, assetGuid: {}, productGuid: {}, operation: {}, edgeLabel: {}",
                            workflowId, assetGuid, productGuid, operation, edgeLabel);
                } else {
                    LOG.info("Processing lineage operation for assetGuid: {}, productGuid: {}, operation: {}, edgeLabel: {}",
                            assetGuid, productGuid, operation, edgeLabel);
                }

                if (StringUtils.isEmpty(edgeLabel)) {
                    processProductAssetLink(assetGuid, productGuid, operation);
                } else {
                    processProductAssetInputRelation(assetGuid, productGuid, operation, edgeLabel);
                }
            }
            commitChanges();
        } catch (AtlasBaseException | RepositoryException e){
            LOG.error("Error while creating lineage", e);
            throw e;
        }finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    public void processProductAssetLink (String assetGuid, String productGuid, BusinessLineageRequest.OperationType operation) throws AtlasBaseException {
        try {
            AtlasVertex assetVertex = entityRetriever.getEntityVertex(assetGuid);
            AtlasVertex productVertex = entityRetriever.getEntityVertex(productGuid);


            if (assetVertex == null || productVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, assetGuid + " or " + productGuid);
            }

            switch (operation) {
                case ADD:
                    linkProductToAsset (assetVertex, productGuid);
                    break;
                case REMOVE:
                    unlinkProductFromAsset (assetVertex, productGuid);
                    break;
                default:
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Invalid operation type");
            }
        } catch (AtlasBaseException e){
            LOG.error("Error while processing product asset link", e);
            throw e;
        }
    }

    public void processProductAssetInputRelation(String assetGuid, String productGuid, BusinessLineageRequest.OperationType operation, String edgeLabel) throws AtlasBaseException, RepositoryException {
        try {
             AtlasVertex assetVertex = entityRetriever.getEntityVertex(assetGuid);
             AtlasVertex productVertex = entityRetriever.getEntityVertex(productGuid);

            if (assetVertex == null || productVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, assetGuid + " or " + productGuid);
            }

            switch (operation) {
                case ADD:
                    addInputRelation(assetVertex, productVertex, edgeLabel);
                    break;
                case REMOVE:
                    removeInputRelation(assetVertex, productVertex, edgeLabel);
                    break;
                default:
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Invalid operation type");
            }
        } catch (AtlasBaseException | RepositoryException e){
            LOG.error("Error while processing product asset input relation", e);
            throw e;
        }
    }

    public void linkProductToAsset (AtlasVertex assetVertex, String productGuid) throws AtlasBaseException {
        try {
            String typeName = assetVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);
            if (excludedTypes.contains(typeName)){
                LOG.warn("Type {} is not allowed to link with PRODUCT entity", typeName);
            }
            Set<String> existingValues = assetVertex.getMultiValuedSetProperty(PRODUCT_GUIDS_ATTR, String.class);

            if (!existingValues.contains(productGuid)) {
                assetVertex.setProperty(PRODUCT_GUIDS_ATTR, productGuid);
                existingValues.add(productGuid);

                updateModificationMetadata(assetVertex);

                cacheDifferentialMeshEntity(assetVertex, existingValues);

            }
        } catch (Exception e){
            LOG.error("Error while linking product to asset", e);
            throw e;
        }
    }

    public void unlinkProductFromAsset (AtlasVertex assetVertex, String productGuid) throws AtlasBaseException {
        try {
            Set<String> existingValues = assetVertex.getMultiValuedSetProperty(PRODUCT_GUIDS_ATTR, String.class);

            if (existingValues.contains(productGuid)) {
                existingValues.remove(productGuid);
                assetVertex.removePropertyValue(PRODUCT_GUIDS_ATTR, productGuid);

                updateModificationMetadata(assetVertex);

                cacheDifferentialMeshEntity(assetVertex, existingValues);
            }
        } catch (Exception e){
            LOG.error("Error while unlinking product from asset", e);
            throw e;
        }
    }

    public void addInputRelation(AtlasVertex assetVertex, AtlasVertex productVertex, String edgeLabel) throws AtlasBaseException, RepositoryException{
        try{
            if(StringUtils.equals(INPUT_PORT_PRODUCT_EDGE_LABEL, edgeLabel)) {
                AtlasEdge outputPortEdge = graphHelper.getEdge(assetVertex, productVertex, OUTPUT_PORT_PRODUCT_EDGE_LABEL);
                if(outputPortEdge == null){
                    AtlasEdge inputPortEdge = graphHelper.addEdge(assetVertex, productVertex, INPUT_PORT_PRODUCT_EDGE_LABEL);
                    LOG.info("Added input relation between asset and product");
                }
            }
        } catch (AtlasBaseException | RepositoryException e){
            LOG.error("Error while adding input relation", e);
            throw e;
        }
    }

    public void removeInputRelation(AtlasVertex assetVertex, AtlasVertex productVertex, String edgeLabel) throws AtlasBaseException, RepositoryException{
        try{
            if(StringUtils.equals(INPUT_PORT_PRODUCT_EDGE_LABEL, edgeLabel)) {

                AtlasEdge inputPortEdge = graphHelper.getEdge(assetVertex, productVertex, INPUT_PORT_PRODUCT_EDGE_LABEL);
                if(inputPortEdge != null){
                    graph.removeEdge(inputPortEdge);
                }
            }
        } catch (AtlasBaseException | RepositoryException e){
            LOG.error("Error while removing input relation", e);
            throw e;
        }
    }

    private void cacheDifferentialMeshEntity(AtlasVertex ev, Set<String> existingValues) {
        AtlasEntity diffEntity = new AtlasEntity(ev.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        diffEntity.setGuid(ev.getProperty(GUID_PROPERTY_KEY, String.class));
        diffEntity.setUpdatedBy(ev.getProperty(MODIFIED_BY_KEY, String.class));
        diffEntity.setUpdateTime(new Date(RequestContext.get().getRequestTime()));
        diffEntity.setAttribute(PRODUCT_GUIDS_ATTR, existingValues);

        RequestContext requestContext = RequestContext.get();
        requestContext.cacheDifferentialEntity(diffEntity);
    }

    public void commitChanges() throws AtlasBaseException {
        try {
            transactionInterceptHelper.intercept();
            LOG.info("Committed a entity to the graph");
        } catch (Exception e){
            LOG.error("Failed to commit asset: ", e);
            throw e;
        }
    }
}

