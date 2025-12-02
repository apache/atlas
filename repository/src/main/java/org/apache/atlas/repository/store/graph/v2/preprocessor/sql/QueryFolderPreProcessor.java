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
package org.apache.atlas.repository.store.graph.v2.preprocessor.sql;


import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.updateQueryResourceAttributes;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.COLLECTION_QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.CHILDREN_FOLDERS;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.CHILDREN_QUERIES;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.PARENT_QUALIFIED_NAME;

public class QueryFolderPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryFolderPreProcessor.class);

    private static String qualifiedNameFormat = "%s/folder/%s/%s";

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;

    public QueryFolderPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;

    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("QueryFolderPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreate(entity);
                break;
            case UPDATE:
                processUpdate(entity, vertex, context);
                break;
        }
    }

    private void processCreate(AtlasStruct entity) throws AtlasBaseException {
        String collectionQualifiedName = (String) entity.getAttribute(COLLECTION_QUALIFIED_NAME);

        if (StringUtils.isEmpty(collectionQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, entity.getTypeName(), COLLECTION_QUALIFIED_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(collectionQualifiedName));
    }

    private void processUpdate(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        String currentCollectionQualifiedName = vertex.getProperty(COLLECTION_QUALIFIED_NAME, String.class);
        String newCollectionQualifiedName     = updateQueryResourceAttributes(typeRegistry, entityRetriever, entity, vertex, context);

        if(StringUtils.isNotEmpty(newCollectionQualifiedName) && !currentCollectionQualifiedName.equals(newCollectionQualifiedName)) {
            processParentCollectionUpdation(vertex, currentCollectionQualifiedName, newCollectionQualifiedName);
            LOG.debug("Moved folder {} from collection {} to collection {}", entity.getAttribute(QUALIFIED_NAME), currentCollectionQualifiedName, newCollectionQualifiedName);
        }

    }


    private void processParentCollectionUpdation(AtlasVertex folderVertex, String currentCollectionQualifiedName, String newCollectionQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder folderProcessMetric = RequestContext.get().startMetricRecord("processParentCollectionUpdation");

        String folderQualifiedName        = folderVertex.getProperty(QUALIFIED_NAME, String.class);
        String updatedFolderQualifiedName = folderQualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);

        /**
         * 1. Move all the queries to new parent first
         * 2. Move all the child folders to new parent
         * 3. Update the qualified name of current folder
         * 4. Recursively find the child folders and move child queries to new collection
         */
        moveQueriesToDifferentCollection(folderVertex, currentCollectionQualifiedName, newCollectionQualifiedName, updatedFolderQualifiedName);

        Iterator<AtlasVertex> childrenFolders = getActiveChildren(folderVertex, CHILDREN_FOLDERS);

        while (childrenFolders.hasNext()) {
            AtlasVertex nestedFolderVertex = childrenFolders.next();
            if (nestedFolderVertex != null) {
                updateChildAttributesOnUpdatingCollection(nestedFolderVertex, currentCollectionQualifiedName, newCollectionQualifiedName, updatedFolderQualifiedName);
                /**
                 * Recursively find the child folders and move child queries to new collection
                 * folder1 -> folder2 -> query1
                 * When we will move folder1 to new collection, recursively it will find folder2
                 * Then it will move all the children of folder2 also to new collection
                 */
                processParentCollectionUpdation(nestedFolderVertex, currentCollectionQualifiedName, newCollectionQualifiedName);

                LOG.info("Moved nested folder into new collection {}", newCollectionQualifiedName);
            }
        }

        LOG.info("Moved current folder with qualified name {} into new collection {}", folderQualifiedName, newCollectionQualifiedName);

        RequestContext.get().endMetricRecord(folderProcessMetric);
    }

    /**
     * Move all child queries to new collection and update the qualified name of folder
     * @param folderVertex Parent folder vertex
     * @param currentCollectionQualifiedName  Current collection qualified name
     * @param newCollectionQualifiedName New collection qualified name
     * @param folderQualifiedName Qualified name of folder
     * @throws AtlasBaseException
     */
    private void moveQueriesToDifferentCollection(AtlasVertex folderVertex, String currentCollectionQualifiedName,
                                                          String newCollectionQualifiedName, String folderQualifiedName) throws AtlasBaseException {

        AtlasPerfMetrics.MetricRecorder queryProcessMetric = RequestContext.get().startMetricRecord("moveQueriesToDifferentCollection");
        Iterator<AtlasVertex> childrenQueriesIterator = getActiveChildren(folderVertex, CHILDREN_QUERIES);

        //Update all the children query attribute
        while (childrenQueriesIterator.hasNext()) {
            AtlasVertex queryVertex = childrenQueriesIterator.next();
            if(queryVertex != null) {
                updateChildAttributesOnUpdatingCollection(queryVertex, currentCollectionQualifiedName,
                        newCollectionQualifiedName, folderQualifiedName);
            }
        }

        RequestContext.get().endMetricRecord(queryProcessMetric);
    }

    /**
     * Get all the active children of folder
     * @param folderVertex Parent folder vertex
     * @param childrenEdgeLabel Edge label of children
     * @return Iterator of children vertices
     */
    private Iterator<AtlasVertex> getActiveChildren(AtlasVertex folderVertex, String childrenEdgeLabel) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getActiveChildren");
        try {
            return folderVertex.query()
                    .direction(AtlasEdgeDirection.OUT)
                    .label(childrenEdgeLabel)
                    .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                    .vertices()
                    .iterator();
        } catch (Exception e) {
            LOG.error("Error while getting active children of folder", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

    }

    /**
     * Update the child attributes on updating collection of parent folder
     * @param childVertex Child vertex, could be query or folder
     * @param currentCollectionQualifiedName Collection qualified name of parent folder / current collection
     * @param newCollectionQualifiedName New collection qualified name of parent folder/ new collection
     * @param folderQualifiedName Qualified name of parent folder
     */
    private void updateChildAttributesOnUpdatingCollection(AtlasVertex childVertex,  String currentCollectionQualifiedName, String newCollectionQualifiedName,
                                                          String folderQualifiedName) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateChildAttributesOnUpdatingCollection");
        Map<String, Object> updatedAttributes = new HashMap<>();
        String qualifiedName            =   childVertex.getProperty(QUALIFIED_NAME, String.class);
        String updatedQualifiedName     =   qualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);

        if (!qualifiedName.equals(updatedQualifiedName)) {
            AtlasGraphUtilsV2.setEncodedProperty(childVertex, QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);
        }

        if(!currentCollectionQualifiedName.equals(newCollectionQualifiedName)) {
            AtlasGraphUtilsV2.setEncodedProperty(childVertex, COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);
            updatedAttributes.put(COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);
        }

        AtlasGraphUtilsV2.setEncodedProperty(childVertex, PARENT_QUALIFIED_NAME, folderQualifiedName);

        //update system properties
        GraphHelper.setModifiedByAsString(childVertex, RequestContext.get().getUser());
        GraphHelper.setModifiedTime(childVertex, System.currentTimeMillis());

        updatedAttributes.put(PARENT_QUALIFIED_NAME, folderQualifiedName);

        //Record the updated child entities, it will be used to send notification and store audit logs
        recordUpdatedChildEntities(childVertex, updatedAttributes);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    /**
     * Record the updated child entities, it will be used to send notification and store audit logs
     * @param entityVertex Child entity vertex
     * @param updatedAttributes Updated attributes while updating required attributes on updating collection
     */
    private void recordUpdatedChildEntities(AtlasVertex entityVertex, Map<String, Object> updatedAttributes) {
        RequestContext requestContext = RequestContext.get();
        AtlasPerfMetrics.MetricRecorder metricRecorder = requestContext.startMetricRecord("recordUpdatedChildEntities");
        AtlasEntity entity = new AtlasEntity();
        entity = entityRetriever.mapSystemAttributes(entityVertex, entity);
        entity.setAttributes(updatedAttributes);
        requestContext.cacheDifferentialEntity(new AtlasEntity(entity), entityVertex);

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


    public static String createQualifiedName(String collectionQualifiedName) {
        return String.format(qualifiedNameFormat, collectionQualifiedName, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
