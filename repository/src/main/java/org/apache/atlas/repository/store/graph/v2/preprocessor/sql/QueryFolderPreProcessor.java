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
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.COLLECTION_QUALIFIED_NAME;

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
        AtlasEntityType entityType      = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasObjectId newParentAttr     = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME);
        String relationshipType         = AtlasEntityUtil.getRelationshipType(newParentAttr);
        AtlasAttribute parentAttribute  = entityType.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME, relationshipType);
        AtlasObjectId currentParentAttr = (AtlasObjectId) entityRetriever.getEntityAttribute(vertex, parentAttribute);

        if (parentAttribute.getAttributeType().areEqualValues(currentParentAttr, newParentAttr, context.getGuidAssignments())) {
            return;
        }

        AtlasVertex currentParentVertex = entityRetriever.getEntityVertex(currentParentAttr);
        AtlasVertex newParentVertex = entityRetriever.getEntityVertex(newParentAttr);

        if (currentParentVertex == null || newParentVertex == null) {
            LOG.warn("Current or new parent vertex is null");
            return;
        }

        String currentCollectionQualifiedName = currentParentVertex.getProperty(getCollectionPropertyName(currentParentVertex), String.class);
        String newCollectionQualifiedName     = newParentVertex.getProperty(getCollectionPropertyName(newParentVertex), String.class);

        if (StringUtils.isEmpty(newCollectionQualifiedName) || StringUtils.isEmpty(currentCollectionQualifiedName)) {
            LOG.warn("Collection qualified name in parent or current entity is empty or null");
            return;
        }

        processParentCollectionUpdation(vertex, currentCollectionQualifiedName, newCollectionQualifiedName);
    }

    private void processParentCollectionUpdation(AtlasVertex folderVertex, String currentCollectionQualifiedName, String newCollectionQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder folderProcessMetric = RequestContext.get().startMetricRecord("processParentCollectionUpdation");

        String folderQualifiedName        = folderVertex.getProperty(QUALIFIED_NAME, String.class);
        String updatedFolderQualifiedName = folderQualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);

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
                processParentCollectionUpdation(nestedFolderVertex, currentCollectionQualifiedName, newCollectionQualifiedName) ;
            }
        }

        LOG.info("Moved current folder with qualified name {} into new collection {}", folderQualifiedName, newCollectionQualifiedName);

        RequestContext.get().endMetricRecord(folderProcessMetric);
    }


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

    private Iterator<AtlasVertex> getActiveChildren(AtlasVertex folderVertex, String childrenEdgeLabel) {
        return folderVertex.query()
                .direction(AtlasEdgeDirection.OUT)
                .label(childrenEdgeLabel)
                .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                .vertices()
                .iterator();
    }

    private void updateChildAttributesOnUpdatingCollection(AtlasVertex childVertex,  String currentCollectionQualifiedName, String newCollectionQualifiedName,
                                                          String folderQualifiedName) throws  AtlasBaseException{
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

        recordUpdatedChildEntities(childVertex, updatedAttributes);
    }

    private void recordUpdatedChildEntities(AtlasVertex entityVertex, Map<String, Object> updatedAttributes) throws AtlasBaseException {
        RequestContext requestContext = RequestContext.get();
        AtlasEntity entity = new AtlasEntity();
        entity = entityRetriever.mapSystemAttributes(entityVertex, entity);
        entity.setAttributes(updatedAttributes);
        requestContext.cacheDifferentialEntity(entity);

        requestContext.recordEntityUpdate(new AtlasEntityHeader(entity));
    }


    public static String createQualifiedName(String collectionQualifiedName) {
        return String.format(qualifiedNameFormat, collectionQualifiedName, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
