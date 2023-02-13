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


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.Constants;
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
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.isEntityIncomplete;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.COLLECTION_QUALIFIED_NAME;

public class QueryFolderPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryFolderPreProcessor.class);

    private static String qualifiedNameFormat = "%s/folder/%s/%s";

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasInstanceConverter instanceConverter;

    public QueryFolderPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasInstanceConverter instanceConverter) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.instanceConverter = instanceConverter;
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

        AtlasEntityType entityType          = typeRegistry.getEntityTypeByName(entity.getTypeName());
        Object          newParentAttr       = entity.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME);
        String          relationshipType    = AtlasEntityUtil.getRelationshipType(newParentAttr);
        AtlasAttribute  parentAttribute     = entityType.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME, relationshipType);
        Object          currentParentAttr   = entityRetriever.getEntityAttribute(vertex, parentAttribute);

        if(!parentAttribute.getAttributeType().areEqualValues(currentParentAttr, newParentAttr, context.getGuidAssignments())) {
            AtlasVertex currentParentVertex         = entityRetriever.getEntityVertex((AtlasObjectId) currentParentAttr);
            String currentCollectionQualifiedName   = currentParentVertex.getProperty(COLLECTION_QUALIFIED_NAME, String.class);
            String newCollectionQualifiedName       = (String) entity.getAttribute(COLLECTION_QUALIFIED_NAME);
            if(StringUtils.isEmpty(newCollectionQualifiedName) || StringUtils.isEmpty(currentCollectionQualifiedName)) {
                LOG.warn("Collection qualified name in parent or current entity empty or null");
                return;
            }
            // Will update the children's property on updating parent
            processParentCollectionUpdation(vertex, currentCollectionQualifiedName, newCollectionQualifiedName);
        }
    }

    public void processParentCollectionUpdation( AtlasVertex folderVertex, String currentCollectionQualifiedName,
                                                 String newCollectionQualifiedName) throws AtlasBaseException  {

        AtlasPerfMetrics.MetricRecorder folderProcessMetric = RequestContext.get().startMetricRecord("processParentCollectionUpdation");
        String folderQualifiedName                          = folderVertex.getProperty(QUALIFIED_NAME, String.class);
        String updatedFolderQualifiedName                   = folderQualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);

        moveQueriesToDifferentCollection(folderVertex, currentCollectionQualifiedName, newCollectionQualifiedName, updatedFolderQualifiedName);

        Iterator<AtlasVertex> childrenFolders = folderVertex.query()
                .direction(AtlasEdgeDirection.OUT)
                .label(CHILDREN_FOLDERS)
                .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                .vertices()
                .iterator();

        if (childrenFolders.hasNext()) {
            AtlasVertex nestedFolderVertex = childrenFolders.next();
            if(nestedFolderVertex != null) {
                updateChildAttributesOnUpdatingCollection(nestedFolderVertex, currentCollectionQualifiedName, newCollectionQualifiedName, updatedFolderQualifiedName);
                /**
                 * Recursively find the child folders and move child queries to new collection
                 * folder1 -> folder2 -> query1
                 * When we will move folder1 to new collection, recursively it will find folder2
                 * Then it will move all the children of folder2 also to new collection
                 */
                processParentCollectionUpdation(nestedFolderVertex, currentCollectionQualifiedName, newCollectionQualifiedName);
            }
        }
        LOG.info("Moved current folder with qualifiedName {} into new collection {}", folderQualifiedName, newCollectionQualifiedName);


        RequestContext.get().endMetricRecord(folderProcessMetric);
    }


    public void moveQueriesToDifferentCollection(AtlasVertex folderVertex, String currentCollectionQualifiedName,
                                                          String newCollectionQualifiedName, String folderQualifiedName) throws AtlasBaseException {

        AtlasPerfMetrics.MetricRecorder queryProcessMetric = RequestContext.get().startMetricRecord("moveQueriesToDifferentCollection");
        Iterator<AtlasVertex> childrenQueriesIterator = folderVertex.query()
                .direction(AtlasEdgeDirection.OUT)
                .label(CHILDREN_QUERIES)
                .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                .vertices()
                .iterator();

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

    public void updateChildAttributesOnUpdatingCollection(AtlasVertex childVertex,  String currentCollectionQualifiedName, String newCollectionQualifiedName,
                                                          String folderQualifiedName) throws  AtlasBaseException{
        String qualifiedName            =   childVertex.getProperty(QUALIFIED_NAME, String.class);
        String updatedQualifiedName     =   qualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);
        AtlasGraphUtilsV2.setEncodedProperty(childVertex, QUALIFIED_NAME, updatedQualifiedName);
        AtlasGraphUtilsV2.setEncodedProperty(childVertex, COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);
        AtlasGraphUtilsV2.setEncodedProperty(childVertex, PARENT_QUALIFIED_NAME, folderQualifiedName);

        Map<String, Object> updatedAttributes = new HashMap<>();
        updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);
        updatedAttributes.put(COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);
        updatedAttributes.put(PARENT_QUALIFIED_NAME, folderQualifiedName);


        recordAndStoreDiffEntity(childVertex, updatedAttributes);

    }

    private void recordAndStoreDiffEntity(AtlasVertex entityVertex, Map<String, Object> updatedAttributes) throws AtlasBaseException {
        RequestContext requestContext = RequestContext.get();
        AtlasEntity entity = new AtlasEntity();
        entity = entityRetriever.mapSystemAttributes(entityVertex, entity);
        entity.setAttributes(updatedAttributes);
        requestContext.cacheDifferentialEntity(entity);

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        if (entityType != null) {
            for (AtlasAttribute attribute : entityType.getMinInfoAttributes().values()) {
                if(updatedAttributes.containsKey(attribute.getName())) {
                    continue;
                }

                Object attrValue = entityRetriever.getVertexAttribute(entityVertex, attribute);

                if (attrValue != null) {
                    entity.setAttribute(attribute.getName(), attrValue);
                }
            }
        }

        entityRetriever.mapClassifications(entityVertex, entity);
        requestContext.recordEntityUpdate(new AtlasEntityHeader(entity));
    }


    public static String createQualifiedName(String collectionQualifiedName) {
        return String.format(qualifiedNameFormat, collectionQualifiedName, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
