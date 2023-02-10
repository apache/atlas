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
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
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

import java.util.Iterator;

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

        AtlasEntityType entityType          = typeRegistry.getEntityTypeByName(entity.getTypeName());
        Object          newParentAttr       = entity.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME);
        String          relationshipType    = AtlasEntityUtil.getRelationshipType(newParentAttr);
        AtlasAttribute  parentAttribute     = entityType.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME, relationshipType);
        Object          currentParentAttr   = entityRetriever.getEntityAttribute(vertex, parentAttribute);

        if(!parentAttribute.getAttributeType().areEqualValues(currentParentAttr, newParentAttr, context.getGuidAssignments())) {
            AtlasVertex currentParentVertex = entityRetriever.getEntityVertex((AtlasObjectId) currentParentAttr);
            processParentCollectionUpdation(entity, vertex, currentParentVertex);
        }
    }

    public void processParentCollectionUpdation(AtlasEntity folderEntity, AtlasVertex folderVertex, AtlasVertex currentParentCollectionVertex) {

        AtlasPerfMetrics.MetricRecorder queryProcessMetric = RequestContext.get().startMetricRecord("processParentCollectionUpdation");

        String currentCollectionQualifiedName   = currentParentCollectionVertex.getProperty(QUALIFIED_NAME, String.class);
        String newCollectionQualifiedName       = (String) folderEntity.getAttribute(COLLECTION_QUALIFIED_NAME);

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
                updateQueryAttributesOnUpdatingCollection(queryVertex, currentCollectionQualifiedName,
                        newCollectionQualifiedName, (String) folderEntity.getAttribute(QUALIFIED_NAME));
            }
        }

        RequestContext.get().endMetricRecord(queryProcessMetric);
    }

    public void updateQueryAttributesOnUpdatingCollection(AtlasVertex childVertex,  String currentCollectionQualifiedName, String newCollectionQualifiedName, String folderQualifiedName) {
        String qualifiedName            =   childVertex.getProperty(QUALIFIED_NAME, String.class);
        String updatedQualifiedName     =   qualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);
        AtlasGraphUtilsV2.setEncodedProperty(childVertex, QUALIFIED_NAME, updatedQualifiedName);
        AtlasGraphUtilsV2.setEncodedProperty(childVertex, COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);
        AtlasGraphUtilsV2.setEncodedProperty(childVertex, PARENT_QUALIFIED_NAME, folderQualifiedName);
    }


    public static String createQualifiedName(String collectionQualifiedName) {
        return String.format(qualifiedNameFormat, collectionQualifiedName, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
