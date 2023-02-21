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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.PARENT_ATTRIBUTE_NAME;

public class QueryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryPreProcessor.class);

    private static String qualifiedNameFormat = "%s/query/%s/%s";

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;

    public QueryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("QueryPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateQueryCollection(entity);
                break;
            case UPDATE:
                processUpdateQueryCollection(entity, vertex, context);
                break;
        }
    }

    private void processCreateQueryCollection(AtlasStruct entity) throws AtlasBaseException {
        String collectionQualifiedName = (String) entity.getAttribute(COLLECTION_QUALIFIED_NAME);

        if (StringUtils.isEmpty(collectionQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, entity.getTypeName(), COLLECTION_QUALIFIED_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(collectionQualifiedName));
    }

    private void processUpdateQueryCollection(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        AtlasEntityType entityType                      = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasObjectId newParentAttr                     = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME);
        String relationshipType                         = AtlasEntityUtil.getRelationshipType(newParentAttr);
        AtlasStructType.AtlasAttribute parentAttribute  = entityType.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME, relationshipType);
        AtlasObjectId currentParentAttr                 = (AtlasObjectId) entityRetriever.getEntityAttribute(vertex, parentAttribute);
        //Qualified name of the folder will not be updated if parent attribute is not changed
        String queryQualifiedName                       = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, queryQualifiedName);

        //If parent has not changed, return
        if (parentAttribute.getAttributeType().areEqualValues(currentParentAttr, newParentAttr, context.getGuidAssignments())) {
            return;
        }

        //If parent attribute is changed, update the qualified name and other attributes
        AtlasVertex currentParentVertex         = entityRetriever.getEntityVertex(currentParentAttr);
        AtlasVertex newParentVertex             = entityRetriever.getEntityVertex(newParentAttr);


        if (currentParentVertex == null || newParentVertex == null) {
            LOG.warn("Current or new parent vertex is null");
            return;
        }

        String currentCollectionQualifiedName   = currentParentVertex.getProperty(getCollectionPropertyName(currentParentVertex), String.class);
        String newCollectionQualifiedName       = newParentVertex.getProperty(getCollectionPropertyName(newParentVertex), String.class);
        String updatedParentQualifiedName       = newParentVertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(newCollectionQualifiedName) || StringUtils.isEmpty(currentCollectionQualifiedName)) {
            LOG.warn("Collection qualified name in parent or current entity is empty or null");
            return;
        }

        entity.setAttribute(PARENT_QUALIFIED_NAME, updatedParentQualifiedName);

        if (!currentCollectionQualifiedName.equals(newCollectionQualifiedName)) {

            String updatedQueryQualifiedName = queryQualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);
            //Update this values into AtlasEntity
            entity.setAttribute(QUALIFIED_NAME, updatedQueryQualifiedName);
            entity.setAttribute(COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);
        }

    }

    public static String createQualifiedName(String collectionQualifiedName) {
        return String.format(qualifiedNameFormat, collectionQualifiedName, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
