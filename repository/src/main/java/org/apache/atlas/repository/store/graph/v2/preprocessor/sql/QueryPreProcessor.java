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
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.COLLECTION_QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;

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
                processUpdateQueryCollection(entity, vertex);
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

    private void processUpdateQueryCollection(AtlasStruct entity, AtlasVertex vertex) {
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
    }

    public static String createQualifiedName(String collectionQualifiedName) {
        return String.format(qualifiedNameFormat, collectionQualifiedName, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
