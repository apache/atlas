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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.PREFIX_QUERY_QN;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.getUUID;

public class QueryCollectionPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QueryCollectionPreProcessor.class);

    private static String qualifiedNameFormat = PREFIX_QUERY_QN + "%s/%s";

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;

    public QueryCollectionPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("QueryCollectionPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreate(entity);
                break;
            case UPDATE:
                processUpdate(entity, vertex);
                break;
        }
    }

    private void processCreate(AtlasStruct entity) {
        entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
    }

    private void processUpdate(AtlasStruct entity, AtlasVertex vertex) {

    }

    public static String createQualifiedName() {
        return String.format(qualifiedNameFormat, AtlasAuthorizationUtils.getCurrentUserName(), getUUID());
    }
}
