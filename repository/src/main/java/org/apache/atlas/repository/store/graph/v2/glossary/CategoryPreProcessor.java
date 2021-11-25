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
package org.apache.atlas.repository.store.graph.v2.glossary;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.glossary.GlossaryUtils.ANCHOR;
import static org.apache.atlas.repository.store.graph.v2.glossary.GlossaryUtils.CATEGORY_PARENT;
import static org.apache.atlas.repository.store.graph.v2.glossary.GlossaryUtils.isNameInvalid;
import static org.apache.atlas.repository.store.graph.v2.glossary.GlossaryUtils.getUUID;

public class CategoryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final EntityMutations.EntityOperation operation;

    private AtlasEntityHeader anchor;
    private AtlasEntityHeader parentCategory;

    public CategoryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                EntityMutations.EntityOperation operation) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.operation = operation;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("CategoryPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        setAnchorAndParent(entity, context);

        switch (operation) {
            case CREATE:
                processCreateCategory(entity, vertex);
                break;
            case UPDATE:
                processUpdateCategory(entity, vertex);
                break;
        }
    }

    private void processCreateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String catName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(vertex));
    }

    private void processUpdateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String catName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
    }

    private void setAnchorAndParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {

        if (anchor == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(ANCHOR);

            if (StringUtils.isNotEmpty(objectId.getGuid())) {
                AtlasVertex vertex = context.getVertex(objectId.getGuid());

                if (vertex == null) {
                    anchor = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                } else {
                    anchor = entityRetriever.toAtlasEntityHeader(vertex);
                }

            } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                    StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                anchor = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

            }
        }

        if (parentCategory == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(CATEGORY_PARENT);

            if (objectId != null) {
                if (StringUtils.isNotEmpty(objectId.getGuid())) {
                    AtlasVertex vertex = context.getVertex(objectId.getGuid());

                    if (vertex == null) {
                        parentCategory = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                    } else {
                        parentCategory = entityRetriever.toAtlasEntityHeader(vertex);
                    }

                } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    parentCategory = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

                }
            }
        }
    }

    private String createQualifiedName(AtlasVertex vertex) {

        if (vertex != null) {
            String catQName = vertex.getProperty(QUALIFIED_NAME, String.class);
            if (StringUtils.isNotEmpty(catQName)) {
                return catQName;
            }
        }
        return getUUID() + "@" + anchor.getAttribute(QUALIFIED_NAME);
    }
}
