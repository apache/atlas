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
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.atlas.glossary.GlossaryService.isNameInvalid;
import static org.apache.atlas.repository.store.graph.v2.glossary.Utils.*;

public class TermPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TermPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final EntityMutations.EntityOperation operation;

    private AtlasEntityHeader anchor;

    public TermPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                            EntityMutations.EntityOperation operation) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.operation = operation;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, AtlasVertex vertex) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("GlossaryPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        LOG.info("GlossaryPreProcessor.processAttributes: pre processing {}", AtlasType.toJson(entityStruct));

        AtlasEntity entity = (AtlasEntity) entityStruct;
        setAnchor(entity);

        switch (operation) {
            case CREATE:
                processCreateTerm(entity, vertex);
                break;
            case UPDATE:
                processUpdateTerm(entity, vertex);
                break;
        }
    }

    private void processCreateTerm(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String termName = (String) entity.getAttribute(NAME);
        String termQName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(termName) || isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        if (termExists(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(termQName));
    }

    private void processUpdateTerm(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String termName = (String) entity.getAttribute(NAME);
        String vertexName = vertex.getProperty(NAME, String.class);

        if (!vertexName.equals(termName) && termExists(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
        }

        if (isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
    }

    protected String createQualifiedName(String termQualifiedName) {
        String qName = "";
        if (!StringUtils.isEmpty(termQualifiedName)) {
            //extract existing nanoid for term
            qName = termQualifiedName.split("@")[0];
        }
        qName = StringUtils.isEmpty(qName) ? getUUID() : qName;

        return qName + "@" + anchor.getAttribute(QUALIFIED_NAME);
    }

    private boolean termExists(String termName) {

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(Utils.ATLAS_GLOSSARY_TERM_TYPENAME);
        String glossaryQName = (String) anchor.getAttribute(QUALIFIED_NAME);

        List<AtlasVertex> vertexList = AtlasGraphUtilsV2.glossaryFindChildByTypeAndPropertyName(entityType, termName, glossaryQName);

        return CollectionUtils.isNotEmpty(vertexList);
    }

    private void setAnchor(AtlasEntity entity) throws AtlasBaseException {

        if (anchor == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(ANCHOR);

            if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                    StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                anchor = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

            } else {
                anchor = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
            }
        }
    }
}
