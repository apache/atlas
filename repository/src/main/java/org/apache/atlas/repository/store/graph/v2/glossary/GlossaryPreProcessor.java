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
import org.apache.atlas.glossary.GlossaryService;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.atlas.repository.store.graph.v2.glossary.Utils.*;

public class GlossaryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final EntityMutations.EntityOperation operation;

    public GlossaryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityMutations.EntityOperation operation) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.operation = operation;
    }

    @Override
    public void processAttributes(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("GlossaryPreProcessor.processAttributes: pre processing {}, {}", entity.getAttribute(QUALIFIED_NAME), operation);
        }

        LOG.info("GlossaryPreProcessor.processAttributes: pre processing {}", AtlasType.toJson(entity));

        switch (operation) {
            case CREATE:
                processCreateGlossary(entity);
                break;
            case UPDATE:
                processUpdateGlossary(entity, vertex);
                break;
        }
    }

    private void processCreateGlossary(AtlasStruct entity) throws AtlasBaseException {
        String glossaryName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(glossaryName) || GlossaryService.isNameInvalid(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        if (glossaryExists(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_ALREADY_EXISTS,glossaryName);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
    }

    private void processUpdateGlossary(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        String glossaryName = (String) entity.getAttribute(NAME);
        String vertexName = vertex.getProperty(NAME, String.class);

        if (!vertexName.equals(glossaryName) && glossaryExists(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_ALREADY_EXISTS,glossaryName);
        }

        if (StringUtils.isEmpty(glossaryName) || GlossaryService.isNameInvalid(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
    }

    public static String createQualifiedName() {
        return getUUID();
    }

    private boolean glossaryExists(String glossaryName) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(ATLAS_GLOSSARY_TYPENAME);

        AtlasVertex vertex = AtlasGraphUtilsV2.glossaryFindByTypeAndPropertyName(entityType, glossaryName);

        return Objects.nonNull(vertex);
    }
}
