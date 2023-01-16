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
package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;

import org.apache.atlas.accesscontrol.persona.AtlasPersonaService;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.accesscontrol.purpose.AtlasPurposeService;
import org.apache.atlas.accesscontrol.purpose.PurposeContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.CategoryPreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class AccessControlPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasPersonaService personaService;
    private final AtlasPurposeService purposeService;

    public AccessControlPreProcessor(AtlasTypeRegistry typeRegistry, AtlasGraph graph, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        personaService = new AtlasPersonaService(graph, entityRetriever);
        purposeService = new AtlasPurposeService(graph, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AccessControlPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateAccessControl(entity, vertex);
                break;
            case UPDATE:
                processUpdateAccessControl(entity, vertex);
                break;
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);

        if(PERSONA_ENTITY_TYPE.equals(entityWithExtInfo.getEntity().getTypeName())) {
            personaService.deletePersona(entityWithExtInfo);
        } else {
            purposeService.deletePurpose(entityWithExtInfo);
        }
    }

    private void processCreateAccessControl(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {

        if(PERSONA_ENTITY_TYPE.equals(entity.getTypeName())) {
            PersonaContext context = new PersonaContext(new AtlasEntity.AtlasEntityWithExtInfo(entity));
            personaService.createPersona(context);
        } else {
            PurposeContext context = new PurposeContext(new AtlasEntity.AtlasEntityWithExtInfo(entity));
            purposeService.createPurpose(context);
        }
    }


    private void processUpdateAccessControl(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, vertexQName);

        if(PERSONA_ENTITY_TYPE.equals(entity.getTypeName())) {
            PersonaContext context = new PersonaContext(new AtlasEntity.AtlasEntityWithExtInfo(entity));
            personaService.updatePersona(context, entityRetriever.toAtlasEntityWithExtInfo(vertex));
        } else {
            PurposeContext context = new PurposeContext(new AtlasEntity.AtlasEntityWithExtInfo(entity));
            purposeService.updatePurpose(context, entityRetriever.toAtlasEntityWithExtInfo(vertex));
        }
    }
}