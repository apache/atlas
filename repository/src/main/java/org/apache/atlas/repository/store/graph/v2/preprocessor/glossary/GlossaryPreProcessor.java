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
package org.apache.atlas.repository.store.graph.v2.preprocessor.glossary;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.type.Constants.LEXICOGRAPHICAL_SORT_ORDER;

public class GlossaryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    protected EntityDiscoveryService discovery;

    public GlossaryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                AtlasGraph graph, DynamicVertexService dynamicVertexService) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        try{
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, dynamicVertexService, null, entityRetriever);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("GlossaryPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateGlossary(entity);
                break;
            case UPDATE:
                processUpdateGlossary(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreateGlossary(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateGlossary");
        String glossaryName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(glossaryName) || isNameInvalid(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }
        String lexicographicalSortOrder = (String) entity.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);


        if (glossaryExists(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_ALREADY_EXISTS,glossaryName);
        }

        if(StringUtils.isEmpty(lexicographicalSortOrder)) {
            assignNewLexicographicalSortOrder((AtlasEntity) entity, null, null, this.discovery);
        } else {
            isValidLexoRank(lexicographicalSortOrder, "", "", this.discovery);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateGlossary(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateGlossary");
        String glossaryName = (String) entity.getAttribute(NAME);
        String vertexName = vertex.getProperty(NAME, String.class);
        if (!vertexName.equals(glossaryName) && glossaryExists(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_ALREADY_EXISTS,glossaryName);
        }

        if (StringUtils.isEmpty(glossaryName) || isNameInvalid(glossaryName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }
        String lexicographicalSortOrder = (String) entity.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        if(StringUtils.isNotEmpty(lexicographicalSortOrder)) {
            isValidLexoRank(lexicographicalSortOrder, "", "", this.discovery);
        } else {
            entity.removeAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        }

        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName() {
        return getUUID();
    }

    private boolean glossaryExists(String glossaryName) {
        return AtlasGraphUtilsV2.glossaryExists(glossaryName);
    }
}
