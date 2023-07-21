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


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTask;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE;
import static org.apache.atlas.type.Constants.*;

@Component
public class TermPreProcessor extends AbstractGlossaryPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TermPreProcessor.class);

    private AtlasEntityHeader anchor;
    public TermPreProcessor( AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, TaskManagement taskManagement) {
        super(typeRegistry, entityRetriever, graph, taskManagement);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("TermPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setAnchor(entity, context);

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
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateTerm");
        String termName = (String) entity.getAttribute(NAME);
        String termQName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(termName) || isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        String glossaryQName = (String) anchor.getAttribute(QUALIFIED_NAME);
        if (termExists(termName, glossaryQName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
        }

        validateCategory(entity);

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                "create entity: type=", entity.getTypeName());

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateTerm(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateTerm");
        String termName = (String) entity.getAttribute(NAME);
        String vertexName = vertex.getProperty(NAME, String.class);
        String termGuid = GraphHelper.getGuid(vertex);

        if (StringUtils.isEmpty(termName) || isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        AtlasEntity storeObject = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId existingAnchor = (AtlasRelatedObjectId) storeObject.getRelationshipAttribute(ANCHOR);

        String termQualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
        String glossaryQualifiedName = (String) anchor.getAttribute(QUALIFIED_NAME);

        validateCategory(entity);

        if (existingAnchor != null && !existingAnchor.getGuid().equals(anchor.getGuid())){
            String updatedTermQualifiedName = moveTermToAnotherGlossary(entity, vertex, glossaryQualifiedName, termName, termQualifiedName);

            if (checkEntityTermAssociation(termQualifiedName)) {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, vertexName, termName, termQualifiedName, updatedTermQualifiedName, vertex);
                } else {
                    updateMeaningsAttributesInEntitiesOnTermUpdate(vertexName, termName, termQualifiedName, updatedTermQualifiedName, termGuid);
                }
            }

        } else {

            if (!vertexName.equals(termName) && termExists(termName, glossaryQualifiedName)) {
                throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
            }

            entity.setAttribute(QUALIFIED_NAME, termQualifiedName);

            if (!termName.equals(vertexName) && checkEntityTermAssociation(termQualifiedName)) {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, vertexName, termName, termQualifiedName, null, vertex);
                } else {
                    updateMeaningsAttributesInEntitiesOnTermUpdate(vertexName, termName, termQualifiedName, null, termGuid);
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void validateCategory(AtlasEntity entity) throws AtlasBaseException {
        String glossaryQualifiedName = (String) anchor.getAttribute(QUALIFIED_NAME);

        if (entity.hasRelationshipAttribute(ATTR_CATEGORIES) && entity.getRelationshipAttribute(ATTR_CATEGORIES) != null) {
            List<AtlasObjectId> categories = (List<AtlasObjectId>) entity.getRelationshipAttribute(ATTR_CATEGORIES);

            if (CollectionUtils.isNotEmpty(categories)) {
                AtlasObjectId category = categories.get(0);
                String categoryQualifiedName;

                if (category.getUniqueAttributes() != null && category.getUniqueAttributes().containsKey(QUALIFIED_NAME)) {
                    categoryQualifiedName = (String) category.getUniqueAttributes().get(QUALIFIED_NAME);
                } else {
                    AtlasVertex categoryVertex = entityRetriever.getEntityVertex(category.getGuid());
                    categoryQualifiedName = categoryVertex.getProperty(QUALIFIED_NAME, String.class);
                }

                if (!categoryQualifiedName.endsWith(glossaryQualifiedName)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Passed category doesn't belongs to Passed Glossary");
                }
            }
        }
    }

    public String moveTermToAnotherGlossary(AtlasEntity entity, AtlasVertex vertex,
                                           String targetGlossaryQualifiedName,
                                           String newTermName,
                                           String currentTermQualifiedName) throws AtlasBaseException {

        //check duplicate term name
        if (termExists(newTermName, targetGlossaryQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, newTermName);
        }

        //qualifiedName, __u_qualifiedName
        String sourceGlossaryQualifiedName = currentTermQualifiedName.split("@")[1];

        String updatedQualifiedName = currentTermQualifiedName.replace(sourceGlossaryQualifiedName, targetGlossaryQualifiedName);

        entity.setAttribute(QUALIFIED_NAME, updatedQualifiedName);

        // __glossary
        entity.setAttribute(GLOSSARY_PROPERTY_KEY, targetGlossaryQualifiedName);

        // __categories
        /* check whether category is passed in relationshipAttributes
            -- if it is not passed, extract it from store
            if category does not belong to target glossary, throw an exception
         */
        if (!entity.hasRelationshipAttribute(ATTR_CATEGORIES)) {
            Iterator<AtlasVertex> categoriesItr = getActiveParents(vertex, CATEGORY_TERMS_EDGE_LABEL);

            if (categoriesItr.hasNext()) {
                AtlasVertex categoryVertex = categoriesItr.next();

                String categoryQualifiedName = categoryVertex.getProperty(QUALIFIED_NAME, String.class);

                LOG.info("categoryQualifiedName {}, targetGlossaryQualifiedName {}", categoryQualifiedName, targetGlossaryQualifiedName);

                if (!categoryQualifiedName.endsWith(targetGlossaryQualifiedName)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Passed category doesn't belongs to Passed Glossary");
                }
            }
        }

        return updatedQualifiedName;
    }

    private String createQualifiedName() {
        return getUUID() + "@" + anchor.getAttribute(QUALIFIED_NAME);
    }

    private void setAnchor(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("TermPreProcessor.setAnchor");
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
        RequestContext.get().endMetricRecord(metricRecorder);
    }
}
