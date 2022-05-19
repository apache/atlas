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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE;
import static org.apache.atlas.type.Constants.MEANINGS_TEXT_PROPERTY_KEY;

@Component
public class TermPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TermPreProcessor.class);

    static final boolean DEFERRED_ACTION_ENABLED = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();


    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final TaskManagement taskManagement;
    private EntityDiscoveryService discovery;

    private AtlasEntityHeader anchor;
    public TermPreProcessor( AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, TaskManagement taskManagement) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.taskManagement = taskManagement;
        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
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

        if (termExists(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName());
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                "create entity: type=", entity.getTypeName());

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateTerm(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateTerm");
        String termName = (String) entity.getAttribute(NAME);
        String vertexName = vertex.getProperty(NAME, String.class);

        if (!vertexName.equals(termName) && termExists(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_TERM_ALREADY_EXISTS, termName);
        }

        if (StringUtils.isEmpty(termName) || isNameInvalid(termName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        AtlasEntity storeObject = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId existingAnchor = (AtlasRelatedObjectId) storeObject.getRelationshipAttribute(ANCHOR);
        if (existingAnchor != null && !existingAnchor.getGuid().equals(anchor.getGuid())){
            throw new AtlasBaseException(AtlasErrorCode.ACHOR_UPDATION_NOT_SUPPORTED);
        }

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQName);

        String termGuid = GraphHelper.getGuid(vertex);

        if(!termName.equals(vertexName) && checkEntityTermAssociation(vertexQName)){
            try {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, termName, vertexQName, vertex);
                } else {
                    updateMeaningsNamesInEntitiesOnTermUpdate(termName, vertexQName, termGuid);
                }
            } catch (AtlasBaseException e) {
                throw e;
            }
        }


        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private boolean checkEntityTermAssociation(String termQName) throws AtlasBaseException{
        List<AtlasEntityHeader> entityHeader;
        entityHeader = discovery.searchUsingTermQualifiedName(0,1,termQName,null,null);
        Boolean hasEntityAssociation = entityHeader != null ? true : false;
        return hasEntityAssociation;
    }


    public void updateMeaningsNamesInEntitiesOnTermUpdate(String updatedTermName, String termQName, String termGuid) throws AtlasBaseException {
        int from = 0;
        Set<String> attributes = new HashSet<String>(){{
            add("meanings");
        }};
        Set<String> relationAttributes = new HashSet<String>(){{
            add("__state");
            add("name");
        }};
        while (true) {
            List<AtlasEntityHeader> entityHeaders = discovery.searchUsingTermQualifiedName(from, ELASTICSEARCH_PAGINATION_SIZE,
                    termQName,attributes,relationAttributes);
            if (entityHeaders == null)
                break;
            for (AtlasEntityHeader entityHeader : entityHeaders) {
                List<AtlasObjectId> meanings = (List<AtlasObjectId>) entityHeader.getAttribute("meanings");

                   String updatedMeaningsText = meanings
                           .stream()
                           .filter(x->!x.getAttributes().get("__state").equals("DELETED"))
                           .map(x -> x.getGuid().equals(termGuid) ? updatedTermName : x.getAttributes().get("name").toString())
                           .collect(Collectors.joining(","));

                AtlasGraphUtilsV2.setEncodedProperty(AtlasGraphUtilsV2.findByGuid(entityHeader.getGuid()), MEANINGS_TEXT_PROPERTY_KEY, updatedMeaningsText);
            }
            from += ELASTICSEARCH_PAGINATION_SIZE;

            if (entityHeaders.size() < ELASTICSEARCH_PAGINATION_SIZE)
                break;
        }

    }

    public void createAndQueueTask(String taskType, String updatedTermName, String termQName, AtlasVertex termVertex) {
        String termGuid = GraphHelper.getGuid(termVertex);
        String currentUser = RequestContext.getCurrentUser();
        Map<String, Object> taskParams = MeaningsTask.toParameters(updatedTermName, termQName, termGuid);
        AtlasTask task = taskManagement.createTask(taskType, currentUser, taskParams);

        AtlasGraphUtilsV2.addItemToListProperty(termVertex, EDGE_PENDING_TASKS_PROPERTY_KEY, task.getGuid());

        RequestContext.get().queueTask(task);
    }

    private String createQualifiedName() {
        return getUUID() + "@" + anchor.getAttribute(QUALIFIED_NAME);
    }

    private boolean termExists(String termName) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("termExists");
        boolean ret = false;
        String glossaryQName = (String) anchor.getAttribute(QUALIFIED_NAME);

        ret = AtlasGraphUtilsV2.termExists(termName, glossaryQName);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
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
