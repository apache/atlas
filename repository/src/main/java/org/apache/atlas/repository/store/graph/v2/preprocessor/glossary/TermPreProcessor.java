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
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.MEANINGS_TEXT_UPDATE;
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
        try {
            if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                createAndQueueTask(MEANINGS_TEXT_UPDATE, termName, vertexQName, vertex, ELASTICSEARCH_PAGINATION_OFFSET);
            } else {
                updateMeaningsNamesInEntities(termName, vertexQName, termGuid, ELASTICSEARCH_PAGINATION_OFFSET);
            }
        } catch (AtlasBaseException e) {
            throw e;
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public void updateMeaningsNamesInEntities(String updatedTermName, String termQname, String termGuid, int size) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        int from = 0;
        while (true) {
            Map<String, Object> dsl = getMap("from", from);
            dsl.put("size", size);
            dsl.put("query", getMap("term", getMap("__meanings", getMap("value",termQname))));
            indexSearchParams.setDsl(dsl);
            AtlasSearchResult searchResult = discovery.directIndexSearch(indexSearchParams);
            List<AtlasEntityHeader> entityHeaders = searchResult.getEntities();

            if (entityHeaders == null)
                break;

            for (AtlasEntityHeader entityHeader : entityHeaders) {
                StringBuilder meaningsText = new StringBuilder("");
                List<AtlasTermAssignmentHeader> meanings = entityHeader.getMeanings();
                if (!meanings.isEmpty()) {
                    for (AtlasTermAssignmentHeader meaning : meanings) {
                        String guid = meaning.getTermGuid();

                        if (termGuid == guid) {
                            meaningsText.append("," + updatedTermName);
                        } else {
                            meaningsText.append("," + meaning.getDisplayText());
                        }
                    }
                    meaningsText.deleteCharAt(0);
                }

                AtlasGraphUtilsV2.setEncodedProperty(AtlasGraphUtilsV2.findByGuid(entityHeader.getGuid()), MEANINGS_TEXT_PROPERTY_KEY, meaningsText.toString());
            }
            from += size;

            if (entityHeaders.size() < size)
                break;
        }

    }

    public void createAndQueueTask(String taskType, String updatedTermName, String termQName, AtlasVertex termVertex, int size) {
        String termGuid = GraphHelper.getGuid(termVertex);
        String currentUser = RequestContext.getCurrentUser();
        Map<String, Object> taskParams = MeaningsTask.toParameters(updatedTermName, termQName, termGuid, size);
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
