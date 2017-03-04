/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.resources;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.impexp.AtlasExportRequest.OPTION_FETCH_TYPE;
import static org.apache.atlas.model.impexp.AtlasExportRequest.OPTION_ATTR_MATCH_TYPE;
import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_FULL;
import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_CONNECTED;
import static org.apache.atlas.model.impexp.AtlasExportRequest.MATCH_TYPE_STARTS_WITH;
import static org.apache.atlas.model.impexp.AtlasExportRequest.MATCH_TYPE_CONTAINS;
import static org.apache.atlas.model.impexp.AtlasExportRequest.MATCH_TYPE_MATCHES;
import static org.apache.atlas.model.impexp.AtlasExportRequest.MATCH_TYPE_ENDS_WITH;

public class ExportService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);

    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasGraph                atlasGraph;
    private final EntityGraphRetriever      entityGraphRetriever;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;

    public ExportService(final AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this.typeRegistry         = typeRegistry;
        this.entityGraphRetriever = new EntityGraphRetriever(this.typeRegistry);
        this.atlasGraph           = AtlasGraphProvider.getGraphInstance();
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
    }

    public AtlasExportResult run(ZipSink exportSink, AtlasExportRequest request, String userName, String hostName,
                                 String requestingIP) throws AtlasBaseException {
        long              startTime = System.currentTimeMillis();
        AtlasExportResult result    = new AtlasExportResult(request, userName, hostName, requestingIP, startTime);
        ExportContext     context   = new ExportContext(result, exportSink);

        try {
            LOG.info("==> export(user={}, from={})", userName, requestingIP);

            for (AtlasObjectId item : request.getItemsToExport()) {
                processObjectId(item, context);
            }

            long endTime = System.currentTimeMillis();

            context.sink.setExportOrder(context.result.getData().getEntityCreationOrder());
            context.sink.setTypesDef(context.result.getData().getTypesDef());
            context.result.setData(null);
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.SUCCESS);
            context.result.incrementMeticsCounter("duration", (int) (endTime - startTime));

            context.sink.setResult(context.result);
        } catch(Exception ex) {
            LOG.error("Operation failed: ", ex);
        } finally {
            atlasGraph.releaseGremlinScriptEngine(context.scriptEngine);

            LOG.info("<== export(user={}, from={}): status {}", userName, requestingIP, context.result.getOperationStatus());
        }

        return context.result;
    }

    private void processObjectId(AtlasObjectId item, ExportContext context) throws AtlasServiceException, AtlasException, AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> processObjectId({})", item);
        }

        try {
            List<AtlasEntity> entities = getStartingEntity(item, context);

            for (AtlasEntity entity: entities) {
                processEntity(entity, context, TraversalDirection.UNKNOWN);
            }

            while (!context.guidsToProcess.isEmpty()) {
                String             guid      = context.guidsToProcess.remove(0);
                TraversalDirection direction = context.guidDirection.get(guid);
                AtlasEntity        entity    = entityGraphRetriever.toAtlasEntity(guid);

                processEntity(entity, context, direction);
            }
        } catch (AtlasBaseException excp) {
            context.result.setOperationStatus(AtlasExportResult.OperationStatus.PARTIAL_SUCCESS);

            LOG.error("Fetching entity failed for: {}", item, excp);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== processObjectId({})", item);
        }
    }

    private List<AtlasEntity> getStartingEntity(AtlasObjectId item, ExportContext context) throws AtlasBaseException {
        List<AtlasEntity> ret = new ArrayList<>();

        if (StringUtils.isNotEmpty(item.getGuid())) {
            AtlasEntity entity = entityGraphRetriever.toAtlasEntity(item);

            if (entity != null) {
                ret = Collections.singletonList(entity);
            }
        } else if (StringUtils.isNotEmpty(item.getTypeName()) && MapUtils.isNotEmpty(item.getUniqueAttributes())) {
            String          typeName   = item.getTypeName();
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, typeName);
            }

            final String queryTemplate;
            if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_STARTS_WITH)) {
                queryTemplate = gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_STARTS_WITH);
            } else if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_ENDS_WITH)) {
                queryTemplate = gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_ENDS_WITH);
            } else if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_CONTAINS)) {
                queryTemplate = gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_CONTAINS);
            } else if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_MATCHES)) {
                queryTemplate = gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_MATCHES);
            } else { // default
                queryTemplate = gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_DEFAULT);
            }

            for (Map.Entry<String, Object> e : item.getUniqueAttributes().entrySet()) {
                String attrName  = e.getKey();
                Object attrValue = e.getValue();

                AtlasAttribute attribute = entityType.getAttribute(attrName);

                if (attribute == null || attrValue == null) {
                    continue;
                }

                context.bindings.clear();
                context.bindings.put("typeName", typeName);
                context.bindings.put("attrName", attribute.getQualifiedName());
                context.bindings.put("attrValue", attrValue);

                List<String> guids = executeGremlinQuery(queryTemplate, context);

                if (CollectionUtils.isNotEmpty(guids)) {
                    for (String guid : guids) {
                        AtlasEntity entity = entityGraphRetriever.toAtlasEntity(guid);

                        if (entity == null) {
                            continue;
                        }

                        ret.add(entity);
                    }
                }

                break;
            }

            LOG.info("export(item={}; matchType={}, fetchType={}): found {} entities", item, context.matchType, context.fetchType, ret.size());
        }

        return ret;
    }

    private void processEntity(AtlasEntity entity, ExportContext context, TraversalDirection direction) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> processEntity({})", AtlasTypeUtil.getAtlasObjectId(entity));
        }

        if (!context.guidsProcessed.contains(entity.getGuid())) {
            context.guidsProcessed.add(entity.getGuid());
            context.result.getData().getEntityCreationOrder().add(entity.getGuid());

            addTypesAsNeeded(entity.getTypeName(), context);
            addClassificationsAsNeeded(entity, context);
            addEntity(entity, context);

            getConntedEntitiesBasedOnOption(entity, context, direction);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== processEntity({})", AtlasTypeUtil.getAtlasObjectId(entity));
        }
    }

    private void getConntedEntitiesBasedOnOption(AtlasEntity entity, ExportContext context, TraversalDirection direction) throws AtlasBaseException {
        switch (context.fetchType) {
            case CONNECTED:
                getEntityGuidsForConnectedFetch(entity, context, direction);
                break;

            case FULL:
            default:
                getEntityGuidsForFullFetch(entity, context);
        }
    }

    private void getEntityGuidsForConnectedFetch(AtlasEntity entity, ExportContext context, TraversalDirection direction) throws AtlasBaseException {
        if (direction == TraversalDirection.UNKNOWN) {
            getConnectedEntityGuids(entity, context, TraversalDirection.OUTWARD, TraversalDirection.OUTWARD);
        } else {
            if (isProcessEntity(entity)) {
                direction = TraversalDirection.OUTWARD;
            }

            getConnectedEntityGuids(entity, context, direction);
        }
    }

    private boolean isProcessEntity(AtlasEntity entity) throws AtlasBaseException {
        String          typeName   = entity.getTypeName();
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

        return entityType.isSubTypeOf(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS);
    }

    private void getConnectedEntityGuids(AtlasEntity entity, ExportContext context, TraversalDirection... directions) {
        if(directions == null) {
            return;
        }

        for (TraversalDirection direction : directions) {
            String query = getQueryForTraversalDirection(direction);

            if (LOG.isDebugEnabled()) {
                LOG.debug("==> getConnectedEntityGuids({}): guidsToProcess {} query {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size(), query);
            }

            context.bindings.clear();
            context.bindings.put("startGuid", entity.getGuid());

            List<String> guids = executeGremlinQuery(query, context);

            if (CollectionUtils.isEmpty(guids)) {
                continue;
            }

            for (String guid : guids) {
                TraversalDirection currentDirection = context.guidDirection.get(guid);

                if (currentDirection == null) {
                    context.guidDirection.put(guid, direction);

                    if (!context.guidsToProcess.contains(guid)) {
                        context.guidsToProcess.add(guid);
                    }
                } else if (currentDirection == TraversalDirection.OUTWARD && direction == TraversalDirection.INWARD) {
                    context.guidDirection.put(guid, direction);

                    // the entity should be reprocessed to get inward entities
                    context.guidsProcessed.remove(guid);

                    if (!context.guidsToProcess.contains(guid)) {
                        context.guidsToProcess.add(guid);
                    }
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== getConnectedEntityGuids({}): found {} guids; guidsToProcess {}", entity.getGuid(), guids.size(), context.guidsToProcess.size());
            }
        }
    }

    private String getQueryForTraversalDirection(TraversalDirection direction) {
        switch (direction) {
            case INWARD:
                return this.gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_BY_GUID_CONNECTED_IN_EDGE);

            default:
            case OUTWARD:
                return this.gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_BY_GUID_CONNECTED_OUT_EDGE);
        }
    }

    private void getEntityGuidsForFullFetch(AtlasEntity entity, ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEntityGuidsForFullFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        String query = this.gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_BY_GUID_FULL);

        context.bindings.clear();
        context.bindings.put("startGuid", entity.getGuid());

        List<String> result = executeGremlinQuery(query, context);

        if (result == null) {
            return;
        }

        for (String guid : result) {
            if (!context.guidsProcessed.contains(guid)) {
                if (!context.guidsToProcess.contains(guid)) {
                    context.guidsToProcess.add(guid);
                }

                context.guidDirection.put(guid, TraversalDirection.BOTH);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getEntityGuidsForFullFetch({}): found {} guids; guidsToProcess {}", entity.getGuid(), result.size(), context.guidsToProcess.size());
        }
    }

    private void addEntity(AtlasEntity entity, ExportContext context) throws AtlasBaseException {
        context.sink.add(entity);

        context.result.incrementMeticsCounter(String.format("entity:%s", entity.getTypeName()));
        context.result.incrementMeticsCounter("entities");

        if (context.guidsProcessed.size() % 10 == 0) {
            LOG.info("export(): in progress.. number of entities exported: {}", context.guidsProcessed.size());
        }
    }

    private void addClassificationsAsNeeded(AtlasEntity entity, ExportContext context) {
        AtlasExportResult result   = context.result;
        AtlasTypesDef     typesDef = result.getData().getTypesDef();

        if(CollectionUtils.isNotEmpty(entity.getClassifications())) {
            for (AtlasClassification c : entity.getClassifications()) {
                if (typesDef.hasClassificationDef(c.getTypeName())) {
                    continue;
                }

                AtlasClassificationDef cd = typeRegistry.getClassificationDefByName(c.getTypeName());

                typesDef.getClassificationDefs().add(cd);
                result.incrementMeticsCounter("typedef:classification");
            }
        }
    }

    private void addTypesAsNeeded(String typeName, ExportContext context) {
        AtlasExportResult result   = context.result;
        AtlasTypesDef     typesDef = result.getData().getTypesDef();

        if(!typesDef.hasEntityDef(typeName)) {
            AtlasEntityDef typeDefinition = typeRegistry.getEntityDefByName(typeName);

            typesDef.getEntityDefs().add(typeDefinition);
            result.incrementMeticsCounter("typedef:" + typeDefinition.getName());
        }
    }

    private List<String> executeGremlinQuery(String query, ExportContext context) {
        try {
            return (List<String>) atlasGraph.executeGremlinScript(context.scriptEngine, context.bindings, query, false);
        } catch (ScriptException e) {
            LOG.error("Script execution failed for query: ", query, e);
            return null;
        }
    }


    private enum TraversalDirection {
        UNKNOWN,
        INWARD,
        OUTWARD,
        BOTH;
    }


    public enum ExportFetchType {
        FULL(FETCH_TYPE_FULL),
        CONNECTED(FETCH_TYPE_CONNECTED);

        final String str;
        ExportFetchType(String s) {
            this.str = s;
        }

        public static final ExportFetchType from(String s) {
            for (ExportFetchType b : ExportFetchType.values()) {
                if (b.str.equalsIgnoreCase(s)) {
                    return b;
                }
            }

            return FULL;
        }
    }


    private class ExportContext {
        final Set<String>                     guidsProcessed = new HashSet<>();
        final List<String>                    guidsToProcess = new ArrayList<>();
        final Map<String, TraversalDirection> guidDirection  = new HashMap<>();
        final AtlasExportResult               result;
        final ZipSink                         sink;

        private final ScriptEngine        scriptEngine;
        private final Map<String, Object> bindings;
        private final ExportFetchType     fetchType;
        private final String              matchType;

        ExportContext(AtlasExportResult result, ZipSink sink) {
            this.result = result;
            this.sink   = sink;

            scriptEngine = atlasGraph.getGremlinScriptEngine();
            bindings     = new HashMap<>();
            fetchType    = getFetchType(result.getRequest());
            matchType    = getMatchType(result.getRequest());
        }

        private ExportFetchType getFetchType(AtlasExportRequest request) {
            Object fetchOption = request.getOptions() != null ? request.getOptions().get(OPTION_FETCH_TYPE) : null;

            if (fetchOption instanceof String) {
                return ExportFetchType.from((String) fetchOption);
            } else if (fetchOption instanceof ExportFetchType) {
                return (ExportFetchType) fetchOption;
            }

            return ExportFetchType.FULL;
        }

        private String getMatchType(AtlasExportRequest request) {
            String matchType = null;

            if (MapUtils.isNotEmpty(request.getOptions())) {
                if (request.getOptions().get(OPTION_ATTR_MATCH_TYPE) != null) {
                    matchType = request.getOptions().get(OPTION_ATTR_MATCH_TYPE).toString();
                }
            }

            return matchType;
        }
    }
}
