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
package org.apache.atlas.repository.impexp;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.util.UniqueList;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_CONNECTED;
import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_FULL;
import static org.apache.atlas.model.impexp.AtlasExportRequest.FETCH_TYPE_INCREMENTAL;

@Component
public class ExportService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);

    public static final String PROPERTY_GUID = "__guid";
    private static final String PROPERTY_IS_PROCESS = "isProcess";

    private final AtlasTypeRegistry         typeRegistry;
    private final String QUERY_BINDING_START_GUID = "startGuid";
    private final StartEntityFetchByExportRequest startEntityFetchByExportRequest;
    private       AuditsWriter              auditsWriter;
    private final AtlasGraph                atlasGraph;
    private final EntityGraphRetriever      entityGraphRetriever;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;
    private       ExportTypeProcessor       exportTypeProcessor;
    private final HdfsPathEntityCreator     hdfsPathEntityCreator;
    private       IncrementalExportEntityProvider incrementalExportEntityProvider;

    @Inject
    public ExportService(final AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph,
                         AuditsWriter auditsWriter, HdfsPathEntityCreator hdfsPathEntityCreator) {
        this.typeRegistry         = typeRegistry;
        this.entityGraphRetriever = new EntityGraphRetriever(this.typeRegistry);
        this.atlasGraph           = atlasGraph;
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
        this.auditsWriter         = auditsWriter;
        this.hdfsPathEntityCreator = hdfsPathEntityCreator;
        this.startEntityFetchByExportRequest = new StartEntityFetchByExportRequest(atlasGraph, typeRegistry, AtlasGremlinQueryProvider.INSTANCE);
    }

    public AtlasExportResult run(ZipSink exportSink, AtlasExportRequest request, String userName, String hostName,
                                 String requestingIP) throws AtlasBaseException {
        long startTime = System.currentTimeMillis();
        AtlasExportResult result = new AtlasExportResult(request, userName, requestingIP,
                hostName, startTime, getCurrentChangeMarker());

        ExportContext context = new ExportContext(atlasGraph, result, exportSink);
        exportTypeProcessor = new ExportTypeProcessor(typeRegistry);

        try {
            LOG.info("==> export(user={}, from={})", userName, requestingIP);

            AtlasExportResult.OperationStatus[] statuses = processItems(request, context);

            processTypesDef(context);
            long endTime = System.currentTimeMillis();
            updateSinkWithOperationMetrics(userName, context, statuses, startTime, endTime);
        } catch(Exception ex) {
            LOG.error("Operation failed: ", ex);
        } finally {
            atlasGraph.releaseGremlinScriptEngine(context.scriptEngine);
            LOG.info("<== export(user={}, from={}): status {}: changeMarker: {}",
                    userName, requestingIP, context.result.getOperationStatus(), context.result.getChangeMarker());
            context.clear();
            result.clear();
            incrementalExportEntityProvider = null;
        }

        return context.result;
    }

    private long getCurrentChangeMarker() {
        return RequestContext.earliestActiveRequestTime();
    }

    private void updateSinkWithOperationMetrics(String userName, ExportContext context,
                                                AtlasExportResult.OperationStatus[] statuses,
                                                long startTime, long endTime) throws AtlasBaseException {
        int duration = getOperationDuration(startTime, endTime);
        context.result.setSourceClusterName(AuditsWriter.getCurrentClusterName());

        context.sink.setExportOrder(context.entityCreationOrder.getList());
        context.sink.setTypesDef(context.result.getData().getTypesDef());
        context.result.setOperationStatus(getOverallOperationStatus(statuses));
        context.result.incrementMeticsCounter("duration", duration);
        auditsWriter.write(userName, context.result, startTime, endTime, context.entityCreationOrder.getList());

        context.result.setData(null);
        context.sink.setResult(context.result);
    }

    private int getOperationDuration(long startTime, long endTime) {
        return (int) (endTime - startTime);
    }

    private void processTypesDef(ExportContext context) {
        AtlasTypesDef typesDef = context.result.getData().getTypesDef();

        for (String entityType : context.entityTypes) {
            AtlasEntityDef entityDef = typeRegistry.getEntityDefByName(entityType);

            typesDef.getEntityDefs().add(entityDef);
        }

        for (String classificationType : context.classificationTypes) {
            AtlasClassificationDef classificationDef = typeRegistry.getClassificationDefByName(classificationType);

            typesDef.getClassificationDefs().add(classificationDef);
        }

        for (String structType : context.structTypes) {
            AtlasStructDef structDef = typeRegistry.getStructDefByName(structType);

            typesDef.getStructDefs().add(structDef);
        }

        for (String enumType : context.enumTypes) {
            AtlasEnumDef enumDef = typeRegistry.getEnumDefByName(enumType);

            typesDef.getEnumDefs().add(enumDef);
        }

        for (String relationshipType : context.relationshipTypes) {
            AtlasRelationshipDef relationshipDef = typeRegistry.getRelationshipDefByName(relationshipType);

            typesDef.getRelationshipDefs().add(relationshipDef);
        }
    }

    private AtlasExportResult.OperationStatus[] processItems(AtlasExportRequest request, ExportContext context) {
        AtlasExportResult.OperationStatus statuses[] = new AtlasExportResult.OperationStatus[request.getItemsToExport().size()];
        List<AtlasObjectId> itemsToExport = request.getItemsToExport();
        for (int i = 0; i < itemsToExport.size(); i++) {
            AtlasObjectId item = itemsToExport.get(i);
            statuses[i] = processObjectId(item, context);
        }
        return statuses;
    }

    @VisibleForTesting
    AtlasExportResult.OperationStatus getOverallOperationStatus(AtlasExportResult.OperationStatus... statuses) {
        AtlasExportResult.OperationStatus overall = (statuses.length == 0) ?
                AtlasExportResult.OperationStatus.FAIL : statuses[0];

        for (AtlasExportResult.OperationStatus s : statuses) {
            if (overall != s) {
                overall = AtlasExportResult.OperationStatus.PARTIAL_SUCCESS;
            }
        }

        return overall;
    }

    private AtlasExportResult.OperationStatus processObjectId(AtlasObjectId item, ExportContext context) {
        debugLog("==> processObjectId({})", item);

        try {
            List<String> entityGuids = getStartingEntity(item, context);
            if(entityGuids.size() == 0) {
                return AtlasExportResult.OperationStatus.FAIL;
            }

            for (String guid : entityGuids) {
                processEntityGuid(guid, context);
                populateEntitesForIncremental(guid, context);
            }

            while (!context.guidsToProcess.isEmpty()) {
                while (!context.guidsToProcess.isEmpty()) {
                    String guid = context.guidsToProcess.remove(0);
                    processEntityGuid(guid, context);
                }

                if (!context.lineageToProcess.isEmpty()) {
                    context.guidsToProcess.addAll(context.lineageToProcess);
                    context.lineageProcessed.addAll(context.lineageToProcess.getList());
                    context.lineageToProcess.clear();
                }
            }
        } catch (AtlasBaseException excp) {
            LOG.error("Fetching entity failed for: {}", item, excp);
            return AtlasExportResult.OperationStatus.FAIL;
        }

        debugLog("<== processObjectId({})", item);
        return AtlasExportResult.OperationStatus.SUCCESS;
    }

    private List<String> getStartingEntity(AtlasObjectId item, ExportContext context) throws AtlasBaseException {
        if(item.getTypeName().equalsIgnoreCase(HdfsPathEntityCreator.HDFS_PATH_TYPE)) {
            hdfsPathEntityCreator.getCreateEntity(item);
        }

        return startEntityFetchByExportRequest.get(context.result.getRequest(), item);
    }

    private void debugLog(String s, Object... params) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug(s, params);
    }

    private void processEntityGuid(String guid, ExportContext context) throws AtlasBaseException {
        debugLog("==> processEntityGuid({})", guid);

        if (context.guidsProcessed.contains(guid)) {
            return;
        }

        TraversalDirection direction = context.guidDirection.get(guid);
        AtlasEntityWithExtInfo entityWithExtInfo = entityGraphRetriever.toAtlasEntityWithExtInfo(guid);

        processEntity(entityWithExtInfo, context, direction);

        debugLog("<== processEntityGuid({})", guid);
    }

    public void processEntity(AtlasEntityWithExtInfo entityWithExtInfo,
                              ExportContext context,
                              TraversalDirection direction) throws AtlasBaseException {

        addEntity(entityWithExtInfo, context);
        exportTypeProcessor.addTypes(entityWithExtInfo.getEntity(), context);

        context.guidsProcessed.add(entityWithExtInfo.getEntity().getGuid());
        getConntedEntitiesBasedOnOption(entityWithExtInfo.getEntity(), context, direction);

        if (entityWithExtInfo.getReferredEntities() != null) {
            for (AtlasEntity e : entityWithExtInfo.getReferredEntities().values()) {
                exportTypeProcessor.addTypes(e, context);
                getConntedEntitiesBasedOnOption(e, context, direction);
            }

            context.guidsProcessed.addAll(entityWithExtInfo.getReferredEntities().keySet());
        }
    }

    private void getConntedEntitiesBasedOnOption(AtlasEntity entity, ExportContext context, TraversalDirection direction) {
        switch (context.fetchType) {
            case CONNECTED:
                getEntityGuidsForConnectedFetch(entity, context, direction);
                break;

            case INCREMENTAL:
                if(context.isHiveDBIncrementalSkipLineage()) {
                    break;
                }

            case FULL:
            default:
                getEntityGuidsForFullFetch(entity, context);
        }
    }

    private void populateEntitesForIncremental(String topLevelEntityGuid, ExportContext context) {
        if (context.isHiveDBIncrementalSkipLineage() == false || incrementalExportEntityProvider != null) {
            return;
        }

        incrementalExportEntityProvider = new IncrementalExportEntityProvider(atlasGraph, context.scriptEngine);
        incrementalExportEntityProvider.populate(topLevelEntityGuid, context.changeMarker, context.guidsToProcess);
    }

    private void getEntityGuidsForConnectedFetch(AtlasEntity entity, ExportContext context, TraversalDirection direction) {
        if (direction == null || direction == TraversalDirection.UNKNOWN) {
            getConnectedEntityGuids(entity, context, TraversalDirection.OUTWARD, TraversalDirection.INWARD);
        } else {
            if (isProcessEntity(entity)) {
                direction = TraversalDirection.OUTWARD;
            }

            getConnectedEntityGuids(entity, context, direction);
        }
    }

    private boolean isProcessEntity(AtlasEntity entity) {
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

            if(LOG.isDebugEnabled()) {
                debugLog("==> getConnectedEntityGuids({}): guidsToProcess {} query {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size(), query);
            }

            context.bindings.clear();
            context.bindings.put(QUERY_BINDING_START_GUID, entity.getGuid());

            List<Map<String, Object>> result = executeGremlinQuery(query, context);

            if (CollectionUtils.isEmpty(result)) {
                continue;
            }

            for (Map<String, Object> hashMap : result) {
                String             guid             = (String) hashMap.get(PROPERTY_GUID);
                TraversalDirection currentDirection = context.guidDirection.get(guid);
                boolean            isLineage        = (boolean) hashMap.get(PROPERTY_IS_PROCESS);

                if(context.skipLineage && isLineage) continue;

                if (currentDirection == null) {
                    context.addToBeProcessed(isLineage, guid, direction);

                } else if (currentDirection == TraversalDirection.OUTWARD && direction == TraversalDirection.INWARD) {
                    // the entity should be reprocessed to get inward entities
                    context.guidsProcessed.remove(guid);
                    context.addToBeProcessed(isLineage, guid, direction);
                }
            }

            if(LOG.isDebugEnabled()) {
                debugLog("<== getConnectedEntityGuids({}): found {} guids; guidsToProcess {}", entity.getGuid(), result.size(), context.guidsToProcess.size());
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
        if(LOG.isDebugEnabled()) {
            debugLog("==> getEntityGuidsForFullFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        String query = this.gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_BY_GUID_FULL);

        context.bindings.clear();
        context.bindings.put(QUERY_BINDING_START_GUID, entity.getGuid());

        List<Map<String, Object>> result = executeGremlinQuery(query, context);

        if (CollectionUtils.isEmpty(result)) {
            return;
        }

        for (Map<String, Object> hashMap : result) {
            String  guid      = (String) hashMap.get(PROPERTY_GUID);
            boolean isLineage = (boolean) hashMap.get(PROPERTY_IS_PROCESS);

            if(context.getSkipLineage() && isLineage) continue;

            if (!context.guidsProcessed.contains(guid)) {
                context.addToBeProcessed(isLineage, guid, TraversalDirection.BOTH);
            }
        }

        if(LOG.isDebugEnabled()) {
            debugLog("<== getEntityGuidsForFullFetch({}): found {} guids; guidsToProcess {}",
                                            entity.getGuid(), result.size(), context.guidsToProcess.size());
        }
    }

    private void addEntity(AtlasEntityWithExtInfo entityWithExtInfo, ExportContext context) throws AtlasBaseException {
        if(context.sink.hasEntity(entityWithExtInfo.getEntity().getGuid())) {
            return;
        }

        if(context.doesTimestampQualify(entityWithExtInfo.getEntity())) {
            context.addToSink(entityWithExtInfo);

            context.result.incrementMeticsCounter(String.format("entity:%s", entityWithExtInfo.getEntity().getTypeName()));
            if (entityWithExtInfo.getReferredEntities() != null) {
                for (AtlasEntity e : entityWithExtInfo.getReferredEntities().values()) {
                    context.result.incrementMeticsCounter(String.format("entity:%s", e.getTypeName()));
                }
            }

            context.result.incrementMeticsCounter("entity:withExtInfo");
        } else {
            List<AtlasEntity> entities = context.getEntitiesWithModifiedTimestamp(entityWithExtInfo);
            for (AtlasEntity e : entities) {
                context.addToSink(new AtlasEntityWithExtInfo(e));
                context.result.incrementMeticsCounter(String.format("entity:%s", e.getTypeName()));
            }
        }

        context.reportProgress();
    }

    private List<Map<String, Object>> executeGremlinQuery(String query, ExportContext context) {
        try {
            return (List<Map<String, Object>>) atlasGraph.executeGremlinScript(context.scriptEngine, context.bindings, query, false);
        } catch (ScriptException e) {
            LOG.error("Script execution failed for query: ", query, e);
            return null;
        }
    }

    public enum TraversalDirection {
        UNKNOWN,
        INWARD,
        OUTWARD,
        BOTH;
    }


    public enum ExportFetchType {
        FULL(FETCH_TYPE_FULL),
        CONNECTED(FETCH_TYPE_CONNECTED),
        INCREMENTAL(FETCH_TYPE_INCREMENTAL);

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

    static class ExportContext {
        private static final int REPORTING_THREASHOLD = 1000;
        private static final String ATLAS_TYPE_HIVE_DB = "hive_db";


        final UniqueList<String>              entityCreationOrder = new UniqueList<>();
        final Set<String>                     guidsProcessed = new HashSet<>();
        final private UniqueList<String>      guidsToProcess = new UniqueList<>();
        final UniqueList<String>              lineageToProcess = new UniqueList<>();
        final Set<String>                     lineageProcessed = new HashSet<>();
        final Map<String, TraversalDirection> guidDirection  = new HashMap<>();
        final Set<String>                     entityTypes         = new HashSet<>();
        final Set<String>                     classificationTypes = new HashSet<>();
        final Set<String>                     structTypes         = new HashSet<>();
        final Set<String>                     enumTypes           = new HashSet<>();
        final Set<String>                     relationshipTypes   = new HashSet<>();
        final AtlasExportResult               result;
        private final ZipSink                 sink;

        private final ScriptEngine        scriptEngine;
        private final Map<String, Object> bindings;
        private final ExportFetchType     fetchType;
        private final boolean             skipLineage;
        private final long                changeMarker;
        private final boolean isHiveDBIncremental;

        private       int                 progressReportCount = 0;

        ExportContext(AtlasGraph atlasGraph, AtlasExportResult result, ZipSink sink) throws AtlasBaseException {
            this.result = result;
            this.sink   = sink;

            scriptEngine = atlasGraph.getGremlinScriptEngine();
            bindings     = new HashMap<>();
            fetchType    = ExportFetchType.from(result.getRequest().getFetchTypeOptionValue());
            skipLineage  = result.getRequest().getSkipLineageOptionValue();
            this.changeMarker = result.getRequest().getChangeTokenFromOptions();
            this.isHiveDBIncremental = checkHiveDBIncrementalSkipLineage(result.getRequest());
        }

        private boolean checkHiveDBIncrementalSkipLineage(AtlasExportRequest request) {
            if(request.getItemsToExport().size() == 0) {
                return false;
            }

            return request.getItemsToExport().get(0).getTypeName().equalsIgnoreCase(ATLAS_TYPE_HIVE_DB) &&
                    request.getFetchTypeOptionValue().equalsIgnoreCase(AtlasExportRequest.FETCH_TYPE_INCREMENTAL) &&
                    request.getSkipLineageOptionValue();
        }

        public List<AtlasEntity> getEntitiesWithModifiedTimestamp(AtlasEntityWithExtInfo entityWithExtInfo) {
            if(fetchType != ExportFetchType.INCREMENTAL) {
                return new ArrayList<>();
            }

            List<AtlasEntity> ret = new ArrayList<>();
            if(doesTimestampQualify(entityWithExtInfo.getEntity())) {
                ret.add(entityWithExtInfo.getEntity());
                return ret;
            }

            for (AtlasEntity entity : entityWithExtInfo.getReferredEntities().values()) {
                if((doesTimestampQualify(entity))) {
                    ret.add(entity);
                }
            }

            return ret;
        }

        public void clear() {
            guidsToProcess.clear();
            guidsProcessed.clear();
            guidDirection.clear();
        }

        public void addToBeProcessed(boolean isSuperTypeProcess, String guid, TraversalDirection direction) {
            if(isSuperTypeProcess) {
                lineageToProcess.add(guid);
            } else {
                guidsToProcess.add(guid);
            }

            guidDirection.put(guid, direction);
        }

        public void reportProgress() {
            if ((guidsProcessed.size() - progressReportCount) > REPORTING_THREASHOLD) {
                progressReportCount = guidsProcessed.size();

                LOG.info("export(): in progress.. number of entities exported: {}", this.guidsProcessed.size());
            }
        }

        public boolean doesTimestampQualify(AtlasEntity entity) {
            if(fetchType != ExportFetchType.INCREMENTAL) {
                return true;
            }

            return changeMarker <= entity.getUpdateTime().getTime();
        }

        public boolean getSkipLineage() {
            return skipLineage;
        }

        public void addToSink(AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
            addToEntityCreationOrder(entityWithExtInfo.getEntity().getGuid());
            sink.add(entityWithExtInfo);
        }

        public boolean isHiveDBIncrementalSkipLineage() {
            return isHiveDBIncremental;
        }

        public void addToEntityCreationOrder(String guid) {
            entityCreationOrder.add(guid);
        }
    }
}
