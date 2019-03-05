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
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.util.UniqueList;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;

import static org.apache.atlas.model.impexp.AtlasExportRequest.*;

@Component
public class ExportService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);

    public static final String PROPERTY_GUID = "__guid";
    private static final String PROPERTY_IS_PROCESS = "isProcess";


    private final AtlasTypeRegistry         typeRegistry;
    private final String QUERY_BINDING_START_GUID = "startGuid";
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
    }

    public AtlasExportResult run(ZipSink exportSink, AtlasExportRequest request, String userName, String hostName,
                                 String requestingIP) throws AtlasBaseException {
        long startTime = System.currentTimeMillis();
        AtlasExportResult result = new AtlasExportResult(request, userName, requestingIP,
                hostName, startTime, getCurrentChangeMarker());

        ExportContext context = new ExportContext(atlasGraph, result, exportSink);
        exportTypeProcessor = new ExportTypeProcessor(typeRegistry, context);

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
        context.addToEntityCreationOrder(context.lineageProcessed);

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

    private void debugLog(String s, Object... params) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug(s, params);
    }

    private List<String> getStartingEntity(AtlasObjectId item, ExportContext context) throws AtlasBaseException {
        List<String> ret = null;

        if(item.getTypeName().equalsIgnoreCase(HdfsPathEntityCreator.HDFS_PATH_TYPE)) {
            hdfsPathEntityCreator.getCreateEntity(item);
        }

        if (StringUtils.isNotEmpty(item.getGuid())) {
            ret = Collections.singletonList(item.getGuid());
        } else if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_FOR_TYPE) && StringUtils.isNotEmpty(item.getTypeName())) {
            ret = getStartingEntityForMatchTypeForType(item, context);
        } else if (StringUtils.isNotEmpty(item.getTypeName()) && MapUtils.isNotEmpty(item.getUniqueAttributes())) {
            ret = getStartingEntityUsingQueryTemplate(item, context, ret);
        }

        if (ret == null) {
            ret = Collections.emptyList();
        }

        logInfoStartingEntitiesFound(item, context, ret);
        return ret;
    }

    private List<String> getStartingEntityUsingQueryTemplate(AtlasObjectId item, ExportContext context, List<String> ret) throws AtlasBaseException {
        final String          queryTemplate = getQueryTemplateForMatchType(context);
        final String          typeName      = item.getTypeName();
        final AtlasEntityType entityType    = typeRegistry.getEntityTypeByName(typeName);

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, typeName);
        }

        for (Map.Entry<String, Object> e : item.getUniqueAttributes().entrySet()) {
            String attrName  = e.getKey();
            Object attrValue = e.getValue();

            AtlasAttribute attribute = entityType.getAttribute(attrName);
            if (attribute == null || attrValue == null) {
                continue;
            }

            setupBindingsForTypeNameAttrNameAttrValue(context, typeName, attrValue, attribute);

            List<String> guids = executeGremlinQueryForGuids(queryTemplate, context);

            if (CollectionUtils.isNotEmpty(guids)) {
                if (ret == null) {
                    ret = new ArrayList<>();
                }

                for (String guid : guids) {
                    if (!ret.contains(guid)) {
                        ret.add(guid);
                    }
                }
            }
        }
        return ret;
    }

    private List<String> getStartingEntityForMatchTypeForType(AtlasObjectId item, ExportContext context) {
        setupBindingsForTypeName(context, item.getTypeName());
        return executeGremlinQueryForGuids(getQueryTemplateForMatchType(context), context);
    }

    private void logInfoStartingEntitiesFound(AtlasObjectId item, ExportContext context, List<String> ret) {
        LOG.info("export(item={}; matchType={}, fetchType={}): found {} entities: options: {}", item,
                context.matchType, context.fetchType, ret.size(), AtlasType.toJson(context.result.getRequest()));
    }

    private void setupBindingsForTypeName(ExportContext context, String typeName) {
        context.bindings.clear();
        context.bindings.put("typeName", new HashSet<String>(Arrays.asList(StringUtils.split(typeName,","))));
    }

    private void setupBindingsForTypeNameAttrNameAttrValue(ExportContext context,
                                                           String typeName, Object attrValue, AtlasAttribute attribute) {
        context.bindings.clear();
        context.bindings.put("typeName", typeName);
        context.bindings.put("attrName", attribute.getQualifiedName());
        context.bindings.put("attrValue", attrValue);
    }

    private String getQueryTemplateForMatchType(ExportContext context) {
        if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_STARTS_WITH)) {
            return gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_STARTS_WITH);
        }

        if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_ENDS_WITH)) {
            return gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_ENDS_WITH);
        }

        if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_CONTAINS)) {
            return gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_CONTAINS);
        }

        if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_MATCHES)) {
            return gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_MATCHES);
        }

        if (StringUtils.equalsIgnoreCase(context.matchType, MATCH_TYPE_FOR_TYPE)) {
            return gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_ALL_FOR_TYPE);
        }

        return gremlinQueryProvider.getQuery(AtlasGremlinQuery.EXPORT_TYPE_DEFAULT);
    }

    private void processEntityGuid(String guid, ExportContext context) throws AtlasBaseException {
        debugLog("==> processEntityGuid({})", guid);

        if (context.guidsProcessed.contains(guid)) {
            return;
        }

        TraversalDirection direction = context.guidDirection.get(guid);
        AtlasEntityWithExtInfo entityWithExtInfo = entityGraphRetriever.toAtlasEntityWithExtInfo(guid);

        processEntity(guid, entityWithExtInfo, context, direction);

        debugLog("<== processEntityGuid({})", guid);
    }

    public void processEntity(String guid, AtlasEntityWithExtInfo entityWithExtInfo,
                               ExportContext context,
                               TraversalDirection direction) throws AtlasBaseException {

        if (!context.lineageProcessed.contains(guid) && context.doesTimestampQualify(entityWithExtInfo.getEntity())) {
            context.addToEntityCreationOrder(entityWithExtInfo.getEntity().getGuid());
        }

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

    private void populateEntitesForIncremental(String topLevelEntityGuid, ExportContext context) throws AtlasBaseException {
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
                context.addToEntityCreationOrder(e.getGuid());
                context.addToSink(new AtlasEntityWithExtInfo(e));
                context.result.incrementMeticsCounter(String.format("entity:%s", e.getTypeName()));
            }
        }

        context.reportProgress();
    }

    private void addTypes(AtlasEntity entity, ExportContext context) {
        addEntityType(entity.getTypeName(), context);

        if(CollectionUtils.isNotEmpty(entity.getClassifications())) {
            for (AtlasClassification c : entity.getClassifications()) {
                addClassificationType(c.getTypeName(), context);
            }
        }
    }

    private void addType(String typeName, ExportContext context) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);

            addType(type, context);
        } catch (AtlasBaseException excp) {
            LOG.error("unknown type {}", typeName);
        }
    }

    private void addEntityType(String typeName, ExportContext context) {
        if (!context.entityTypes.contains(typeName)) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            addEntityType(entityType, context);
        }
    }

    private void addClassificationType(String typeName, ExportContext context) {
        if (!context.classificationTypes.contains(typeName)) {
            AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(typeName);

            addClassificationType(classificationType, context);
        }
    }

    private void addType(AtlasType type, ExportContext context) {
        if (type.getTypeCategory() == TypeCategory.PRIMITIVE) {
            return;
        }

        if (type instanceof AtlasArrayType) {
            AtlasArrayType arrayType = (AtlasArrayType)type;

            addType(arrayType.getElementType(), context);
        } else if (type instanceof AtlasMapType) {
            AtlasMapType mapType = (AtlasMapType)type;

            addType(mapType.getKeyType(), context);
            addType(mapType.getValueType(), context);
        } else if (type instanceof AtlasEntityType) {
            addEntityType((AtlasEntityType)type, context);
        } else if (type instanceof AtlasClassificationType) {
            addClassificationType((AtlasClassificationType)type, context);
        } else if (type instanceof AtlasStructType) {
            addStructType((AtlasStructType)type, context);
        } else if (type instanceof AtlasEnumType) {
            addEnumType((AtlasEnumType)type, context);
        } else if (type instanceof AtlasRelationshipType) {
            addRelationshipType(type.getTypeName(), context);
        }
    }

    private void addEntityType(AtlasEntityType entityType, ExportContext context) {
        if (!context.entityTypes.contains(entityType.getTypeName())) {
            context.entityTypes.add(entityType.getTypeName());

            addAttributeTypes(entityType, context);
            addRelationshipTypes(entityType, context);

            if (CollectionUtils.isNotEmpty(entityType.getAllSuperTypes())) {
                for (String superType : entityType.getAllSuperTypes()) {
                    addEntityType(superType, context);
                }
            }
        }
    }

    private void addClassificationType(AtlasClassificationType classificationType, ExportContext context) {
        if (!context.classificationTypes.contains(classificationType.getTypeName())) {
            context.classificationTypes.add(classificationType.getTypeName());

            addAttributeTypes(classificationType, context);

            if (CollectionUtils.isNotEmpty(classificationType.getAllSuperTypes())) {
                for (String superType : classificationType.getAllSuperTypes()) {
                    addClassificationType(superType, context);
                }
            }
        }
    }

    private void addStructType(AtlasStructType structType, ExportContext context) {
        if (!context.structTypes.contains(structType.getTypeName())) {
            context.structTypes.add(structType.getTypeName());

            addAttributeTypes(structType, context);
        }
    }

    private void addEnumType(AtlasEnumType enumType, ExportContext context) {
        if (!context.enumTypes.contains(enumType.getTypeName())) {
            context.enumTypes.add(enumType.getTypeName());
        }
    }

    private void addRelationshipType(String relationshipTypeName, ExportContext context) {
        if (!context.relationshipTypes.contains(relationshipTypeName)) {
            AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);

            if (relationshipType != null) {
                context.relationshipTypes.add(relationshipTypeName);

                addAttributeTypes(relationshipType, context);
                addEntityType(relationshipType.getEnd1Type(), context);
                addEntityType(relationshipType.getEnd2Type(), context);
            }
        }
    }

    private void addAttributeTypes(AtlasStructType structType, ExportContext context) {
        for (AtlasAttributeDef attributeDef : structType.getStructDef().getAttributeDefs()) {
            addType(attributeDef.getTypeName(), context);
        }
    }

    private void addRelationshipTypes(AtlasEntityType entityType, ExportContext context) {
        for (Map.Entry<String, Map<String, AtlasAttribute>> entry : entityType.getRelationshipAttributes().entrySet()) {
            for (String relationshipType : entry.getValue().keySet()) {
                addRelationshipType(relationshipType, context);
            }
        }
    }

    private List<Map<String, Object>> executeGremlinQuery(String query, ExportContext context) {
        try {
            return (List<Map<String, Object>>) atlasGraph.executeGremlinScript(context.scriptEngine, context.bindings, query, false);
        } catch (ScriptException e) {
            LOG.error("Script execution failed for query: ", query, e);
            return null;
        }
    }

    private List<String> executeGremlinQueryForGuids(String query, ExportContext context) {
        try {
            return (List<String>) atlasGraph.executeGremlinScript(context.scriptEngine, context.bindings, query, false);
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
        private final String              matchType;
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
            matchType    = result.getRequest().getMatchTypeOptionValue();
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
            sink.add(entityWithExtInfo);
        }

        public boolean isHiveDBIncrementalSkipLineage() {
            return isHiveDBIncremental;
        }

        public void addToEntityCreationOrder(String guid) {
            entityCreationOrder.add(guid);
        }

        public void addToEntityCreationOrder(Collection<String> guids) {
            entityCreationOrder.addAll(guids);
        }
    }
}
