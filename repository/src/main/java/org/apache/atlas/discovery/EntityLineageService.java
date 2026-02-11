/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.discovery;


import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.lineage.*;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.lineage.AtlasLineageOnDemandInfo.LineageInfoOnDemand;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.v1.model.lineage.SchemaResponse.SchemaDetails;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.INSTANCE_LINEAGE_QUERY_FAILED;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.*;
import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_DATASET_PROCESS_LINEAGE;
import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.graphdb.AtlasEdgeDirection.IN;
import static org.apache.atlas.repository.graphdb.AtlasEdgeDirection.OUT;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery.*;

@Service
public class EntityLineageService implements AtlasLineageService {
    private static final Logger LOG = LoggerFactory.getLogger(EntityLineageService.class);

    private static final String PROCESS_INPUTS_EDGE = "__Process.inputs";
    private static final String PROCESS_OUTPUTS_EDGE = "__Process.outputs";
    private static final String COLUMNS = "columns";
    private static final boolean LINEAGE_USING_GREMLIN = AtlasConfiguration.LINEAGE_USING_GREMLIN.getBoolean();
    private static final Integer DEFAULT_LINEAGE_MAX_NODE_COUNT       = 9000;
    private static final int     LINEAGE_ON_DEMAND_DEFAULT_DEPTH      = 3;
    private static final String  SEPARATOR                            = "->";
    public static final String IS_DATA_PRODUCT = "isDataProduct";
    public static final String IS_DATASET = "isDataset";

    private final AtlasGraph graph;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasTypeRegistry atlasTypeRegistry;
    private final VertexEdgeCache vertexEdgeCache;

    private static final List<String> FETCH_ENTITY_ATTRIBUTES = Arrays.asList(ATTRIBUTE_NAME_GUID, QUALIFIED_NAME, NAME);
    private static final Map<String, String[]> LINEAGE_MAP = new HashMap<String, String[]>(){{
        put(LINEAGE_TYPE_DATASET_PROCESS_LINEAGE, new String[]{PROCESS_INPUTS_EDGE, PROCESS_OUTPUTS_EDGE});
        put(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE, new String[]{OUTPUT_PORT_PRODUCT_EDGE_LABEL, INPUT_PORT_PRODUCT_EDGE_LABEL});
    }};

    private static String[] getLineageLabelsForType(String lineageType) throws AtlasBaseException {
        String[] lineageLabels = LINEAGE_MAP.get(lineageType);
        if (lineageLabels == null) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    "Invalid lineage type: " + lineageType);
        }
        return lineageLabels;
    }

    @Inject
    EntityLineageService(AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph, VertexEdgeCache vertexEdgeCache, EntityGraphRetriever entityRetriever) {
        this.graph = atlasGraph;
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
        this.entityRetriever = entityRetriever;
        this.atlasTypeRegistry = typeRegistry;
        this.vertexEdgeCache = vertexEdgeCache;
    }

    @VisibleForTesting
    EntityLineageService() {
        this.graph = null;
        this.gremlinQueryProvider = null;
        this.entityRetriever = null;
        this.atlasTypeRegistry = null;
        this.vertexEdgeCache = null;
    }

    @Override
    public AtlasLineageInfo getAtlasLineageInfo(String guid, LineageDirection direction, int depth, boolean hideProcess, int offset, int limit, boolean calculateRemainingVertexCounts) throws AtlasBaseException {
        return getAtlasLineageInfo(new AtlasLineageRequest(guid, depth, direction, hideProcess, offset, limit, calculateRemainingVertexCounts));
    }

    @Override
    @GraphTransaction
    public AtlasLineageInfo getAtlasLineageInfo(AtlasLineageRequest lineageRequest) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getAtlasLineageInfo");

        AtlasLineageInfo ret;
        String guid = lineageRequest.getGuid();
        AtlasLineageContext lineageRequestContext = new AtlasLineageContext(lineageRequest, atlasTypeRegistry);
        RequestContext.get().setRelationAttrsForSearch(lineageRequest.getRelationAttributes());

        RequestContext.get().setLineageInputLabel(PROCESS_INPUTS_EDGE);
        RequestContext.get().setLineageOutputLabel(PROCESS_OUTPUTS_EDGE);

        AtlasVertex entity = AtlasGraphUtilsV2.findByGuid(guid);
        String entityTypeName = entity.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);


        AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName(entityTypeName);

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, entityTypeName);
        }

        boolean isProcess = entityType.getTypeAndAllSuperTypes().contains(PROCESS_SUPER_TYPE);
        if (isProcess) {
            if (lineageRequest.isHideProcess()) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_LINEAGE_ENTITY_TYPE_HIDE_PROCESS, guid, entityTypeName);
            }
            lineageRequestContext.setProcess(true);
        }else {
            boolean isDataSet = entityType.getTypeAndAllSuperTypes().contains(DATA_SET_SUPER_TYPE);
            if (!isDataSet) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_LINEAGE_ENTITY_TYPE, guid, entityTypeName);
            }
            lineageRequestContext.setDataset(true);
        }

        if (LINEAGE_USING_GREMLIN) {
            ret = getLineageInfoV1(lineageRequestContext);
        } else {
            ret = getLineageInfoV2(lineageRequestContext);
        }

        scrubLineageEntities(ret.getGuidEntityMap().values());
        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasLineageInfo getAtlasLineageInfo(String guid, LineageDirection direction, int depth) throws AtlasBaseException {
        return getAtlasLineageInfo(guid, direction, depth, false, -1, -1, false);
    }

    @Override
    @GraphTransaction
    public AtlasLineageOnDemandInfo getAtlasLineageInfo(String guid, LineageOnDemandRequest lineageOnDemandRequest) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getAtlasLineageInfo");

        RequestContext.get().setRelationAttrsForSearch(lineageOnDemandRequest.getRelationAttributes());
        String[] lineageLabels = getLineageLabelsForType(lineageOnDemandRequest.getLineageType());
        RequestContext.get().setLineageInputLabel(lineageLabels[0]);
        RequestContext.get().setLineageOutputLabel(lineageLabels[1]);

        AtlasLineageOnDemandContext atlasLineageOnDemandContext = new AtlasLineageOnDemandContext(lineageOnDemandRequest, atlasTypeRegistry);
        EntityValidationResult entityValidationResult = validateAndGetEntityTypeMap(guid);
        AtlasLineageOnDemandInfo ret = getLineageInfoOnDemand(guid, atlasLineageOnDemandContext, entityValidationResult);
        appendLineageOnDemandPayload(ret, lineageOnDemandRequest);
        // filtering out on-demand relations which has input & output nodes within the limit
        cleanupRelationsOnDemand(ret);
        scrubLineageEntities(ret.getGuidEntityMap().values());
        RequestContext.get().endMetricRecord(metricRecorder);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasLineageListInfo getLineageListInfoOnDemand(String guid, LineageListRequest lineageListRequest) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getLineageListInfoOnDemand");

        String[] lineageLabels = getLineageLabelsForType(lineageListRequest.getLineageType());
        RequestContext.get().setLineageInputLabel(lineageLabels[0]);
        RequestContext.get().setLineageOutputLabel(lineageLabels[1]);
        AtlasLineageListInfo ret = new AtlasLineageListInfo(new ArrayList<>());
        RequestContext.get().setRelationAttrsForSearch(lineageListRequest.getRelationAttributes());

        traverseEdgesUsingBFS(guid, new AtlasLineageListContext(lineageListRequest, atlasTypeRegistry), ret);
        ret.setSearchParameters(lineageListRequest);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    public static class EntityValidationResult {
        public final boolean isProcess;
        public final boolean isDataSet;
        public final boolean isConnection;
        public final boolean isConnectionProcess;
        public final boolean isDataProduct;

        public EntityValidationResult(boolean isProcess, boolean isDataSet, boolean isConnection, boolean isConnectionProcess, boolean isDataProduct) {
            this.isProcess = isProcess;
            this.isDataSet = isDataSet;
            this.isConnection = isConnection;
            this.isConnectionProcess = isConnectionProcess;
            this.isDataProduct = isDataProduct;
        }

        public boolean checkIfConnectorVertex(String lineageType){

            if(lineageType.equals(LINEAGE_TYPE_PRODUCT_ASSET_LINEAGE)){
                return !isDataProduct;
            }

            if(isDataSet || isConnection || isDataProduct){
                return false;
            }
            return true;
        }
    }

    private EntityValidationResult validateAndGetEntityTypeMap(String guid) throws AtlasBaseException {
        String  typeName = entityRetriever.getEntityVertex(guid).getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
        AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName(typeName);
        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, typeName);
        }

        boolean isProcess = entityType.getTypeAndAllSuperTypes().contains(PROCESS_SUPER_TYPE);
        boolean isDataProduct = entityType.getTypeName().equals(DATA_PRODUCT_ENTITY_TYPE);
        boolean isConnectionProcess = false;
        boolean isDataSet  = false;
        boolean isConnection = false;
        if (!isProcess) {
            isConnectionProcess = entityType.getTypeAndAllSuperTypes().contains(CONNECTION_PROCESS_ENTITY_TYPE);
            if(!isConnectionProcess){
                isDataSet = entityType.getTypeAndAllSuperTypes().contains(DATA_SET_SUPER_TYPE);
                if (!isDataSet) {
                    isConnection = entityType.getTypeAndAllSuperTypes().contains(CONNECTION_ENTITY_TYPE);
                    if(!isConnection && !isDataProduct){
                        throw new AtlasBaseException(AtlasErrorCode.INVALID_LINEAGE_ENTITY_TYPE, guid, typeName);
                    }
                }
            }
        }
        return new EntityValidationResult(isProcess, isDataSet, isConnection, isConnectionProcess, isDataProduct);
    }

    private LineageOnDemandConstraints getLineageConstraints(String guid, LineageOnDemandBaseParams defaultParams) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("No lineage on-demand constraints provided for guid: {}, configuring with default values direction: {}, inputRelationsLimit: {}, outputRelationsLimit: {}, depth: {}",
                    guid, BOTH, defaultParams.getInputRelationsLimit(), defaultParams.getOutputRelationsLimit(), LINEAGE_ON_DEMAND_DEFAULT_DEPTH);
        }

        return new LineageOnDemandConstraints(defaultParams);
    }

    private LineageOnDemandConstraints getAndValidateLineageConstraintsByGuid(String guid, AtlasLineageOnDemandContext context) {
        Map<String, LineageOnDemandConstraints> lineageConstraintsMap = context.getConstraints();
        LineageOnDemandBaseParams defaultParams = context.getDefaultParams();

        if (lineageConstraintsMap == null || !lineageConstraintsMap.containsKey(guid)) {
            return getLineageConstraints(guid, defaultParams);
        }

        LineageOnDemandConstraints lineageConstraintsByGuid = lineageConstraintsMap.get(guid);
        if (lineageConstraintsByGuid == null) {
            return getLineageConstraints(guid, defaultParams);
        }

        if (Objects.isNull(lineageConstraintsByGuid.getDirection())) {
            LOG.info("No lineage on-demand direction provided for guid: {}, configuring with default value {}", guid, LineageDirection.BOTH);
            lineageConstraintsByGuid.setDirection(AtlasLineageOnDemandInfo.LineageDirection.BOTH);
        }

        if (lineageConstraintsByGuid.getInputRelationsLimit() < 0) {
            LOG.info("No lineage on-demand constraint inputRelationsLimit provided for guid: {}, configuring with default value {}", guid, context.getDefaultParams().getInputRelationsLimit());
            lineageConstraintsByGuid.setInputRelationsLimit(context.getDefaultParams().getInputRelationsLimit());
        }

        if (lineageConstraintsByGuid.getOutputRelationsLimit() < 0) {
            LOG.info("No lineage on-demand constraint outputRelationsLimit provided for guid: {}, configuring with default value {}", guid, context.getDefaultParams().getOutputRelationsLimit());
            lineageConstraintsByGuid.setOutputRelationsLimit(context.getDefaultParams().getOutputRelationsLimit());
        }

        if (lineageConstraintsByGuid.getDepth() == 0) {
            LOG.info("No lineage on-demand depth provided for guid: {}, configuring with default value {}", guid, LINEAGE_ON_DEMAND_DEFAULT_DEPTH);
            lineageConstraintsByGuid.setDepth(LINEAGE_ON_DEMAND_DEFAULT_DEPTH);
        }

        return lineageConstraintsByGuid;

    }

    private void appendLineageOnDemandPayload(AtlasLineageOnDemandInfo lineageInfo, LineageOnDemandRequest lineageOnDemandRequest) {
        if (lineageInfo == null) {
            return;
        }
        lineageInfo.setLineageOnDemandPayload(lineageOnDemandRequest);
    }

    //Consider only relationsOnDemand which has either more inputs or more outputs than given limit
    private void cleanupRelationsOnDemand(AtlasLineageOnDemandInfo lineageInfo) {
        if (lineageInfo != null && MapUtils.isNotEmpty(lineageInfo.getRelationsOnDemand())) {
            lineageInfo.getRelationsOnDemand().entrySet().removeIf(x ->
                    !(x.getValue().hasMoreInputs() || x.getValue().hasMoreOutputs()
                            || x.getValue().hasUpstream() || x.getValue().hasDownstream()));
        }
    }

    private AtlasLineageOnDemandInfo getLineageInfoOnDemand(String guid, AtlasLineageOnDemandContext atlasLineageOnDemandContext, EntityValidationResult entityValidationResult) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getLineageInfoOnDemand");
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        String lineageType = atlasLineageOnDemandContext.getLineageType();

        LineageOnDemandConstraints lineageConstraintsByGuid = getAndValidateLineageConstraintsByGuid(guid, atlasLineageOnDemandContext);
        AtlasLineageOnDemandInfo.LineageDirection direction = lineageConstraintsByGuid.getDirection();
        int level = 0;
        int depth = lineageConstraintsByGuid.getDepth();
        AtlasLineageOnDemandInfo ret = initializeLineageOnDemandInfo(guid);

        if (depth == 0)
            depth = -1;
        if (!ret.getRelationsOnDemand().containsKey(guid))
            ret.getRelationsOnDemand().put(guid, new LineageInfoOnDemand(lineageConstraintsByGuid));

        AtomicInteger inputEntitiesTraversed = new AtomicInteger(0);
        AtomicInteger outputEntitiesTraversed = new AtomicInteger(0);
        AtomicInteger traversalOrder = new AtomicInteger(1);

        boolean isConnectorVertex = entityValidationResult.checkIfConnectorVertex(atlasLineageOnDemandContext.getLineageType());

        if (!isConnectorVertex) {
            AtlasVertex datasetVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);
            if (direction == AtlasLineageOnDemandInfo.LineageDirection.INPUT || direction == AtlasLineageOnDemandInfo.LineageDirection.BOTH)
                traverseEdgesOnDemand(datasetVertex, true, depth, level, new HashSet<>(), atlasLineageOnDemandContext, ret, guid, inputEntitiesTraversed, traversalOrder);
            if (direction == AtlasLineageOnDemandInfo.LineageDirection.OUTPUT || direction == AtlasLineageOnDemandInfo.LineageDirection.BOTH)
                traverseEdgesOnDemand(datasetVertex, false, depth, level, new HashSet<>(), atlasLineageOnDemandContext, ret, guid, outputEntitiesTraversed, traversalOrder);
            AtlasEntityHeader baseEntityHeader = entityRetriever.toAtlasEntityHeader(datasetVertex, atlasLineageOnDemandContext.getAttributes());
            setGraphTraversalMetadata(level, traversalOrder, baseEntityHeader);
            ret.getGuidEntityMap().put(guid, baseEntityHeader);
        } else {
            AtlasVertex processVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);
            // make one hop to the next dataset vertices from process vertex and traverse with 'depth = depth - 1'
            if (direction == AtlasLineageOnDemandInfo.LineageDirection.INPUT || direction == AtlasLineageOnDemandInfo.LineageDirection.BOTH) {
                Iterator<AtlasEdge> processEdges = processVertex.getEdges(AtlasEdgeDirection.OUT, lineageInputLabel).iterator();
                traverseEdgesOnDemand(processEdges, true, depth, level, atlasLineageOnDemandContext, ret, processVertex, guid, inputEntitiesTraversed, traversalOrder);
            }
            if (direction == AtlasLineageOnDemandInfo.LineageDirection.OUTPUT || direction == AtlasLineageOnDemandInfo.LineageDirection.BOTH) {
                Iterator<AtlasEdge> processEdges = processVertex.getEdges(AtlasEdgeDirection.OUT, lineageOutputLabel).iterator();
                traverseEdgesOnDemand(processEdges, false, depth, level, atlasLineageOnDemandContext, ret, processVertex, guid, outputEntitiesTraversed, traversalOrder);
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private static void setGraphTraversalMetadata(int level, AtomicInteger traversalOrder, AtlasEntityHeader baseEntityHeader) {
        baseEntityHeader.setDepth(level);
        baseEntityHeader.setTraversalOrder(0);
        baseEntityHeader.setFinishTime(traversalOrder.get());
    }

    private void traverseEdgesOnDemand(Iterator<AtlasEdge> processEdges, boolean isInput, int depth, int level, AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasLineageOnDemandInfo ret, AtlasVertex processVertex, String baseGuid, AtomicInteger entitiesTraversed, AtomicInteger traversalOrder) throws AtlasBaseException {
        AtlasLineageOnDemandInfo.LineageDirection direction = isInput ? AtlasLineageOnDemandInfo.LineageDirection.INPUT : AtlasLineageOnDemandInfo.LineageDirection.OUTPUT;
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        int nextLevel = isInput ? level - 1: level + 1;

        while (processEdges.hasNext()) {
            AtlasEdge processEdge = processEdges.next();
            AtlasVertex datasetVertex = processEdge.getInVertex();

            if (!vertexMatchesEvaluation(datasetVertex, atlasLineageOnDemandContext) || !edgeMatchesEvaluation(processEdge, atlasLineageOnDemandContext)) {
                continue;
            }

            if (checkForOffset(processEdge, processVertex, atlasLineageOnDemandContext, ret)) {
                continue;
            }

            boolean isInputEdge  = processEdge.getLabel().equalsIgnoreCase(lineageInputLabel);
            if (incrementAndCheckIfRelationsLimitReached(processEdge, isInputEdge, atlasLineageOnDemandContext, ret, depth, entitiesTraversed, direction, new HashSet<>())) {
                break;
            } else {
                addEdgeToResult(processEdge, ret, atlasLineageOnDemandContext, nextLevel, traversalOrder);
                traversalOrder.incrementAndGet();
            }

            String inGuid = AtlasGraphUtilsV2.getIdFromVertex(datasetVertex);
            LineageOnDemandConstraints inGuidLineageConstrains = getAndValidateLineageConstraintsByGuid(inGuid, atlasLineageOnDemandContext);

            if (!ret.getRelationsOnDemand().containsKey(inGuid)) {
                ret.getRelationsOnDemand().put(inGuid, new LineageInfoOnDemand(inGuidLineageConstrains));
            }

            traverseEdgesOnDemand(datasetVertex, isInput, depth - 1, nextLevel, new HashSet<>(), atlasLineageOnDemandContext, ret, baseGuid, entitiesTraversed, traversalOrder);
        }
    }

    private void traverseEdgesOnDemand(AtlasVertex datasetVertex, boolean isInput, int depth, int level, Set<String> visitedVertices, AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasLineageOnDemandInfo ret, String baseGuid, AtomicInteger entitiesTraversed, AtomicInteger traversalOrder) throws AtlasBaseException {
        if (isEntityTraversalLimitReached(entitiesTraversed))
            return;
        if (depth != 0) { // base condition of recursion for depth
            AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("traverseEdgesOnDemand");
            AtlasLineageOnDemandInfo.LineageDirection direction = isInput ? AtlasLineageOnDemandInfo.LineageDirection.INPUT : AtlasLineageOnDemandInfo.LineageDirection.OUTPUT;
            String lineageInputLabel = RequestContext.get().getLineageInputLabel();
            String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
            int nextLevel = isInput ? level - 1: level + 1;
            // keep track of visited vertices to avoid circular loop
            visitedVertices.add(getId(datasetVertex));

            AtlasPerfMetrics.MetricRecorder traverseEdgesOnDemandGetEdgesIn = RequestContext.get().startMetricRecord("traverseEdgesOnDemandGetEdgesIn");
            Iterator<AtlasEdge> incomingEdges = GraphHelper.getActiveEdges(datasetVertex, isInput ? lineageOutputLabel : lineageInputLabel, IN);
            RequestContext.get().endMetricRecord(traverseEdgesOnDemandGetEdgesIn);

            while (incomingEdges.hasNext()) {
                AtlasEdge incomingEdge = incomingEdges.next();
                AtlasVertex connectorVertex = incomingEdge.getOutVertex();

                if (!vertexMatchesEvaluation(connectorVertex, atlasLineageOnDemandContext) || !edgeMatchesEvaluation(incomingEdge, atlasLineageOnDemandContext)) {
                    continue;
                }

                if (checkForOffset(incomingEdge, datasetVertex, atlasLineageOnDemandContext, ret)) {
                    continue;
                }

                if (incrementAndCheckIfRelationsLimitReached(incomingEdge, !isInput, atlasLineageOnDemandContext, ret, depth, entitiesTraversed, direction, visitedVertices)) {
                    LineageInfoOnDemand entityOnDemandInfo = ret.getRelationsOnDemand().get(baseGuid);
                    if (entityOnDemandInfo == null)
                        continue;
                    if (isInput ? entityOnDemandInfo.isInputRelationsReachedLimit() : entityOnDemandInfo.isOutputRelationsReachedLimit())
                        break;
                    else
                        continue;
                } else {
                    addEdgeToResult(incomingEdge, ret, atlasLineageOnDemandContext, level, traversalOrder);
                }

                AtlasPerfMetrics.MetricRecorder traverseEdgesOnDemandGetEdgesOut = RequestContext.get().startMetricRecord("traverseEdgesOnDemandGetEdgesOut");
                Iterator<AtlasEdge> outgoingEdges = GraphHelper.getActiveEdges(connectorVertex, isInput ? lineageInputLabel : lineageOutputLabel, OUT);
                RequestContext.get().endMetricRecord(traverseEdgesOnDemandGetEdgesOut);

                while (outgoingEdges.hasNext()) {
                    AtlasEdge outgoingEdge = outgoingEdges.next();
                    AtlasVertex entityVertex = outgoingEdge.getInVertex();

                    if (!vertexMatchesEvaluation(entityVertex, atlasLineageOnDemandContext) || !edgeMatchesEvaluation(outgoingEdge, atlasLineageOnDemandContext)) {
                        continue;
                    }

                    if (checkForOffset(outgoingEdge, connectorVertex, atlasLineageOnDemandContext, ret)) {
                        continue;
                    }
                    if (incrementAndCheckIfRelationsLimitReached(outgoingEdge, isInput, atlasLineageOnDemandContext, ret, depth, entitiesTraversed, direction, visitedVertices)) {
                        String processGuid = AtlasGraphUtilsV2.getIdFromVertex(connectorVertex);
                        LineageInfoOnDemand entityOnDemandInfo = ret.getRelationsOnDemand().get(processGuid);
                        if (entityOnDemandInfo == null)
                            continue;
                        if (isInput ? entityOnDemandInfo.isInputRelationsReachedLimit() : entityOnDemandInfo.isOutputRelationsReachedLimit())
                            break;
                        else
                            continue;
                    } else {
                        addEdgeToResult(outgoingEdge, ret, atlasLineageOnDemandContext, nextLevel, traversalOrder);
                        entitiesTraversed.incrementAndGet();
                        traversalOrder.incrementAndGet();
                        if (isEntityTraversalLimitReached(entitiesTraversed))
                            setEntityLimitReachedFlag(isInput, ret);
                    }
                    if (entityVertex != null && !visitedVertices.contains(getId(entityVertex))) {
                        String entityGuid = AtlasGraphUtilsV2.getIdFromVertex(entityVertex);
                        LineageOnDemandConstraints entityLineageConstraints = getAndValidateLineageConstraintsByGuid(entityGuid, atlasLineageOnDemandContext);

                        if (!ret.getRelationsOnDemand().containsKey(entityGuid)) {
                            ret.getRelationsOnDemand().put(entityGuid, new LineageInfoOnDemand(entityLineageConstraints));
                        }

                        traverseEdgesOnDemand(entityVertex, isInput, depth - 1, nextLevel, visitedVertices, atlasLineageOnDemandContext, ret, baseGuid, entitiesTraversed, traversalOrder);
                        AtlasEntityHeader traversedEntity = ret.getGuidEntityMap().get(AtlasGraphUtilsV2.getIdFromVertex(entityVertex));
                        traversedEntity.setFinishTime(traversalOrder.get());
                    }
                }
            }

            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static void setEntityLimitReachedFlag(boolean isInput, AtlasLineageOnDemandInfo ret) {
        if (isInput)
            ret.setUpstreamEntityLimitReached(true);
        else
            ret.setDownstreamEntityLimitReached(true);
    }

    private void traverseEdgesUsingBFS(String baseGuid, AtlasLineageListContext lineageListContext, AtlasLineageListInfo ret) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("traverseEdgesUsingBFS");

        String lineageType = lineageListContext.getLineageType();
        Set<String> visitedVertices = new HashSet<>();
        visitedVertices.add(baseGuid);
        Set<String> skippedVertices = new HashSet<>();
        Queue<String> traversalQueue = new LinkedList<>();

        Map<String, List<String>> lineageParentsForEntityMap = new HashMap<>();  // New map to track parent nodes
        Map<String, List<String>> lineageChildrenForEntityMap = new HashMap<>();  // New map to track parent nodes


        AtlasVertex baseVertex = AtlasGraphUtilsV2.findByGuid(this.graph, baseGuid);
        EntityValidationResult entityValidationResult = validateAndGetEntityTypeMap(baseGuid);

        boolean isNotConnectorVertex = !entityValidationResult.checkIfConnectorVertex(lineageType);
        // Get the neighbors for the current node
        enqueueNeighbours(baseVertex, entityValidationResult, lineageListContext, traversalQueue, visitedVertices, skippedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);
        int currentDepth = 0;
        int currentLevel = isNotConnectorVertex? 0: 1;

        if(lineageListContext.getImmediateNeighbours()){
            // Add the current node and its neighbors to the result
            appendToResult(baseVertex, lineageListContext, ret, currentLevel);
        }

        while (!traversalQueue.isEmpty() && !lineageListContext.isEntityLimitReached() && currentDepth < lineageListContext.getDepth()) {
            currentDepth++;

            // update level at every alternate depth
            if ((isNotConnectorVertex && currentDepth % 2 != 0) || (!isNotConnectorVertex && currentDepth % 2 == 0))
                currentLevel++;

            int entitiesInCurrentDepth = traversalQueue.size();
            for (int i = 0; i < entitiesInCurrentDepth; i++) {
                if (lineageListContext.isEntityLimitReached())
                    break;

                String currentGUID = traversalQueue.poll();
                AtlasVertex currentVertex = AtlasGraphUtilsV2.findByGuid(this.graph, currentGUID);
                if (Objects.isNull(currentVertex))
                    throw new AtlasBaseException("Found null vertex during lineage graph traversal for guid: " + currentGUID);

                EntityValidationResult currentEntityValidationResult = validateAndGetEntityTypeMap(currentGUID);
                if (!lineageListContext.evaluateVertexFilter(currentVertex)) {
                    enqueueNeighbours(currentVertex, currentEntityValidationResult, lineageListContext, traversalQueue, visitedVertices, skippedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);
                    continue;
                }
                if (checkOffsetAndSkipEntity(lineageListContext, ret)) {
                    skippedVertices.add(currentGUID);
                    enqueueNeighbours(currentVertex, currentEntityValidationResult, lineageListContext, traversalQueue, visitedVertices, skippedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);
                    continue;
                }

                lineageListContext.incrementEntityCount();
                // Get the neighbors for the current node
                enqueueNeighbours(currentVertex, currentEntityValidationResult, lineageListContext, traversalQueue, visitedVertices, skippedVertices, lineageParentsForEntityMap, lineageChildrenForEntityMap);

                // Add the current node and its neighbors to the result
                appendToResult(currentVertex, lineageListContext, ret, currentLevel);

                if (isLastEntityInLastDepth(lineageListContext.getDepth(), currentDepth, entitiesInCurrentDepth, i)) {
                    ret.setHasMore(false);
                    lineageListContext.setHasMoreUpdated(true);
                }
            }
        }

        if(lineageListContext.getImmediateNeighbours()){
            // update parents for each entity
            updateNeighbourNodesForEachEntity(lineageListContext, ret, lineageParentsForEntityMap, lineageChildrenForEntityMap);
        }

        if (currentDepth > lineageListContext.getDepth())
            lineageListContext.setDepthLimitReached(true);

        setPageMetadata(lineageListContext, ret, traversalQueue);
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void enqueueNeighbours(AtlasVertex currentVertex, EntityValidationResult entityValidationResult,
                                   AtlasLineageListContext lineageListContext, Queue<String> traversalQueue,
                                   Set<String> visitedVertices, Set<String> skippedVertices,
                                   Map<String, List<String>> lineageParentsForEntityMap, Map<String, List<String>> lineageChildrenForEntityMap) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder traverseEdgesOnDemandGetEdges = RequestContext.get().startMetricRecord("traverseEdgesOnDemandGetEdges");
        Iterator<AtlasEdge> edges;

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        String lineageType = lineageListContext.getLineageType();
        boolean isConnectorVertex =  entityValidationResult.checkIfConnectorVertex(lineageType);
        if (!isConnectorVertex)
            edges = GraphHelper.getActiveEdges(currentVertex, isInputDirection(lineageListContext) ? lineageOutputLabel : lineageInputLabel, IN);
        else
            edges = GraphHelper.getActiveEdges(currentVertex, isInputDirection(lineageListContext) ? lineageInputLabel : lineageOutputLabel, OUT);

        RequestContext.get().endMetricRecord(traverseEdgesOnDemandGetEdges);
        while (edges.hasNext()) {
            AtlasEdge currentEdge = edges.next();
            if (!lineageListContext.evaluateTraversalFilter(currentEdge))
                continue;
            AtlasVertex neighbourVertex;
            if (!isConnectorVertex)
                neighbourVertex = currentEdge.getOutVertex();
            else
                neighbourVertex = currentEdge.getInVertex();

            String vertexGuid = getGuid(neighbourVertex);
            if (StringUtils.isEmpty(vertexGuid) || !lineageListContext.evaluateTraversalFilter(neighbourVertex))
                continue;

            if (!skippedVertices.contains(vertexGuid) && !visitedVertices.contains(vertexGuid)) {
                visitedVertices.add(vertexGuid);
                traversalQueue.add(vertexGuid);
                addEntitiesToCache(neighbourVertex);
            }

            if(lineageListContext.getImmediateNeighbours()){
                lineageParentsForEntityMap
                        .computeIfAbsent(vertexGuid, k -> new ArrayList<>())
                        .add(getGuid(currentVertex));
                lineageChildrenForEntityMap
                        .computeIfAbsent(getGuid(currentVertex), k -> new ArrayList<>())
                        .add(vertexGuid);
            }
        }
    }

    private void updateNeighbourNodesForEachEntity(AtlasLineageListContext lineageListContext, AtlasLineageListInfo ret,
                                                   Map<String, List<String>> lineageParentsForEntityMap,
                                                   Map<String, List<String>> lineageChildrenForEntityMap) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("updateNeighbourNodesForEachEntity");
        List<AtlasEntityHeader> entityList = ret.getEntities();
        if (entityList == null) return;

        for (AtlasEntityHeader entity : entityList) {
            if (entity == null || entity.getGuid() == null) continue;

            updateLineageForEntity(entity, lineageParentsForEntityMap, true, lineageListContext);
            updateLineageForEntity(entity, lineageChildrenForEntityMap, false, lineageListContext);
        }
        RequestContext.get().endMetricRecord(metric);
    }

    private void updateLineageForEntity(AtlasEntityHeader entity, Map<String, List<String>> lineageMap,
                                        boolean isParentMap, AtlasLineageListContext lineageListContext) {
        List<String> relatedProcessNodes = lineageMap.get(entity.getGuid());
        if (relatedProcessNodes == null) return;

        Set<String> seenGuids = new HashSet<>();
        List<Map<String, String>> relatedDatasetNodes = new ArrayList<>();

        for (String node : relatedProcessNodes) {
            List<String> subNodes = lineageMap.get(node);
            if (subNodes == null) continue;

            for (String subNode : subNodes) {
                AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(this.graph, subNode);
                if (vertex != null && seenGuids.add(subNode)) {
                    Map<String, String> details = fetchAttributes(vertex, FETCH_ENTITY_ATTRIBUTES);
                    relatedDatasetNodes.add(details);
                }
            }
        }

        if (isParentMap) {
            if (isInputDirection(lineageListContext)) {
                entity.setImmediateDownstream(relatedDatasetNodes);
            } else {
                entity.setImmediateUpstream(relatedDatasetNodes);
            }
        } else {
            if (isInputDirection(lineageListContext)) {
                entity.setImmediateUpstream(relatedDatasetNodes);
            } else {
                entity.setImmediateDownstream(relatedDatasetNodes);
            }
        }
    }

    private void appendToResult(AtlasVertex currentVertex, AtlasLineageListContext lineageListContext,
                                AtlasLineageListInfo ret, int currentLevel) throws AtlasBaseException {
        AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(currentVertex, lineageListContext.getAttributes());
        entity.setDepth(currentLevel);
        ret.getEntities().add(entity);
    }

    private static void addEntitiesToCache(AtlasVertex vertex) {
        GraphTransactionInterceptor.addToVertexCache(getGuid(vertex), vertex);
    }

    private static void setPageMetadata(AtlasLineageListContext lineageListContext, AtlasLineageListInfo ret, Queue<String> traversalQueue) {
        if (!lineageListContext.isHasMoreUpdated())
            updateHasMore(lineageListContext, ret, traversalQueue);
        ret.setEntityCount(lineageListContext.getCurrentEntityCounter());
    }

    private static void updateHasMore(AtlasLineageListContext lineageListContext, AtlasLineageListInfo ret, Queue<String> traversalQueue) {
        if (!traversalQueue.isEmpty())
            ret.setHasMore(true);
        if (lineageListContext.isDepthLimitReached())
            ret.setHasMore(false);
    }

    private static boolean isLastEntityInLastDepth(int lastDepth, int currentDepth, int entitiesInCurrentDepth, int entityIndexInCurrentDepth) {
        return entityIndexInCurrentDepth == entitiesInCurrentDepth - 1 && currentDepth == lastDepth;
    }

    private static boolean isInputDirection(AtlasLineageListContext lineageListContext) {
        return LineageListRequest.LineageDirection.INPUT.equals(lineageListContext.getDirection());
    }

    private boolean checkForOffset(AtlasEdge atlasEdge, AtlasVertex entityVertex, AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasLineageOnDemandInfo ret) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("checkForOffset");
        try {
            String entityGuid = getGuid(entityVertex);
            LineageOnDemandConstraints entityConstraints = getAndValidateLineageConstraintsByGuid(entityGuid, atlasLineageOnDemandContext);
            LineageInfoOnDemand entityLineageInfo = ret.getRelationsOnDemand().containsKey(entityGuid) ? ret.getRelationsOnDemand().get(entityGuid) : new LineageInfoOnDemand(entityConstraints);

            if (entityConstraints.getFrom() != 0 && entityLineageInfo.getFromCounter() < entityConstraints.getFrom()) {
                if (! lineageContainsSkippedEdgeV2(ret, atlasEdge)) {
                    addEdgeToSkippedEdges(ret, atlasEdge);
                    entityLineageInfo.incrementFromCounter();
                }
                return true;
            }
            return false;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private boolean checkOffsetAndSkipEntity(AtlasLineageListContext atlasLineageListContext, AtlasLineageListInfo ret) {
        if (atlasLineageListContext.getFrom() != 0 && atlasLineageListContext.getCurrentFromCounter() < atlasLineageListContext.getFrom()) {
            atlasLineageListContext.incrementCurrentFromCounter();
            return true;
        }
        return false;
    }

    private static String getId(AtlasVertex vertex) {
        return vertex.getIdForDisplay();
    }

    private boolean incrementAndCheckIfRelationsLimitReached(AtlasEdge atlasEdge, boolean isInput, AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasLineageOnDemandInfo ret, int depth, AtomicInteger entitiesTraversed, AtlasLineageOnDemandInfo.LineageDirection direction, Set<String> visitedVertices) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("incrementAndCheckIfRelationsLimitReached");

        AtlasVertex                inVertex                 = isInput ? atlasEdge.getOutVertex() : atlasEdge.getInVertex();
        String                     inGuid                   = AtlasGraphUtilsV2.getIdFromVertex(inVertex);
        LineageOnDemandConstraints inGuidLineageConstraints = getAndValidateLineageConstraintsByGuid(inGuid, atlasLineageOnDemandContext);

        AtlasVertex                outVertex                 = isInput ? atlasEdge.getInVertex() : atlasEdge.getOutVertex();
        String                     outGuid                   = AtlasGraphUtilsV2.getIdFromVertex(outVertex);
        LineageOnDemandConstraints outGuidLineageConstraints = getAndValidateLineageConstraintsByGuid(outGuid, atlasLineageOnDemandContext);

        LineageInfoOnDemand inLineageInfo = ret.getRelationsOnDemand().containsKey(inGuid) ? ret.getRelationsOnDemand().get(inGuid) : new LineageInfoOnDemand(inGuidLineageConstraints);
        LineageInfoOnDemand outLineageInfo = ret.getRelationsOnDemand().containsKey(outGuid) ? ret.getRelationsOnDemand().get(outGuid) : new LineageInfoOnDemand(outGuidLineageConstraints);

        setHorizontalPaginationFlags(isInput, atlasLineageOnDemandContext, ret, depth, entitiesTraversed, inVertex, inGuid, outVertex, outGuid, inLineageInfo, outLineageInfo, visitedVertices);

        boolean hasRelationsLimitReached = setVerticalPaginationFlags(entitiesTraversed, inLineageInfo, outLineageInfo);
        if (!hasRelationsLimitReached) {
            ret.getRelationsOnDemand().put(inGuid, inLineageInfo);
            ret.getRelationsOnDemand().put(outGuid, outLineageInfo);
        }
        RequestContext.get().endMetricRecord(metricRecorder);

        return hasRelationsLimitReached;
    }

    private boolean setVerticalPaginationFlags(AtomicInteger entitiesTraversed, LineageInfoOnDemand inLineageInfo, LineageInfoOnDemand outLineageInfo) {
        boolean hasRelationsLimitReached = false;
        if (inLineageInfo.isInputRelationsReachedLimit() || outLineageInfo.isOutputRelationsReachedLimit() || isEntityTraversalLimitReached(entitiesTraversed)) {
            inLineageInfo.setHasMoreInputs(true);
            outLineageInfo.setHasMoreOutputs(true);
            hasRelationsLimitReached = true;
        }

        if (!hasRelationsLimitReached) {
            inLineageInfo.incrementInputRelationsCount();
            outLineageInfo.incrementOutputRelationsCount();
        }
        return hasRelationsLimitReached;
    }

    private void setHorizontalPaginationFlags(boolean isInput, AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasLineageOnDemandInfo ret, int depth, AtomicInteger entitiesTraversed, AtlasVertex inVertex, String inGuid, AtlasVertex outVertex, String outGuid, LineageInfoOnDemand inLineageInfo, LineageInfoOnDemand outLineageInfo, Set<String> visitedVertices) {
        boolean isOutVertexVisited = visitedVertices.contains(getId(outVertex));
        boolean isInVertexVisited = visitedVertices.contains(getId(inVertex));
        if (depth == 1 || entitiesTraversed.get() == getLineageMaxNodeAllowedCount()-1) { // is the vertex a leaf?
            if (isInput && ! isOutVertexVisited)
                setHasUpstream(atlasLineageOnDemandContext, outVertex, outLineageInfo);
            else if (!isInput && ! isInVertexVisited)
                setHasDownstream(atlasLineageOnDemandContext, inVertex, inLineageInfo);
        }
    }

    private void setHasDownstream(AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasVertex inVertex, LineageInfoOnDemand inLineageInfo) {
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        List<AtlasEdge> filteredEdges = getFilteredAtlasEdges(inVertex, IN, lineageInputLabel, atlasLineageOnDemandContext);
        if (!filteredEdges.isEmpty()) {
            inLineageInfo.setHasDownstream(true);
            inLineageInfo.setTotalOutputRelationsCount(filteredEdges.size());
        }
    }

    private void setHasUpstream(AtlasLineageOnDemandContext atlasLineageOnDemandContext, AtlasVertex outVertex, LineageInfoOnDemand outLineageInfo) {
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        List<AtlasEdge> filteredEdges = getFilteredAtlasEdges(outVertex, IN, lineageOutputLabel, atlasLineageOnDemandContext);
        if (!filteredEdges.isEmpty()) {
            outLineageInfo.setHasUpstream(true);
            outLineageInfo.setTotalInputRelationsCount(filteredEdges.size());
        }
    }

    private List<AtlasEdge> getFilteredAtlasEdges(AtlasVertex outVertex, AtlasEdgeDirection direction, String processEdgeLabel, AtlasLineageOnDemandContext atlasLineageOnDemandContext) {
        List<AtlasEdge> filteredEdges = new ArrayList<>();
        Iterable<AtlasEdge> edges = outVertex.getEdges(direction, processEdgeLabel);
        for (AtlasEdge edge : edges) {
            if (edgeMatchesEvaluation(edge, atlasLineageOnDemandContext)) {
                filteredEdges.add(edge);
            }
        }
        return filteredEdges;
    }

    private boolean isEntityTraversalLimitReached(AtomicInteger entitiesTraversed) {
        return entitiesTraversed.get() >= getLineageMaxNodeAllowedCount();
    }

    @Override
    @GraphTransaction
    public SchemaDetails getSchemaForHiveTableByName(final String datasetName) throws AtlasBaseException {
        if (StringUtils.isEmpty(datasetName)) {
            // TODO: Complete error handling here
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST);
        }

        AtlasEntityType hive_table = atlasTypeRegistry.getEntityTypeByName("hive_table");

        Map<String, Object> lookupAttributes = new HashMap<>();
        lookupAttributes.put("qualifiedName", datasetName);
        String guid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(hive_table, lookupAttributes);

        return getSchemaForHiveTableByGuid(guid);
    }

    @Override
    @GraphTransaction
    public SchemaDetails getSchemaForHiveTableByGuid(final String guid) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST);
        }
        SchemaDetails ret = new SchemaDetails();
        AtlasEntityType hive_column = atlasTypeRegistry.getEntityTypeByName("hive_column");

        ret.setDataType(AtlasTypeUtil.toClassTypeDefinition(hive_column));

        AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(guid);
        AtlasEntity entity = entityWithExtInfo.getEntity();

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(atlasTypeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(entity)),
                "read entity schema: guid=", guid);

        Map<String, AtlasEntity> referredEntities = entityWithExtInfo.getReferredEntities();
        List<String> columnIds = getColumnIds(entity);

        if (MapUtils.isNotEmpty(referredEntities)) {
            List<Map<String, Object>> rows = referredEntities.entrySet()
                    .stream()
                    .filter(e -> isColumn(columnIds, e))
                    .map(e -> AtlasTypeUtil.toMap(e.getValue()))
                    .collect(Collectors.toList());
            ret.setRows(rows);
        }

        return ret;
    }

    private void scrubLineageEntities(Collection<AtlasEntityHeader> entityHeaders) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("scrubLineageEntities");

        AtlasSearchResult searchResult = new AtlasSearchResult();
        searchResult.setEntities(new ArrayList<>(entityHeaders));
        AtlasSearchResultScrubRequest request = new AtlasSearchResultScrubRequest(atlasTypeRegistry, searchResult);

        AtlasAuthorizationUtils.scrubSearchResults(request, true);
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private List<String> getColumnIds(AtlasEntity entity) {
        List<String> ret = new ArrayList<>();
        Object columnObjs = entity.getAttribute(COLUMNS);

        if (columnObjs instanceof List) {
            for (Object pkObj : (List) columnObjs) {
                if (pkObj instanceof AtlasObjectId) {
                    ret.add(((AtlasObjectId) pkObj).getGuid());
                }
            }
        }

        return ret;
    }

    private boolean isColumn(List<String> columnIds, Map.Entry<String, AtlasEntity> e) {
        return columnIds.contains(e.getValue().getGuid());
    }

    private AtlasLineageInfo getLineageInfoV1(AtlasLineageContext lineageContext) throws AtlasBaseException {
        AtlasLineageInfo ret;
        LineageDirection direction = lineageContext.getDirection();

        if (direction.equals(INPUT)) {
            ret = getLineageInfo(lineageContext, INPUT);
        } else if (direction.equals(OUTPUT)) {
            ret = getLineageInfo(lineageContext, OUTPUT);
        } else {
            ret = getBothLineageInfoV1(lineageContext);
        }

        return ret;
    }

    private AtlasLineageInfo getLineageInfo(AtlasLineageContext lineageContext, LineageDirection direction) throws AtlasBaseException {
        int depth = lineageContext.getDepth();
        String guid = lineageContext.getGuid();
        boolean isDataSet = lineageContext.isDataset();

        final Map<String, Object> bindings = new HashMap<>();
        String lineageQuery = getLineageQuery(guid, direction, depth, isDataSet, bindings);
        List results = executeGremlinScript(bindings, lineageQuery);
        Map<String, AtlasEntityHeader> entities = new HashMap<>();
        Set<LineageRelation> relations = new HashSet<>();

        if (CollectionUtils.isNotEmpty(results)) {
            for (Object result : results) {
                if (result instanceof Map) {
                    for (final Object o : ((Map) result).entrySet()) {
                        final Map.Entry entry = (Map.Entry) o;
                        Object value = entry.getValue();

                        if (value instanceof List) {
                            for (Object elem : (List) value) {
                                if (elem instanceof AtlasEdge) {
                                    processEdge((AtlasEdge) elem, entities, relations, lineageContext);
                                } else {
                                    LOG.warn("Invalid value of type {} found, ignoring", (elem != null ? elem.getClass().getSimpleName() : "null"));
                                }
                            }
                        } else if (value instanceof AtlasEdge) {
                            processEdge((AtlasEdge) value, entities, relations, lineageContext);
                        } else {
                            LOG.warn("Invalid value of type {} found, ignoring", (value != null ? value.getClass().getSimpleName() : "null"));
                        }
                    }
                } else if (result instanceof AtlasEdge) {
                    processEdge((AtlasEdge) result, entities, relations, lineageContext);
                }
            }
        }

        return new AtlasLineageInfo(guid, entities, relations, direction, depth, -1, -1);
    }

    private AtlasLineageInfo getLineageInfoV2(AtlasLineageContext lineageContext) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getLineageInfoV2");

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        int depth = lineageContext.getDepth();
        String guid = lineageContext.getGuid();
        LineageDirection direction = lineageContext.getDirection();

        AtlasLineageInfo ret = initializeLineageInfo(guid, direction, depth, lineageContext.getLimit(), lineageContext.getOffset());

        if (depth == 0) {
            depth = -1;
        }

        if (lineageContext.isDataset()) {
            AtlasVertex datasetVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);
            lineageContext.setStartDatasetVertex(datasetVertex);

            if (direction == INPUT || direction == BOTH) {
                traverseEdges(datasetVertex, true, depth, new HashSet<>(), ret, lineageContext);
            }

            if (direction == OUTPUT || direction == BOTH) {
                traverseEdges(datasetVertex, false, depth, new HashSet<>(), ret, lineageContext);
            }
        } else {
            AtlasVertex processVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

            // make one hop to the next dataset vertices from process vertex and traverse with 'depth = depth - 1'
            if (direction == INPUT || direction == BOTH) {
                Iterator<AtlasEdge> processEdges = vertexEdgeCache.getEdges(processVertex, OUT, lineageInputLabel).iterator();

                List<AtlasEdge> qualifyingEdges = getQualifyingProcessEdges(processEdges, lineageContext);
                ret.setHasChildrenForDirection(getGuid(processVertex), new LineageChildrenInfo(INPUT, hasMoreChildren(qualifyingEdges)));

                for (AtlasEdge processEdge : qualifyingEdges) {
                    addEdgeToResult(processEdge, ret, lineageContext);

                    AtlasVertex datasetVertex = processEdge.getInVertex();

                    traverseEdges(datasetVertex, true, depth - 1, new HashSet<>(), ret, lineageContext);
                }
            }

            if (direction == OUTPUT || direction == BOTH) {
                Iterator<AtlasEdge> processEdges = vertexEdgeCache.getEdges(processVertex, OUT, lineageOutputLabel).iterator();

                List<AtlasEdge> qualifyingEdges = getQualifyingProcessEdges(processEdges, lineageContext);
                ret.setHasChildrenForDirection(getGuid(processVertex), new LineageChildrenInfo(OUTPUT, hasMoreChildren(qualifyingEdges)));

                for (AtlasEdge processEdge : qualifyingEdges) {
                    addEdgeToResult(processEdge, ret, lineageContext);

                    AtlasVertex datasetVertex = processEdge.getInVertex();

                    traverseEdges(datasetVertex, false, depth - 1, new HashSet<>(), ret, lineageContext);
                }
            }
        }
        RequestContext.get().endMetricRecord(metric);

        return ret;
    }

    private List<AtlasEdge> getQualifyingProcessEdges(Iterator<AtlasEdge> processEdges, AtlasLineageContext lineageContext) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getQualifyingProcessEdges");
        List<AtlasEdge> qualifyingEdges = new ArrayList<>();
        while (processEdges.hasNext()) {
            AtlasEdge processEdge = processEdges.next();
            if (shouldProcessEdge(lineageContext, processEdge) && lineageContext.evaluate(processEdge.getInVertex())) {
                qualifyingEdges.add(processEdge);
            }
        }
        RequestContext.get().endMetricRecord(metric);
        return qualifyingEdges;
    }

    private void addEdgeToResult(AtlasEdge edge, AtlasLineageInfo lineageInfo,
                                 AtlasLineageContext requestContext) throws AtlasBaseException {
        if (!lineageContainsEdge(lineageInfo, edge)) {
            processEdge(edge, lineageInfo, requestContext);
        }
    }

    private void addEdgeToResult(AtlasEdge edge, AtlasLineageOnDemandInfo lineageInfo, AtlasLineageOnDemandContext atlasLineageOnDemandContext, int level, AtomicInteger traversalOrder) throws AtlasBaseException {
        if (!lineageContainsVisitedEdgeV2(lineageInfo, edge)) {
            processEdge(edge, lineageInfo, atlasLineageOnDemandContext, level, traversalOrder);
        }
    }

    private int getLineageMaxNodeAllowedCount() {
        return AtlasConfiguration.LINEAGE_MAX_NODE_COUNT.getInt();
    }

    private String getEdgeLabel(AtlasEdge edge) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getEdgeLabel");
        try {
            String lineageInputLabel = RequestContext.get().getLineageInputLabel();
            AtlasVertex inVertex     = edge.getInVertex();
            AtlasVertex outVertex    = edge.getOutVertex();
            String      inGuid       = AtlasGraphUtilsV2.getIdFromVertex(inVertex);
            String      outGuid      = AtlasGraphUtilsV2.getIdFromVertex(outVertex);
            String      relationGuid = AtlasGraphUtilsV2.getEncodedProperty(edge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
            boolean     isInputEdge  = edge.getLabel().equalsIgnoreCase(lineageInputLabel);

            if (isLineageOnDemandEnabled()) {
                return getEdgeLabelFromGuids(isInputEdge, inGuid, outGuid);
            }
            return relationGuid;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private String getEdgeLabelFromGuids(boolean isInputEdge, String inGuid, String outGuid) {
        return isInputEdge ? inGuid + SEPARATOR + outGuid : outGuid + SEPARATOR + inGuid;
    }

    private boolean hasMoreChildren(List<AtlasEdge> edges) {
        return edges.stream().anyMatch(edge -> getStatus(edge) == AtlasEntity.Status.ACTIVE);
    }

    private void traverseEdges(AtlasVertex currentVertex, boolean isInput, int depth, Set<String> visitedVertices, AtlasLineageInfo ret,
                               AtlasLineageContext lineageContext) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("traverseEdges");
        if (depth != 0) {
            processIntermediateLevel(currentVertex, isInput, depth, visitedVertices, ret, lineageContext);
        } else {
            processLastLevel(currentVertex, isInput, ret, lineageContext);
        }
        RequestContext.get().endMetricRecord(metric);
    }

    private void processIntermediateLevel(AtlasVertex currentVertex,
                                          boolean isInput,
                                          int depth,
                                          Set<String> visitedVertices,
                                          AtlasLineageInfo ret,
                                          AtlasLineageContext lineageContext) throws AtlasBaseException {
        // keep track of visited vertices to avoid circular loop
        visitedVertices.add(currentVertex.getIdForDisplay());

        if (!vertexMatchesEvaluation(currentVertex, lineageContext)) {
            return;
        }
        List<AtlasEdge> currentVertexEdges = getEdgesOfCurrentVertex(currentVertex, isInput, lineageContext);
        if (lineageContext.shouldApplyPagination()) {
            if (lineageContext.isCalculateRemainingVertexCounts()) {
                calculateRemainingVertexCounts(currentVertex, isInput, ret);
            }
            addPaginatedVerticesToResult(isInput, depth, visitedVertices, ret, lineageContext, currentVertexEdges, currentVertex);
        } else {
            addLimitlessVerticesToResult(isInput, depth, visitedVertices, ret, lineageContext, currentVertexEdges);
        }
    }

    private void calculateRemainingVertexCounts(AtlasVertex currentVertex, boolean isInput, AtlasLineageInfo ret) {
        if (isInput) {
            Long totalUpstreamVertexCount = getTotalUpstreamVertexCount(getGuid(currentVertex));
            ret.calculateRemainingUpstreamVertexCount(totalUpstreamVertexCount);
        } else {
            Long totalDownstreamVertexCount = getTotalDownstreamVertexCount(getGuid(currentVertex));
            ret.calculateRemainingDownstreamVertexCount(totalDownstreamVertexCount);
        }
    }

    private Long getTotalDownstreamVertexCount(String guid) {
        return (Long) graph
                .V()
                .has("__guid", guid)
                .inE("__Process.inputs").has("__state", "ACTIVE")
                .outV().has("__state", "ACTIVE")
                .outE("__Process.outputs").has("__state", "ACTIVE")
                .inV()
                .count()
                .next();

    }

    private Long getTotalUpstreamVertexCount(String guid) {
        return (Long) graph
                .V()
                .has("__guid", guid)
                .outE("__Process.outputs").has("__state", "ACTIVE")
                .inV().has("__state", "ACTIVE")
                .inE("__Process.inputs").has("__state", "ACTIVE")
                .inV()
                .count()
                .next();
    }

    private void addPaginatedVerticesToResult(boolean isInput,
                                              int depth,
                                              Set<String> visitedVertices,
                                              AtlasLineageInfo ret,
                                              AtlasLineageContext lineageContext,
                                              List<AtlasEdge> currentVertexEdges,
                                              AtlasVertex currentVertex) throws AtlasBaseException {
        Set<Pair<String, String>> paginationCalculatedProcessOutputPair = new HashSet<>();
        long inputVertexCount = !isInput ? nonProcessEntityCount(ret) : 0;
        int currentOffset = lineageContext.getOffset();
        boolean isFirstValidProcessReached = false;
        for (int i = 0; i < currentVertexEdges.size(); i++) {
            AtlasEdge edge = currentVertexEdges.get(i);
            AtlasVertex processVertex = edge.getOutVertex();
            paginationCalculatedProcessOutputPair.add(Pair.of(getGuid(processVertex), getGuid(currentVertex)));
            if (!shouldProcessDeletedProcess(lineageContext, processVertex) || getStatus(edge) == DELETED) {
                continue;
            }

            List<AtlasEdge> edgesOfProcess = getEdgesOfProcess(isInput, lineageContext, paginationCalculatedProcessOutputPair, processVertex, currentOffset);

            if (isFirstValidProcessReached)
                currentOffset = 0;
            if (edgesOfProcess.size() > currentOffset) {
                isFirstValidProcessReached = true;
                ret.setHasChildrenForDirection(getGuid(processVertex), new LineageChildrenInfo(isInput ? INPUT : OUTPUT, hasMoreChildren(edgesOfProcess)));
                boolean isLimitReached = executeCurrentProcessVertex(isInput, depth, visitedVertices, ret, lineageContext, currentVertexEdges, inputVertexCount, currentOffset, i, edge, edgesOfProcess);
                if (isLimitReached)
                    return;
            } else
                currentOffset -= edgesOfProcess.size();
        }
    }

    private List<AtlasEdge> getEdgesOfProcess(boolean isInput, AtlasLineageContext lineageContext, Set<Pair<String, String>> paginationCalculatedProcessOutputPair, AtlasVertex processVertex, int currentOffset) {
        List<Pair<AtlasEdge, String>> processEdgeOutputVertexIdPairs = getUnvisitedProcessEdgesWithOutputVertexIds(isInput, lineageContext, paginationCalculatedProcessOutputPair, processVertex, currentOffset);
        processEdgeOutputVertexIdPairs.forEach(pair -> paginationCalculatedProcessOutputPair.add(Pair.of(getGuid(processVertex), pair.getRight())));
        return processEdgeOutputVertexIdPairs
                .stream()
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }

    private boolean executeCurrentProcessVertex(boolean isInput,
                                                int depth,
                                                Set<String> visitedVertices,
                                                AtlasLineageInfo ret,
                                                AtlasLineageContext lineageContext,
                                                List<AtlasEdge> currentVertexEdges,
                                                long inputVertexCount, int currentOffset, int vertexEdgeIndex,
                                                AtlasEdge edge,
                                                List<AtlasEdge> edgesOfProcess) throws AtlasBaseException {
        for (int j = currentOffset; j < edgesOfProcess.size(); j++) {
            AtlasEdge edgeOfProcess = edgesOfProcess.get(j);
            AtlasVertex entityVertex = edgeOfProcess.getInVertex();
            if (entityVertex == null) {
                continue;
            }
            if (shouldTerminate(isInput, ret, lineageContext, currentVertexEdges, inputVertexCount, vertexEdgeIndex, edgesOfProcess, j)) {
                return true;
            }
            if (!visitedVertices.contains(entityVertex.getIdForDisplay())) {
                traverseEdges(entityVertex, isInput, depth - 1, visitedVertices, ret, lineageContext);
            }
            if (lineageContext.isHideProcess()) {
                processVirtualEdge(edge, edgeOfProcess, ret, lineageContext);
            } else {
                processEdges(edge, edgeOfProcess, ret, lineageContext);
            }
        }
        return false;
    }

    private boolean shouldProcessDeletedProcess(AtlasLineageContext lineageContext, AtlasVertex processVertex) {
        return isVertexActive(processVertex) || lineageContext.isAllowDeletedProcess();
    }

    private boolean isVertexActive(AtlasVertex vertex) {
        return getStatus(vertex) == AtlasEntity.Status.ACTIVE;
    }

    private List<Pair<AtlasEdge, String>> getUnvisitedProcessEdgesWithOutputVertexIds(boolean isInput, AtlasLineageContext lineageContext, Set<Pair<String, String>> paginationCalculatedProcessOutputPair, AtlasVertex processVertex, int currentOffset) {
        if (lineageContext.getIgnoredProcesses() != null &&
                lineageContext.getIgnoredProcesses().contains(processVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class))) {
            return Collections.emptyList();
        }

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        List<Pair<AtlasEdge, String>> unvisitedProcessEdgesWithOutputVertexIds = new ArrayList<>();

        Iterable<AtlasEdge> outgoingEdges = vertexEdgeCache.getEdges(processVertex, OUT, isInput ? lineageInputLabel : lineageOutputLabel);

        for (AtlasEdge outgoingEdge : outgoingEdges) {
            AtlasVertex outputVertex = outgoingEdge.getInVertex();
            if (outputVertex != null &&
                    shouldProcessEdge(lineageContext, outgoingEdge) &&
                    vertexMatchesEvaluation(outputVertex, lineageContext) &&
                    !paginationCalculatedProcessOutputPair.contains(Pair.of(getGuid(processVertex), outputVertex.getIdForDisplay()))) {
                unvisitedProcessEdgesWithOutputVertexIds.add(Pair.of(outgoingEdge, outputVertex.getIdForDisplay()));
                if (unvisitedProcessEdgesWithOutputVertexIds.size() == lineageContext.getLimit() + currentOffset + 1) { // +1 is required for downstream check while trying to terminate the loop before it ends
                    break;
                }
            }
        }

        return unvisitedProcessEdgesWithOutputVertexIds;
    }

    @VisibleForTesting
    boolean shouldTerminate(boolean isInput,
                            AtlasLineageInfo ret,
                            AtlasLineageContext lineageContext,
                            List<AtlasEdge> currentVertexEdges,
                            long inputVertexCount,
                            int currentVertexEdgeIndex,
                            List<AtlasEdge> edgesOfProcess,
                            int processEdgeIndex) {
        if (lineageContext.getDirection() == BOTH) {
            if (isInput && nonProcessEntityCount(ret) == lineageContext.getLimit()) {
                ret.setHasMoreUpstreamVertices(true);
                return true;
            } else if (!isInput && nonProcessEntityCount(ret) - inputVertexCount == lineageContext.getLimit()) {
                ret.setHasMoreDownstreamVertices(true);
                return true;
            }
        } else if (nonProcessEntityCount(ret) == lineageContext.getLimit()) {
            setVertexCountsForOneDirection(isInput, ret, currentVertexEdges, currentVertexEdgeIndex, edgesOfProcess, processEdgeIndex);
            return true;
        }
        return false;
    }

    private void setVertexCountsForOneDirection(boolean isInput, AtlasLineageInfo ret, List<AtlasEdge> currentVertexEdges, int currentVertexEdgeIndex, List<AtlasEdge> edgesOfProcess, int processEdgeIndex) {
        if (isInput) {
            ret.setHasMoreUpstreamVertices(true);
        } else {
            ret.setHasMoreDownstreamVertices(true);
        }
    }

    private long nonProcessEntityCount(AtlasLineageInfo ret) {
        long nonProcessVertexCount = ret.getGuidEntityMap()
                .values()
                .stream()
                .filter(vertex -> !vertex.getTypeName().contains("Process"))
                .count();

        //We subtract 1 because the base entity is added to the result as well. We want 'limit' number of child
        //vertices, excluding the base entity.
        return Math.max(nonProcessVertexCount - 1, 0);
    }

    private void addLimitlessVerticesToResult(boolean isInput, int depth, Set<String> visitedVertices, AtlasLineageInfo ret, AtlasLineageContext lineageContext, List<AtlasEdge> currentVertexEdges) throws AtlasBaseException {
        for (AtlasEdge edge : currentVertexEdges) {
            AtlasVertex processVertex = edge.getOutVertex();
            List<AtlasEdge> outputEdgesOfProcess = getEdgesOfProcess(isInput, lineageContext, processVertex);

            ret.setHasChildrenForDirection(getGuid(processVertex), new LineageChildrenInfo(isInput ? INPUT : OUTPUT, hasMoreChildren(outputEdgesOfProcess)));
            for (AtlasEdge outgoingEdge : outputEdgesOfProcess) {
                AtlasVertex entityVertex = outgoingEdge.getInVertex();

                if (entityVertex != null) {
                    if (lineageContext.isHideProcess()) {
                        processVirtualEdge(edge, outgoingEdge, ret, lineageContext);
                    } else {
                        processEdges(edge, outgoingEdge, ret, lineageContext);
                    }

                    if (!visitedVertices.contains(entityVertex.getIdForDisplay())) {
                        traverseEdges(entityVertex, isInput, depth - 1, visitedVertices, ret, lineageContext);
                    }
                }
            }
        }
    }

    private void processLastLevel(AtlasVertex currentVertex, boolean isInput, AtlasLineageInfo ret, AtlasLineageContext lineageContext) {
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        List<AtlasEdge> processEdges = vertexEdgeCache.getEdges(currentVertex, IN, isInput ? lineageOutputLabel : lineageInputLabel);

        // Filter lineages based on ignored process types
        processEdges = CollectionUtils.isNotEmpty(lineageContext.getIgnoredProcesses()) ?
                processEdges.stream()
                        .filter(processEdge -> processEdge.getOutVertex() != null)
                        .filter(processEdge -> !lineageContext.getIgnoredProcesses().contains(processEdge.getOutVertex().getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class)))
                        .collect(Collectors.toList())
                : processEdges;

        // Filter lineages if child has only self-cyclic relation
        processEdges = processEdges.stream()
                .filter(processEdge -> processEdge.getOutVertex() != null)
                .filter(processEdge -> !childHasOnlySelfCycle(processEdge.getOutVertex(), currentVertex, isInput))
                .collect(Collectors.toList());

        ret.setHasChildrenForDirection(getGuid(currentVertex), new LineageChildrenInfo(isInput ? INPUT : OUTPUT, hasMoreChildren(processEdges)));
    }

    private boolean childHasOnlySelfCycle(AtlasVertex processVertex, AtlasVertex currentVertex, boolean isInput) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("childHasSelfCycle");
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        Iterator<AtlasEdge> processEdgeIterator;
        processEdgeIterator = processVertex.getEdges(OUT, isInput ? lineageInputLabel : lineageOutputLabel).iterator();
        Set<AtlasEdge> processOutputEdges = new HashSet<>();
        while (processEdgeIterator.hasNext()) {
            processOutputEdges.add(processEdgeIterator.next());
        }

        Set<AtlasVertex> linkedVertices = processOutputEdges.stream().map(x -> x.getInVertex()).collect(Collectors.toSet());
        RequestContext.get().endMetricRecord(metricRecorder);
        return linkedVertices.size() == 1 && linkedVertices.contains(currentVertex);
    }

    private List<AtlasEdge> getEdgesOfProcess(boolean isInput, AtlasLineageContext lineageContext, AtlasVertex processVertex) {
        if (lineageContext.getIgnoredProcesses() != null &&
                lineageContext.getIgnoredProcesses().contains(processVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class))) {
            return Collections.emptyList();
        }

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();

        return vertexEdgeCache.getEdges(processVertex, OUT, isInput ? lineageInputLabel : lineageOutputLabel)
                .stream()
                .filter(edge -> shouldProcessEdge(lineageContext, edge) && vertexMatchesEvaluation(edge.getInVertex(), lineageContext))
                .sorted(Comparator.comparing(edge -> edge.getProperty("_r__guid", String.class)))
                .collect(Collectors.toList());
    }

    private boolean vertexMatchesEvaluation(AtlasVertex currentVertex, AtlasLineageContext lineageContext) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("vertexMatchesEvaluation");
        try {
            return currentVertex.equals(lineageContext.getStartDatasetVertex()) || lineageContext.evaluate(currentVertex);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private boolean vertexMatchesEvaluation(AtlasVertex currentVertex, AtlasLineageOnDemandContext atlasLineageOnDemandContext) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("vertexMatchesEvaluation");
        try {
            return atlasLineageOnDemandContext.evaluate(currentVertex);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private boolean edgeMatchesEvaluation(AtlasEdge currentEdge, AtlasLineageOnDemandContext atlasLineageOnDemandContext) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("edgeMatchesEvaluation");
        try {
            return atlasLineageOnDemandContext.evaluate(currentEdge);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private boolean shouldProcessEdge(AtlasLineageContext lineageContext, AtlasEdge edge) {
        return lineageContext.isAllowDeletedProcess() ||
                (getStatus(edge.getOutVertex()) == AtlasEntity.Status.ACTIVE && getStateAsString(edge).equals(ACTIVE_STATE_VALUE));
    }

    private List<AtlasEdge> getEdgesOfCurrentVertex(AtlasVertex currentVertex, boolean isInput, AtlasLineageContext lineageContext) {
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        return vertexEdgeCache
                .getEdges(currentVertex, IN, isInput ? lineageOutputLabel : lineageInputLabel)
                .stream()
                .sorted(Comparator.comparing(edge -> edge.getProperty("_r__guid", String.class)))
                .filter(edge -> shouldProcessEdge(lineageContext, edge))
                .collect(Collectors.toList());
    }

    private boolean lineageContainsEdge(AtlasLineageInfo lineageInfo, AtlasEdge edge) {
        boolean ret = false;

        if (lineageInfo != null && CollectionUtils.isNotEmpty(lineageInfo.getRelations()) && edge != null) {
            String relationGuid = AtlasGraphUtilsV2.getEncodedProperty(edge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
            Set<LineageRelation> relations = lineageInfo.getRelations();
            for (LineageRelation relation : relations) {
                if (relation.getRelationshipId().equals(relationGuid)) {
                    ret = true;
                    break;
                }
            }
        }

        return ret;
    }

    private boolean lineageContainsVisitedEdgeV2(AtlasLineageOnDemandInfo lineageInfo, AtlasEdge edge) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("lineageContainsVisitedEdgeV2");

        boolean ret = false;

        if (edge != null && lineageInfo != null && CollectionUtils.isNotEmpty(lineageInfo.getVisitedEdges())) {
            if (lineageInfo.getVisitedEdges().contains(getEdgeLabel(edge))) {
                ret = true;
            }
        }

        RequestContext.get().endMetricRecord(metric);

        return ret;
    }

    private boolean lineageContainsSkippedEdgeV2(AtlasLineageOnDemandInfo lineageInfo, AtlasEdge edge) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("lineageContainsSkippedEdgeV2");

        boolean ret = false;

        if (edge != null && lineageInfo != null && CollectionUtils.isNotEmpty(lineageInfo.getSkippedEdges())) {
            if (lineageInfo.getSkippedEdges().contains(getEdgeLabel(edge))) {
                ret = true;
            }
        }

        RequestContext.get().endMetricRecord(metric);

        return ret;
    }

    private void addEdgeToSkippedEdges(AtlasLineageOnDemandInfo lineageInfo, AtlasEdge edge) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("addEdgeToSkippedEdges");
        if (lineageInfo.getSkippedEdges() != null) {
            lineageInfo.getSkippedEdges().add(getEdgeLabel(edge));
        }
        RequestContext.get().endMetricRecord(metric);
    }

    private AtlasLineageInfo initializeLineageInfo(String guid, LineageDirection direction, int depth, int limit, int offset) {
        return new AtlasLineageInfo(guid, new HashMap<>(), new HashSet<>(), direction, depth, limit, offset);
    }

    private AtlasLineageOnDemandInfo initializeLineageOnDemandInfo(String guid) {
        return new AtlasLineageOnDemandInfo(guid, new HashMap<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashMap<>());
    }

    private List executeGremlinScript(Map<String, Object> bindings, String lineageQuery) throws AtlasBaseException {
        List ret;
        ScriptEngine engine = graph.getGremlinScriptEngine();

        try {
            ret = (List) graph.executeGremlinScript(engine, bindings, lineageQuery, false);
        } catch (ScriptException e) {
            throw new AtlasBaseException(INSTANCE_LINEAGE_QUERY_FAILED, lineageQuery);
        } finally {
            graph.releaseGremlinScriptEngine(engine);
        }

        return ret;
    }

    private boolean processVirtualEdge(final AtlasEdge incomingEdge, final AtlasEdge outgoingEdge, AtlasLineageInfo lineageInfo,
                                       AtlasLineageContext lineageContext) throws AtlasBaseException {
        final Map<String, AtlasEntityHeader> entities = lineageInfo.getGuidEntityMap();
        final Set<LineageRelation> relations = lineageInfo.getRelations();

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        AtlasVertex inVertex = incomingEdge.getInVertex();
        AtlasVertex outVertex = outgoingEdge.getInVertex();
        AtlasVertex processVertex = outgoingEdge.getOutVertex();
        String inGuid = AtlasGraphUtilsV2.getIdFromVertex(inVertex);
        String outGuid = AtlasGraphUtilsV2.getIdFromVertex(outVertex);
        String processGuid = AtlasGraphUtilsV2.getIdFromVertex(processVertex);
        String relationGuid = null;
        boolean isInputEdge = incomingEdge.getLabel().equalsIgnoreCase(lineageInputLabel);

        if (!entities.containsKey(inGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(inVertex, lineageContext.getAttributes());
            GraphTransactionInterceptor.addToVertexGuidCache(inVertex.getId(), entityHeader.getGuid());
            entities.put(inGuid, entityHeader);
        }

        if (!entities.containsKey(outGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(outVertex, lineageContext.getAttributes());
            GraphTransactionInterceptor.addToVertexGuidCache(outVertex.getId(), entityHeader.getGuid());
            entities.put(outGuid, entityHeader);
        }

        if (!entities.containsKey(processGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(processVertex, lineageContext.getAttributes());
            GraphTransactionInterceptor.addToVertexGuidCache(processVertex.getId(), entityHeader.getGuid());
            entities.put(processGuid, entityHeader);
        }

        if (isInputEdge) {
            relations.add(new LineageRelation(inGuid, outGuid, relationGuid, getGuid(processVertex)));
        } else {
            relations.add(new LineageRelation(outGuid, inGuid, relationGuid, getGuid(processVertex)));
        }
        return false;
    }

    private void processEdges(final AtlasEdge incomingEdge, AtlasEdge outgoingEdge, AtlasLineageInfo lineageInfo,
                              AtlasLineageContext lineageContext) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("processEdges");

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        final Map<String, AtlasEntityHeader> entities = lineageInfo.getGuidEntityMap();
        final Set<LineageRelation> relations = lineageInfo.getRelations();

        AtlasVertex leftVertex = incomingEdge.getInVertex();
        AtlasVertex processVertex = incomingEdge.getOutVertex();
        AtlasVertex rightVertex = outgoingEdge.getInVertex();

        String leftGuid = AtlasGraphUtilsV2.getIdFromVertex(leftVertex);
        String rightGuid = AtlasGraphUtilsV2.getIdFromVertex(rightVertex);
        String processGuid = AtlasGraphUtilsV2.getIdFromVertex(processVertex);

        if (!entities.containsKey(leftGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(leftVertex, lineageContext.getAttributes());
            entities.put(leftGuid, entityHeader);
        }

        if (!entities.containsKey(processGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(processVertex, lineageContext.getAttributes());
            entities.put(processGuid, entityHeader);
        }

        if (!entities.containsKey(rightGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(rightVertex, lineageContext.getAttributes());
            entities.put(rightGuid, entityHeader);
        }

        String relationGuid = AtlasGraphUtilsV2.getEncodedProperty(incomingEdge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        if (incomingEdge.getLabel().equalsIgnoreCase(lineageInputLabel)) {
            relations.add(new LineageRelation(leftGuid, processGuid, relationGuid));
        } else {
            relations.add(new LineageRelation(processGuid, leftGuid, relationGuid));
        }

        relationGuid = AtlasGraphUtilsV2.getEncodedProperty(outgoingEdge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        if (outgoingEdge.getLabel().equalsIgnoreCase(lineageInputLabel)) {
            relations.add(new LineageRelation(rightGuid, processGuid, relationGuid));
        } else {
            relations.add(new LineageRelation(processGuid, rightGuid, relationGuid));
        }
        RequestContext.get().endMetricRecord(metric);
    }

    private void processEdge(final AtlasEdge edge, AtlasLineageInfo lineageInfo,
                             AtlasLineageContext lineageContext) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("processEdge");

        final Map<String, AtlasEntityHeader> entities = lineageInfo.getGuidEntityMap();
        final Set<LineageRelation> relations = lineageInfo.getRelations();

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        AtlasVertex inVertex = edge.getInVertex();
        AtlasVertex outVertex = edge.getOutVertex();
        String inGuid = AtlasGraphUtilsV2.getIdFromVertex(inVertex);
        String outGuid = AtlasGraphUtilsV2.getIdFromVertex(outVertex);
        String relationGuid = AtlasGraphUtilsV2.getEncodedProperty(edge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        boolean isInputEdge = edge.getLabel().equalsIgnoreCase(lineageInputLabel);

        if (!entities.containsKey(inGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(inVertex, lineageContext.getAttributes());
            entities.put(inGuid, entityHeader);
        }

        if (!entities.containsKey(outGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(outVertex, lineageContext.getAttributes());
            entities.put(outGuid, entityHeader);
        }

        if (isInputEdge) {
            relations.add(new LineageRelation(inGuid, outGuid, relationGuid));
        } else {
            relations.add(new LineageRelation(outGuid, inGuid, relationGuid));
        }
        RequestContext.get().endMetricRecord(metric);
    }

    private void processEdge(final AtlasEdge edge, final Map<String, AtlasEntityHeader> entities,
                             final Set<LineageRelation> relations, AtlasLineageContext lineageContext) throws AtlasBaseException {
        //Backward compatibility method
        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        AtlasVertex inVertex = edge.getInVertex();
        AtlasVertex outVertex = edge.getOutVertex();
        String inGuid = AtlasGraphUtilsV2.getIdFromVertex(inVertex);
        String outGuid = AtlasGraphUtilsV2.getIdFromVertex(outVertex);
        String relationGuid = AtlasGraphUtilsV2.getEncodedProperty(edge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        boolean isInputEdge = edge.getLabel().equalsIgnoreCase(lineageInputLabel);

        if (!entities.containsKey(inGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(inVertex, lineageContext.getAttributes());
            entities.put(inGuid, entityHeader);
        }

        if (!entities.containsKey(outGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(outVertex, lineageContext.getAttributes());
            entities.put(outGuid, entityHeader);
        }

        if (isInputEdge) {
            relations.add(new LineageRelation(inGuid, outGuid, relationGuid));
        } else {
            relations.add(new LineageRelation(outGuid, inGuid, relationGuid));
        }
    }

    private void processEdge(final AtlasEdge edge, final AtlasLineageOnDemandInfo lineageInfo, AtlasLineageOnDemandContext atlasLineageOnDemandContext, int level, AtomicInteger traversalOrder) throws AtlasBaseException {
        processEdge(edge, lineageInfo.getGuidEntityMap(), lineageInfo.getRelations(), lineageInfo.getVisitedEdges(), atlasLineageOnDemandContext.getAttributes(), level, traversalOrder);
    }

    private void processEdge(final AtlasEdge edge, final Map<String, AtlasEntityHeader> entities, final Set<AtlasLineageOnDemandInfo.LineageRelation> relations, final Set<String> visitedEdges, final Set<String> attributes, int level, AtomicInteger traversalOrder) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processEdge");
        AtlasVertex inVertex     = edge.getInVertex();
        AtlasVertex outVertex    = edge.getOutVertex();

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String inTypeName = AtlasGraphUtilsV2.getTypeName(inVertex);
        AtlasEntityType inEntityType = atlasTypeRegistry.getEntityTypeByName(inTypeName);
        if (inEntityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, inTypeName);
        }
        boolean inIsProcess = inEntityType.getTypeAndAllSuperTypes().contains(PROCESS_SUPER_TYPE);

        String      inGuid       = AtlasGraphUtilsV2.getIdFromVertex(inVertex);
        String      outGuid      = AtlasGraphUtilsV2.getIdFromVertex(outVertex);
        String      relationGuid = AtlasGraphUtilsV2.getEncodedProperty(edge, RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
        boolean     isInputEdge  = edge.getLabel().equalsIgnoreCase(lineageInputLabel);

        if (!entities.containsKey(inGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(inVertex, attributes);
            if (!inIsProcess) {
                entityHeader.setDepth(level);
                entityHeader.setTraversalOrder(traversalOrder.get());
            }
            entities.put(inGuid, entityHeader);
        }
        if (!entities.containsKey(outGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(outVertex, attributes);
            if (inIsProcess) {
                entityHeader.setDepth(level);
                entityHeader.setTraversalOrder(traversalOrder.get());
            }
            entities.put(outGuid, entityHeader);
        }
        if (isInputEdge) {
            relations.add(new AtlasLineageOnDemandInfo.LineageRelation(inGuid, outGuid, relationGuid));
        } else {
            relations.add(new AtlasLineageOnDemandInfo.LineageRelation(outGuid, inGuid, relationGuid));
        }

        if (visitedEdges != null) {
            visitedEdges.add(getEdgeLabel(edge));
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private AtlasLineageInfo getBothLineageInfoV1(AtlasLineageContext lineageContext) throws AtlasBaseException {
        AtlasLineageInfo inputLineage = getLineageInfo(lineageContext, INPUT);
        AtlasLineageInfo outputLineage = getLineageInfo(lineageContext, OUTPUT);
        AtlasLineageInfo ret = inputLineage;

        ret.getRelations().addAll(outputLineage.getRelations());
        ret.getGuidEntityMap().putAll(outputLineage.getGuidEntityMap());
        ret.setLineageDirection(BOTH);

        return ret;
    }

    private String getLineageQuery(String entityGuid, LineageDirection direction, int depth, boolean isDataSet, Map<String, Object> bindings) {
        String incomingFrom = null;
        String outgoingTo = null;
        String ret;

        String lineageInputLabel = RequestContext.get().getLineageInputLabel();
        String lineageOutputLabel = RequestContext.get().getLineageOutputLabel();
        if (direction.equals(INPUT)) {
            incomingFrom = lineageOutputLabel;
            outgoingTo = lineageInputLabel;
        } else if (direction.equals(OUTPUT)) {
            incomingFrom = lineageInputLabel;
            outgoingTo = lineageOutputLabel;
        }

        bindings.put("guid", entityGuid);
        bindings.put("incomingEdgeLabel", incomingFrom);
        bindings.put("outgoingEdgeLabel", outgoingTo);
        bindings.put("dataSetDepth", depth);
        bindings.put("processDepth", depth - 1);

        if (depth < 1) {
            ret = isDataSet ? gremlinQueryProvider.getQuery(FULL_LINEAGE_DATASET) :
                    gremlinQueryProvider.getQuery(FULL_LINEAGE_PROCESS);
        } else {
            ret = isDataSet ? gremlinQueryProvider.getQuery(PARTIAL_LINEAGE_DATASET) :
                    gremlinQueryProvider.getQuery(PARTIAL_LINEAGE_PROCESS);
        }

        return ret;
    }

    public boolean isLineageOnDemandEnabled() {
        return AtlasConfiguration.LINEAGE_ON_DEMAND_ENABLED.getBoolean();
    }

}