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

package org.apache.atlas.discovery;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.AtlasClient.DATA_SET_SUPER_TYPE;
import static org.apache.atlas.AtlasClient.PROCESS_SUPER_TYPE;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_LINEAGE_QUERY_FAILED;
import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.BOTH;
import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.INPUT;
import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.OUTPUT;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery.FULL_LINEAGE_DATASET;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery.FULL_LINEAGE_PROCESS;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery.PARTIAL_LINEAGE_DATASET;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery.PARTIAL_LINEAGE_PROCESS;

@Service
public class EntityLineageService implements AtlasLineageService {
    private static final String INPUT_PROCESS_EDGE      =  "__Process.inputs";
    private static final String OUTPUT_PROCESS_EDGE     =  "__Process.outputs";

    private final AtlasGraph                graph;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;
    private final EntityGraphRetriever      entityRetriever;
    private final AtlasTypeRegistry         atlasTypeRegistry;

    @Inject
    EntityLineageService(AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph) {
        this.graph                = atlasGraph;
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
        this.entityRetriever      = new EntityGraphRetriever(typeRegistry);
        this.atlasTypeRegistry    = typeRegistry;
    }

    @Override
    @GraphTransaction
    public AtlasLineageInfo getAtlasLineageInfo(String guid, LineageDirection direction, int depth) throws AtlasBaseException {
        AtlasLineageInfo  ret;
        AtlasEntityHeader entity     = entityRetriever.toAtlasEntityHeader(guid);
        AtlasEntityType   entityType = atlasTypeRegistry.getEntityTypeByName(entity.getTypeName());

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, entity.getTypeName());
        }

        List<String> typeAndAllSuperTypes = getTypeAndAllSuperTypes(entityType);

        boolean isDataSet = typeAndAllSuperTypes.contains(DATA_SET_SUPER_TYPE);

        if (!isDataSet) {
            boolean isProcess = typeAndAllSuperTypes.contains(PROCESS_SUPER_TYPE);

            if (!isProcess) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_LINEAGE_ENTITY_TYPE, guid, entity.getTypeName());
            }
        }

        if (direction != null) {
            if (direction.equals(INPUT)) {
                ret = getLineageInfo(guid, INPUT, depth, isDataSet);
            } else if (direction.equals(OUTPUT)) {
                ret = getLineageInfo(guid, OUTPUT, depth, isDataSet);
            } else if (direction.equals(BOTH)) {
                ret = getBothLineageInfo(guid, depth, isDataSet);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_LINEAGE_INVALID_PARAMS, "direction", direction.toString());
            }
        } else {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_LINEAGE_INVALID_PARAMS, "direction", null);
        }

        return ret;
    }

    private AtlasLineageInfo getLineageInfo(String guid, LineageDirection direction, int depth, boolean isDataSet) throws AtlasBaseException {
        Map<String, AtlasEntityHeader> entities     = new HashMap<>();
        Set<LineageRelation>           relations    = new HashSet<>();
        final Map<String, Object>      bindings     = new HashMap<>();
        String                         lineageQuery = getLineageQuery(guid, direction, depth, isDataSet, bindings);
        List                           edges        = executeGremlinScript(lineageQuery, bindings);

        if (CollectionUtils.isNotEmpty(edges)) {
            for (Object edge : edges) {
                if (edge instanceof AtlasEdge) {
                    processEdge((AtlasEdge) edge, entities, relations);
                }
            }
        }

        return new AtlasLineageInfo(guid, entities, relations, direction, depth);
    }

    private List executeGremlinScript(String lineageQuery, Map<String, Object> bindings) throws AtlasBaseException {
        List         ret;
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

    private void processEdge(final AtlasEdge edge, final Map<String, AtlasEntityHeader> entities, final Set<LineageRelation> relations) throws AtlasBaseException {
        AtlasVertex inVertex    = edge.getInVertex();
        AtlasVertex outVertex   = edge.getOutVertex();
        String      inGuid      = AtlasGraphUtilsV1.getIdFromVertex(inVertex);
        String      outGuid     = AtlasGraphUtilsV1.getIdFromVertex(outVertex);
        boolean     isInputEdge = edge.getLabel().equalsIgnoreCase(INPUT_PROCESS_EDGE);

        if (!entities.containsKey(inGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(inVertex);
            entities.put(inGuid, entityHeader);
        }

        if (!entities.containsKey(outGuid)) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeader(outVertex);
            entities.put(outGuid, entityHeader);
        }

        if (isInputEdge) {
            relations.add(new LineageRelation(inGuid, outGuid));
        } else {
            relations.add(new LineageRelation(outGuid, inGuid));
        }
    }

    private AtlasLineageInfo getBothLineageInfo(String guid, int depth, boolean isDataSet) throws AtlasBaseException {
        AtlasLineageInfo inputLineage  = getLineageInfo(guid, INPUT, depth, isDataSet);
        AtlasLineageInfo outputLineage = getLineageInfo(guid, OUTPUT, depth, isDataSet);
        AtlasLineageInfo ret           = inputLineage;

        ret.getRelations().addAll(outputLineage.getRelations());
        ret.getGuidEntityMap().putAll(outputLineage.getGuidEntityMap());
        ret.setLineageDirection(BOTH);

        return ret;
    }

    private String getLineageQuery(String entityGuid, LineageDirection direction, int depth, boolean isDataSet, Map<String, Object> bindings) {
        String ret;

        if (depth < 1) {
            ret = isDataSet ? gremlinQueryProvider.getQuery(FULL_LINEAGE_DATASET) : gremlinQueryProvider.getQuery(FULL_LINEAGE_PROCESS);
        } else {
            ret = isDataSet ? gremlinQueryProvider.getQuery(PARTIAL_LINEAGE_DATASET) : gremlinQueryProvider.getQuery(PARTIAL_LINEAGE_PROCESS);
        }

        String incoming = null;
        String outgoing = null;

        if (direction.equals(INPUT)) {
            incoming = OUTPUT_PROCESS_EDGE;
            outgoing = INPUT_PROCESS_EDGE;
        } else if (direction.equals(OUTPUT)) {
            incoming = INPUT_PROCESS_EDGE;
            outgoing = OUTPUT_PROCESS_EDGE;
        }

        bindings.put("guid", entityGuid);
        bindings.put("incomingEdgeLabel", incoming);
        bindings.put("outgoingEdgeLabel", outgoing);
        bindings.put("processDepth", depth);
        bindings.put("dataSetDepth", depth * 2);

        return ret;
    }

    private List<String> getTypeAndAllSuperTypes(AtlasEntityType entityType) {
        List<String> ret = null;

        if (entityType != null) {
            ret = new ArrayList<>();

            ret.add(entityType.getTypeName());

            if (CollectionUtils.isNotEmpty(entityType.getAllSuperTypes())) {
                ret.addAll(entityType.getAllSuperTypes());
            }
        }

        return ret;
    }
}