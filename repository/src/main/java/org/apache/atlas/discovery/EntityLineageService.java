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


import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.commons.collections.CollectionUtils;

import javax.inject.Inject;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityLineageService implements AtlasLineageService {
    private static final String INPUT_PROCESS_EDGE      =  "__Process.inputs";
    private static final String OUTPUT_PROCESS_EDGE     =  "__Process.outputs";

    private final AtlasGraph graph;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;

    @Inject
    EntityLineageService() throws DiscoveryException {
        this.graph = AtlasGraphProvider.getGraphInstance();
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
    }

    @Override
    public AtlasLineageInfo getAtlasLineageInfo(String guid, LineageDirection direction, int depth) throws AtlasBaseException {
        AtlasLineageInfo lineageInfo;

        if (!entityExists(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        if (direction != null) {
            if (direction.equals(LineageDirection.INPUT)) {
                lineageInfo = getLineageInfo(guid, LineageDirection.INPUT, depth);
            } else if (direction.equals(LineageDirection.OUTPUT)) {
                lineageInfo = getLineageInfo(guid, LineageDirection.OUTPUT, depth);
            } else if (direction.equals(LineageDirection.BOTH)) {
                lineageInfo = getBothLineageInfo(guid, depth);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_LINEAGE_INVALID_PARAMS, "direction", direction.toString());
            }
        } else {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_LINEAGE_INVALID_PARAMS, "direction", null);
        }

        return lineageInfo;
    }

    private AtlasLineageInfo getLineageInfo(String guid, LineageDirection direction, int depth) throws AtlasBaseException {
        Map<String, AtlasEntityHeader> entities     = new HashMap<String, AtlasEntityHeader>();
        Set<LineageRelation>           relations    = new HashSet<LineageRelation>();
        String                         lineageQuery = getLineageQuery(guid, direction, depth);

        try {
            List paths = (List) graph.executeGremlinScript(lineageQuery, true);

            if (CollectionUtils.isNotEmpty(paths)) {
                for (Object path : paths) {
                    if (path instanceof List) {
                        List vertices = (List) path;

                        if (CollectionUtils.isNotEmpty(vertices)) {
                            AtlasEntityHeader prev = null;

                            for (Object vertex : vertices) {
                                AtlasEntityHeader entity = toAtlasEntityHeader(vertex);

                                if (!entities.containsKey(entity.getGuid())) {
                                    entities.put(entity.getGuid(), entity);
                                }

                                if (prev != null) {
                                    if (direction.equals(LineageDirection.INPUT)) {
                                        relations.add(new LineageRelation(entity.getGuid(), prev.getGuid()));
                                    } else if (direction.equals(LineageDirection.OUTPUT)) {
                                        relations.add(new LineageRelation(prev.getGuid(), entity.getGuid()));
                                    }
                                }
                                prev = entity;
                            }
                        }
                    }
                }
            }

        } catch (ScriptException e) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_LINEAGE_QUERY_FAILED, lineageQuery);
        }

        return new AtlasLineageInfo(guid, entities, relations, direction, depth);
    }

    private AtlasLineageInfo getBothLineageInfo(String guid, int depth) throws AtlasBaseException {
        AtlasLineageInfo inputLineage  = getLineageInfo(guid, LineageDirection.INPUT, depth);
        AtlasLineageInfo outputLineage = getLineageInfo(guid, LineageDirection.OUTPUT, depth);
        AtlasLineageInfo ret           = inputLineage;

        ret.getRelations().addAll(outputLineage.getRelations());
        ret.getGuidEntityMap().putAll(outputLineage.getGuidEntityMap());
        ret.setLineageDirection(LineageDirection.BOTH);

        return ret;
    }

    private String getLineageQuery(String entityGuid, LineageDirection direction, int depth) throws AtlasBaseException {
        String lineageQuery = null;

        if (direction.equals(LineageDirection.INPUT)) {
            lineageQuery = generateLineageQuery(entityGuid, depth, OUTPUT_PROCESS_EDGE, INPUT_PROCESS_EDGE);

        } else if (direction.equals(LineageDirection.OUTPUT)) {
            lineageQuery = generateLineageQuery(entityGuid, depth, INPUT_PROCESS_EDGE, OUTPUT_PROCESS_EDGE);
        }

        return lineageQuery;
    }

    private String generateLineageQuery(String entityGuid, int depth, String incomingFrom, String outgoingTo) {
        String lineageQuery;
        if (depth < 1) {
            String query = gremlinQueryProvider.getQuery(AtlasGremlinQuery.FULL_LINEAGE);
            lineageQuery = String.format(query, entityGuid, incomingFrom, outgoingTo);
        } else {
            String query = gremlinQueryProvider.getQuery(AtlasGremlinQuery.PARTIAL_LINEAGE);
            lineageQuery = String.format(query, entityGuid, incomingFrom, outgoingTo, depth);
        }
        return lineageQuery;
    }

    private AtlasEntityHeader toAtlasEntityHeader(Object vertexObj) {
        AtlasEntityHeader ret = new AtlasEntityHeader();

        if (vertexObj instanceof AtlasVertex) {
            AtlasVertex vertex = (AtlasVertex) vertexObj;
            ret.setTypeName(vertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class));
            ret.setGuid(vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class));
            ret.setDisplayText(vertex.getProperty(Constants.QUALIFIED_NAME, String.class));

            String state  = vertex.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
            Status status = (state.equalsIgnoreCase("ACTIVE") ? Status.ACTIVE : Status.DELETED);
            ret.setStatus(status);
        }

        return ret;
    }

    private boolean entityExists(String guid) {
        boolean ret = false;
        Iterator<AtlasVertex> results = graph.query()
                .has(Constants.GUID_PROPERTY_KEY, guid)
                .has(Constants.SUPER_TYPES_PROPERTY_KEY, AtlasClient.DATA_SET_SUPER_TYPE)
                .vertices().iterator();

        while (results.hasNext()) {
            return true;
        }

        return ret;
    }
}