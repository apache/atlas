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


import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class EntityLineageService implements AtlasLineageService {
    private static final String INPUT_PROCESS_EDGE  = "__Process.inputs";
    private static final String OUTPUT_PROCESS_EDGE = "__Process.outputs";

    public static final String DATASET_SCHEMA_QUERY_PREFIX = "atlas.lineage.schema.query.";

    private final AtlasGraph                graph;
    private final AtlasGremlinQueryProvider gremlinQueryProvider;
    private final EntityGraphRetriever      entityRetriever;
    private final AtlasDiscoveryService     atlasDiscoveryService;
    private final Configuration             atlasConfiguration;

    @Inject
    EntityLineageService(AtlasTypeRegistry typeRegistry, AtlasGraph atlasGraph, final AtlasDiscoveryService atlasDiscoveryService, final Configuration atlasConfiguration) throws DiscoveryException {
        this.graph = atlasGraph;
        this.atlasDiscoveryService = atlasDiscoveryService;
        this.atlasConfiguration = atlasConfiguration;
        this.gremlinQueryProvider = AtlasGremlinQueryProvider.INSTANCE;
        this.entityRetriever = new EntityGraphRetriever(typeRegistry);
    }

    @Override
    @GraphTransaction
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

    @Override
    @GraphTransaction
    public String getSchema(final String datasetName) throws AtlasBaseException {
        if (StringUtils.isEmpty(datasetName)) {
            // TODO: Complete error handling here
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST);
        }
        Iterator<AtlasVertex> vertices = graph.query().has("Referenceable.qualifiedName", datasetName)
                                              .has(Constants.STATE_PROPERTY_KEY, "ACTIVE")
                                              .has(Constants.SUPER_TYPES_PROPERTY_KEY, "DataSet")
                                              .vertices().iterator();

        if (vertices.hasNext()) {
            AtlasVertex vertex   = vertices.next();
            String      typeName = GraphHelper.getTypeName(vertex);
            String      guid     = GraphHelper.getGuid(vertex);
            return getSchemaForId(typeName, guid);
        }
        return null;
    }

    @Override
    @GraphTransaction
    public String getSchemaForEntity(final String guid) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST);
        }
        Iterator<AtlasVertex> vertices = graph.query().has(Constants.GUID_PROPERTY_KEY, guid)
                                              .has(Constants.SUPER_TYPES_PROPERTY_KEY, AtlasClient.DATA_SET_SUPER_TYPE)
                                              .vertices().iterator();

        if (vertices.hasNext()) {
            AtlasVertex vertex = vertices.next();
            String      typeName = GraphHelper.getTypeName(vertex);
            return getSchemaForId(typeName, guid);

        }
        return null;
    }

    private AtlasLineageInfo getLineageInfo(String guid, LineageDirection direction, int depth) throws AtlasBaseException {
        Map<String, AtlasEntityHeader> entities     = new HashMap<>();
        Set<LineageRelation>           relations    = new HashSet<>();
        String                         lineageQuery = getLineageQuery(guid, direction, depth);

        List paths = (List) graph.executeGremlinScript(lineageQuery, true);

        if (CollectionUtils.isNotEmpty(paths)) {
            for (Object path : paths) {
                if (path instanceof List) {
                    List vertices = (List) path;

                    if (CollectionUtils.isNotEmpty(vertices)) {
                        AtlasEntityHeader prev = null;

                        for (Object vertex : vertices) {
                            if (!(vertex instanceof AtlasVertex)) {
                                continue;
                            }

                            AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader((AtlasVertex) vertex);

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

    private boolean entityExists(String guid) {
        boolean ret = false;
        Iterator<AtlasVertex> results = graph.query()
                                             .has(Constants.GUID_PROPERTY_KEY, guid)
                                             .vertices().iterator();

        while (results.hasNext()) {
            AtlasVertex  entityVertex = results.next();
            List<String> superTypes   = GraphHelper.getSuperTypeNames(entityVertex);

            ret = (CollectionUtils.isNotEmpty(superTypes)) ? superTypes.contains(AtlasClient.DATA_SET_SUPER_TYPE) : false;
        }

        return ret;
    }

    private String getSchemaForId(String typeName, String guid) throws AtlasBaseException {
        String configName     = DATASET_SCHEMA_QUERY_PREFIX + typeName;
        String schemaTemplate = atlasConfiguration.getString(configName);
        if (schemaTemplate != null) {
            final String      schemaQuery  = String.format(schemaTemplate, guid);
            int               limit        = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
            AtlasSearchResult searchResult = atlasDiscoveryService.searchUsingDslQuery(schemaQuery, limit, 0);
            // TODO: Fix the return there
            return null;
        }
        throw new AtlasBaseException("Schema is not configured for type " + typeName + ". Configure " + configName);
    }
}