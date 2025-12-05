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

package org.apache.atlas.util;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.AtlanElasticSearchIndex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.repository.graphdb.janus.cassandra.ESConnector;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.janusgraph.core.JanusGraph;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.LEAN_GRAPH_ENABLED;
import static org.apache.atlas.type.Constants.INDEX_NAME_EDGE_INDEX;
import static org.apache.atlas.type.Constants.INDEX_NAME_VERTEX_INDEX;


@Component
public class RepairIndex {
    private static final Logger LOG = LoggerFactory.getLogger(RepairIndex.class);

    private static final int  MAX_TRIES_ON_FAILURE = 3;

    private JanusGraph graph;
    private AtlasGraph atlasGraph;
    private AtlanElasticSearchIndex searchIndex;
    private EntityMutationService entityMutationService;
    private AtlasTypeRegistry typeRegistry;
    private GraphHelper graphHelper;

    @Inject
    public RepairIndex(EntityMutationService entityMutationService, AtlasGraph atlasGraph,
                       AtlasTypeRegistry typeRegistry) {
        this.entityMutationService = entityMutationService;
        this.typeRegistry = typeRegistry;
        this.atlasGraph = atlasGraph;
        graphHelper = new GraphHelper(atlasGraph);
    }

    /**
     * Used by DLQ replay service
     * @param indexName
     * @param vertexIds
     * @throws Exception
     */
    public void reindexVerticesByIds(String indexName, Set<String> vertexIds) throws Exception {
        Set<AtlasVertex> vertices = graphHelper.getVertices(vertexIds);
        Set<String> guids = vertices.stream().map(GraphHelper::getGuid).collect(Collectors.toSet());

        if (LEAN_GRAPH_ENABLED) {
            Map<String, Map<String, Object>> toReIndex = ((AtlasJanusGraph) atlasGraph).getESPropertiesForUpdateFromVertices(vertices, this.typeRegistry);
            try {
                ESConnector.syncToEs(toReIndex, true, null);
            } catch (Exception e){
                LOG.info("Exception: " + e.getMessage());
            }
        } else {

            Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new java.util.HashMap<>();
            StandardJanusGraphTx tx = null;
            try {
                tx = (StandardJanusGraphTx) graph.newTransaction();
                JanusGraphSchemaVertex indexV = tx.getSchemaVertex(JanusGraphSchemaCategory.GRAPHINDEX.getSchemaName(indexName));
                MixedIndexType indexType = (MixedIndexType) indexV.asIndexType();
                StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
                IndexSerializer indexSerializer = janusGraph.getIndexSerializer();

                for (AtlasVertex vertex : vertices) {
                    indexSerializer.reindexElement(vertex.getWrappedElement(), indexType, documentsPerStore);
                }
                searchIndex.restore(documentsPerStore, indexSerializer.getIndexInfoRetriever(tx).get("search"));

            } catch (BackendException e) {
                throw e;
            } finally {
                if (tx != null && tx.isOpen()) {
                    tx.rollback();
                }
            }
        }
        entityMutationService.repairClassificationMappings(new ArrayList<>(guids));
    }

    @PostConstruct
    protected void setupGraph() {
        LOG.info("Initializing graph: ");
        graph = AtlasJanusGraphDatabase.getGraphInstance();
        try {
            searchIndex = new AtlanElasticSearchIndex();
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Graph Initialized!");
    }

    public void restoreSelective(String guid, Map<String, AtlasEntity> referredEntities) throws Exception {
        Set<String> referencedGUIDs = new HashSet<>(getEntityAndReferenceGuids(guid, referredEntities));
        LOG.info("processing referencedGuids => " + referencedGUIDs);

        StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
        IndexSerializer indexSerializer = janusGraph.getIndexSerializer();

        for (String indexName : getIndexes()) {
            LOG.info("Restoring: " + indexName);
            long startTime = System.currentTimeMillis();
            reindexVertex(indexName, indexSerializer, referencedGUIDs);

            LOG.info(": Time taken: " + (System.currentTimeMillis() - startTime) + " ms");
        }
        entityMutationService.repairClassificationMappings(new ArrayList<>(referencedGUIDs));
    }

    public void restoreByIds(Set<String> guids) throws Exception {

        StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
        IndexSerializer indexSerializer = janusGraph.getIndexSerializer();

        for (String indexName : getIndexes()) {
            LOG.info("Restoring: " + indexName);
            long startTime = System.currentTimeMillis();
            reindexVertex(indexName, indexSerializer, guids);

            LOG.info(": Time taken: " + (System.currentTimeMillis() - startTime) + " ms");
            LOG.info(": Done!");
        }
        entityMutationService.repairClassificationMappings(new ArrayList<>(guids));
    }

    private static String[] getIndexes() {
        return new String[]{ INDEX_NAME_VERTEX_INDEX, INDEX_NAME_EDGE_INDEX};
    }

    private void reindexVertex(String indexName, IndexSerializer indexSerializer, Set<String> entityGUIDs) throws Exception {
        Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new java.util.HashMap<>();
        StandardJanusGraphTx tx = null;

        if (LEAN_GRAPH_ENABLED) {
            int chunkSize = 100;

            Iterator<String> iterator = entityGUIDs.iterator();
            while (iterator.hasNext()) {

                Set<AtlasVertex> currentChunk = new HashSet<>(chunkSize);
                int count = 0;

                while (iterator.hasNext() && count < chunkSize) {
                    AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(iterator.next());
                    if (vertex != null) {
                        currentChunk.add(vertex);
                        count++;
                    }
                }

                Map<String, Map<String, Object>> toReIndex = ((AtlasJanusGraph) atlasGraph).getESPropertiesForUpdateFromVertices(currentChunk, this.typeRegistry);
                try {
                    ESConnector.syncToEs(toReIndex, true, null);
                } catch (Exception e){
                    LOG.info("Exception: " + e.getMessage());
                }
            }
        } else {

            try {
                tx = (StandardJanusGraphTx) graph.newTransaction();
                JanusGraphSchemaVertex indexV = tx.getSchemaVertex(JanusGraphSchemaCategory.GRAPHINDEX.getSchemaName(indexName));
                MixedIndexType indexType = (MixedIndexType) indexV.asIndexType();

                for (String entityGuid : entityGUIDs) {
                    for (int attemptCount = 1; attemptCount <= MAX_TRIES_ON_FAILURE; attemptCount++) {
                        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entityGuid);
                        if (vertex != null) {
                            try {
                                indexSerializer.reindexElement(vertex.getWrappedElement(), indexType, documentsPerStore);
                                break;
                            } catch (Exception e) {
                                LOG.info("Exception: " + e.getMessage());
                                LOG.info("Pausing before retry..");
                                Thread.sleep(2000 * attemptCount);
                            }
                        }
                    }
                }
                searchIndex.restore(documentsPerStore, indexSerializer.getIndexInfoRetriever(tx).get("search"));
            } catch (InterruptedException | BackendException e) {
                throw e;
            } finally {
                if (tx != null && tx.isOpen()) {
                    tx.rollback();
                }
            }
        }
    }

    private static Set<String> getEntityAndReferenceGuids(String guid, Map<String, AtlasEntity> referredEntities) throws Exception {
        Set<String> set = new HashSet<>();
        set.add(guid);
        if (referredEntities == null || referredEntities.isEmpty()) {
            return set;
        }
        set.addAll(referredEntities.keySet());
        return set;
    }

}
