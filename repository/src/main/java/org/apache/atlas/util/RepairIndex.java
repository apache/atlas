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
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RepairIndex {
    private static final Logger LOG = LoggerFactory.getLogger(RepairIndex.class);

    private static final int  MAX_TRIES_ON_FAILURE = 3;

    private static final String INDEX_NAME_VERTEX_INDEX = "vertex_index";
    private static final String INDEX_NAME_FULLTEXT_INDEX = "fulltext_index";
    private static final String INDEX_NAME_EDGE_INDEX = "edge_index";

    private static JanusGraph graph;

    public static void setupGraph() {
        LOG.info("Initializing graph: ");
        graph = AtlasJanusGraphDatabase.getGraphInstance();
        LOG.info("Graph Initialized!");
    }

    private static String[] getIndexes() {
        return new String[]{ INDEX_NAME_VERTEX_INDEX, INDEX_NAME_EDGE_INDEX, INDEX_NAME_FULLTEXT_INDEX};
    }

    private static void reindexVertex(String indexName, IndexSerializer indexSerializer, Set<String> entityGUIDs) throws Exception {
        Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new java.util.HashMap<>();
        ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        StandardJanusGraphTx tx = mgmt.getWrappedTx();
        BackendTransaction mutator = tx.getTxHandle();
        JanusGraphIndex index = mgmt.getGraphIndex(indexName);
        MixedIndexType indexType = (MixedIndexType) mgmt.getSchemaVertex(index).asIndexType();

        for (String entityGuid : entityGUIDs){
            for (int attemptCount = 1; attemptCount <= MAX_TRIES_ON_FAILURE; attemptCount++) {
                AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entityGuid);
                try {
                    indexSerializer.reindexElement(vertex.getWrappedElement(), indexType, documentsPerStore);
                    break;
                } catch (Exception e){
                    LOG.info("Exception: " + e.getMessage());
                    LOG.info("Pausing before retry..");
                    Thread.sleep(2000 * attemptCount);
                }
            }
        }
        mutator.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
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
    }
}
