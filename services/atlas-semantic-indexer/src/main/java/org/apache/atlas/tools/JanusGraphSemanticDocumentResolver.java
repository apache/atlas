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
package org.apache.atlas.tools;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JanusGraphSemanticDocumentResolver {
    private JanusGraphSemanticDocumentResolver() {
    }

    public static String resolveVertexIndexDocumentId(String guid) throws AtlasBaseException {
        return resolveVertexIndexDocumentId(AtlasGraphUtilsV2.findByGuid(guid));
    }

    public static String resolveVertexIndexDocumentId(AtlasVertex vertex) throws AtlasBaseException {
        if (vertex == null) {
            return null;
        }

        JanusGraph graph = AtlasJanusGraphDatabase.getGraphInstance();
        if (!(graph instanceof StandardJanusGraph)) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "JanusGraph instance is required to resolve OpenSearch document id");
        }

        StandardJanusGraph janusGraph      = (StandardJanusGraph) graph;
        IndexSerializer    indexSerializer = janusGraph.getIndexSerializer();
        ManagementSystem   mgmt            = null;

        try {
            mgmt = (ManagementSystem) graph.openManagement();
            JanusGraphIndex index     = mgmt.getGraphIndex(Constants.VERTEX_INDEX);
            MixedIndexType  indexType = (MixedIndexType) mgmt.getSchemaVertex(index).asIndexType();

            Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
            indexSerializer.reindexElement(vertex.getWrappedElement(), indexType, documentsPerStore);

            for (Map<String, List<IndexEntry>> storeDocs : documentsPerStore.values()) {
                if (!storeDocs.isEmpty()) {
                    return storeDocs.keySet().iterator().next();
                }
            }

            return null;
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e, "Failed to resolve OpenSearch document id for vertex");
        } finally {
            if (mgmt != null) {
                mgmt.commit();
            }
        }
    }
}
