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

package org.apache.hadoop.metadata.repository.graph;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.storage.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.UUID;

/**
 * Utility class for graph operations.
 */
public final class GraphHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphHelper.class);

    private GraphHelper() {
    }

    public static Vertex createVertex(Graph graph,
                                      ITypedReferenceableInstance typedInstance) {
        return createVertex(graph, typedInstance, typedInstance.getId());
    }

    public static Vertex createVertex(Graph graph,
                                      ITypedInstance typedInstance,
                                      Id typedInstanceId) {
        return createVertex(graph, typedInstance.getTypeName(), typedInstanceId);
    }

    public static Vertex createVertex(Graph graph,
                                      String typeName,
                                      Id typedInstanceId) {
        final Vertex instanceVertex = graph.addVertex(null);
        // type
        instanceVertex.setProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);

        // id
        final String guid = UUID.randomUUID().toString();
        instanceVertex.setProperty(Constants.GUID_PROPERTY_KEY, guid);

        // version
        instanceVertex.setProperty(Constants.VERSION_PROPERTY_KEY, typedInstanceId.version);

        return instanceVertex;
    }

    public static Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        LOG.debug("Adding edge for {} -> struct label {} -> v{}",
                fromVertex, edgeLabel, toVertex);
        return fromVertex.addEdge(edgeLabel, toVertex);
    }

    public static Vertex findVertexByGUID(Graph blueprintsGraph,
                                          String value) {
        LOG.debug("Finding vertex for key={}, value={}", Constants.GUID_PROPERTY_KEY, value);

        GraphQuery query = blueprintsGraph.query()
                .has(Constants.GUID_PROPERTY_KEY, Compare.EQUAL, value);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since guid should be unique
        return results.hasNext() ? results.next() : null;
    }

    public static String vertexString(final Vertex vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            properties.append(propertyKey)
                    .append("=").append(vertex.getProperty(propertyKey))
                    .append(", ");
        }

        return "v[" + vertex.getId() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final Edge edge) {
        return "e[" + edge.getLabel() + "], ["
                + edge.getVertex(Direction.OUT)
                + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN)
                + "]";
    }

    public static void dumpToLog(final Graph graph) {
        LOG.debug("*******************Graph Dump****************************");
        LOG.debug("Vertices of {}", graph);
        for (Vertex vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (Edge edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
        LOG.debug("*******************Graph Dump****************************");
    }
}