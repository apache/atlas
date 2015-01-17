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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Utility class for graph operations.
 */
public final class GraphUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GraphUtils.class);

    private GraphUtils() {
    }

    public static Edge addEdge(Vertex fromVertex, Vertex toVertex,
                               String vertexPropertyKey, String edgeLabel) {
        return addEdge(fromVertex, toVertex, vertexPropertyKey, edgeLabel, null);
    }

    public static Edge addEdge(Vertex fromVertex, Vertex toVertex,
                               String vertexPropertyKey, String edgeLabel, String timestamp) {
        Edge edge = findEdge(fromVertex, toVertex, vertexPropertyKey, edgeLabel);

        Edge edgeToVertex = edge != null ? edge : fromVertex.addEdge(edgeLabel, toVertex);
        if (timestamp != null) {
            edgeToVertex.setProperty(Constants.TIMESTAMP_PROPERTY_KEY, timestamp);
        }

        return edgeToVertex;
    }

    public static Edge findEdge(Vertex fromVertex, Vertex toVertex,
                                String vertexPropertyKey, String edgeLabel) {
        return findEdge(fromVertex, toVertex.getProperty(vertexPropertyKey),
                vertexPropertyKey, edgeLabel);
    }

    public static Edge findEdge(Vertex fromVertex, Object toVertexName,
                                String vertexPropertyKey, String edgeLabel) {
        Edge edgeToFind = null;
        for (Edge edge : fromVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (edge.getVertex(Direction.IN).getProperty(vertexPropertyKey).equals(toVertexName)) {
                edgeToFind = edge;
                break;
            }
        }

        return edgeToFind;
    }

    public static Vertex findVertex(Graph blueprintsGraph,
                                    String key, String value) {
        LOG.debug("Finding vertex for key={}, value={}", key, value);

        GraphQuery query = blueprintsGraph.query().has(key, value);
        Iterator<Vertex> results = query.vertices().iterator();
        // returning one since name/type is unique
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
                + edge.getVertex(Direction.OUT).getProperty("name")
                + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN).getProperty("name")
                + "]";
    }

    public static void dumpToLog(final Graph graph) {
        LOG.debug("Vertices of {}", graph);
        for (Vertex vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
            System.out.println(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (Edge edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
            System.out.println(edgeString(edge));
        }
    }
}