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

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class TitanBootstrap {

    private final TitanGraph graph;

    public TitanBootstrap() {
        graph = TitanFactory.build().set("storage.backend", "inmemory").open();
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

    public static void main(String[] args) {
        TitanBootstrap bootstrap = new TitanBootstrap();
        TitanGraph graph = bootstrap.getGraph();

        try {
            Vertex harish = bootstrap.createVertex();
            harish.setProperty("name", "harish");

            Vertex venkatesh = bootstrap.createVertex();
            venkatesh.setProperty("name", "venkatesh");

            harish.addEdge("buddy", venkatesh);

            for (Vertex v : graph.getVertices()) {
                System.out.println("v = " + vertexString(v));
                for (Edge e : v.getEdges(Direction.OUT)) {
                    System.out.println("e = " + edgeString(e));
                }
            }

            System.out.println("====================");
            GraphQuery graphQuery = graph.query()
                    .has("name", Cmp.EQUAL, "harish");
            for (Vertex v : graphQuery.vertices()) {
                System.out.println("v = " + vertexString(v));
            }

            graphQuery = graph.query()
                    .has("name", Cmp.EQUAL, "venkatesh");
            for (Edge e : graphQuery.edges()) {
                System.out.println("e = " + edgeString(e));
            }


        } finally {
            graph.shutdown();
        }
    }

    public TitanGraph getGraph() {
        return graph;
    }

    public Vertex createVertex() {
        return graph.addVertex(null);
    }
}
