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
package org.apache.atlas.repository.graphdb.titan0;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Titan 0.5.4 implementation of AtlasEdge.
 */
public class Titan0Edge extends Titan0Element<Edge> implements AtlasEdge<Titan0Vertex, Titan0Edge> {


    public Titan0Edge(Titan0Graph graph, Edge edge) {
        super(graph, edge);
    }

    @Override
    public String getLabel() {
        return wrappedElement.getLabel();
    }

    @Override
    public Titan0Edge getE() {
        return this;
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> getInVertex() {
        Vertex v = wrappedElement.getVertex(Direction.IN);
        return GraphDbObjectFactory.createVertex(graph, v);
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> getOutVertex() {
        Vertex v = wrappedElement.getVertex(Direction.OUT);
        return GraphDbObjectFactory.createVertex(graph, v);
    }

    @Override
    public String toString() {
        return "Titan0Edge [id=" + getId() + "]";
    }
}
