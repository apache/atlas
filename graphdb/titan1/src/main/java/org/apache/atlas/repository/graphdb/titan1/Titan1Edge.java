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
package org.apache.atlas.repository.graphdb.titan1;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * Titan 1.0.0 implementation of AtlasEdge.
 */
public class Titan1Edge extends Titan1Element<Edge> implements AtlasEdge<Titan1Vertex, Titan1Edge> {


    public Titan1Edge(Titan1Graph graph, Edge edge) {
        super(graph, edge);
    }

    @Override
    public String getLabel() {
        return getWrappedElement().label();
    }

    @Override
    public Titan1Edge getE() {

        return this;
    }

    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> getInVertex() {
        return GraphDbObjectFactory.createVertex(graph, getWrappedElement().inVertex());
    }

    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> getOutVertex() {
        return GraphDbObjectFactory.createVertex(graph, getWrappedElement().outVertex());
    }

}
