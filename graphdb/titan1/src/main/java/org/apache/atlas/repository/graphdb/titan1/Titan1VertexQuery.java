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
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;

import com.thinkaurelius.titan.core.TitanVertexQuery;

/**
 * Titan 1.0.0 implementation of AtlasVertexQuery.
 */
public class Titan1VertexQuery implements AtlasVertexQuery<Titan1Vertex, Titan1Edge> {

    private Titan1Graph graph;
    private TitanVertexQuery<?> query;

    public Titan1VertexQuery(Titan1Graph graph, TitanVertexQuery<?> query) {
        this.query = query;
        this.graph = graph;
    }

    @Override
    public AtlasVertexQuery<Titan1Vertex, Titan1Edge> direction(AtlasEdgeDirection queryDirection) {
        query.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;

    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices() {
        Iterable vertices = query.vertices();
        return graph.wrapVertices(vertices);
    }

    @Override
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> edges() {
        Iterable edges = query.edges();
        return graph.wrapEdges(edges);

    }

    @Override
    public long count() {
        return query.count();
    }


}
