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

import com.google.common.base.Preconditions;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * Titan 0.5.4 implementation of AtlasVertexQuery.
 */
public class Titan0VertexQuery implements AtlasVertexQuery<Titan0Vertex, Titan0Edge> {

    private Titan0Graph graph;
    private VertexQuery vertexQuery;

    public Titan0VertexQuery(Titan0Graph graph, VertexQuery vertexQuery) {
        this.vertexQuery = vertexQuery;
        this.graph = graph;
    }

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> direction(AtlasEdgeDirection queryDirection) {
        vertexQuery.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;

    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices() {
        Iterable<Vertex> vertices = vertexQuery.vertices();
        return graph.wrapVertices(vertices);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices(int limit) {
        Preconditions.checkArgument(limit >=0, "Limit should be greater than or equals to 0");
        Iterable<Vertex> vertices = vertexQuery.limit(limit).vertices();
        return graph.wrapVertices(vertices);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges() {
        Iterable<Edge> edges = vertexQuery.edges();
        return graph.wrapEdges(edges);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges(int limit) {
        Preconditions.checkArgument(limit >=0, "Limit should be greater than or equals to 0");
        Iterable<Edge> edges = vertexQuery.limit(limit).edges();
        return graph.wrapEdges(edges);
    }

    @Override
    public long count() {
        return vertexQuery.count();
    }

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> label(String label) {
        vertexQuery.labels(label);
        return this;
    }

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> has(String key, Object value) {
        vertexQuery.has(key, value);
        return this;
    }
}
