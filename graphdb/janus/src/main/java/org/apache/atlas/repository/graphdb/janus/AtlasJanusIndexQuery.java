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
package org.apache.atlas.repository.graphdb.janus;

import com.google.common.collect.Iterators;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphVertex;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Janus implementation of AtlasIndexQuery.
 */
public class AtlasJanusIndexQuery implements AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> {
    private final AtlasJanusGraph      graph;
    private final JanusGraphIndexQuery query;

    public AtlasJanusIndexQuery(AtlasJanusGraph graph, JanusGraphIndexQuery query) {
        this.query = query;
        this.graph = graph;
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices() {
        Iterator<JanusGraphIndexQuery.Result<JanusGraphVertex>> results = query.vertexStream().iterator();

        return Iterators.transform(results, ResultImpl::new);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices(int offset, int limit, String sortBy, Order sortOrder) {
        checkArgument(offset >= 0, "Index offset should be greater than or equals to 0");
        checkArgument(limit >= 0, "Index limit should be greater than or equals to 0");

        Iterator<JanusGraphIndexQuery.Result<JanusGraphVertex>> results = query.orderBy(sortBy, sortOrder)
                .offset(offset)
                .limit(limit)
                .vertexStream().iterator();

        return Iterators.transform(results, ResultImpl::new);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices(int offset, int limit) {
        checkArgument(offset >= 0, "Index offset should be greater than or equals to 0");
        checkArgument(limit >= 0, "Index limit should be greater than or equals to 0");

        Iterator<JanusGraphIndexQuery.Result<JanusGraphVertex>> results = query.offset(offset)
                .limit(limit)
                .vertexStream().iterator();

        return Iterators.transform(results, ResultImpl::new);
    }

    @Override
    public Long vertexTotals() {
        return query.vertexTotals();
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> edges() {
        Iterator<JanusGraphIndexQuery.Result<JanusGraphEdge>> results = query.edgeStream().iterator();

        return Iterators.transform(results, ResultImpl::new);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> edges(int offset, int limit, String sortBy, Order sortOrder) {
        checkArgument(offset >= 0, "Index offset should be greater than or equals to 0");
        checkArgument(limit >= 0, "Index limit should be greater than or equals to 0");

        Iterator<JanusGraphIndexQuery.Result<JanusGraphEdge>> results = query.orderBy(sortBy, sortOrder)
                .offset(offset)
                .limit(limit)
                .edgeStream().iterator();

        return Iterators.transform(results, ResultImpl::new);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> edges(int offset, int limit) {
        checkArgument(offset >= 0, "Index offset should be greater than or equals to 0");
        checkArgument(limit >= 0, "Index limit should be greater than or equals to 0");

        Iterator<JanusGraphIndexQuery.Result<JanusGraphEdge>> results = query.offset(offset)
                .limit(limit)
                .edgeStream().iterator();

        return Iterators.transform(results, ResultImpl::new);
    }

    @Override
    public Long edgeTotals() {
        return query.edgeTotals();
    }

    /**
     * Janus implementation of AtlasIndexQuery.Result.
     */
    public final class ResultImpl implements AtlasIndexQuery.Result<AtlasJanusVertex, AtlasJanusEdge> {
        private final JanusGraphIndexQuery.Result<JanusGraphVertex> vertex;
        private final JanusGraphIndexQuery.Result<JanusGraphEdge>   edge;

        public ResultImpl(JanusGraphIndexQuery.Result<?> source) {
            Element element = source.getElement();

            if (element instanceof JanusGraphVertex) {
                this.vertex = (JanusGraphIndexQuery.Result<JanusGraphVertex>) source;
                this.edge   = null;
            } else if (element instanceof JanusGraphEdge) {
                this.vertex = null;
                this.edge   = (JanusGraphIndexQuery.Result<JanusGraphEdge>) source;
            } else {
                this.vertex = null;
                this.edge   = null;
            }
        }

        @Override
        public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getVertex() {
            return GraphDbObjectFactory.createVertex(graph, vertex.getElement());
        }

        @Override
        public AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> getEdge() {
            return GraphDbObjectFactory.createEdge(graph, edge.getElement());
        }

        @Override
        public double getScore() {
            return vertex.getScore();
        }
    }
}
