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

import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * Titan 0.5.4 implementation of AtlasIndexQuery.
 */
public class Titan0IndexQuery implements AtlasIndexQuery<Titan0Vertex, Titan0Edge> {

    private Titan0Graph graph;
    private TitanIndexQuery wrappedIndexQuery;


    public Titan0IndexQuery(Titan0Graph graph, TitanIndexQuery query) {
        wrappedIndexQuery = query;
        this.graph = graph;
    }

    @Override
    public Iterator<AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge>> vertices() {
        Iterator<TitanIndexQuery.Result<Vertex>> results = wrappedIndexQuery.vertices().iterator();

        Function<TitanIndexQuery.Result<Vertex>, AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge>> function =
                new Function<TitanIndexQuery.Result<Vertex>, AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge>>() {

                @Override
                public AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge> apply(TitanIndexQuery.Result<Vertex> source) {
                    return new ResultImpl(source);
                }
            };
        return Iterators.transform(results, function);
    }

    @Override
    public Iterator<Result<Titan0Vertex, Titan0Edge>> vertices(int offset, int limit) {
        Preconditions.checkArgument(offset >=0, "Index offset should be greater than or equals to 0");
        Preconditions.checkArgument(limit >=0, "Index limit should be greater than or equals to 0");
        Iterator<TitanIndexQuery.Result<Vertex>> results = wrappedIndexQuery
                .offset(offset)
                .limit(limit)
                .vertices().iterator();

        Function<TitanIndexQuery.Result<Vertex>, AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge>> function =
                new Function<TitanIndexQuery.Result<Vertex>, AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge>>() {

                    @Override
                    public AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge> apply(TitanIndexQuery.Result<Vertex> source) {
                        return new ResultImpl(source);
                    }
                };
        return Iterators.transform(results, function);
    }

    private final class ResultImpl implements AtlasIndexQuery.Result<Titan0Vertex, Titan0Edge> {
        private TitanIndexQuery.Result<Vertex> wrappedResult;

        ResultImpl(TitanIndexQuery.Result<Vertex> source) {
            wrappedResult = source;
        }

        @Override
        public AtlasVertex<Titan0Vertex, Titan0Edge> getVertex() {
            return GraphDbObjectFactory.createVertex(graph, wrappedResult.getElement());
        }

        @Override
        public double getScore() {
            return wrappedResult.getScore();
        }
    }
}
