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

import java.util.Iterator;

import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.TitanVertex;

/**
 * Titan 1.0.0 implementation of AtlasIndexQuery.
 */
public class Titan1IndexQuery implements AtlasIndexQuery<Titan1Vertex, Titan1Edge> {

    private Titan1Graph graph;
    private TitanIndexQuery query;

    public Titan1IndexQuery(Titan1Graph graph, TitanIndexQuery query) {
        this.query = query;
        this.graph = graph;
    }

    @Override
    public Iterator<Result<Titan1Vertex, Titan1Edge>> vertices() {
        Iterator<TitanIndexQuery.Result<TitanVertex>> results = query.vertices().iterator();

        Function<TitanIndexQuery.Result<TitanVertex>, Result<Titan1Vertex, Titan1Edge>> function =
            new Function<TitanIndexQuery.Result<TitanVertex>, Result<Titan1Vertex, Titan1Edge>>() {

                @Override
                public Result<Titan1Vertex, Titan1Edge> apply(TitanIndexQuery.Result<TitanVertex> source) {
                    return new ResultImpl(source);
                }
            };

        return Iterators.transform(results, function);
    }

    /**
     * Titan 1.0.0 implementation of AtlasIndexQuery.Result.
     */
    public final class ResultImpl implements AtlasIndexQuery.Result<Titan1Vertex, Titan1Edge> {
        private TitanIndexQuery.Result<TitanVertex> source;

        public ResultImpl(TitanIndexQuery.Result<TitanVertex> source) {
            this.source = source;
        }

        @Override
        public AtlasVertex<Titan1Vertex, Titan1Edge> getVertex() {
            return GraphDbObjectFactory.createVertex(graph, source.getElement());
        }

        @Override
        public double getScore() {
            return source.getScore();
        }
    }
}
