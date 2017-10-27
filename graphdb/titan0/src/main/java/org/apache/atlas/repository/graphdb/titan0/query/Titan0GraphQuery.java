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
package org.apache.atlas.repository.graphdb.titan0.query;

import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.tinkerpop.query.TinkerpopGraphQuery;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopGraphQuery;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopQueryFactory;
import org.apache.atlas.repository.graphdb.titan0.Titan0Edge;
import org.apache.atlas.repository.graphdb.titan0.Titan0Graph;
import org.apache.atlas.repository.graphdb.titan0.Titan0Vertex;

/**
 * Titan 0.5.4 implementation of AtlasGraphQuery.
 */
public class Titan0GraphQuery extends TinkerpopGraphQuery<Titan0Vertex, Titan0Edge>
    implements NativeTinkerpopQueryFactory<Titan0Vertex, Titan0Edge> {

    public Titan0GraphQuery(Titan0Graph graph, boolean isChildQuery) {
        super(graph, isChildQuery);
    }

    public Titan0GraphQuery(Titan0Graph graph) {
        super(graph);
    }

    @Override
    public AtlasGraphQuery<Titan0Vertex, Titan0Edge> createChildQuery() {
        return new Titan0GraphQuery((Titan0Graph)graph, true);
    }

    @Override
    protected NativeTinkerpopQueryFactory<Titan0Vertex, Titan0Edge> getQueryFactory() {
        return this;
    }


    @Override
    public NativeTinkerpopGraphQuery<Titan0Vertex, Titan0Edge> createNativeTinkerpopQuery() {
        return new NativeTitan0GraphQuery((Titan0Graph)graph);
    }
}
