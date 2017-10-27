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
package org.apache.atlas.repository.graphdb.titan1.query;

import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.tinkerpop.query.TinkerpopGraphQuery;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopGraphQuery;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopQueryFactory;
import org.apache.atlas.repository.graphdb.titan1.Titan1Edge;
import org.apache.atlas.repository.graphdb.titan1.Titan1Graph;
import org.apache.atlas.repository.graphdb.titan1.Titan1Vertex;
/**
 * Titan 1.0.0 implementation of TitanGraphQuery.
 */
public class Titan1GraphQuery extends TinkerpopGraphQuery<Titan1Vertex, Titan1Edge>
        implements NativeTinkerpopQueryFactory<Titan1Vertex, Titan1Edge> {

    public Titan1GraphQuery(Titan1Graph graph, boolean isChildQuery) {
        super(graph, isChildQuery);
    }

    public Titan1GraphQuery(Titan1Graph graph) {
        super(graph);
    }

    @Override
    public AtlasGraphQuery<Titan1Vertex, Titan1Edge> createChildQuery() {
        return new Titan1GraphQuery((Titan1Graph) graph, true);
    }

    @Override
    protected NativeTinkerpopQueryFactory<Titan1Vertex, Titan1Edge> getQueryFactory() {
        return this;
    }

    @Override
    public NativeTinkerpopGraphQuery<Titan1Vertex, Titan1Edge> createNativeTinkerpopQuery() {
        return new NativeTitan1GraphQuery((Titan1Graph) graph);
    }
}
