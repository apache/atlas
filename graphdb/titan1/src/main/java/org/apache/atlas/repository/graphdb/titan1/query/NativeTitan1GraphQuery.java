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

import java.util.Collection;

import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.titan.query.NativeTitanGraphQuery;
import org.apache.atlas.repository.graphdb.titan1.Titan1Edge;
import org.apache.atlas.repository.graphdb.titan1.Titan1Graph;
import org.apache.atlas.repository.graphdb.titan1.Titan1GraphDatabase;
import org.apache.atlas.repository.graphdb.titan1.Titan1Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;

import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/**
 * Titan 1.0.0 implementation of NativeTitanGraphQuery.
 */
public class NativeTitan1GraphQuery implements NativeTitanGraphQuery<Titan1Vertex, Titan1Edge> {

    private Titan1Graph graph;
    private TitanGraphQuery<?> query;

    public NativeTitan1GraphQuery(Titan1Graph graph) {
        this.query = Titan1GraphDatabase.getGraphInstance().query();
        this.graph = graph;
    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices() {
        Iterable<TitanVertex> it = query.vertices();
        return graph.wrapVertices(it);
    }

    @Override
    public void in(String propertyName, Collection<? extends Object> values) {
        query.has(propertyName, Contain.IN, values);

    }

    @Override
    public void has(String propertyName, ComparisionOperator op, Object value) {

        Compare c = getGremlinPredicate(op);
        TitanPredicate pred = TitanPredicate.Converter.convert(c);
        query.has(propertyName, pred, value);
    }

    private Compare getGremlinPredicate(ComparisionOperator op) {
        switch (op) {
        case EQUAL:
            return Compare.eq;
        case GREATER_THAN_EQUAL:
            return Compare.gte;
        case LESS_THAN_EQUAL:
            return Compare.lte;
        case NOT_EQUAL:
            return Compare.neq;

        default:
            throw new RuntimeException("Unsupported comparison operator:" + op);
        }
    }

}
