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

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.MatchingOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.QueryOperator;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopGraphQuery;
import org.apache.atlas.repository.graphdb.titan1.Titan1Edge;
import org.apache.atlas.repository.graphdb.titan1.Titan1Graph;
import org.apache.atlas.repository.graphdb.titan1.Titan1GraphDatabase;
import org.apache.atlas.repository.graphdb.titan1.Titan1Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Titan 1.0.0 implementation of NativeTinkerpopGraphQuery.
 */
public class NativeTitan1GraphQuery implements NativeTinkerpopGraphQuery<Titan1Vertex, Titan1Edge> {

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
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> edges() {
        Iterable<TitanEdge> it = query.edges();
        return graph.wrapEdges(it);
    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices(int limit) {
        Iterable<TitanVertex> it = query.limit(limit).vertices();
        return graph.wrapVertices(it);
    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices(int offset, int limit) {
        List<Vertex>               result = new ArrayList<>(limit);
        Iterator<? extends Vertex> iter   = query.limit(offset + limit).vertices().iterator();

        for (long resultIdx = 0; iter.hasNext() && result.size() < limit; resultIdx++) {
            if (resultIdx < offset) {
                continue;
            }

            result.add(iter.next());
        }

        return graph.wrapVertices(result);
    }

    @Override
    public void in(String propertyName, Collection<? extends Object> values) {
        query.has(propertyName, Contain.IN, values);

    }

    @Override
    public void has(String propertyName, QueryOperator op, Object value) {
        TitanPredicate pred;
        if (op instanceof ComparisionOperator) {
            Compare c = getGremlinPredicate((ComparisionOperator) op);
            pred = TitanPredicate.Converter.convert(c);
        } else {
            pred = getGremlinPredicate((MatchingOperator)op);
        }
        query.has(propertyName, pred, value);
    }

    private Text getGremlinPredicate(MatchingOperator op) {
        switch (op) {
            case CONTAINS:
                return Text.CONTAINS;
            case PREFIX:
                return Text.PREFIX;
            case SUFFIX:
                return Text.CONTAINS_REGEX;
            case REGEX:
                return Text.REGEX;
            default:
                throw new RuntimeException("Unsupported matching operator:" + op);
        }
    }

    private Compare getGremlinPredicate(ComparisionOperator op) {
        switch (op) {
            case EQUAL:
                return Compare.eq;
            case GREATER_THAN:
                return Compare.gt;
            case GREATER_THAN_EQUAL:
                return Compare.gte;
            case LESS_THAN:
                return Compare.lt;
            case LESS_THAN_EQUAL:
                return Compare.lte;
            case NOT_EQUAL:
                return Compare.neq;

            default:
                throw new RuntimeException("Unsupported comparison operator:" + op);
        }
    }

}
