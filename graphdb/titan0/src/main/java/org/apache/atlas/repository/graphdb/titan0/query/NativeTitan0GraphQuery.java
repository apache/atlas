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

import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.MatchingOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.QueryOperator;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopGraphQuery;
import org.apache.atlas.repository.graphdb.titan0.Titan0Edge;
import org.apache.atlas.repository.graphdb.titan0.Titan0Graph;
import org.apache.atlas.repository.graphdb.titan0.Titan0GraphDatabase;
import org.apache.atlas.repository.graphdb.titan0.Titan0Vertex;

import java.util.*;

/**
 * Titan 0.5.4 implementation of NativeTitanGraphQuery.
 *
 * @author jeff
 *
 */
public class NativeTitan0GraphQuery implements NativeTinkerpopGraphQuery<Titan0Vertex, Titan0Edge> {

    private Titan0Graph graph;
    private TitanGraphQuery<?> query;

    public NativeTitan0GraphQuery(Titan0Graph graph) {
        query = Titan0GraphDatabase.getGraphInstance().query();
        this.graph = graph;
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices() {
        Iterable it = query.vertices();
        return graph.wrapVertices(it);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges() {
        Iterable it = query.edges();
        return graph.wrapEdges(it);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges(int limit) {
        Iterable it = query.limit(limit).edges();
        return graph.wrapEdges(it);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges(int offset, int limit) {
        List<Edge>     result = new ArrayList<>(limit);
        Iterator<Edge> iter   = query.limit(offset + limit).edges().iterator();

        for (long resultIdx = 0; iter.hasNext() && result.size() < limit; resultIdx++) {
            if (resultIdx < offset) {
                continue;
            }

            result.add(iter.next());
        }

        return graph.wrapEdges(result);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices(int limit) {
        Iterable it = query.limit(limit).vertices();
        return graph.wrapVertices(it);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices(int offset, int limit) {
        List<Vertex>     result = new ArrayList<>(limit);
        Iterator<Vertex> iter   = query.limit(offset + limit).vertices().iterator();

        for (long resultIdx = 0; iter.hasNext() && result.size() < limit; resultIdx++) {
            if (resultIdx < offset) {
                continue;
            }

            result.add(iter.next());
        }

        return graph.wrapVertices(result);
    }


    @Override
    public void in(String propertyName, Collection<?> values) {
        query.has(propertyName, Contain.IN, values);

    }

    @Override
    public void has(String propertyName, QueryOperator op, Object value) {
        TitanPredicate pred;
        if (op instanceof ComparisionOperator) {
            Compare c = getGremlinPredicate((ComparisionOperator) op);
            pred = TitanPredicate.Converter.convert(c);
        } else {
            pred = getGremlinPredicate((MatchingOperator) op);
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
                return Compare.EQUAL;
            case GREATER_THAN:
                return Compare.GREATER_THAN;
            case GREATER_THAN_EQUAL:
                return Compare.GREATER_THAN_EQUAL;
            case LESS_THAN:
                return Compare.LESS_THAN;
            case LESS_THAN_EQUAL:
                return Compare.LESS_THAN_EQUAL;
            case NOT_EQUAL:
                return Compare.NOT_EQUAL;
            default:
                throw new RuntimeException("Unsupported comparison operator:" + op);
        }
    }

}
