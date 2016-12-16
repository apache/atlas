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

import java.util.Collection;

import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.titan.query.NativeTitanGraphQuery;
import org.apache.atlas.repository.graphdb.titan0.Titan0GraphDatabase;
import org.apache.atlas.repository.graphdb.titan0.Titan0Edge;
import org.apache.atlas.repository.graphdb.titan0.Titan0Graph;
import org.apache.atlas.repository.graphdb.titan0.Titan0Vertex;

import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.blueprints.Compare;

/**
 * Titan 0.5.4 implementation of NativeTitanGraphQuery.
 *
 * @author jeff
 *
 */
public class NativeTitan0GraphQuery implements NativeTitanGraphQuery<Titan0Vertex, Titan0Edge> {

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
    public void in(String propertyName, Collection<?> values) {
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
            return Compare.EQUAL;
        case GREATER_THAN_EQUAL:
            return Compare.GREATER_THAN_EQUAL;
        case LESS_THAN_EQUAL:
            return Compare.LESS_THAN_EQUAL;
        case NOT_EQUAL:
            return Compare.NOT_EQUAL;

        default:
            throw new RuntimeException("Unsupported comparison operator:" + op);
        }
    }

}
