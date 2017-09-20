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

import com.thinkaurelius.titan.core.EdgeLabel;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.titan0.query.Titan0GraphQuery;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Factory that serves up instances of graph database abstraction layer classes
 * that correspond to Titan/Tinkerpop classes.
 */
public final class GraphDbObjectFactory {

    private GraphDbObjectFactory() {

    }

    /**
     * Creates a Titan0Edge that corresponds to the given Gremlin Edge.
     *
     * @param graph The graph the edge should be created in
     * @param source The gremlin edge
     */
    public static Titan0Edge createEdge(Titan0Graph graph, Edge source) {

        if (source == null) {
            return null;
        }
        return new Titan0Edge(graph, source);
    }

    /**
     * Creates a Titan0GraphQuery that corresponds to the given GraphQuery.
     *
     * @param graph the graph that is being quried
     */
    public static Titan0GraphQuery createQuery(Titan0Graph graph) {

        return new Titan0GraphQuery(graph);
    }

    /**
     * Creates a Titan0Vertex that corresponds to the given Gremlin Vertex.
     *
     * @param graph The graph that contains the vertex
     * @param source the Gremlin vertex
     */
    public static Titan0Vertex createVertex(Titan0Graph graph, Vertex source) {

        if (source == null) {
            return null;
        }
        return new Titan0Vertex(graph, source);
    }

    /**
     * @param propertyKey The Gremlin propertyKey.
     *
     */
    public static Titan0PropertyKey createPropertyKey(PropertyKey propertyKey) {
        if (propertyKey == null) {
            return null;
        }
        return new Titan0PropertyKey(propertyKey);
    }

    /**
     * @param label The label.
     *
     */
    public static Titan0EdgeLabel createEdgeLabel(EdgeLabel label) {
        if (label == null) {
            return null;
        }
        return new Titan0EdgeLabel(label);
    }

    /**
     * @param index The gremlin index.
     * @return
     */
    public static AtlasGraphIndex createGraphIndex(TitanGraphIndex index) {
        if (index == null) {
            return null;
        }
        return new Titan0GraphIndex(index);
    }

    /**
     * Converts a Multiplicity to a Cardinality.
     *
     * @param cardinality
     * @return
     */
    public static AtlasCardinality createCardinality(Cardinality cardinality) {

        if (cardinality == Cardinality.SINGLE) {
            return AtlasCardinality.SINGLE;
        } else if (cardinality == Cardinality.LIST) {
            return AtlasCardinality.LIST;
        }
        return AtlasCardinality.SET;
    }

}
