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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Titan 0.5.4 implementation of AtlasVertex.
 */
public class Titan0Vertex extends Titan0Element<Vertex> implements AtlasVertex<Titan0Vertex, Titan0Edge> {

    public Titan0Vertex(Titan0Graph graph, Vertex source) {
        super(graph, source);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges(AtlasEdgeDirection dir, String edgeLabel) {
        Iterable<Edge> titanEdges = wrappedElement.getEdges(TitanObjectFactory.createDirection(dir), edgeLabel);
        return graph.wrapEdges(titanEdges);
    }

    private TitanVertex getAsTitanVertex() {
        return (TitanVertex) wrappedElement;
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges(AtlasEdgeDirection in) {
        Iterable<Edge> titanResult = wrappedElement.getEdges(TitanObjectFactory.createDirection(in));
        return graph.wrapEdges(titanResult);

    }

    @Override
    public <T> T getProperty(String propertyName, Class<T> clazz) {

        if (graph.isMultiProperty(propertyName)) {
            // throw exception in this case to be consistent with Titan 1.0.0
            // behavior.
            throw new IllegalStateException();
        }
        return super.getProperty(propertyName, clazz);
    }

    public <T> void setProperty(String propertyName, T value) {

        try {
            super.setProperty(propertyName, value);
        } catch (UnsupportedOperationException e) {
            // For consistency with Titan 1.0.0, treat sets of multiplicity many
            // properties as adds. Handle this here since this is an uncommon
            // occurrence.
            if (graph.isMultiProperty(propertyName)) {
                addProperty(propertyName, value);
            } else {
                throw e;
            }
        }
    }

    @Override
    public <T> void addProperty(String propertyName, T value) {
        try {
            getAsTitanVertex().addProperty(propertyName, value);
        } catch (SchemaViolationException e) {
            if (getPropertyValues(propertyName, value.getClass()).contains(value)) {
                // follow java set semantics, don't throw an exception if
                // value is already there.
                return;
            }
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public <T> void addListProperty(String propertyName, T value) {
        try {
            getAsTitanVertex().addProperty(propertyName, value);
        } catch (SchemaViolationException e) {
            if (getPropertyValues(propertyName, value.getClass()).contains(value)) {
                // follow java set semantics, don't throw an exception if
                // value is already there.
                return;
            }
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public <T> Collection<T> getPropertyValues(String key, Class<T> clazz) {

        TitanVertex tv = getAsTitanVertex();
        Collection<T> result = new ArrayList<>();
        for (TitanProperty property : tv.getProperties(key)) {
            result.add((T) property.getValue());
        }
        return result;
    }

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> query() {
        return new Titan0VertexQuery(graph, wrappedElement.query());
    }

    @Override
    public Titan0Vertex getV() {

        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Titan0Vertex [id=" + getId() + "]";
    }

}
