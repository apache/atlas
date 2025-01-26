/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents an Index in Janus.
 */
public class AtlasJanusGraphIndex implements AtlasGraphIndex {
    private final JanusGraphIndex wrapped;

    public AtlasJanusGraphIndex(JanusGraphIndex toWrap) {
        this.wrapped = toWrap;
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null || other.getClass() != getClass()) {
            return false;
        }

        AtlasJanusGraphIndex otherKey = (AtlasJanusGraphIndex) other;

        return otherKey.wrapped.equals(wrapped);
    }

    @Override
    public boolean isMixedIndex() {
        return wrapped.isMixedIndex();
    }

    @Override
    public boolean isCompositeIndex() {
        return wrapped.isCompositeIndex();
    }

    @Override
    public boolean isEdgeIndex() {
        return Edge.class.isAssignableFrom(wrapped.getIndexedElement());
    }

    @Override
    public boolean isVertexIndex() {
        return Vertex.class.isAssignableFrom(wrapped.getIndexedElement());
    }

    @Override
    public boolean isUnique() {
        return wrapped.isUnique();
    }

    @Override
    public Set<AtlasPropertyKey> getFieldKeys() {
        PropertyKey[]         keys   = wrapped.getFieldKeys();
        Set<AtlasPropertyKey> result = new HashSet<>();

        for (PropertyKey key : keys) {
            result.add(GraphDbObjectFactory.createPropertyKey(key));
        }

        return result;
    }
}
