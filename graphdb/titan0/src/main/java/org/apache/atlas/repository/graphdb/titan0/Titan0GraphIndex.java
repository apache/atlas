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

import java.util.HashSet;
import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Titan 0.5.4 implementation of AtlasGraphIndex.
 */
public class Titan0GraphIndex implements AtlasGraphIndex {

    private TitanGraphIndex wrappedIndex;

    public Titan0GraphIndex(TitanGraphIndex toWrap) {
        wrappedIndex = toWrap;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphIndex#isMixedIndex()
     */
    @Override
    public boolean isMixedIndex() {
        return wrappedIndex.isMixedIndex();
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphIndex#isEdgeIndex()
     */
    @Override
    public boolean isEdgeIndex() {
        return Edge.class.isAssignableFrom(wrappedIndex.getIndexedElement());
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphIndex#isVertexIndex()
     */
    @Override
    public boolean isVertexIndex() {
        return Vertex.class.isAssignableFrom(wrappedIndex.getIndexedElement());
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphIndex#isCompositeIndex()
     */
    @Override
    public boolean isCompositeIndex() {
        return wrappedIndex.isCompositeIndex();
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphIndex#isUnique()
     */
    @Override
    public boolean isUnique() {
        return wrappedIndex.isUnique();
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphIndex#getFieldKeys()
     */
    @Override
    public Set<AtlasPropertyKey> getFieldKeys() {
        PropertyKey[] keys = wrappedIndex.getFieldKeys();
        Set<AtlasPropertyKey> result = new HashSet<>();
        for(PropertyKey key  : keys) {
            result.add(GraphDbObjectFactory.createPropertyKey(key));
        }
        return result;
    }

}
