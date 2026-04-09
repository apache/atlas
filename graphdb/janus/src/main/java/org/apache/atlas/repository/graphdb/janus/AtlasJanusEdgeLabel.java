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

import org.apache.atlas.repository.graphdb.AtlasEdgeLabel;
import org.janusgraph.core.EdgeLabel;

/**
 *
 */
public class AtlasJanusEdgeLabel implements AtlasEdgeLabel {
    private final EdgeLabel wrapped;

    public AtlasJanusEdgeLabel(EdgeLabel toWrap) {
        wrapped = toWrap;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasPropertyKey#getName()
     */
    @Override
    public String getName() {
        return wrapped.name();
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

        AtlasJanusEdgeLabel otherKey = (AtlasJanusEdgeLabel) other;

        return otherKey.wrapped.equals(wrapped);
    }
}
