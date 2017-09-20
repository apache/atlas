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
import org.apache.atlas.repository.graphdb.AtlasEdgeLabel;

/**
 * Titan 0.5.4 implementaiton of AtlasEdgeLabel.
 */
public class Titan0EdgeLabel implements AtlasEdgeLabel {
    private final EdgeLabel wrappedEdgeLabel;

    public Titan0EdgeLabel(EdgeLabel toWrap) {
        wrappedEdgeLabel = toWrap;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.atlas.repository.graphdb.AtlasEdgeLabel#getName()
     */
    @Override
    public String getName() {
        return wrappedEdgeLabel.getName();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Titan0EdgeLabel)) {
            return false;
        }
        Titan0EdgeLabel otherLabel = (Titan0EdgeLabel) other;
        return wrappedEdgeLabel.equals(otherLabel.wrappedEdgeLabel);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + wrappedEdgeLabel.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return wrappedEdgeLabel.getName();
    }
}
