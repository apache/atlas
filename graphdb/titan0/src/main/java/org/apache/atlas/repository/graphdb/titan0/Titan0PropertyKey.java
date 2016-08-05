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

import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

import com.thinkaurelius.titan.core.PropertyKey;

/**
 * Titan 0.5.4 implementaiton of AtlasPropertyKey.
 */
public class Titan0PropertyKey implements AtlasPropertyKey {

    private PropertyKey wrappedPropertyKey;

    public Titan0PropertyKey(PropertyKey toWrap) {
        wrappedPropertyKey = toWrap;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.atlas.repository.graphdb.AtlasPropertyKey#getName()
     */
    @Override
    public String getName() {
        return wrappedPropertyKey.getName();
    }

    /**
     * @return
     */
    public PropertyKey getWrappedPropertyKey() {
        return wrappedPropertyKey;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Titan0PropertyKey)) {
            return false;
        }
        Titan0PropertyKey otherKey = (Titan0PropertyKey) other;
        return wrappedPropertyKey.equals(otherKey.wrappedPropertyKey);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + wrappedPropertyKey.hashCode();
        return result;
    }

}
