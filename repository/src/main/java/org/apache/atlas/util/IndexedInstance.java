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
package org.apache.atlas.util;

import org.apache.atlas.v1.model.instance.Referenceable;

/**
 * Data structure that stores an IReferenceableInstance and its location within a list.
 *
 */
public class IndexedInstance {
    private final Referenceable instance;
    private final int           index;

    public IndexedInstance(Referenceable instance, int index) {
        super();
        this.instance = instance;
        this.index    = index;
    }

    public Referenceable getInstance() {
        return instance;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        return instance.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof IndexedInstance)) {
            return false;
        }

        IndexedInstance otherInstance = (IndexedInstance) other;

        return instance.equals(otherInstance.getInstance());
    }
}
