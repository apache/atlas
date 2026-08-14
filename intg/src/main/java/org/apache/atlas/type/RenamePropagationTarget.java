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
package org.apache.atlas.type;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Typedef-time representation of a rename propagation target for an entity type.
 */
public class RenamePropagationTarget {
    private final String                         targetTypeName;
    private final AtlasStructType.AtlasAttribute relAttr;
    private final List<Map<String, String>>      propagateAttributes;

    public RenamePropagationTarget(String targetTypeName, AtlasStructType.AtlasAttribute relAttr,
                                   List<Map<String, String>> propagateAttributes) {
        this.targetTypeName       = targetTypeName;
        this.relAttr              = relAttr;
        this.propagateAttributes  = propagateAttributes != null ? Collections.unmodifiableList(propagateAttributes) : Collections.emptyList();
    }

    public String getTargetTypeName() {
        return targetTypeName;
    }

    public AtlasStructType.AtlasAttribute getRelAttr() {
        return relAttr;
    }

    public List<Map<String, String>> getPropagateAttributes() {
        return propagateAttributes;
    }

    /**
     * Same target type and relationship attribute instance as wired on this type.
     * {@link #propagateAttributes} is not part of equality — duplicates are detected before add.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RenamePropagationTarget that = (RenamePropagationTarget) o;
        return Objects.equals(targetTypeName, that.targetTypeName)
                && relAttr == that.relAttr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetTypeName, System.identityHashCode(relAttr));
    }
}
