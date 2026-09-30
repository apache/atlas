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

import java.util.Objects;

/**
 * Typedef-time representation of a delete propagation target for an entity type.
 */
public class DeletePropagationTarget {
    private final String                         targetTypeName;
    private final AtlasStructType.AtlasAttribute relAttr;

    public DeletePropagationTarget(String targetTypeName, AtlasStructType.AtlasAttribute relAttr) {
        this.targetTypeName = targetTypeName;
        this.relAttr        = relAttr;
    }

    public String getTargetTypeName() {
        return targetTypeName;
    }

    public AtlasStructType.AtlasAttribute getRelAttr() {
        return relAttr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeletePropagationTarget that = (DeletePropagationTarget) o;
        return Objects.equals(targetTypeName, that.targetTypeName)
                && relAttr == that.relAttr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetTypeName, System.identityHashCode(relAttr));
    }
}
