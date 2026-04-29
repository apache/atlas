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

import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Typedef-time representation of a rename propagation target for an entity type.
 */
public class RenamePropagationTarget {
    private final String               targetTypeName;
    private final RelationshipCategory category;
    private final AtlasStructType.AtlasAttribute relAttr;
    private final List<Map<String, String>>  propagateAttributes;

    public RenamePropagationTarget(String targetTypeName, RelationshipCategory category,
                                   AtlasStructType.AtlasAttribute relAttr, List<Map<String, String>> propagateAttributes) {
        this.targetTypeName    = targetTypeName;
        this.category          = category;
        this.relAttr           = relAttr;
        this.propagateAttributes = propagateAttributes != null ? Collections.unmodifiableList(propagateAttributes) : Collections.emptyList();
    }

    public String getTargetTypeName() {
        return targetTypeName;
    }

    public RelationshipCategory getCategory() {
        return category;
    }

    public AtlasStructType.AtlasAttribute getRelAttr() {
        return relAttr;
    }

    public List<Map<String, String>> getPropagateAttributes() {
        return propagateAttributes;
    }
}
