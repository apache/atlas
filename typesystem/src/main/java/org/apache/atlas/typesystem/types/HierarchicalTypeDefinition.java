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

package org.apache.atlas.typesystem.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.classification.InterfaceAudience;

public class HierarchicalTypeDefinition<T extends HierarchicalType> extends StructTypeDefinition {

    public final ImmutableSet<String> superTypes;
    public final String hierarchicalMetaTypeName;

    /**
     * Used for json deserialization only.
     * not intended public consumption
     * @param hierarchicalMetaTypeName
     * @param typeName
     * @param typeDescription
     * @param superTypes
     * @param attributeDefinitions
     * @throws ClassNotFoundException
     */
    @InterfaceAudience.Private
    public HierarchicalTypeDefinition(String hierarchicalMetaTypeName, String typeName, String typeDescription, String[] superTypes,
            AttributeDefinition[] attributeDefinitions) throws ClassNotFoundException {
        this((Class<T>) Class.forName(hierarchicalMetaTypeName), typeName, typeDescription, ImmutableSet.copyOf(superTypes),
                attributeDefinitions);
    }

    public HierarchicalTypeDefinition(Class<T> hierarchicalMetaType, String typeName, String typeDescription, ImmutableSet<String> superTypes,
        AttributeDefinition[] attributeDefinitions) {
        super(typeName, typeDescription, false, attributeDefinitions);
        hierarchicalMetaTypeName = hierarchicalMetaType.getName();
        this.superTypes = superTypes == null ? ImmutableSet.<String>of() : superTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        HierarchicalTypeDefinition that = (HierarchicalTypeDefinition) o;

        if (!hierarchicalMetaTypeName.equals(that.hierarchicalMetaTypeName)) {
            return false;
        }
        if (!superTypes.equals(that.superTypes)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + superTypes.hashCode();
        result = 31 * result + hierarchicalMetaTypeName.hashCode();
        return result;
    }
}
