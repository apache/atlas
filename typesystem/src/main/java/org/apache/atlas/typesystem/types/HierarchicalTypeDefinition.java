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

import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasConstants;

import java.util.Objects;

public class HierarchicalTypeDefinition<T extends HierarchicalType> extends StructTypeDefinition {

    public final ImmutableSet<String> superTypes;
    public final String hierarchicalMetaTypeName;

    public HierarchicalTypeDefinition(Class<T> hierarchicalMetaType, String typeName, String typeDescription, ImmutableSet<String> superTypes,
            AttributeDefinition[] attributeDefinitions) {
        this(hierarchicalMetaType, typeName, typeDescription, AtlasConstants.DEFAULT_TYPE_VERSION, superTypes,
                attributeDefinitions);
    }

    // Used only for de-serializing JSON String to typedef.
    public HierarchicalTypeDefinition( String hierarchicalMetaTypeName, String typeName, String typeDescription, String typeVersion, String[] superTypes, AttributeDefinition[] attributeDefinitions) throws ClassNotFoundException {
        this((Class<T>) Class.forName(hierarchicalMetaTypeName), typeName, typeDescription, typeVersion, ImmutableSet.copyOf(superTypes), attributeDefinitions);
    }
    // Used only for de-serializing JSON String to typedef (no typeVersion).
    public HierarchicalTypeDefinition( String hierarchicalMetaTypeName, String typeName, String typeDescription, String[] superTypes, AttributeDefinition[] attributeDefinitions) throws ClassNotFoundException {
        this((Class<T>) Class.forName(hierarchicalMetaTypeName), typeName, typeDescription, AtlasConstants.DEFAULT_TYPE_VERSION, ImmutableSet.copyOf(superTypes), attributeDefinitions);
    }
    // Used only for serializing typedef to JSON String.
    public HierarchicalTypeDefinition( String hierarchicalMetaTypeName, String typeName, String typeDescription, String typeVersion, ImmutableSet<String> superTypes, AttributeDefinition[] attributeDefinitions, String typeDef) throws ClassNotFoundException {
        this((Class<T>) Class.forName(hierarchicalMetaTypeName), typeName, typeDescription, typeVersion, superTypes, attributeDefinitions);
    }
    // Used only for serializing typedef to JSON String (no typeVersion).
    public HierarchicalTypeDefinition( String hierarchicalMetaTypeName, String typeName, String typeDescription, ImmutableSet<String> superTypes, AttributeDefinition[] attributeDefinitions, String typeDef) throws ClassNotFoundException {
        this((Class<T>) Class.forName(hierarchicalMetaTypeName), typeName, typeDescription, AtlasConstants.DEFAULT_TYPE_VERSION, superTypes, attributeDefinitions);
    }

    public HierarchicalTypeDefinition(Class<T> hierarchicalMetaType, String typeName, String typeDescription, String typeVersion, ImmutableSet<String> superTypes, AttributeDefinition[] attributeDefinitions) {
        super(typeName, typeDescription, typeVersion, false, attributeDefinitions);
        this.hierarchicalMetaTypeName = hierarchicalMetaType.getName();
        this.superTypes = superTypes == null ? ImmutableSet.<String>of() : superTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        HierarchicalTypeDefinition<?> that = (HierarchicalTypeDefinition<?>) o;
        return Objects.equals(superTypes, that.superTypes) &&
                Objects.equals(hierarchicalMetaTypeName, that.hierarchicalMetaTypeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), superTypes, hierarchicalMetaTypeName);
    }
}
