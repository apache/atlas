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

package org.apache.hadoop.metadata.typesystem.types;

import org.apache.hadoop.metadata.MetadataException;

import java.util.Map;

public class AttributeInfo {
    public final String name;
    public final Multiplicity multiplicity;
    public final boolean isComposite;
    public final boolean isUnique;
    public final boolean isIndexable;
    /**
     * If this is a reference attribute, then the name of the attribute on the Class
     * that this refers to.
     */
    public final String reverseAttributeName;
    private IDataType dataType;

    AttributeInfo(TypeSystem t, AttributeDefinition def, Map<String, IDataType> tempTypes) throws MetadataException {
        TypeUtils.validateName(def.name);
        this.name = def.name;
        this.dataType = (tempTypes != null && tempTypes.containsKey(def.dataTypeName)) ?
                tempTypes.get(def.dataTypeName) : t.getDataType(IDataType.class, def.dataTypeName);
        this.multiplicity = def.multiplicity;
        this.isComposite = def.isComposite;
        this.isUnique = def.isUnique;
        this.isIndexable = def.isIndexable;
        this.reverseAttributeName = def.reverseAttributeName;
    }

    public IDataType dataType() {
        return dataType;
    }

    void setDataType(IDataType dT) {
        dataType = dT;
    }

    @Override
    public String toString() {
        return "AttributeInfo{" +
                "name='" + name + '\'' +
                ", dataType=" + dataType +
                ", multiplicity=" + multiplicity +
                ", isComposite=" + isComposite +
                ", isUnique=" + isUnique +
                ", isIndexable=" + isIndexable +
                ", reverseAttributeName='" + reverseAttributeName + '\'' +
                '}';
    }
}
