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

package org.apache.atlas.repository.typestore;

import java.util.Objects;

import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;

/**
 * Records the information needed to create a particular type vertex.
 */
public class TypeVertexInfo {

    private DataTypes.TypeCategory category;
    private String typeName;
    private String typeDescription;

    public TypeVertexInfo(TypeCategory category, String typeName, String typeDescription) {
        super();
        this.category = category;
        this.typeName = typeName;
        this.typeDescription = typeDescription;
    }

    public DataTypes.TypeCategory getCategory() {
        return category;
    }

    public void setCategory(DataTypes.TypeCategory category) {
        this.category = category;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeDescription() {
        return typeDescription;
    }

    public void setTypeDescription(String typeDescription) {
        this.typeDescription = typeDescription;
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, typeName);
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        TypeVertexInfo other = (TypeVertexInfo)obj;
        if(! Objects.equals(category, other.category)) {
            return false;
        }

        if(! Objects.equals(typeName, other.typeName)) {
            return false;
        }

        return true;
    }

}