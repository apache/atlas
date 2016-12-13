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

import org.apache.atlas.AtlasConstants;
import org.apache.atlas.utils.ParamChecker;

import java.util.Arrays;
import java.util.Objects;

public class StructTypeDefinition {

    public final String typeName;
    public final String typeDescription;//optional field
    public final String typeVersion;
    public final AttributeDefinition[] attributeDefinitions;

    protected StructTypeDefinition(String typeName, String typeDescription, boolean validate,
                                   AttributeDefinition... attributeDefinitions) {
        this(typeName, typeDescription, AtlasConstants.DEFAULT_TYPE_VERSION, validate, attributeDefinitions);
    }

    protected StructTypeDefinition(String typeName, String typeDescription, String typeVersion, boolean validate,
                                   AttributeDefinition... attributeDefinitions) {
        this.typeName = ParamChecker.notEmpty(typeName, "Struct type name");
        this.typeDescription = typeDescription;
        if (validate) {
            ParamChecker.notNullElements(attributeDefinitions, "Attribute definitions");
        }
        this.attributeDefinitions = attributeDefinitions;
        this.typeVersion = typeVersion;
    }

    public StructTypeDefinition(String typeName, AttributeDefinition[] attributeDefinitions) {
        this(typeName, null, AtlasConstants.DEFAULT_TYPE_VERSION,  attributeDefinitions);
    }

    public StructTypeDefinition(String typeName, String typeDescription,
        AttributeDefinition[] attributeDefinitions) {

        this(typeName, typeDescription, AtlasConstants.DEFAULT_TYPE_VERSION,  attributeDefinitions);
    }

    public StructTypeDefinition(String typeName, String typeDescription, String typeVersion,
                                AttributeDefinition[] attributeDefinitions) {
        this.typeName = ParamChecker.notEmpty(typeName, "Struct type name");
        this.typeDescription = typeDescription;
        this.typeVersion = typeVersion;
        this.attributeDefinitions = ParamChecker.notNullElements(attributeDefinitions, "Attribute definitions");
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructTypeDefinition that = (StructTypeDefinition) o;
        return Objects.equals(typeName, that.typeName) &&
                Objects.equals(typeDescription, that.typeDescription) &&
                Objects.equals(typeVersion, that.typeVersion) &&
                Arrays.equals(attributeDefinitions, that.attributeDefinitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, typeDescription, typeVersion, attributeDefinitions);
    }
}
