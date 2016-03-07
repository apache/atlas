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

import org.apache.atlas.utils.ParamChecker;

import java.util.Arrays;

public class StructTypeDefinition {

    public final String typeName;
    public final String typeDescription;//optional field
    public final AttributeDefinition[] attributeDefinitions;

    protected StructTypeDefinition(String typeName, boolean validate, AttributeDefinition... attributeDefinitions) {
        this(typeName, null, validate, attributeDefinitions);
    }

    protected StructTypeDefinition(String typeName, String typeDescription, boolean validate, AttributeDefinition... attributeDefinitions) {
        this.typeName = ParamChecker.notEmpty(typeName, "Struct type name");
        this.typeDescription = typeDescription;
        if (attributeDefinitions != null && attributeDefinitions.length != 0) {
            ParamChecker.notNullElements(attributeDefinitions, "Attribute definitions");
        }
        this.attributeDefinitions = attributeDefinitions;
    }

    public StructTypeDefinition(String typeName, AttributeDefinition[] attributeDefinitions) {
        this(typeName, null, attributeDefinitions);
    }

    public StructTypeDefinition(String typeName, String typeDescription,
        AttributeDefinition[] attributeDefinitions) {
        this.typeName = ParamChecker.notEmpty(typeName, "Struct type name");
        this.typeDescription = typeDescription;
        this.attributeDefinitions = ParamChecker.notNullElements(attributeDefinitions, "Attribute definitions");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StructTypeDefinition that = (StructTypeDefinition) o;

        if (!Arrays.equals(attributeDefinitions, that.attributeDefinitions)) {
            return false;
        }
        if (!typeName.equals(that.typeName)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = typeName.hashCode();
        result = 31 * result + Arrays.hashCode(attributeDefinitions);
        return result;
    }
}
