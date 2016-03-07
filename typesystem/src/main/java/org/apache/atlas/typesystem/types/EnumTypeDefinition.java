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

public final class EnumTypeDefinition {

    public final String name;
    public final String description;
    public final EnumValue[] enumValues;

    public EnumTypeDefinition(String name, EnumValue... enumValues) {
        this(name, null, enumValues);
    }

    public EnumTypeDefinition(String name, String description, EnumValue... enumValues) {
        this.name = ParamChecker.notEmpty(name, "Enum type name");
        this.description = description;
        this.enumValues = ParamChecker.notNullElements(enumValues, "Enum values");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EnumTypeDefinition that = (EnumTypeDefinition) o;

        if (!Arrays.equals(enumValues, that.enumValues)) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Arrays.hashCode(enumValues);
        return result;
    }
}
