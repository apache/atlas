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
import org.apache.atlas.AtlasConstants;

import java.util.Arrays;
import java.util.Objects;

public final class EnumTypeDefinition {

    public final String name;
    public final String description;
    public final String version;
    public final EnumValue[] enumValues;

    public EnumTypeDefinition(String name, EnumValue... enumValues) {
        this(name, null, AtlasConstants.DEFAULT_TYPE_VERSION, enumValues);
    }

    public EnumTypeDefinition(String name, String description, EnumValue... enumValues) {
        this(name, description, AtlasConstants.DEFAULT_TYPE_VERSION, enumValues);
    }

    public EnumTypeDefinition(String name, String description, String version, EnumValue... enumValues) {
        this.name = ParamChecker.notEmpty(name, "Enum type name");
        this.description = description;
        this.enumValues = ParamChecker.notNullElements(enumValues, "Enum values");
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnumTypeDefinition that = (EnumTypeDefinition) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(description, that.description) &&
                Objects.equals(version, that.version) &&
                Arrays.equals(enumValues, that.enumValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, version, enumValues);
    }
}
