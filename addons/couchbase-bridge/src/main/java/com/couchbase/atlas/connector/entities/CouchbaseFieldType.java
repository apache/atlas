/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.atlas.connector.entities;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.json.JsonObject;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.type.AtlasTypeUtil;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public enum CouchbaseFieldType {
    BOOLEAN,
    NUMBER,
    STRING,
    ARRAY,
    OBJECT,
    BINARY;

    public static CouchbaseFieldType infer(@Nonnull Object value) {
        if (value instanceof Map || value instanceof JsonObject) {
            return OBJECT;
        } else if (value instanceof Collection || value.getClass().isArray()) {
            if (value.getClass().isArray() && Byte.class.isAssignableFrom(value.getClass().getComponentType())) {
                return BINARY;
            }
            return ARRAY;
        } else if (value instanceof Number) {
            return NUMBER;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else if (value instanceof String) {
            String sValue = (String) value;
            if ("true".equalsIgnoreCase(sValue) || "false".equalsIgnoreCase(sValue)) {
                return BOOLEAN;
            }
            try {
                Double.parseDouble(sValue);
                return NUMBER;
            } catch (NumberFormatException nfe) {
                return STRING;
            }
        }

        throw new IllegalArgumentException("Failed to infer type");
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.getDefault());
    }

    public static AtlasEnumDef atlasEnumDef() {
        return AtlasTypeUtil.createEnumTypeDef(
                "couchbase_field_type",
                "",
                new AtlasEnumDef.AtlasEnumElementDef("boolean", "", 0),
                new AtlasEnumDef.AtlasEnumElementDef("number", "", 1),
                new AtlasEnumDef.AtlasEnumElementDef("string", "", 2),
                new AtlasEnumDef.AtlasEnumElementDef("array", "", 3),
                new AtlasEnumDef.AtlasEnumElementDef("object", "", 4),
                new AtlasEnumDef.AtlasEnumElementDef("binary", "", 5)
        );
    }
}