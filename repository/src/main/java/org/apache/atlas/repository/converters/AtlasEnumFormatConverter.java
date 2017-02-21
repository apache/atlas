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
package org.apache.atlas.repository.converters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.EnumValue;

import java.util.Map;


public class AtlasEnumFormatConverter extends AtlasAbstractFormatConverter {
    public AtlasEnumFormatConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry) {
        super(registry, typeRegistry, TypeCategory.ENUM);
    }

    @Override
    public Object fromV1ToV2(Object v1Obj, AtlasType type, ConverterContext ctx) throws AtlasBaseException {
        String ret = null;

        if (v1Obj == null || !(type instanceof AtlasEnumType)) {
            return ret;
        }

        Object v1Value = null;

        if (v1Obj instanceof EnumValue) {
            EnumValue enumValue = (EnumValue)v1Obj;

            v1Value = enumValue.value;

            if (v1Value == null) {
                v1Value = enumValue.ordinal;
            }
        } else if (v1Obj instanceof Map) {
            Map mapValue = (Map)v1Obj;

            v1Value = mapValue.get("value");

            if (v1Value == null) {
                v1Value = mapValue.get("ordinal");
            }
        }

        if (v1Value == null) { // could be 'value' or 'ordinal'
            v1Value = v1Obj;
        }

        AtlasEnumElementDef elementDef;

        if (v1Value instanceof Number) {
            elementDef = ((AtlasEnumType)type).getEnumElementDef((Number) v1Value);
        } else {
            elementDef = ((AtlasEnumType)type).getEnumElementDef(v1Value.toString());
        }

        if (elementDef != null) {
            ret = elementDef.getValue();
        }

        return ret;
    }

    @Override
    public Object fromV2ToV1(Object v2Obj, AtlasType type, ConverterContext ctx) throws AtlasBaseException {
        EnumValue ret = null;

        if (v2Obj == null || !(type instanceof AtlasEnumType)) {
            return ret;
        }

        AtlasEnumType       enumType   = (AtlasEnumType) type;
        AtlasEnumElementDef elementDef = enumType.getEnumElementDef(v2Obj.toString());

        if (elementDef != null) {
            ret = new EnumValue(elementDef.getValue(), elementDef.getOrdinal());
        }

        return ret;
    }
}

