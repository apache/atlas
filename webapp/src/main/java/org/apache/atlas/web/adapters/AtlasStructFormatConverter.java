/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.adapters;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.*;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Struct;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AtlasStructFormatConverter extends AtlasAbstractFormatConverter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructFormatConverter.class);

    public static final String ATTRIBUTES_PROPERTY_KEY = "attributes";

    public AtlasStructFormatConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry) {
        this(registry, typeRegistry, TypeCategory.STRUCT);
    }

    protected AtlasStructFormatConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry, TypeCategory typeCategory) {
        super(registry, typeRegistry, typeCategory);
    }

    @Override
    public Object fromV1ToV2(Object v1Obj, AtlasType type, ConverterContext converterContext) throws AtlasBaseException {
        AtlasStruct ret = null;

        if (v1Obj != null) {
            AtlasStructType structType = (AtlasStructType)type;

            if (v1Obj instanceof Map) {
                final Map v1Map     = (Map) v1Obj;
                final Map v1Attribs = (Map) v1Map.get(ATTRIBUTES_PROPERTY_KEY);

                if (MapUtils.isNotEmpty(v1Attribs)) {
                    ret = new AtlasStruct(type.getTypeName(), fromV1ToV2(structType, v1Attribs, converterContext));
                } else {
                    ret = new AtlasStruct(type.getTypeName());
                }
            } else if (v1Obj instanceof IStruct) {
                IStruct             struct    = (IStruct) v1Obj;
                Map<String, Object> v1Attribs = null;

                try {
                    v1Attribs = struct.getValuesMap();
                } catch (AtlasException excp) {
                    LOG.error("IStruct.getValuesMap() failed", excp);
                }

                ret = new AtlasStruct(type.getTypeName(), fromV1ToV2(structType, v1Attribs, converterContext));
            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNEXPECTED_TYPE, "Map or IStruct", v1Obj.getClass().getCanonicalName());
            }
        }

        return ret;
    }

    @Override
    public Object fromV2ToV1(Object v2Obj, AtlasType type, ConverterContext converterContext) throws AtlasBaseException {
        Struct ret = null;

        if (v2Obj != null) {
            AtlasStructType structType = (AtlasStructType)type;

            if (v2Obj instanceof Map) {
                final Map v2Map     = (Map) v2Obj;
                final Map v2Attribs;

                if (v2Map.containsKey(ATTRIBUTES_PROPERTY_KEY)) {
                    v2Attribs = (Map) v2Map.get(ATTRIBUTES_PROPERTY_KEY);
                } else {
                    v2Attribs = v2Map;
                }

                if (MapUtils.isNotEmpty(v2Attribs)) {
                    ret = new Struct(type.getTypeName(), fromV2ToV1(structType, v2Attribs, converterContext));
                } else {
                    ret = new Struct(type.getTypeName());
                }
            } else if (v2Obj instanceof AtlasStruct) {
                AtlasStruct struct = (AtlasStruct) v2Obj;

                ret = new Struct(type.getTypeName(), fromV2ToV1(structType, struct.getAttributes(), converterContext));
            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNEXPECTED_TYPE, "Map or AtlasStruct", v2Obj.getClass().getCanonicalName());
            }
        }

        return ret;
    }

    protected Map<String, Object> fromV2ToV1(AtlasStructType structType, Map attributes, ConverterContext context) throws AtlasBaseException {
        Map<String, Object> ret = null;

        if (MapUtils.isNotEmpty(attributes)) {
            ret = new HashMap<>();

            for (AtlasStructType.AtlasAttribute attr : structType.getAllAttributes().values()) {
                AtlasType attrType = attr.getAttributeType();

                if (attrType == null) {
                    LOG.warn("ignored attribute {}.{}: failed to find AtlasType", structType.getTypeName(), attr.getName());
                    continue;
                }

                Object v2Value = attributes.get(attr.getName());
                Object v1Value = null;

                AtlasFormatConverter attrConverter = null;
                if (attrType.getTypeCategory() == TypeCategory.OBJECT_ID_TYPE && !attr.isOwnedRef()) {
                    attrConverter = new AtlasObjectIdConverter(converterRegistry, typeRegistry);
                    v1Value = attrConverter.fromV2ToV1(v2Value, attrType, context);
                } else {
                    attrConverter = converterRegistry.getConverter(attrType.getTypeCategory());
                    v1Value = attrConverter.fromV2ToV1(v2Value, attrType, context);
                }
                ret.put(attr.getName(), v1Value);
            }
        }

        return ret;
    }

    protected Map<String, Object> fromV1ToV2(AtlasStructType structType, Map attributes, ConverterContext context) throws AtlasBaseException {
        Map<String, Object> ret = null;

        if (MapUtils.isNotEmpty(attributes)) {
            ret = new HashMap<>();

            for (AtlasStructType.AtlasAttribute attr : structType.getAllAttributes().values()) {
                AtlasType attrType = attr.getAttributeType();

                if (attrType == null) {
                    LOG.warn("ignored attribute {}.{}: failed to find AtlasType", structType.getTypeName(), attr.getName());
                    continue;
                }

                AtlasFormatConverter attrConverter = converterRegistry.getConverter(attrType.getTypeCategory());
                Object               v1Value       = attributes.get(attr.getName());
                Object               v2Value       = attrConverter.fromV1ToV2(v1Value, attrType, context);

                ret.put(attr.getAttributeDef().getName(), v2Value);
            }
        }

        return ret;
    }
}
