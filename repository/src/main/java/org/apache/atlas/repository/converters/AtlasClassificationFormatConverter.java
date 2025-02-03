/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.converters;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

public class AtlasClassificationFormatConverter extends AtlasStructFormatConverter {
    public AtlasClassificationFormatConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry) {
        super(registry, typeRegistry, TypeCategory.CLASSIFICATION);
    }

    @Override
    public AtlasClassification fromV1ToV2(Object v1Obj, AtlasType type, ConverterContext ctx) throws AtlasBaseException {
        AtlasClassification ret = null;

        if (v1Obj != null) {
            AtlasClassificationType classificationType = (AtlasClassificationType) type;

            if (v1Obj instanceof Map) {
                final Map<String, Object> v1Map     = (Map<String, Object>) v1Obj;
                final Map<String, Object> v1Attribs = (Map<String, Object>) v1Map.get(ATTRIBUTES_PROPERTY_KEY);

                if (MapUtils.isNotEmpty(v1Attribs)) {
                    ret = new AtlasClassification(type.getTypeName(), fromV1ToV2(classificationType, v1Attribs, ctx));
                } else {
                    ret = new AtlasClassification(type.getTypeName());
                }
            } else if (v1Obj instanceof Struct) {
                Struct struct = (Struct) v1Obj;

                ret = new AtlasClassification(type.getTypeName(), fromV1ToV2(classificationType, struct.getValues(), ctx));
            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNEXPECTED_TYPE, "Map or Struct",
                        v1Obj.getClass().getCanonicalName());
            }
        }

        return ret;
    }
}
