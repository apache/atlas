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
package org.apache.atlas.web.adapters.v1;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.web.adapters.AtlasFormatAdapter;
import org.apache.atlas.web.adapters.AtlasFormatConverters;
import org.apache.atlas.web.adapters.AtlasInstanceRestAdapters;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class StructToAtlasStructConverter implements AtlasFormatAdapter {

    protected AtlasFormatConverters registry;

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2);
    }

    @Override
    public Object convert(final String sourceVersion, final String targetVersion, final AtlasType type, final Object source) throws AtlasBaseException {

        if (source != null) {
            if (isStructType(source)) {
                IStruct entity = (IStruct) source;
                //Resolve attributes
                StructToAtlasStructConverter converter = (StructToAtlasStructConverter) registry.getConverter(sourceVersion, targetVersion, TypeCategory.STRUCT);
                return new AtlasStruct(type.getTypeName(), converter.convertAttributes((AtlasStructType) type, entity));
            }

        }
        return null;
    }

    private boolean isStructType(Object o) {
        if (o != null && o instanceof IStruct) {
            return true;
        }
        return false;
    }

    @Override
    public TypeCategory getTypeCategory() {
        return TypeCategory.STRUCT;
    }

    public Map<String, Object> convertAttributes(AtlasStructType structType, Object entity) throws AtlasBaseException {
        Collection<AtlasStructDef.AtlasAttributeDef> attributeDefs;

        if (structType.getTypeCategory() == TypeCategory.STRUCT) {
            attributeDefs = structType.getStructDef().getAttributeDefs();
        } else if (structType.getTypeCategory() == TypeCategory.CLASSIFICATION) {
            attributeDefs = ((AtlasClassificationType)structType).getAllAttributeDefs().values();
        } else if (structType.getTypeCategory() == TypeCategory.ENTITY) {
            attributeDefs = ((AtlasEntityType)structType).getAllAttributeDefs().values();
        } else {
            attributeDefs = Collections.emptyList();
        }

        Map<String, Object> newAttrMap = new HashMap<>();
        for (AtlasStructDef.AtlasAttributeDef attrDef : attributeDefs) {
            AtlasType attrType = structType.getAttributeType(attrDef.getName());
            AtlasFormatAdapter attrConverter = registry.getConverter(AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2, attrType.getTypeCategory());

            Object attrVal = null;
            if ( AtlasFormatConverters.isMapType(entity)) {
                attrVal = ((Map)entity).get(attrDef.getName());
            } else {
                try {
                    attrVal = ((IStruct)entity).get(attrDef.getName());
                } catch (AtlasException e) {
                    throw AtlasInstanceRestAdapters.toAtlasBaseException(e);
                }
            }
            final Object convertedVal = attrConverter.convert(AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2, attrType, attrVal);
            newAttrMap.put(attrDef.getName(), convertedVal);
        }

        return newAttrMap;
    }
}
