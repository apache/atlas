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

package org.apache.atlas.query;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.type.*;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

class RegistryBasedLookup implements Lookup {
    private final List<String> errorList;
    private final AtlasTypeRegistry typeRegistry;

    public RegistryBasedLookup(AtlasTypeRegistry typeRegistry) {
        this.errorList = new ArrayList<>();
        this.typeRegistry = typeRegistry;
    }

    @Override
    public AtlasType getType(String typeName) {
        try {
            return typeRegistry.getType(typeName);
        } catch (AtlasBaseException e) {
            addError(e.getMessage());
        }

        return null;
    }

    @Override
    public String getQualifiedName(GremlinQueryComposer.Context context, String name) {
        try {
            AtlasEntityType et = context.getActiveEntityType();
            if(et == null) {
                return "";
            }

            return et.getQualifiedAttributeName(name);
        } catch (AtlasBaseException e) {
            addError(e.getMessage());
        }

        return "";
    }

    @Override
    public boolean isPrimitive(GremlinQueryComposer.Context context, String attributeName) {
        AtlasEntityType et = context.getActiveEntityType();
        if(et == null) {
            return false;
        }

        AtlasType attr = et.getAttributeType(attributeName);
        if(attr == null) {
            return false;
        }

        TypeCategory attrTypeCategory = attr.getTypeCategory();
        return (attrTypeCategory != null) && (attrTypeCategory == TypeCategory.PRIMITIVE || attrTypeCategory == TypeCategory.ENUM);
    }

    @Override
    public String getRelationshipEdgeLabel(GremlinQueryComposer.Context context, String attributeName) {
        AtlasEntityType et = context.getActiveEntityType();
        if(et == null) {
            return "";
        }

        AtlasStructType.AtlasAttribute attr = et.getAttribute(attributeName);
        return (attr != null) ? attr.getRelationshipEdgeLabel() : "";
    }

    @Override
    public boolean hasAttribute(GremlinQueryComposer.Context context, String typeName) {
        return (context.getActiveEntityType() != null) && context.getActiveEntityType().getAttribute(typeName) != null;
    }

    @Override
    public boolean doesTypeHaveSubTypes(GremlinQueryComposer.Context context) {
        return (context.getActiveEntityType() != null && context.getActiveEntityType().getAllSubTypes().size() > 0);
    }

    @Override
    public String getTypeAndSubTypes(GremlinQueryComposer.Context context) {
        String[] str = context.getActiveEntityType() != null ?
                        context.getActiveEntityType().getTypeAndAllSubTypes().toArray(new String[]{}) :
                        new String[]{};
        if(str.length == 0) {
            return null;
        }

        String[] quoted = new String[str.length];
        for (int i = 0; i < str.length; i++) {
            quoted[i] = IdentifierHelper.getQuoted(str[i]);
        }

        return StringUtils.join(quoted, ",");
    }

    @Override
    public boolean isTraitType(GremlinQueryComposer.Context context) {
        return (context.getActiveType() != null &&
                context.getActiveType().getTypeCategory() == TypeCategory.CLASSIFICATION);
    }

    @Override
    public String getTypeFromEdge(GremlinQueryComposer.Context context, String item) {
        AtlasEntityType et = context.getActiveEntityType();
        if(et == null) {
            return "";
        }

        AtlasStructType.AtlasAttribute attr = et.getAttribute(item);
        if(attr == null) {
            return null;
        }

        AtlasType at = attr.getAttributeType();
        if(at.getTypeCategory() == TypeCategory.ARRAY) {
            AtlasArrayType arrType = ((AtlasArrayType)at);
            return ((AtlasBuiltInTypes.AtlasObjectIdType) arrType.getElementType()).getObjectType();
        }

        return context.getActiveEntityType().getAttribute(item).getTypeName();
    }

    @Override
    public boolean isDate(GremlinQueryComposer.Context context, String attributeName) {
        AtlasEntityType et = context.getActiveEntityType();
        if (et == null) {
            return false;
        }

        AtlasType attr = et.getAttributeType(attributeName);
        return attr != null && attr.getTypeName().equals(AtlasBaseTypeDef.ATLAS_TYPE_DATE);

    }

    protected void addError(String s) {
        errorList.add(s);
    }

    @Override
    public List<String> getErrorList() {
        return errorList;
    }
}
