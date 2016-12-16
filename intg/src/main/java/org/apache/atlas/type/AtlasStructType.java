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
package org.apache.atlas.type;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_REF_ATTRIBUTE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_MAPPED_FROM_REF;


/**
 * class that implements behaviour of a struct-type.
 */
public class AtlasStructType extends AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    private final AtlasStructDef structDef;

    private Map<String, AtlasType>         attrTypes               = Collections.emptyMap();
    private Set<String>                    foreignKeyAttributes    = new HashSet<>();
    private Map<String, TypeAttributePair> mappedFromRefAttributes = new HashMap<>();


    public AtlasStructType(AtlasStructDef structDef) {
        super(structDef);

        this.structDef = structDef;
    }

    public AtlasStructType(AtlasStructDef structDef, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super(structDef);

        this.structDef = structDef;

        this.resolveReferences(typeRegistry);
    }

    public AtlasStructDef getStructDef() { return structDef; }

    public AtlasType getAttributeType(String attributeName) { return attrTypes.get(attributeName); }

    public AtlasAttributeDef getAttributeDef(String attributeName) { return structDef.getAttribute(attributeName); }

    public boolean isForeignKeyAttribute(String attributeName) {
        return foreignKeyAttributes.contains(attributeName);
    }

    public boolean isMappedFromRefAttribute(String attributeName) {
        return mappedFromRefAttributes.containsKey(attributeName);
    }

    public String getMappedFromRefAttribute(String typeName, String attribName) {
        String ret = null;

        for (Map.Entry<String, TypeAttributePair> e : mappedFromRefAttributes.entrySet()) {
            String refTypeName   = e.getValue().typeName;
            String refAttribName = e.getValue().attributeName;

            if(StringUtils.equals(refTypeName, typeName) && StringUtils.equals(refAttribName, attribName)) {
                ret = e.getKey();

                break;
            }
        }

        return ret;
    }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        Map<String, AtlasType> a = new HashMap<>();

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            AtlasType attrType = typeRegistry.getType(attributeDef.getTypeName());

            resolveConstraints(attributeDef, attrType);

            Cardinality cardinality = attributeDef.getCardinality();

            if (cardinality == Cardinality.LIST || cardinality == Cardinality.SET) {
                if (!(attrType instanceof AtlasArrayType)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_ATTRIBUTE_TYPE_FOR_CARDINALITY,
                                                 getTypeName(), attributeDef.getName());
                }

                AtlasArrayType arrayType = (AtlasArrayType)attrType;

                arrayType.setMinCount(attributeDef.getValuesMinCount());
                arrayType.setMaxCount(attributeDef.getValuesMaxCount());
            }

            a.put(attributeDef.getName(), attrType);
        }

        this.attrTypes = Collections.unmodifiableMap(a);
    }

    @Override
    public AtlasStruct createDefaultValue() {
        AtlasStruct ret = new AtlasStruct(structDef.getName());

        populateDefaultValues(ret);

        return  ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            if (obj instanceof AtlasStruct) {
                AtlasStruct structObj = (AtlasStruct) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    if (!isAssignableValue(structObj.getAttribute(attributeDef.getName()), attributeDef)) {
                        return false;
                    }
                }
            } else if (obj instanceof Map) {
                Map map = (Map) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    if (!isAssignableValue(map.get(attributeDef.getName()), attributeDef)) {
                        return false; // no value for non-optinal attribute
                    }
                }
            } else {
                return false; // invalid type
            }
        }

        return true;
    }

    @Override
    public Object getNormalizedValue(Object obj) {
        Object ret = null;

        if (obj != null) {
            if (isValidValue(obj)) {
                if (obj instanceof AtlasStruct) {
                    normalizeAttributeValues((AtlasStruct) obj);
                    ret = obj;
                } else if (obj instanceof Map) {
                    normalizeAttributeValues((Map) obj);
                    ret = obj;
                }
            }
        }

        return ret;
    }

    @Override
    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            if (obj instanceof AtlasStruct) {
                AtlasStruct structObj = (AtlasStruct) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    String    attrName = attributeDef.getName();
                    AtlasType dataType = attrTypes.get(attributeDef.getName());

                    if (dataType != null) {
                        Object value     = structObj.getAttribute(attrName);
                        String fieldName = objName + "." + attrName;

                        if (value != null) {
                            ret = dataType.validateValue(value, fieldName, messages) && ret;
                        } else if (!attributeDef.getIsOptional()) {
                            ret = false;

                            messages.add(fieldName + ": mandatory attribute value missing in type " + getTypeName());
                        }
                    }
                }
            } else if (obj instanceof Map) {
                Map map = (Map) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    String    attrName = attributeDef.getName();
                    AtlasType dataType = attrTypes.get(attributeDef.getName());

                    if (dataType != null) {
                        Object value     = map.get(attrName);
                        String fieldName = objName + "." + attrName;

                        if (value != null) {
                            ret = dataType.validateValue(value, fieldName, messages) && ret;
                        } else if (!attributeDef.getIsOptional()) {
                            ret = false;

                            messages.add(fieldName + ": mandatory attribute value missing in type " + getTypeName());
                        }
                    }
                }
            } else {
                ret = false;

                messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
            }
        }

        return ret;
    }

    public void normalizeAttributeValues(AtlasStruct obj) {
        if (obj != null) {
            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                String attributeName = attributeDef.getName();

                if (obj.hasAttribute(attributeName)) {
                    Object attributeValue = getNormalizedValue(obj.getAttribute(attributeName), attributeDef);

                    obj.setAttribute(attributeName, attributeValue);
                } else if (!attributeDef.getIsOptional()) {
                    obj.setAttribute(attributeName, createDefaultValue(attributeDef));
                }
            }
        }
    }

    public void normalizeAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                String attributeName = attributeDef.getName();

                if (obj.containsKey(attributeName)) {
                    Object attributeValue = getNormalizedValue(obj.get(attributeName), attributeDef);

                    obj.put(attributeName, attributeValue);
                } else if (!attributeDef.getIsOptional()) {
                    obj.put(attributeName, createDefaultValue(attributeDef));
                }
            }
        }
    }

    public void populateDefaultValues(AtlasStruct obj) {
        if (obj != null) {
            Map<String, Object> attributes = obj.getAttributes();

            if (attributes == null) {
                attributes = new HashMap<>();
            }

            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                if (!attributeDef.getIsOptional()) {
                    attributes.put(attributeDef.getName(), createDefaultValue(attributeDef));
                }
            }

            obj.setAttributes(attributes);
        }
    }

    private Object createDefaultValue(AtlasAttributeDef attributeDef) {
        Object ret = null;

        if (attributeDef != null) {
            AtlasType dataType = attrTypes.get(attributeDef.getName());

            if (dataType != null) {
                ret = dataType.createDefaultValue();
            }
        }

        return ret;
    }

    private boolean isAssignableValue(Object value, AtlasAttributeDef attributeDef) {
        boolean ret = true;

        if (value != null) {
            AtlasType attrType = attrTypes.get(attributeDef.getName());

            if (attrType != null) {
                if (!attrType.isValidValue(value)) {
                    ret = false; // invalid value
                }
            }
        } else if (!attributeDef.getIsOptional()) {
            ret = false; // mandatory attribute not present
        }

        return ret;
    }

    private Object getNormalizedValue(Object value, AtlasAttributeDef attributeDef) {
        AtlasType attrType = attrTypes.get(attributeDef.getName());

        if (attrType != null) {
            if (value == null) {
                if (!attributeDef.getIsOptional()) {
                    return attrType.createDefaultValue();
                }
            } else {
                return attrType.getNormalizedValue(value);
            }
        }

        return null;
    }

    private void resolveConstraints(AtlasAttributeDef attribDef, AtlasType attribType) throws AtlasBaseException {
        if (attribDef == null || CollectionUtils.isEmpty(attribDef.getConstraintDefs()) || attribType == null) {
            return;
        }

        for (AtlasConstraintDef constraintDef : attribDef.getConstraintDefs()) {
            String constraintType = constraintDef != null ? constraintDef.getType() : null;

            if (StringUtils.isBlank(constraintType)) {
                continue;
            }

            switch (constraintType) {
                case AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY:
                    resolveForeignKeyConstraint(attribDef, constraintDef, attribType);
                    break;
                case CONSTRAINT_TYPE_MAPPED_FROM_REF:
                    resolveMappedFromRefConstraint(attribDef, constraintDef, attribType);
                    break;
                default:
                    throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_CONSTRAINT, constraintType,
                            getTypeName(), attribDef.getName());
            }
        }
    }

    /*
     * valid conditions for foreign-key constraint:
     *  - supported only in entity-type
     *  - attribute should be an entity-type or an array of entity-type
     */
    private void resolveForeignKeyConstraint(AtlasAttributeDef attribDef, AtlasConstraintDef constraintDef,
                                             AtlasType attribType) throws AtlasBaseException {
        if (this.getTypeCategory() != TypeCategory.ENTITY) {
            throw new AtlasBaseException(AtlasErrorCode.UNSUPPORTED_CONSTRAINT,
                    AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY , getTypeName(), attribDef.getName());
        }

        if (attribType.getTypeCategory() == TypeCategory.ARRAY) {
            attribType = ((AtlasArrayType)attribType).getElementType();
        }

        if (attribType.getTypeCategory() != TypeCategory.ENTITY) {
            throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_SATISFIED,
                    getTypeName(), attribDef.getName(), AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY,
                    attribType.getTypeName());
        }

        foreignKeyAttributes.add(attribDef.getName());
    }

    /*
     * valid conditions for mapped-from-ref constraint:
     *  - supported only in entity-type
     *  - attribute should be an entity-type or an array of entity-type
     *  - attribute's entity-type should have a foreign-key constraint to this type
     */
    private void resolveMappedFromRefConstraint(AtlasAttributeDef attribDef, AtlasConstraintDef constraintDef,
                                                AtlasType attribType) throws AtlasBaseException {

        if (this.getTypeCategory() != TypeCategory.ENTITY) {
            throw new AtlasBaseException(AtlasErrorCode.UNSUPPORTED_CONSTRAINT, getTypeName(),
                    attribDef.getName(), CONSTRAINT_TYPE_MAPPED_FROM_REF);
        }

        if (attribType.getTypeCategory() == TypeCategory.ARRAY) {
            attribType = ((AtlasArrayType)attribType).getElementType();
        }

        if (attribType.getTypeCategory() != TypeCategory.ENTITY) {
            throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_SATISFIED, getTypeName(),
                    attribDef.getName(), CONSTRAINT_TYPE_MAPPED_FROM_REF, attribDef.getTypeName());
        }

        String refAttribName = AtlasTypeUtil.getStringValue(constraintDef.getParams(), CONSTRAINT_PARAM_REF_ATTRIBUTE);

        if (StringUtils.isBlank(refAttribName)) {
            throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_MISSING_PARAMS,
                    getTypeName(), attribDef.getName(),
                    CONSTRAINT_PARAM_REF_ATTRIBUTE, CONSTRAINT_TYPE_MAPPED_FROM_REF,
                    String.valueOf(constraintDef.getParams()));
        }

        AtlasStructType   structType = (AtlasStructType)attribType;
        AtlasAttributeDef refAttrib  = structType.getAttributeDef(refAttribName);

        if (refAttrib == null) {
            throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_EXIST,
                    getTypeName(), attribDef.getName(),
                    CONSTRAINT_PARAM_REF_ATTRIBUTE, structType.getTypeName(), refAttribName);
        }

        if (!StringUtils.equals(getTypeName(), refAttrib.getTypeName())) {
            throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_MATCHED,
                    getTypeName(), attribDef.getName(),
                    CONSTRAINT_PARAM_REF_ATTRIBUTE, structType.getTypeName(), refAttribName,
                    getTypeName(), refAttrib.getTypeName());
        }

        mappedFromRefAttributes.put(attribDef.getName(), new TypeAttributePair(attribType.getTypeName(), refAttribName));
    }

    private class TypeAttributePair {
        public final String typeName;
        public final String attributeName;

        public TypeAttributePair(String typeName, String attributeName) {
            this.typeName      = typeName;
            this.attributeName = attributeName;
        }
    }
}
