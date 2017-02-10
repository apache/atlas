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

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * class that implements behaviour of a struct-type.
 */
public class AtlasStructType extends AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    private final AtlasStructDef structDef;

    protected Map<String, AtlasAttribute> allAttributes = Collections.emptyMap();

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

    public AtlasType getAttributeType(String attributeName) {
        AtlasAttribute attribute = getAttribute(attributeName);

        return attribute != null ? attribute.getAttributeType() : null;
    }

    public AtlasAttributeDef getAttributeDef(String attributeName) {
        AtlasAttribute attribute = getAttribute(attributeName);

        return attribute != null ? attribute.getAttributeDef() : null;
    }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        Map<String, AtlasAttribute> a = new HashMap<>();

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            AtlasType      attrType  = typeRegistry.getType(attributeDef.getTypeName());
            AtlasAttribute attribute = new AtlasAttribute(this, attributeDef, attrType);

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

            a.put(attributeDef.getName(), attribute);
        }

        resolveConstraints(typeRegistry);

        this.allAttributes = Collections.unmodifiableMap(a);
    }

    private void resolveConstraints(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        for (AtlasAttributeDef attributeDef : getStructDef().getAttributeDefs()) {
            if (CollectionUtils.isEmpty(attributeDef.getConstraints())) {
                continue;
            }

            for (AtlasConstraintDef constraint : attributeDef.getConstraints()) {
                if (constraint.isConstraintType(CONSTRAINT_TYPE_OWNED_REF)) {
                    AtlasEntityType attrType = getReferencedEntityType(typeRegistry.getType(attributeDef.getTypeName()));

                    if (attrType == null) {
                        throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_OWNED_REF_ATTRIBUTE_INVALID_TYPE,
                                getTypeName(), attributeDef.getName(), CONSTRAINT_TYPE_OWNED_REF, attributeDef.getTypeName());
                    }
                } else if (constraint.isConstraintType(CONSTRAINT_TYPE_INVERSE_REF)) {
                    AtlasEntityType attrType = getReferencedEntityType(typeRegistry.getType(attributeDef.getTypeName()));

                    if (attrType == null) {
                        throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_INVERSE_REF_ATTRIBUTE_INVALID_TYPE,
                                getTypeName(), attributeDef.getName(), CONSTRAINT_TYPE_INVERSE_REF,
                                attributeDef.getTypeName());
                    }

                    String inverseRefAttrName = AtlasTypeUtil.getStringValue(constraint.getParams(), CONSTRAINT_PARAM_ATTRIBUTE);

                    if (StringUtils.isBlank(inverseRefAttrName)) {
                        throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_MISSING_PARAMS,
                                getTypeName(), attributeDef.getName(),
                                CONSTRAINT_PARAM_ATTRIBUTE, CONSTRAINT_TYPE_INVERSE_REF,
                                String.valueOf(constraint.getParams()));
                    }

                    AtlasAttributeDef inverseRefAttrDef = attrType.getStructDef().getAttribute(inverseRefAttrName);

                    if (inverseRefAttrDef == null) {
                        throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_INVERSE_REF_INVERSE_ATTRIBUTE_NON_EXISTING,
                                getTypeName(), attributeDef.getName(),
                                CONSTRAINT_TYPE_INVERSE_REF, attrType.getTypeName(), inverseRefAttrName);
                    }

                    AtlasEntityType inverseRefAttrType = getReferencedEntityType(typeRegistry.getType(inverseRefAttrDef.getTypeName()));

                    if (inverseRefAttrType == null) {
                        throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_INVERSE_REF_INVERSE_ATTRIBUTE_INVALID_TYPE,
                                getTypeName(), attributeDef.getName(),
                                CONSTRAINT_TYPE_INVERSE_REF, attrType.getTypeName(), inverseRefAttrName);
                    }
                }
            }
        }
    }

    @Override
    public void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);
    }

    @Override
    public AtlasStruct createDefaultValue() {
        AtlasStruct ret = new AtlasStruct(structDef.getName());

        populateDefaultValues(ret);

        return  ret;
    }

    public Map<String, AtlasAttribute> getAllAttributes() {
        return allAttributes;
    }

    public AtlasAttribute getAttribute(String attributeName) {
        return allAttributes.get(attributeName);
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
                return false;
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

                    AtlasAttribute attribute = allAttributes.get(attributeDef.getName());

                    if (attribute != null) {
                        AtlasType dataType = attribute.getAttributeType();
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
                Map attributes = AtlasTypeUtil.toStructAttributes((Map)obj);

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    String    attrName = attributeDef.getName();
                    AtlasAttribute attribute = allAttributes.get(attributeDef.getName());

                    if (attribute != null) {
                        AtlasType dataType = attribute.getAttributeType();
                        Object value     = attributes.get(attrName);
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
            AtlasAttribute attribute = allAttributes.get(attributeDef.getName());

            if (attribute != null) {
                AtlasType dataType = attribute.getAttributeType();

                ret = dataType.createDefaultValue();
            }
        }

        return ret;
    }

    private boolean isAssignableValue(Object value, AtlasAttributeDef attributeDef) {
        boolean ret = true;

        if (value != null) {
            AtlasAttribute attribute = allAttributes.get(attributeDef.getName());

            if (attribute != null) {
                AtlasType attrType = attribute.getAttributeType();

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
        AtlasAttribute attribute = allAttributes.get(attributeDef.getName());

        if (attribute != null) {
            AtlasType attrType = attribute.getAttributeType();

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

    public String getQualifiedAttributeName(String attrName) throws AtlasBaseException {
        if ( allAttributes.containsKey(attrName)) {
            return allAttributes.get(attrName).getQualifiedName();
        }

        throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attrName, structDef.getName());
    }

    private AtlasEntityType getReferencedEntityType(AtlasType type) {
        if (type instanceof AtlasArrayType) {
            type = ((AtlasArrayType)type).getElementType();
        }

        return type instanceof AtlasEntityType ? (AtlasEntityType)type : null;
    }

    public static class AtlasAttribute {
        private final AtlasStructType   definedInType;
        private final AtlasType         attributeType;
        private final AtlasAttributeDef attributeDef;
        private final String            qualifiedName;
        private final String            vertexPropertyName;
        private final boolean           isOwnedRef;
        private final String            inverseRefAttribute;

        public AtlasAttribute(AtlasStructType definedInType, AtlasAttributeDef attrDef, AtlasType attributeType) {
            this.definedInType      = definedInType;
            this.attributeDef       = attrDef;
            this.attributeType      = attributeType;
            this.qualifiedName      = getQualifiedAttributeName(definedInType.getStructDef(), attributeDef.getName());
            this.vertexPropertyName = encodePropertyKey(this.qualifiedName);

            boolean isOwnedRef          = false;
            String  inverseRefAttribute = null;

            if (CollectionUtils.isNotEmpty(attributeDef.getConstraints())) {
                for (AtlasConstraintDef constraint : attributeDef.getConstraints()) {
                    if (constraint.isConstraintType(CONSTRAINT_TYPE_OWNED_REF)) {
                        isOwnedRef = true;
                    }

                    if (constraint.isConstraintType(CONSTRAINT_TYPE_INVERSE_REF)) {
                        Object val = constraint.getParam(CONSTRAINT_PARAM_ATTRIBUTE);

                        if (val != null) {
                            inverseRefAttribute = val.toString();
                        }
                    }
                }
            }

            this.isOwnedRef          = isOwnedRef;
            this.inverseRefAttribute = inverseRefAttribute;
        }

        public AtlasStructType getDefinedInType() { return definedInType; }

        public AtlasStructDef getDefinedInDef() { return definedInType.getStructDef(); }

        public AtlasType getAttributeType() {
            return attributeType;
        }

        public AtlasAttributeDef getAttributeDef() {
            return attributeDef;
        }

        public String getName() { return attributeDef.getName(); }

        public String getTypeName() { return attributeDef.getTypeName(); }

        public String getQualifiedName() { return qualifiedName; }

        public String getVertexPropertyName() { return vertexPropertyName; }

        public boolean isOwnedRef() { return isOwnedRef; }

        public String getInverseRefAttribute() { return inverseRefAttribute; }

        private static String getQualifiedAttributeName(AtlasStructDef structDef, String attrName) {
            final String typeName = structDef.getName();
            return attrName.contains(".") ? attrName : String.format("%s.%s", typeName, attrName);
        }

        private static String encodePropertyKey(String key) {
            if (StringUtils.isBlank(key)) {
                return key;
            }

            for (String[] strMap : RESERVED_CHAR_ENCODE_MAP) {
                key = key.replace(strMap[0], strMap[1]);
            }

            return key;
        }

        private static String decodePropertyKey(String key) {
            if (StringUtils.isBlank(key)) {
                return key;
            }

            for (String[] strMap : RESERVED_CHAR_ENCODE_MAP) {
                key = key.replace(strMap[1], strMap[0]);
            }

            return key;
        }

        private static String[][] RESERVED_CHAR_ENCODE_MAP = new String[][] {
                new String[] { "{",  "_o" },
                new String[] { "}",  "_c" },
                new String[] { "\"", "_q" },
                new String[] { "$",  "_d" },
                new String[] { "%", "_p"  },
        };
    }
}
