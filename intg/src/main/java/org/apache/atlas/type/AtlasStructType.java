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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * class that implements behaviour of a struct-type.
 */
public class AtlasStructType extends AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    private final AtlasStructDef structDef;

    private   Map<String, AtlasConstraintDef> foreignKeyAttributes = Collections.emptyMap();
    protected Map<String, AtlasAttribute>     allAttributes        = Collections.emptyMap();

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

    public Set<String> getForeignKeyAttributes() { return foreignKeyAttributes.keySet(); }

    public boolean isForeignKeyAttribute(String attributeName) {
        return foreignKeyAttributes.containsKey(attributeName);
    }

    public AtlasConstraintDef getForeignKeyConstraint(String attributeName) {
        return foreignKeyAttributes.get(attributeName);
    }

    public String getForeignKeyOnDeleteAction(String attributeName) {
        String ret = null;

        AtlasConstraintDef fkConstraint = getForeignKeyConstraint(attributeName);

        if (fkConstraint != null && MapUtils.isNotEmpty(fkConstraint.getParams())) {
            Object onDeleteAction = fkConstraint.getParams().get(AtlasConstraintDef.CONSTRAINT_PARAM_ON_DELETE);

            if (onDeleteAction != null) {
                ret = onDeleteAction.toString();
            }
        }

        return ret;
    }

    public boolean isForeignKeyOnDeleteActionCascade(String attributeName) {
        return StringUtils.equals(getForeignKeyOnDeleteAction(attributeName),
                                  AtlasConstraintDef.CONSTRAINT_PARAM_VAL_CASCADE);
    }

    public boolean isForeignKeyOnDeleteActionUpdate(String attributeName) {
        return StringUtils.equals(getForeignKeyOnDeleteAction(attributeName),
                                  AtlasConstraintDef.CONSTRAINT_PARAM_VAL_UPDATE);
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

        this.allAttributes = Collections.unmodifiableMap(a);

        foreignKeyAttributes = Collections.unmodifiableMap(resolveForeignKeyConstraints(allAttributes.values()));
    }

    @Override
    public void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);

        for (Map.Entry<String, AtlasConstraintDef> e : foreignKeyAttributes.entrySet()) {
            String             attributeName = e.getKey();
            AtlasAttribute     attribute     = getAttribute(attributeName);
            AtlasConstraintDef constraint    = e.getValue();

            AtlasType attributeType = attribute.getAttributeType();

            if (attributeType instanceof AtlasEntityType) {
                ((AtlasEntityType)attributeType).addForeignKeyReference(attribute, constraint);
            }
        }
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
                Map map = (Map) obj;

                for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                    String    attrName = attributeDef.getName();
                    AtlasAttribute attribute = allAttributes.get(attributeDef.getName());

                    if (attribute != null) {
                        AtlasType dataType = attribute.getAttributeType();
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

    /*
     * valid conditions for foreign-key constraint:
     *  - supported only in entity-type
     *  - attribute should be an entity-type or an array of entity-type
     */
    private Map<String, AtlasConstraintDef> resolveForeignKeyConstraints(Collection<AtlasAttribute> attributes)
                                                                                            throws AtlasBaseException {
        Map<String, AtlasConstraintDef> ret = null;

        for (AtlasAttribute attribute : attributes) {
            AtlasAttributeDef attribDef = attribute.getAttributeDef();

            if (CollectionUtils.isEmpty(attribDef.getConstraintDefs())) {
                continue;
            }

            for (AtlasConstraintDef constraintDef : attribDef.getConstraintDefs()) {
                if (!StringUtils.equals(constraintDef.getType(), AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY)) {
                    continue;
                }

                if (this.getTypeCategory() != TypeCategory.ENTITY) {
                    throw new AtlasBaseException(AtlasErrorCode.UNSUPPORTED_CONSTRAINT,
                            AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY, getTypeName(), attribute.getName());
                }

                AtlasType attribType = attribute.getAttributeType();

                if (attribType.getTypeCategory() == TypeCategory.ARRAY) {
                    attribType = ((AtlasArrayType) attribType).getElementType();
                }

                if (attribType.getTypeCategory() != TypeCategory.ENTITY) {
                    throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_SATISFIED,
                            getTypeName(), attribute.getName(), AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY,
                            attribType.getTypeName());
                }

                if (ret == null) {
                    ret = new HashMap<>();
                }

                ret.put(attribute.getName(), constraintDef);

                break;
            }
        }

        return ret == null ? Collections.<String, AtlasConstraintDef>emptyMap() : ret;
    }

    public String getQualifiedAttributeName(String attrName) throws AtlasBaseException {
        if ( allAttributes.containsKey(attrName)) {
            return allAttributes.get(attrName).getQualifiedName();
        }

        throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attrName, structDef.getName());
    }

    public static class AtlasAttribute {

        private final AtlasStructType   structType;
        private final AtlasType         attributeType;
        private final AtlasAttributeDef attributeDef;
        private final String            qualifiedName;

        public AtlasAttribute(AtlasStructType structType, AtlasAttributeDef attrDef, AtlasType attributeType) {
            this.structType    = structType;
            this.attributeDef  = attrDef;
            this.attributeType = attributeType;
            this.qualifiedName = getQualifiedAttributeName(structType.getStructDef(), attributeDef.getName());
        }

        public AtlasStructType getStructType() { return structType; }

        public AtlasStructDef getStructDef() { return structType.getStructDef(); }

        public AtlasType getAttributeType() {
            return attributeType;
        }

        public AtlasAttributeDef getAttributeDef() {
            return attributeDef;
        }

        public String getName() { return attributeDef.getName(); }

        public String getTypeName() { return attributeDef.getTypeName(); }

        public String getQualifiedName() { return qualifiedName; }

        public String getQualifiedAttributeName() {
            return qualifiedName;
        }

        /*
         * "isContainedAttribute" can not be computed and cached in the constructor - as structType is not fully
         * populated at the time AtlasAttribute object is constructed.
         */
        public boolean isContainedAttribute() {
            if ( structType.isForeignKeyOnDeleteActionUpdate(attributeDef.getName()) ) {
                return true;
            }

            if ( structType instanceof AtlasEntityType) {
                return ((AtlasEntityType) structType).isMappedFromRefAttribute(attributeDef.getName());
            }

            return false;
        }

        public static String getQualifiedAttributeName(AtlasStructDef structDef, String attrName) {
            final String typeName = structDef.getName();
            return attrName.contains(".") ? attrName : String.format("%s.%s", typeName, attrName);
        }
    }
}
