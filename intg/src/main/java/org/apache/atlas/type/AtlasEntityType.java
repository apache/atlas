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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasObjectIdType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * class that implements behaviour of an entity-type.
 */
public class AtlasEntityType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityType.class);

    private final AtlasEntityDef entityDef;
    private final String         typeQryStr;

    private static final String INTERNAL_TYPENAME = "__internal";

    private List<AtlasEntityType>                    superTypes                 = Collections.emptyList();
    private Set<String>                              allSuperTypes              = Collections.emptySet();
    private Set<String>                              subTypes                   = Collections.emptySet();
    private Set<String>                              allSubTypes                = Collections.emptySet();
    private Set<String>                              typeAndAllSubTypes         = Collections.emptySet();
    private Set<String>                              typeAndAllSuperTypes       = Collections.emptySet();
    private Map<String, AtlasAttribute>              relationshipAttributes     = Collections.emptyMap();
    private Map<String, List<AtlasRelationshipType>> relationshipAttributesType = Collections.emptyMap();
    private String                                   typeAndAllSubTypesQryStr   = "";
    private boolean                                  isInternalType             = false;


    public AtlasEntityType(AtlasEntityDef entityDef) {
        super(entityDef);

        this.entityDef  = entityDef;
        this.typeQryStr = AtlasAttribute.escapeIndexQueryValue(Collections.singleton(getTypeName()));
    }

    public AtlasEntityType(AtlasEntityDef entityDef, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super(entityDef);

        this.entityDef  = entityDef;
        this.typeQryStr = AtlasAttribute.escapeIndexQueryValue(Collections.singleton(getTypeName()));

        resolveReferences(typeRegistry);
    }

    public AtlasEntityDef getEntityDef() { return entityDef; }

    @Override
    void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferences(typeRegistry);

        List<AtlasEntityType>       s    = new ArrayList<>();
        Set<String>                 allS = new HashSet<>();
        Map<String, AtlasAttribute> allA = new HashMap<>();

        getTypeHierarchyInfo(typeRegistry, allS, allA);

        for (String superTypeName : entityDef.getSuperTypes()) {
            AtlasType superType = typeRegistry.getType(superTypeName);

            if (superType instanceof AtlasEntityType) {
                s.add((AtlasEntityType)superType);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INCOMPATIBLE_SUPERTYPE, superTypeName, entityDef.getName());
            }
        }

        this.superTypes                 = Collections.unmodifiableList(s);
        this.allSuperTypes              = Collections.unmodifiableSet(allS);
        this.allAttributes              = Collections.unmodifiableMap(allA);
        this.uniqAttributes             = getUniqueAttributes(this.allAttributes);
        this.subTypes                   = new HashSet<>(); // this will be populated in resolveReferencesPhase2()
        this.allSubTypes                = new HashSet<>(); // this will be populated in resolveReferencesPhase2()
        this.typeAndAllSubTypes         = new HashSet<>(); // this will be populated in resolveReferencesPhase2()
        this.relationshipAttributes     = new HashMap<>(); // this will be populated in resolveReferencesPhase3()
        this.relationshipAttributesType = new HashMap<>(); // this will be populated in resolveReferencesPhase3()

        this.typeAndAllSubTypes.add(this.getTypeName());

        this.typeAndAllSuperTypes = new HashSet<>(this.allSuperTypes);
        this.typeAndAllSuperTypes.add(this.getTypeName());
        this.typeAndAllSuperTypes = Collections.unmodifiableSet(this.typeAndAllSuperTypes);
    }

    @Override
    void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);

        for (AtlasEntityType superType : superTypes) {
            superType.addSubType(this);
        }

        for (String superTypeName : allSuperTypes) {
            AtlasEntityType superType = typeRegistry.getEntityTypeByName(superTypeName);
            superType.addToAllSubTypes(this);
        }
    }

    @Override
    void resolveReferencesPhase3(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        for (AtlasAttributeDef attributeDef : getStructDef().getAttributeDefs()) {
            String          attributeName       = attributeDef.getName();
            AtlasType       attributeType       = typeRegistry.getType(attributeDef.getTypeName());
            AtlasEntityType attributeEntityType = getReferencedEntityType(attributeType);

            // validate if RelationshipDefs is defined for all entityDefs
            if (attributeEntityType != null && !hasRelationshipAttribute(attributeName)) {
                LOG.warn("No RelationshipDef defined between {} and {} on attribute: {}.{}", getTypeName(),
                          attributeEntityType.getTypeName(), getTypeName(), attributeName);
            }
        }

        for (String superTypeName : allSuperTypes) {
            if (INTERNAL_TYPENAME.equals(superTypeName)) {
                isInternalType = true;
            }

            AtlasEntityType superType = typeRegistry.getEntityTypeByName(superTypeName);

            Map<String, AtlasAttribute> superTypeRelationshipAttributes = superType.getRelationshipAttributes();

            if (MapUtils.isNotEmpty(superTypeRelationshipAttributes)) {
                relationshipAttributes.putAll(superTypeRelationshipAttributes);
            }

            Map<String, List<AtlasRelationshipType>> superTypeRelationshipAttributesType = superType.getRelationshipAttributesType();

            if (MapUtils.isNotEmpty(superTypeRelationshipAttributesType)) {
                relationshipAttributesType.putAll(superTypeRelationshipAttributesType);
            }
        }

        subTypes                   = Collections.unmodifiableSet(subTypes);
        allSubTypes                = Collections.unmodifiableSet(allSubTypes);
        typeAndAllSubTypes         = Collections.unmodifiableSet(typeAndAllSubTypes);
        typeAndAllSubTypesQryStr   = ""; // will be computed on next access
        relationshipAttributes     = Collections.unmodifiableMap(relationshipAttributes);
        relationshipAttributesType = Collections.unmodifiableMap(relationshipAttributesType);

        entityDef.setSubTypes(subTypes);
    }

    public Set<String> getSuperTypes() {
        return entityDef.getSuperTypes();
    }

    public Set<String> getAllSuperTypes() {
        return allSuperTypes;
    }

    public Set<String> getSubTypes() { return subTypes; }

    public Set<String> getAllSubTypes() { return allSubTypes; }

    public Set<String> getTypeAndAllSubTypes() { return typeAndAllSubTypes; }

    public Set<String> getTypeAndAllSuperTypes() { return typeAndAllSuperTypes; }

    public boolean isSuperTypeOf(AtlasEntityType entityType) {
        return entityType != null && allSubTypes.contains(entityType.getTypeName());
    }

    public boolean isSuperTypeOf(String entityTypeName) {
        return StringUtils.isNotEmpty(entityTypeName) && allSubTypes.contains(entityTypeName);
    }

    public boolean isTypeOrSuperTypeOf(String entityTypeName) {
        return StringUtils.isNotEmpty(entityTypeName) && typeAndAllSubTypes.contains(entityTypeName);
    }

    public boolean isSubTypeOf(AtlasEntityType entityType) {
        return entityType != null && allSuperTypes.contains(entityType.getTypeName());
    }

    public boolean isSubTypeOf(String entityTypeName) {
        return StringUtils.isNotEmpty(entityTypeName) && allSuperTypes.contains(entityTypeName);
    }

    public boolean isInternalType() {
        return isInternalType;
    }

    public Map<String, AtlasAttribute> getRelationshipAttributes() { return relationshipAttributes; }

    public AtlasAttribute getRelationshipAttribute(String attributeName) { return relationshipAttributes.get(attributeName); }

    // this method should be called from AtlasRelationshipType.resolveReferencesPhase2()
    void addRelationshipAttribute(String attributeName, AtlasAttribute attribute) {
        relationshipAttributes.put(attributeName, attribute);
    }

    // this method should be called from AtlasRelationshipType.resolveReferencesPhase2()
    void addRelationshipAttributeType(String attributeName, AtlasRelationshipType relationshipType) {
        List<AtlasRelationshipType> relationshipTypes = relationshipAttributesType.get(attributeName);

        if (relationshipTypes == null) {
            relationshipTypes = new ArrayList<>();
            relationshipAttributesType.put(attributeName, relationshipTypes);
        }

        relationshipTypes.add(relationshipType);
    }

    public List<AtlasRelationshipType> getRelationshipAttributeType(String attributeName) {
        return relationshipAttributesType.get(attributeName);
    }

    public Map<String, List<AtlasRelationshipType>> getRelationshipAttributesType() {
        return relationshipAttributesType;
    }

    public String getTypeAndAllSubTypesQryStr() {
        if (StringUtils.isEmpty(typeAndAllSubTypesQryStr)) {
            typeAndAllSubTypesQryStr = AtlasAttribute.escapeIndexQueryValue(typeAndAllSubTypes);
        }

        return typeAndAllSubTypesQryStr;
    }

    public String getTypeQryStr() { return typeQryStr; }

    public boolean hasRelationshipAttribute(String attributeName) {
        return relationshipAttributes.containsKey(attributeName);
    }

    public String getQualifiedAttributeName(String attrName) throws AtlasBaseException {
        if (allAttributes.containsKey(attrName)) {
            return allAttributes.get(attrName).getQualifiedName();
        } else if (relationshipAttributes.containsKey(attrName)) {
            return relationshipAttributes.get(attrName).getQualifiedName();
        }

        throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attrName, entityDef.getName());
    }

    @Override
    public AtlasEntity createDefaultValue() {
        AtlasEntity ret = new AtlasEntity(entityDef.getName());

        populateDefaultValues(ret);

        return ret;
    }

    @Override
    public AtlasEntity createDefaultValue(Object defaultValue){
        AtlasEntity ret = new AtlasEntity(entityDef.getName());

        populateDefaultValues(ret);

        return ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                if (!superType.isValidValue(obj)) {
                    return false;
                }
            }

            return super.isValidValue(obj) && validateRelationshipAttributes(obj);
        }

        return true;
    }

    @Override
    public boolean areEqualValues(Object val1, Object val2) {
        for (AtlasEntityType superType : superTypes) {
            if (!superType.areEqualValues(val1, val2)) {
                return false;
            }
        }

        return super.areEqualValues(val1, val2);
    }

    @Override
    public boolean isValidValueForUpdate(Object obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                if (!superType.isValidValueForUpdate(obj)) {
                    return false;
                }
            }
            return super.isValidValueForUpdate(obj);
        }

        return true;
    }

    @Override
    public Object getNormalizedValue(Object obj) {
        Object ret = null;

        if (obj != null) {
            if (isValidValue(obj)) {
                if (obj instanceof AtlasEntity) {
                    normalizeAttributeValues((AtlasEntity) obj);
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
    public Object getNormalizedValueForUpdate(Object obj) {
        Object ret = null;

        if (obj != null) {
            if (isValidValueForUpdate(obj)) {
                if (obj instanceof AtlasEntity) {
                    normalizeAttributeValuesForUpdate((AtlasEntity) obj);
                    ret = obj;
                } else if (obj instanceof Map) {
                    normalizeAttributeValuesForUpdate((Map) obj);
                    ret = obj;
                }
            }
        }

        return ret;
    }

    @Override
    public AtlasAttribute getAttribute(String attributeName) {
        return allAttributes.get(attributeName);
    }

    @Override
    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            if (obj instanceof AtlasEntity || obj instanceof Map) {
                for (AtlasEntityType superType : superTypes) {
                    ret = superType.validateValue(obj, objName, messages) && ret;
                }

                ret = super.validateValue(obj, objName, messages) && validateRelationshipAttributes(obj, objName, messages) && ret;
            } else {
                ret = false;
                messages.add(objName + ": invalid value type '" + obj.getClass().getName());
            }
        }

        return ret;
    }

    @Override
    public boolean validateValueForUpdate(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            if (obj instanceof AtlasEntity || obj instanceof Map) {
                for (AtlasEntityType superType : superTypes) {
                    ret = superType.validateValueForUpdate(obj, objName, messages) && ret;
                }

                ret = super.validateValueForUpdate(obj, objName, messages) && ret;

            } else {
                ret = false;
                messages.add(objName + ": invalid value type '" + obj.getClass().getName());
            }
        }

        return ret;
    }

    @Override
    public AtlasType getTypeForAttribute() {
        AtlasType attributeType = new AtlasObjectIdType(getTypeName());

        if (LOG.isDebugEnabled()) {
            LOG.debug("getTypeForAttribute(): {} ==> {}", getTypeName(), attributeType.getTypeName());
        }

        return attributeType;
    }

    public void normalizeAttributeValues(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValues(ent);
            }

            normalizeValues(ent);
        }
    }

    public void normalizeAttributeValuesForUpdate(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValuesForUpdate(ent);
            }

            super.normalizeAttributeValuesForUpdate(ent);
        }
    }

    @Override
    public void normalizeAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValues(obj);
            }

            normalizeValues(obj);
        }
    }

    public void normalizeAttributeValuesForUpdate(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValuesForUpdate(obj);
            }

            super.normalizeAttributeValuesForUpdate(obj);
        }
    }

    public void populateDefaultValues(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.populateDefaultValues(ent);
            }

            super.populateDefaultValues(ent);
        }
    }

    private void addSubType(AtlasEntityType subType) {
        subTypes.add(subType.getTypeName());
    }

    private void addToAllSubTypes(AtlasEntityType subType) {
        allSubTypes.add(subType.getTypeName());
        typeAndAllSubTypes.add(subType.getTypeName());
    }

    private void getTypeHierarchyInfo(AtlasTypeRegistry              typeRegistry,
                                      Set<String>                    allSuperTypeNames,
                                      Map<String, AtlasAttribute> allAttributes) throws AtlasBaseException {
        List<String> visitedTypes = new ArrayList<>();

        collectTypeHierarchyInfo(typeRegistry, allSuperTypeNames, allAttributes, visitedTypes);
    }



    /*
     * This method should not assume that resolveReferences() has been called on all superTypes.
     * this.entityDef is the only safe member to reference here
     */

    private void collectTypeHierarchyInfo(AtlasTypeRegistry              typeRegistry,
                                          Set<String>                    allSuperTypeNames,
                                          Map<String, AtlasAttribute>    allAttributes,
                                          List<String>                   visitedTypes) throws AtlasBaseException {
        if (visitedTypes.contains(entityDef.getName())) {
            throw new AtlasBaseException(AtlasErrorCode.CIRCULAR_REFERENCE, entityDef.getName(),
                                         visitedTypes.toString());
        }

        if (CollectionUtils.isNotEmpty(entityDef.getSuperTypes())) {
            visitedTypes.add(entityDef.getName());
            for (String superTypeName : entityDef.getSuperTypes()) {
                AtlasEntityType superType = typeRegistry.getEntityTypeByName(superTypeName);

                if (superType != null) {
                    superType.collectTypeHierarchyInfo(typeRegistry, allSuperTypeNames, allAttributes, visitedTypes);
                }
            }
            visitedTypes.remove(entityDef.getName());
            allSuperTypeNames.addAll(entityDef.getSuperTypes());
        }

        if (CollectionUtils.isNotEmpty(entityDef.getAttributeDefs())) {
            for (AtlasAttributeDef attributeDef : entityDef.getAttributeDefs()) {

                AtlasType type = typeRegistry.getType(attributeDef.getTypeName());
                allAttributes.put(attributeDef.getName(), new AtlasAttribute(this, attributeDef, type));
            }
        }
    }
    boolean isAssignableFrom(AtlasObjectId objId) {
        boolean ret = AtlasTypeUtil.isValid(objId) && (StringUtils.equals(objId.getTypeName(), getTypeName()) || isSuperTypeOf(objId.getTypeName()));

        return ret;
    }

    private boolean validateRelationshipAttributes(Object obj) {
        if (obj != null && MapUtils.isNotEmpty(relationshipAttributes)) {
            if (obj instanceof AtlasEntity) {
                AtlasEntity entityObj = (AtlasEntity) obj;

                for (AtlasAttribute attribute : relationshipAttributes.values()) {
                    Object            attributeValue = entityObj.getRelationshipAttribute(attribute.getName());
                    AtlasAttributeDef attributeDef   = attribute.getAttributeDef();

                    if (!isAssignableValue(attributeValue, attributeDef)) {
                        return false;
                    }
                }
            } else if (obj instanceof Map) {
                Map map = AtlasTypeUtil.toRelationshipAttributes((Map) obj);

                for (AtlasAttribute attribute : relationshipAttributes.values()) {
                    Object            attributeValue = map.get(attribute.getName());
                    AtlasAttributeDef attributeDef   = attribute.getAttributeDef();

                    if (!isAssignableValue(attributeValue, attributeDef)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        return true;
    }

    /**
     * Takes a set of entityType names and a registry and returns a set of the entitytype names and the names of all their subTypes.
     *
     * @param entityTypes
     * @param typeRegistry
     * @return set of strings of the types and their subtypes.
     */
    static public Set<String> getEntityTypesAndAllSubTypes(Set<String> entityTypes, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        Set<String> ret = new HashSet<>();

        for (String typeName : entityTypes) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, typeName);
            }

            ret.addAll(entityType.getTypeAndAllSubTypes());
        }

        return ret;
    }

    private boolean isAssignableValue(Object value, AtlasAttributeDef attributeDef) {
        boolean ret = true;

        if (value != null) {
            AtlasAttribute attribute = relationshipAttributes.get(attributeDef.getName());

            if (attribute != null) {
                AtlasType attrType = attribute.getAttributeType();

                if (!isValidRelationshipType(attrType) && !attrType.isValidValue(value)) {
                    ret = false;
                }
            }
        }

        return ret;
    }
    private boolean isValidRelationshipType(AtlasType attributeType) {
        boolean ret = false;

        if (attributeType != null) {
            if (attributeType instanceof AtlasArrayType) {
                attributeType = ((AtlasArrayType) attributeType).getElementType();
            }

            if (attributeType instanceof AtlasObjectIdType || attributeType instanceof AtlasEntityType) {
                ret = true;
            }
        }

        return ret;
    }

    private void normalizeRelationshipAttributeValues(AtlasStruct obj) {
        if (obj != null && obj instanceof AtlasEntity) {
            AtlasEntity entityObj = (AtlasEntity) obj;

            for (AtlasAttribute attribute : relationshipAttributes.values()) {
                String            attributeName = attribute.getName();
                AtlasAttributeDef attributeDef  = attribute.getAttributeDef();

                if (((AtlasEntity) obj).hasRelationshipAttribute(attributeName)) {
                    Object attributeValue = getNormalizedValue(entityObj.getAttribute(attributeName), attributeDef);

                    obj.setAttribute(attributeName, attributeValue);
                }
            }
        }
    }

    public void normalizeRelationshipAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasAttribute attribute : relationshipAttributes.values()) {
                String            attributeName = attribute.getName();
                AtlasAttributeDef attributeDef  = attribute.getAttributeDef();

                if (obj.containsKey(attributeName)) {
                    Object attributeValue = getNormalizedValue(obj.get(attributeName), attributeDef);

                    obj.put(attributeName, attributeValue);
                }
            }
        }
    }

    private Object getNormalizedValue(Object value, AtlasAttributeDef attributeDef) {
        AtlasAttribute attribute = relationshipAttributes.get(attributeDef.getName());

        if (attribute != null) {
            AtlasType attrType = attribute.getAttributeType();

            if (isValidRelationshipType(attrType) && value != null) {
                return attrType.getNormalizedValue(value);
            }
        }

        return null;
    }

    private void normalizeValues(AtlasEntity ent) {
        super.normalizeAttributeValues(ent);

        normalizeRelationshipAttributeValues(ent);
    }

    private void normalizeValues(Map<String, Object> obj) {
        super.normalizeAttributeValues(obj);

        normalizeRelationshipAttributeValues(obj);
    }

    private boolean validateRelationshipAttributes(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null && MapUtils.isNotEmpty(relationshipAttributes)) {
            if (obj instanceof AtlasEntity) {
                AtlasEntity entityObj = (AtlasEntity) obj;

                for (AtlasAttribute attribute : relationshipAttributes.values()) {
                    if (attribute != null) {
                        String    attributeName = attribute.getName();
                        AtlasType dataType      = attribute.getAttributeType();
                        Object    value         = entityObj.getAttribute(attributeName);
                        String    fieldName     = objName + "." + attributeName;

                        if (isValidRelationshipType(dataType) && value != null) {
                            ret = dataType.validateValue(value, fieldName, messages) && ret;
                        }
                    }

                }
            } else if (obj instanceof Map) {
                Map attributes = AtlasTypeUtil.toStructAttributes((Map)obj);

                for (AtlasAttribute attribute : relationshipAttributes.values()) {

                    if (attribute != null) {
                        String    attributeName = attribute.getName();
                        AtlasType dataType      = attribute.getAttributeType();
                        Object    value         = attributes.get(attributeName);
                        String    fieldName     = objName + "." + attributeName;

                        if (isValidRelationshipType(dataType) && value != null) {
                            ret = dataType.validateValue(value, fieldName, messages) && ret;
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
}
