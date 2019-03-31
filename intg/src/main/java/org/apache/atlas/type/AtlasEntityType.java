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
package org.apache.atlas.type;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasRelationshipAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasObjectIdType;
import org.apache.atlas.utils.AtlasEntityUtil;
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

    private static final String NAME        = "name";
    private static final String DESCRIPTION = "description";
    private static final String OWNER       = "owner";
    private static final String CREATE_TIME = "createTime";

    private static final String[] ENTITY_HEADER_ATTRIBUTES = new String[]{NAME, DESCRIPTION, OWNER, CREATE_TIME};
    private static final String   OPTION_SCHEMA_ATTRIBUTES = "schemaAttributes";

    private final AtlasEntityDef entityDef;
    private final String         typeQryStr;

    private static final String INTERNAL_TYPENAME = "__internal";

    private List<AtlasEntityType>                    superTypes                 = Collections.emptyList();
    private Set<String>                              allSuperTypes              = Collections.emptySet();
    private Set<String>                              subTypes                   = Collections.emptySet();
    private Set<String>                              allSubTypes                = Collections.emptySet();
    private Set<String>                              typeAndAllSubTypes         = Collections.emptySet();
    private Set<String>                              typeAndAllSuperTypes       = Collections.emptySet();
    private Map<String, Map<String, AtlasAttribute>> relationshipAttributes     = Collections.emptyMap();
    private List<AtlasAttribute>                     ownedRefAttributes         = Collections.emptyList();
    private String                                   typeAndAllSubTypesQryStr   = "";
    private boolean                                  isInternalType             = false;
    private Map<String, AtlasAttribute>              headerAttributes           = Collections.emptyMap();
    private Map<String, AtlasAttribute>              minInfoAttributes          = Collections.emptyMap();


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

    public AtlasEntityDef getEntityDef() {
        return entityDef;
    }

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
                s.add((AtlasEntityType) superType);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INCOMPATIBLE_SUPERTYPE, superTypeName, entityDef.getName());
            }
        }

        this.superTypes             = Collections.unmodifiableList(s);
        this.allSuperTypes          = Collections.unmodifiableSet(allS);
        this.allAttributes          = Collections.unmodifiableMap(allA);
        this.uniqAttributes         = getUniqueAttributes(this.allAttributes);
        this.subTypes               = new HashSet<>(); // this will be populated in resolveReferencesPhase2()
        this.allSubTypes            = new HashSet<>(); // this will be populated in resolveReferencesPhase2()
        this.typeAndAllSubTypes     = new HashSet<>(); // this will be populated in resolveReferencesPhase2()
        this.relationshipAttributes = new HashMap<>(); // this will be populated in resolveReferencesPhase3()

        this.typeAndAllSubTypes.add(this.getTypeName());

        this.typeAndAllSuperTypes = new HashSet<>(this.allSuperTypes);
        this.typeAndAllSuperTypes.add(this.getTypeName());
        this.typeAndAllSuperTypes = Collections.unmodifiableSet(this.typeAndAllSuperTypes);

        // headerAttributes includes uniqAttributes & ENTITY_HEADER_ATTRIBUTES
        this.headerAttributes = new HashMap<>(this.uniqAttributes);

        for (String headerAttributeName : ENTITY_HEADER_ATTRIBUTES) {
            AtlasAttribute headerAttribute = getAttribute(headerAttributeName);

            if (headerAttribute != null) {
                this.headerAttributes.put(headerAttributeName, headerAttribute);
            }
        }

        // minInfoAttributes includes all headerAttributes & schema-attributes
        this.minInfoAttributes = new HashMap<>(this.headerAttributes);

        Map<String, String> typeDefOptions       = entityDef.getOptions();
        String              jsonList             = typeDefOptions != null ? typeDefOptions.get(OPTION_SCHEMA_ATTRIBUTES) : null;
        List<String>        schemaAttributeNames = StringUtils.isNotEmpty(jsonList) ? AtlasType.fromJson(jsonList, List.class) : null;

        if (CollectionUtils.isNotEmpty(schemaAttributeNames)) {
            for (String schemaAttributeName : schemaAttributeNames) {
                AtlasAttribute schemaAttribute = getAttribute(schemaAttributeName);

                if (schemaAttribute != null) {
                    this.minInfoAttributes.put(schemaAttributeName, schemaAttribute);
                }
            }
        }
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
                typeRegistry.reportMissingRelationshipDef(getTypeName(), attributeEntityType.getTypeName(), attributeName);
            }
        }

        for (String superTypeName : allSuperTypes) {
            if (INTERNAL_TYPENAME.equals(superTypeName)) {
                isInternalType = true;
            }

            AtlasEntityType                          superType                       = typeRegistry.getEntityTypeByName(superTypeName);
            Map<String, Map<String, AtlasAttribute>> superTypeRelationshipAttributes = superType.getRelationshipAttributes();

            if (MapUtils.isNotEmpty(superTypeRelationshipAttributes)) {
                for (String attrName : superTypeRelationshipAttributes.keySet()) {
                    Map<String, AtlasAttribute> superTypeAttributes = superTypeRelationshipAttributes.get(attrName);

                    if (MapUtils.isNotEmpty(superTypeAttributes)) {
                        Map<String, AtlasAttribute> attributes = relationshipAttributes.get(attrName);

                        if (attributes == null) {
                            attributes = new HashMap<>();

                            relationshipAttributes.put(attrName, attributes);
                        }

                        for (String relationshipType : superTypeAttributes.keySet()) {
                            if (!attributes.containsKey(relationshipType)) {
                                attributes.put(relationshipType, superTypeAttributes.get(relationshipType));
                            }
                        }
                    }
                }
            }
        }

        ownedRefAttributes = new ArrayList<>();

        for (AtlasAttribute attribute : allAttributes.values()) {
            if (attribute.isOwnedRef()) {
                ownedRefAttributes.add(attribute);
            }
        }

        for (Map<String, AtlasAttribute> attributes : relationshipAttributes.values()) {
            for (AtlasAttribute attribute : attributes.values()) {
                if (attribute.isOwnedRef()) {
                    ownedRefAttributes.add(attribute);
                }
            }
        }

        subTypes                   = Collections.unmodifiableSet(subTypes);
        allSubTypes                = Collections.unmodifiableSet(allSubTypes);
        typeAndAllSubTypes         = Collections.unmodifiableSet(typeAndAllSubTypes);
        typeAndAllSubTypesQryStr   = ""; // will be computed on next access
        relationshipAttributes     = Collections.unmodifiableMap(relationshipAttributes);
        ownedRefAttributes         = Collections.unmodifiableList(ownedRefAttributes);

        entityDef.setSubTypes(subTypes);

        List<AtlasRelationshipAttributeDef> relationshipAttrDefs = new ArrayList<>();

        for (Map.Entry<String, Map<String, AtlasAttribute>> attrEntry : relationshipAttributes.entrySet()) {
            Map<String, AtlasAttribute> relations = attrEntry.getValue();

            for (Map.Entry<String, AtlasAttribute> relationsEntry : relations.entrySet()) {
                String         relationshipType = relationsEntry.getKey();
                AtlasAttribute relationshipAttr = relationsEntry.getValue();

                relationshipAttrDefs.add(new AtlasRelationshipAttributeDef(relationshipType, relationshipAttr.isLegacyAttribute(), relationshipAttr.getAttributeDef()));
            }
        }

        entityDef.setRelationshipAttributeDefs(Collections.unmodifiableList(relationshipAttrDefs));
    }

    public Set<String> getSuperTypes() {
        return entityDef.getSuperTypes();
    }

    public Set<String> getAllSuperTypes() {
        return allSuperTypes;
    }

    public Set<String> getSubTypes() {
        return subTypes;
    }

    public Set<String> getAllSubTypes() {
        return allSubTypes;
    }

    public Set<String> getTypeAndAllSubTypes() {
        return typeAndAllSubTypes;
    }

    public Set<String> getTypeAndAllSuperTypes() {
        return typeAndAllSuperTypes;
    }

    public Map<String, AtlasAttribute> getHeaderAttributes() { return headerAttributes; }

    public Map<String, AtlasAttribute> getMinInfoAttributes() { return minInfoAttributes; }

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

    public Map<String, Map<String, AtlasAttribute>> getRelationshipAttributes() {
        return relationshipAttributes;
    }

    public List<AtlasAttribute> getOwnedRefAttributes() {
        return ownedRefAttributes;
    }

    public AtlasAttribute getRelationshipAttribute(String attributeName, String relationshipType) {
        final AtlasAttribute        ret;
        Map<String, AtlasAttribute> attributes = relationshipAttributes.get(attributeName);

        if (MapUtils.isNotEmpty(attributes)) {
            if (relationshipType != null && attributes.containsKey(relationshipType)) {
                ret = attributes.get(relationshipType);
            } else {
                ret = attributes.values().iterator().next();
            }
        } else {
            ret = null;
        }

        return ret;
    }

    // this method should be called from AtlasRelationshipType.resolveReferencesPhase2()
    void addRelationshipAttribute(String attributeName, AtlasAttribute attribute, AtlasRelationshipType relationshipType) {
        Map<String, AtlasAttribute> attributes = relationshipAttributes.get(attributeName);

        if (attributes == null) {
            attributes = new HashMap<>();

            relationshipAttributes.put(attributeName, attributes);
        }

        attributes.put(relationshipType.getTypeName(), attribute);
    }

    public Set<String> getAttributeRelationshipTypes(String attributeName) {
        Map<String, AtlasAttribute> attributes = relationshipAttributes.get(attributeName);

        return attributes != null ? attributes.keySet() : null;
    }

    public String getTypeAndAllSubTypesQryStr() {
        if (StringUtils.isEmpty(typeAndAllSubTypesQryStr)) {
            typeAndAllSubTypesQryStr = AtlasAttribute.escapeIndexQueryValue(typeAndAllSubTypes);
        }

        return typeAndAllSubTypesQryStr;
    }

    public String getTypeQryStr() {
        return typeQryStr;
    }

    public boolean hasAttribute(String attributeName) {
        return allAttributes.containsKey(attributeName);
    }

    public boolean hasRelationshipAttribute(String attributeName) {
        return relationshipAttributes.containsKey(attributeName);
    }

    public String getQualifiedAttributeName(String attrName) throws AtlasBaseException {
        if (allAttributes.containsKey(attrName)) {
            return allAttributes.get(attrName).getQualifiedName();
        } else if (relationshipAttributes.containsKey(attrName)) {
            return relationshipAttributes.get(attrName).values().iterator().next().getQualifiedName();
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
    public AtlasEntity createDefaultValue(Object defaultValue) {
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
    public boolean areEqualValues(Object val1, Object val2, Map<String, String> guidAssignments) {
        for (AtlasEntityType superType : superTypes) {
            if (!superType.areEqualValues(val1, val2, guidAssignments)) {
                return false;
            }
        }

        return super.areEqualValues(val1, val2, guidAssignments);
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

            super.normalizeAttributeValues(ent);

            normalizeRelationshipAttributeValues(ent, false);
        }
    }

    public void normalizeAttributeValuesForUpdate(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValuesForUpdate(ent);
            }

            super.normalizeAttributeValuesForUpdate(ent);

            normalizeRelationshipAttributeValues(ent, true);
        }
    }

    @Override
    public void normalizeAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValues(obj);
            }

            super.normalizeAttributeValues(obj);

            normalizeRelationshipAttributeValues(obj, false);
        }
    }

    public void normalizeAttributeValuesForUpdate(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValuesForUpdate(obj);
            }

            super.normalizeAttributeValuesForUpdate(obj);

            normalizeRelationshipAttributeValues(obj, true);
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

    private void getTypeHierarchyInfo(AtlasTypeRegistry typeRegistry,
                                      Set<String> allSuperTypeNames,
                                      Map<String, AtlasAttribute> allAttributes) throws AtlasBaseException {
        List<String> visitedTypes = new ArrayList<>();

        collectTypeHierarchyInfo(typeRegistry, allSuperTypeNames, allAttributes, visitedTypes);
    }



    /*
     * This method should not assume that resolveReferences() has been called on all superTypes.
     * this.entityDef is the only safe member to reference here
     */

    private void collectTypeHierarchyInfo(AtlasTypeRegistry typeRegistry,
                                          Set<String> allSuperTypeNames,
                                          Map<String, AtlasAttribute> allAttributes,
                                          List<String> visitedTypes) throws AtlasBaseException {
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

                for (String attributeName : relationshipAttributes.keySet()) {
                    Object            attributeValue   = entityObj.getRelationshipAttribute(attributeName);
                    String            relationshipType = AtlasEntityUtil.getRelationshipType(attributeValue);
                    AtlasAttribute    attribute        = getRelationshipAttribute(attributeName, relationshipType);
                    AtlasAttributeDef attributeDef     = attribute.getAttributeDef();

                    if (!isAssignableValue(attributeValue, attributeDef)) {
                        return false;
                    }
                }
            } else if (obj instanceof Map) {
                Map map = AtlasTypeUtil.toRelationshipAttributes((Map) obj);

                for (String attributeName : relationshipAttributes.keySet()) {
                    Object            attributeValue   = map.get(attributeName);
                    String            relationshipType = AtlasEntityUtil.getRelationshipType(attributeValue);
                    AtlasAttribute    attribute        = getRelationshipAttribute(attributeName, relationshipType);
                    AtlasAttributeDef attributeDef     = attribute.getAttributeDef();

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
            String         relationshipType = AtlasEntityUtil.getRelationshipType(value);
            AtlasAttribute attribute        = getRelationshipAttribute(attributeDef.getName(), relationshipType);

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

    private void normalizeRelationshipAttributeValues(AtlasEntity entity, boolean isUpdate) {
        if (entity != null) {
            for (String attributeName : relationshipAttributes.keySet()) {
                if (entity.hasRelationshipAttribute(attributeName)) {
                    Object         attributeValue   = entity.getRelationshipAttribute(attributeName);
                    String         relationshipType = AtlasEntityUtil.getRelationshipType(attributeValue);
                    AtlasAttribute attribute        = getRelationshipAttribute(attributeName, relationshipType);

                    if (attribute != null) {
                        AtlasType attrType = attribute.getAttributeType();

                        if (isValidRelationshipType(attrType)) {
                            if (isUpdate) {
                                attributeValue = attrType.getNormalizedValueForUpdate(attributeValue);
                            } else {
                                attributeValue = attrType.getNormalizedValue(attributeValue);
                            }

                            entity.setRelationshipAttribute(attributeName, attributeValue);
                        }
                    }
                }
            }
        }
    }

    public void normalizeRelationshipAttributeValues(Map<String, Object> obj, boolean isUpdate) {
        if (obj != null) {
            for (String attributeName : relationshipAttributes.keySet()) {
                if (obj.containsKey(attributeName)) {
                    Object         attributeValue   = obj.get(attributeName);
                    String         relationshipType = AtlasEntityUtil.getRelationshipType(attributeValue);
                    AtlasAttribute attribute        = getRelationshipAttribute(attributeName, relationshipType);

                    if (attribute != null) {
                        AtlasType attrType = attribute.getAttributeType();

                        if (isValidRelationshipType(attrType)) {
                            if (isUpdate) {
                                attributeValue = attrType.getNormalizedValueForUpdate(attributeValue);
                            } else {
                                attributeValue = attrType.getNormalizedValue(attributeValue);
                            }

                            obj.put(attributeName, attributeValue);
                        }
                    }
                }
            }
        }
    }

    private boolean validateRelationshipAttributes(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null && MapUtils.isNotEmpty(relationshipAttributes)) {
            if (obj instanceof AtlasEntity) {
                AtlasEntity entityObj = (AtlasEntity) obj;

                for (String attributeName : relationshipAttributes.keySet()) {
                    Object         value            = entityObj.getRelationshipAttribute(attributeName);
                    String         relationshipType = AtlasEntityUtil.getRelationshipType(value);
                    AtlasAttribute attribute        = getRelationshipAttribute(attributeName, relationshipType);

                    if (attribute != null) {
                        AtlasType dataType = attribute.getAttributeType();

                        if (!attribute.getAttributeDef().getIsOptional()) {
                            // if required attribute is null, check if attribute value specified in relationship
                            if (value == null) {
                                value = entityObj.getRelationshipAttribute(attributeName);
                            }

                            if (value == null) {
                                ret = false;
                                messages.add(objName + "." + attributeName + ": mandatory attribute value missing in type " + getTypeName());
                            }
                        }

                        if (isValidRelationshipType(dataType) && value != null) {
                            ret = dataType.validateValue(value, objName + "." + attributeName, messages) && ret;
                        }
                    }
                }
            } else if (obj instanceof Map) {
                Map attributes = AtlasTypeUtil.toRelationshipAttributes((Map) obj);

                for (String attributeName : relationshipAttributes.keySet()) {
                    Object         value            = attributes.get(attributeName);
                    String         relationshipType = AtlasEntityUtil.getRelationshipType(value);
                    AtlasAttribute attribute        = getRelationshipAttribute(attributeName, relationshipType);

                    if (attribute != null) {
                        AtlasType dataType = attribute.getAttributeType();

                        if (isValidRelationshipType(dataType) && value != null) {
                            ret = dataType.validateValue(value, objName + "." + attributeName, messages) && ret;
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
