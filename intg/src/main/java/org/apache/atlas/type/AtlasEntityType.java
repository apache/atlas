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
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasObjectIdType;
import org.apache.commons.collections.CollectionUtils;
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

    private List<AtlasEntityType> superTypes               = Collections.emptyList();
    private Set<String>           allSuperTypes            = Collections.emptySet();
    private Set<String>           allSubTypes              = Collections.emptySet();
    private Set<String>           typeAndAllSubTypes       = Collections.emptySet();
    private String                typeAndAllSubTypesQryStr = "";

    public AtlasEntityType(AtlasEntityDef entityDef) {
        super(entityDef);

        this.entityDef = entityDef;
    }

    public AtlasEntityType(AtlasEntityDef entityDef, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super(entityDef);

        this.entityDef = entityDef;

        resolveReferences(typeRegistry);
    }

    public AtlasEntityDef getEntityDef() { return entityDef; }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
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

        this.superTypes         = Collections.unmodifiableList(s);
        this.allSuperTypes      = Collections.unmodifiableSet(allS);
        this.allAttributes      = Collections.unmodifiableMap(allA);
        this.uniqAttributes     = getUniqueAttributes(this.allAttributes);
        this.allSubTypes        = new HashSet<>();   // this will be populated in resolveReferencesPhase2()
        this.typeAndAllSubTypes = new HashSet<>();   // this will be populated in resolveReferencesPhase2()

        this.typeAndAllSubTypes.add(this.getTypeName());
    }

    @Override
    public void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);

        for (String superTypeName : allSuperTypes) {
            AtlasEntityType superType = typeRegistry.getEntityTypeByName(superTypeName);
            superType.addSubType(this);
        }
    }

    @Override
    public void resolveReferencesPhase3(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        allSubTypes              = Collections.unmodifiableSet(allSubTypes);
        typeAndAllSubTypes       = Collections.unmodifiableSet(typeAndAllSubTypes);
        typeAndAllSubTypesQryStr = ""; // will be computed on next access
    }

    public Set<String> getSuperTypes() {
        return entityDef.getSuperTypes();
    }

    public Set<String> getAllSuperTypes() {
        return allSuperTypes;
    }

    public Set<String> getAllSubTypes() { return allSubTypes; }

    public Set<String> getTypeAndAllSubTypes() { return typeAndAllSubTypes; }

    public boolean isSuperTypeOf(AtlasEntityType entityType) {
        return entityType != null && allSubTypes.contains(entityType.getTypeName());
    }

    public boolean isSuperTypeOf(String entityTypeName) {
        return StringUtils.isNotEmpty(entityTypeName) && allSubTypes.contains(entityTypeName);
    }

    public boolean isSubTypeOf(AtlasEntityType entityType) {
        return entityType != null && allSuperTypes.contains(entityType.getTypeName());
    }

    public boolean isSubTypeOf(String entityTypeName) {
        return StringUtils.isNotEmpty(entityTypeName) && allSuperTypes.contains(entityTypeName);
    }

    public String getTypeAndAllSubTypesQryStr() {
        if (StringUtils.isEmpty(typeAndAllSubTypesQryStr)) {
            typeAndAllSubTypesQryStr = AtlasAttribute.escapeIndexQueryValue(typeAndAllSubTypes);
        }

        return typeAndAllSubTypesQryStr;
    }

    @Override
    public AtlasEntity createDefaultValue() {
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
            return super.isValidValue(obj);
        }

        return true;
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

                ret = super.validateValue(obj, objName, messages) && ret;

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

            super.normalizeAttributeValues(obj);
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
}
