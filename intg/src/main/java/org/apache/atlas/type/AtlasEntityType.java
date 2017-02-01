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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.*;

/**
 * class that implements behaviour of an entity-type.
 */
public class AtlasEntityType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityType.class);

    private final AtlasEntityDef entityDef;

    private List<AtlasEntityType>       superTypes              = Collections.emptyList();
    private Set<String>                 allSuperTypes           = Collections.emptySet();
    private Set<String>                 allSubTypes             = Collections.emptySet();
    private Map<String, AtlasAttribute> mappedFromRefAttributes = new HashMap<>();
    private List<ForeignKeyReference>   foreignKeyReferences    = Collections.emptyList();

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
        Map<String, AtlasAttribute> allA    = new HashMap<>();

        getTypeHierarchyInfo(typeRegistry, allS, allA);

        for (String superTypeName : entityDef.getSuperTypes()) {
            AtlasType superType = typeRegistry.getType(superTypeName);

            if (superType instanceof AtlasEntityType) {
                s.add((AtlasEntityType)superType);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INCOMPATIBLE_SUPERTYPE, superTypeName, entityDef.getName());
            }
        }

        this.superTypes    = Collections.unmodifiableList(s);
        this.allSuperTypes = Collections.unmodifiableSet(allS);
        this.allAttributes = Collections.unmodifiableMap(allA);
        this.allSubTypes          = new HashSet<>();   // this will be populated in resolveReferencesPhase2()
        this.foreignKeyReferences = new ArrayList<>(); // this will be populated in resolveReferencesPhase2()
    }

    @Override
    public void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);

        mappedFromRefAttributes = Collections.unmodifiableMap(resolveMappedFromRefConstraint(allAttributes.values()));

        for (String superTypeName : allSuperTypes) {
            AtlasEntityType superType = typeRegistry.getEntityTypeByName(superTypeName);
            superType.addSubType(this);
        }
    }

    public Set<String> getSuperTypes() {
        return entityDef.getSuperTypes();
    }

    public Set<String> getAllSuperTypes() {
        return allSuperTypes;
    }

    public Set<String> getAllSubTypes() { return Collections.unmodifiableSet(allSubTypes); }

    public Collection<String> getMappedFromRefAttributes() { return mappedFromRefAttributes.keySet(); }

    public boolean isMappedFromRefAttribute(String attributeName) {
        return mappedFromRefAttributes.containsKey(attributeName);
    }

    public String getMappedFromRefAttribute(String typeName, String attribName) {
        String ret = null;

        for (Map.Entry<String, AtlasAttribute> e : mappedFromRefAttributes.entrySet()) {
            AtlasAttribute attribute = e.getValue();

            if(StringUtils.equals(attribute.getStructType().getTypeName(), typeName) && StringUtils.equals(attribute.getName(), attribName)) {
                ret = e.getKey();

                break;
            }
        }

        return ret;
    }

    public List<ForeignKeyReference> getForeignKeyReferences() {
        return Collections.unmodifiableList(foreignKeyReferences);
    }

    public ForeignKeyReference getForeignKeyReference(String fromTypeName, String fromAttributeName) {
        ForeignKeyReference ret = null;

        for (ForeignKeyReference fkRef : foreignKeyReferences) {
            if (StringUtils.equals(fkRef.fromTypeName(), fromTypeName) &&
                StringUtils.equals(fkRef.fromAttributeName(), fromAttributeName)) {
                ret = fkRef;

                break;
            }
        }

        return ret;
    }

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

    @Override
    public AtlasEntity createDefaultValue() {
        AtlasEntity ret = new AtlasEntity(entityDef.getName());

        populateDefaultValues(ret);

        return ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            if (obj instanceof AtlasObjectId) {
                AtlasObjectId objId = (AtlasObjectId ) obj;
                return validateAtlasObjectId(objId);
            } else {
                for (AtlasEntityType superType : superTypes) {
                    if (!superType.isValidValue(obj)) {
                        return false;
                    }
                }
                return super.isValidValue(obj);
            }
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
                } else if (obj instanceof AtlasObjectId) {
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
            if (obj instanceof AtlasObjectId) {
                AtlasObjectId objId = (AtlasObjectId ) obj;
                return validateAtlasObjectId(objId);
            }

            for (AtlasEntityType superType : superTypes) {
                ret = superType.validateValue(obj, objName, messages) && ret;
            }

            ret = super.validateValue(obj, objName, messages) && ret;
        }

        return ret;
    }

    public void normalizeAttributeValues(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValues(ent);
            }

            super.normalizeAttributeValues(ent);
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

    public void populateDefaultValues(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.populateDefaultValues(ent);
            }

            super.populateDefaultValues(ent);
        }
    }

    void addForeignKeyReference(AtlasAttribute attribute, AtlasConstraintDef refConstraint) {
        foreignKeyReferences.add(new ForeignKeyReference(attribute, refConstraint));
    }

    private void addSubType(AtlasEntityType subType) {
        allSubTypes.add(subType.getTypeName());
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

    /*
     * valid conditions for mapped-from-ref constraint:
     *  - supported only in entity-type
     *  - attribute should be an entity-type or an array of entity-type
     *  - attribute's entity-type should have a foreign-key constraint to this type
     */
    private Map<String, AtlasAttribute> resolveMappedFromRefConstraint(Collection<AtlasAttribute> attributes) throws AtlasBaseException {
        Map<String, AtlasAttribute> ret = null;

        for (AtlasAttribute attribute : attributes) {
            AtlasAttributeDef attribDef = attribute.getAttributeDef();

            if (CollectionUtils.isEmpty(attribDef.getConstraintDefs())) {
                continue;
            }

            for (AtlasConstraintDef constraintDef : attribDef.getConstraintDefs()) {
                if (!StringUtils.equals(constraintDef.getType(), CONSTRAINT_TYPE_MAPPED_FROM_REF)) {
                    continue;
                }

                AtlasType attribType = attribute.getAttributeType();

                if (attribType.getTypeCategory() == TypeCategory.ARRAY) {
                    attribType = ((AtlasArrayType)attribType).getElementType();
                }

                if (attribType.getTypeCategory() != TypeCategory.ENTITY) {
                    throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_SATISFIED, getTypeName(),
                                                 attribDef.getName(), CONSTRAINT_TYPE_MAPPED_FROM_REF,
                                                 attribDef.getTypeName());
                }

                String refAttribName = AtlasTypeUtil.getStringValue(constraintDef.getParams(), CONSTRAINT_PARAM_REF_ATTRIBUTE);

                if (StringUtils.isBlank(refAttribName)) {
                    throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_MISSING_PARAMS,
                                                 getTypeName(), attribDef.getName(),
                                                 CONSTRAINT_PARAM_REF_ATTRIBUTE, CONSTRAINT_TYPE_MAPPED_FROM_REF,
                                                 String.valueOf(constraintDef.getParams()));
                }

                AtlasEntityType entityType = (AtlasEntityType) attribType;
                AtlasAttribute  refAttrib  = entityType.getAttribute(refAttribName);

                if (refAttrib == null) {
                    throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_EXIST,
                                                 getTypeName(), attribDef.getName(), CONSTRAINT_PARAM_REF_ATTRIBUTE,
                                                 entityType.getTypeName(), refAttribName);
                }

                if (!StringUtils.equals(getTypeName(), refAttrib.getTypeName())) {
                    throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_MATCHED,
                                                 getTypeName(), attribDef.getName(), CONSTRAINT_PARAM_REF_ATTRIBUTE,
                                                 entityType.getTypeName(), refAttribName, getTypeName(),
                                                 refAttrib.getTypeName());
                }

                if (ret == null) {
                    ret = new HashMap<>();
                }

                ret.put(attribDef.getName(), refAttrib);

                break;
            }
        }

        return ret == null ? Collections.<String, AtlasAttribute>emptyMap() : ret;
    }

    private boolean validateAtlasObjectId(AtlasObjectId objId) {
        if (StringUtils.isEmpty(objId.getTypeName()) || StringUtils.isEmpty(objId.getGuid())) {
            return false;
        } else {
            String typeName = objId.getTypeName();
            if (!typeName.equals(getTypeName()) && !isSuperTypeOf(typeName)) {
                return false;
            }
        }
        return AtlasEntity.isAssigned(objId.getGuid()) || AtlasEntity.isUnAssigned((objId.getGuid()));
    }

    public static class ForeignKeyReference {
        private final AtlasAttribute     fromAttribute;
        private final AtlasConstraintDef refConstraint;

        public ForeignKeyReference(AtlasAttribute fromAttribute, AtlasConstraintDef refConstraint) {
            this.fromAttribute = fromAttribute;
            this.refConstraint = refConstraint;
        }

        public String fromTypeName() { return fromType().getTypeName(); }

        public String fromAttributeName() { return fromAttribute.getName(); }

        public String toTypeName() { return fromAttribute.getTypeName(); }

        public AtlasStructType fromType() { return fromAttribute.getStructType(); }

        public AtlasAttribute fromAttribute() { return fromAttribute; }

        public AtlasEntityType toType() { return (AtlasEntityType)fromAttribute.getAttributeType(); }

        public AtlasConstraintDef getConstraint() { return refConstraint; }

        public boolean isOnDeleteCascade() {
            return StringUtils.equals(getOnDeleteAction(), CONSTRAINT_PARAM_VAL_CASCADE);
        }

        public boolean isOnDeleteUpdate() {
            return StringUtils.equals(getOnDeleteAction(), CONSTRAINT_PARAM_VAL_UPDATE);
        }

        private String getOnDeleteAction() {
            Map<String, Object> params = refConstraint.getParams();

            Object action = MapUtils.isNotEmpty(params) ? params.get(AtlasConstraintDef.CONSTRAINT_PARAM_ON_DELETE) : null;

            return (action != null) ? action.toString() : null;
        }
    }
}
