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
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * class that implements behaviour of an relationship-type.
 */
public class AtlasRelationshipType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipType.class);

    private final AtlasRelationshipDef relationshipDef;
    private       AtlasEntityType      end1Type;
    private       AtlasEntityType      end2Type;

    public AtlasRelationshipType(AtlasRelationshipDef relationshipDef) {
        super(relationshipDef);

        this.relationshipDef = relationshipDef;
    }

    public AtlasRelationshipType(AtlasRelationshipDef relationshipDef, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super(relationshipDef);

        this.relationshipDef = relationshipDef;

        resolveReferences(typeRegistry);
    }
    public AtlasRelationshipDef getRelationshipDef() { return relationshipDef; }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferences(typeRegistry);

        if (relationshipDef == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "relationshipDef is null");
        }

        String end1TypeName = relationshipDef.getEndDef1() != null ? relationshipDef.getEndDef1().getType() : null;
        String end2TypeName = relationshipDef.getEndDef2() != null ? relationshipDef.getEndDef2().getType() : null;

        AtlasType type1 = typeRegistry.getType(end1TypeName);
        AtlasType type2 = typeRegistry.getType(end2TypeName);

        if (type1 instanceof AtlasEntityType) {
            end1Type = (AtlasEntityType) type1;

        } else {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_INVALID_END_TYPE, getTypeName(), end1TypeName);
        }

        if (type2 instanceof AtlasEntityType) {
            end2Type = (AtlasEntityType) type2;

        } else {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_INVALID_END_TYPE, getTypeName(), end2TypeName);
        }

        validateAtlasRelationshipDef(this.relationshipDef);
    }

    @Override
    public void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);

        addRelationshipAttributeToEndType(relationshipDef.getEndDef1(), end1Type, end2Type.getTypeName(), typeRegistry);

        addRelationshipAttributeToEndType(relationshipDef.getEndDef2(), end2Type, end1Type.getTypeName(), typeRegistry);
    }

    @Override
    public boolean isValidValue(Object obj) {
        boolean ret = true;

        if (obj != null) {

            if (obj instanceof AtlasRelationshipType) {
                validateAtlasRelationshipType((AtlasRelationshipType) obj);
            }

            ret = super.isValidValue(obj);
        }

        return ret;
    }

    @Override
    public boolean isValidValueForUpdate(Object obj) {
        boolean ret = true;

        if (obj != null) {
            validateAtlasRelationshipType((AtlasRelationshipType) obj);
            ret = super.isValidValueForUpdate(obj);
        }

        return ret;
    }

    public AtlasEntityType getEnd1Type() { return end1Type; }

    public AtlasEntityType getEnd2Type() { return end2Type; }

    /**
     * Validate the fields in the the RelationshipType are consistent with respect to themselves.
     * @param type
     * @throws AtlasBaseException
     */
    private boolean validateAtlasRelationshipType(AtlasRelationshipType type) {
        boolean isValid = false;
        try {
            validateAtlasRelationshipDef(type.getRelationshipDef());
            isValid = true;
        } catch (AtlasBaseException abe) {
            LOG.error("Validation error for AtlasRelationshipType", abe);
        }
        return isValid;
    }

    /**
     * Throw an exception so we can junit easily.
     *
     * This method assumes that the 2 ends are not null.
     *
     * @param relationshipDef
     * @throws AtlasBaseException
     */
    public static void validateAtlasRelationshipDef(AtlasRelationshipDef relationshipDef) throws AtlasBaseException {

        AtlasRelationshipEndDef endDef1              = relationshipDef.getEndDef1();
        AtlasRelationshipEndDef endDef2              = relationshipDef.getEndDef2();
        RelationshipCategory    relationshipCategory = relationshipDef.getRelationshipCategory();
        String                  name                 = relationshipDef.getName();

        boolean                 isContainer1         = endDef1.getIsContainer();
        boolean                 isContainer2         = endDef2.getIsContainer();

        if (isContainer1 && isContainer2) {
            // we support 0 or 1 of these flags.
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_DOUBLE_CONTAINERS, name);
        }
        if ((isContainer1 || isContainer2)) {
            // we have an isContainer defined in an end
            if (relationshipCategory == RelationshipCategory.ASSOCIATION) {
                // associations are not containment relaitonships - so do not allow an endpoiint with isContainer
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_ASSOCIATION_AND_CONTAINER, name);
            }
        } else {
            // we do not have an isContainer defined in an end
            if (relationshipCategory == RelationshipCategory.COMPOSITION) {
                // COMPOSITION needs one end to be the container.
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_NO_CONTAINER, name);
            } else if (relationshipCategory == RelationshipCategory.AGGREGATION) {
                // AGGREGATION needs one end to be the container.
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_AGGREGATION_NO_CONTAINER, name);
            }
        }
        if (relationshipCategory == RelationshipCategory.COMPOSITION) {
            // composition containers should not be multiple cardinality
            if (endDef1.getCardinality() == AtlasAttributeDef.Cardinality.SET &&
                    endDef1.getIsContainer()) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_SET_CONTAINER, name);
            }
            if ((endDef2.getCardinality() == AtlasAttributeDef.Cardinality.SET) &&
                    endDef2.getIsContainer()) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_SET_CONTAINER, name);
            }
            if ((endDef1.getCardinality() == AtlasAttributeDef.Cardinality.LIST) ||
                    (endDef2.getCardinality() == AtlasAttributeDef.Cardinality.LIST)) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_LIST_ON_END, name);
            }
        }
    }

    private void addRelationshipAttributeToEndType(AtlasRelationshipEndDef endDef,
                                                   AtlasEntityType entityType,
                                                   String attrTypeName,
                                                   AtlasTypeRegistry typeRegistry) throws AtlasBaseException {

        String attrName = (endDef != null) ? endDef.getName() : null;

        if (StringUtils.isEmpty(attrName)) {
            return;
        }

        AtlasAttribute attribute = entityType.getAttribute(attrName);

        if (attribute == null) { //attr doesn't exist in type - is a new relationship attribute

            if (endDef.getCardinality() == Cardinality.SET) {
                attrTypeName = AtlasBaseTypeDef.getArrayTypeName(attrTypeName);
            }

            attribute = new AtlasAttribute(entityType, new AtlasAttributeDef(attrName, attrTypeName), typeRegistry.getType(attrTypeName));
        }

        entityType.addRelationshipAttribute(attrName, attribute);
    }
}
