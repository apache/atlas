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
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * class that implements behaviour of an relationship-type.
 */
public class AtlasRelationshipType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipType.class);

    private final AtlasRelationshipDef relationshipDef;

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

        validateAtlasRelationshipDef(this.relationshipDef);
    }

    @Override
    public boolean isValidValue(Object obj) {
        boolean ret = true;

        if (obj != null) {
           validateAtlasRelationshipType((AtlasRelationshipType) obj);
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
     * @param relationshipDef
     * @throws AtlasBaseException
     */
    public static void validateAtlasRelationshipDef(AtlasRelationshipDef relationshipDef) throws AtlasBaseException {
        AtlasRelationshipEndDef endDef1              = relationshipDef.getEndDef1();
        AtlasRelationshipEndDef endDef2              = relationshipDef.getEndDef2();
        boolean                 isContainer1         = endDef1.getIsContainer();
        boolean                 isContainer2         = endDef2.getIsContainer();
        RelationshipCategory    relationshipCategory = relationshipDef.getRelationshipCategory();
        String                  name                 = relationshipDef.getName();

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
            if (endDef1 != null &&
                    endDef1.getCardinality() == AtlasAttributeDef.Cardinality.SET &&
                    endDef1.getIsContainer()) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_SET_CONTAINER, name);
            }
            if (endDef2 != null && endDef2 != null &&
                    endDef2.getCardinality() == AtlasAttributeDef.Cardinality.SET &&
                    endDef2.getIsContainer()) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_SET_CONTAINER, name);
            }
        }
        if ((endDef1 != null && endDef1.getCardinality() == AtlasAttributeDef.Cardinality.LIST) ||
                (endDef2 != null && endDef2.getCardinality() == AtlasAttributeDef.Cardinality.LIST)) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_LIST_ON_END, name);
        }
    }
}
