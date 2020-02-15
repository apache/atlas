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
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasNamespaceDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.model.typedef.AtlasNamespaceDef.*;


public class AtlasNamespaceType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasNamespaceType.class);

    private final AtlasNamespaceDef namespaceDef;


    public AtlasNamespaceType(AtlasNamespaceDef namespaceDef) {
        super(namespaceDef);

        this.namespaceDef = namespaceDef;
    }

    @Override
    public boolean isValidValue(Object o) {
        return true; // there is no runtime instance for Namespaces, so return true
    }

    @Override
    public AtlasStruct createDefaultValue() {
        return null;  // there is no runtime instance for Namespaces, so return null
    }

    @Override
    public Object getNormalizedValue(Object a) {
        return null;  // there is no runtime instance for Namespaces, so return null
    }

    public AtlasNamespaceDef getNamespaceDef() {
        return namespaceDef;
    }

    @Override
    void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferences(typeRegistry);

        Map<String, AtlasNamespaceAttribute> a = new HashMap<>();

        for (AtlasAttribute attribute : super.allAttributes.values()) {
            AtlasAttributeDef attributeDef = attribute.getAttributeDef();
            String            attrName     = attribute.getName();
            AtlasType         attrType     = attribute.getAttributeType();

            if (attrType instanceof AtlasArrayType) {
                attrType = ((AtlasArrayType) attrType).getElementType();
            } else if (attrType instanceof AtlasMapType) {
                attrType = ((AtlasMapType) attrType).getValueType();
            }

            // check if attribute type is not struct/classification/entity/namespace
            if (attrType instanceof AtlasStructType) {
                throw new AtlasBaseException(AtlasErrorCode.NAMESPACE_DEF_ATTRIBUTE_TYPE_INVALID, getTypeName(), attrName);
            }

            Set<String>          entityTypeNames = attribute.getOptionSet(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
            Set<AtlasEntityType> entityTypes     = new HashSet<>();

            if (CollectionUtils.isNotEmpty(entityTypeNames)) {
                for (String entityTypeName : entityTypeNames) {
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);

                    if (entityType == null) {
                        throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, entityTypeName);
                    }

                    entityTypes.add(entityType);
                }
            }

            AtlasNamespaceAttribute nsAttribute;
            if (attribute.getAttributeType() instanceof AtlasBuiltInTypes.AtlasStringType) {
                Integer maxStringLength = attribute.getOptionInt(ATTR_MAX_STRING_LENGTH);
                if (maxStringLength == null) {
                    throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, attributeDef.getName(), "options." + ATTR_MAX_STRING_LENGTH);
                }

                String validPattern = attribute.getOptionString(ATTR_VALID_PATTERN);
                nsAttribute = new AtlasNamespaceAttribute(attribute, entityTypes, maxStringLength, validPattern);
            } else {
                nsAttribute = new AtlasNamespaceAttribute(attribute, entityTypes);
            }

            a.put(attrName, nsAttribute);
        }

        super.allAttributes = Collections.unmodifiableMap(a);
    }

    @Override
    void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferencesPhase2(typeRegistry);

        for (AtlasAttribute attribute : super.allAttributes.values()) {
            AtlasNamespaceAttribute nsAttribute = (AtlasNamespaceAttribute) attribute;
            Set<AtlasEntityType>    entityTypes = nsAttribute.getApplicableEntityTypes();

            if (CollectionUtils.isNotEmpty(entityTypes)) {
                for (AtlasEntityType entityType : entityTypes) {
                    entityType.addNamespaceAttribute(nsAttribute);
                }
            }
        }
    }

    public static class AtlasNamespaceAttribute extends AtlasAttribute {
        private final Set<AtlasEntityType> applicableEntityTypes;
        private final int                  maxStringLength;
        private final String               validPattern;

        public AtlasNamespaceAttribute(AtlasAttribute attribute, Set<AtlasEntityType> applicableEntityTypes) {
            super(attribute);

            this.maxStringLength       = 0;
            this.validPattern          = null;
            this.applicableEntityTypes = applicableEntityTypes;
        }

        public AtlasNamespaceAttribute(AtlasAttribute attribute, Set<AtlasEntityType> applicableEntityTypes, int maxStringLength, String validPattern) {
            super(attribute);

            this.maxStringLength       = maxStringLength;
            this.validPattern          = validPattern;
            this.applicableEntityTypes = applicableEntityTypes;
        }

        @Override
        public AtlasNamespaceType getDefinedInType() {
            return (AtlasNamespaceType) super.getDefinedInType();
        }

        public Set<AtlasEntityType> getApplicableEntityTypes() {
            return applicableEntityTypes;
        }

        public String getValidPattern() {
            return validPattern;
        }

        public int getMaxStringLength() {
            return maxStringLength;
        }
    }
}
