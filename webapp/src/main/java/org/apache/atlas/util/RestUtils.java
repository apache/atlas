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

package org.apache.atlas.util;

import static org.apache.atlas.AtlasErrorCode.INVALID_TYPE_DEFINITION;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE;
import static org.apache.atlas.type.AtlasTypeUtil.isArrayType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.atlas.model.typedef.AtlasTypeDefHeader;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.v1.AtlasStructDefStoreV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.INVALID_TYPE_DEFINITION;
import static org.apache.atlas.type.AtlasTypeUtil.isArrayType;


public final class RestUtils {
    private RestUtils() {}
    private static final Logger LOG = LoggerFactory.getLogger(RestUtils.class);

    public static TypesDef toTypesDef(AtlasType type, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        final TypesDef ret;

        if (type instanceof AtlasEnumType) {
            ret = RestUtils.enumToTypesDef((AtlasEnumType)type);
        } else if (type instanceof AtlasEntityType) {
            ret = RestUtils.entityToTypesDef((AtlasEntityType)type, typeRegistry);
        } else if (type instanceof AtlasClassificationType) {
            ret = RestUtils.classificationToTypesDef((AtlasClassificationType)type, typeRegistry);
        } else if (type instanceof AtlasStructType) {
            ret = RestUtils.structToTypesDef((AtlasStructType)type, typeRegistry);
        } else {
            ret = new TypesDef();
        }

        return ret;
    }

    private static TypesDef enumToTypesDef(AtlasEnumType enumType) {
        TypesDef ret = null;

        AtlasEnumDef enumDef = enumType.getEnumDef();

        String      enumName    = enumDef.getName();
        String      enumDesc    = enumDef.getDescription();
        String      enumVersion = enumDef.getTypeVersion();
        EnumValue[] enumValues  = getEnumValues(enumDef.getElementDefs());

        if (enumName != null && enumValues != null && enumValues.length > 0) {
            EnumTypeDefinition enumTypeDef = new EnumTypeDefinition(enumName, enumDesc, enumVersion, enumValues);

            ret = TypesUtil.getTypesDef(ImmutableList.of(enumTypeDef),
                                        ImmutableList.<StructTypeDefinition>of(),
                                        ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                                        ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        }

        return ret;
    }

    private static TypesDef structToTypesDef(AtlasStructType structType, AtlasTypeRegistry registry)
                                                                                            throws AtlasBaseException {
        String                typeName      = structType.getStructDef().getName();
        String                typeDesc      = structType.getStructDef().getDescription();
        String                typeVersion   = structType.getStructDef().getTypeVersion();
        AttributeDefinition[] attributes    = getAttributes(structType, registry);
        StructTypeDefinition  structTypeDef = TypesUtil.createStructTypeDef(typeName, typeDesc, typeVersion, attributes);

        TypesDef ret = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                                             ImmutableList.of(structTypeDef),
                                             ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                                             ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        return ret;
    }

    private static TypesDef entityToTypesDef(AtlasEntityType entityType, AtlasTypeRegistry registry)
                                                                                             throws AtlasBaseException {
        String                typeName    = entityType.getEntityDef().getName();
        String                typeDesc    = entityType.getEntityDef().getDescription();
        String                typeVersion = entityType.getEntityDef().getTypeVersion();
        ImmutableSet          superTypes  = ImmutableSet.copyOf(entityType.getEntityDef().getSuperTypes());
        AttributeDefinition[] attributes  = getAttributes(entityType, registry);

        HierarchicalTypeDefinition<ClassType> classType = TypesUtil.createClassTypeDef(typeName, typeDesc, typeVersion,
                                                                                       superTypes, attributes);
        TypesDef ret = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                                             ImmutableList.<StructTypeDefinition>of(),
                                             ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                                             ImmutableList.of(classType));

        return ret;
    }

    private static TypesDef classificationToTypesDef(AtlasClassificationType classificationType,
                                                     AtlasTypeRegistry registry) throws AtlasBaseException {
        String                typeName    = classificationType.getClassificationDef().getName();
        String                typeDesc    = classificationType.getClassificationDef().getDescription();
        String                typeVersion = classificationType.getClassificationDef().getTypeVersion();
        ImmutableSet          superTypes  = ImmutableSet.copyOf(classificationType.getClassificationDef().getSuperTypes());
        AttributeDefinition[] attributes  = getAttributes(classificationType, registry);

        HierarchicalTypeDefinition traitType = TypesUtil.createTraitTypeDef(typeName, typeDesc, typeVersion, superTypes,
                                                                             attributes);
        TypesDef ret = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                                             ImmutableList.<StructTypeDefinition>of(),
                                             ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(traitType),
                                             ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        return ret;
    }



    public static AtlasTypesDef toAtlasTypesDef(String typeDefinition, AtlasTypeRegistry registry) throws AtlasBaseException {
        AtlasTypesDef ret = new AtlasTypesDef();

        try {
            if (StringUtils.isEmpty(typeDefinition)) {
                throw new AtlasBaseException(INVALID_TYPE_DEFINITION, typeDefinition);
            }

            TypesDef typesDef = TypesSerialization.fromJson(typeDefinition);
            if (CollectionUtils.isNotEmpty(typesDef.enumTypesAsJavaList())) {
                List<AtlasEnumDef> enumDefs = toAtlasEnumDefs(typesDef.enumTypesAsJavaList());
                ret.setEnumDefs(enumDefs);
            }

            if (CollectionUtils.isNotEmpty(typesDef.structTypesAsJavaList())) {
                List<AtlasStructDef> structDefs = toAtlasStructDefs(typesDef.structTypesAsJavaList());
                ret.setStructDefs(structDefs);
            }

            if (CollectionUtils.isNotEmpty(typesDef.classTypesAsJavaList())) {
                List<AtlasEntityDef> entityDefs = toAtlasEntityDefs(typesDef.classTypesAsJavaList(), registry);
                ret.setEntityDefs(entityDefs);
            }

            if (CollectionUtils.isNotEmpty(typesDef.traitTypesAsJavaList())) {
                List<AtlasClassificationDef> classificationDefs = toAtlasClassificationDefs(typesDef.traitTypesAsJavaList());
                ret.setClassificationDefs(classificationDefs);
            }

        } catch (Exception e) {
            LOG.error("Invalid type definition = {}", typeDefinition, e);
            throw new AtlasBaseException(INVALID_TYPE_DEFINITION, typeDefinition);
        }

        return ret;
    }

    public static ImmutableList<String> getTypeNames(List<AtlasTypeDefHeader> atlasTypesDefs) {
        List<String> ret = new ArrayList<String>();
        if (CollectionUtils.isNotEmpty(atlasTypesDefs)) {
            for (AtlasTypeDefHeader atlasTypesDef : atlasTypesDefs) {
                ret.add(atlasTypesDef.getName());
            }
        }

        return ImmutableList.copyOf(ret);
    }

    public static List<String> getTypeNames(AtlasTypesDef typesDef) {
        List<AtlasTypeDefHeader> atlasTypesDefs = AtlasTypeUtil.toTypeDefHeader(typesDef);
        return getTypeNames(atlasTypesDefs);
    }

    private static List<AtlasEnumDef> toAtlasEnumDefs(List<EnumTypeDefinition> enumTypeDefinitions) {
        List<AtlasEnumDef> ret = new ArrayList<AtlasEnumDef>();

        for (EnumTypeDefinition enumType : enumTypeDefinitions) {
            AtlasEnumDef enumDef = new AtlasEnumDef();
            enumDef.setName(enumType.name);
            enumDef.setDescription(enumType.description);
            enumDef.setTypeVersion(enumType.version);
            enumDef.setElementDefs(getAtlasEnumElementDefs(enumType.enumValues));

            ret.add(enumDef);
        }

        return ret;
    }

    private static List<AtlasStructDef> toAtlasStructDefs(List<StructTypeDefinition> structTypeDefinitions)
            throws AtlasBaseException {
        List<AtlasStructDef> ret = new ArrayList<AtlasStructDef>();

        for (StructTypeDefinition structType : structTypeDefinitions) {
            AtlasStructDef          structDef = new AtlasStructDef();
            List<AtlasAttributeDef> attrDefs  = new ArrayList<AtlasAttributeDef>();

            structDef.setName(structType.typeName);
            structDef.setDescription(structType.typeDescription);
            structDef.setTypeVersion(structType.typeVersion);

            AttributeDefinition[] attrDefinitions = structType.attributeDefinitions;
            for (AttributeDefinition attrDefinition : attrDefinitions) {
                attrDefs.add(toAtlasAttributeDef(attrDefinition));
            }

            structDef.setAttributeDefs(attrDefs);
            ret.add(structDef);
        }

        return ret;
    }

    private static List<AtlasClassificationDef> toAtlasClassificationDefs(List<HierarchicalTypeDefinition<TraitType>> traitTypeDefinitions)
            throws AtlasBaseException {
        List<AtlasClassificationDef> ret = new ArrayList<AtlasClassificationDef>();

        for (HierarchicalTypeDefinition<TraitType> traitType : traitTypeDefinitions) {
            AtlasClassificationDef  classifDef = new AtlasClassificationDef();
            List<AtlasAttributeDef> attrDefs   = new ArrayList<AtlasAttributeDef>();

            classifDef.setName(traitType.typeName);
            classifDef.setDescription(traitType.typeDescription);
            classifDef.setTypeVersion(traitType.typeVersion);
            classifDef.setSuperTypes(traitType.superTypes);

            AttributeDefinition[] attrDefinitions = traitType.attributeDefinitions;
            for (AttributeDefinition attrDefinition : attrDefinitions) {
                attrDefs.add(toAtlasAttributeDef(attrDefinition));
            }

            classifDef.setAttributeDefs(attrDefs);
            ret.add(classifDef);
        }

        return ret;
    }

    private static List<AtlasEntityDef> toAtlasEntityDefs(List<HierarchicalTypeDefinition<ClassType>> classTypeDefinitions,
                                                          AtlasTypeRegistry registry) throws AtlasBaseException {
        List<AtlasEntityDef> atlasEntityDefs = new ArrayList<AtlasEntityDef>();

        for (HierarchicalTypeDefinition<ClassType> classType : classTypeDefinitions) {
            List<AtlasAttributeDef> attrDefs         = new ArrayList<AtlasAttributeDef>();
            AtlasEntityDef          atlasEntityDef   = new AtlasEntityDef();
            String                  classTypeDefName = classType.typeName;

            atlasEntityDef.setName(classTypeDefName);
            atlasEntityDef.setDescription(classType.typeDescription);
            atlasEntityDef.setTypeVersion(classType.typeVersion);
            atlasEntityDef.setSuperTypes(classType.superTypes);

            AttributeDefinition[] attrDefinitions = classType.attributeDefinitions;
            for (AttributeDefinition oldAttr : attrDefinitions) {
                AtlasAttributeDef newAttr = toAtlasAttributeDef(oldAttr);
                attrDefs.add(newAttr);
            }

            atlasEntityDef.setAttributeDefs(attrDefs);
            atlasEntityDefs.add(atlasEntityDef);
        }

        return atlasEntityDefs;
    }

    private static String getArrayTypeName(String attrType) {
        String ret = null;
        if (isArrayType(attrType)) {
            Set<String> typeNames = AtlasTypeUtil.getReferencedTypeNames(attrType);
            if (typeNames.size() > 0) {
                ret = typeNames.iterator().next();
            }
        }

        return ret;
    }

    private static List<AtlasEnumElementDef> getAtlasEnumElementDefs(EnumValue[] enums) {
        List<AtlasEnumElementDef> ret = new ArrayList<AtlasEnumElementDef>();

        for (EnumValue enumElem : enums) {
            ret.add(new AtlasEnumElementDef(enumElem.value, null, enumElem.ordinal));
        }

        return ret;
    }

    private static EnumValue[] getEnumValues(List<AtlasEnumElementDef> enumDefs) {
        List<EnumValue> ret = new ArrayList<EnumValue>();

        if (CollectionUtils.isNotEmpty(enumDefs)) {
            for (AtlasEnumElementDef enumDef : enumDefs) {
                if (enumDef != null) {
                    ret.add(new EnumValue(enumDef.getValue(), enumDef.getOrdinal()));
                }
            }
        }

        return ret.toArray(new EnumValue[ret.size()]);
    }

    private static AtlasAttributeDef toAtlasAttributeDef(final AttributeDefinition attrDefinition) {
        AtlasAttributeDef ret = new AtlasAttributeDef();

        ret.setName(attrDefinition.name);
        ret.setTypeName(attrDefinition.dataTypeName);
        ret.setIsIndexable(attrDefinition.isIndexable);
        ret.setIsUnique(attrDefinition.isUnique);
        if (attrDefinition.isComposite) {
            ret.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_OWNED_REF));
        }

        if (StringUtils.isNotBlank(attrDefinition.reverseAttributeName)) {
            ret.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_INVERSE_REF,
                                       new HashMap<String, Object>() {{
                                           put(CONSTRAINT_PARAM_ATTRIBUTE, attrDefinition.reverseAttributeName);
                                       }}));
        }

        // Multiplicity attribute mapping
        Multiplicity multiplicity = attrDefinition.multiplicity;
        int          minCount     = multiplicity.lower;
        int          maxCount     = multiplicity.upper;
        boolean      isUnique     = multiplicity.isUnique;

        if (minCount == 0) {
            ret.setIsOptional(true);
            ret.setValuesMinCount(0);
        } else {
            ret.setIsOptional(false);
            ret.setValuesMinCount(minCount);
        }

        if (maxCount < 2) {
            ret.setCardinality(Cardinality.SINGLE);
            ret.setValuesMaxCount(1);
        } else {
            if (!isUnique) {
                ret.setCardinality(Cardinality.LIST);
            } else {
                ret.setCardinality(Cardinality.SET);
            }

            ret.setValuesMaxCount(maxCount);
        }

        return ret;
    }

    private static AttributeDefinition[] getAttributes(AtlasStructType structType, AtlasTypeRegistry registry) throws AtlasBaseException {
        List<AttributeDefinition> ret      = new ArrayList<>();
        List<AtlasAttributeDef>   attrDefs = structType.getStructDef().getAttributeDefs();

        if (CollectionUtils.isNotEmpty(attrDefs)) {
            for (AtlasAttributeDef attrDef : attrDefs) {
                AtlasAttribute attribute = structType.getAttribute(attrDef.getName());

                ret.add(AtlasStructDefStoreV1.toAttributeDefintion(attribute));
            }
        }

        return ret.toArray(new AttributeDefinition[ret.size()]);
    }
}
