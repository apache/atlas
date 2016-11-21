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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.INVALID_TYPE_DEFINITION;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ON_DELETE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_VAL_CASCADE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_MAPPED_FROM_REF;
import static org.apache.atlas.type.AtlasTypeUtil.isArrayType;


public final class RestUtils {
    private RestUtils() {}
    private static final Logger LOG = LoggerFactory.getLogger(RestUtils.class);

    public static TypesDef toTypesDef(AtlasEnumDef enumDef) {
        TypesDef ret = null;

        if (enumDef != null) {
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
        }

        return ret;
    }

    public static TypesDef toTypesDef(AtlasStructDef structDef, AtlasTypeRegistry registry) throws AtlasBaseException {
        String                typeName   = structDef.getName();
        String                typeDesc   = structDef.getDescription();
        AttributeDefinition[] attributes = getAttributes(structDef, registry);
        StructTypeDefinition  structType = TypesUtil.createStructTypeDef(typeName, typeDesc, attributes);

        TypesDef ret = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                                             ImmutableList.of(structType),
                                             ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                                             ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        return ret;
    }

    public static TypesDef toTypesDef(AtlasEntityDef entityDef, AtlasTypeRegistry registry) throws AtlasBaseException {
        String                typeName    = entityDef.getName();
        String                typeDesc    = entityDef.getDescription();
        String                typeVersion = entityDef.getTypeVersion();
        ImmutableSet          superTypes  = ImmutableSet.copyOf(entityDef.getSuperTypes());
        AttributeDefinition[] attributes  = getAttributes(entityDef, registry);

        HierarchicalTypeDefinition<ClassType> classType = TypesUtil.createClassTypeDef(typeName, typeDesc, typeVersion,
                                                                                        superTypes, attributes);
        TypesDef ret = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                                             ImmutableList.<StructTypeDefinition>of(),
                                             ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                                             ImmutableList.of(classType));

        return ret;
    }

    public static TypesDef toTypesDef(AtlasClassificationDef classifDef, AtlasTypeRegistry registry) throws AtlasBaseException {
        String                typeName    = classifDef.getName();
        String                typeDesc    = classifDef.getDescription();
        String                typeVersion = classifDef.getTypeVersion();
        ImmutableSet          superTypes  = ImmutableSet.copyOf(classifDef.getSuperTypes());
        AttributeDefinition[] attributes  = getAttributes(classifDef, registry);

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

                // isComposite and reverseAttributeName applicable only for entities/classes.
                if (oldAttr.isComposite) {
                    String attrType = oldAttr.dataTypeName;
                    attrType = isArrayType(attrType) ? getArrayTypeName(attrType) : attrType;

                    if (!AtlasTypeUtil.isBuiltInType(attrType)) {
                        String refAttrName = null;

                        // 1. Check if attribute datatype is present in payload definition, if present get the typeDefinition,
                        // check all its attributes and find attribute that matches with classTypeDefName and reverseAttributeName
                        HierarchicalTypeDefinition<ClassType> refType = findClassType(classTypeDefinitions, attrType);
                        if (refType != null) {
                            for (AttributeDefinition refAttr : refType.attributeDefinitions) {
                                String refAttrDataTypeName = refAttr.dataTypeName;
                                String refAttrRevAttrName  = refAttr.reverseAttributeName;

                                if (StringUtils.equals(refAttrDataTypeName, classTypeDefName) &&
                                        StringUtils.equals(refAttrRevAttrName, oldAttr.name)) {
                                    refAttrName = refAttr.name;
                                    break;
                                }
                            }
                        }

                        // 2. Check if attribute present in typeRegistry. If present fetch all attributes associated with the type and
                        // check revAttrName equals base type attr name AND classTypeDefName equals attribute name
                        else {
                            if (registry.isRegisteredType(attrType)) {
                                AtlasType atlasType = registry.getType(attrType);

                                if (isEntity(atlasType)) {
                                    AtlasEntityType         entityType    = (AtlasEntityType) atlasType;
                                    List<AtlasAttributeDef> atlasAttrDefs = entityType.getEntityDef().getAttributeDefs();

                                    for (AtlasAttributeDef attrDef : atlasAttrDefs) {
                                        boolean isForeignKey = entityType.isForeignKeyAttribute(attrDef.getName());

                                        if (isForeignKey) {
                                            AtlasType attribType = entityType.getAttributeType(attrDef.getName());

                                            if (attribType != null) {
                                                if (attribType.getTypeCategory() == TypeCategory.ARRAY) {
                                                    attribType = ((AtlasArrayType) attribType).getElementType();
                                                }

                                                if (attribType.getTypeCategory() == TypeCategory.ENTITY) {
                                                    String revAttrName = ((AtlasEntityType) attribType).
                                                            getMappedFromRefAttribute(entityType.getTypeName(), attrDef.getName());

                                                    if (StringUtils.equals(classTypeDefName, attrDef.getTypeName()) &&
                                                            StringUtils.equals(oldAttr.name, revAttrName)) {
                                                        refAttrName = attrDef.getName();
                                                    }
                                                }
                                            }
                                        }

                                    }
                                }
                            }
                        }

                        if (StringUtils.isNotBlank(refAttrName)) { // ex: hive_table.columns, hive_column.table
                            Map<String, Object> params = new HashMap<>();
                            params.put(AtlasConstraintDef.CONSTRAINT_PARAM_REF_ATTRIBUTE, refAttrName);

                            newAttr.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_MAPPED_FROM_REF, params));
                        } else { // ex: hive_table.partitionKeys, with no reverseAttribute-reference
                            newAttr.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_FOREIGN_KEY));
                        }
                    }

                } else if (StringUtils.isNotEmpty(oldAttr.reverseAttributeName)) {
                    Map<String, Object> params = new HashMap<>();
                    params.put(CONSTRAINT_PARAM_ON_DELETE, CONSTRAINT_PARAM_VAL_CASCADE);

                    newAttr.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_FOREIGN_KEY, params));
                }
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

    private static AtlasAttributeDef toAtlasAttributeDef(AttributeDefinition attrDefinition) {
        AtlasAttributeDef ret = new AtlasAttributeDef();

        ret.setName(attrDefinition.name);
        ret.setTypeName(attrDefinition.dataTypeName);
        ret.setIsIndexable(attrDefinition.isIndexable);
        ret.setIsUnique(attrDefinition.isUnique);

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

    private static AttributeDefinition[] getAttributes(AtlasStructDef structDef, AtlasTypeRegistry registry) throws AtlasBaseException {
        List<AttributeDefinition> ret      = new ArrayList<>();
        List<AtlasAttributeDef>   attrDefs = structDef.getAttributeDefs();

        if (CollectionUtils.isNotEmpty(attrDefs)) {

            for (AtlasAttributeDef attrDef : attrDefs) {
                String  name              = attrDef.getName();
                String  dataTypeName      = attrDef.getTypeName();
                Boolean isUnique          = attrDef.getIsUnique();
                Boolean isIndexable       = attrDef.getIsIndexable();
                String  reverseAttribName = null;
                boolean isComposite;

                // Multiplicity mapping
                final int lower;
                final int upper;

                if (attrDef.getCardinality() == Cardinality.SINGLE) {
                    lower = attrDef.getIsOptional() ? 0 : 1;
                    upper = 1;
                } else {
                    if(attrDef.getIsOptional()) {
                        lower = 0;
                    } else {
                        lower = attrDef.getValuesMinCount() < 1 ? 1 : attrDef.getValuesMinCount();
                    }

                    upper = attrDef.getValuesMaxCount() < 2 ? Integer.MAX_VALUE : attrDef.getValuesMaxCount();
                }
                
                Multiplicity multiplicity = new Multiplicity(lower, upper, Cardinality.SET.equals(attrDef.getCardinality()));

                // Constraint checks:
                // 1. [ mappedFromRef -> isComposite ]
                // 2. [ foreignKey(onDelete=cascade) -> reverseAttribute ]
                AtlasStructType structType      = (AtlasStructType) registry.getType(structDef.getName());
                boolean         isForeignKey    = structType.isForeignKeyAttribute(attrDef.getName());
                boolean         isMappedFromRef = structType.isMappedFromRefAttribute(attrDef.getName());
                AtlasType       attrType        = structType.getAttributeType(attrDef.getName());

                if (attrType != null && isForeignKey) {
                    if (attrType.getTypeCategory() == TypeCategory.ARRAY) {
                        attrType = ((AtlasArrayType) attrType).getElementType();
                    }

                    if (attrType.getTypeCategory() == TypeCategory.ENTITY) {
                        reverseAttribName = ((AtlasStructType) attrType).
                                                 getMappedFromRefAttribute(structType.getTypeName(), attrDef.getName());
                    }
                }
                isComposite = isMappedFromRef || (isForeignKey && StringUtils.isBlank(reverseAttribName));
                ret.add(new AttributeDefinition(name, dataTypeName, multiplicity, isComposite, isUnique, isIndexable, reverseAttribName));
            }
        }

        return ret.toArray(new AttributeDefinition[ret.size()]);
    }

    private static HierarchicalTypeDefinition<ClassType> findClassType(List<HierarchicalTypeDefinition<ClassType>> classDefs,
                                                                             String typeName) {
        HierarchicalTypeDefinition<ClassType> ret = null;

        if (CollectionUtils.isNotEmpty(classDefs)) {
            for (HierarchicalTypeDefinition<ClassType> classType : classDefs) {
                if (classType.typeName.equalsIgnoreCase(typeName)) {
                    ret = classType;
                }
            }
        }

        return ret;
    }

    private static boolean isEntity(AtlasType type) {
        return type.getTypeCategory() == TypeCategory.ENTITY;
    }
}