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

package org.apache.atlas.services;


import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AtlasTypeAttributePatch extends AtlasTypePatch {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeAttributePatch.class);
    private static final String STRUCT_TYPE = "STRUCT";
    private static final String CLASS_TYPE = "CLASS";
    private static final String TRAIT_TYPE = "TRAIT";
    private static final String[] SUPPORTED_ACTIONS = new String[] { "ADD_ATTRIBUTE", "UPDATE_ATTRIBUTE", "DELETE_ATTRIBUTE" };

    public AtlasTypeAttributePatch(DefaultMetadataService metadataService, TypeSystem typeSystem) {
        super(metadataService, typeSystem, SUPPORTED_ACTIONS);
    }

    /********* SAMPLE PATCH DATA ***********
     *
     *
     {
     "patches": [
        {
          "action": "ADD_ATTRIBUTE",
          "typeName": "hive_column",
          "applyToVersion": "1.0",
          "updateToVersion": "2.0",
          "params": {
              "optionalParam1" : "true",
              "optionalParam2" : "false"
           },
          "attributeDefinitions": [
            {
              "name": "position",
              "dataTypeName": "string",
              "multiplicity": "optional",
              "isComposite": false,
              "isUnique": false,
              "isIndexable": false,
              "reverseAttributeName": null
            }
          ]
        }
       ]
     }
     *
     */

    @Override
    public PatchResult applyPatch(PatchData patchData) {
        String typeName = patchData.getTypeName();
        String applyVersion = patchData.getApplyToVersion();
        TypesDef updatedTypeDef;
        PatchResult result;
        try {
            // Check if type exists in type system
            if (checkIfTypeExists(typeName, metadataService)) {
                TypesDef typesDef = getTypeDef(typeName);
                String currentVersion = getTypeVersion(typeName);

                // Check version to apply patch
                if (currentVersion == null || currentVersion.equalsIgnoreCase(applyVersion) || currentVersion.startsWith(applyVersion + ".")) {
                    updatedTypeDef = updateTypesDef(typesDef, patchData);

                    if (updatedTypeDef != null) {
                        metadataService.updateType(TypesSerialization.toJson(updatedTypeDef));
                        LOG.info("updated " + patchData.getTypeName() + " type from version " + patchData.getApplyToVersion() + " to " + patchData.getUpdateToVersion());
                        result = new PatchResult("patch applied successfully!", PatchStatus.SUCCESS);
                    } else {
                        LOG.error("failed to create updated typedef for type=" +typeName+ "; applyToVersion=" + applyVersion + "; updateToVersion=" + patchData.getUpdateToVersion() );
                        result = new PatchResult("patch failed!", PatchStatus.FAILED);
                    }
                } else {
                    LOG.info("patch skipped for " + patchData.getTypeName());
                    result = new PatchResult("patch skipped!", PatchStatus.SKIPPED);
                }

            } else {
                LOG.error("failed to apply patch (typeName=" + typeName + "; applyToVersion=" + applyVersion + "; updateToVersion=" + patchData.getUpdateToVersion() + "): type doesn't exist");
                result = new PatchResult("patch failed!", PatchStatus.FAILED);
            }

        } catch (AtlasException e) {
            LOG.error("error in updating type for " + patchData.getTypeName());
            result = new PatchResult("unable to update type", PatchStatus.FAILED);
        } catch (JSONException e) {
            LOG.error("error in updating typedef for  " + patchData.getTypeName());
            result = new PatchResult("unable to update typedef", PatchStatus.FAILED);
        }

        return result;
    }

    public TypesDef updateTypesDef(TypesDef typesDef, PatchData patchData) throws AtlasException, JSONException {
        AttributeDefinition[] typeAttributes = getAttributesFromTypesDef(typesDef, patchData.getTypeName());
        AttributeDefinition[] patchAttributes = patchData.getAttributeDefinitions();
        AttributeDefinition[] newAttributes = new AttributeDefinition[0];
        String patchAction = patchData.getAction();
        TypesDef newTypeDef = null;

        if (patchAction != null && typeAttributes != null && patchAttributes != null) {
            switch (patchAction) {
                case "ADD_ATTRIBUTE":
                    LOG.info("adding new attribute to type {}", patchData.getTypeName());
                    newAttributes = addAttributes(typeAttributes, patchAttributes);
                    break;
                case "DELETE_ATTRIBUTE":
                    LOG.info("deleting attribute from type {}", patchData.getTypeName());
                    newAttributes = deleteAttributes(typeAttributes, patchAttributes);
                    break;
                case "UPDATE_ATTRIBUTE":
                    LOG.info("updating attribute in type {}", patchData.getTypeName());
                    newAttributes = updateAttributes(typeAttributes, patchAttributes);
                    break;
                default:
                    LOG.info("invalid action " + patchAction + ", supported actions: " + Arrays.toString(SUPPORTED_ACTIONS));
                    break;
            }

            newTypeDef = createTypeDefWithNewAttributes(typesDef, patchData.getTypeName(), newAttributes, patchData.getUpdateToVersion());
        }

        return newTypeDef;
    }

    private AttributeDefinition[] addAttributes(AttributeDefinition[] typeAttributes, AttributeDefinition[] patchAttributes) {
        ArrayList<AttributeDefinition> newAttrList = new ArrayList<AttributeDefinition>(Arrays.asList(typeAttributes));
        Map<String, AttributeDefinition> typeAttrs = getAttributeNamesFromDefinition(typeAttributes);

        for (AttributeDefinition attr : patchAttributes) {
            if (!typeAttrs.containsKey(attr.name))
                newAttrList.add(attr);
        }

        return newAttrList.toArray(new AttributeDefinition[newAttrList.size()]);
    }

    private AttributeDefinition[] deleteAttributes(AttributeDefinition[] typeAttributes, AttributeDefinition[] patchAttributes) {
        ArrayList<AttributeDefinition> newAttrList = new ArrayList<AttributeDefinition>();
        Map<String, AttributeDefinition> patchAttrs = getAttributeNamesFromDefinition(patchAttributes);

        for (AttributeDefinition attr : typeAttributes) {
            if (!patchAttrs.containsKey(attr.name))
                newAttrList.add(attr);
        }

        return newAttrList.toArray(new AttributeDefinition[newAttrList.size()]);
    }

    private AttributeDefinition[] updateAttributes(AttributeDefinition[] typeAttributes, AttributeDefinition[] patchAttributes) {
        ArrayList<AttributeDefinition> newAttrList = new ArrayList<AttributeDefinition>();
        Map<String, AttributeDefinition> patchAttrs = getAttributeNamesFromDefinition(patchAttributes);
        AttributeDefinition newAttr;

        for (AttributeDefinition attr : typeAttributes) {
            newAttr = patchAttrs.get(attr.name);
            if (patchAttrs.containsKey(attr.name) && checkIfAttributeUpdatable(attr, newAttr)) {
                newAttrList.add(newAttr);
            } else {
                newAttrList.add(attr);
            }
        }

        return newAttrList.toArray(new AttributeDefinition[newAttrList.size()]);
    }

    private TypesDef createTypeDefWithNewAttributes(TypesDef typesDef, String typeName, AttributeDefinition[] newAttributes, String newVersion) throws AtlasException {
        ImmutableList.Builder<EnumTypeDefinition> enums = ImmutableList.builder();
        ImmutableList.Builder<StructTypeDefinition> structs = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<ClassType>> classTypes = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.builder();
        String dataType = getDataType(typeName).toUpperCase();
        switch (dataType) {
            case STRUCT_TYPE:
                StructTypeDefinition structType = typesDef.structTypesAsJavaList().get(0);
                structs.add(new StructTypeDefinition(structType.typeName, structType.typeDescription, newVersion, newAttributes));
                break;
            case CLASS_TYPE:
                HierarchicalTypeDefinition<ClassType> classType = typesDef.classTypesAsJavaList().get(0);
                classTypes.add(new HierarchicalTypeDefinition(ClassType.class, classType.typeName, classType.typeDescription, newVersion, classType.superTypes, newAttributes));
                break;
            case TRAIT_TYPE:
                HierarchicalTypeDefinition<TraitType> traitType = typesDef.traitTypesAsJavaList().get(0);
                traits.add(new HierarchicalTypeDefinition(TraitType.class, traitType.typeName, traitType.typeDescription, newVersion, traitType.superTypes, newAttributes));
                break;
            default:
                LOG.error("unhandled datatype {} when creating new typedef", dataType);
        }

        return TypesUtil.getTypesDef(enums.build(), structs.build(), traits.build(), classTypes.build());
    }

    private AttributeDefinition[] getAttributesFromTypesDef(TypesDef typesDef, String typeName) throws AtlasException {
        AttributeDefinition[] typeAttributes = null;
        String dataType = getDataType(typeName).toUpperCase();
        switch (dataType) {
            case STRUCT_TYPE:
                typeAttributes = typesDef.structTypesAsJavaList().get(0).attributeDefinitions;
                break;
            case CLASS_TYPE:
                typeAttributes = typesDef.classTypesAsJavaList().get(0).attributeDefinitions;
                break;
            case TRAIT_TYPE:
                typeAttributes = typesDef.traitTypesAsJavaList().get(0).attributeDefinitions;
                break;
            default:
                LOG.error("unhandled datatype {}", dataType);
        }

        return typeAttributes;
    }

    private Map<String, AttributeDefinition> getAttributeNamesFromDefinition(AttributeDefinition[] attribDef) {
        Map<String, AttributeDefinition> attrsMap = new HashMap<String, AttributeDefinition>();
        for (AttributeDefinition attr : attribDef) {
            attrsMap.put(attr.name, attr);
        }

        return attrsMap;
    }

    private boolean checkIfAttributeUpdatable(AttributeDefinition attr, AttributeDefinition newAttr) {
        boolean result = false;
        if (!attr.equals(newAttr) && (attr.multiplicity == Multiplicity.REQUIRED
                && newAttr.multiplicity == Multiplicity.OPTIONAL)) {
            result = true;
        }

        return result;
    }

    // Returns the datatype the typename belongs to - PRIMITIVE, ENUM, ARRAY, MAP, STRUCT, TRAIT, CLASS
    private String getDataType(String typeName) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
        return dataType.getTypeCategory().toString();
    }

    private String getTypeVersion(String typeName) throws AtlasException {
        return typeSystem.getDataType(IDataType.class, typeName).getVersion();
    }

    private TypesDef getTypeDef(String typeName) throws AtlasException {
        return TypesSerialization.fromJson(metadataService.getTypeDefinition(typeName));
    }

    private static boolean checkIfTypeExists(String typeName, DefaultMetadataService metadataService) {
        boolean result = true;
        try {
            metadataService.getTypeDefinition(typeName);
        } catch (AtlasException e) {
            result = false;
        }

        return result;
    }

}