/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.localrepository.repositorycontentmanager;


import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.*;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.*;

/**
 * OMRSRepositoryHelper provides methods to repository connectors and repository event mappers to help
 * them build valid type definitions (TypeDefs), entities and relationships.  It is a facade to the
 * repository content manager which holds an in memory cache of all the active TypeDefs in the local server.
 * OMRSRepositoryHelper's purpose is to create a object that the repository connectors and event mappers can
 * create, use and discard without needing to know how to connect to the repository content manager.
 */
public class OMRSRepositoryHelper implements OMRSTypeDefHelper, OMRSInstanceHelper
{
    private static OMRSRepositoryContentManager defaultRepositoryContentManager = null;

    private OMRSRepositoryContentManager repositoryContentManager;


    /**
     * Set up the default local repository's content manager.  This maintains a cache of the local repository's type
     * definitions and rules to provide helpers and validators for TypeDefs and instances that are
     * exchanged amongst the open metadata repositories and open metadata access services (OMAS).
     *
     * @param repositoryContentManager - link to repository content manager.
     */
    public static synchronized void setRepositoryContentManager(OMRSRepositoryContentManager repositoryContentManager)
    {
        OMRSRepositoryHelper.defaultRepositoryContentManager = repositoryContentManager;
    }


    /**
     * Default constructor uses the default repository content manager.  Repository connectors should use the
     * repository helper from their superclass.
     */
    @Deprecated
    public OMRSRepositoryHelper()
    {
        this.repositoryContentManager = defaultRepositoryContentManager;
    }


    /**
     * Creates a repository helper linked to the supplied repository content manager.
     *
     * @param repositoryContentManager - object associated with the local repository.
     */
    public OMRSRepositoryHelper(OMRSRepositoryContentManager repositoryContentManager)
    {
        this.repositoryContentManager = repositoryContentManager;
    }


    /*
     * ========================
     * OMRSTypeDefHelper
     */

    /**
     * Return the list of typedefs active in the local repository.
     *
     * @return TypeDef gallery
     */
    public TypeDefGallery getActiveTypeDefGallery()
    {
        final String methodName = "getActiveTypeDefGallery";

        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getActiveTypeDefGallery();
    }


    /**
     * Return the list of typedefs known by the local repository.
     *
     * @return TypeDef gallery
     */
    public TypeDefGallery getKnownTypeDefGallery()
    {
        final String methodName = "getKnownTypeDefGallery";

        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getKnownTypeDefGallery();
    }


    /**
     * Return the TypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName  - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    public TypeDef getTypeDefByName(String sourceName,
                                    String typeDefName)
    {
        final String methodName = "getTypeDefByName";

        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getTypeDefByName(sourceName, typeDefName);
    }


    /**
     * Return the AttributeTypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param attributeTypeDefName - unique name for the TypeDef
     * @return AttributeTypeDef object or null if AttributeTypeDef is not known.
     */
    public AttributeTypeDef getAttributeTypeDefByName(String sourceName,
                                                      String attributeTypeDefName)
    {
        final String methodName = "getAttributeTypeDefByName";

        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getAttributeTypeDefByName(sourceName, attributeTypeDefName);
    }


    /**
     * Return the TypeDefs identified by the name supplied by the caller.  The TypeDef name may have wild
     * card characters in it which is why the results are returned in a list.
     *
     * @param sourceName  - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    public TypeDefGallery getActiveTypesByWildCardName(String sourceName,
                                                       String typeDefName)
    {
        final String methodName = "getActiveTypesByWildCardName";

        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getActiveTypesByWildCardName(sourceName, typeDefName);
    }


    /**
     * Return the TypeDef identified by the guid supplied by the caller.  This call is used when
     * retrieving a type that only the guid is known.
     *
     * @param sourceName  - source of the request (used for logging)
     * @param parameterName - name of guid parameter
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public TypeDef getTypeDef(String sourceName,
                              String parameterName,
                              String typeDefGUID,
                              String methodName) throws TypeErrorException
    {
        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getTypeDef(sourceName, parameterName, typeDefGUID, methodName);
    }


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that only the guid is known.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public AttributeTypeDef getAttributeTypeDef(String sourceName,
                                                String attributeTypeDefGUID,
                                                String methodName) throws TypeErrorException
    {
        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getAttributeTypeDef(sourceName, attributeTypeDefGUID, methodName);
    }


    /**
     * Return the TypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type of a metadata instance.
     *
     * @param sourceName  - source of the request (used for logging)
     * @param guidParameterName - name of guid parameter
     * @param nameParameterName - name of type name parameter
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param typeDefName - unique name for the TypeDef
     * @param methodName  - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public TypeDef getTypeDef(String sourceName,
                              String guidParameterName,
                              String nameParameterName,
                              String typeDefGUID,
                              String typeDefName,
                              String methodName) throws TypeErrorException
    {
        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getTypeDef(sourceName,
                                                   guidParameterName,
                                                   nameParameterName,
                                                   typeDefGUID,
                                                   typeDefName,
                                                   methodName);
    }


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type definition of a metadata instance's
     * property.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @param attributeTypeDefName - unique name for the AttributeTypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public AttributeTypeDef getAttributeTypeDef(String sourceName,
                                                String attributeTypeDefGUID,
                                                String attributeTypeDefName,
                                                String methodName) throws TypeErrorException
    {
        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getAttributeTypeDef(sourceName,
                                                            attributeTypeDefGUID,
                                                            attributeTypeDefName,
                                                            methodName);
    }


    /**
     * Returns an updated TypeDef that has had the supplied patch applied.  It throws an exception if any part of
     * the patch is incompatible with the original TypeDef.  For example, if there is a mismatch between
     * the type or version that either represents.
     *
     * @param sourceName      - source of the TypeDef (used for logging)
     * @param typeDefPatch    - patch to apply
     * @param originalTypeDef - typeDef to patch
     * @return updated TypeDef
     * @throws PatchErrorException       - the patch is either badly formatted, or does not apply to the supplied TypeDef
     * @throws InvalidParameterException - the TypeDefPatch is null.
     */
    public TypeDef applyPatch(String       sourceName,
                              TypeDef      originalTypeDef,
                              TypeDefPatch typeDefPatch) throws PatchErrorException,
                                                                InvalidParameterException
    {
        final String  methodName = "applyPatch";

        validateRepositoryContentManager(methodName);

        TypeDef clonedTypeDef  = null;
        TypeDef updatedTypeDef = null;

        /*
         * Begin with simple validation of the typeDef patch.
         */
        if (typeDefPatch != null)
        {
            // TODO invalid parameter exception
        }

        long newVersion = typeDefPatch.getUpdateToVersion();
        if (newVersion <= typeDefPatch.getApplyToVersion())
        {
            // TODO PatchError
        }

        TypeDefPatchAction patchAction = typeDefPatch.getAction();
        if (patchAction == null)
        {
            // TODO patch error
        }


        /*
         * Is the version compatible?
         */
        if (originalTypeDef.getVersion() != typeDefPatch.getApplyToVersion())
        {
            // TODO throw PatchException - incompatible versions
        }

        /*
         * OK to perform the update.  Need to create a new TypeDef object.  TypeDef is an abstract class
         * so need to use the TypeDefCategory to create a new object of the correct type.
         */
        TypeDefCategory category = originalTypeDef.getCategory();
        if (category == null)
        {
            // TODO Throw PatchError - base type is messed up
        }

        try
        {
            switch (category)
            {
                case ENTITY_DEF:
                    clonedTypeDef = new EntityDef((EntityDef) originalTypeDef);
                    break;

                case RELATIONSHIP_DEF:
                    clonedTypeDef = new RelationshipDef((RelationshipDef) originalTypeDef);
                    break;

                case CLASSIFICATION_DEF:
                    clonedTypeDef = new ClassificationDef((ClassificationDef) originalTypeDef);
                    break;
            }
        }
        catch (ClassCastException castError)
        {
            // TODO Throw PatchError - base type is messed up
        }

        /*
         * Now we have a new TypeDef - just need to make the changes.  The Action
         */
        if (clonedTypeDef != null)
        {
            switch (patchAction)
            {
                case ADD_ATTRIBUTES:
                    updatedTypeDef = this.patchTypeDefAttributes(clonedTypeDef, typeDefPatch.getTypeDefAttributes());
                    break;

                case ADD_OPTIONS:
                    updatedTypeDef = this.patchTypeDefNewOptions(clonedTypeDef, typeDefPatch.getTypeDefOptions());
                    break;

                case UPDATE_OPTIONS:
                    updatedTypeDef = this.patchTypeDefUpdateOptions(clonedTypeDef, typeDefPatch.getTypeDefOptions());
                    break;

                case DELETE_OPTIONS:
                    updatedTypeDef = this.patchTypeDefDeleteOptions(clonedTypeDef, typeDefPatch.getTypeDefOptions());
                    break;

                case ADD_EXTERNAL_STANDARDS:
                    updatedTypeDef = this.patchTypeDefAddExternalStandards(clonedTypeDef,
                                                                           typeDefPatch.getExternalStandardMappings(),
                                                                           typeDefPatch.getTypeDefAttributes());
                    break;

                case UPDATE_EXTERNAL_STANDARDS:
                    updatedTypeDef = this.patchTypeDefUpdateExternalStandards(clonedTypeDef,
                                                                              typeDefPatch.getExternalStandardMappings(),
                                                                              typeDefPatch.getTypeDefAttributes());
                    break;

                case DELETE_EXTERNAL_STANDARDS:
                    updatedTypeDef = this.patchTypeDefDeleteExternalStandards(clonedTypeDef,
                                                                              typeDefPatch.getExternalStandardMappings(),
                                                                              typeDefPatch.getTypeDefAttributes());
                    break;

                case UPDATE_DESCRIPTIONS:
                    updatedTypeDef = this.patchTypeDefNewDescriptions(clonedTypeDef,
                                                                      typeDefPatch.getDescription(),
                                                                      typeDefPatch.getDescriptionGUID(),
                                                                      typeDefPatch.getTypeDefAttributes());
                    break;
            }
        }


        if (updatedTypeDef != null)
        {
            updatedTypeDef.setVersion(typeDefPatch.getUpdateToVersion());
            updatedTypeDef.setVersionName(typeDefPatch.getNewVersionName());
        }

        return updatedTypeDef;
    }


    /**
     * Add the supplied attributes to the properties definition for the cloned typedef.
     *
     * @param clonedTypeDef     - TypeDef object to update
     * @param typeDefAttributes - new attributes to add.
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding attributes
     */
    private TypeDef patchTypeDefAttributes(TypeDef clonedTypeDef,
                                           List<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        List<TypeDefAttribute> propertyDefinitions = clonedTypeDef.getPropertiesDefinition();

        if (propertyDefinitions == null)
        {
            propertyDefinitions = new ArrayList<>();
        }

        for (TypeDefAttribute newAttribute : typeDefAttributes)
        {
            if (newAttribute != null)
            {
                String           attributeName = newAttribute.getAttributeName();
                AttributeTypeDef attributeType = newAttribute.getAttributeType();

                if ((attributeName != null) && (attributeType != null))
                {
                    if (propertyDefinitions.contains(newAttribute))
                    {
                        // TODO Patch error - Duplicate Attribute
                    }
                    else
                    {
                        propertyDefinitions.add(newAttribute);
                    }
                }
                else
                {
                    // TODO Patch Error - Invalid Attribute in patch
                }
            }
        }

        if (propertyDefinitions.size() > 0)
        {
            clonedTypeDef.setPropertiesDefinition(propertyDefinitions);
        }
        else
        {
            clonedTypeDef.setPropertiesDefinition(null);
        }

        return clonedTypeDef;
    }


    /**
     * @param clonedTypeDef  - TypeDef object to update
     * @param typeDefOptions - new options to add
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding options
     */
    private TypeDef patchTypeDefNewOptions(TypeDef clonedTypeDef,
                                           Map<String, String> typeDefOptions) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * @param clonedTypeDef  - TypeDef object to update
     * @param typeDefOptions - options to update
     * @return updated TypeDef
     * @throws PatchErrorException - problem updating options
     */
    private TypeDef patchTypeDefUpdateOptions(TypeDef clonedTypeDef,
                                              Map<String, String> typeDefOptions) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * @param clonedTypeDef  - TypeDef object to update
     * @param typeDefOptions - options to delete
     * @return updated TypeDef
     * @throws PatchErrorException - problem deleting options
     */
    private TypeDef patchTypeDefDeleteOptions(TypeDef clonedTypeDef,
                                              Map<String, String> typeDefOptions) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Add new mappings to external standards to the TypeDef.
     *
     * @param clonedTypeDef            - TypeDef object to update
     * @param externalStandardMappings - new mappings to add
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding mapping(s)
     */
    private TypeDef patchTypeDefAddExternalStandards(TypeDef clonedTypeDef,
                                                     List<ExternalStandardMapping> externalStandardMappings,
                                                     List<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Update the supplied mappings from the TypeDef.
     *
     * @param clonedTypeDef            - TypeDef object to update
     * @param externalStandardMappings - mappings to update
     * @return updated TypeDef
     * @throws PatchErrorException - problem updating mapping(s)
     */
    private TypeDef patchTypeDefUpdateExternalStandards(TypeDef clonedTypeDef,
                                                        List<ExternalStandardMapping> externalStandardMappings,
                                                        List<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Delete the supplied mappings from the TypeDef.
     *
     * @param clonedTypeDef            - TypeDef object to update
     * @param externalStandardMappings - list of mappings to delete
     * @return updated TypeDef
     * @throws PatchErrorException - problem deleting mapping(s)
     */
    private TypeDef patchTypeDefDeleteExternalStandards(TypeDef clonedTypeDef,
                                                        List<ExternalStandardMapping> externalStandardMappings,
                                                        List<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Update the descriptions for the TypeDef or any of its attributes.  If the description values are null, they are
     * not changes in the TypeDef.  This means there is no way to clear a description - just update it for a better one.
     *
     * @param clonedTypeDef   - TypeDef object to update
     * @param description     - new description
     * @param descriptionGUID - new unique identifier for glossary term that provides detailed description of TypeDef
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding new description
     */
    private TypeDef patchTypeDefNewDescriptions(TypeDef clonedTypeDef,
                                                String description,
                                                String descriptionGUID,
                                                List<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        if (description != null)
        {
            clonedTypeDef.setDescription(description);
        }
        if (descriptionGUID != null)
        {
            clonedTypeDef.setDescriptionGUID(descriptionGUID);
        }

        if (typeDefAttributes != null)
        {
            List<TypeDefAttribute> propertiesDefinition = clonedTypeDef.getPropertiesDefinition();

            if (propertiesDefinition == null)
            {
                // TODO throw patch error - attempting to Patch TypeDef with no properties
            }

            for (TypeDefAttribute patchTypeDefAttribute : typeDefAttributes)
            {
                if (patchTypeDefAttribute != null)
                {
                    String patchTypeDefAttributeName = patchTypeDefAttribute.getAttributeName();

                    if (patchTypeDefAttributeName != null)
                    {
                        for (TypeDefAttribute existingProperty : propertiesDefinition)
                        {
                            if (existingProperty != null)
                            {
                                if (patchTypeDefAttributeName.equals(existingProperty.getAttributeName()))
                                {

                                }
                            }
                            else
                            {
                                // TODO throw Patch Error because basic Type is messed up
                            }
                        }
                    }
                    else
                    {
                        //  TODO throw Patch Error null attribute name
                    }
                }
                else
                {
                    // TODO throw Patch Error null attribute included
                }
            }
        }

        return clonedTypeDef;
    }

    /*
     * ======================
     * OMRSInstanceHelper
     */

    /**
     * Return an entity with the header and type information filled out.  The caller only needs to add properties
     * and classifications to complete the set up of the entity.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType       - origin of the entity
     * @param userName             - name of the creator
     * @param typeName             - name of the type
     * @return partially filled out entity - needs classifications and properties
     * @throws TypeErrorException - the type name is not recognized.
     */
    public EntityDetail getSkeletonEntity(String sourceName,
                                          String metadataCollectionId,
                                          InstanceProvenanceType provenanceType,
                                          String userName,
                                          String typeName) throws TypeErrorException
    {
        final String methodName = "getSkeletonEntity";

        validateRepositoryContentManager(methodName);

        EntityDetail entity = new EntityDetail();
        String       guid   = UUID.randomUUID().toString();

        entity.setInstanceProvenanceType(provenanceType);
        entity.setMetadataCollectionId(metadataCollectionId);
        entity.setCreateTime(new Date());
        entity.setGUID(guid);
        entity.setVersion(1L);

        entity.setType(repositoryContentManager.getInstanceType(sourceName, TypeDefCategory.ENTITY_DEF, typeName, methodName));
        entity.setStatus(repositoryContentManager.getInitialStatus(sourceName, typeName, methodName));
        entity.setCreatedBy(userName);
        entity.setInstanceURL(repositoryContentManager.getEntityURL(sourceName, guid));

        return entity;
    }


    /**
     * Return a classification with the header and type information filled out.  The caller only needs to add properties
     * and possibility origin information if it is propagated to complete the set up of the classification.
     *
     * @param sourceName             - source of the request (used for logging)
     * @param userName               - name of the creator
     * @param classificationTypeName - name of the classification type
     * @param entityTypeName         - name of the type for the entity that this classification is to be attached to.
     * @return partially filled out classification - needs properties and possibly origin information
     * @throws TypeErrorException - the type name is not recognized as a classification type.
     */
    public Classification getSkeletonClassification(String sourceName,
                                                    String userName,
                                                    String classificationTypeName,
                                                    String entityTypeName) throws TypeErrorException
    {
        final String methodName = "getSkeletonClassification";

        validateRepositoryContentManager(methodName);


        if (repositoryContentManager.isValidTypeCategory(sourceName,
                                                         TypeDefCategory.CLASSIFICATION_DEF,
                                                         classificationTypeName,
                                                         methodName))
        {
            if (repositoryContentManager.isValidClassificationForEntity(sourceName,
                                                                        classificationTypeName,
                                                                        entityTypeName,
                                                                        methodName))
            {
                Classification classification = new Classification();

                classification.setName(classificationTypeName);
                classification.setCreateTime(new Date());
                classification.setCreatedBy(userName);
                classification.setVersion(1L);
                classification.setType(repositoryContentManager.getInstanceType(sourceName,
                                                                                TypeDefCategory.CLASSIFICATION_DEF,
                                                                                classificationTypeName,
                                                                                methodName));
                classification.setStatus(repositoryContentManager.getInitialStatus(sourceName,
                                                                                   classificationTypeName,
                                                                                   methodName));

                return classification;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(classificationTypeName, entityTypeName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                             this.getClass().getName(),
                                             methodName,
                                             errorMessage,
                                             errorCode.getSystemAction(),
                                             errorCode.getUserAction());
            }
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.UNKNOWN_CLASSIFICATION;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(classificationTypeName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         methodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
        }
    }


    /**
     * Return a relationship with the header and type information filled out.  The caller only needs to add properties
     * to complete the set up of the relationship.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType       - origin type of the relationship
     * @param userName             - name of the creator
     * @param typeName             - name of the relationship's type
     * @return partially filled out relationship - needs properties
     * @throws TypeErrorException - the type name is not recognized as a relationship type.
     */
    public Relationship getSkeletonRelationship(String sourceName,
                                                String metadataCollectionId,
                                                InstanceProvenanceType provenanceType,
                                                String userName,
                                                String typeName) throws TypeErrorException
    {
        final String methodName = "getSkeletonRelationship";

        validateRepositoryContentManager(methodName);

        Relationship relationship = new Relationship();
        String       guid         = UUID.randomUUID().toString();

        relationship.setInstanceProvenanceType(provenanceType);
        relationship.setMetadataCollectionId(metadataCollectionId);
        relationship.setCreateTime(new Date());
        relationship.setGUID(guid);
        relationship.setVersion(1L);

        relationship.setType(repositoryContentManager.getInstanceType(sourceName,
                                                                      TypeDefCategory.RELATIONSHIP_DEF,
                                                                      typeName,
                                                                      methodName));
        relationship.setStatus(repositoryContentManager.getInitialStatus(sourceName, typeName, methodName));
        relationship.setCreatedBy(userName);
        relationship.setInstanceURL(repositoryContentManager.getRelationshipURL(sourceName, guid));

        return relationship;
    }


    /**
     * Return a relationship with the header and type information filled out.  The caller only needs to add properties
     * to complete the set up of the relationship.
     *
     * @param sourceName     - source of the request (used for logging)
     * @param typeDefSummary - details of the new type
     * @return instance type
     * @throws TypeErrorException - the type name is not recognized as a relationship type.
     */
    public InstanceType getNewInstanceType(String sourceName,
                                           TypeDefSummary typeDefSummary) throws TypeErrorException
    {
        final String methodName = "getNewInstanceType";

        validateRepositoryContentManager(methodName);

        return repositoryContentManager.getInstanceType(sourceName,
                                                        typeDefSummary.getCategory(),
                                                        typeDefSummary.getName(),
                                                        methodName);
    }


    /**
     * Return a filled out entity.  It just needs to add the classifications.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType       - origin of the entity
     * @param userName             - name of the creator
     * @param typeName             - name of the type
     * @param properties           - properties for the entity
     * @param classifications      - list of classifications for the entity
     * @return an entity that is filled out
     * @throws TypeErrorException - the type name is not recognized as an entity type
     */
    public EntityDetail getNewEntity(String sourceName,
                                     String metadataCollectionId,
                                     InstanceProvenanceType provenanceType,
                                     String userName,
                                     String typeName,
                                     InstanceProperties properties,
                                     List<Classification> classifications) throws TypeErrorException
    {
        EntityDetail entity = this.getSkeletonEntity(sourceName,
                                                     metadataCollectionId,
                                                     provenanceType,
                                                     userName,
                                                     typeName);

        entity.setProperties(properties);
        entity.setClassifications(classifications);

        return entity;
    }


    /**
     * Return a filled out relationship - just needs the entity proxies added.
     *
     * @param sourceName           - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType       - origin of the relationship
     * @param userName             - name of the creator
     * @param typeName             - name of the type
     * @param properties           - properties for the relationship
     * @return a relationship that is filled out
     * @throws TypeErrorException - the type name is not recognized as a relationship type
     */
    public Relationship getNewRelationship(String sourceName,
                                           String metadataCollectionId,
                                           InstanceProvenanceType provenanceType,
                                           String userName,
                                           String typeName,
                                           InstanceProperties properties) throws TypeErrorException
    {
        Relationship relationship = this.getSkeletonRelationship(sourceName,
                                                                 metadataCollectionId,
                                                                 provenanceType,
                                                                 userName,
                                                                 typeName);

        relationship.setProperties(properties);

        return relationship;
    }


    /**
     * Return a classification with the header and type information filled out.  The caller only needs to add properties
     * to complete the set up of the classification.
     *
     * @param sourceName     - source of the request (used for logging)
     * @param userName       - name of the creator
     * @param typeName       - name of the type
     * @param entityTypeName - name of the type for the entity that this classification is to be attached to.
     * @param properties     - properties for the classification
     * @return partially filled out classification - needs properties and possibly origin information
     * @throws TypeErrorException - the type name is not recognized as a classification type.
     */
    public Classification getNewClassification(String sourceName,
                                               String userName,
                                               String typeName,
                                               String entityTypeName,
                                               ClassificationOrigin classificationOrigin,
                                               String classificationOriginGUID,
                                               InstanceProperties properties) throws TypeErrorException
    {
        Classification classification = this.getSkeletonClassification(sourceName,
                                                                       userName,
                                                                       typeName,
                                                                       entityTypeName);

        classification.setClassificationOrigin(classificationOrigin);
        classification.setClassificationOriginGUID(classificationOriginGUID);
        classification.setProperties(properties);

        return classification;
    }


    /**
     * Add a classification to an existing entity.
     *
     * @param sourceName        - source of the request (used for logging)
     * @param entity            - entity to update
     * @param newClassification - classification to update
     * @param methodName        - calling method
     * @return updated entity
     */
    public EntityDetail addClassificationToEntity(String sourceName,
                                                  EntityDetail entity,
                                                  Classification newClassification,
                                                  String methodName)
    {
        EntityDetail updatedEntity = new EntityDetail(entity);

        if (newClassification != null)
        {
            /*
             * Duplicate classifications are not allowed so a hash map is used to remove duplicates.
             */
            HashMap<String, Classification> entityClassificationsMap = new HashMap<>();
            List<Classification>            entityClassifications    = updatedEntity.getClassifications();

            if (entityClassifications != null)
            {
                for (Classification existingClassification : entityClassifications)
                {
                    if (existingClassification != null)
                    {
                        entityClassificationsMap.put(existingClassification.getName(), existingClassification);
                    }
                }
            }

            entityClassificationsMap.put(newClassification.getName(), newClassification);

            if (entityClassificationsMap.isEmpty())
            {
                updatedEntity.setClassifications(null);
            }
            else
            {
                entityClassifications = new ArrayList<>(entityClassificationsMap.values());

                updatedEntity.setClassifications(entityClassifications);
            }

            return updatedEntity;
        }
        else
        {
            final String thisMethodName = "addClassificationToEntity";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_CREATED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                     thisMethodName,
                                                                                                     methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return the names classification from an existing entity.
     *
     * @param sourceName         - source of the request (used for logging)
     * @param entity             - entity to update
     * @param classificationName - classification to retrieve
     * @param methodName         - calling method
     * @return located classification
     * @throws ClassificationErrorException - the classification is not attached to the entity
     */
    public Classification getClassificationFromEntity(String sourceName,
                                                      EntityDetail entity,
                                                      String classificationName,
                                                      String methodName) throws ClassificationErrorException
    {
        final String thisMethodName = "getClassificationFromEntity";

        if ((entity == null) || (classificationName == null))
        {
            OMRSErrorCode errorCode = OMRSErrorCode.VALIDATION_LOGIC_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                     thisMethodName,
                                                                                                     methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        List<Classification> entityClassifications = entity.getClassifications();

        if (entityClassifications != null)
        {
            for (Classification entityClassification : entityClassifications)
            {
                if (classificationName.equals(entityClassification.getName()))
                {
                    return entityClassification;
                }
            }
        }

        OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;
        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                 sourceName,
                                                                                                 classificationName,
                                                                                                 entity.getGUID());
        throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
    }


    /**
     * Replace an existing classification with a new one
     *
     * @param sourceName        - source of the request (used for logging)
     * @param userName          - name of the editor
     * @param entity            - entity to update
     * @param newClassification - classification to update
     * @param methodName        - calling method
     * @return updated entity
     */
    public EntityDetail updateClassificationInEntity(String sourceName,
                                                     String userName,
                                                     EntityDetail entity,
                                                     Classification newClassification,
                                                     String methodName)
    {
        if (newClassification != null)
        {
            Classification updatedClassification = new Classification(newClassification);

            updatedClassification = incrementVersion(userName, newClassification, updatedClassification);

            return this.addClassificationToEntity(sourceName, entity, updatedClassification, methodName);
        }
        else
        {
            final String thisMethodName = "updateClassificationInEntity";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_CREATED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                     thisMethodName,
                                                                                                     methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return a oldClassification with the header and type information filled out.  The caller only needs to add properties
     * to complete the set up of the oldClassification.
     *
     * @param sourceName            - source of the request (used for logging)
     * @param entity                - entity to update
     * @param oldClassificationName - classification to remove
     * @param methodName            - calling method
     * @return updated entity
     * @throws ClassificationErrorException - the entity was not classified with this classification
     */
    public EntityDetail deleteClassificationFromEntity(String sourceName,
                                                       EntityDetail entity,
                                                       String oldClassificationName,
                                                       String methodName) throws ClassificationErrorException
    {
        EntityDetail updatedEntity = new EntityDetail(entity);

        if (oldClassificationName != null)
        {
            /*
             * Duplicate classifications are not allowed so a hash map is used to remove duplicates.
             */
            HashMap<String, Classification> entityClassificationsMap = new HashMap<>();
            List<Classification>            entityClassifications    = updatedEntity.getClassifications();

            if (entityClassifications != null)
            {
                for (Classification existingClassification : entityClassifications)
                {
                    if (existingClassification != null)
                    {
                        entityClassificationsMap.put(existingClassification.getName(), existingClassification);
                    }
                }
            }

            Classification oldClassification = entityClassificationsMap.remove(oldClassificationName);

            if (oldClassification == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                         sourceName,
                                                                                                         oldClassificationName,
                                                                                                         entity.getGUID());
                throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                                                       this.getClass().getName(),
                                                       methodName,
                                                       errorMessage,
                                                       errorCode.getSystemAction(),
                                                       errorCode.getUserAction());
            }

            if (entityClassificationsMap.isEmpty())
            {
                entity.setClassifications(null);
            }
            else
            {
                entityClassifications = new ArrayList<>(entityClassificationsMap.values());

                updatedEntity.setClassifications(entityClassifications);
            }

            return updatedEntity;
        }
        else
        {
            final String thisMethodName = "deleteClassificationFromEntity";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_NAME;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                     thisMethodName,
                                                                                                     methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Merge two sets of instance properties.
     *
     * @param sourceName         - source of the request (used for logging)
     * @param existingProperties - current set of properties
     * @param newProperties      - properties to add/update
     * @return merged properties
     */
    public InstanceProperties mergeInstanceProperties(String             sourceName,
                                                      InstanceProperties existingProperties,
                                                      InstanceProperties newProperties)
    {
        InstanceProperties mergedProperties;

        if (existingProperties == null)
        {
            mergedProperties = newProperties;
        }
        else
        {
            mergedProperties = existingProperties;

            if (newProperties != null)
            {
                Iterator<String> newPropertyNames = newProperties.getPropertyNames();

                while (newPropertyNames.hasNext())
                {
                    String newPropertyName = newPropertyNames.next();

                    mergedProperties.setProperty(newPropertyName, newProperties.getPropertyValue(newPropertyName));
                }
            }
        }

        return mergedProperties;
    }


    /**
     * Changes the control information to reflect an update in an instance.
     *
     * @param userId           - user making the change.
     * @param originalInstance - original instance before the change
     * @param updatedInstance  - new version of the instance that needs updating
     */
    public Relationship incrementVersion(String              userId,
                                         InstanceAuditHeader originalInstance,
                                         Relationship        updatedInstance)
    {
        updatedInstance.setUpdatedBy(userId);
        updatedInstance.setUpdateTime(new Date());

        long currentVersion = originalInstance.getVersion();
        updatedInstance.setVersion(currentVersion++);

        return updatedInstance;
    }


    /**
     * Changes the control information to reflect an update in an instance.
     *
     * @param userId           - user making the change.
     * @param originalInstance - original instance before the change
     * @param updatedInstance  - new version of the instance that needs updating
     */
    public Classification incrementVersion(String              userId,
                                           InstanceAuditHeader originalInstance,
                                           Classification      updatedInstance)
    {
        updatedInstance.setUpdatedBy(userId);
        updatedInstance.setUpdateTime(new Date());

        long currentVersion = originalInstance.getVersion();
        updatedInstance.setVersion(currentVersion++);

        return updatedInstance;
    }


    /**
     * Changes the control information to reflect an update in an instance.
     *
     * @param userId           - user making the change.
     * @param originalInstance - original instance before the change
     * @param updatedInstance  - new version of the instance that needs updating
     */
    public EntityDetail incrementVersion(String              userId,
                                         InstanceAuditHeader originalInstance,
                                         EntityDetail        updatedInstance)
    {
        updatedInstance.setUpdatedBy(userId);
        updatedInstance.setUpdateTime(new Date());

        long currentVersion = originalInstance.getVersion();
        updatedInstance.setVersion(currentVersion++);

        return updatedInstance;
    }


    /**
     * Generate an entity proxy from an entity and its TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param entity     - entity instance
     * @return - new entity proxy
     * @throws RepositoryErrorException - logic error in the repository - corrupted entity
     */
    public EntityProxy getNewEntityProxy(String       sourceName,
                                         EntityDetail entity) throws RepositoryErrorException
    {
        final String  methodName = "getNewEntityProxy";
        final String  parameterName = "entity";

        validateRepositoryContentManager(methodName);

        if (entity != null)
        {
            InstanceType type = entity.getType();

            if (type != null)
            {
                try
                {
                    TypeDef typeDef = repositoryContentManager.getTypeDef(sourceName,
                                                                          parameterName,
                                                                          parameterName,
                                                                          type.getTypeDefGUID(),
                                                                          type.getTypeDefName(),
                                                                          methodName);

                    EntityProxy            entityProxy          = new EntityProxy(entity);
                    InstanceProperties     entityProperties     = entity.getProperties();
                    List<TypeDefAttribute> propertiesDefinition = typeDef.getPropertiesDefinition();
                    InstanceProperties     uniqueAttributes     = new InstanceProperties();

                    for (TypeDefAttribute typeDefAttribute : propertiesDefinition)
                    {
                        if (typeDefAttribute != null)
                        {
                            String propertyName = typeDefAttribute.getAttributeName();

                            if ((typeDefAttribute.isUnique()) && (propertyName != null))
                            {
                                InstancePropertyValue propertyValue = entityProperties.getPropertyValue(propertyName);

                                if (propertyValue != null)
                                {
                                    uniqueAttributes.setProperty(propertyName, propertyValue);
                                }
                            }
                        }
                    }

                    if (uniqueAttributes.getPropertyCount() > 0)
                    {
                        entityProxy.setUniqueProperties(uniqueAttributes);
                    }

                    return entityProxy;
                }
                catch (TypeErrorException error)
                {
                    OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                             methodName,
                                                                                                             error.getErrorMessage());

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                       this.getClass().getName(),
                                                       methodName,
                                                       errorMessage,
                                                       errorCode.getSystemAction(),
                                                       errorCode.getUserAction());
                }
            }
        }

        return null;
    }


    /**
     * Return boolean true if entity is linked by this relationship.
     *
     * @param sourceName   - name of source requesting help
     * @param entityGUID   - unique identifier of entity
     * @param relationship - relationship to test
     * @return boolean indicating whether the entity is mentioned in the relationship
     */
    public boolean relatedEntity(String sourceName,
                                 String entityGUID,
                                 Relationship relationship)
    {
        if (relationship != null)
        {
            EntityProxy entityOneProxy = relationship.getEntityOneProxy();
            EntityProxy entityTwoProxy = relationship.getEntityTwoProxy();

            if (entityOneProxy != null)
            {
                if (entityGUID.equals(entityOneProxy.getGUID()))
                {
                    return true;
                }
            }

            if (entityTwoProxy != null)
            {
                if (entityGUID.equals(entityTwoProxy.getGUID()))
                {
                    return true;
                }
            }
        }

        return false;
    }


    /**
     * Return the requested property or null if property is not found.  If the property is not
     * a string property then a logic exception is thrown
     *
     * @param sourceName - source of call
     * @param propertyName - name of requested property
     * @param properties - properties from the instance.
     * @param methodName - method of caller
     * @return string property value or null
     */
    public String getStringProperty(String             sourceName,
                                    String             propertyName,
                                    InstanceProperties properties,
                                    String             methodName)
    {
        final String  thisMethodName = "getStringProperty";

        if (properties != null)
        {
            InstancePropertyValue instancePropertyValue = properties.getPropertyValue(propertyName);

            try
            {
                if (instancePropertyValue.getInstancePropertyCategory() == InstancePropertyCategory.PRIMITIVE)
                {
                    PrimitivePropertyValue primitivePropertyValue = (PrimitivePropertyValue) instancePropertyValue;

                    if (primitivePropertyValue.getPrimitiveDefCategory() == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING)
                    {
                        return primitivePropertyValue.getPrimitiveValue().toString();
                    }
                }
            }
            catch (Throwable  error)
            {
                throwHelperLogicError(sourceName, methodName, thisMethodName);
            }
        }

        return null;
    }


    /**
     * Return the requested property or null if property is not found.  If the property is not
     * a string property then a logic exception is thrown
     *
     * @param sourceName - source of call
     * @param propertyName - name of requested property
     * @param properties - properties from the instance.
     * @param methodName - method of caller
     * @return string property value or null
     */
    public InstanceProperties getMapProperty(String             sourceName,
                                             String             propertyName,
                                             InstanceProperties properties,
                                             String             methodName)
    {
        final String  thisMethodName = "getMapProperty";

        if (properties != null)
        {
            InstancePropertyValue instancePropertyValue = properties.getPropertyValue(propertyName);

            try
            {
                if (instancePropertyValue.getInstancePropertyCategory() == InstancePropertyCategory.MAP)
                {
                    MapPropertyValue mapPropertyValue = (MapPropertyValue) instancePropertyValue;

                    return mapPropertyValue.getMapValues();
                }
            }
            catch (Throwable  error)
            {
                throwHelperLogicError(sourceName, methodName, thisMethodName);
            }
        }

        return null;
    }


    /**
     * Return the requested property or 0 if property is not found.  If the property is not
     * a int property then a logic exception is thrown
     *
     * @param sourceName - source of call
     * @param propertyName - name of requested property
     * @param properties - properties from the instance.
     * @param methodName - method of caller
     * @return string property value or null
     */
    public int    getIntProperty(String             sourceName,
                                 String             propertyName,
                                 InstanceProperties properties,
                                 String             methodName)
    {
        final String  thisMethodName = "getIntProperty";

        if (properties != null)
        {
            InstancePropertyValue instancePropertyValue = properties.getPropertyValue(propertyName);

            try
            {
                if (instancePropertyValue.getInstancePropertyCategory() == InstancePropertyCategory.PRIMITIVE)
                {
                    PrimitivePropertyValue primitivePropertyValue = (PrimitivePropertyValue) instancePropertyValue;

                    if (primitivePropertyValue.getPrimitiveDefCategory() == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_INT)
                    {
                        return Integer.valueOf(primitivePropertyValue.getPrimitiveValue().toString());
                    }
                }
            }
            catch (Throwable  error)
            {
                throwHelperLogicError(sourceName, methodName, thisMethodName);
            }
        }

        return 0;
    }


    /**
     * Return the requested property or false if property is not found.  If the property is not
     * a boolean property then a logic exception is thrown
     *
     * @param sourceName - source of call
     * @param propertyName - name of requested property
     * @param properties - properties from the instance.
     * @param methodName - method of caller
     * @return string property value or null
     */
    public boolean getBooleanProperty(String             sourceName,
                                      String             propertyName,
                                      InstanceProperties properties,
                                      String             methodName)
    {
        final String  thisMethodName = "getBooleanProperty";

        if (properties != null)
        {
            InstancePropertyValue instancePropertyValue = properties.getPropertyValue(propertyName);

            try
            {
                if (instancePropertyValue.getInstancePropertyCategory() == InstancePropertyCategory.PRIMITIVE)
                {
                    PrimitivePropertyValue primitivePropertyValue = (PrimitivePropertyValue) instancePropertyValue;

                    if (primitivePropertyValue.getPrimitiveDefCategory() == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BOOLEAN)
                    {
                        return Boolean.valueOf(primitivePropertyValue.getPrimitiveValue().toString());
                    }
                }
            }
            catch (Throwable  error)
            {
                throwHelperLogicError(sourceName, methodName, thisMethodName);
            }
        }

        return false;
    }


    /**
     * Add the supplied property to an instance properties object.  If the instance property object
     * supplied is null, a new instance properties object is created.
     *
     * @param sourceName - name of caller
     * @param properties - properties object to add property to - may be null.
     * @param propertyName - name of property
     * @param propertyValue - value of property
     * @param methodName - calling method name
     * @return instance properties object.
     */
    public InstanceProperties addStringPropertyToInstance(String             sourceName,
                                                          InstanceProperties properties,
                                                          String             propertyName,
                                                          String             propertyValue,
                                                          String             methodName)
    {
        InstanceProperties  resultingProperties;

        if (properties == null)
        {
            resultingProperties = new InstanceProperties();
        }
        else
        {
            resultingProperties = properties;
        }


        PrimitivePropertyValue primitivePropertyValue = new PrimitivePropertyValue();

        primitivePropertyValue.setPrimitiveDefCategory(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
        primitivePropertyValue.setPrimitiveValue(propertyValue);

        resultingProperties.setProperty(propertyName, primitivePropertyValue);

        return resultingProperties;
    }


    /**
     * Add the supplied property to an instance properties object.  If the instance property object
     * supplied is null, a new instance properties object is created.
     *
     * @param sourceName - name of caller
     * @param properties - properties object to add property to - may be null.
     * @param propertyName - name of property
     * @param propertyValue - value of property
     * @param methodName - calling method name
     * @return instance properties object.
     */
    public InstanceProperties addIntPropertyToInstance(String             sourceName,
                                                       InstanceProperties properties,
                                                       String             propertyName,
                                                       int                propertyValue,
                                                       String             methodName)
    {
        InstanceProperties  resultingProperties;

        if (properties == null)
        {
            resultingProperties = new InstanceProperties();
        }
        else
        {
            resultingProperties = properties;
        }


        PrimitivePropertyValue primitivePropertyValue = new PrimitivePropertyValue();

        primitivePropertyValue.setPrimitiveDefCategory(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_INT);
        primitivePropertyValue.setPrimitiveValue(propertyValue);

        resultingProperties.setProperty(propertyName, primitivePropertyValue);

        return resultingProperties;
    }


    /**
     * Add the supplied property to an instance properties object.  If the instance property object
     * supplied is null, a new instance properties object is created.
     *
     * @param sourceName - name of caller
     * @param properties - properties object to add property to - may be null.
     * @param propertyName - name of property
     * @param propertyValue - value of property
     * @param methodName - calling method name
     * @return instance properties object.
     */
    public InstanceProperties addBooleanPropertyToInstance(String             sourceName,
                                                           InstanceProperties properties,
                                                           String             propertyName,
                                                           boolean            propertyValue,
                                                           String             methodName)
    {
        InstanceProperties  resultingProperties;

        if (properties == null)
        {
            resultingProperties = new InstanceProperties();
        }
        else
        {
            resultingProperties = properties;
        }


        PrimitivePropertyValue primitivePropertyValue = new PrimitivePropertyValue();

        primitivePropertyValue.setPrimitiveDefCategory(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BOOLEAN);
        primitivePropertyValue.setPrimitiveValue(propertyValue);

        resultingProperties.setProperty(propertyName, primitivePropertyValue);

        return resultingProperties;
    }


    /**
     * Add the supplied property to an instance properties object.  If the instance property object
     * supplied is null, a new instance properties object is created.
     *
     * @param sourceName - name of caller
     * @param properties - properties object to add property to - may be null.
     * @param propertyName - name of property
     * @param ordinal - numeric value of property
     * @param symbolicName - String value of property
     * @param description - String description of property value
     * @param methodName - calling method name
     * @return instance properties object.
     */
    public InstanceProperties addEnumPropertyToInstance(String             sourceName,
                                                        InstanceProperties properties,
                                                        String             propertyName,
                                                        int                ordinal,
                                                        String             symbolicName,
                                                        String             description,
                                                        String             methodName)
    {
        InstanceProperties  resultingProperties;

        if (properties == null)
        {
            resultingProperties = new InstanceProperties();
        }
        else
        {
            resultingProperties = properties;
        }


        EnumPropertyValue enumPropertyValue = new EnumPropertyValue();

        enumPropertyValue.setOrdinal(ordinal);
        enumPropertyValue.setSymbolicName(symbolicName);
        enumPropertyValue.setDescription(description);

        resultingProperties.setProperty(propertyName, enumPropertyValue);

        return resultingProperties;
    }


    /**
     * Throws a logic error exception when the repository helper is called with invalid parameters.
     * Normally this means the repository helper methods have been called in the wrong order.
     *
     * @param sourceName - name of the calling repository or service
     * @param originatingMethodName - method that called the repository validator
     * @param localMethodName - local method that deleted the error
     */
    private void throwHelperLogicError(String     sourceName,
                                       String     originatingMethodName,
                                       String     localMethodName)
    {
        OMRSErrorCode errorCode = OMRSErrorCode.HELPER_LOGIC_ERROR;
        String errorMessage     = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                     localMethodName,
                                                                                                     originatingMethodName);

        throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          localMethodName,
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
    }


    /**
     * Throw a logic error exception if this object does not have a repository content manager.
     * This would occur if if is being used in an environment where the OMRS has not been properly
     * initialized.
     *
     * @param methodName - name of calling method.
     */
    private void validateRepositoryContentManager(String   methodName)
    {
        if (repositoryContentManager == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }
}
