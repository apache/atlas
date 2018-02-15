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
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.ffdc.exception.PatchErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefPatch;

import org.apache.atlas.omrs.ffdc.exception.TypeErrorException;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

/**
 * OMRSRepositoryHelper provides methods to repository connectors and repository event mappers to help
 * them build valid type definitions (TypeDefs), entities and relationships.  It is a facade to the
 * repository content manager which holds an in memory cache of all the active TypeDefs in the local server.
 * OMRSRepositoryHelper's purpose is to create a object that the repository connectors and event mappers can
 * create, use and discard without needing to know how to connect to the repository content manager.
 */
public class OMRSRepositoryHelper implements OMRSTypeDefHelper, OMRSInstanceHelper
{
    private static OMRSRepositoryContentManager    repositoryContentManager = null;

    /**
     * Set up the local repository's content manager.  This maintains a cache of the local repository's type
     * definitions and rules to provide helpers and validators for TypeDefs and instances that are
     * exchanged amongst the open metadata repositories and open metadata access services (OMAS).
     *
     * @param repositoryContentManager - link to repository content manager.
     */
    public static synchronized void setRepositoryContentManager(OMRSRepositoryContentManager    repositoryContentManager)
    {
        OMRSRepositoryHelper.repositoryContentManager = repositoryContentManager;
    }


    /*
     * ========================
     * OMRSTypeDefHelper
     */

    /**
     * Return the TypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    public TypeDef  getTypeDefByName (String    sourceName,
                                      String    typeDefName)
    {
        final String  methodName = "getTypeDefByName()";

        if (repositoryContentManager != null)
        {
            /*
             * Delegate call to repository content manager.
             */
            return repositoryContentManager.getTypeDefByName(sourceName, typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return the AttributeTypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefName - unique name for the TypeDef
     * @return AttributeTypeDef object or null if AttributeTypeDef is not known.
     */
    public AttributeTypeDef getAttributeTypeDefByName (String    sourceName,
                                                       String    attributeTypeDefName)
    {
        final String  methodName = "getAttributeTypeDefByName()";

        if (repositoryContentManager != null)
        {
            /*
             * Delegate call to repository content manager.
             */
            return repositoryContentManager.getAttributeTypeDefByName(sourceName, attributeTypeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return the TypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type of a metadata instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public TypeDef  getTypeDef (String    sourceName,
                                String    typeDefGUID,
                                String    typeDefName) throws TypeErrorException
    {
        final String  methodName = "getTypeDef()";

        if (repositoryContentManager != null)
        {
            /*
             * Delegate call to repository content manager.
             */
            return repositoryContentManager.getTypeDef(sourceName, typeDefGUID, typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type definition of a metadata instance's
     * property.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @param attributeTypeDefName - unique name for the AttributeTypeDef
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public AttributeTypeDef  getAttributeTypeDef (String    sourceName,
                                                  String    attributeTypeDefGUID,
                                                  String    attributeTypeDefName) throws TypeErrorException
    {
        final String  methodName = "getAttributeTypeDef()";

        if (repositoryContentManager != null)
        {
            /*
             * Delegate call to repository content manager.
             */
            return repositoryContentManager.getAttributeTypeDef(sourceName, attributeTypeDefGUID, attributeTypeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Returns an updated TypeDef that has had the supplied patch applied.  It throws an exception if any part of
     * the patch is incompatible with the original TypeDef.  For example, if there is a mismatch between
     * the type or version that either represents.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefPatch - patch to apply
     * @return updated TypeDef
     * @throws PatchErrorException - the patch is either badly formatted, or does not apply to the supplied TypeDef
     */
    public TypeDef applyPatch(String sourceName, TypeDefPatch typeDefPatch) throws PatchErrorException
    {
        final String  methodName = "applyPatch()";

        if (repositoryContentManager != null)
        {
            /*
             * Delegate call to repository content manager.
             */
            return repositoryContentManager.applyPatch(sourceName, typeDefPatch);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /*
     * ======================
     * OMRSInstanceHelper
     */


    /**
     * Return an entity with the header and type information filled out.  The caller only needs to add properties
     * and classifications to complete the set up of the entity.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin of the entity
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @return partially filled out entity - needs classifications and properties
     * @throws TypeErrorException - the type name is not recognized.
     */
    public EntityDetail getSkeletonEntity(String                  sourceName,
                                          String                  metadataCollectionId,
                                          InstanceProvenanceType  provenanceType,
                                          String                  userName,
                                          String                  typeName) throws TypeErrorException
    {
        final String methodName = "getSkeletonEntity()";

        if (repositoryContentManager != null)
        {
            EntityDetail entity = new EntityDetail();
            String       guid   = UUID.randomUUID().toString();

            entity.setInstanceProvenanceType(provenanceType);
            entity.setMetadataCollectionId(metadataCollectionId);
            entity.setCreateTime(new Date());
            entity.setGUID(guid);
            entity.setVersion(1L);

            entity.setType(repositoryContentManager.getInstanceType(sourceName, TypeDefCategory.ENTITY_DEF, typeName));
            entity.setStatus(repositoryContentManager.getInitialStatus(sourceName, typeName));
            entity.setCreatedBy(userName);
            entity.setInstanceURL(repositoryContentManager.getInstanceURL(sourceName, guid));

            return entity;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return a classification with the header and type information filled out.  The caller only needs to add properties
     * and possibility origin information if it is propagated to complete the set up of the classification.
     *
     * @param sourceName - source of the request (used for logging)
     * @param userName - name of the creator
     * @param classificationTypeName - name of the classification type
     * @param entityTypeName - name of the type for the entity that this classification is to be attached to.
     * @return partially filled out classification - needs properties and possibly origin information
     * @throws TypeErrorException - the type name is not recognized as a classification type.
     */
    public Classification getSkeletonClassification(String       sourceName,
                                                    String       userName,
                                                    String       classificationTypeName,
                                                    String       entityTypeName) throws TypeErrorException
    {
        final String  methodName = "getSkeletonClassification()";

        if (repositoryContentManager != null)
        {
            if (repositoryContentManager.isValidTypeCategory(sourceName,
                                                             TypeDefCategory.CLASSIFICATION_DEF,
                                                             classificationTypeName))
            {
                if (repositoryContentManager.isValidClassificationForEntity(sourceName,
                                                                            classificationTypeName,
                                                                            entityTypeName))
                {
                    Classification classification = new Classification();

                    classification.setName(classificationTypeName);
                    classification.setCreateTime(new Date());
                    classification.setCreatedBy(userName);
                    classification.setVersion(1L);
                    classification.setStatus(repositoryContentManager.getInitialStatus(sourceName,
                                                                                       classificationTypeName));

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
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
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
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin type of the relationship
     * @param userName - name of the creator
     * @param typeName - name of the relationship's type
     * @return partially filled out relationship - needs properties
     * @throws TypeErrorException - the type name is not recognized as a relationship type.
     */
    public Relationship getSkeletonRelationship(String                  sourceName,
                                                String                  metadataCollectionId,
                                                InstanceProvenanceType  provenanceType,
                                                String                  userName,
                                                String                  typeName) throws TypeErrorException
    {
        final String  methodName = "getSkeletonRelationship()";


        if (repositoryContentManager != null)
        {
            Relationship relationship = new Relationship();
            String       guid = UUID.randomUUID().toString();

            relationship.setInstanceProvenanceType(provenanceType);
            relationship.setMetadataCollectionId(metadataCollectionId);
            relationship.setCreateTime(new Date());
            relationship.setGUID(guid);
            relationship.setVersion(1L);

            relationship.setType(repositoryContentManager.getInstanceType(sourceName,
                                                                          TypeDefCategory.RELATIONSHIP_DEF,
                                                                          typeName));
            relationship.setStatus(repositoryContentManager.getInitialStatus(sourceName, typeName));
            relationship.setCreatedBy(userName);
            relationship.setInstanceURL(repositoryContentManager.getInstanceURL(sourceName, guid));

            return relationship;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return a filled out entity.  It just needs to add the classifications.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin of the entity
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @param properties - properties for the entity
     * @param classifications - list of classifications for the entity
     * @return an entity that is filled out
     * @throws TypeErrorException - the type name is not recognized as an entity type
     */
    public EntityDetail getNewEntity(String                    sourceName,
                                     String                    metadataCollectionId,
                                     InstanceProvenanceType    provenanceType,
                                     String                    userName,
                                     String                    typeName,
                                     InstanceProperties        properties,
                                     ArrayList<Classification> classifications) throws TypeErrorException
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
     * Return a filled out relationship.
     *
     * @param sourceName - source of the request (used for logging)
     * @param metadataCollectionId - unique identifier for the home metadata collection
     * @param provenanceType - origin of the relationship
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @param properties - properties for the relationship
     * @return a relationship that is filled out
     * @throws TypeErrorException - the type name is not recognized as a relationship type
     */
    public Relationship getNewRelationship(String                  sourceName,
                                           String                  metadataCollectionId,
                                           InstanceProvenanceType  provenanceType,
                                           String                  userName,
                                           String                  typeName,
                                           InstanceProperties      properties) throws TypeErrorException
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
     * @param sourceName - source of the request (used for logging)
     * @param userName - name of the creator
     * @param typeName - name of the type
     * @param entityTypeName - name of the type for the entity that this classification is to be attached to.
     * @param properties - properties for the classification
     * @return partially filled out classification - needs properties and possibly origin information
     * @throws TypeErrorException - the type name is not recognized as a classification type.
     */
    public Classification getNewClassification(String               sourceName,
                                               String               userName,
                                               String               typeName,
                                               String               entityTypeName,
                                               ClassificationOrigin classificationOrigin,
                                               String               classificationOriginGUID,
                                               InstanceProperties   properties) throws TypeErrorException
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
}
