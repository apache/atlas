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

package org.apache.atlas.omrs.archivemanager;

import org.apache.atlas.omrs.archivemanager.properties.*;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * OMRSArchiveBuilder creates an in-memory copy of an open metadata archive that can be saved to disk or processed
 * by a server.
 */
public class OMRSArchiveBuilder
{
    /*
     * Archive properties supplied on the constructor
     */
    private OpenMetadataArchiveProperties archiveProperties = null;

    /*
     * Hash maps for accumulating TypeDefs and instances as the content of the archive is built up.
     */
    private Map<String, PrimitiveDef>      primitiveDefMap       = new HashMap<>();
    private ArrayList<PrimitiveDef>        primitiveDefList      = new ArrayList<>();
    private Map<String, EnumDef>           enumDefMap            = new HashMap<>();
    private ArrayList<EnumDef>             enumDefList           = new ArrayList<>();
    private Map<String, CollectionDef>     collectionDefMap      = new HashMap<>();
    private ArrayList<CollectionDef>       collectionDefList     = new ArrayList<>();
    private Map<String, ClassificationDef> classificationDefMap  = new HashMap<>();
    private ArrayList<ClassificationDef>   classificationDefList = new ArrayList<>();
    private Map<String, EntityDef>         entityDefMap          = new HashMap<>();
    private ArrayList<EntityDef>           entityDefList         = new ArrayList<>();
    private Map<String, RelationshipDef>   relationshipDefMap    = new HashMap<>();
    private ArrayList<RelationshipDef>     relationshipDefList   = new ArrayList<>();
    private Map<String, TypeDefPatch>      typeDefPatchMap       = new HashMap<>();
    private ArrayList<TypeDefPatch>        typeDefPatchList      = new ArrayList<>();
    private Map<String, EntityDetail>      entityDetailMap       = new HashMap<>();
    private ArrayList<EntityDetail>        entityDetailList      = new ArrayList<>();
    private Map<String, Relationship>      relationshipMap       = new HashMap<>();
    private ArrayList<Relationship>        relationshipList      = new ArrayList<>();
    private Map<String, Object>            guidMap               = new HashMap<>();
    private Map<String, Object>            nameMap               = new HashMap<>();
    /*
     * map to keep track of all the AttributeDef names including relationship end names per EntityDef name.
     */
    private Map<String,Set<String>> entityAttributeMap = new HashMap();


    private static final Logger log = LoggerFactory.getLogger(OMRSArchiveBuilder.class);


    /**
     * Typical constructor passes parameters used to build the open metadata archive's property header.
     *
     * @param archiveGUID - unique identifier for this open metadata archive.
     * @param archiveName - name of the open metadata archive.
     * @param archiveDescription - description of the open metadata archive.
     * @param archiveType - enum describing the type of archive this is.
     * @param originatorName - name of the originator (person or organization) of the archive.
     * @param creationDate - data that this archive was created.
     * @param dependsOnArchives - list of GUIDs for archives that this archive depends on (null for no dependencies).
     */
    public OMRSArchiveBuilder(String                  archiveGUID,
                              String                  archiveName,
                              String                  archiveDescription,
                              OpenMetadataArchiveType archiveType,
                              String                  originatorName,
                              Date                    creationDate,
                              ArrayList<String>       dependsOnArchives)
    {
        this.archiveProperties = new OpenMetadataArchiveProperties();

        this.archiveProperties.setArchiveGUID(archiveGUID);
        this.archiveProperties.setArchiveName(archiveName);
        this.archiveProperties.setArchiveDescription(archiveDescription);
        this.archiveProperties.setArchiveType(archiveType);
        this.archiveProperties.setOriginatorName(originatorName);
        this.archiveProperties.setCreationDate(creationDate);
        this.archiveProperties.setDependsOnArchives(dependsOnArchives);
    }


    /**
     * Add a new PrimitiveDef to the archive.
     *
     * @param primitiveDef - type to add - nulls are ignored
     */
    public void addPrimitiveDef(PrimitiveDef   primitiveDef)
    {
        final String methodName = "addPrimitiveDef()";

        if (primitiveDef != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding PrimitiveDef: " + primitiveDef.toString());
            }
            this.checkForBlanksInTypeName(primitiveDef.getName());

            PrimitiveDef duplicateElement = primitiveDefMap.put(primitiveDef.getName(), primitiveDef);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(primitiveDef.getName(),
                                                                                AttributeTypeDefCategory.PRIMITIVE.getTypeName(),
                                                                                duplicateElement.toString(),
                                                                                primitiveDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(primitiveDef.getGUID(), primitiveDef);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(primitiveDef.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                primitiveDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateName = nameMap.put(primitiveDef.getName(), primitiveDef);

            if (duplicateName != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPENAME_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(primitiveDef.getGUID(),
                                                                                duplicateName.toString(),
                                                                                primitiveDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            primitiveDefList.add(primitiveDef);
        }
    }


    /**
     * Retrieve a PrimitiveDef from the archive.
     *
     * @param primitiveDefName - primitive to retrieve
     * @return PrimitiveDef type
     */
    public PrimitiveDef getPrimitiveDef(String   primitiveDefName)
    {
        final String methodName = "getPrimitiveDef()";

        if (log.isDebugEnabled())
        {
            log.debug("Retrieving PrimitiveDef: " + primitiveDefName);
        }

        if (primitiveDefName != null)
        {
            PrimitiveDef   primitiveDef  = primitiveDefMap.get(primitiveDefName);

            if (primitiveDef == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.MISSING_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(primitiveDefName,
                                                                                AttributeTypeDefCategory.PRIMITIVE.getTypeName());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            return primitiveDef;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.MISSING_NAME_FOR_ARCHIVE;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(AttributeTypeDefCategory.PRIMITIVE.getTypeName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Add a new CollectionDef to the archive.
     *
     * @param collectionDef - type to add
     */
    public void addCollectionDef(CollectionDef  collectionDef)
    {
        final String methodName = "addCollectionDef()";

        if (collectionDef != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding CollectionDef: " + collectionDef.toString());
            }
            this.checkForBlanksInTypeName(collectionDef.getName());

            CollectionDef duplicateElement = collectionDefMap.put(collectionDef.getName(), collectionDef);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(collectionDef.getName(),
                                                                                AttributeTypeDefCategory.COLLECTION.getTypeName(),
                                                                                duplicateElement.toString(),
                                                                                collectionDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(collectionDef.getGUID(), collectionDef);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(collectionDef.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                collectionDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateName = nameMap.put(collectionDef.getName(), collectionDef);

            if (duplicateName != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPENAME_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(collectionDef.getGUID(),
                                                                                duplicateName.toString(),
                                                                                collectionDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            collectionDefList.add(collectionDef);
        }
    }


    /**
     * Retrieve a CollectionDef from the archive.
     *
     * @param collectionDefName - type to retrieve
     * @return CollectionDef type
     */
    public CollectionDef getCollectionDef(String  collectionDefName)
    {
        final String methodName = "getCollectionDef()";

        if (log.isDebugEnabled())
        {
            log.debug("Retrieving CollectionDef: " + collectionDefName);
        }

        if (collectionDefName != null)
        {
            CollectionDef  collectionDef = collectionDefMap.get(collectionDefName);

            if (collectionDef == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.MISSING_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(collectionDefName,
                                                                                AttributeTypeDefCategory.COLLECTION.getTypeName());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            return collectionDef;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.MISSING_NAME_FOR_ARCHIVE;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(AttributeTypeDefCategory.COLLECTION.getTypeName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Add a new EnumDef to the archive.
     *
     * @param enumDef - type to add
     */
    public void addEnumDef(EnumDef    enumDef)
    {
        final String methodName = "addEnumDef()";

        if (enumDef != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding EnumDef: " + enumDef.toString());
            }
            this.checkForBlanksInTypeName(enumDef.getName());

            EnumDef duplicateElement = enumDefMap.put(enumDef.getName(), enumDef);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(enumDef.getName(),
                                                                                AttributeTypeDefCategory.ENUM_DEF.getTypeName(),
                                                                                duplicateElement.toString(),
                                                                                enumDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(enumDef.getGUID(), enumDef);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(enumDef.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                enumDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateName = nameMap.put(enumDef.getName(), enumDef);

            if (duplicateName != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPENAME_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(enumDef.getGUID(),
                                                                                duplicateName.toString(),
                                                                                enumDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            enumDefList.add(enumDef);
        }
    }


    /**
     * Get an existing EnumDef from the archive.
     *
     * @param enumDefName - type to retrieve
     * @return EnumDef object
     */
    public EnumDef getEnumDef(String    enumDefName)
    {
        final String methodName = "getEnumDef()";

        if (log.isDebugEnabled())
        {
            log.debug("Retrieving EnumDef: " + enumDefName);
        }

        if (enumDefName != null)
        {
            EnumDef enumDef = enumDefMap.get(enumDefName);

            if (enumDef == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.MISSING_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(enumDefName,
                                                                                AttributeTypeDefCategory.ENUM_DEF.getTypeName());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            return enumDef;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.MISSING_NAME_FOR_ARCHIVE;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(AttributeTypeDefCategory.ENUM_DEF.getTypeName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Add a new ClassificationDef to the archive.
     *
     * @param classificationDef - type to add
     */
    public void addClassificationDef(ClassificationDef   classificationDef)
    {
        final String methodName = "addClassificationDef()";

        if (classificationDef != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding ClassificationDef: " + classificationDef.toString());
            }
            this.checkForBlanksInTypeName(classificationDef.getName());

            ClassificationDef duplicateElement = classificationDefMap.put(classificationDef.getName(), classificationDef);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(classificationDef.getName(),
                                                                                TypeDefCategory.CLASSIFICATION_DEF.getTypeName(),
                                                                                duplicateElement.toString(),
                                                                                classificationDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(classificationDef.getGUID(), classificationDef);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(classificationDef.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                classificationDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateName = nameMap.put(classificationDef.getName(), classificationDef);

            if (duplicateName != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPENAME_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(classificationDef.getName(),
                                                                                duplicateName.toString(),
                                                                                classificationDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }
            if (classificationDef.getPropertiesDefinition()!=null)
            {
                Set<String> attrSet = new HashSet();

                for (TypeDefAttribute typeDefAttr :classificationDef.getPropertiesDefinition())
                {
                    String duplicateAttributeName = typeDefAttr.getAttributeName();
                    if (attrSet.contains(duplicateAttributeName))
                    {
                        // relationship duplicate attribute name
                        OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_CLASSIFICATION_ATTR_IN_ARCHIVE;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(
                                duplicateAttributeName,
                                classificationDef.getName()
                        );

                        throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    attrSet.add(duplicateAttributeName);
                }
            }

            classificationDefList.add(classificationDef);
        }
    }


    /**
     * Add a new EntityDef to the archive.
     *
     * @param entityDef - type to add
     */
    public void addEntityDef(EntityDef    entityDef)
    {
        final String methodName = "addEntityDef()";

        if (entityDef != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding EntityDef: " + entityDef.toString());
            }
            this.checkForBlanksInTypeName(entityDef.getName());

            EntityDef duplicateElement = entityDefMap.put(entityDef.getName(), entityDef);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(entityDef.getName(),
                                                                                TypeDefCategory.ENTITY_DEF.getTypeName(),
                                                                                duplicateElement.toString(),
                                                                                entityDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(entityDef.getGUID(), entityDef);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(entityDef.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                entityDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateName = nameMap.put(entityDef.getName(), entityDef);

            if (duplicateName != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPENAME_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(entityDef.getName(),
                                                                                duplicateName.toString(),
                                                                                entityDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }
            if (entityDef.getPropertiesDefinition()!=null)
            {
                Set<String> attrSet = new HashSet();

                for (TypeDefAttribute typeDefAttr :entityDef.getPropertiesDefinition())
                {
                    String duplicateAttributeName = typeDefAttr.getAttributeName();
                    if (attrSet.contains(duplicateAttributeName))
                    {
                        // relationship duplicate attribute name
                        OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_ENTITY_ATTR_IN_ARCHIVE;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(
                                duplicateAttributeName,
                                entityDef.getName()
                        );

                        throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    attrSet.add(duplicateAttributeName);
                }
            }
            entityDefList.add(entityDef);
        }
    }


    /**
     * Retrieve the entityDef - or null if it is not defined.
     *
     * @param entityDefName - name of the entity
     * @return the retrieved Entity def
     */
    public EntityDef  getEntityDef(String   entityDefName)
    {
        final String methodName = "getEntityDef()";

        if (log.isDebugEnabled())
        {
            log.debug("Retrieving EntityDef: " + entityDefName);
        }

        if (entityDefName != null)
        {
            EntityDef retrievedEntityDef = entityDefMap.get(entityDefName);

            if (retrievedEntityDef == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.MISSING_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(entityDefName,
                                                                                TypeDefCategory.ENTITY_DEF.getTypeName());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            return retrievedEntityDef;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.MISSING_NAME_FOR_ARCHIVE;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(TypeDefCategory.ENTITY_DEF.getTypeName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Add a new RelationshipDef to the archive.
     *
     * @param relationshipDef - type to add
     */
    public void addRelationshipDef(RelationshipDef   relationshipDef)
    {
        final String methodName = "addRelationshipDef()";

        if (relationshipDef != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding RelationshipDef: " + relationshipDef.toString());
            }
            this.checkForBlanksInTypeName(relationshipDef.getName());
            RelationshipDef duplicateElement = relationshipDefMap.put(relationshipDef.getName(), relationshipDef);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(relationshipDef.getName(),
                                                                                TypeDefCategory.RELATIONSHIP_DEF.getTypeName(),
                                                                                duplicateElement.toString(),
                                                                                relationshipDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(relationshipDef.getGUID(), relationshipDef);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(relationshipDef.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                relationshipDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateName = nameMap.put(relationshipDef.getName(), relationshipDef);

            if (duplicateName != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_TYPENAME_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipDef.getName(),
                        duplicateName.toString(),
                        relationshipDef.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            this.checkRelationshipDefDuplicateAttributes(relationshipDef);

            if (relationshipDef.getPropertiesDefinition()!=null)
            {
                Set<String> attrSet = new HashSet();
                for (TypeDefAttribute typeDefAttr :relationshipDef.getPropertiesDefinition())
                {
                    String duplicateAttributeName = typeDefAttr.getAttributeName();
                    if (attrSet.contains(duplicateAttributeName))
                    {
                        // relationship duplicate attribute name
                        OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_RELATIONSHIP_ATTR_IN_ARCHIVE;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(
                                duplicateAttributeName,
                                relationshipDef.getName()
                        );

                        throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    attrSet.add(duplicateAttributeName);
                }
            }
            relationshipDefList.add(relationshipDef);
        }
    }

    /**
     * Check whether the relationshipDef supplies any attributes that already exist.
     * @param relationshipDef
     * @throws OMRSLogicErrorException
     */
    protected void checkRelationshipDefDuplicateAttributes(RelationshipDef relationshipDef) throws OMRSLogicErrorException
    {
        final String methodName = "checkRelationshipDefDuplicateAttributes()";
        // check whether the end2 type already has an attribute called end1name already exists locally or in any of its relationships
        String relationshipName = relationshipDef.getName();

        RelationshipEndDef end1 = relationshipDef.getEndDef1();
        RelationshipEndDef end2 = relationshipDef.getEndDef2();

        String end1Name = end1.getAttributeName();
        String end1Type = end1.getEntityType().getName();

        String end2Name = end2.getAttributeName();
        String end2Type = end2.getEntityType().getName();

        if (end1Name.equals(end2Name) && end1Type.equals(end2Type))
        {

            if (entityAttributeMap.get(end1Type) == null)
            {
                // we have not seen this entity before
                Set<String> attrSet = new HashSet();
                attrSet.add(end1Name);
                entityAttributeMap.put(end1Type, attrSet);
            } else {
                Set<String> attrset = entityAttributeMap.get(end1Type);
                // this attribute name should not already be defined for this entity

                if (attrset.contains(end2Name))
                {
                    OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_ENDDEF2_NAME_IN_ARCHIVE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(
                            end1Type,
                            end2Name,
                            relationshipDef.getName()
                    );

                    throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                attrset.add(end1Name);
            }
        } else
        {
            if (entityAttributeMap.get(end1Type) == null)
            {
                // we have not seen this entity before
                Set<String> attrSet = new HashSet();
                attrSet.add(end2Name);
                entityAttributeMap.put(end1Type, attrSet);
            } else
            {
                Set<String> attrset = entityAttributeMap.get(end1Type);
                // this attribute name should not already be defined for this entity
                if (attrset.contains(end2Name)){
                    OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_ENDDEF2_NAME_IN_ARCHIVE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(
                            end1Type,
                            end2Name,
                            relationshipDef.getName()
                    );

                    throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                attrset.add(end2Name);
            }

            if (entityAttributeMap.get(end2Type) == null)
            {
                // we have not seen this entity before
                Set<String> attrSet = new HashSet();
                attrSet.add(end1Name);
                entityAttributeMap.put(end2Type, attrSet);
            } else
                {
                Set<String> attrset = entityAttributeMap.get(end2Type);
                // this attribute name should not already be defined for this entity
                if (attrset.contains(end1Name))
                {
                    OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_ENDDEF1_NAME_IN_ARCHIVE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(
                            end2Type,
                            end1Name,
                            relationshipDef.getName()
                    );

                    throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                attrset.add(end1Name);
            }
        }
        // check whether end1 has a local attribute name that clashes with a relationship end
        EntityDef entityDef1 = entityDefMap.get(end1Type);

        if (entityDef1.getPropertiesDefinition()!=null)
        {
            Set<String> attrset = entityAttributeMap.get(end1Type);
            for (TypeDefAttribute typeDefAttr : entityDef1.getPropertiesDefinition())
            {
                String localAttributeName = typeDefAttr.getAttributeName();
                // this attribute name should not already be defined for this entity

                if (localAttributeName.equals(end2Name))
                {
                    OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_ENDDEF2_NAME_IN_ARCHIVE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(
                            end1Type,
                            end2Name,
                            relationshipDef.getName()
                    );

                    throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                attrset.add(end2Name);
            }
        }
        EntityDef entityDef2 = entityDefMap.get(end2Type);
        if (entityDef2.getPropertiesDefinition()!=null)
        {
            Set<String> attrset = entityAttributeMap.get(end2Type);
            for (TypeDefAttribute typeDefAttr : entityDef2.getPropertiesDefinition())
            {
                String localAttributeName = typeDefAttr.getAttributeName();
                // this attribute name should not already be defined for this entity

                if (localAttributeName.equals(end1Name))
                {
                    OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_ENDDEF1_NAME_IN_ARCHIVE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(
                            end2Type,
                            end1Name,
                            relationshipDef.getName()
                    );

                    throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                attrset.add(end1Name);
            }
        }
    }


    /**
     * Add a new entity to the archive.
     *
     * @param entity - instance to add
     */
    public void addEntity(EntityDetail   entity)
    {
        final String methodName = "addEntity()";

        if (entity != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding Entity: " + entity.toString());
            }

            EntityDetail   duplicateElement = entityDetailMap.put(entity.getGUID(), entity);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_INSTANCE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(TypeDefCategory.ENTITY_DEF.getTypeName(),
                                                                                entity.getGUID(),
                                                                                duplicateElement.toString(),
                                                                                entity.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(entity.getGUID(), entity);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(entity.getGUID(),
                                                                                duplicateGUID.toString(),
                                                                                entity.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            entityDetailList.add(entity);
        }
    }


    /**
     * Add a new relationship to the archive.
     *
     * @param relationship - instance to add
     */
    public void addRelationship(Relationship  relationship)
    {
        final String methodName = "addRelationship()";

        if (relationship != null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Adding Relationship: " + relationship.toString());
            }

            Relationship   duplicateElement = relationshipMap.put(relationship.getGUID(), relationship);

            if (duplicateElement != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_INSTANCE_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(TypeDefCategory.ENTITY_DEF.getTypeName(),
                                                                                relationship.getGUID(),
                                                                                duplicateElement.toString(),
                                                                                relationship.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            Object  duplicateGUID = guidMap.put(relationship.getGUID(), relationship);

            if (duplicateGUID != null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.DUPLICATE_GUID_IN_ARCHIVE;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationship.getGUID(),
                                                             duplicateGUID.toString(),
                                                             relationship.toString());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }

            relationshipList.add(relationship);
        }
    }


    /**
     * Once the content of the archive has been added to the archive builder, an archive object can be retrieved.
     *
     * @return open metadata archive object with all of the supplied content in it.
     */
    public OpenMetadataArchive  getOpenMetadataArchive()
    {
        final String methodName = "getOpenMetadataArchive()";

        if (log.isDebugEnabled())
        {
            log.debug("Retrieving Open Metadata Archive: " + archiveProperties.getArchiveName());
        }

        OpenMetadataArchive    archive = new OpenMetadataArchive();

        /*
         * Set up the archive properties
         */
        archive.setArchiveProperties(this.archiveProperties);

        /*
         * Set up the TypeStore.  The types are added in a strict order to ensure that the dependencies are resolved.
         */
        ArrayList<AttributeTypeDef>  attributeTypeDefs = new ArrayList<>();
        ArrayList<TypeDef>           typeDefs = new ArrayList<>();
        ArrayList<TypeDefPatch>      typeDefPatches = new ArrayList<>();

        if (! primitiveDefList.isEmpty())
        {
            attributeTypeDefs.addAll(primitiveDefList);
        }
        if (! collectionDefList.isEmpty())
        {
            attributeTypeDefs.addAll(collectionDefList);
        }
        if (! enumDefList.isEmpty())
        {
            attributeTypeDefs.addAll(enumDefList);
        }
        if (! entityDefList.isEmpty())
        {
            typeDefs.addAll(entityDefList);
        }
        if (! classificationDefList.isEmpty())
        {
            typeDefs.addAll(classificationDefList);
        }
        if (! relationshipDefList.isEmpty())
        {
            typeDefs.addAll(relationshipDefList);
        }

        if (! typeDefPatchList.isEmpty())
        {
            typeDefPatches.addAll(typeDefPatchList);
        }

        if ((! typeDefs.isEmpty()) || (! typeDefPatches.isEmpty()) || (! attributeTypeDefs.isEmpty()))
        {
            OpenMetadataArchiveTypeStore   typeStore = new OpenMetadataArchiveTypeStore();

            if (! attributeTypeDefs.isEmpty())
            {
                typeStore.setAttributeTypeDefs(attributeTypeDefs);
            }

            if (! typeDefs.isEmpty())
            {
                typeStore.setNewTypeDefs(typeDefs);
            }

            if (! typeDefPatches.isEmpty())
            {
                typeStore.setTypeDefPatches(typeDefPatches);
            }

            archive.setArchiveTypeStore(typeStore);
        }


        /*
         * Finally set up the instance store
         */
        ArrayList<EntityDetail>  entities      = new ArrayList<>();
        ArrayList<Relationship>  relationships = new ArrayList<>();

        if (! entityDetailList.isEmpty())
        {
            entities.addAll(entityDetailList);
        }
        if (! relationshipList.isEmpty())
        {
            relationships.addAll(relationshipList);
        }

        if ((! entities.isEmpty()) || (! relationships.isEmpty()))
        {
            OpenMetadataArchiveInstanceStore   instanceStore = new OpenMetadataArchiveInstanceStore();

            if (! entities.isEmpty())
            {
                instanceStore.setEntities(entities);
            }

            if (! relationships.isEmpty())
            {
                instanceStore.setRelationships(relationships);
            }

            archive.setArchiveInstanceStore(instanceStore);
        }

        return archive;
    }
    private void checkForBlanksInTypeName(String typeName)
    {
        final String methodName = "checkForBlanksInTypeName()";
        if (typeName.contains(" "))
        {
            OMRSErrorCode errorCode = OMRSErrorCode.BLANK_TYPENAME_IN_ARCHIVE;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }
}
