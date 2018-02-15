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
package org.apache.atlas.omrs.localrepository.repositoryconnector;

import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventProcessor;
import org.apache.atlas.omrs.ffdc.*;
import org.apache.atlas.omrs.ffdc.exception.*;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.MatchCriteria;
import org.apache.atlas.omrs.metadatacollection.properties.SequencingOrder;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.ArrayList;
import java.util.Date;

/**
 * LocalOMRSMetadataCollection provides a wrapper around the metadata collection for the real local repository.
 * Its role is to manage outbound repository events and audit logging/debug for the real local repository.
 */
public class LocalOMRSMetadataCollection extends OMRSMetadataCollection
{
    private static final String  sourceName = "Local Repository";

    private OMRSMetadataCollection       realMetadataCollection           = null;
    private String                       localServerName                  = null;
    private String                       localServerType                  = null;
    private String                       localOrganizationName            = null;
    private OMRSRepositoryEventProcessor outboundRepositoryEventProcessor = null;
    private OMRSRepositoryContentManager localRepositoryContentManager    = null;


    /**
     * Constructor used by LocalOMRSRepositoryConnector
     *
     * @param localMetadataConnectionId - unique identifier for the local metadata collection.
     * @param localServerName - name of the local server.
     * @param localServerType - type of the local server.
     * @param localOrganizationName - name of the organization that owns the local server.
     * @param realMetadataCollection - metadata collection of the rela local connector.
     * @param outboundRepositoryEventProcessor - outbound event processor
     *                                         (may be null if a repository event mapper is deployed).
     * @param repositoryContentManager - manager of in-memory cache of type definitions (TypeDefs).
     */
     LocalOMRSMetadataCollection(String                       localMetadataConnectionId,
                                 String                       localServerName,
                                 String                       localServerType,
                                 String                       localOrganizationName,
                                 OMRSMetadataCollection       realMetadataCollection,
                                 OMRSRepositoryEventProcessor outboundRepositoryEventProcessor,
                                 OMRSRepositoryContentManager repositoryContentManager)
    {
        /*
         * The super class manages the local metadata collection id.  This is a locally managed value.
         */
        super(localMetadataConnectionId);

        /*
         * Save the metadata collection object for the real repository.  This is the metadata that does all of the
         * work.  LocalOMRSMetadataCollection is just a wrapper for managing repository events and debug and
         * audit logging.
         */
        if (realMetadataCollection == null)
        {
            String            actionDescription = "Local OMRS Metadata Collection Constructor";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_LOCAL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              actionDescription,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
        this.realMetadataCollection = realMetadataCollection;

        /*
         * Save the information needed to send repository events.
         */
        this.localServerName = localServerName;
        this.localServerType = localServerType;
        this.localOrganizationName = localOrganizationName;
        this.outboundRepositoryEventProcessor = outboundRepositoryEventProcessor;
        this.localRepositoryContentManager = repositoryContentManager;
    }


    /* ==============================
     * Group 2: Working with typedefs
     */


    /**
     * Returns the list of different types of metadata organized into two groups.  The first are the
     * attribute type definitions (AttributeTypeDefs).  These provide types for properties in full
     * type definitions.  Full type definitions (TypeDefs) describe types for entities, relationships
     * and classifications.
     *
     * @param userId - unique identifier for requesting user.
     * @return TypeDefs - Lists of different categories of TypeDefs.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery getAllTypes(String userId) throws RepositoryErrorException,
                                                                UserNotAuthorizedException
    {
        return realMetadataCollection.getAllTypes(userId);
    }


    /**
     * Returns a list of TypeDefs that have the specified name.  TypeDef names should be unique.  This
     * method allows wildcard character to be included in the name.  These are * (asterisk) for an arbitrary string of
     * characters and ampersand for an arbitrary character.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - name of the TypeDefs to return (including wildcard characters).
     * @return TypeDefs list.
     * @throws InvalidParameterException - the name of the TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery findTypesByName(String      userId,
                                          String      name) throws InvalidParameterException,
                                                                   RepositoryErrorException,
                                                                   UserNotAuthorizedException
    {
        return realMetadataCollection.findTypesByName(userId, name);
    }


    /**
     * Returns all of the TypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param category - enum value for the category of TypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException - the TypeDefCategory is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public ArrayList<TypeDef> findTypeDefsByCategory(String          userId,
                                                     TypeDefCategory category) throws InvalidParameterException,
                                                                                      RepositoryErrorException,
                                                                                      UserNotAuthorizedException
    {
        return realMetadataCollection.findTypeDefsByCategory(userId, category);
    }


    /**
     * Returns all of the AttributeTypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param category - enum value for the category of an AttributeTypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException - the TypeDefCategory is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public ArrayList<AttributeTypeDef> findAttributeTypeDefsByCategory(String                   userId,
                                                                       AttributeTypeDefCategory category) throws InvalidParameterException,
                                                                                                                 RepositoryErrorException,
                                                                                                                 UserNotAuthorizedException
    {
        return realMetadataCollection.findAttributeTypeDefsByCategory(userId, category);
    }



    /**
     * Return the TypeDefs that have the properties matching the supplied match criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param matchCriteria - TypeDefProperties - a list of property names and values.
     * @return TypeDefs list.
     * @throws InvalidParameterException - the matchCriteria is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public ArrayList<TypeDef> findTypeDefsByProperty(String            userId,
                                                     TypeDefProperties matchCriteria) throws InvalidParameterException,
                                                                                             RepositoryErrorException,
                                                                                             UserNotAuthorizedException
    {
        return realMetadataCollection.findTypeDefsByProperty(userId, matchCriteria);
    }


    /**
     * Return the types that are linked to the elements from the specified standard.
     *
     * @param userId - unique identifier for requesting user.
     * @param standard - name of the standard - null means any.
     * @param organization - name of the organization - null means any.
     * @param identifier - identifier of the element in the standard - null means any.
     * @return TypeDefs list - each entry in the list contains a typedef.  This is is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException - all attributes of the external id are null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery findTypesByExternalID(String    userId,
                                                String    standard,
                                                String    organization,
                                                String    identifier) throws InvalidParameterException,
                                                                             RepositoryErrorException,
                                                                             UserNotAuthorizedException
    {
        return realMetadataCollection.findTypesByExternalID(userId, standard, organization, identifier);
    }

    /**
     * Return the TypeDefs that match the search criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param searchCriteria - String - search criteria.
     * @return TypeDefs list - each entry in the list contains a typedef.  This is is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException - the searchCriteria is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public ArrayList<TypeDef> searchForTypeDefs(String    userId,
                                                String    searchCriteria) throws InvalidParameterException,
                                                                                 RepositoryErrorException,
                                                                                 UserNotAuthorizedException
    {
        return realMetadataCollection.searchForTypeDefs(userId, searchCriteria);
    }


    /**
     * Return the TypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique id of the TypeDef
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException - The requested TypeDef is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef getTypeDefByGUID(String    userId,
                                    String    guid) throws InvalidParameterException,
                                                           RepositoryErrorException,
                                                           TypeDefNotKnownException,
                                                           UserNotAuthorizedException
    {
        return realMetadataCollection.getTypeDefByGUID(userId, guid);
    }


    /**
     * Return the AttributeTypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique id of the TypeDef
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException - The requested TypeDef is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  AttributeTypeDef getAttributeTypeDefByGUID(String    userId,
                                                       String    guid) throws InvalidParameterException,
                                                                              RepositoryErrorException,
                                                                              TypeDefNotKnownException,
                                                                              UserNotAuthorizedException
    {
        return realMetadataCollection.getAttributeTypeDefByGUID(userId, guid);
    }


    /**
     * Return the TypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException - the name is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef getTypeDefByName(String    userId,
                                    String    name) throws InvalidParameterException,
                                                           RepositoryErrorException,
                                                           TypeDefNotKnownException,
                                                           UserNotAuthorizedException
    {
        return realMetadataCollection.getTypeDefByName(userId, name);
    }


    /**
     * Return the AttributeTypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException - the name is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  AttributeTypeDef getAttributeTypeDefByName(String    userId,
                                                       String    name) throws InvalidParameterException,
                                                                              RepositoryErrorException,
                                                                              TypeDefNotKnownException,
                                                                              UserNotAuthorizedException
    {
        return realMetadataCollection.getAttributeTypeDefByName(userId, name);
    }


    /**
     * Create a collection of related types.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypes - TypeDefGallery structure describing the new AttributeTypeDefs and TypeDefs.
     * @throws InvalidParameterException - the new TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  void addTypeDefGallery(String          userId,
                                   TypeDefGallery  newTypes) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    TypeDefNotSupportedException,
                                                                    TypeDefKnownException,
                                                                    TypeDefConflictException,
                                                                    InvalidTypeDefException,
                                                                    UserNotAuthorizedException
    {

    }


    /**
     * Create a definition of a new TypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException - the new TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void addTypeDef(String    userId,
                           TypeDef   newTypeDef) throws InvalidParameterException,
                                                        RepositoryErrorException,
                                                        TypeDefNotSupportedException,
                                                        TypeDefKnownException,
                                                        TypeDefConflictException,
                                                        InvalidTypeDefException,
                                                        UserNotAuthorizedException
    {
        realMetadataCollection.addTypeDef(userId, newTypeDef);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.addTypeDef(sourceName, newTypeDef);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processNewTypeDefEvent(sourceName,
                                                                    metadataCollectionId,
                                                                    localServerName,
                                                                    localServerType,
                                                                    localOrganizationName,
                                                                    newTypeDef);
        }
    }

    /**
     * Create a definition of a new AttributeTypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param newAttributeTypeDef - TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException - the new TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  void addAttributeTypeDef(String             userId,
                                     AttributeTypeDef   newAttributeTypeDef) throws InvalidParameterException,
                                                                                    RepositoryErrorException,
                                                                                    TypeDefNotSupportedException,
                                                                                    TypeDefKnownException,
                                                                                    TypeDefConflictException,
                                                                                    InvalidTypeDefException,
                                                                                    UserNotAuthorizedException
    {
        realMetadataCollection.addAttributeTypeDef(userId, newAttributeTypeDef);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.addAttributeTypeDef(sourceName, newAttributeTypeDef);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processNewAttributeTypeDefEvent(sourceName,
                                                                             metadataCollectionId,
                                                                             localServerName,
                                                                             localServerType,
                                                                             localOrganizationName,
                                                                             newAttributeTypeDef);
        }
    }


    /**
     * Verify that a definition of a TypeDef is either new - or matches the definition already stored.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDef - TypeDef structure describing the TypeDef to test.
     * @return boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known.
     * @throws InvalidParameterException - the TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public boolean verifyTypeDef(String    userId,
                                 TypeDef   typeDef) throws InvalidParameterException,
                                                           RepositoryErrorException,
                                                           TypeDefNotSupportedException,
                                                           TypeDefConflictException,
                                                           InvalidTypeDefException,
                                                           UserNotAuthorizedException
    {
        return realMetadataCollection.verifyTypeDef(userId, typeDef);
    }


    /**
     * Verify that a definition of an AttributeTypeDef is either new - or matches the definition already stored.
     *
     * @param userId - unique identifier for requesting user.
     * @param attributeTypeDef - TypeDef structure describing the TypeDef to test.
     * @return boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known.
     * @throws InvalidParameterException - the TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  boolean verifyAttributeTypeDef(String            userId,
                                           AttributeTypeDef  attributeTypeDef) throws InvalidParameterException,
                                                                                      RepositoryErrorException,
                                                                                      TypeDefNotSupportedException,
                                                                                      TypeDefConflictException,
                                                                                      InvalidTypeDefException,
                                                                                      UserNotAuthorizedException
    {
        return realMetadataCollection.verifyAttributeTypeDef(userId, attributeTypeDef);
    }


    /**
     * Update one or more properties of the TypeDef.  The TypeDefPatch controls what types of updates
     * are safe to make to the TypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefPatch - TypeDef patch describing change to TypeDef.
     * @return updated TypeDef
     * @throws InvalidParameterException - the TypeDefPatch is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection.
     * @throws PatchErrorException - the TypeDef can not be updated because the supplied patch is incompatible
     *                               with the stored TypeDef.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef updateTypeDef(String       userId,
                                 TypeDefPatch typeDefPatch) throws InvalidParameterException,
                                                                   RepositoryErrorException,
                                                                   TypeDefNotKnownException,
                                                                   PatchErrorException,
                                                                   UserNotAuthorizedException
    {
        TypeDef   updatedTypeDef = realMetadataCollection.updateTypeDef(userId, typeDefPatch);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.updateTypeDef(sourceName, updatedTypeDef);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processUpdatedTypeDefEvent(sourceName,
                                                                        metadataCollectionId,
                                                                        localServerName,
                                                                        localServerType,
                                                                        localOrganizationName,
                                                                        typeDefPatch);
        }

        return updatedTypeDef;
    }


    /**
     * Delete the TypeDef.  This is only possible if the TypeDef has never been used to create instances or any
     * instances of this TypeDef have been purged from the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     * @throws InvalidParameterException - the one of TypeDef identifiers is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection.
     * @throws TypeDefInUseException - the TypeDef can not be deleted because there are instances of this type in the
     *                                 the metadata collection.  These instances need to be purged before the
     *                                 TypeDef can be deleted.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteTypeDef(String    userId,
                              String    obsoleteTypeDefGUID,
                              String    obsoleteTypeDefName) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    TypeDefNotKnownException,
                                                                    TypeDefInUseException,
                                                                    UserNotAuthorizedException
    {
        if ((obsoleteTypeDefGUID == null) || (obsoleteTypeDefName == null))
        {
            // TODO Throw InvalidParameterException
        }

        realMetadataCollection.deleteTypeDef(userId,
                                             obsoleteTypeDefGUID,
                                             obsoleteTypeDefName);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.deleteTypeDef(sourceName,
                                                        obsoleteTypeDefGUID,
                                                        obsoleteTypeDefName);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processDeletedTypeDefEvent(sourceName,
                                                                        metadataCollectionId,
                                                                        localServerName,
                                                                        localServerType,
                                                                        localOrganizationName,
                                                                        obsoleteTypeDefGUID,
                                                                        obsoleteTypeDefName);
        }
    }


    /**
     * Delete an AttributeTypeDef.  This is only possible if the AttributeTypeDef has never been used to create
     * instances or any instances of this AttributeTypeDef have been purged from the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     * @throws InvalidParameterException - the one of AttributeTypeDef identifiers is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws TypeDefInUseException - the AttributeTypeDef can not be deleted because there are instances of this type in the
     *                                 the metadata collection.  These instances need to be purged before the
     *                                 AttributeTypeDef can be deleted.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteAttributeTypeDef(String    userId,
                                       String    obsoleteTypeDefGUID,
                                       String    obsoleteTypeDefName) throws InvalidParameterException,
                                                                             RepositoryErrorException,
                                                                             TypeDefNotKnownException,
                                                                             TypeDefInUseException,
                                                                             UserNotAuthorizedException
    {
        if ((obsoleteTypeDefGUID == null) || (obsoleteTypeDefName == null))
        {
            // TODO Throw InvalidParameterException
        }

        realMetadataCollection.deleteAttributeTypeDef(userId,
                                                      obsoleteTypeDefGUID,
                                                      obsoleteTypeDefName);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.deleteAttributeTypeDef(sourceName,
                                                                 obsoleteTypeDefGUID,
                                                                 obsoleteTypeDefName);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processDeletedAttributeTypeDefEvent(sourceName,
                                                                                 metadataCollectionId,
                                                                                 localServerName,
                                                                                 localServerType,
                                                                                 localOrganizationName,
                                                                                 obsoleteTypeDefGUID,
                                                                                 obsoleteTypeDefName);
        }
    }


    /**
     * Change the guid or name of an existing TypeDef to a new value.  This is used if two different
     * TypeDefs are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId - unique identifier for requesting user.
     * @param originalTypeDefGUID - the original guid of the TypeDef.
     * @param originalTypeDefName - the original name of the TypeDef.
     * @param newTypeDefGUID - the new identifier for the TypeDef.
     * @param newTypeDefName - new name for this TypeDef.
     * @return typeDef - new values for this TypeDef, including the new guid/name.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException - the TypeDef identified by the original guid/name is not found
     *                                    in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  TypeDef reIdentifyTypeDef(String     userId,
                                      String     originalTypeDefGUID,
                                      String     originalTypeDefName,
                                      String     newTypeDefGUID,
                                      String     newTypeDefName) throws InvalidParameterException,
                                                                        RepositoryErrorException,
                                                                        TypeDefNotKnownException,
                                                                        UserNotAuthorizedException
    {
        if ((originalTypeDefGUID == null) || (originalTypeDefName == null) ||
                 (newTypeDefGUID == null) || (newTypeDefName == null))
        {
            // TODO Throw InvalidParameterException
        }

        TypeDef   originalTypeDef = realMetadataCollection.getTypeDefByGUID(userId, originalTypeDefGUID);

        TypeDef   newTypeDef = realMetadataCollection.reIdentifyTypeDef(userId,
                                                                        originalTypeDefGUID,
                                                                        originalTypeDefName,
                                                                        newTypeDefGUID,
                                                                        newTypeDefName);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.reIdentifyTypeDef(sourceName,
                                                            originalTypeDefGUID,
                                                            originalTypeDefName,
                                                            newTypeDef);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processReIdentifiedTypeDefEvent(sourceName,
                                                                             metadataCollectionId,
                                                                             localServerName,
                                                                             localServerType,
                                                                             localOrganizationName,
                                                                             originalTypeDef,
                                                                             newTypeDef);
        }

        return newTypeDef;
    }


    /**
     * Change the guid or name of an existing TypeDef to a new value.  This is used if two different
     * TypeDefs are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId - unique identifier for requesting user.
     * @param originalAttributeTypeDefGUID - the original guid of the AttributeTypeDef.
     * @param originalAttributeTypeDefName - the original name of the AttributeTypeDef.
     * @param newAttributeTypeDefGUID - the new identifier for the AttributeTypeDef.
     * @param newAttributeTypeDefName - new name for this AttributeTypeDef.
     * @return attributeTypeDef - new values for this AttributeTypeDef, including the new guid/name.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException - the AttributeTypeDef identified by the original guid/name is not
     *                                    found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  AttributeTypeDef reIdentifyAttributeTypeDef(String     userId,
                                                        String     originalAttributeTypeDefGUID,
                                                        String     originalAttributeTypeDefName,
                                                        String     newAttributeTypeDefGUID,
                                                        String     newAttributeTypeDefName) throws InvalidParameterException,
                                                                                                   RepositoryErrorException,
                                                                                                   TypeDefNotKnownException,
                                                                                                   UserNotAuthorizedException
    {
        if ((originalAttributeTypeDefGUID == null) || (originalAttributeTypeDefName == null) ||
                 (newAttributeTypeDefGUID == null) || (newAttributeTypeDefName == null))
        {
            // TODO Throw InvalidParameterException
        }

        AttributeTypeDef   originalAttributeTypeDef = realMetadataCollection.getAttributeTypeDefByGUID(userId, originalAttributeTypeDefGUID);

        AttributeTypeDef   newAttributeTypeDef = realMetadataCollection.reIdentifyAttributeTypeDef(userId,
                                                                                                   originalAttributeTypeDefGUID,
                                                                                                   originalAttributeTypeDefName,
                                                                                                   newAttributeTypeDefGUID,
                                                                                                   newAttributeTypeDefName);

        if (localRepositoryContentManager != null)
        {
            localRepositoryContentManager.reIdentifyAttributeTypeDef(sourceName,
                                                                     originalAttributeTypeDefGUID,
                                                                     originalAttributeTypeDefName,
                                                                     newAttributeTypeDef);
        }

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processReIdentifiedAttributeTypeDefEvent(sourceName,
                                                                                      metadataCollectionId,
                                                                                      localServerName,
                                                                                      localServerType,
                                                                                      localOrganizationName,
                                                                                      originalAttributeTypeDef,
                                                                                      newAttributeTypeDef);
        }

        return newAttributeTypeDef;
    }



    /* ===================================================
     * Group 3: Locating entity and relationship instances
     */


    /**
     * Returns a boolean indicating if the entity is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity
     * @return entity details if the entity is found in the metadata collection; otherwise return null
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail  isEntityKnown(String    userId,
                                       String    guid) throws InvalidParameterException,
                                                              RepositoryErrorException,
                                                              UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.isEntityKnown(userId, guid);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }
        }

        return entity;
    }


    /**
     * Return the header and classifications for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity
     * @return EntitySummary structure
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntitySummary getEntitySummary(String    userId,
                                          String    guid) throws InvalidParameterException,
                                                                 RepositoryErrorException,
                                                                 EntityNotKnownException,
                                                                 UserNotAuthorizedException
    {
        EntitySummary  entity =  realMetadataCollection.getEntitySummary(userId, guid);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }
        }

        return entity;
    }


    /**
     * Return the header, classifications and properties of a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @return EntityDetail structure.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws EntityNotKnownException - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail getEntityDetail(String    userId,
                                        String    guid) throws InvalidParameterException,
                                                               RepositoryErrorException,
                                                               EntityNotKnownException,
                                                               UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.getEntityDetail(userId, guid);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }
        }

        return entity;
    }


    /**
     * Return a historical version of an entity - includes the header, classifications and properties of the entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @param asOfTime - the time used to determine which version of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException - the guid or date is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws EntityNotKnownException - the requested entity instance is not known in the metadata collection
     *                                   at the time requested.
     * @throws PropertyErrorException - the asOfTime property is for a future time
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  EntityDetail getEntityDetail(String    userId,
                                         String    guid,
                                         Date      asOfTime) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    EntityNotKnownException,
                                                                    PropertyErrorException,
                                                                    UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.getEntityDetail(userId, guid, asOfTime);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }
        }

        return entity;
    }


    /**
     * Return the header, classifications, properties and relationships for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @return EntityUniverse structure.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityUniverse getEntityUniverse(String    userId,
                                            String    guid) throws InvalidParameterException,
                                                                   RepositoryErrorException,
                                                                   EntityNotKnownException,
                                                                   UserNotAuthorizedException
    {
        EntityUniverse entity = realMetadataCollection.getEntityUniverse(userId, guid);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }
        }

        return entity;
    }


    /**
     * Return the relationships for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier for the entity.
     * @param fromRelationshipElement - the starting element number of the relationships to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize -- the maximum number of result classifications that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return Relationships list.  Null means no relationships associated with the entity.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the requested entity instance is not known in the metadata collection.
     * @throws PropertyErrorException - the sequencing property is not valid for the attached classifications.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public ArrayList<Relationship> getRelationshipsForEntity(String                     userId,
                                                             String                     entityGUID,
                                                             int                        fromRelationshipElement,
                                                             ArrayList<InstanceStatus>  limitResultsByStatus,
                                                             Date                       asOfTime,
                                                             String                     sequencingProperty,
                                                             SequencingOrder            sequencingOrder,
                                                             int                        pageSize) throws InvalidParameterException,
                                                                                                         RepositoryErrorException,
                                                                                                         EntityNotKnownException,
                                                                                                         PropertyErrorException,
                                                                                                         PagingErrorException,
                                                                                                         UserNotAuthorizedException
    {
        return realMetadataCollection.getRelationshipsForEntity(userId,
                                                                entityGUID,
                                                                fromRelationshipElement,
                                                                limitResultsByStatus,
                                                                asOfTime,
                                                                sequencingProperty,
                                                                sequencingOrder,
                                                                pageSize);
    }


    /**
     * Return a list of entities that match the supplied properties according to the match criteria.  The results
     * can be returned over many pages.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - String unique identifier for the entity type of interest (null means any entity type).
     * @param matchProperties - List of entity properties to match to (null means match on entityTypeGUID only).
     * @param matchCriteria - Enum defining how the properties should be matched to the entities in the repository.
     * @param fromEntityDetailElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty - String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws PropertyErrorException - the properties specified are not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  ArrayList<EntityDetail> findEntitiesByProperty(String                    userId,
                                                           String                    entityTypeGUID,
                                                           InstanceProperties        matchProperties,
                                                           MatchCriteria             matchCriteria,
                                                           int                       fromEntityDetailElement,
                                                           ArrayList<InstanceStatus> limitResultsByStatus,
                                                           ArrayList<String>         limitResultsByClassification,
                                                           Date                      asOfTime,
                                                           String                    sequencingProperty,
                                                           SequencingOrder           sequencingOrder,
                                                           int                       pageSize) throws InvalidParameterException,
                                                                                                      RepositoryErrorException,
                                                                                                      TypeErrorException,
                                                                                                      PropertyErrorException,
                                                                                                      PagingErrorException,
                                                                                                      UserNotAuthorizedException
    {
        return realMetadataCollection.findEntitiesByProperty(userId,
                                                             entityTypeGUID,
                                                             matchProperties,
                                                             matchCriteria,
                                                             fromEntityDetailElement,
                                                             limitResultsByStatus,
                                                             limitResultsByClassification,
                                                             asOfTime,
                                                             sequencingProperty,
                                                             sequencingOrder,
                                                             pageSize);
    }


    /**
     * Return a list of entities that have the requested type of classification attached.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - unique identifier for the type of entity requested.  Null mans any type of entity.
     * @param classificationName - name of the classification - a null is not valid.
     * @param matchClassificationProperties - list of classification properties used to narrow the search.
     * @param matchCriteria - Enum defining how the properties should be matched to the classifications in the repository.
     * @param fromEntityDetailElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty - String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws ClassificationErrorException - the classification request is not known to the metadata collection.
     * @throws PropertyErrorException - the properties specified are not valid for the requested type of
     *                                  classification.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  ArrayList<EntityDetail> findEntitiesByClassification(String                    userId,
                                                                 String                    entityTypeGUID,
                                                                 String                    classificationName,
                                                                 InstanceProperties        matchClassificationProperties,
                                                                 MatchCriteria             matchCriteria,
                                                                 ArrayList<InstanceStatus> limitResultsByStatus,
                                                                 Date                      asOfTime,
                                                                 String                    sequencingProperty,
                                                                 SequencingOrder           sequencingOrder,
                                                                 int                       fromEntityDetailElement,
                                                                 int                       pageSize) throws InvalidParameterException,
                                                                                                            RepositoryErrorException,
                                                                                                            TypeErrorException,
                                                                                                            ClassificationErrorException,
                                                                                                            PropertyErrorException,
                                                                                                            PagingErrorException,
                                                                                                            UserNotAuthorizedException
    {
        return realMetadataCollection.findEntitiesByClassification(userId,
                                                                   entityTypeGUID,
                                                                   classificationName,
                                                                   matchClassificationProperties,
                                                                   matchCriteria,
                                                                   limitResultsByStatus,
                                                                   asOfTime,
                                                                   sequencingProperty,
                                                                   sequencingOrder,
                                                                   fromEntityDetailElement,
                                                                   pageSize);
    }


    /**
     * Return a list of entities matching the search criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param searchCriteria - String expression of the characteristics of the required relationships.
     * @param fromEntityDetailElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  ArrayList<EntityDetail> searchForEntities(String                          userId,
                                                      String                          searchCriteria,
                                                      int                             fromEntityDetailElement,
                                                      ArrayList<InstanceStatus>       limitResultsByStatus,
                                                      ArrayList<String>               limitResultsByClassification,
                                                      Date                            asOfTime,
                                                      String                          sequencingProperty,
                                                      SequencingOrder                 sequencingOrder,
                                                      int                             pageSize) throws InvalidParameterException,
                                                                                                       RepositoryErrorException,
                                                                                                       PropertyErrorException,
                                                                                                       PagingErrorException,
                                                                                                       UserNotAuthorizedException
    {
        return realMetadataCollection.searchForEntities(userId,
                                                        searchCriteria,
                                                        fromEntityDetailElement,
                                                        limitResultsByStatus,
                                                        limitResultsByClassification,
                                                        asOfTime,
                                                        sequencingProperty,
                                                        sequencingOrder,
                                                        pageSize);
    }


    /**
     * Returns a boolean indicating if the relationship is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @return relationship details if the relationship is found in the metadata collection; otherwise return null.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship  isRelationshipKnown(String    userId,
                                             String    guid) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    UserNotAuthorizedException
    {
        return realMetadataCollection.isRelationshipKnown(userId, guid);
    }


    /**
     * Return a requested relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @return a relationship structure.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the metadata collection does not have a relationship with
     *                                         the requested GUID stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship getRelationship(String    userId,
                                        String    guid) throws InvalidParameterException,
                                                               RepositoryErrorException,
                                                               RelationshipNotKnownException,
                                                               UserNotAuthorizedException
    {
        return realMetadataCollection.getRelationship(userId, guid);
    }


    /**
     * Return a historical version of a relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @param asOfTime - the time used to determine which version of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException - the guid or date is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested entity instance is not known in the metadata collection
     *                                   at the time requested.
     * @throws PropertyErrorException - the asOfTime property is for a future time.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  Relationship getRelationship(String    userId,
                                         String    guid,
                                         Date      asOfTime) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    RelationshipNotKnownException,
                                                                    PropertyErrorException,
                                                                    UserNotAuthorizedException
    {
        return realMetadataCollection.getRelationship(userId, guid, asOfTime);
    }


    /**
     * Return a list of relationships that match the requested properties by hte matching criteria.   The results
     * can be broken into pages.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipTypeGUID - unique identifier (guid) for the new relationship's type.
     * @param matchProperties - list of  properties used to narrow the search.
     * @param matchCriteria - Enum defining how the properties should be matched to the relationships in the repository.
     * @param fromEntityDetailElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result relationships that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of relationships.  Null means no matching relationships.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws PropertyErrorException - the properties specified are not valid for any of the requested types of
     *                                  relationships.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  ArrayList<Relationship> findRelationshipsByProperty(String                    userId,
                                                                String                    relationshipTypeGUID,
                                                                InstanceProperties        matchProperties,
                                                                MatchCriteria             matchCriteria,
                                                                int                       fromEntityDetailElement,
                                                                ArrayList<InstanceStatus> limitResultsByStatus,
                                                                Date                      asOfTime,
                                                                String                    sequencingProperty,
                                                                SequencingOrder           sequencingOrder,
                                                                int                       pageSize) throws InvalidParameterException,
                                                                                                           RepositoryErrorException,
                                                                                                           TypeErrorException,
                                                                                                           PropertyErrorException,
                                                                                                           PagingErrorException,
                                                                                                           UserNotAuthorizedException
    {
        return realMetadataCollection.findRelationshipsByProperty(userId,
                                                                  relationshipTypeGUID,
                                                                  matchProperties,
                                                                  matchCriteria,
                                                                  fromEntityDetailElement,
                                                                  limitResultsByStatus,
                                                                  asOfTime,
                                                                  sequencingProperty,
                                                                  sequencingOrder,
                                                                  pageSize);
    }


    /**
     * Return a list of relationships that match the search criteria.  The results can be paged.
     *
     * @param userId - unique identifier for requesting user.
     * @param searchCriteria - String expression of the characteristics of the required relationships.
     * @param fromRelationshipElement - Element number of the results to skip to when building the results list
     *                                to return.  Zero means begin at the start of the results.  This is used
     *                                to retrieve the results over a number of pages.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result relationships that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of relationships.  Null means no matching relationships.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws PropertyErrorException - there is a problem with one of the other parameters.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  ArrayList<Relationship> searchForRelationships(String                    userId,
                                                           String                    searchCriteria,
                                                           int                       fromRelationshipElement,
                                                           ArrayList<InstanceStatus> limitResultsByStatus,
                                                           Date                      asOfTime,
                                                           String                    sequencingProperty,
                                                           SequencingOrder           sequencingOrder,
                                                           int                       pageSize) throws InvalidParameterException,
                                                                                                      RepositoryErrorException,
                                                                                                      PropertyErrorException,
                                                                                                      PagingErrorException,
                                                                                                      UserNotAuthorizedException
    {
        return realMetadataCollection.searchForRelationships(userId,
                                                             searchCriteria,
                                                             fromRelationshipElement,
                                                             limitResultsByStatus,
                                                             asOfTime,
                                                             sequencingProperty,
                                                             sequencingOrder,
                                                             pageSize);
    }


    /**
     * Return all of the relationships and intermediate entities that connect the startEntity with the endEntity.
     *
     * @param userId - unique identifier for requesting user.
     * @param startEntityGUID - The entity that is used to anchor the query.
     * @param endEntityGUID - the other entity that defines the scope of the query.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @return InstanceGraph - the sub-graph that represents the returned linked entities and their relationships.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by either the startEntityGUID or the endEntityGUID
     *                                   is not found in the metadata collection.
     * @throws PropertyErrorException - there is a problem with one of the other parameters.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  InstanceGraph getLinkingEntities(String                    userId,
                                             String                    startEntityGUID,
                                             String                    endEntityGUID,
                                             ArrayList<InstanceStatus> limitResultsByStatus,
                                             Date                      asOfTime) throws InvalidParameterException,
                                                                                        RepositoryErrorException,
                                                                                        EntityNotKnownException,
                                                                                        PropertyErrorException,
                                                                                        UserNotAuthorizedException
    {
        return realMetadataCollection.getLinkingEntities(userId,
                                                         startEntityGUID,
                                                         endEntityGUID,
                                                         limitResultsByStatus,
                                                         asOfTime);
    }


    /**
     * Return the entities and relationships that radiate out from the supplied entity GUID.
     * The results are scoped both the instance type guids and the level.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - the starting point of the query.
     * @param entityTypeGUIDs - list of entity types to include in the query results.  Null means include
     *                          all entities found, irrespective of their type.
     * @param relationshipTypeGUIDs - list of relationship types to include in the query results.  Null means include
     *                                all relationships found, irrespective of their type.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param level - the number of the relationships out from the starting entity that the query will traverse to
     *              gather results.
     * @return InstanceGraph - the sub-graph that represents the returned linked entities and their relationships.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeErrorException - one or more of the type guids passed on the request is not known by the
     *                              metadata collection.
     * @throws EntityNotKnownException - the entity identified by the entityGUID is not found in the metadata collection.
     * @throws PropertyErrorException - there is a problem with one of the other parameters.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  InstanceGraph getEntityNeighborhood(String                    userId,
                                                String                    entityGUID,
                                                ArrayList<String>         entityTypeGUIDs,
                                                ArrayList<String>         relationshipTypeGUIDs,
                                                ArrayList<InstanceStatus> limitResultsByStatus,
                                                ArrayList<String>         limitResultsByClassification,
                                                Date                      asOfTime,
                                                int                       level) throws InvalidParameterException,
                                                                                        RepositoryErrorException,
                                                                                        TypeErrorException,
                                                                                        EntityNotKnownException,
                                                                                        PropertyErrorException,
                                                                                        UserNotAuthorizedException
    {
        return realMetadataCollection.getEntityNeighborhood(userId,
                                                            entityGUID,
                                                            entityTypeGUIDs,
                                                            relationshipTypeGUIDs,
                                                            limitResultsByStatus,
                                                            limitResultsByClassification,
                                                            asOfTime,
                                                            level);
    }


    /**
     * Return the list of entities that are of the types listed in instanceTypes and are connected, either directly or
     * indirectly to the entity identified by startEntityGUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param startEntityGUID - unique identifier of the starting entity
     * @param instanceTypes - list of types to search for.  Null means an type.
     * @param fromEntityElement - starting element for results list.  Used in paging.  Zero means first element.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return list of entities either directly or indirectly connected to the start entity
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                              hosting the metadata collection.
     * @throws EntityNotKnownException - the entity identified by the startEntityGUID
     *                                   is not found in the metadata collection.
     * @throws PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  ArrayList<EntityDetail> getRelatedEntities(String                    userId,
                                                       String                    startEntityGUID,
                                                       ArrayList<String>         instanceTypes,
                                                       int                       fromEntityElement,
                                                       ArrayList<InstanceStatus> limitResultsByStatus,
                                                       ArrayList<String>         limitResultsByClassification,
                                                       Date                      asOfTime,
                                                       String                    sequencingProperty,
                                                       SequencingOrder           sequencingOrder,
                                                       int                       pageSize) throws InvalidParameterException,
                                                                                                  RepositoryErrorException,
                                                                                                  TypeErrorException,
                                                                                                  EntityNotKnownException,
                                                                                                  PropertyErrorException,
                                                                                                  PagingErrorException,
                                                                                                  UserNotAuthorizedException
    {
        return realMetadataCollection.getRelatedEntities(userId,
                                                         startEntityGUID,
                                                         instanceTypes,
                                                         fromEntityElement,
                                                         limitResultsByStatus,
                                                         limitResultsByClassification,
                                                         asOfTime,
                                                         sequencingProperty,
                                                         sequencingOrder,
                                                         pageSize);
    }


    /* ======================================================
     * Group 4: Maintaining entity and relationship instances
     */

    /**
     * Create a new entity and put it in the requested state.  The new entity is returned.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - unique identifier (guid) for the new entity's type.
     * @param initialProperties - initial list of properties for the new entity - null means no properties.
     * @param initialClassifications - initial list of classifications for the new entity - null means no classifications.
     * @param initialStatus - initial status - typically DRAFT, PREPARED or ACTIVE.
     * @return EntityDetail showing the new header plus the requested properties and classifications.  The entity will
     * not have any relationships at this stage.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                              hosting the metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this entity's type.
     * @throws ClassificationErrorException - one or more of the requested classifications are either not known or
     *                                           not defined for this entity type.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                       the requested status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail addEntity(String                     userId,
                                  String                     entityTypeGUID,
                                  InstanceProperties         initialProperties,
                                  ArrayList<Classification>  initialClassifications,
                                  InstanceStatus             initialStatus) throws InvalidParameterException,
                                                                                   RepositoryErrorException,
                                                                                   TypeErrorException,
                                                                                   PropertyErrorException,
                                                                                   ClassificationErrorException,
                                                                                   StatusNotSupportedException,
                                                                                   UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.addEntity(userId,
                                                                 entityTypeGUID,
                                                                 initialProperties,
                                                                 initialClassifications,
                                                                 initialStatus);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processNewEntityEvent(sourceName,
                                                                       metadataCollectionId,
                                                                       localServerName,
                                                                       localServerType,
                                                                       localOrganizationName,
                                                                       entity);
            }
        }

        return entity;
    }


    /**
     * Create an entity proxy in the metadata collection.  This is used to store relationships that span metadata
     * repositories.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityProxy - details of entity to add.
     * @throws InvalidParameterException - the entity proxy is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this entity's type.
     * @throws ClassificationErrorException - one or more of the requested classifications are either not known or
     *                                         not defined for this entity type.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void addEntityProxy(String       userId,
                               EntityProxy  entityProxy) throws InvalidParameterException,
                                                                RepositoryErrorException,
                                                                TypeErrorException,
                                                                PropertyErrorException,
                                                                ClassificationErrorException,
                                                                StatusNotSupportedException,
                                                                UserNotAuthorizedException
    {
        /*
         * EntityProxies are used to store a relationship where the entity at one end of the relationship is
         * not stored locally.
         */
        realMetadataCollection.addEntityProxy(userId, entityProxy);
    }


    /**
     * Update the status for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - unique identifier (guid) for the requested entity.
     * @param newStatus - new InstanceStatus for the entity.
     * @return EntityDetail showing the current entity header, properties and classifications.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                      the requested status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityStatus(String           userId,
                                           String           entityGUID,
                                           InstanceStatus   newStatus) throws InvalidParameterException,
                                                                              RepositoryErrorException,
                                                                              EntityNotKnownException,
                                                                              StatusNotSupportedException,
                                                                              UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.updateEntityStatus(userId, entityGUID, newStatus);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processUpdatedEntityEvent(sourceName,
                                                                           metadataCollectionId,
                                                                           localServerName,
                                                                           localServerType,
                                                                           localOrganizationName,
                                                                           entity);
            }
        }

        return entity;
    }


    /**
     * Update selected properties in an entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param properties - a list of properties to change.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this entity's type
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityProperties(String               userId,
                                               String               entityGUID,
                                               InstanceProperties   properties) throws InvalidParameterException,
                                                                                       RepositoryErrorException,
                                                                                       EntityNotKnownException,
                                                                                       PropertyErrorException,
                                                                                       UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.updateEntityProperties(userId, entityGUID, properties);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processUpdatedEntityEvent(sourceName,
                                                                           metadataCollectionId,
                                                                           localServerName,
                                                                           localServerType,
                                                                           localOrganizationName,
                                                                           entity);
            }
        }

        return entity;
    }


    /**
     * Undo the last update to an entity and return the previous content.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail undoEntityUpdate(String    userId,
                                         String    entityGUID) throws InvalidParameterException,
                                                                      RepositoryErrorException,
                                                                      EntityNotKnownException,
                                                                      UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.undoEntityUpdate(userId, entityGUID);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processUndoneEntityEvent(sourceName,
                                                                           metadataCollectionId,
                                                                           localServerName,
                                                                           localServerType,
                                                                           localOrganizationName,
                                                                           entity);
            }
        }

        return entity;
    }


    /**
     * Delete an entity.  The entity is soft deleted.  This means it is still in the graph but it is no longer returned
     * on queries.  All relationships to the entity are also soft-deleted and will no longer be usable.
     * To completely eliminate the entity from the graph requires a call to the purgeEntity() method after the delete call.
     * The restoreEntity() method will switch an entity back to Active status to restore the entity to normal use.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - unique identifier of the type of the entity to delete.
     * @param typeDefName - unique name of the type of the entity to delete.
     * @param obsoleteEntityGUID - String unique identifier (guid) for the entity
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                       soft-deletes - use purgeEntity() to remove the entity permanently.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void   deleteEntity(String    userId,
                               String    typeDefGUID,
                               String    typeDefName,
                               String    obsoleteEntityGUID) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    EntityNotKnownException,
                                                                    StatusNotSupportedException,
                                                                    UserNotAuthorizedException
    {
        realMetadataCollection.deleteEntity(userId,
                                            typeDefGUID,
                                            typeDefName,
                                            obsoleteEntityGUID);

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processDeletedEntityEvent(sourceName,
                                                                       metadataCollectionId,
                                                                       localServerName,
                                                                       localServerType,
                                                                       localOrganizationName,
                                                                       typeDefGUID,
                                                                       typeDefName,
                                                                       obsoleteEntityGUID);
        }

    }


    /**
     * Permanently removes a deleted entity from the metadata collection.  This request can not be undone.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - unique identifier of the type of the entity to purge.
     * @param typeDefName - unique name of the type of the entity to purge.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * @throws EntityNotDeletedException - the entity is not in DELETED status and so can not be purged
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeEntity(String    userId,
                            String    typeDefGUID,
                            String    typeDefName,
                            String    deletedEntityGUID) throws InvalidParameterException,
                                                                RepositoryErrorException,
                                                                EntityNotKnownException,
                                                                EntityNotDeletedException,
                                                                UserNotAuthorizedException
    {
        realMetadataCollection.purgeEntity(userId,
                                           typeDefGUID,
                                           typeDefName,
                                           deletedEntityGUID);

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processPurgedEntityEvent(sourceName,
                                                                      metadataCollectionId,
                                                                      localServerName,
                                                                      localServerType,
                                                                      localOrganizationName,
                                                                      typeDefGUID,
                                                                      typeDefName,
                                                                      deletedEntityGUID);
        }
    }


    /**
     * Restore the requested entity to the state it was before it was deleted.
     *
     * @param userId - unique identifier for requesting user.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetail showing the restored entity header, properties and classifications.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     * the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * @throws EntityNotDeletedException - the entity is currently not in DELETED status and so it can not be restored
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail restoreEntity(String    userId,
                                      String    deletedEntityGUID) throws InvalidParameterException,
                                                                          RepositoryErrorException,
                                                                          EntityNotKnownException,
                                                                          EntityNotDeletedException,
                                                                          UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.restoreEntity(userId, deletedEntityGUID);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processRestoredEntityEvent(sourceName,
                                                                            metadataCollectionId,
                                                                            localServerName,
                                                                            localServerType,
                                                                            localOrganizationName,
                                                                            entity);
            }
        }

        return entity;
    }


    /**
     * Add the requested classification to a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @param classificationProperties - list of properties to set in the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is either not known or not valid
     *                                         for the entity.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this classification type
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail classifyEntity(String               userId,
                                       String               entityGUID,
                                       String               classificationName,
                                       InstanceProperties   classificationProperties) throws InvalidParameterException,
                                                                                             RepositoryErrorException,
                                                                                             EntityNotKnownException,
                                                                                             ClassificationErrorException,
                                                                                             PropertyErrorException,
                                                                                             UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.classifyEntity(userId,
                                                                      entityGUID,
                                                                      classificationName,
                                                                      classificationProperties);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processClassifiedEntityEvent(sourceName,
                                                                              metadataCollectionId,
                                                                              localServerName,
                                                                              localServerType,
                                                                              localOrganizationName,
                                                                              entity);
            }
        }

        return entity;
    }


    /**
     * Remove a specific classification from an entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is not set on the entity.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail declassifyEntity(String    userId,
                                         String    entityGUID,
                                         String    classificationName) throws InvalidParameterException,
                                                                              RepositoryErrorException,
                                                                              EntityNotKnownException,
                                                                              ClassificationErrorException,
                                                                              UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.declassifyEntity(userId,
                                                                        entityGUID,
                                                                        classificationName);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processDeclassifiedEntityEvent(sourceName,
                                                                                metadataCollectionId,
                                                                                localServerName,
                                                                                localServerType,
                                                                                localOrganizationName,
                                                                                entity);
            }
        }

        return entity;
    }


    /**
     * Update one or more properties in one of an entity's classifications.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @param properties - list of properties for the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is not attached to the classification.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this classification type
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityClassification(String               userId,
                                                   String               entityGUID,
                                                   String               classificationName,
                                                   InstanceProperties   properties) throws InvalidParameterException,
                                                                                           RepositoryErrorException,
                                                                                           EntityNotKnownException,
                                                                                           ClassificationErrorException,
                                                                                           PropertyErrorException,
                                                                                           UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.updateEntityClassification(userId,
                                                                                  entityGUID,
                                                                                  classificationName,
                                                                                  properties);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReclassifiedEntityEvent(sourceName,
                                                                                metadataCollectionId,
                                                                                localServerName,
                                                                                localServerType,
                                                                                localOrganizationName,
                                                                                entity);
            }
        }

        return entity;
    }



    /**
     * Add a new relationship between two entities to the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipTypeGUID - unique identifier (guid) for the new relationship's type.
     * @param initialProperties - initial list of properties for the new entity - null means no properties.
     * @param entityOneGUID - the unique identifier of one of the entities that the relationship is connecting together.
     * @param entityTwoGUID - the unique identifier of the other entity that the relationship is connecting together.
     * @param initialStatus - initial status - typically DRAFT, PREPARED or ACTIVE.
     * @return Relationship structure with the new header, requested entities and properties.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this relationship's type.
     * @throws EntityNotKnownException - one of the requested entities is not known in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship addRelationship(String               userId,
                                        String               relationshipTypeGUID,
                                        InstanceProperties   initialProperties,
                                        String               entityOneGUID,
                                        String               entityTwoGUID,
                                        InstanceStatus       initialStatus) throws InvalidParameterException,
                                                                                   RepositoryErrorException,
                                                                                   TypeErrorException,
                                                                                   PropertyErrorException,
                                                                                   EntityNotKnownException,
                                                                                   StatusNotSupportedException,
                                                                                   UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.addRelationship(userId,
                                                                             relationshipTypeGUID,
                                                                             initialProperties,
                                                                             entityOneGUID,
                                                                             entityTwoGUID,
                                                                             initialStatus);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processNewRelationshipEvent(sourceName,
                                                                             metadataCollectionId,
                                                                             localServerName,
                                                                             localServerType,
                                                                             localOrganizationName,
                                                                             relationship);
            }
        }

        return relationship;
    }


    /**
     * Update the status of a specific relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param newStatus - new InstanceStatus for the relationship.
     * @return Resulting relationship structure with the new status set.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship updateRelationshipStatus(String           userId,
                                                 String           relationshipGUID,
                                                 InstanceStatus   newStatus) throws InvalidParameterException,
                                                                                    RepositoryErrorException,
                                                                                    RelationshipNotKnownException,
                                                                                    StatusNotSupportedException,
                                                                                    UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.updateRelationshipStatus(userId,
                                                                                      relationshipGUID,
                                                                                      newStatus);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processUpdatedRelationshipEvent(sourceName,
                                                                                 metadataCollectionId,
                                                                                 localServerName,
                                                                                 localServerType,
                                                                                 localOrganizationName,
                                                                                 relationship);
            }
        }

        return relationship;
    }


    /**
     * Update the properties of a specific relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param properties - list of the properties to update.
     * @return Resulting relationship structure with the new properties set.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this relationship's type.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship updateRelationshipProperties(String               userId,
                                                     String               relationshipGUID,
                                                     InstanceProperties   properties) throws InvalidParameterException,
                                                                                             RepositoryErrorException,
                                                                                             RelationshipNotKnownException,
                                                                                             PropertyErrorException,
                                                                                             UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.updateRelationshipProperties(userId,
                                                                                          relationshipGUID,
                                                                                          properties);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processUpdatedRelationshipEvent(sourceName,
                                                                                 metadataCollectionId,
                                                                                 localServerName,
                                                                                 localServerType,
                                                                                 localOrganizationName,
                                                                                 relationship);
            }
        }

        return relationship;
    }


    /**
     * Undo the latest change to a relationship (either a change of properties or status).
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @return Relationship structure with the new current header, requested entities and properties.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship undoRelationshipUpdate(String    userId,
                                               String    relationshipGUID) throws InvalidParameterException,
                                                                                  RepositoryErrorException,
                                                                                  RelationshipNotKnownException,
                                                                                  UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.undoRelationshipUpdate(userId,
                                                                                    relationshipGUID);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processUndoneRelationshipEvent(sourceName,
                                                                                metadataCollectionId,
                                                                                localServerName,
                                                                                localServerType,
                                                                                localOrganizationName,
                                                                                relationship);
            }
        }

        return relationship;
    }

    /**
     * Delete a specific relationship.  This is a soft-delete which means the relationship's status is updated to
     * DELETED and it is no longer available for queries.  To remove the relationship permanently from the
     * metadata collection, use purgeRelationship().
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - unique identifier of the type of the relationship to delete.
     * @param typeDefName - unique name of the type of the relationship to delete.
     * @param obsoleteRelationshipGUID - String unique identifier (guid) for the relationship.
     * @throws InvalidParameterException - one of the parameters is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     soft-deletes.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteRelationship(String    userId,
                                   String    typeDefGUID,
                                   String    typeDefName,
                                   String    obsoleteRelationshipGUID) throws InvalidParameterException,
                                                                              RepositoryErrorException,
                                                                              RelationshipNotKnownException,
                                                                              StatusNotSupportedException,
                                                                              UserNotAuthorizedException
    {
        realMetadataCollection.deleteRelationship(userId, typeDefGUID, typeDefName, obsoleteRelationshipGUID);

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processDeletedRelationshipEvent(sourceName,
                                                                             metadataCollectionId,
                                                                             localServerName,
                                                                             localServerType,
                                                                             localOrganizationName,
                                                                             typeDefGUID,
                                                                             typeDefName,
                                                                             obsoleteRelationshipGUID);
        }
    }


    /**
     * Permanently delete the relationship from the repository.  There is no means to undo this request.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - unique identifier of the type of the relationship to purge.
     * @param typeDefName - unique name of the type of the relationship to purge.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @throws InvalidParameterException - one of the parameters is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws RelationshipNotDeletedException - the requested relationship is not in DELETED status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeRelationship(String    userId,
                                  String    typeDefGUID,
                                  String    typeDefName,
                                  String    deletedRelationshipGUID) throws InvalidParameterException,
                                                                            RepositoryErrorException,
                                                                            RelationshipNotKnownException,
                                                                            RelationshipNotDeletedException,
                                                                            UserNotAuthorizedException
    {
        realMetadataCollection.purgeRelationship(userId, typeDefGUID, typeDefName, deletedRelationshipGUID);

        if (outboundRepositoryEventProcessor != null)
        {
            outboundRepositoryEventProcessor.processPurgedRelationshipEvent(sourceName,
                                                                            metadataCollectionId,
                                                                            localServerName,
                                                                            localServerType,
                                                                            localOrganizationName,
                                                                            typeDefGUID,
                                                                            typeDefName,
                                                                            deletedRelationshipGUID);
        }
    }


    /**
     * Restore a deleted relationship into the metadata collection.  The new status will be ACTIVE and the
     * restored details of the relationship are returned to the caller.
     *
     * @param userId - unique identifier for requesting user.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @return Relationship structure with the restored header, requested entities and properties.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     * the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws RelationshipNotDeletedException - the requested relationship is not in DELETED status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship restoreRelationship(String    userId,
                                            String    deletedRelationshipGUID) throws InvalidParameterException,
                                                                                      RepositoryErrorException,
                                                                                      RelationshipNotKnownException,
                                                                                      RelationshipNotDeletedException,
                                                                                      UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.restoreRelationship(userId, deletedRelationshipGUID);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processRestoredRelationshipEvent(sourceName,
                                                                                  metadataCollectionId,
                                                                                  localServerName,
                                                                                  localServerType,
                                                                                  localOrganizationName,
                                                                                  relationship);
            }
        }

        return relationship;
    }


    /* ======================================================================
     * Group 5: Change the control information in entities and relationships
     */


    /**
     * Change the guid of an existing entity to a new value.  This is used if two different
     * entities are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - the guid of the TypeDef for the entity - used to verify the entity identity.
     * @param typeDefName - the name of the TypeDef for the entity - used to verify the entity identity.
     * @param entityGUID - the existing identifier for the entity.
     * @param newEntityGUID - the new guid for the entity.
     * @return entity - new values for this entity, including the new guid.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reIdentifyEntity(String    userId,
                                         String    typeDefGUID,
                                         String    typeDefName,
                                         String    entityGUID,
                                         String    newEntityGUID) throws InvalidParameterException,
                                                                         RepositoryErrorException,
                                                                         EntityNotKnownException,
                                                                         UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.reIdentifyEntity(userId,
                                                                        typeDefGUID,
                                                                        typeDefName,
                                                                        entityGUID,
                                                                        newEntityGUID);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReIdentifiedEntityEvent(sourceName,
                                                                                metadataCollectionId,
                                                                                localServerName,
                                                                                localServerType,
                                                                                localOrganizationName,
                                                                                entityGUID,
                                                                                entity);
            }
        }

        return entity;
    }


    /**
     * Change the type of an existing entity.  Typically this action is taken to move an entity's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type and the properties adjusted.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - the unique identifier for the entity to change.
     * @param currentTypeDefSummary - the current details of the TypeDef for the entity - used to verify the entity identity
     * @param newTypeDefSummary - details of this entity's new TypeDef.
     * @return entity - new values for this entity, including the new type information.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.     *
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reTypeEntity(String         userId,
                                     String         entityGUID,
                                     TypeDefSummary currentTypeDefSummary,
                                     TypeDefSummary newTypeDefSummary) throws InvalidParameterException,
                                                                              RepositoryErrorException,
                                                                              TypeErrorException,
                                                                              EntityNotKnownException,
                                                                              UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.reTypeEntity(userId,
                                                                    entityGUID,
                                                                    currentTypeDefSummary,
                                                                    newTypeDefSummary);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReTypedEntityEvent(sourceName,
                                                                           metadataCollectionId,
                                                                           localServerName,
                                                                           localServerType,
                                                                           localOrganizationName,
                                                                           currentTypeDefSummary,
                                                                           entity);
            }
        }

        return entity;
    }


    /**
     * Change the home of an existing entity.  This action is taken for example, if the original home repository
     * becomes permanently unavailable, or if the user community updating this entity move to working
     * from a different repository in the open metadata repository cohort.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - the unique identifier for the entity to change.
     * @param typeDefGUID - the guid of the TypeDef for the entity - used to verify the entity identity.
     * @param typeDefName - the name of the TypeDef for the entity - used to verify the entity identity.
     * @param homeMetadataCollectionId - the identifier of the metadata collection where this entity currently is homed.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return entity - new values for this entity, including the new home information.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reHomeEntity(String    userId,
                                     String    entityGUID,
                                     String    typeDefGUID,
                                     String    typeDefName,
                                     String    homeMetadataCollectionId,
                                     String    newHomeMetadataCollectionId) throws InvalidParameterException,
                                                                                   RepositoryErrorException,
                                                                                   EntityNotKnownException,
                                                                                   UserNotAuthorizedException
    {
        EntityDetail   entity = realMetadataCollection.reHomeEntity(userId,
                                                                    entityGUID,
                                                                    typeDefGUID,
                                                                    typeDefName,
                                                                    homeMetadataCollectionId,
                                                                    newHomeMetadataCollectionId);

        if (entity != null)
        {
            /*
             * Ensure the provenance of the entity is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the entity
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (entity.getMetadataCollectionId() == null)
            {
                entity.setMetadataCollectionId(metadataCollectionId);
                entity.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReHomedEntityEvent(sourceName,
                                                                           metadataCollectionId,
                                                                           localServerName,
                                                                           localServerType,
                                                                           localOrganizationName,
                                                                           metadataCollectionId,
                                                                           entity);
            }
        }

        return entity;
    }


    /**
     * Change the guid of an existing relationship.  This is used if two different
     * relationships are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param relationshipGUID - the existing identifier for the relationship.
     * @param newRelationshipGUID - the new identifier for the relationship.
     * @return relationship - new values for this relationship, including the new guid.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                         metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship reIdentifyRelationship(String    userId,
                                               String    typeDefGUID,
                                               String    typeDefName,
                                               String    relationshipGUID,
                                               String    newRelationshipGUID) throws InvalidParameterException,
                                                                                     RepositoryErrorException,
                                                                                     RelationshipNotKnownException,
                                                                                     UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.reIdentifyRelationship(userId,
                                                                                    typeDefGUID,
                                                                                    typeDefName,
                                                                                    relationshipGUID,
                                                                                    newRelationshipGUID);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReIdentifiedRelationshipEvent(sourceName,
                                                                                      metadataCollectionId,
                                                                                      localServerName,
                                                                                      localServerType,
                                                                                      localOrganizationName,
                                                                                      relationshipGUID,
                                                                                      relationship);
            }
        }

        return relationship;
    }


    /**
     * Change the type of an existing relationship.  Typically this action is taken to move a relationship's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - the unique identifier for the relationship.
     * @param currentTypeDefSummary - the details of the TypeDef for the relationship - used to verify the relationship identity.
     * @param newTypeDefSummary - new details for this relationship's TypeDef.
     * @return relationship - new values for this relationship, including the new type information.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                         metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship reTypeRelationship(String         userId,
                                           String         relationshipGUID,
                                           TypeDefSummary currentTypeDefSummary,
                                           TypeDefSummary newTypeDefSummary) throws InvalidParameterException,
                                                                                    RepositoryErrorException,
                                                                                    TypeErrorException,
                                                                                    RelationshipNotKnownException,
                                                                                    UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.reTypeRelationship(userId,
                                                                                relationshipGUID,
                                                                                currentTypeDefSummary,
                                                                                newTypeDefSummary);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReTypedRelationshipEvent(sourceName,
                                                                                 metadataCollectionId,
                                                                                 localServerName,
                                                                                 localServerType,
                                                                                 localOrganizationName,
                                                                                 currentTypeDefSummary,
                                                                                 relationship);
            }
        }

        return relationship;
    }


    /**
     * Change the home of an existing relationship.  This action is taken for example, if the original home repository
     * becomes permanently unavailable, or if the user community updating this relationship move to working
     * from a different repository in the open metadata repository cohort.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - the unique identifier for the relationship.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - the existing identifier for this relationship's home.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return relationship - new values for this relationship, including the new home information.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                         metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship reHomeRelationship(String    userId,
                                           String    relationshipGUID,
                                           String    typeDefGUID,
                                           String    typeDefName,
                                           String    homeMetadataCollectionId,
                                           String    newHomeMetadataCollectionId) throws InvalidParameterException,
                                                                                         RepositoryErrorException,
                                                                                         RelationshipNotKnownException,
                                                                                         UserNotAuthorizedException
    {
        Relationship   relationship = realMetadataCollection.reHomeRelationship(userId,
                                                                                relationshipGUID,
                                                                                typeDefGUID,
                                                                                typeDefName,
                                                                                homeMetadataCollectionId,
                                                                                newHomeMetadataCollectionId);

        if (relationship != null)
        {
            /*
             * Ensure the provenance of the relationship is correctly set.  A repository may not support the storing of
             * the metadata collection id in the repository (or uses null to mean "local").  When the relationship
             * detail is sent out, it must have its home metadata collection id set up.  So LocalOMRSMetadataCollection
             * fixes up the provenance.
             */
            if (relationship.getMetadataCollectionId() == null)
            {
                relationship.setMetadataCollectionId(metadataCollectionId);
                relationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            }

            /*
             * OK to send out
             */
            if (outboundRepositoryEventProcessor != null)
            {
                outboundRepositoryEventProcessor.processReHomedRelationshipEvent(sourceName,
                                                                                 metadataCollectionId,
                                                                                 localServerName,
                                                                                 localServerType,
                                                                                 localOrganizationName,
                                                                                 homeMetadataCollectionId,
                                                                                 relationship);
            }
        }

        return relationship;
    }



    /* ======================================================================
     * Group 6: Local house-keeping of reference metadata instances
     */


    /**
     * Save the entity as a reference copy.  The id of the home metadata collection is already set up in the
     * entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entity - details of the entity to save
     * @throws InvalidParameterException - the entity is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this entity's type.
     * @throws HomeEntityException - the entity belongs to the local repository so creating a reference
     *                               copy would be invalid.
     * @throws EntityConflictException - the new entity conflicts with an existing entity.
     * @throws InvalidEntityException - the new entity has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void saveEntityReferenceCopy(String         userId,
                                        EntityDetail   entity) throws InvalidParameterException,
                                                                      RepositoryErrorException,
                                                                      TypeErrorException,
                                                                      PropertyErrorException,
                                                                      HomeEntityException,
                                                                      EntityConflictException,
                                                                      InvalidEntityException,
                                                                      UserNotAuthorizedException
    {
        realMetadataCollection.saveEntityReferenceCopy(userId, entity);
    }


    /**
     * Remove a reference copy of the the entity from the local repository.  This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or entities that have come from open metadata archives.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - the unique identifier for the entity.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the new home repository.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws HomeEntityException - the entity belongs to the local repository so creating a reference
     *                               copy would be invalid.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeEntityReferenceCopy(String    userId,
                                         String    entityGUID,
                                         String    typeDefGUID,
                                         String    typeDefName,
                                         String    homeMetadataCollectionId) throws InvalidParameterException,
                                                                                    RepositoryErrorException,
                                                                                    EntityNotKnownException,
                                                                                    HomeEntityException,
                                                                                    UserNotAuthorizedException
    {
        realMetadataCollection.purgeEntityReferenceCopy(userId, entityGUID, typeDefGUID, typeDefName, homeMetadataCollectionId);
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified entity sends out the details of this entity so the local repository can create a reference copy.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - unique identifier of requested entity
     * @param typeDefGUID - unique identifier of requested entity's TypeDef
     * @param typeDefName - unique name of requested entity's TypeDef
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * @throws HomeEntityException - the entity belongs to the local repository so creating a reference
     *                               copy would be invalid.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void refreshEntityReferenceCopy(String    userId,
                                           String    entityGUID,
                                           String    typeDefGUID,
                                           String    typeDefName,
                                           String    homeMetadataCollectionId) throws InvalidParameterException,
                                                                                      RepositoryErrorException,
                                                                                      EntityNotKnownException,
                                                                                      HomeEntityException,
                                                                                      UserNotAuthorizedException
    {
        realMetadataCollection.refreshEntityReferenceCopy(userId,
                                                          entityGUID,
                                                          typeDefGUID,
                                                          typeDefName,
                                                          homeMetadataCollectionId);
    }


    /**
     * Save the relationship as a reference copy.  The id of the home metadata collection is already set up in the
     * relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationship - relationship to save
     * @throws InvalidParameterException - the relationship is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection.
     * @throws EntityNotKnownException - one of the entities identified by the relationship is not found in the
     *                                   metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this relationship's type.
     * @throws HomeRelationshipException - the relationship belongs to the local repository so creating a reference
     *                                     copy would be invalid.
     * @throws RelationshipConflictException - the new relationship conflicts with an existing relationship.
     * @throws InvalidRelationshipException - the new relationship has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void saveRelationshipReferenceCopy(String         userId,
                                              Relationship   relationship) throws InvalidParameterException,
                                                                                  RepositoryErrorException,
                                                                                  TypeErrorException,
                                                                                  EntityNotKnownException,
                                                                                  PropertyErrorException,
                                                                                  HomeRelationshipException,
                                                                                  RelationshipConflictException,
                                                                                  InvalidRelationshipException,
                                                                                  UserNotAuthorizedException
    {
        realMetadataCollection.saveRelationshipReferenceCopy(userId, relationship);
    }

    /**
     * Remove the reference copy of the relationship from the local repository. This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or relationships that have come from open metadata archives.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - the unique identifier for the relationship.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identifier is not recognized.
     * @throws HomeRelationshipException - the relationship belongs to the local repository so creating a reference
     *                                     copy would be invalid.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeRelationshipReferenceCopy(String    userId,
                                               String    relationshipGUID,
                                               String    typeDefGUID,
                                               String    typeDefName,
                                               String    homeMetadataCollectionId) throws InvalidParameterException,
                                                                                          RepositoryErrorException,
                                                                                          RelationshipNotKnownException,
                                                                                          HomeRelationshipException,
                                                                                          UserNotAuthorizedException
    {
        realMetadataCollection.purgeRelationshipReferenceCopy(userId,
                                                              relationshipGUID,
                                                              typeDefGUID,
                                                              typeDefName,
                                                              homeMetadataCollectionId);
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified relationship sends out the details of this relationship so the local repository can create a
     * reference copy.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - unique identifier of the relationship
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identifier is not recognized.
     * @throws HomeRelationshipException - the relationship belongs to the local repository so creating a reference
     *                                     copy would be invalid.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void refreshRelationshipReferenceCopy(String    userId,
                                                 String    relationshipGUID,
                                                 String    typeDefGUID,
                                                 String    typeDefName,
                                                 String    homeMetadataCollectionId) throws InvalidParameterException,
                                                                                            RepositoryErrorException,
                                                                                            RelationshipNotKnownException,
                                                                                            HomeRelationshipException,
                                                                                            UserNotAuthorizedException
    {
        realMetadataCollection.refreshRelationshipReferenceCopy(userId,
                                                                relationshipGUID,
                                                                typeDefGUID,
                                                                typeDefName,
                                                                homeMetadataCollectionId);
    }
}
