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
package org.apache.atlas.omrs.enterprise.repositoryconnector;

import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.*;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.MatchCriteria;
import org.apache.atlas.omrs.metadatacollection.properties.SequencingOrder;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;


/**
 * EnterpriseOMRSMetadataCollection executes the calls to the open metadata repositories registered
 * with the OMRSEnterpriseConnectorManager.  The effect is a federated view over these open metadata
 * repositories.
 * <p>
 *     EnterpriseOMRSMetadataCollection is part of an EnterpriseOMRSRepositoryConnector.  The EnterpriseOMRSRepositoryConnector
 *     holds the list of OMRS Connectors, one for each of the metadata repositories.  This list may change
 *     over time as metadata repositories register and deregister with the connected cohorts.
 *     The EnterpriseOMRSRepositoryConnector is responsible for keeping the list of connectors up-to-date through
 *     contact with the OMRSEnterpriseConnectorManager.
 * </p>
 * <p>
 *     When a request is made to the EnterpriseOMRSMetadataCollection, it calls the EnterpriseOMRSRepositoryConnector
 *     to request the appropriate list of metadata collection for the request.  Then the EnterpriseOMRSConnector
 *     calls the appropriate remote connectors.
 * </p>
 * <p>
 *     The first OMRS Connector in the list is the OMRS Repository Connector for the "local" repository.
 *     The local repository is favoured when new metadata is to be created, unless the type of metadata
 *     is not supported by the local repository. In which case, the EnterpriseOMRSMetadataCollection searches its
 *     list looking for the first metadata repository that supports the metadata type and stores it there.
 * </p>
 * <p>
 *     Updates and deletes are routed to the owning (home) repository.  Searches are made to each repository in turn
 *     and the duplicates are removed.  Queries are directed to the local repository and then the remote repositories
 *     until all of the requested metadata is assembled.
 * </p>
 */
public class EnterpriseOMRSMetadataCollection extends OMRSMetadataCollection
{

    /*
     * Private variables for a metadata collection instance
     */
    private EnterpriseOMRSRepositoryConnector parentConnector;
    private String                            enterpriseMetadataCollectionId;
    private String                            enterpriseMetadataCollectionName;
    private OMRSRepositoryValidator           repositoryValidator;
    private OMRSRepositoryHelper              repositoryHelper;


    /**
     * Default constructor.
     *
     * @param parentConnector - connector that this metadata collection supports.  The connector has the information
     *                        to call the metadata repository.
     * @param enterpriseMetadataCollectionId - unique identifier for the metadata collection.
     * @param enterpriseMetadataCollectionName - name of the metadata collection - used for messages.
     */
    public EnterpriseOMRSMetadataCollection(EnterpriseOMRSRepositoryConnector parentConnector,
                                            OMRSRepositoryHelper              repositoryHelper,
                                            OMRSRepositoryValidator           repositoryValidator,
                                            String                            enterpriseMetadataCollectionId,
                                            String                            enterpriseMetadataCollectionName)
    {
        /*
         * The metadata collection Id is the unique Id for the metadata collection.  It is managed by the super class.
         */
        super(enterpriseMetadataCollectionId);
        this.enterpriseMetadataCollectionId = enterpriseMetadataCollectionId;
        this.enterpriseMetadataCollectionName = enterpriseMetadataCollectionName;

        /*
         * Save parentConnector and associated helper and validator since this has the connection information and
         * access to the metadata about the open metadata repository cohort.
         */
        this.parentConnector = parentConnector;
        this.repositoryHelper = repositoryHelper;
        this.repositoryValidator = repositoryValidator;
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
     * @return TypeDefs - List of different categories of TypeDefs.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery getAllTypes(String userId) throws RepositoryErrorException,
                                                            UserNotAuthorizedException
    {
        final String                       methodName = "getTypeDefs()";

        /*
         * The list of metadata collections are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        ArrayList<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and may be returned if
         * there are no results from any repository.
         */
        List<TypeDef>               combinedTypeDefResults          = new ArrayList<>();
        List<AttributeTypeDef>      combinedAttributeTypeDefResults = new ArrayList<>();
        UserNotAuthorizedException  userNotAuthorizedException      = null;
        RepositoryErrorException    repositoryErrorException        = null;
        RuntimeException            anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection == null)
            {
                /*
                 * A problem in the set up of the metadata collection list.  Repository connectors implemented
                 * with no metadata collection are tested for in the OMRSEnterpriseConnectorManager so something
                 * else has gone wrong.
                 */
                OMRSErrorCode errorCode = OMRSErrorCode.NULL_ENTERPRISE_METADATA_COLLECTION;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }

            /*
             * Process the retrieved metadata collection
             */
            try
            {
                TypeDefGallery     results = metadataCollection.getAllTypes(userId);

                /*
                 * Step through the list of returned TypeDefs and consolidate.
                 */
                if (results != null)
                {
                    List<TypeDef>           typeDefResults     = results.getTypeDefs();
                    List<AttributeTypeDef>  attributeResults   = results.getAttributeTypeDefs();

                    if (typeDefResults != null)
                    {
                        combinedTypeDefResults.addAll(typeDefResults);
                    }

                    if (attributeResults != null)
                    {
                        combinedAttributeTypeDefResults.addAll(attributeResults);
                    }
                }
            }
            catch (RepositoryErrorException  error)
            {
                repositoryErrorException = error;
            }
            catch (UserNotAuthorizedException  error)
            {
                userNotAuthorizedException = error;
            }
            catch (RuntimeException   error)
            {
                anotherException = error;
            }
        }

        int  resultCount = (combinedTypeDefResults.size() + combinedAttributeTypeDefResults.size());

        /*
         * Return any results, or exception if nothing is found.
         */
        if (resultCount > 0)
        {
            TypeDefGallery combinedResults = new TypeDefGallery();

            combinedResults.setAttributeTypeDefs(combinedAttributeTypeDefResults);
            combinedResults.setTypeDefs(combinedTypeDefResults);

            if (repositoryValidator.validateEnterpriseTypeDefs(enterpriseMetadataCollectionName,
                                                               combinedTypeDefResults))
            {
                return combinedResults;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
        else
        {
            /*
             * A system with no defined TypeDefs is unbelievable.  Certainly not very useful :)
             * This probably means that exceptions were thrown.  Re-throw any cached exceptions
             * or, if no exceptions occurred then throw repository error because something is wrong.
             */
            if (userNotAuthorizedException != null)
            {
                throw userNotAuthorizedException;
            }
            else if (repositoryErrorException != null)
            {
                throw repositoryErrorException;
            }
            else if (anotherException != null)
            {
                throw anotherException;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEFS_DEFINED;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
    }


    /**
     * Returns a list of type definitions that have the specified name.  Type names should be unique.  This
     * method allows wildcard character to be included in the name.  These are * (asterisk) for an
     * arbitrary string of characters and ampersand for an arbitrary character.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - name of the TypeDefs to return (including wildcard characters).
     * @return TypeDefs list.
     * @throws InvalidParameterException - the name of the TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery findTypesByName(String   userId,
                                          String   name) throws InvalidParameterException,
                                                                RepositoryErrorException,
                                                                UserNotAuthorizedException
    {
        final String                       methodName = "findTypeDefsByName()";

        /*
         * Validate that there is a name supplied.
         */
        if (name == null)
        {
            /*
             * No Name supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF_NAME;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        /*
         * The list of metadata collections are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and may be returned if
         * there are no results from any repository.
         */
        List<TypeDef>               combinedTypeDefResults          = new ArrayList<>();
        List<AttributeTypeDef>      combinedAttributeTypeDefResults = new ArrayList<>();
        InvalidParameterException   invalidParameterException       = null;
        UserNotAuthorizedException  userNotAuthorizedException      = null;
        RepositoryErrorException    repositoryErrorException        = null;
        RuntimeException            anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection == null)
            {
                /*
                 * A problem in the set up of the metadata collection list.  Repository connectors implemented
                 * with no metadata collection are tested for in the OMRSEnterpriseConnectorManager so something
                 * else has gone wrong.
                 */
                OMRSErrorCode errorCode = OMRSErrorCode.NULL_ENTERPRISE_METADATA_COLLECTION;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }

            /*
             * Process the retrieved metadata collection
             */
            try
            {
                TypeDefGallery     results              = metadataCollection.findTypesByName(userId, name);
                String             metadataCollectionId = metadataCollection.getMetadataCollectionId();

                /*
                 * Combine the results from the metadata collection with those elements previously retrieved.
                 */
                if (results != null)
                {
                    List<TypeDef>           typeDefResults     = results.getTypeDefs();
                    List<AttributeTypeDef>  attributeResults   = results.getAttributeTypeDefs();

                    if (typeDefResults != null)
                    {
                        combinedTypeDefResults.addAll(typeDefResults);
                    }

                    if (attributeResults != null)
                    {
                        combinedAttributeTypeDefResults.addAll(attributeResults);
                    }
                }
            }
            catch (InvalidParameterException  error)
            {
                invalidParameterException = error;
            }
            catch (RepositoryErrorException  error)
            {
                repositoryErrorException = error;
            }
            catch (UserNotAuthorizedException  error)
            {
                userNotAuthorizedException = error;
            }
            catch (RuntimeException   error)
            {
                anotherException = error;
            }
        }

        int  resultCount = (combinedTypeDefResults.size() + combinedAttributeTypeDefResults.size());

        /*
         * Return any results, or exception if nothing is found.
         */
        if (resultCount > 0)
        {
            TypeDefGallery combinedResults = new TypeDefGallery();

            combinedResults.setAttributeTypeDefs(combinedAttributeTypeDefResults);
            combinedResults.setTypeDefs(combinedTypeDefResults);

            if (repositoryValidator.validateEnterpriseTypeDefs(enterpriseMetadataCollectionName,
                                                               combinedTypeDefResults))
            {
                return combinedResults;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
        else
        {
            if (userNotAuthorizedException != null)
            {
                throw userNotAuthorizedException;
            }
            else if (repositoryErrorException != null)
            {
                throw repositoryErrorException;
            }
            else if (invalidParameterException != null)
            {
                throw invalidParameterException;
            }
            else if (anotherException != null)
            {
                throw anotherException;
            }
            else
            {
                return null;
            }
        }
    }


    /**
     * Returns all of the TypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefCategory - enum value for the category of TypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException - the TypeDefCategory is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypeDefsByCategory(String          userId,
                                                TypeDefCategory typeDefCategory) throws InvalidParameterException,
                                                                                        RepositoryErrorException,
                                                                                        UserNotAuthorizedException
    {
        final String                       methodName = "findTypeDefsByCategory()";

        if (typeDefCategory == null)
        {
            /*
             * No category supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF_CATEGORY;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        /*
         * The list of metadata collections are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and may be returned if
         * there are no results from any repository.
         */
        List<TypeDef>              combinedResults            = new ArrayList<>();
        InvalidParameterException  invalidParameterException  = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        RuntimeException           anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection == null)
            {
                /*
                 * A problem in the set up of the metadata collection list.  Repository connectors implemented
                 * with no metadata collection are tested for in the OMRSEnterpriseConnectorManager so something
                 * else has gone wrong.
                 */
                OMRSErrorCode errorCode = OMRSErrorCode.NULL_ENTERPRISE_METADATA_COLLECTION;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }

            /*
             * Process the retrieved metadata collection
             */
            try
            {
                List<TypeDef>    results              = metadataCollection.findTypeDefsByCategory(userId, typeDefCategory);
                String           metadataCollectionId = metadataCollection.getMetadataCollectionId();

                /*
                 * Step through the list of returned TypeDefs and remove duplicates.
                 */
                if (results != null)
                {
                    for (TypeDef returnedTypeDef : results)
                    {
                        combinedResults = this.addUniqueTypeDef(combinedResults,
                                                                returnedTypeDef,
                                                                metadataCollectionId);
                    }
                }
            }
            catch (InvalidParameterException  error)
            {
                invalidParameterException = error;
            }
            catch (RepositoryErrorException  error)
            {
                repositoryErrorException = error;
            }
            catch (UserNotAuthorizedException  error)
            {
                userNotAuthorizedException = error;
            }
            catch (RuntimeException   error)
            {
                anotherException = error;
            }
        }

        /*
         * Return any results, or null if nothing is found.
         */
        if (combinedResults.size() > 0)
        {
            if (repositoryValidator.validateEnterpriseTypeDefs(enterpriseMetadataCollectionName, combinedResults))
            {
                return combinedResults;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
        else
        {
            if (userNotAuthorizedException != null)
            {
                throw userNotAuthorizedException;
            }
            else if (repositoryErrorException != null)
            {
                throw repositoryErrorException;
            }
            else if (invalidParameterException != null)
            {
                throw invalidParameterException;
            }
            else if (anotherException != null)
            {
                throw anotherException;
            }
            else
            {
                return null;
            }
        }
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
    public List<AttributeTypeDef> findAttributeTypeDefsByCategory(String                   userId,
                                                                  AttributeTypeDefCategory category) throws InvalidParameterException,
                                                                                                            RepositoryErrorException,
                                                                                                            UserNotAuthorizedException
    {
        return null;
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
    public List<TypeDef> findTypeDefsByProperty(String            userId,
                                                TypeDefProperties matchCriteria) throws InvalidParameterException,
                                                                                        RepositoryErrorException,
                                                                                        UserNotAuthorizedException
    {
        final String                       methodName = "findTypeDefsByProperty()";

        if (matchCriteria == null)
        {
            /*
             * No match criteria supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_MATCH_CRITERIA;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        /*
         * The list of metadata collections are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and may be returned if
         * there are no results from any repository.
         */
        List<TypeDef>              combinedResults            = new ArrayList<>();
        InvalidParameterException  invalidParameterException  = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        RuntimeException           anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection == null)
            {
                /*
                 * A problem in the set up of the metadata collection list.  Repository connectors implemented
                 * with no metadata collection are tested for in the OMRSEnterpriseConnectorManager so something
                 * else has gone wrong.
                 */
                OMRSErrorCode errorCode = OMRSErrorCode.NULL_ENTERPRISE_METADATA_COLLECTION;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }

            /*
             * Process the retrieved metadata collection
             */
            try
            {
                List<TypeDef>      results              = metadataCollection.findTypeDefsByProperty(userId, matchCriteria);
                String             metadataCollectionId = metadataCollection.getMetadataCollectionId();

                /*
                 * Step through the list of returned TypeDefs and remove duplicates.
                 */
                if (results != null)
                {
                    for (TypeDef returnedTypeDef : results)
                    {
                        combinedResults = this.addUniqueTypeDef(combinedResults,
                                                                returnedTypeDef,
                                                                metadataCollectionId);
                    }
                }
            }
            catch (InvalidParameterException  error)
            {
                invalidParameterException = error;
            }
            catch (RepositoryErrorException  error)
            {
                repositoryErrorException = error;
            }
            catch (UserNotAuthorizedException  error)
            {
                userNotAuthorizedException = error;
            }
            catch (RuntimeException   error)
            {
                anotherException = error;
            }
        }

        /*
         * Return any results, or null if nothing is found.
         */
        if (combinedResults.size() > 0)
        {
            if (repositoryValidator.validateEnterpriseTypeDefs(enterpriseMetadataCollectionName, combinedResults))
            {
                return combinedResults;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
        else
        {
            if (userNotAuthorizedException != null)
            {
                throw userNotAuthorizedException;
            }
            else if (repositoryErrorException != null)
            {
                throw repositoryErrorException;
            }
            else if (invalidParameterException != null)
            {
                throw invalidParameterException;
            }
            else if (anotherException != null)
            {
                throw anotherException;
            }
            else
            {
                return null;
            }
        }
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
     * @throws InvalidParameterException - all attributes of the external Id are null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypesByExternalID(String    userId,
                                                String    standard,
                                                String    organization,
                                                String    identifier) throws InvalidParameterException,
                                                                             RepositoryErrorException,
                                                                             UserNotAuthorizedException
    {
        final String                       methodName = "findTypeDefsByExternalID()";

        if ((standard == null) && (organization == null) && (identifier == null))
        {
            /*
             * No external Id properties supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_EXTERNAL_ID;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        // todo

        return null;
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
    public List<TypeDef> searchForTypeDefs(String userId,
                                           String searchCriteria) throws InvalidParameterException,
                                                                         RepositoryErrorException,
                                                                         UserNotAuthorizedException
    {
        final String                       methodName = "searchForTypeDefs()";

        if (searchCriteria == null)
        {
            /*
             * No search criteria supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_SEARCH_CRITERIA;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        /*
         * The list of metadata collections are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and may be returned if
         * there are no results from any repository.
         */
        List<TypeDef>              combinedResults            = new ArrayList<>();
        InvalidParameterException  invalidParameterException  = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        RuntimeException           anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection == null)
            {
                /*
                 * A problem in the set up of the metadata collection list.  Repository connectors implemented
                 * with no metadata collection are tested for in the OMRSEnterpriseConnectorManager so something
                 * else has gone wrong.
                 */
                OMRSErrorCode errorCode = OMRSErrorCode.NULL_ENTERPRISE_METADATA_COLLECTION;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }

            /*
             * Process the retrieved metadata collection
             */
            try
            {
                List<TypeDef> results              = metadataCollection.searchForTypeDefs(userId, searchCriteria);
                String        metadataCollectionId = metadataCollection.getMetadataCollectionId();

                /*
                 * Step through the list of returned TypeDefs and remove duplicates.
                 */
                if (results != null)
                {
                    for (TypeDef returnedTypeDef : results)
                    {
                        combinedResults = this.addUniqueTypeDef(combinedResults,
                                                                returnedTypeDef,
                                                                metadataCollectionId);
                    }
                }
            }
            catch (InvalidParameterException  error)
            {
                invalidParameterException = error;
            }
            catch (RepositoryErrorException  error)
            {
                repositoryErrorException = error;
            }
            catch (UserNotAuthorizedException  error)
            {
                userNotAuthorizedException = error;
            }
            catch (RuntimeException   error)
            {
                anotherException = error;
            }
        }

        /*
         * Return any results, or null if nothing is found.
         */
        if (combinedResults.size() > 0)
        {
            if (repositoryValidator.validateEnterpriseTypeDefs(enterpriseMetadataCollectionName, combinedResults))
            {
                return combinedResults;
            }
            else
            {
                OMRSErrorCode errorCode = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
        else
        {
            if (userNotAuthorizedException != null)
            {
                throw userNotAuthorizedException;
            }
            else if (repositoryErrorException != null)
            {
                throw repositoryErrorException;
            }
            else if (invalidParameterException != null)
            {
                throw invalidParameterException;
            }
            else if (anotherException != null)
            {
                throw anotherException;
            }
            else
            {
                return null;
            }
        }
    }


    /**
     * Return the TypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique Id of the TypeDef
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
        final String    methodName = "getTypeDefByGUID()";

        if (guid == null)
        {
            /*
             * No guid supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_GUID;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        TypeDefNotKnownException   typeDefNotKnownException = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException = null;
        RuntimeException           anotherException = null;

        /*
         * Loop through the metadata collections requesting the typedef from each repository.
         * The first TypeDef retrieved is returned to the caller.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection != null)
            {
                try
                {
                    TypeDef retrievedTypeDef = getTypeDefByGUID(userId, guid);

                    if (retrievedTypeDef != null)
                    {
                        return retrievedTypeDef;
                    }
                }
                catch (TypeDefNotKnownException  error)
                {
                    typeDefNotKnownException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (RuntimeException error)
                {
                    anotherException = error;
                }
            }
        }

        /*
         * The TypeDef has not been found. Process the exceptions.
         */
        if (typeDefNotKnownException != null)
        {
            throw typeDefNotKnownException;
        }
        else if (userNotAuthorizedException != null)
        {
            throw userNotAuthorizedException;
        }
        else if (repositoryErrorException != null)
        {
            throw repositoryErrorException;
        }
        else if (anotherException != null)
        {
            throw anotherException;
        }
        else
        {
            /*
             * All of the repositories have returned a null TypeDef so default to TypeDefNotKnownException.
             */
            // todo - bad parms for message
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(guid);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

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
        return null;
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
     */
    public TypeDef getTypeDefByName(String    userId,
                                    String    name) throws InvalidParameterException,
                                                           RepositoryErrorException,
                                                           TypeDefNotKnownException,
                                                           UserNotAuthorizedException
    {
        final String    methodName = "getTypeDefByName()";

        if (name == null)
        {
            /*
             * No name supplied so can not perform the search
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF_NAME;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();

        if (metadataCollections == null)
        {
            /*
             * No repositories available
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        TypeDefNotKnownException   typeDefNotKnownException = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException = null;
        RuntimeException           anotherException = null;

        /*
         * Loop through the metadata collections requesting the typedef from each repository.
         * The first TypeDef retrieved is returned to the caller.
         */
        for (OMRSMetadataCollection metadataCollection : metadataCollections)
        {
            if (metadataCollection != null)
            {
                try
                {
                    TypeDef retrievedTypeDef = getTypeDefByName(userId, name);

                    if (retrievedTypeDef != null)
                    {
                        return retrievedTypeDef;
                    }
                }
                catch (TypeDefNotKnownException  error)
                {
                    typeDefNotKnownException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (RuntimeException error)
                {
                    anotherException = error;
                }
            }
        }

        /*
         * The TypeDef has not been found. Process the exceptions.
         */
        if (typeDefNotKnownException != null)
        {
            throw typeDefNotKnownException;
        }
        else if (userNotAuthorizedException != null)
        {
            throw userNotAuthorizedException;
        }
        else if (repositoryErrorException != null)
        {
            throw repositoryErrorException;
        }
        else if (anotherException != null)
        {
            throw anotherException;
        }
        else
        {
            /*
             * All of the repositories have returned a null TypeDef so default to TypeDefNotKnownException.
             */
            // todo - bad parms for message
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(name);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }
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
        return null;
    }


    /**
     * Create a collection of related types.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypes - TypeDefGalleryResponse structure describing the new AttributeTypeDefs and TypeDefs.
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public  void addTypeDefGallery(String          userId,
                                   TypeDefGallery  newTypes) throws FunctionNotSupportedException

    {
        final String    methodName = "addTypeDefGallery()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * Create a definition of a new TypeDef.   This new TypeDef is pushed to each repository that will accept it.
     * An exception is passed to the caller if the TypeDef is invalid, or if none of the repositories accept it.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public void addTypeDef(String       userId,
                           TypeDef      newTypeDef) throws FunctionNotSupportedException
    {
        final String    methodName = "addTypeDef()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * Create a definition of a new AttributeTypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param newAttributeTypeDef - TypeDef structure describing the new TypeDef.
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public  void addAttributeTypeDef(String             userId,
                                     AttributeTypeDef   newAttributeTypeDef) throws FunctionNotSupportedException
    {
        final String    methodName = "addAttributeTypeDef()";

        throwNotEnterpriseFunction(methodName);
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
    public boolean verifyTypeDef(String       userId,
                                 TypeDef      typeDef) throws InvalidParameterException,
                                                              RepositoryErrorException,
                                                              TypeDefNotSupportedException,
                                                              TypeDefConflictException,
                                                              InvalidTypeDefException,
                                                              UserNotAuthorizedException
    {
        final String                       methodName = "verifyTypeDef()";

        // todo

        return true;
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
        final String                       methodName = "verifyAttributeTypeDef()";

        // todo

        return true;
    }


    /**
     * Update one or more properties of the TypeDef.  The TypeDefPatch controls what types of updates
     * are safe to make to the TypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefPatch - TypeDef patch describing change to TypeDef.
     * @return updated TypeDef
     * @throws FunctionNotSupportedException - the repository does not support this call.

     */
    public TypeDef updateTypeDef(String       userId,
                                 TypeDefPatch typeDefPatch) throws FunctionNotSupportedException
    {
        final String                       methodName = "updateTypeDef()";

        throwNotEnterpriseFunction(methodName);

        return null;
    }


    /**
     * Delete the TypeDef.  This is only possible if the TypeDef has never been used to create instances or any
     * instances of this TypeDef have been purged from the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public void deleteTypeDef(String    userId,
                              String    obsoleteTypeDefGUID,
                              String    obsoleteTypeDefName) throws FunctionNotSupportedException
    {
        final String                       methodName = "deleteTypeDef()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * Delete an AttributeTypeDef.  This is only possible if the AttributeTypeDef has never been used to create
     * instances or any instances of this AttributeTypeDef have been purged from the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public void deleteAttributeTypeDef(String    userId,
                                       String    obsoleteTypeDefGUID,
                                       String    obsoleteTypeDefName) throws FunctionNotSupportedException
    {
        final String                       methodName = "deleteAttributeTypeDef()";

        throwNotEnterpriseFunction(methodName);
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
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public  TypeDef reIdentifyTypeDef(String     userId,
                                      String     originalTypeDefGUID,
                                      String     originalTypeDefName,
                                      String     newTypeDefGUID,
                                      String     newTypeDefName) throws FunctionNotSupportedException
    {
        final String                       methodName = "reIdentifyTypeDef()";

        throwNotEnterpriseFunction(methodName);

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support this call.
     */
    public  AttributeTypeDef reIdentifyAttributeTypeDef(String     userId,
                                                        String     originalAttributeTypeDefGUID,
                                                        String     originalAttributeTypeDefName,
                                                        String     newAttributeTypeDefGUID,
                                                        String     newAttributeTypeDefName) throws FunctionNotSupportedException
    {
        final String                       methodName = "reIdentifyAttributeTypeDef()";

        throwNotEnterpriseFunction(methodName);

        return null;
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
    public EntityDetail isEntityKnown(String    userId,
                                      String    guid) throws InvalidParameterException,
                                                             RepositoryErrorException,
                                                             UserNotAuthorizedException
    {
        final String                       methodName = "isEntityKnown()";

        // TODO - missing implementation

        return null;
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
        EntityDetail     entity = this.isEntityKnown(userId, guid);

        if (entity == null)
        {
            final String   methodName = "getEntitySummary()";
            OMRSErrorCode  errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String         errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(guid,
                                                                             enterpriseMetadataCollectionName + " (" +
                                                                                     enterpriseMetadataCollectionId + ")");

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
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
     * @throws EntityProxyOnlyException - the requested entity instance is only a proxy in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail getEntityDetail(String    userId,
                                        String    guid) throws InvalidParameterException,
                                                               RepositoryErrorException,
                                                               EntityNotKnownException,
                                                               EntityProxyOnlyException,
                                                               UserNotAuthorizedException
    {
        EntityDetail     entity = this.isEntityKnown(userId, guid);

        if (entity == null)
        {
            final String   methodName = "getEntityDetail()";
            OMRSErrorCode  errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String         errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid,
                                                         enterpriseMetadataCollectionName + " (" +
                                                                 enterpriseMetadataCollectionId + ")");

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        return entity;
    }


    /**
     * Return a historical versionName of an entity - includes the header, classifications and properties of the entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @param asOfTime - the time used to determine which versionName of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException - the guid or date is null or the date is for a future time.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws EntityNotKnownException - the requested entity instance is not known in the metadata collection
     *                                   at the time requested.
     * @throws EntityProxyOnlyException - the requested entity instance is only a proxy in the metadata collection.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  EntityDetail getEntityDetail(String    userId,
                                         String    guid,
                                         Date      asOfTime) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    EntityNotKnownException,
                                                                    EntityProxyOnlyException,
                                                                    FunctionNotSupportedException,
                                                                    UserNotAuthorizedException
    {
        final String                       methodName = "isEntityKnown()";

        EntityDetail entity = null;

        // TODO - missing implementation


        if (entity == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String         errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(guid,
                                                         enterpriseMetadataCollectionName + " (" +
                                                                 enterpriseMetadataCollectionId + ")");

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        return null;
    }



    /**
     * Return the relationships for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier for the entity.
     * @param relationshipTypeGUID - String GUID of the the type of relationship required (null for all).
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<Relationship> getRelationshipsForEntity(String                     userId,
                                                        String                     entityGUID,
                                                        String                     relationshipTypeGUID,
                                                        int                        fromRelationshipElement,
                                                        List<InstanceStatus>       limitResultsByStatus,
                                                        Date                       asOfTime,
                                                        String                     sequencingProperty,
                                                        SequencingOrder            sequencingOrder,
                                                        int                        pageSize) throws InvalidParameterException,
                                                                                                    RepositoryErrorException,
                                                                                                    EntityNotKnownException,
                                                                                                    PropertyErrorException,
                                                                                                    PagingErrorException,
                                                                                                    FunctionNotSupportedException,
                                                                                                    UserNotAuthorizedException
    {
        final String                       methodName = "getRelationshipsForEntity()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Return a list of entities that match the supplied properties according to the match criteria.  The results
     * can be returned over many pages.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - String unique identifier for the entity type of interest (null means any entity type).
     * @param matchProperties - List of entity properties to match to (null means match on entityTypeGUID only).
     * @param matchCriteria - Enum defining how the properties should be matched to the entities in the repository.
     * @param fromEntityElement - the starting element number of the entities to return.
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<EntityDetail> findEntitiesByProperty(String                    userId,
                                                      String                    entityTypeGUID,
                                                      InstanceProperties        matchProperties,
                                                      MatchCriteria             matchCriteria,
                                                      int                       fromEntityElement,
                                                      List<InstanceStatus>      limitResultsByStatus,
                                                      List<String>              limitResultsByClassification,
                                                      Date                      asOfTime,
                                                      String                    sequencingProperty,
                                                      SequencingOrder           sequencingOrder,
                                                      int                       pageSize) throws InvalidParameterException,
                                                                                                 RepositoryErrorException,
                                                                                                 TypeErrorException,
                                                                                                 PropertyErrorException,
                                                                                                 PagingErrorException,
                                                                                                 FunctionNotSupportedException,
                                                                                                 UserNotAuthorizedException
    {
        final String                       methodName = "findEntitiesByProperty()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Return a list of entities that have the requested type of classification attached.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - unique identifier for the type of entity requested.  Null mans any type of entity.
     * @param classificationName - name of the classification - a null is not valid.
     * @param matchClassificationProperties - list of classification properties used to narrow the search.
     * @param matchCriteria - Enum defining how the properties should be matched to the classifications in the repository.
     * @param fromEntityElement - the starting element number of the entities to return.
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<EntityDetail> findEntitiesByClassification(String                    userId,
                                                            String                    entityTypeGUID,
                                                            String                    classificationName,
                                                            InstanceProperties        matchClassificationProperties,
                                                            MatchCriteria             matchCriteria,
                                                            int                       fromEntityElement,
                                                            List<InstanceStatus>      limitResultsByStatus,
                                                            Date                      asOfTime,
                                                            String                    sequencingProperty,
                                                            SequencingOrder           sequencingOrder,
                                                            int                       pageSize) throws InvalidParameterException,
                                                                                                       RepositoryErrorException,
                                                                                                       TypeErrorException,
                                                                                                       ClassificationErrorException,
                                                                                                       PropertyErrorException,
                                                                                                       PagingErrorException,
                                                                                                       FunctionNotSupportedException,
                                                                                                       UserNotAuthorizedException
    {
        final String                       methodName = "findEntitiesByProperty()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Return a list of entities matching the search criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param searchCriteria - String expression of the characteristics of the required relationships.
     * @param fromEntityElement - the starting element number of the entities to return.
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<EntityDetail> searchForEntities(String                     userId,
                                                 String                     searchCriteria,
                                                 int                        fromEntityElement,
                                                 List<InstanceStatus>       limitResultsByStatus,
                                                 List<String>               limitResultsByClassification,
                                                 Date                       asOfTime,
                                                 String                     sequencingProperty,
                                                 SequencingOrder            sequencingOrder,
                                                 int                        pageSize) throws InvalidParameterException,
                                                                                             RepositoryErrorException,
                                                                                             PropertyErrorException,
                                                                                             PagingErrorException,
                                                                                             FunctionNotSupportedException,
                                                                                             UserNotAuthorizedException
    {
        final String                       methodName = "searchForEntities()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Returns a boolean indicating if the relationship is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship
     * @return relationship details if the relationship is found in the metadata collection; otherwise return null
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
        final String                       methodName = "isRelationshipKnown()";

        // TODO - missing implementation

        return null;
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
        Relationship     relationship = this.isRelationshipKnown(userId, guid);

        if (relationship == null)
        {
            final String   methodName = "getRelationship()";
            OMRSErrorCode  errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String         errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(guid,
                                                                             enterpriseMetadataCollectionName + " (" +
                                                                                     enterpriseMetadataCollectionId + ")");

            throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return relationship;
    }


    /**
     * Return a historical version of a relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @param asOfTime - the time used to determine which versionName of the entity that is desired.
     * @return Relationship structure.
     * @throws InvalidParameterException - the guid or date is null or the date is for a future time.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested entity instance is not known in the metadata collection
     *                                   at the time requested.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  Relationship getRelationship(String    userId,
                                         String    guid,
                                         Date      asOfTime) throws InvalidParameterException,
                                                                    RepositoryErrorException,
                                                                    RelationshipNotKnownException,
                                                                    FunctionNotSupportedException,
                                                                    UserNotAuthorizedException
    {
        final String                       methodName = "getRelationship()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Return a list of relationships that match the requested properties by hte matching criteria.   The results
     * can be broken into pages.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipTypeGUID - unique identifier (guid) for the new relationship's type.
     * @param matchProperties - list of  properties used to narrow the search.
     * @param matchCriteria - Enum defining how the properties should be matched to the relationships in the repository.
     * @param fromRelationshipElement - the starting element number of the entities to return.
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<Relationship> findRelationshipsByProperty(String                    userId,
                                                           String                    relationshipTypeGUID,
                                                           InstanceProperties        matchProperties,
                                                           MatchCriteria             matchCriteria,
                                                           int                       fromRelationshipElement,
                                                           List<InstanceStatus>      limitResultsByStatus,
                                                           Date                      asOfTime,
                                                           String                    sequencingProperty,
                                                           SequencingOrder           sequencingOrder,
                                                           int                       pageSize) throws InvalidParameterException,
                                                                                                      RepositoryErrorException,
                                                                                                      TypeErrorException,
                                                                                                      PropertyErrorException,
                                                                                                      PagingErrorException,
                                                                                                      FunctionNotSupportedException,
                                                                                                      UserNotAuthorizedException
    {
        final String                       methodName = "findRelationshipsByProperty()";

        // TODO - missing implementation

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<Relationship> searchForRelationships(String                    userId,
                                                      String                    searchCriteria,
                                                      int                       fromRelationshipElement,
                                                      List<InstanceStatus>      limitResultsByStatus,
                                                      Date                      asOfTime,
                                                      String                    sequencingProperty,
                                                      SequencingOrder           sequencingOrder,
                                                      int                       pageSize) throws InvalidParameterException,
                                                                                                 RepositoryErrorException,
                                                                                                 PropertyErrorException,
                                                                                                 PagingErrorException,
                                                                                                 FunctionNotSupportedException,
                                                                                                 UserNotAuthorizedException
    {
        final String                       methodName = "searchForRelationships()";

        // TODO - missing implementation

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  InstanceGraph getLinkingEntities(String                    userId,
                                             String                    startEntityGUID,
                                             String                    endEntityGUID,
                                             List<InstanceStatus>      limitResultsByStatus,
                                             Date                      asOfTime) throws InvalidParameterException,
                                                                                        RepositoryErrorException,
                                                                                        EntityNotKnownException,
                                                                                        PropertyErrorException,
                                                                                        FunctionNotSupportedException,
                                                                                        UserNotAuthorizedException
    {
        final String                       methodName = "getLinkingEntities()";

        // TODO - missing implementation

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  InstanceGraph getEntityNeighborhood(String               userId,
                                                String               entityGUID,
                                                List<String>         entityTypeGUIDs,
                                                List<String>         relationshipTypeGUIDs,
                                                List<InstanceStatus> limitResultsByStatus,
                                                List<String>         limitResultsByClassification,
                                                Date                 asOfTime,
                                                int                  level) throws InvalidParameterException,
                                                                                   RepositoryErrorException,
                                                                                   TypeErrorException,
                                                                                   EntityNotKnownException,
                                                                                   PropertyErrorException,
                                                                                   FunctionNotSupportedException,
                                                                                   UserNotAuthorizedException
    {
        final String                       methodName = "getEntityNeighborhood()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Return the list of entities that are of the types listed in instanceTypes and are connected, either directly or
     * indirectly to the entity identified by startEntityGUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param startEntityGUID - unique identifier of the starting entity
     * @param instanceTypes - list of types to search for.  Null means any type.
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
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<EntityDetail> getRelatedEntities(String               userId,
                                                  String               startEntityGUID,
                                                  List<String>         instanceTypes,
                                                  int                  fromEntityElement,
                                                  List<InstanceStatus> limitResultsByStatus,
                                                  List<String>         limitResultsByClassification,
                                                  Date                 asOfTime,
                                                  String               sequencingProperty,
                                                  SequencingOrder      sequencingOrder,
                                                  int                  pageSize) throws InvalidParameterException,
                                                                                        RepositoryErrorException,
                                                                                        TypeErrorException,
                                                                                        EntityNotKnownException,
                                                                                        PropertyErrorException,
                                                                                        PagingErrorException,
                                                                                        FunctionNotSupportedException,
                                                                                        UserNotAuthorizedException
    {
        final String                       methodName = "getRelatedEntities()";

        // TODO - missing implementation

        return null;
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
                                  List<Classification>       initialClassifications,
                                  InstanceStatus             initialStatus) throws InvalidParameterException,
                                                                                   RepositoryErrorException,
                                                                                   TypeErrorException,
                                                                                   PropertyErrorException,
                                                                                   ClassificationErrorException,
                                                                                   StatusNotSupportedException,
                                                                                   UserNotAuthorizedException
    {
        final String                       methodName = "addEntity()";

        // TODO - missing implementation

        return null;
    }


    /**
     * Create an entity proxy in the metadata collection.  This is used to store relationships that span metadata
     * repositories.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityProxy - details of entity to add.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
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
     * @throws FunctionNotSupportedException - the repository does not support entity proxies as first class elements.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void addEntityProxy(String       userId,
                               EntityProxy  entityProxy) throws InvalidParameterException,
                                                                RepositoryErrorException,
                                                                TypeErrorException,
                                                                PropertyErrorException,
                                                                ClassificationErrorException,
                                                                StatusNotSupportedException,
                                                                FunctionNotSupportedException,
                                                                UserNotAuthorizedException
    {
        final String                       methodName = "addEntityProxy()";

        // TODO - missing implementation
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
        final String   methodName = "updateEntityStatus()";

        EntityDetail     entityDetail = this.isEntityKnown(userId, entityGUID);
        EntityUniverse   entity = null;

        if (entityDetail == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String         errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(entityGUID,
                                                         enterpriseMetadataCollectionName + " (" +
                                                                 enterpriseMetadataCollectionId + ")");

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        String    homeMetadataCollectionId = entityDetail.getMetadataCollectionId();

        if (homeMetadataCollectionId == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NULL_HOME_METADATA_COLLECTION_ID;
            String         errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID,
                                                         enterpriseMetadataCollectionName + " (" +
                                                                 enterpriseMetadataCollectionId + ")");

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

    /*
     * Step through the registered open metadata repositories to locate the home.  Once it is found
     * the update request can be made.
     */
        List<OMRSMetadataCollection>  metadataCollections = parentConnector.getMetadataCollections();
        for (OMRSMetadataCollection  metadataCollection : metadataCollections)
        {
            if (metadataCollection != null)
            {
                String             metadataCollectionId = metadataCollection.getMetadataCollectionId();
                if (homeMetadataCollectionId.equals(metadataCollectionId))
                {
                /*
                 * Issue request
                 */
                }
            }
        }

        if (entity == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String         errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID,
                                                         enterpriseMetadataCollectionName + " (" +
                                                                 enterpriseMetadataCollectionId + ")");

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
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
        final String                       methodName = "updateEntityProperties()";

        // TODO - missing implementation

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support undo.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail undoEntityUpdate(String    userId,
                                         String    entityGUID) throws InvalidParameterException,
                                                                      RepositoryErrorException,
                                                                      EntityNotKnownException,
                                                                      FunctionNotSupportedException,
                                                                      UserNotAuthorizedException
    {
        final String                       methodName = "undoEntityUpdate()";

        // TODO - missing implementation

        return null;
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
     * @throws FunctionNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                       soft-deletes - use purgeEntity() to remove the entity permanently.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail  deleteEntity(String userId,
                                      String typeDefGUID,
                                      String typeDefName,
                                      String obsoleteEntityGUID) throws InvalidParameterException,
                                                                        RepositoryErrorException,
                                                                        EntityNotKnownException,
                                                                        FunctionNotSupportedException,
                                                                        UserNotAuthorizedException
    {
        final String                       methodName = "deleteEntity()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "purgeEntity()";

        // TODO - missing implementation
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
     * @throws FunctionNotSupportedException - the repository does not support soft-deletes.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail restoreEntity(String    userId,
                                      String    deletedEntityGUID) throws InvalidParameterException,
                                                                          RepositoryErrorException,
                                                                          EntityNotKnownException,
                                                                          EntityNotDeletedException,
                                                                          FunctionNotSupportedException,
                                                                          UserNotAuthorizedException
    {
        final String                       methodName = "restoreEntity()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "classifyEntity()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "declassifyEntity()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "updateEntityClassification()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "addRelationship()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "updateRelationshipStatus()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "updateRelationshipProperties()";

        // TODO - missing implementation

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support undo.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship undoRelationshipUpdate(String    userId,
                                               String    relationshipGUID) throws InvalidParameterException,
                                                                                  RepositoryErrorException,
                                                                                  RelationshipNotKnownException,
                                                                                  FunctionNotSupportedException,
                                                                                  UserNotAuthorizedException
    {
        final String                       methodName = "undoRelationshipUpdate()";

        // TODO - missing implementation

        return null;
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
     * @return delete relationship
     * @throws InvalidParameterException - one of the parameters is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     * the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws FunctionNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     soft-deletes.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship deleteRelationship(String    userId,
                                           String    typeDefGUID,
                                           String    typeDefName,
                                           String    obsoleteRelationshipGUID) throws InvalidParameterException,
                                                                                      RepositoryErrorException,
                                                                                      RelationshipNotKnownException,
                                                                                      FunctionNotSupportedException,
                                                                                      UserNotAuthorizedException
    {
        final String                       methodName = "deleteRelationship()";

        // TODO - missing implementation

        return null;
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
        final String                       methodName = "purgeRelationship()";

        // TODO - missing implementation
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
     * @throws FunctionNotSupportedException - the repository does not support soft-deletes.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship restoreRelationship(String    userId,
                                            String    deletedRelationshipGUID) throws InvalidParameterException,
                                                                                      RepositoryErrorException,
                                                                                      RelationshipNotKnownException,
                                                                                      RelationshipNotDeletedException,
                                                                                      FunctionNotSupportedException,
                                                                                      UserNotAuthorizedException
    {
        final String                       methodName = "restoreRelationship()";

        // TODO - missing implementation

        return null;
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
     * @param newEntityGUID - new unique identifier for the entity.
     * @return entity - new values for this entity, including the new guid.
     * @throws FunctionNotSupportedException - the repository does not support the re-identification of instances.
     */
    public EntityDetail reIdentifyEntity(String    userId,
                                         String    typeDefGUID,
                                         String    typeDefName,
                                         String    entityGUID,
                                         String    newEntityGUID) throws FunctionNotSupportedException
    {
        final String                       methodName = "reIdentifyEntity()";

        throwNotEnterpriseFunction(methodName);

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support the re-typing of instances.
     */
    public EntityDetail reTypeEntity(String         userId,
                                     String         entityGUID,
                                     TypeDefSummary currentTypeDefSummary,
                                     TypeDefSummary newTypeDefSummary) throws FunctionNotSupportedException
    {
        final String                       methodName = "reTypeEntity()";

        throwNotEnterpriseFunction(methodName);

        return null;
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
     * @param homeMetadataCollectionId - the existing identifier for this entity's home.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return entity - new values for this entity, including the new home information.
     * @throws FunctionNotSupportedException - the repository does not support the re-homing of instances.
     */
    public EntityDetail reHomeEntity(String         userId,
                                     String         entityGUID,
                                     String         typeDefGUID,
                                     String         typeDefName,
                                     String         homeMetadataCollectionId,
                                     String         newHomeMetadataCollectionId) throws FunctionNotSupportedException
    {
        final String                       methodName = "reHomeEntity()";

        throwNotEnterpriseFunction(methodName);

        return null;
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
     * @param newRelationshipGUID  - the new unique identifier for the relationship.
     * @return relationship - new values for this relationship, including the new guid.
     * @throws FunctionNotSupportedException - the repository does not support the re-identification of instances.
     */
    public Relationship reIdentifyRelationship(String     userId,
                                               String     typeDefGUID,
                                               String     typeDefName,
                                               String     relationshipGUID,
                                               String     newRelationshipGUID) throws FunctionNotSupportedException
    {
        final String                       methodName = "reIdentifyRelationship()";

        throwNotEnterpriseFunction(methodName);

        return null;
    }


    /**
     * Change the type of an existing relationship.  Typically this action is taken to move a relationship's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - the unique identifier for the relationship.
     * @param currentTypeDefSummary - the details of the TypeDef for the relationship - used to verify the relationship identity.
     * @param newTypeDefSummary - details of this relationship's new TypeDef.
     * @return relationship - new values for this relationship, including the new type information.
     * @throws FunctionNotSupportedException - the repository does not support the re-typing of instances.
     */
    public Relationship reTypeRelationship(String         userId,
                                           String         relationshipGUID,
                                           TypeDefSummary currentTypeDefSummary,
                                           TypeDefSummary newTypeDefSummary) throws FunctionNotSupportedException
    {
        final String methodName = "reTypeRelationship()";

        throwNotEnterpriseFunction(methodName);

        return null;
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
     * @throws FunctionNotSupportedException - the repository does not support the re-homing of instances.
     */
    public Relationship reHomeRelationship(String   userId,
                                           String   relationshipGUID,
                                           String   typeDefGUID,
                                           String   typeDefName,
                                           String   homeMetadataCollectionId,
                                           String   newHomeMetadataCollectionId) throws FunctionNotSupportedException
    {
        final String                       methodName = "reHomeRelationship()";

        throwNotEnterpriseFunction(methodName);

        return null;
    }



    /* ======================================================================
     * Group 6: Local house-keeping of reference metadata instances
     * These methods are not supported by the EnterpriseOMRSRepositoryConnector
     */


    /**
     * Save the entity as a reference copy.  The id of the home metadata collection is already set up in the
     * entity.
     *
     * @param serverName - unique identifier for requesting server.
     * @param entity - details of the entity to save
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void saveEntityReferenceCopy(String         serverName,
                                        EntityDetail   entity) throws FunctionNotSupportedException
    {
        final String    methodName = "saveEntityReferenceCopy()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * Remove a reference copy of the the entity from the local repository.  This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or entities that have come from open metadata archives.
     *
     * @param serverName - unique identifier for requesting server.
     * @param entityGUID - the unique identifier for the entity.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void purgeEntityReferenceCopy(String   serverName,
                                         String   entityGUID,
                                         String   typeDefGUID,
                                         String   typeDefName,
                                         String   homeMetadataCollectionId) throws FunctionNotSupportedException
    {
        final String    methodName = "purgeEntityReferenceCopy()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified entity sends out the details of this entity so the local repository can create a reference copy.
     *
     * @param serverName - unique identifier for requesting server.
     * @param entityGUID - unique identifier of requested entity
     * @param typeDefGUID - unique identifier of requested entity's TypeDef
     * @param typeDefName - unique name of requested entity's TypeDef
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void refreshEntityReferenceCopy(String   serverName,
                                           String   entityGUID,
                                           String   typeDefGUID,
                                           String   typeDefName,
                                           String   homeMetadataCollectionId) throws FunctionNotSupportedException
    {
        final String    methodName = "refreshEntityReferenceCopy()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * Save the relationship as a reference copy.  The id of the home metadata collection is already set up in the
     * relationship.
     *
     * @param serverName - unique identifier for requesting server.
     * @param relationship - relationship to save
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void saveRelationshipReferenceCopy(String         serverName,
                                              Relationship   relationship) throws FunctionNotSupportedException
    {
        final String    methodName = "saveRelationshipReferenceCopy()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * Remove the reference copy of the relationship from the local repository. This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or relationships that have come from open metadata archives.
     *
     * @param serverName - unique identifier for requesting server.
     * @param relationshipGUID - the unique identifier for the relationship.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void purgeRelationshipReferenceCopy(String   serverName,
                                               String   relationshipGUID,
                                               String   typeDefGUID,
                                               String   typeDefName,
                                               String   homeMetadataCollectionId) throws FunctionNotSupportedException
    {
        final String    methodName = "purgeRelationshipReferenceCopy()";

        throwNotEnterpriseFunction(methodName);
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified relationship sends out the details of this relationship so the local repository can create a
     * reference copy.
     *
     * @param serverName - unique identifier for requesting user.
     * @param relationshipGUID - unique identifier of the relationship
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void refreshRelationshipReferenceCopy(String   serverName,
                                                 String   relationshipGUID,
                                                 String   typeDefGUID,
                                                 String   typeDefName,
                                                 String   homeMetadataCollectionId) throws FunctionNotSupportedException
    {
        final String    methodName = "refreshRelationshipReferenceCopy()";

        throwNotEnterpriseFunction(methodName);
    }


    /*
     * =================================================
     * Private validation and processing methods
     */

    /**
     * Adds the returned TypeDef to the combined results if it is not already included.
     * Also performs multiple validations since it is pulling metadata from a potentially heterogeneous
     * metadata repository ecosystem.
     *
     * @param currentResults - current list of TypeDefs
     * @param returnedTypeDef - new TypeDef
     * @param sourceMetadataCollectionId - identifier of the metadata collection where the returned TypeDef came from
     * @return list of unique TypeDefs
     * @throws RepositoryErrorException - if the returned TypeDef is not valid
     */
    private  List<TypeDef>   addUniqueTypeDef(List<TypeDef> currentResults,
                                              TypeDef            returnedTypeDef,
                                              String             sourceMetadataCollectionId) throws RepositoryErrorException
    {
        if (returnedTypeDef  != null)
        {
            String    returnedTypeDefGUID = returnedTypeDef.getGUID();
            String    returnedTypeDefName = returnedTypeDef.getName();
            long      returnedTypeDefVersion = returnedTypeDef.getVersion();

            /*
             * The returnedTypeDef is ignored if it does not have its header filled out properly.
             */
            if ((returnedTypeDefGUID != null) && (returnedTypeDefName != null))
            {
                boolean isUnique = true;

                for (TypeDef currentTypeDef : currentResults)
                {
                    if (currentTypeDef.getGUID().equals(returnedTypeDefGUID))
                    {
                        if (currentTypeDef.getName().equals(returnedTypeDefName))
                        {
                            if (currentTypeDef.getVersion() == returnedTypeDefVersion)
                            {
                                if (currentTypeDef.equals(returnedTypeDef))
                                {
                                    isUnique = false;
                                }
                                else
                                {
                                    /*
                                     * There are differences in the contents of the TypeDef's values.
                                     * This could cause an integrity problem.
                                     * An exception is thrown and a message is logged to the administrator.
                                     */

                                }
                            }
                            else
                            {
                                /*
                                 * Multiple versions of the same TypeDef are active.  If the patches are compatible it
                                 * may not be a problem but there may be an issue.  Both versions of the TypeDef
                                 * will be returned to the caller and a warning logged to the audit log.
                                 */
                            }
                        }
                        else
                        {
                            /*
                             * Multiple TypeDefs with the same GUID are in operation.  This is a serious problem.
                             * An exception is thrown and a message is logged to the administrator.
                             */
                        }
                    }
                    else /* GUIDs are different */
                    {
                        if (currentTypeDef.getName().equals(returnedTypeDefName))
                        {
                            /*
                             * Multiple TypeDefs with the same name are in operation.This is a serious problem.
                             * An exception is thrown and a message is logged to the administrator.
                             */
                        }
                    }
                }

                /*
                 * The returned Typedef is unique, so add it to the results.
                 */
                if (isUnique)
                {
                    currentResults.add(returnedTypeDef);
                }
            }
            else
            {
                /*
                 * Incomplete TypeDef returned
                 */
            }
        }
        else
        {
            /*
             * Null TypeDef returned
             */
        }

        return currentResults;
    }


    /**
     * Indicates to the caller that the method called is not supported by the enterprise connector.
     *
     * @param methodName - name of the method that was called
     * @throws FunctionNotSupportedException - resulting exception
     */
    private void throwNotEnterpriseFunction(String methodName) throws FunctionNotSupportedException
    {
        OMRSErrorCode errorCode = OMRSErrorCode.ENTERPRISE_NOT_SUPPORTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

        throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
    }
}
