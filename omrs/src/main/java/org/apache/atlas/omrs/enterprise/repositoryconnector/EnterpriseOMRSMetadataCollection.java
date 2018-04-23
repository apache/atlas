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
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

import java.util.HashMap;
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
    private EnterpriseOMRSRepositoryConnector enterpriseParentConnector;


    /**
     * Constructor ensures the metadata collection is linked to its connector and knows its metadata collection Id.
     *
     * @param enterpriseParentConnector - connector that this metadata collection supports.  The connector has the information
     *                        to call the metadata repository.
     * @param repositoryName - name of the repository - used for logging.
     * @param repositoryHelper - class used to build type definitions and instances.
     * @param repositoryValidator - class used to validate type definitions and instances.
     * @param metadataCollectionId - unique Identifier of the metadata collection Id.
     */
    public EnterpriseOMRSMetadataCollection(EnterpriseOMRSRepositoryConnector enterpriseParentConnector,
                                            String                            repositoryName,
                                            OMRSRepositoryHelper              repositoryHelper,
                                            OMRSRepositoryValidator           repositoryValidator,
                                            String                            metadataCollectionId)
    {
        /*
         * The metadata collection Id is the unique identifier for the metadata collection.  It is managed by the super class.
         */
        super(enterpriseParentConnector, repositoryName, metadataCollectionId, repositoryHelper, repositoryValidator);

        /*
         * Save enterpriseParentConnector since this has the connection information and
         * access to the metadata about the open metadata repository cohort.
         */
        this.enterpriseParentConnector = enterpriseParentConnector;

    }


    /* ======================================================================
     * Group 1: Confirm the identity of the metadata repository being called.
     */

    /**
     * Returns the identifier of the metadata repository.  This is the identifier used to register the
     * metadata repository with the metadata repository cohort.  It is also the identifier used to
     * identify the home repository of a metadata instance.
     *
     * @return String - metadata collection id.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     */
    public String      getMetadataCollectionId() throws RepositoryErrorException
    {
        final String methodName = "getMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        /*
         * Perform operation
         */
        return super.metadataCollectionId;
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
        final String                       methodName = "getAllTypes";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);


        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no results from any repository.
         */
        HashMap<String, TypeDef>               combinedTypeDefResults          = new HashMap<>();
        HashMap<String, AttributeTypeDef>      combinedAttributeTypeDefResults = new HashMap<>();

        UserNotAuthorizedException  userNotAuthorizedException      = null;
        RepositoryErrorException    repositoryErrorException        = null;
        Throwable                   anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    TypeDefGallery     results = metadataCollection.getAllTypes(userId);

                    /*
                     * Step through the list of returned TypeDefs and consolidate.
                     */
                    if (results != null)
                    {
                        combinedAttributeTypeDefResults = this.addUniqueAttributeTypeDefs(combinedAttributeTypeDefResults,
                                                                                          results.getAttributeTypeDefs(),
                                                                                          cohortConnector.getServerName(),
                                                                                          cohortConnector.getMetadataCollectionId(),
                                                                                          methodName);
                        combinedTypeDefResults = this.addUniqueTypeDefs(combinedTypeDefResults,
                                                                        results.getTypeDefs(),
                                                                        cohortConnector.getServerName(),
                                                                        cohortConnector.getMetadataCollectionId(),
                                                                        methodName);
                    }
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedTypeDefGalleryResults(repositoryName,
                                              combinedTypeDefResults,
                                              combinedAttributeTypeDefResults,
                                              userNotAuthorizedException,
                                              repositoryErrorException,
                                              anotherException,
                                              methodName);
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
        final String   methodName        = "findTypesByName";
        final String   nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);


        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no results from any repository.
         */
        HashMap<String, TypeDef>               combinedTypeDefResults          = new HashMap<>();
        HashMap<String, AttributeTypeDef>      combinedAttributeTypeDefResults = new HashMap<>();

        UserNotAuthorizedException  userNotAuthorizedException      = null;
        RepositoryErrorException    repositoryErrorException        = null;
        Throwable                   anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    TypeDefGallery     results = metadataCollection.findTypesByName(userId, name);

                    /*
                     * Step through the list of returned TypeDefs and consolidate.
                     */
                    if (results != null)
                    {
                        combinedAttributeTypeDefResults = this.addUniqueAttributeTypeDefs(combinedAttributeTypeDefResults,
                                                                                          results.getAttributeTypeDefs(),
                                                                                          cohortConnector.getServerName(),
                                                                                          cohortConnector.getMetadataCollectionId(),
                                                                                          methodName);
                        combinedTypeDefResults = this.addUniqueTypeDefs(combinedTypeDefResults,
                                                                        results.getTypeDefs(),
                                                                        cohortConnector.getServerName(),
                                                                        cohortConnector.getMetadataCollectionId(),
                                                                        methodName);
                    }
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedTypeDefGalleryResults(repositoryName,
                                              combinedTypeDefResults,
                                              combinedAttributeTypeDefResults,
                                              userNotAuthorizedException,
                                              repositoryErrorException,
                                              anotherException,
                                              methodName);
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
    public List<TypeDef> findTypeDefsByCategory(String          userId,
                                                TypeDefCategory category) throws InvalidParameterException,
                                                                                 RepositoryErrorException,
                                                                                 UserNotAuthorizedException
    {
        final String methodName            = "findTypeDefsByCategory";
        final String categoryParameterName = "category";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no results from any repository.
         */
        HashMap<String, TypeDef>   combinedResults            = new HashMap<>();
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<TypeDef> results  = metadataCollection.findTypeDefsByCategory(userId, category);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueTypeDefs(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedTypeDefListResults(repositoryName,
                                           combinedResults,
                                           userNotAuthorizedException,
                                           repositoryErrorException,
                                           anotherException,
                                           methodName);
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
        final String methodName            = "findAttributeTypeDefsByCategory";
        final String categoryParameterName = "category";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no results from any repository.
         */
        HashMap<String, AttributeTypeDef>   combinedResults   = new HashMap<>();

        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<AttributeTypeDef> results  = metadataCollection.findAttributeTypeDefsByCategory(userId, category);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueAttributeTypeDefs(combinedResults,
                                                                      results,
                                                                      cohortConnector.getServerName(),
                                                                      cohortConnector.getMetadataCollectionId(),
                                                                      methodName);
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedAttributeTypeDefListResults(repositoryName,
                                                    combinedResults,
                                                    userNotAuthorizedException,
                                                    repositoryErrorException,
                                                    anotherException,
                                                    methodName);
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
        final String  methodName                 = "findTypeDefsByProperty";
        final String  matchCriteriaParameterName = "matchCriteria";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName, matchCriteriaParameterName, matchCriteria, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no results from any repository.
         */
        HashMap<String, TypeDef>   combinedResults            = new HashMap<>();
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<TypeDef> results  = metadataCollection.findTypeDefsByProperty(userId, matchCriteria);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueTypeDefs(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedTypeDefListResults(repositoryName,
                                           combinedResults,
                                           userNotAuthorizedException,
                                           repositoryErrorException,
                                           anotherException,
                                           methodName);
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
    public  List<TypeDef> findTypesByExternalID(String    userId,
                                                String    standard,
                                                String    organization,
                                                String    identifier) throws InvalidParameterException,
                                                                             RepositoryErrorException,
                                                                             UserNotAuthorizedException
    {
        final String                       methodName = "findTypesByExternalID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateExternalId(repositoryName, standard, organization, identifier, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, TypeDef>   combinedResults            = new HashMap<>();
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<TypeDef> results  = metadataCollection.findTypesByExternalID(userId, standard, organization, identifier);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueTypeDefs(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedTypeDefListResults(repositoryName,
                                           combinedResults,
                                           userNotAuthorizedException,
                                           repositoryErrorException,
                                           anotherException,
                                           methodName);
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
        final String methodName                  = "searchForTypeDefs";
        final String searchCriteriaParameterName = "searchCriteria";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no results from any repository.
         */
        HashMap<String, TypeDef>   combinedResults            = new HashMap<>();
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<TypeDef> results  = metadataCollection.searchForTypeDefs(userId, searchCriteria);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueTypeDefs(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedTypeDefListResults(repositoryName,
                                           combinedResults,
                                           userNotAuthorizedException,
                                           repositoryErrorException,
                                           anotherException,
                                           methodName);
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
        final String methodName        = "getTypeDefByGUID";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        TypeDefNotKnownException   typeDefNotKnownException   = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.getTypeDefByGUID(userId, guid);
                }
                catch (TypeDefNotKnownException error)
                {
                    typeDefNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeDefNotKnownException(typeDefNotKnownException);

        return null;
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
        final String methodName        = "getAttributeTypeDefByGUID";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        TypeDefNotKnownException   typeDefNotKnownException   = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.getAttributeTypeDefByGUID(userId, guid);
                }
                catch (TypeDefNotKnownException error)
                {
                    typeDefNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);

        if (typeDefNotKnownException != null)
        {
            throw typeDefNotKnownException;
        }

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
        final String  methodName = "getTypeDefByName";
        final String  nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        TypeDefNotKnownException   typeDefNotKnownException   = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.getTypeDefByName(userId, name);
                }
                catch (TypeDefNotKnownException error)
                {
                    typeDefNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeDefNotKnownException(typeDefNotKnownException);

        return null;
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
        final String  methodName = "getAttributeTypeDefByName";
        final String  nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        TypeDefNotKnownException   typeDefNotKnownException   = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.getAttributeTypeDefByName(userId, name);
                }
                catch (TypeDefNotKnownException error)
                {
                    typeDefNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeDefNotKnownException(typeDefNotKnownException);

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
        final String  methodName           = "verifyTypeDef";
        final String  typeDefParameterName = "typeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, typeDef, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        TypeDefNotSupportedException   typeDefNotSupportedException = null;
        UserNotAuthorizedException     userNotAuthorizedException   = null;
        RepositoryErrorException       repositoryErrorException     = null;
        Throwable                      anotherException             = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds (TypeDefConflictException is also returned
                     * immediately.)
                     */
                    return metadataCollection.verifyTypeDef(userId, typeDef);
                }
                catch (TypeDefNotSupportedException error)
                {
                    typeDefNotSupportedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeDefNotSupportedException(typeDefNotSupportedException);

        return false;
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
        final String  methodName           = "verifyAttributeTypeDef";
        final String  typeDefParameterName = "attributeTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, attributeTypeDef, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        TypeDefNotSupportedException   typeDefNotSupportedException = null;
        UserNotAuthorizedException     userNotAuthorizedException   = null;
        RepositoryErrorException       repositoryErrorException     = null;
        Throwable                      anotherException             = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds (TypeDefConflictException is also returned
                     * immediately.)
                     */
                    return metadataCollection.verifyAttributeTypeDef(userId, attributeTypeDef);
                }
                catch (TypeDefNotSupportedException error)
                {
                    typeDefNotSupportedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeDefNotSupportedException(typeDefNotSupportedException);

        return false;
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
     * Returns a boolean indicating if the entity is stored in the metadata collection.  This entity may be a full
     * entity object, or an entity proxy.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity
     * @return the entity details if the entity is found in the metadata collection; otherwise return null
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
        final String  methodName = "isEntityKnown";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    EntityDetail     entity = this.isEntityKnown(userId, guid);

                    repositoryValidator.validateEntityFromStore(repositoryName, guid, entity, methodName);

                    return entity;
                }

                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);

        return null;
    }


    /**
     * Return the header and classifications for a specific entity.  The returned entity summary may be from
     * a full entity object or an entity proxy.
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
        final String  methodName        = "getEntitySummary";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        EntityNotKnownException    entityNotKnownException    = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    EntitySummary     entity = this.getEntitySummary(userId, guid);

                    repositoryValidator.validateEntityFromStore(repositoryName, guid, entity, methodName);

                    return entity;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedEntityNotKnownException(entityNotKnownException);

        return null;
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
        final String  methodName        = "getEntityDetail";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        EntityNotKnownException    entityNotKnownException    = null;
        EntityProxyOnlyException   entityProxyOnlyException   = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    EntityDetail     entity = this.getEntityDetail(userId, guid);

                    repositoryValidator.validateEntityFromStore(repositoryName, guid, entity, methodName);

                    return entity;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (EntityProxyOnlyException error)
                {
                    entityProxyOnlyException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedEntityNotKnownException(entityNotKnownException);
        throwCapturedEntityProxyOnlyException(entityProxyOnlyException);

        return null;
    }


    /**
     * Return a historical version of an entity - includes the header, classifications and properties of the entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @param asOfTime - the time used to determine which version of the entity that is desired.
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
        final String  methodName        = "getEntityDetail";
        final String  guidParameterName = "guid";
        final String  asOfTimeParameter = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        EntityNotKnownException         entityNotKnownException       = null;
        EntityProxyOnlyException        entityProxyOnlyException      = null;
        FunctionNotSupportedException   functionNotSupportedException = null;
        UserNotAuthorizedException      userNotAuthorizedException    = null;
        RepositoryErrorException        repositoryErrorException      = null;
        Throwable                       anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    EntityDetail     entity = this.getEntityDetail(userId, guid, asOfTime);

                    repositoryValidator.validateEntityFromStore(repositoryName, guid, entity, methodName);

                    return entity;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (EntityProxyOnlyException error)
                {
                    entityProxyOnlyException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedEntityNotKnownException(entityNotKnownException);
        throwCapturedEntityProxyOnlyException(entityProxyOnlyException);
        throwCapturedFunctionNotSupportedException(functionNotSupportedException);

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
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
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
                                                                                                    TypeErrorException,
                                                                                                    RepositoryErrorException,
                                                                                                    EntityNotKnownException,
                                                                                                    PropertyErrorException,
                                                                                                    PagingErrorException,
                                                                                                    FunctionNotSupportedException,
                                                                                                    UserNotAuthorizedException
    {
        final String  methodName        = "getRelationshipsForEntity";
        final String  guidParameterName = "entityGUID";
        final String  asOfTimeParameter = "asOfTime";
        final String  typeGUIDParameter = "relationshipTypeGUID";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, entityGUID, methodName);
        repositoryValidator.validateOptionalTypeGUID(repositoryName, typeGUIDParameter, relationshipTypeGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, Relationship> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        EntityNotKnownException       entityNotKnownException       = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        PropertyErrorException        propertyErrorException        = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<Relationship> results = metadataCollection.getRelationshipsForEntity(userId,
                                                                                              entityGUID,
                                                                                              relationshipTypeGUID,
                                                                                              fromRelationshipElement,
                                                                                              limitResultsByStatus,
                                                                                              asOfTime,
                                                                                              sequencingProperty,
                                                                                              sequencingOrder,
                                                                                              pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueRelationships(combinedResults,
                                                                  results,
                                                                  cohortConnector.getServerName(),
                                                                  cohortConnector.getMetadataCollectionId(),
                                                                  methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);
            throwCapturedEntityNotKnownException(entityNotKnownException);

            return null;
        }

        return validatedRelationshipResults(repositoryName,
                                            combinedResults,
                                            sequencingProperty,
                                            sequencingOrder,
                                            pageSize,
                                            methodName);
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
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
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
                                                                                                 TypeErrorException,
                                                                                                 RepositoryErrorException,
                                                                                                 PropertyErrorException,
                                                                                                 PagingErrorException,
                                                                                                 FunctionNotSupportedException,
                                                                                                 UserNotAuthorizedException
    {
        final String  methodName                   = "findEntitiesByProperty";
        final String  matchCriteriaParameterName   = "matchCriteria";
        final String  matchPropertiesParameterName = "matchProperties";
        final String  asOfTimeParameter            = "asOfTime";
        final String  guidParameter                = "entityTypeGUID";
        final String  pageSizeParameter            = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateOptionalTypeGUID(repositoryName, guidParameter, entityTypeGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName,
                                                  matchCriteriaParameterName,
                                                  matchPropertiesParameterName,
                                                  matchCriteria,
                                                  matchProperties,
                                                  methodName);

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, EntityDetail> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        TypeErrorException            typeErrorException            = null;
        PropertyErrorException        propertyErrorException        = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<EntityDetail> results = metadataCollection.findEntitiesByProperty(userId,
                                                                                           entityTypeGUID,
                                                                                           matchProperties,
                                                                                           matchCriteria,
                                                                                           fromEntityElement,
                                                                                           limitResultsByStatus,
                                                                                           limitResultsByClassification,
                                                                                           asOfTime,
                                                                                           sequencingProperty,
                                                                                           sequencingOrder,
                                                                                           pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueEntities(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedTypeErrorException(typeErrorException);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);

            return null;
        }

        return validatedEntityListResults(repositoryName,
                                          combinedResults,
                                          sequencingProperty,
                                          sequencingOrder,
                                          pageSize,
                                          methodName);
    }


    /**
     * Return a list of entities that have the requested type of classifications attached.
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
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
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
                                                                                                       TypeErrorException,
                                                                                                       RepositoryErrorException,
                                                                                                       ClassificationErrorException,
                                                                                                       PropertyErrorException,
                                                                                                       PagingErrorException,
                                                                                                       FunctionNotSupportedException,
                                                                                                       UserNotAuthorizedException
    {
        final String  methodName                   = "findEntitiesByClassification";
        final String  classificationParameterName  = "classificationName";
        final String  entityTypeGUIDParameterName  = "entityTypeGUID";

        final String  matchCriteriaParameterName   = "matchCriteria";
        final String  matchPropertiesParameterName = "matchClassificationProperties";
        final String  asOfTimeParameter            = "asOfTime";
        final String  pageSizeParameter            = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateOptionalTypeGUID(repositoryName, entityTypeGUIDParameterName, entityTypeGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        /*
         * Validate TypeDef
         */
        if (entityTypeGUID != null)
        {
            TypeDef entityTypeDef = repositoryHelper.getTypeDef(repositoryName,
                                                                entityTypeGUIDParameterName,
                                                                entityTypeGUID,
                                                                methodName);

            repositoryValidator.validateTypeDefForInstance(repositoryName,
                                                           entityTypeGUIDParameterName,
                                                           entityTypeDef,
                                                           methodName);

            repositoryValidator.validateClassification(repositoryName,
                                                       classificationParameterName,
                                                       classificationName,
                                                       entityTypeDef.getName(),
                                                       methodName);
        }

        repositoryValidator.validateMatchCriteria(repositoryName,
                                                  matchCriteriaParameterName,
                                                  matchPropertiesParameterName,
                                                  matchCriteria,
                                                  matchClassificationProperties,
                                                  methodName);

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, EntityDetail> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        TypeErrorException            typeErrorException            = null;
        PropertyErrorException        propertyErrorException        = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<EntityDetail> results = metadataCollection.findEntitiesByClassification(userId,
                                                                                                 entityTypeGUID,
                                                                                                 classificationName,
                                                                                                 matchClassificationProperties,
                                                                                                 matchCriteria,
                                                                                                 fromEntityElement,
                                                                                                 limitResultsByStatus,
                                                                                                 asOfTime,
                                                                                                 sequencingProperty,
                                                                                                 sequencingOrder,
                                                                                                 pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueEntities(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedTypeErrorException(typeErrorException);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);

            return null;
        }

        return validatedEntityListResults(repositoryName,
                                          combinedResults,
                                          sequencingProperty,
                                          sequencingOrder,
                                          pageSize,
                                          methodName);
    }


    /**
     * Return a list of entities whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - GUID of the type of entity to search for. Null means all types will
     *                       be searched (could be slow so not recommended).
     * @param searchCriteria - String expression contained in any of the property values within the entities
     *                       of the supplied type.
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
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> findEntitiesByPropertyValue(String                userId,
                                                          String                entityTypeGUID,
                                                          String                searchCriteria,
                                                          int                   fromEntityElement,
                                                          List<InstanceStatus>  limitResultsByStatus,
                                                          List<String>          limitResultsByClassification,
                                                          Date                  asOfTime,
                                                          String                sequencingProperty,
                                                          SequencingOrder       sequencingOrder,
                                                          int                   pageSize) throws InvalidParameterException,
                                                                                                 TypeErrorException,
                                                                                                 RepositoryErrorException,
                                                                                                 PropertyErrorException,
                                                                                                 PagingErrorException,
                                                                                                 FunctionNotSupportedException,
                                                                                                 UserNotAuthorizedException
    {
        final String  methodName = "findEntitiesByPropertyValue";
        final String  searchCriteriaParameterName = "searchCriteria";
        final String  asOfTimeParameter = "asOfTime";
        final String  guidParameter = "entityTypeGUID";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);
        repositoryValidator.validateOptionalTypeGUID(repositoryName, guidParameter, entityTypeGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, EntityDetail> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        TypeErrorException            typeErrorException            = null;
        PropertyErrorException        propertyErrorException        = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<EntityDetail> results = metadataCollection.findEntitiesByPropertyValue(userId,
                                                                                                entityTypeGUID,
                                                                                                searchCriteria,
                                                                                                fromEntityElement,
                                                                                                limitResultsByStatus,
                                                                                                limitResultsByClassification,
                                                                                                asOfTime,
                                                                                                sequencingProperty,
                                                                                                sequencingOrder,
                                                                                                pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueEntities(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedTypeErrorException(typeErrorException);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);

            return null;
        }

        return validatedEntityListResults(repositoryName,
                                          combinedResults,
                                          sequencingProperty,
                                          sequencingOrder,
                                          pageSize,
                                          methodName);
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
        final String  methodName = "isRelationshipKnown";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        UserNotAuthorizedException      userNotAuthorizedException    = null;
        RepositoryErrorException        repositoryErrorException      = null;
        Throwable                       anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    Relationship     relationship = this.isRelationshipKnown(userId, guid);

                    repositoryValidator.validateRelationshipFromStore(repositoryName, guid, relationship, methodName);

                    return relationship;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);

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
        final String  methodName = "getRelationship";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        RelationshipNotKnownException   relationshipNotKnownException = null;
        UserNotAuthorizedException      userNotAuthorizedException    = null;
        RepositoryErrorException        repositoryErrorException      = null;
        Throwable                       anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    Relationship     relationship = this.getRelationship(userId, guid);

                    repositoryValidator.validateRelationshipFromStore(repositoryName, guid, relationship, methodName);

                    return relationship;
                }
                catch (RelationshipNotKnownException error)
                {
                    relationshipNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedRelationshipNotKnownException(relationshipNotKnownException);

        return null;
    }


    /**
     * Return a historical version of a relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @param asOfTime - the time used to determine which version of the entity that is desired.
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
        final String  methodName = "getRelationship";
        final String  guidParameterName = "guid";
        final String  asOfTimeParameter = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        RelationshipNotKnownException   relationshipNotKnownException = null;
        FunctionNotSupportedException   functionNotSupportedException = null;
        UserNotAuthorizedException      userNotAuthorizedException    = null;
        RepositoryErrorException        repositoryErrorException      = null;
        Throwable                       anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    Relationship     relationship = this.getRelationship(userId, guid, asOfTime);

                    repositoryValidator.validateRelationshipFromStore(repositoryName, guid, relationship, methodName);

                    return relationship;
                }
                catch (RelationshipNotKnownException error)
                {
                    relationshipNotKnownException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedRelationshipNotKnownException(relationshipNotKnownException);
        throwCapturedFunctionNotSupportedException(functionNotSupportedException);

        return null;
    }


    /**
     * Return a list of relationships that match the requested properties by the matching criteria.   The results
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
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
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
                                                                                                      TypeErrorException,
                                                                                                      RepositoryErrorException,
                                                                                                      PropertyErrorException,
                                                                                                      PagingErrorException,
                                                                                                      FunctionNotSupportedException,
                                                                                                      UserNotAuthorizedException
    {
        final String  methodName = "findRelationshipsByProperty";
        final String  matchCriteriaParameterName = "matchCriteria";
        final String  matchPropertiesParameterName = "matchProperties";
        final String  asOfTimeParameter = "asOfTime";
        final String  guidParameter = "relationshipTypeGUID";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateOptionalTypeGUID(repositoryName, guidParameter, relationshipTypeGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName,
                                                  matchCriteriaParameterName,
                                                  matchPropertiesParameterName,
                                                  matchCriteria,
                                                  matchProperties,
                                                  methodName);

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, Relationship> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        PropertyErrorException        propertyErrorException        = null;
        TypeErrorException            typeErrorException            = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<Relationship> results = metadataCollection.findRelationshipsByProperty(userId,
                                                                                                relationshipTypeGUID,
                                                                                                matchProperties,
                                                                                                matchCriteria,
                                                                                                fromRelationshipElement,
                                                                                                limitResultsByStatus,
                                                                                                asOfTime,
                                                                                                sequencingProperty,
                                                                                                sequencingOrder,
                                                                                                pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueRelationships(combinedResults,
                                                                  results,
                                                                  cohortConnector.getServerName(),
                                                                  cohortConnector.getMetadataCollectionId(),
                                                                  methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);
            throwCapturedTypeErrorException(typeErrorException);

            return null;
        }

        return validatedRelationshipResults(repositoryName,
                                            combinedResults,
                                            sequencingProperty,
                                            sequencingOrder,
                                            pageSize,
                                            methodName);
    }


    /**
     * Return a list of relationships whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipTypeGUID - GUID of the type of entity to search for. Null means all types will
     *                       be searched (could be slow so not recommended).
     * @param searchCriteria - String expression contained in any of the property values within the entities
     *                       of the supplied type.
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
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws PropertyErrorException - there is a problem with one of the other parameters.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<Relationship> findRelationshipsByPropertyValue(String                    userId,
                                                                String                    relationshipTypeGUID,
                                                                String                    searchCriteria,
                                                                int                       fromRelationshipElement,
                                                                List<InstanceStatus>      limitResultsByStatus,
                                                                Date                      asOfTime,
                                                                String                    sequencingProperty,
                                                                SequencingOrder           sequencingOrder,
                                                                int                       pageSize) throws InvalidParameterException,
                                                                                                           TypeErrorException,
                                                                                                           RepositoryErrorException,
                                                                                                           PropertyErrorException,
                                                                                                           PagingErrorException,
                                                                                                           FunctionNotSupportedException,
                                                                                                           UserNotAuthorizedException
    {
        final String  methodName = "findRelationshipsByPropertyValue";
        final String  asOfTimeParameter = "asOfTime";
        final String  guidParameter = "relationshipTypeGUID";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validateOptionalTypeGUID(repositoryName, guidParameter, relationshipTypeGUID, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, Relationship> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        PropertyErrorException        propertyErrorException        = null;
        TypeErrorException            typeErrorException            = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<Relationship> results = metadataCollection.findRelationshipsByPropertyValue(userId,
                                                                                                     relationshipTypeGUID,
                                                                                                     searchCriteria,
                                                                                                     fromRelationshipElement,
                                                                                                     limitResultsByStatus,
                                                                                                     asOfTime,
                                                                                                     sequencingProperty,
                                                                                                     sequencingOrder,
                                                                                                     pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueRelationships(combinedResults,
                                                                  results,
                                                                  cohortConnector.getServerName(),
                                                                  cohortConnector.getMetadataCollectionId(),
                                                                  methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedTypeErrorException(typeErrorException);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);

            return null;
        }

        return validatedRelationshipResults(repositoryName,
                                            combinedResults,
                                            sequencingProperty,
                                            sequencingOrder,
                                            pageSize,
                                            methodName);
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
        final String methodName                   = "getLinkingEntities";
        final String startEntityGUIDParameterName = "startEntityGUID";
        final String endEntityGUIDParameterName   = "entityGUID";
        final String asOfTimeParameter            = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, startEntityGUIDParameterName, startEntityGUID, methodName);
        repositoryValidator.validateGUID(repositoryName, endEntityGUIDParameterName, endEntityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);


        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, EntityDetail>      combinedEntityResults       = new HashMap<>();
        HashMap<String, Relationship>      combinedRelationshipResults = new HashMap<>();

        EntityNotKnownException        entityNotKnownException         = null;
        FunctionNotSupportedException  functionNotSupportedException   = null;
        PropertyErrorException         propertyErrorException          = null;
        UserNotAuthorizedException     userNotAuthorizedException      = null;
        RepositoryErrorException       repositoryErrorException        = null;
        Throwable                      anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    InstanceGraph     results = metadataCollection.getLinkingEntities(userId,
                                                                                      startEntityGUID,
                                                                                      endEntityGUID,
                                                                                      limitResultsByStatus,
                                                                                      asOfTime);

                    /*
                     * Step through the list of returned TypeDefs and consolidate.
                     */
                    if (results != null)
                    {
                        combinedRelationshipResults = this.addUniqueRelationships(combinedRelationshipResults,
                                                                                  results.getRelationships(),
                                                                                  cohortConnector.getServerName(),
                                                                                  cohortConnector.getMetadataCollectionId(),
                                                                                  methodName);
                        combinedEntityResults = this.addUniqueEntities(combinedEntityResults,
                                                                       results.getEntities(),
                                                                       cohortConnector.getServerName(),
                                                                       cohortConnector.getMetadataCollectionId(),
                                                                       methodName);
                    }
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedInstanceGraphResults(repositoryName,
                                             combinedEntityResults,
                                             combinedRelationshipResults,
                                             userNotAuthorizedException,
                                             propertyErrorException,
                                             functionNotSupportedException,
                                             entityNotKnownException,
                                             repositoryErrorException,
                                             anotherException,
                                             methodName);
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
     * @throws TypeErrorException - one or more of the type guids passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
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
                                                                                   TypeErrorException,
                                                                                   RepositoryErrorException,
                                                                                   EntityNotKnownException,
                                                                                   PropertyErrorException,
                                                                                   FunctionNotSupportedException,
                                                                                   UserNotAuthorizedException
    {
        final String methodName                                  = "getEntityNeighborhood";
        final String entityGUIDParameterName                     = "entityGUID";
        final String entityTypeGUIDParameterName                 = "entityTypeGUIDs";
        final String relationshipTypeGUIDParameterName           = "relationshipTypeGUIDs";
        final String limitedResultsByClassificationParameterName = "limitResultsByClassification";
        final String asOfTimeParameter                           = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        if (entityTypeGUIDs != null)
        {
            for (String guid : entityTypeGUIDs)
            {
                repositoryValidator.validateTypeGUID(repositoryName, entityTypeGUIDParameterName, guid, methodName);
            }
        }

        if (relationshipTypeGUIDs != null)
        {
            for (String guid : relationshipTypeGUIDs)
            {
                repositoryValidator.validateTypeGUID(repositoryName, relationshipTypeGUIDParameterName, guid, methodName);
            }
        }

        if (limitResultsByClassification != null)
        {
            for (String classificationName : limitResultsByClassification)
            {
                repositoryValidator.validateClassificationName(repositoryName,
                                                               limitedResultsByClassificationParameterName,
                                                               classificationName,
                                                               methodName);
            }
        }

        /*
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);


        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, EntityDetail>      combinedEntityResults       = new HashMap<>();
        HashMap<String, Relationship>      combinedRelationshipResults = new HashMap<>();

        EntityNotKnownException        entityNotKnownException         = null;
        FunctionNotSupportedException  functionNotSupportedException   = null;
        PropertyErrorException         propertyErrorException          = null;
        UserNotAuthorizedException     userNotAuthorizedException      = null;
        RepositoryErrorException       repositoryErrorException        = null;
        Throwable                      anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    InstanceGraph     results = metadataCollection.getEntityNeighborhood(userId,
                                                                                         entityGUID,
                                                                                         entityTypeGUIDs,
                                                                                         relationshipTypeGUIDs,
                                                                                         limitResultsByStatus,
                                                                                         limitResultsByClassification,
                                                                                         asOfTime,
                                                                                         level);

                    /*
                     * Step through the list of returned TypeDefs and consolidate.
                     */
                    if (results != null)
                    {
                        combinedRelationshipResults = this.addUniqueRelationships(combinedRelationshipResults,
                                                                                  results.getRelationships(),
                                                                                  cohortConnector.getServerName(),
                                                                                  cohortConnector.getMetadataCollectionId(),
                                                                                  methodName);
                        combinedEntityResults = this.addUniqueEntities(combinedEntityResults,
                                                                       results.getEntities(),
                                                                       cohortConnector.getServerName(),
                                                                       cohortConnector.getMetadataCollectionId(),
                                                                       methodName);
                    }
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        return validatedInstanceGraphResults(repositoryName,
                                             combinedEntityResults,
                                             combinedRelationshipResults,
                                             userNotAuthorizedException,
                                             propertyErrorException,
                                             functionNotSupportedException,
                                             entityNotKnownException,
                                             repositoryErrorException,
                                             anotherException,
                                             methodName);
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
     * @throws TypeErrorException - one or more of the type guids passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
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
                                                                                        TypeErrorException,
                                                                                        RepositoryErrorException,
                                                                                        EntityNotKnownException,
                                                                                        PropertyErrorException,
                                                                                        PagingErrorException,
                                                                                        FunctionNotSupportedException,
                                                                                        UserNotAuthorizedException
    {
        final String  methodName = "getRelatedEntities";
        final String  entityGUIDParameterName  = "startEntityGUID";
        final String  typeGUIDParameterName  = "instanceTypes";
        final String  asOfTimeParameter = "asOfTime";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, startEntityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        if (instanceTypes != null)
        {
            for (String guid : instanceTypes)
            {
                repositoryValidator.validateTypeGUID(repositoryName, typeGUIDParameterName, guid, methodName);
            }
        }

        /*
         * Perform operation
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Search results need to come from all members of the cohort.
         * They need to be combined and then duplicates removed to create the final list of results.
         * Some repositories may produce exceptions.  These exceptions are saved and one selected to
         * be returned if there are no results from any repository.
         */
        HashMap<String, EntityDetail> combinedResults               = new HashMap<>();

        InvalidParameterException     invalidParameterException     = null;
        EntityNotKnownException       entityNotKnownException       = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        TypeErrorException            typeErrorException            = null;
        PropertyErrorException        propertyErrorException        = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request
                     */
                    List<EntityDetail> results = metadataCollection.getRelatedEntities(userId,
                                                                                       startEntityGUID,
                                                                                       instanceTypes,
                                                                                       fromEntityElement,
                                                                                       limitResultsByStatus,
                                                                                       limitResultsByClassification,
                                                                                       asOfTime,
                                                                                       sequencingProperty,
                                                                                       sequencingOrder,
                                                                                       pageSize);

                    /*
                     * Step through the list of returned TypeDefs and remove duplicates.
                     */
                    combinedResults = this.addUniqueEntities(combinedResults,
                                                             results,
                                                             cohortConnector.getServerName(),
                                                             cohortConnector.getMetadataCollectionId(),
                                                             methodName);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }


        if (combinedResults.isEmpty())
        {
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedTypeErrorException(typeErrorException);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedInvalidParameterException(invalidParameterException);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);
            throwCapturedEntityNotKnownException(entityNotKnownException);

            return null;
        }

        return validatedEntityListResults(repositoryName,
                                          combinedResults,
                                          sequencingProperty,
                                          sequencingOrder,
                                          pageSize,
                                          methodName);
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
        final String  methodName                    = "addEntity";
        final String  entityGUIDParameterName       = "entityTypeGUID";
        final String  propertiesParameterName       = "initialProperties";
        final String  classificationsParameterName  = "initialClassifications";
        final String  initialStatusParameterName    = "initialStatus";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeGUID(repositoryName, entityGUIDParameterName, entityTypeGUID, methodName);

        TypeDef  typeDef = repositoryHelper.getTypeDef(repositoryName, entityGUIDParameterName, entityTypeGUID, methodName);

        repositoryValidator.validateTypeDefForInstance(repositoryName, entityGUIDParameterName, typeDef, methodName);
        repositoryValidator.validateClassificationList(repositoryName,
                                                       classificationsParameterName,
                                                       initialClassifications,
                                                       typeDef.getName(),
                                                       methodName);

        repositoryValidator.validatePropertiesForType(repositoryName,
                                                      propertiesParameterName,
                                                      typeDef,
                                                      initialProperties,
                                                      methodName);

        repositoryValidator.validateInstanceStatus(repositoryName,
                                                   initialStatusParameterName,
                                                   initialStatus,
                                                   typeDef,
                                                   methodName);

        /*
         * Validation complete - ok to create new instance
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Create requests occur in the first repository that accepts the call.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no positive results from any repository.
         */
        InvalidParameterException    invalidParameterException    = null;
        TypeErrorException           typeErrorException           = null;
        PropertyErrorException       propertyErrorException       = null;
        ClassificationErrorException classificationErrorException = null;
        StatusNotSupportedException  statusNotSupportedException  = null;
        UserNotAuthorizedException   userNotAuthorizedException   = null;
        RepositoryErrorException     repositoryErrorException     = null;
        Throwable                    anotherException             = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.addEntity(userId,
                                                        entityTypeGUID,
                                                        initialProperties,
                                                        initialClassifications,
                                                        initialStatus);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (ClassificationErrorException error)
                {
                    classificationErrorException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (StatusNotSupportedException error)
                {
                    statusNotSupportedException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeErrorException(typeErrorException);
        throwCapturedClassificationErrorException(classificationErrorException);
        throwCapturedPropertyErrorException(propertyErrorException);
        throwCapturedStatusNotSupportedException(statusNotSupportedException);
        throwCapturedInvalidParameterException(invalidParameterException);

        return null;
    }


    /**
     * Create an entity proxy in the metadata collection.  This is used to store relationships that span metadata
     * repositories.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityProxy - details of entity to add.
     * @throws FunctionNotSupportedException - the repository does not support entity proxies as first class elements.
     */
    public void addEntityProxy(String       userId,
                               EntityProxy  entityProxy) throws FunctionNotSupportedException
    {
        final String  methodName         = "addEntityProxy";

        throwNotEnterpriseFunction(methodName);
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
        final String  methodName               = "updateEntityStatus";
        final String  entityGUIDParameterName  = "entityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String         errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(methodName, entityGUID, entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().updateEntityStatus(userId, entityGUID, newStatus);
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
        final String  methodName = "updateEntityProperties";
        final String  entityGUIDParameterName  = "entityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     entityGUID,
                                                                                                     entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().updateEntityProperties(userId, entityGUID, properties);
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
        final String  methodName = "undoEntityUpdate";
        final String  entityGUIDParameterName  = "entityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     entityGUID,
                                                                                                     entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().undoEntityUpdate(userId, entityGUID);
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
        final String  methodName               = "deleteEntity";
        final String  typeDefGUIDParameterName = "typeDefGUID";
        final String  typeDefNameParameterName = "typeDefName";
        final String  entityGUIDParameterName  = "obsoleteEntityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                                               typeDefGUIDParameterName,
                                               typeDefNameParameterName,
                                               typeDefGUID,
                                               typeDefName,
                                               methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, obsoleteEntityGUID, methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, obsoleteEntityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, obsoleteEntityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     obsoleteEntityGUID,
                                                                                                     entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().deleteEntity(userId, typeDefGUID, typeDefName, obsoleteEntityGUID);
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
        final String  methodName               = "purgeEntity";
        final String  typeDefGUIDParameterName = "typeDefGUID";
        final String  typeDefNameParameterName = "typeDefName";
        final String  entityGUIDParameterName  = "deletedEntityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                                               typeDefGUIDParameterName,
                                               typeDefNameParameterName,
                                               typeDefGUID,
                                               typeDefName,
                                               methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, deletedEntityGUID, methodName);

        /*
         * Validation complete - ok to purge the instance
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Purge requests occur in the first repository that accepts the call.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no positive results from any repository.
         */
        InvalidParameterException  invalidParameterException  = null;
        EntityNotKnownException    entityNotKnownException    = null;
        EntityNotDeletedException  entityNotDeletedException  = null;
        UserNotAuthorizedException userNotAuthorizedException = null;
        RepositoryErrorException   repositoryErrorException   = null;
        Throwable                  anotherException           = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    metadataCollection.purgeEntity(userId, typeDefGUID, typeDefName, deletedEntityGUID);
                    return;
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (EntityNotDeletedException error)
                {
                    entityNotDeletedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedEntityNotDeletedException(entityNotDeletedException);
        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedEntityNotKnownException(entityNotKnownException);
        throwCapturedInvalidParameterException(invalidParameterException);

        return;
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
        final String  methodName              = "restoreEntity";
        final String  entityGUIDParameterName = "deletedEntityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, deletedEntityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail  entity  = this.isEntityKnown(userId, deletedEntityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, deletedEntityGUID, entity, methodName);

        repositoryValidator.validateEntityIsDeleted(repositoryName, entity, methodName);

        /*
         * Validation is complete.  It is ok to restore the entity.
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Restore requests occur in the first repository that accepts the call.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no positive results from any repository.
         */
        InvalidParameterException     invalidParameterException     = null;
        EntityNotKnownException       entityNotKnownException       = null;
        EntityNotDeletedException     entityNotDeletedException     = null;
        UserNotAuthorizedException    userNotAuthorizedException    = null;
        RepositoryErrorException      repositoryErrorException      = null;
        FunctionNotSupportedException functionNotSupportedException = null;
        Throwable                     anotherException              = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.restoreEntity(userId, deletedEntityGUID);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (EntityNotDeletedException error)
                {
                    entityNotDeletedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedEntityNotDeletedException(entityNotDeletedException);
        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedEntityNotKnownException(entityNotKnownException);
        throwCapturedInvalidParameterException(invalidParameterException);
        throwCapturedFunctionNotSupportedException(functionNotSupportedException);

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
        final String  methodName                  = "classifyEntity";
        final String  entityGUIDParameterName     = "entityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     entityGUID,
                                                                                                     entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().classifyEntity(userId,
                                                                              entityGUID,
                                                                              classificationName,
                                                                              classificationProperties);
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
        final String  methodName                  = "declassifyEntity";
        final String  entityGUIDParameterName     = "entityGUID";
        final String  classificationParameterName = "classificationName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
        repositoryValidator.validateClassificationName(repositoryName,
                                                       classificationParameterName,
                                                       classificationName,
                                                       methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     entityGUID,
                                                                                                     entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().declassifyEntity(userId, entityGUID, classificationName);
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
        final String  methodName = "updateEntityClassification";
        final String  entityGUIDParameterName     = "entityGUID";
        final String  classificationParameterName = "classificationName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
        repositoryValidator.validateClassificationName(repositoryName, classificationParameterName, classificationName, methodName);

        /*
         * Locate entity
         */

        EntitySummary     entity = this.getEntitySummary(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(entity, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     entityGUID,
                                                                                                     entity.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().updateEntityClassification(userId,
                                                                                          entityGUID,
                                                                                          classificationName,
                                                                                          properties);
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
        final String  methodName = "addRelationship";
        final String  guidParameterName = "relationshipTypeGUID";
        final String  propertiesParameterName       = "initialProperties";
        final String  initialStatusParameterName    = "initialStatus";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeGUID(repositoryName, guidParameterName, relationshipTypeGUID, methodName);

        TypeDef  typeDef = repositoryHelper.getTypeDef(repositoryName, guidParameterName, relationshipTypeGUID, methodName);

        repositoryValidator.validateTypeDefForInstance(repositoryName, guidParameterName, typeDef, methodName);


        repositoryValidator.validatePropertiesForType(repositoryName,
                                                      propertiesParameterName,
                                                      typeDef,
                                                      initialProperties,
                                                      methodName);

        repositoryValidator.validateInstanceStatus(repositoryName,
                                                   initialStatusParameterName,
                                                   initialStatus,
                                                   typeDef,
                                                   methodName);

        /*
         * Validation complete - ok to create new instance
         */
        /*
         * Validation complete - ok to create new instance
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Create requests occur in the first repository that accepts the call.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no positive results from any repository.
         */
        InvalidParameterException    invalidParameterException    = null;
        EntityNotKnownException      entityNotKnownException      = null;
        TypeErrorException           typeErrorException           = null;
        PropertyErrorException       propertyErrorException       = null;
        StatusNotSupportedException  statusNotSupportedException  = null;
        UserNotAuthorizedException   userNotAuthorizedException   = null;
        RepositoryErrorException     repositoryErrorException     = null;
        Throwable                    anotherException             = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.addRelationship(userId,
                                                              relationshipTypeGUID,
                                                              initialProperties,
                                                              entityOneGUID,
                                                              entityTwoGUID,
                                                              initialStatus);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (EntityNotKnownException error)
                {
                    entityNotKnownException = error;
                }
                catch (TypeErrorException error)
                {
                    typeErrorException = error;
                }
                catch (StatusNotSupportedException error)
                {
                    statusNotSupportedException = error;
                }
                catch (PropertyErrorException error)
                {
                    propertyErrorException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedEntityNotKnownException(entityNotKnownException);
        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedTypeErrorException(typeErrorException);
        throwCapturedPropertyErrorException(propertyErrorException);
        throwCapturedStatusNotSupportedException(statusNotSupportedException);
        throwCapturedInvalidParameterException(invalidParameterException);

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
        final String  methodName          = "updateRelationshipStatus";
        final String  guidParameterName   = "relationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship = this.getRelationship(userId, relationshipGUID);

        repositoryValidator.validateInstanceType(repositoryName, relationship);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(relationship, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     relationshipGUID,
                                                                                                     relationship.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().updateRelationshipStatus(userId, relationshipGUID, newStatus);
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
        final String  methodName = "updateRelationshipProperties";
        final String  guidParameterName = "relationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship = this.getRelationship(userId, relationshipGUID);

        repositoryValidator.validateInstanceType(repositoryName, relationship);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(relationship, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     relationshipGUID,
                                                                                                     relationship.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().updateRelationshipProperties(userId, relationshipGUID, properties);
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
        final String  methodName = "undoRelationshipUpdate";
        final String  guidParameterName = "relationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship = this.getRelationship(userId, relationshipGUID);

        repositoryValidator.validateInstanceType(repositoryName, relationship);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(relationship, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     relationshipGUID,
                                                                                                     relationship.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        return homeRepositoryConnector.getMetadataCollection().undoRelationshipUpdate(userId, relationshipGUID);
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
        final String  methodName = "deleteRelationship";
        final String  guidParameterName = "typeDefGUID";
        final String  nameParameterName = "typeDefName";
        final String  relationshipParameterName = "obsoleteRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, obsoleteRelationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                                               guidParameterName,
                                               nameParameterName,
                                               typeDefGUID,
                                               typeDefName,
                                               methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, obsoleteRelationshipGUID);

        repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                                                          typeDefGUID,
                                                          typeDefName,
                                                          relationship,
                                                          methodName);

        /*
         * Validation complete - ok to make changes
         */
        OMRSRepositoryConnector homeRepositoryConnector = enterpriseParentConnector.getHomeConnector(relationship, methodName);

        if (homeRepositoryConnector == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NO_HOME_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     obsoleteRelationshipGUID,
                                                                                                     relationship.getMetadataCollectionId());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

        /*
         * A delete is a soft-delete that updates the status to DELETED.
         */
        return homeRepositoryConnector.getMetadataCollection().deleteRelationship(userId,
                                                                                  typeDefGUID,
                                                                                  typeDefName,
                                                                                  obsoleteRelationshipGUID);
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
        final String  methodName = "purgeRelationship";
        final String  guidParameterName = "typeDefGUID";
        final String  nameParameterName = "typeDefName";
        final String  relationshipParameterName = "deletedRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, deletedRelationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                                               guidParameterName,
                                               nameParameterName,
                                               typeDefGUID,
                                               typeDefName,
                                               methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, deletedRelationshipGUID);

        repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                                                          typeDefGUID,
                                                          typeDefName,
                                                          relationship,
                                                          methodName);

        repositoryValidator.validateRelationshipIsDeleted(repositoryName, relationship, methodName);


        /*
         * Validation is complete.  It is ok to purge the relationship.
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Restore requests occur in the first repository that accepts the call.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no positive results from any repository.
         */
        InvalidParameterException       invalidParameterException       = null;
        RelationshipNotKnownException   relationshipNotKnownException   = null;
        RelationshipNotDeletedException relationshipNotDeletedException = null;
        UserNotAuthorizedException      userNotAuthorizedException      = null;
        RepositoryErrorException        repositoryErrorException        = null;
        Throwable                       anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    metadataCollection.purgeRelationship(userId, typeDefGUID, typeDefName, deletedRelationshipGUID);
                    return;
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (RelationshipNotKnownException error)
                {
                    relationshipNotKnownException = error;
                }
                catch (RelationshipNotDeletedException error)
                {
                    relationshipNotDeletedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRelationshipNotDeletedException(relationshipNotDeletedException);
        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedRelationshipNotKnownException(relationshipNotKnownException);
        throwCapturedInvalidParameterException(invalidParameterException);

        return;
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
        final String  methodName = "restoreRelationship";
        final String  guidParameterName = "deletedRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, deletedRelationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, deletedRelationshipGUID);

        repositoryValidator.validateRelationshipIsDeleted(repositoryName, relationship, methodName);

        /*
         * Validation is complete.  It is ok to restore the relationship.
         *
         * The list of cohort connectors are retrieved for each request to ensure that any changes in
         * the shape of the cohort are reflected immediately.
         */
        List<OMRSRepositoryConnector> cohortConnectors = enterpriseParentConnector.getCohortConnectors(methodName);

        /*
         * Ready to process the request.  Restore requests occur in the first repository that accepts the call.
         * Some repositories may produce exceptions.  These exceptions are saved and will be returned if
         * there are no positive results from any repository.
         */
        InvalidParameterException       invalidParameterException       = null;
        RelationshipNotKnownException   relationshipNotKnownException   = null;
        RelationshipNotDeletedException relationshipNotDeletedException = null;
        UserNotAuthorizedException      userNotAuthorizedException      = null;
        RepositoryErrorException        repositoryErrorException        = null;
        FunctionNotSupportedException   functionNotSupportedException   = null;
        Throwable                       anotherException                = null;

        /*
         * Loop through the metadata collections extracting the typedefs from each repository.
         */
        for (OMRSRepositoryConnector cohortConnector : cohortConnectors)
        {
            if (cohortConnector != null)
            {
                OMRSMetadataCollection   metadataCollection = cohortConnector.getMetadataCollection();

                validateMetadataCollection(metadataCollection, methodName);

                try
                {
                    /*
                     * Issue the request and return if it succeeds
                     */
                    return metadataCollection.restoreRelationship(userId, deletedRelationshipGUID);
                }
                catch (InvalidParameterException error)
                {
                    invalidParameterException = error;
                }
                catch (FunctionNotSupportedException error)
                {
                    functionNotSupportedException = error;
                }
                catch (RelationshipNotKnownException error)
                {
                    relationshipNotKnownException = error;
                }
                catch (RelationshipNotDeletedException error)
                {
                    relationshipNotDeletedException = error;
                }
                catch (RepositoryErrorException error)
                {
                    repositoryErrorException = error;
                }
                catch (UserNotAuthorizedException error)
                {
                    userNotAuthorizedException = error;
                }
                catch (Throwable error)
                {
                    anotherException = error;
                }
            }
        }

        throwCapturedRelationshipNotDeletedException(relationshipNotDeletedException);
        throwCapturedRepositoryErrorException(repositoryErrorException);
        throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
        throwCapturedThrowableException(anotherException, methodName);
        throwCapturedRelationshipNotKnownException(relationshipNotKnownException);
        throwCapturedInvalidParameterException(invalidParameterException);
        throwCapturedFunctionNotSupportedException(functionNotSupportedException);

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
     * @param userId - unique identifier for requesting server.
     * @param entity - details of the entity to save
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void saveEntityReferenceCopy(String userId,
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
     * @param userId - unique identifier for requesting server.
     * @param entityGUID - the unique identifier for the entity.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void purgeEntityReferenceCopy(String userId,
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
     * @param userId - unique identifier for requesting server.
     * @param entityGUID - unique identifier of requested entity
     * @param typeDefGUID - unique identifier of requested entity's TypeDef
     * @param typeDefName - unique name of requested entity's TypeDef
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void refreshEntityReferenceCopy(String userId,
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
     * @param userId - unique identifier for requesting server.
     * @param relationship - relationship to save
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void saveRelationshipReferenceCopy(String userId,
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
     * @param userId - unique identifier for requesting server.
     * @param relationshipGUID - the unique identifier for the relationship.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void purgeRelationshipReferenceCopy(String userId,
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
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - unique identifier of the relationship
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws FunctionNotSupportedException - the repository does not support reference copies of instances.
     */
    public void refreshRelationshipReferenceCopy(String userId,
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
     * Build a combined list of AttributeTypeDefs.
     *
     * @param accumulatedResults - current accumulated AttributeTypeDefs
     * @param newResults - newly received AttributeTypeDefs
     * @param serverName - name of the server that provided the new AttributeTypeDefs
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new AttributeTypeDefs
     * @param methodName - method name that returned the new AttributeTypeDefs
     * @return combined results
     */
    private HashMap<String, AttributeTypeDef>   addUniqueAttributeTypeDefs(HashMap<String, AttributeTypeDef>   accumulatedResults,
                                                                           List<AttributeTypeDef>              newResults,
                                                                           String                              serverName,
                                                                           String                              metadataCollectionId,
                                                                           String                              methodName)
    {
        HashMap<String, AttributeTypeDef>  combinedResults = new HashMap<>(accumulatedResults);

        if (newResults != null)
        {
            for (AttributeTypeDef   attributeTypeDef : newResults)
            {
                if (attributeTypeDef != null)
                {
                    combinedResults = addUniqueAttributeTypeDef(combinedResults,
                                                                attributeTypeDef,
                                                                serverName,
                                                                metadataCollectionId,
                                                                methodName);
                }
            }
        }

        return combinedResults;
    }


    /**
     * Build a combined list of AttributeTypeDefs.
     *
     * @param accumulatedResults - current accumulated AttributeTypeDefs
     * @param attributeTypeDef - newly received AttributeTypeDef
     * @param serverName - name of the server that provided the new AttributeTypeDef
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new AttributeTypeDef
     * @param methodName - method name that returned the new AttributeTypeDef
     * @return combined results
     */
    private HashMap<String, AttributeTypeDef>   addUniqueAttributeTypeDef(HashMap<String, AttributeTypeDef>   accumulatedResults,
                                                                          AttributeTypeDef                    attributeTypeDef,
                                                                          String                              serverName,
                                                                          String                              metadataCollectionId,
                                                                          String                              methodName)
    {
        HashMap<String, AttributeTypeDef>  combinedResults = new HashMap<>(accumulatedResults);

        if (attributeTypeDef != null)
        {
            AttributeTypeDef   existingAttributeTypeDef = combinedResults.put(attributeTypeDef.getGUID(), attributeTypeDef);

            // todo validate that existing attributeTypeDef and the new one are copies

        }

        return combinedResults;
    }


    /**
     * Build a combined list of TypeDefs.
     *
     * @param accumulatedResults - current accumulated TypeDefs
     * @param returnedTypeDefs - newly received TypeDefs
     * @param serverName - name of the server that provided the new TypeDefs
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new TypeDefs
     * @param methodName - method name that returned the new TypeDefs
     * @return combined results
     */
    private HashMap<String, TypeDef> addUniqueTypeDefs(HashMap<String, TypeDef> accumulatedResults,
                                                       List<TypeDef>            returnedTypeDefs,
                                                       String                   serverName,
                                                       String                   metadataCollectionId,
                                                       String                   methodName)
    {
        HashMap<String, TypeDef>  combinedResults = new HashMap<>(accumulatedResults);

        if (returnedTypeDefs != null)
        {
            for (TypeDef returnedTypeDef : returnedTypeDefs)
            {
                combinedResults = this.addUniqueTypeDef(combinedResults,
                                                        returnedTypeDef,
                                                        serverName,
                                                        metadataCollectionId,
                                                        methodName);
            }
        }

        return combinedResults;
    }

    /**
     * Build a combined list of TypeDefs.
     *
     * @param accumulatedResults - current accumulated TypeDefs
     * @param typeDef - newly received TypeDef
     * @param serverName - name of the server that provided the new TypeDef
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new TypeDef
     * @param methodName - method name that returned the new TypeDef
     * @return combined results
     */
    private HashMap<String, TypeDef>   addUniqueTypeDef(HashMap<String, TypeDef>   accumulatedResults,
                                                        TypeDef                    typeDef,
                                                        String                     serverName,
                                                        String                     metadataCollectionId,
                                                        String                     methodName)
    {
        HashMap<String, TypeDef>  combinedResults = new HashMap<>(accumulatedResults);

        if (typeDef != null)
        {
            TypeDef   existingTypeDef = combinedResults.put(typeDef.getGUID(), typeDef);

            // todo validate that existing typeDef and the new one are copies
        }

        return combinedResults;
    }


    /**
     * Build a combined list of entities.
     *
     * @param accumulatedResults - current accumulated entities
     * @param results - newly received list of entities
     * @param serverName - name of the server that provided the new entity
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new entity
     * @param methodName - method name that returned the new entity
     * @return combined results
     */
    private HashMap<String, EntityDetail> addUniqueEntities(HashMap<String, EntityDetail> accumulatedResults,
                                                            List<EntityDetail>            results,
                                                            String                        serverName,
                                                            String                        metadataCollectionId,
                                                            String                        methodName)
    {
        HashMap<String, EntityDetail>  combinedResults = new HashMap<>(accumulatedResults);

        if (results != null)
        {
            for (EntityDetail returnedEntity : results)
            {
                combinedResults = this.addUniqueEntity(combinedResults,
                                                       returnedEntity,
                                                       serverName,
                                                       metadataCollectionId,
                                                       methodName);
            }
        }

        return combinedResults;
    }


    /**
     * Build a combined list of entities.
     *
     * @param accumulatedResults - current accumulated entities
     * @param entity - newly received entity
     * @param serverName - name of the server that provided the new entity
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new entity
     * @param methodName - method name that returned the new entity
     * @return combined results
     */
    private HashMap<String, EntityDetail> addUniqueEntity(HashMap<String, EntityDetail> accumulatedResults,
                                                          EntityDetail                  entity,
                                                          String                        serverName,
                                                          String                        metadataCollectionId,
                                                          String                        methodName)
    {
        HashMap<String, EntityDetail>  combinedResults = new HashMap<>(accumulatedResults);

        if (entity != null)
        {
            EntityDetail   existingEntity = combinedResults.put(entity.getGUID(), entity);

            // todo validate that existing entity and the new one are copies
        }

        return combinedResults;
    }


    /**
     * Build a combined list of relationships.
     *
     * @param accumulatedResults - current accumulated relationships
     * @param results - newly received list of relationships
     * @param serverName - name of the server that provided the new relationship
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new relationship
     * @param methodName - method name that returned the new relationship
     * @return combined results
     */
    private HashMap<String, Relationship> addUniqueRelationships(HashMap<String, Relationship> accumulatedResults,
                                                                 List<Relationship>            results,
                                                                 String                        serverName,
                                                                 String                        metadataCollectionId,
                                                                 String                        methodName)
    {
        HashMap<String, Relationship>  combinedResults = new HashMap<>(accumulatedResults);

        if (results != null)
        {
            for (Relationship returnedRelationship : results)
            {
                combinedResults = this.addUniqueRelationship(combinedResults,
                                                             returnedRelationship,
                                                             serverName,
                                                             metadataCollectionId,
                                                             methodName);
            }
        }

        return combinedResults;
    }


    /**
     * Build a combined list of relationships.
     *
     * @param accumulatedResults - current accumulated relationships
     * @param relationship - newly received relationship
     * @param serverName - name of the server that provided the new relationship
     * @param metadataCollectionId - unique identifier for metadata collection that provided the new relationship
     * @param methodName - method name that returned the new relationship
     * @return combined results
     */
    private HashMap<String, Relationship> addUniqueRelationship(HashMap<String, Relationship> accumulatedResults,
                                                                Relationship                  relationship,
                                                                String                        serverName,
                                                                String                        metadataCollectionId,
                                                                String                        methodName)
    {
        HashMap<String, Relationship>  combinedResults = new HashMap<>(accumulatedResults);

        if (relationship != null)
        {
            Relationship   existingRelationship = combinedResults.put(relationship.getGUID(), relationship);

            // todo validate that existing relationship and the new one are copies
        }

        return combinedResults;
    }


    /**
     * Verify that a cohort member's metadata collection is not null.
     *
     * @param cohortMetadataCollection - metadata collection
     * @param methodName - name of method
     * @throws RepositoryErrorException - null metadata collection
     */
    private void validateMetadataCollection(OMRSMetadataCollection  cohortMetadataCollection,
                                            String                  methodName) throws RepositoryErrorException
    {
        /*
         * The cohort metadata collection should not be null - in a real mess if this fails.
         */
        if (cohortMetadataCollection == null)
        {
            /*
             * A problem in the set up of the metadata collection list.  Repository connectors implemented
             * with no metadata collection are tested for in the OMRSEnterpriseConnectorManager so something
             * else has gone wrong.
             */
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_ENTERPRISE_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }
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


    /**
     * Throw a TypeDefNotKnownException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws TypeDefNotKnownException - the type definition is not known in any of the federated repositories
     */
    private void throwCapturedTypeDefNotKnownException(TypeDefNotKnownException   exception) throws TypeDefNotKnownException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a InvalidParameterException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws InvalidParameterException - one of the parameters is invalid
     */
    private void throwCapturedInvalidParameterException(InvalidParameterException   exception) throws InvalidParameterException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a TypeErrorException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws TypeErrorException - the type definition of the instance is not known in any of the federated repositories
     */
    private void throwCapturedTypeErrorException(TypeErrorException   exception) throws TypeErrorException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a StatusNotSupportedException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws StatusNotSupportedException - the status is not known in any of the federated repositories
     */
    private void throwCapturedStatusNotSupportedException(StatusNotSupportedException   exception) throws StatusNotSupportedException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a ClassificationErrorException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws ClassificationErrorException - the classification is not known
     */
    private void throwCapturedClassificationErrorException(ClassificationErrorException   exception) throws ClassificationErrorException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a PropertyErrorException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws PropertyErrorException - the properties are not valid for the call
     */
    private void throwCapturedPropertyErrorException(PropertyErrorException   exception) throws PropertyErrorException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a EntityNotKnownException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws EntityNotKnownException - the entity is not known in any of the federated repositories
     */
    private void throwCapturedEntityNotKnownException(EntityNotKnownException   exception) throws EntityNotKnownException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a EntityNotDeletedException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws EntityNotDeletedException - the entity is not in deleted status
     */
    private void throwCapturedEntityNotDeletedException(EntityNotDeletedException   exception) throws EntityNotDeletedException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a EntityNotKnownException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws EntityProxyOnlyException - only entity proxies have been found in the available federated repositories
     */
    private void throwCapturedEntityProxyOnlyException(EntityProxyOnlyException   exception) throws EntityProxyOnlyException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a RelationshipNotKnownException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws RelationshipNotKnownException - the relationship is not known in any of the federated repositories
     */
    private void throwCapturedRelationshipNotKnownException(RelationshipNotKnownException   exception) throws RelationshipNotKnownException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a RelationshipNotDeletedException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws RelationshipNotDeletedException - the relationship is not in deleted status
     */
    private void throwCapturedRelationshipNotDeletedException(RelationshipNotDeletedException   exception) throws RelationshipNotDeletedException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a UserNotAuthorizedException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws TypeDefNotSupportedException - the type definition is not supported in any of the federated repositories
     */
    private void throwCapturedTypeDefNotSupportedException(TypeDefNotSupportedException   exception) throws TypeDefNotSupportedException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a FunctionNotSupportedException if it was returned by all of the calls to the cohort connectors.
     *
     * @param exception - captured exception
     * @throws FunctionNotSupportedException - the requested function is not supported in any of the federated repositories
     */
    private void throwCapturedFunctionNotSupportedException(FunctionNotSupportedException   exception) throws FunctionNotSupportedException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a UserNotAuthorizedException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws UserNotAuthorizedException - the userId is not authorized in the server
     */
    private void throwCapturedUserNotAuthorizedException(UserNotAuthorizedException   exception) throws UserNotAuthorizedException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a RepositoryErrorException if it was returned by one of the calls to a cohort connector.
     *
     * @param exception - captured exception
     * @throws RepositoryErrorException - there was an error in the repository
     */
    private void throwCapturedRepositoryErrorException(RepositoryErrorException   exception) throws RepositoryErrorException
    {
        if (exception != null)
        {
            throw exception;
        }
    }


    /**
     * Throw a RepositoryErrorException if an unexpected Throwable exception was returned by one of the calls
     * to a cohort connector.
     *
     * @param exception - captured exception
     * @throws RepositoryErrorException - there was an unexpected error in the repository
     */
    private void throwCapturedThrowableException(Throwable   exception,
                                                 String      methodName) throws RepositoryErrorException
    {
        if (exception != null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.UNEXPECTED_EXCEPTION_FROM_COHORT;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     exception.getMessage());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
    }


    /**
     * Take the results of the federated connectors and combine them into a valid TypeDef gallery or an
     * exception.
     *
     * @param repositoryName - the name of this repository
     * @param combinedTypeDefResults - TypeDefs returned by the federated connectors
     * @param combinedAttributeTypeDefResults - AttributeTypeDefs returned by the federated connectors
     * @param userNotAuthorizedException - captured user not authorized exception
     * @param repositoryErrorException - captured repository error exception
     * @param anotherException - captured Throwable
     * @param methodName - name of the method issuing the request
     * @return TypeDefGallery
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    private TypeDefGallery validatedTypeDefGalleryResults(String                                 repositoryName,
                                                          HashMap<String, TypeDef>               combinedTypeDefResults,
                                                          HashMap<String, AttributeTypeDef>      combinedAttributeTypeDefResults,
                                                          UserNotAuthorizedException             userNotAuthorizedException,
                                                          RepositoryErrorException               repositoryErrorException,
                                                          Throwable                              anotherException,
                                                          String                                 methodName) throws RepositoryErrorException,
                                                                                                                    UserNotAuthorizedException
    {
        int  resultCount = (combinedTypeDefResults.size() + combinedAttributeTypeDefResults.size());

        /*
         * Return any results, or exception if nothing is found.
         */
        if (resultCount > 0)
        {
            TypeDefGallery         typeDefGallery    = new TypeDefGallery();
            List<AttributeTypeDef> attributeTypeDefs = new ArrayList<>(combinedAttributeTypeDefResults.values());
            List<TypeDef>          typeDefs          = new ArrayList<>(combinedTypeDefResults.values());

            repositoryValidator.validateEnterpriseTypeDefs(repositoryName, typeDefs, methodName);

            typeDefGallery.setAttributeTypeDefs(attributeTypeDefs);
            typeDefGallery.setTypeDefs(typeDefs);

            return typeDefGallery;
        }
        else
        {
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedThrowableException(anotherException, methodName);

            /*
             * Nothing went wrong - there are just no results.
             */
            return null;
        }
    }


    /**
     * Take the results of the federated connectors and combine them into a valid TypeDef list or an
     * exception.
     *
     * @param repositoryName - the name of this repository
     * @param combinedTypeDefResults - TypeDefs returned by the federated connectors
     * @param userNotAuthorizedException - captured user not authorized exception
     * @param repositoryErrorException - captured repository error exception
     * @param anotherException - captured Throwable
     * @param methodName - name of the method issuing the request
     * @return list of TypeDefs
     * @throws RepositoryErrorException - problem in one of the federated connectors
     * @throws UserNotAuthorizedException - user not recognized or not granted access to the TypeDefs
     */
    private List<TypeDef> validatedTypeDefListResults(String                        repositoryName,
                                                      HashMap<String, TypeDef>      combinedTypeDefResults,
                                                      UserNotAuthorizedException    userNotAuthorizedException,
                                                      RepositoryErrorException      repositoryErrorException,
                                                      Throwable                     anotherException,
                                                      String                        methodName) throws RepositoryErrorException,
                                                                                                       UserNotAuthorizedException
    {
        /*
         * Return any results, or null if nothing is found.
         */
        if (! combinedTypeDefResults.isEmpty())
        {
            List<TypeDef>    typeDefs = new ArrayList<>(combinedTypeDefResults.values());

            repositoryValidator.validateEnterpriseTypeDefs(repositoryName, typeDefs, methodName);

            return typeDefs;
        }
        else
        {
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedThrowableException(anotherException, methodName);

            /*
             * Nothing went wrong - there are just no results.
             */
            return null;
        }
    }


    /**
     * Take the results of the federated connectors and combine them into a valid AttributeTypeDef list or an
     * exception.
     *
     * @param repositoryName - the name of this repository
     * @param combinedAttributeTypeDefResults - AttributeTypeDefs returned by the federated connectors
     * @param userNotAuthorizedException - captured user not authorized exception
     * @param repositoryErrorException - captured repository error exception
     * @param anotherException - captured Throwable
     * @param methodName - name of the method issuing the request
     * @return list of AttributeTypeDefs
     * @throws RepositoryErrorException - problem in one of the federated connectors
     * @throws UserNotAuthorizedException - user not recognized or not granted access to the TypeDefs
     */
    private List<AttributeTypeDef> validatedAttributeTypeDefListResults(String                                 repositoryName,
                                                                        HashMap<String, AttributeTypeDef>      combinedAttributeTypeDefResults,
                                                                        UserNotAuthorizedException             userNotAuthorizedException,
                                                                        RepositoryErrorException               repositoryErrorException,
                                                                        Throwable                              anotherException,
                                                                        String                                 methodName) throws RepositoryErrorException,
                                                                                                                                  UserNotAuthorizedException
    {
        /*
         * Return any results, or null if nothing is found.
         */
        if (! combinedAttributeTypeDefResults.isEmpty())
        {
            List<AttributeTypeDef>    attributeTypeDefs = new ArrayList<>(combinedAttributeTypeDefResults.values());

            repositoryValidator.validateEnterpriseAttributeTypeDefs(repositoryName, attributeTypeDefs, methodName);

            return attributeTypeDefs;
        }
        else
        {
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedThrowableException(anotherException, methodName);

            /*
             * Nothing went wrong - there are just no results.
             */
            return null;
        }
    }

    /**
     * Return a validated, sorted list of search results.
     *
     * @param repositoryName - name of this repository
     * @param combinedResults - results from all cohort repositories
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @param methodName - name of method called
     * @return list of entities
     * @throws RepositoryErrorException - there is a problem with the results
     */
    private List<EntityDetail> validatedEntityListResults(String                         repositoryName,
                                                          HashMap<String, EntityDetail>  combinedResults,
                                                          String                         sequencingProperty,
                                                          SequencingOrder                sequencingOrder,
                                                          int                            pageSize,
                                                          String                         methodName) throws RepositoryErrorException
    {
        if (combinedResults.isEmpty())
        {
            return null;
        }

        List<EntityDetail>   actualResults = new ArrayList<>(combinedResults.values());

        // todo - sort results and crop to max page size

        return actualResults;
    }


    /**
     * Return a validated, sorted list of search results.
     *
     * @param repositoryName - name of this repository
     * @param combinedResults - relationships from all cohort repositories
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @param methodName - name of the method called
     * @return list of relationships
     * @throws RepositoryErrorException - there is a problem with the results
     */
    private List<Relationship> validatedRelationshipResults(String                         repositoryName,
                                                            HashMap<String, Relationship>  combinedResults,
                                                            String                         sequencingProperty,
                                                            SequencingOrder                sequencingOrder,
                                                            int                            pageSize,
                                                            String                         methodName) throws RepositoryErrorException
    {
        if (combinedResults.isEmpty())
        {
            return null;
        }

        List<Relationship>   actualResults = new ArrayList<>(combinedResults.values());

        // todo - sort results and crop to max page size

        return actualResults;
    }


    /**
     * Return a validated InstanceGraph.
     *
     * @param repositoryName - name of this repository
     * @param accumulatedEntityResults - list of returned entities
     * @param accumulatedRelationshipResults - list of returned relationships
     * @param userNotAuthorizedException - captured exception
     * @param propertyErrorException - captured exception
     * @param functionNotSupportedException - captured exception
     * @param entityNotKnownException - captured exception
     * @param repositoryErrorException - captured exception
     * @param anotherException - captured Throwable exception
     * @param methodName - name of calling method
     * @return InstanceGraph
     */
    private InstanceGraph validatedInstanceGraphResults(String                        repositoryName,
                                                        HashMap<String, EntityDetail> accumulatedEntityResults,
                                                        HashMap<String, Relationship> accumulatedRelationshipResults,
                                                        UserNotAuthorizedException    userNotAuthorizedException,
                                                        PropertyErrorException        propertyErrorException,
                                                        FunctionNotSupportedException functionNotSupportedException,
                                                        EntityNotKnownException       entityNotKnownException,
                                                        RepositoryErrorException      repositoryErrorException,
                                                        Throwable                     anotherException,
                                                        String                        methodName) throws UserNotAuthorizedException,
                                                                                                         PropertyErrorException,
                                                                                                         FunctionNotSupportedException,
                                                                                                         EntityNotKnownException,
                                                                                                         RepositoryErrorException
    {
        int  resultCount = (accumulatedEntityResults.size() + accumulatedRelationshipResults.size());

        /*
         * Return any results, or exception if nothing is found.
         */
        if (resultCount > 0)
        {
            InstanceGraph         instanceGraph = new InstanceGraph();
            List<EntityDetail>    entityDetails = new ArrayList<>(accumulatedEntityResults.values());
            List<Relationship>    relationships = new ArrayList<>(accumulatedRelationshipResults.values());

            // todo Validate the entities and relationships

            instanceGraph.setEntities(entityDetails);
            instanceGraph.setRelationships(relationships);

            return instanceGraph;
        }
        else
        {
            throwCapturedUserNotAuthorizedException(userNotAuthorizedException);
            throwCapturedRepositoryErrorException(repositoryErrorException);
            throwCapturedPropertyErrorException(propertyErrorException);
            throwCapturedThrowableException(anotherException, methodName);
            throwCapturedFunctionNotSupportedException(functionNotSupportedException);
            throwCapturedEntityNotKnownException(entityNotKnownException);

            /*
             * Nothing went wrong - there are just no results.
             */
            return null;
        }
    }
}
