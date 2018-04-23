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
package org.apache.atlas.omrs.rest.server;

import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.*;
import org.apache.atlas.omrs.localrepository.repositoryconnector.LocalOMRSRepositoryConnector;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.MatchCriteria;
import org.apache.atlas.omrs.metadatacollection.properties.SequencingOrder;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;
import org.apache.atlas.omrs.rest.properties.*;
import org.springframework.web.bind.annotation.*;


import java.text.MessageFormat;
import java.util.Date;
import java.util.List;

/**
 * OMRSRepositoryRESTServices provides the server-side support for the OMRS Repository REST Services API.
 * It is a minimal wrapper around the OMRSRepositoryConnector for the local server's metadata collection.
 * If localRepositoryConnector is null when a REST call is received, the request is rejected.
 *
 * The REST services are based around the OMRSMetadataCollection interface.
 * * <p>
 *     OMRSMetadataCollection is the common interface for working with the contents of a metadata repository.
 *     Within a metadata collection are the type definitions (TypeDefs) and metadata instances (Entities and
 *     Relationships).  OMRSMetadataCollectionBase provides empty implementation of the the abstract methods of
 *     OMRSMetadataCollection.
 *
 *     The methods on OMRSMetadataCollection are in the following major groups:
 * </p>
 * <ul>
 *     <li><b>Methods to retrieve information about the metadata repository</b> -
 *         Used to retrieve or confirm the identity of the metadata repository
 *     </li>
 *     <li><b>Methods for working with typedefs</b> -
 *         Typedefs are used to define the type model for open metadata.
 *         The open metadata support had a comprehensive set of typedefs implemented, and these can be augmented by
 *         different vendors or applications.  The typedefs can be queried, created, updated and deleted though the
 *         metadata collection.
 *     </li>
 *
 *     <li><b>Methods for querying Entities and Relationships</b> -
 *         The metadata repository stores instances of the typedefs as metadata instances.
 *         Principally these are entities (nodes in the graph) and relationships (links between nodes).
 *         Both the entities and relationships can have properties.
 *         The entity may also have structured properties called structs and classifications attached.
 *         This second group of methods supports a range of queries to retrieve these instances.
 *     </li>
 *
 *     <li><b>Methods for maintaining the instances</b> -
 *         The fourth group of methods supports the maintenance of the metadata instances.  Each instance as a status
 *         (see InstanceStatus) that allows an instance to be proposed, drafted and approved before it becomes
 *         active.  The instances can also be soft-deleted and restored or purged from the metadata
 *         collection.
 *     </li>
 *     <li>
 *         <b>Methods for repairing the metadata collections of the cohort</b> -
 *         The fifth group of methods are for editing the control information of entities and relationships to
 *         manage changes in the cohort.  These methods are advanced methods and are rarely used.
 *     </li>
 *     <li>
 *         <b>Methods for local maintenance of a metadata collection</b>
 *         The final group of methods are for removing reference copies of the metadata instances.  These updates
 *         are not broadcast to the rest of the Cohort as events.
 *     </li>
 * </ul>
 */
@RestController
@RequestMapping("/omag/omrs/")
public class OMRSRepositoryRESTServices
{
    private static LocalOMRSRepositoryConnector localRepositoryConnector = null;
    private static OMRSMetadataCollection       localMetadataCollection = null;
    private static String                       localServerURL = null;


    /**
     * Set up the local repository connector that will service the REST Calls.
     *
     * @param localRepositoryConnector - link to the local repository responsible for servicing the REST calls.
     *                                 If localRepositoryConnector is null when a REST calls is received, the request
     *                                 is rejected.
     * @param localServerURL - URL of the local server
     */
    public static void setLocalRepository(LocalOMRSRepositoryConnector    localRepositoryConnector,
                                          String                          localServerURL)
    {
        try
        {
            OMRSRepositoryRESTServices.localRepositoryConnector = localRepositoryConnector;
            OMRSRepositoryRESTServices.localMetadataCollection = localRepositoryConnector.getMetadataCollection();
        }
        catch (Throwable error)
        {
            OMRSRepositoryRESTServices.localRepositoryConnector = null;
            OMRSRepositoryRESTServices.localMetadataCollection = null;
        }

        OMRSRepositoryRESTServices.localServerURL = localServerURL;
    }


    /**
     * Return the URL for the requested instance.
     *
     * @param guid - unique identifier of the instance
     * @return url
     */
    public static String  getEntityURL(String   guid)
    {
        final String   urlTemplate = "/instances/entity/{0}";

        MessageFormat mf = new MessageFormat(urlTemplate);

        return localServerURL + mf.format(guid);
    }


    /**
     * Return the URL for the requested instance.
     *
     * @param guid - unique identifier of the instance
     * @return url
     */
    public static String  getRelationshipURL(String   guid)
    {
        final String   urlTemplate = "/instances/relationship/{0}";

        MessageFormat mf = new MessageFormat(urlTemplate);

        return localServerURL + mf.format(guid);
    }

    /**
     * Default constructor
     */
    public OMRSRepositoryRESTServices()
    {
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
    @RequestMapping(method = RequestMethod.GET, path = "/metadata-collection-id")

    public String      getMetadataCollectionId() throws RepositoryErrorException
    {
        final  String   methodName = "getMetadataCollectionId";

        validateLocalRepository(methodName);

        return localMetadataCollection.getMetadataCollectionId();
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
     * @return TypeDefGalleryResponse:
     * List of different categories of type definitions or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/all")

    public TypeDefGalleryResponse getAllTypes(@PathVariable String   userId)
    {
        final  String   methodName = "getAllTypes";

        TypeDefGalleryResponse  response = new TypeDefGalleryResponse();

        try
        {
            validateLocalRepository(methodName);

            TypeDefGallery typeDefGallery = localMetadataCollection.getAllTypes(userId);
            if (typeDefGallery != null)
            {
                response.setAttributeTypeDefs(typeDefGallery.getAttributeTypeDefs());
                response.setTypeDefs(typeDefGallery.getTypeDefs());
            }
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        return response;
    }


    /**
     * Returns a list of type definitions that have the specified name.  Type names should be unique.  This
     * method allows wildcard character to be included in the name.  These are * (asterisk) for an
     * arbitrary string of characters and ampersand for an arbitrary character.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - name of the TypeDefs to return (including wildcard characters).
     * @return TypeDefGalleryResponse:
     * List of different categories of type definitions or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation or
     * InvalidParameterException - the name of the TypeDef is null.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/by-name")

    public TypeDefGalleryResponse findTypesByName(@PathVariable String userId,
                                                  @RequestParam String name)
    {
        final  String   methodName = "findTypesByName";

        TypeDefGalleryResponse  response = new TypeDefGalleryResponse();

        try
        {
            validateLocalRepository(methodName);

            TypeDefGallery typeDefGallery = localMetadataCollection.findTypesByName(userId, name);
            if (typeDefGallery != null)
            {
                response.setAttributeTypeDefs(typeDefGallery.getAttributeTypeDefs());
                response.setTypeDefs(typeDefGallery.getTypeDefs());
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Returns all of the TypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param category - enum value for the category of TypeDef to return.
     * @return TypeDefListResponse:
     * TypeDefs list or
     * InvalidParameterException - the TypeDefCategory is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedefs/by-category")

    public TypeDefListResponse findTypeDefsByCategory(@PathVariable String          userId,
                                                      @RequestParam TypeDefCategory category)
    {
        final  String   methodName = "findTypeDefsByCategory";

        TypeDefListResponse response = new TypeDefListResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDefs(localMetadataCollection.findTypeDefsByCategory(userId, category));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Returns all of the AttributeTypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param category - enum value for the category of an AttributeTypeDef to return.
     * @return AttributeTypeDefListResponse:
     * AttributeTypeDefs list or
     * InvalidParameterException - the TypeDefCategory is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/attribute-typedefs/by-category")

    public AttributeTypeDefListResponse findAttributeTypeDefsByCategory(@PathVariable String                   userId,
                                                                        @RequestParam AttributeTypeDefCategory category)
    {
        final  String   methodName = "findAttributeTypeDefsByCategory";

        AttributeTypeDefListResponse response = new AttributeTypeDefListResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setAttributeTypeDefs(localMetadataCollection.findAttributeTypeDefsByCategory(userId, category));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Return the TypeDefs that have the properties matching the supplied match criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param matchCriteria - TypeDefProperties - a list of property names and values.
     * @return TypeDefListResponse:
     * TypeDefs list or
     * InvalidParameterException - the matchCriteria is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedefs/by-property")

    public TypeDefListResponse findTypeDefsByProperty(@PathVariable String            userId,
                                                      @RequestParam TypeDefProperties matchCriteria)
    {
        final  String   methodName = "findTypeDefsByProperty";

        TypeDefListResponse response = new TypeDefListResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDefs(localMetadataCollection.findTypeDefsByProperty(userId, matchCriteria));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Return the types that are linked to the elements from the specified standard.
     *
     * @param userId - unique identifier for requesting user.
     * @param standard - name of the standard - null means any.
     * @param organization - name of the organization - null means any.
     * @param identifier - identifier of the element in the standard - null means any.
     * @return TypeDefsGalleryResponse:
     * A list of types or
     * InvalidParameterException - all attributes of the external id are null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedefs/by-external-id")

    public TypeDefListResponse findTypesByExternalID(@PathVariable String    userId,
                                                     @RequestParam String    standard,
                                                     @RequestParam String    organization,
                                                     @RequestParam String    identifier)
    {
        final  String   methodName = "findTypesByExternalID";

        TypeDefListResponse  response = new TypeDefListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<TypeDef> typeDefs = localMetadataCollection.findTypesByExternalID(userId,
                                                                                   standard,
                                                                                   organization,
                                                                                   identifier);
            response.setTypeDefs(typeDefs);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Return the TypeDefs that match the search criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param searchCriteria - String - search criteria.
     * @return TypeDefListResponse:
     * TypeDefs list or
     * InvalidParameterException - the searchCriteria is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedefs/by-property-value")

    public TypeDefListResponse searchForTypeDefs(@PathVariable String userId,
                                                 @RequestParam String searchCriteria)
    {
        final  String   methodName = "searchForTypeDefs";

        TypeDefListResponse response = new TypeDefListResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDefs(localMetadataCollection.searchForTypeDefs(userId, searchCriteria));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Return the TypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique id of the TypeDef.
     * @return TypeDefResponse:
     * TypeDef structure describing its category and properties or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotKnownException - The requested TypeDef is not known in the metadata collection or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedef/{guid}")

    public TypeDefResponse getTypeDefByGUID(@PathVariable String    userId,
                                            @PathVariable String    guid)
    {
        final  String   methodName = "getTypeDefByGUID";

        TypeDefResponse response = new TypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDef(localMetadataCollection.getTypeDefByGUID(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Return the AttributeTypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique id of the TypeDef
     * @return AttributeTypeDefResponse:
     * TypeDef structure describing its category and properties or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotKnownException - The requested TypeDef is not known in the metadata collection or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/attribute-typedef/{guid}")

    public AttributeTypeDefResponse getAttributeTypeDefByGUID(@PathVariable String    userId,
                                                              @PathVariable String    guid)
    {
        final  String   methodName = "getAttributeTypeDefByGUID";

        AttributeTypeDefResponse response = new AttributeTypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setAttributeTypeDef(localMetadataCollection.getAttributeTypeDefByGUID(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnown(response, error);
        }

        return response;
    }



    /**
     * Return the TypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - String name of the TypeDef.
     * @return TypeDefResponse:
     * TypeDef structure describing its category and properties or
     * InvalidParameterException - the name is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedef/name/{name}")

    public TypeDefResponse getTypeDefByName(@PathVariable String    userId,
                                            @PathVariable String    name)
    {
        final  String   methodName = "getTypeDefByName";

        TypeDefResponse response = new TypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDef(localMetadataCollection.getTypeDefByName(userId, name));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Return the AttributeTypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - String name of the TypeDef.
     * @return AttributeTypeDefResponse:
     * AttributeTypeDef structure describing its category and properties or
     * InvalidParameterException - the name is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/attribute-typedef/name/{name}")

    public  AttributeTypeDefResponse getAttributeTypeDefByName(@PathVariable String    userId,
                                                               @PathVariable String    name)
    {
        final  String   methodName = "getAttributeTypeDefByName";

        AttributeTypeDefResponse response = new AttributeTypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setAttributeTypeDef(localMetadataCollection.getAttributeTypeDefByName(userId, name));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnown(response, error);
        }

        return response;
    }


    /**
     * Create a collection of related types.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypes - TypeDefGalleryResponse structure describing the new AttributeTypeDefs and TypeDefs.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the new TypeDef is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotSupportedException - the repository is not able to support this TypeDef or
     * TypeDefKnownException - the TypeDef is already stored in the repository or
     * TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef or
     * InvalidTypeDefException - the new TypeDef has invalid contents or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/types")

    public  VoidResponse addTypeDefGallery(@PathVariable String          userId,
                                           @RequestParam TypeDefGallery  newTypes)
    {
        final  String   methodName = "addTypeDefGallery";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.addTypeDefGallery(userId, newTypes);
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotSupportedException error)
        {
            captureTypeDefNotSupportedException(response, error);
        }
        catch (TypeDefKnownException error)
        {
            captureTypeDefKnownException(response, error);
        }
        catch (TypeDefConflictException error)
        {
            captureTypeDefConflictException(response, error);
        }
        catch (InvalidTypeDefException error)
        {
            captureInvalidTypeDefException(response, error);
        }

        return response;
    }


    /**
     * Create a definition of a new TypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the new TypeDef is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotSupportedException - the repository is not able to support this TypeDef or
     * TypeDefKnownException - the TypeDef is already stored in the repository or
     * TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef or
     * InvalidTypeDefException - the new TypeDef has invalid contents or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/types/typedef")

    public VoidResponse addTypeDef(@PathVariable String       userId,
                                   @RequestParam TypeDef      newTypeDef)
    {
        final  String   methodName = "addTypeDef";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.addTypeDef(userId, newTypeDef);
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotSupportedException error)
        {
            captureTypeDefNotSupportedException(response, error);
        }
        catch (TypeDefKnownException error)
        {
            captureTypeDefKnownException(response, error);
        }
        catch (TypeDefConflictException error)
        {
            captureTypeDefConflictException(response, error);
        }
        catch (InvalidTypeDefException error)
        {
            captureInvalidTypeDefException(response, error);
        }

        return response;
    }


    /**
     * Create a definition of a new AttributeTypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param newAttributeTypeDef - TypeDef structure describing the new TypeDef.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the new TypeDef is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotSupportedException - the repository is not able to support this TypeDef or
     * TypeDefKnownException - the TypeDef is already stored in the repository or
     * TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef or
     * InvalidTypeDefException - the new TypeDef has invalid contents or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/types/attribute-typedef")

    public  VoidResponse addAttributeTypeDef(@PathVariable String             userId,
                                             @RequestParam AttributeTypeDef   newAttributeTypeDef)
    {
        final  String   methodName = "addAttributeTypeDef";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.addAttributeTypeDef(userId, newAttributeTypeDef);
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotSupportedException error)
        {
            captureTypeDefNotSupportedException(response, error);
        }
        catch (TypeDefKnownException error)
        {
            captureTypeDefKnownException(response, error);
        }
        catch (TypeDefConflictException error)
        {
            captureTypeDefConflictException(response, error);
        }
        catch (InvalidTypeDefException error)
        {
            captureInvalidTypeDefException(response, error);
        }

        return response;
    }




    /**
     * Verify that a definition of a TypeDef is either new - or matches the definition already stored.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDef - TypeDef structure describing the TypeDef to test.
     * @return BooleanResponse:
     * boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known or
     * InvalidParameterException - the TypeDef is null.
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * InvalidTypeDefException - the new TypeDef has invalid contents.
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/typedef/compatibility")

    public BooleanResponse verifyTypeDef(@PathVariable String       userId,
                                         @RequestParam TypeDef      typeDef)
    {
        final  String   methodName = "verifyTypeDef";

        BooleanResponse response = new BooleanResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setFlag(localMetadataCollection.verifyTypeDef(userId, typeDef));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotSupportedException error)
        {
            captureTypeDefNotSupportedException(response, error);
        }
        catch (TypeDefConflictException error)
        {
            captureTypeDefConflictException(response, error);
        }
        catch (InvalidTypeDefException error)
        {
            captureInvalidTypeDefException(response, error);
        }

        return response;
    }



    /**
     * Verify that a definition of an AttributeTypeDef is either new - or matches the definition already stored.
     *
     * @param userId - unique identifier for requesting user.
     * @param attributeTypeDef - TypeDef structure describing the TypeDef to test.
     * @return BooleanResponse:
     * boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known or
     * InvalidParameterException - the TypeDef is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeDefNotSupportedException - the repository is not able to support this TypeDef or
     * TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef or
     * InvalidTypeDefException - the new TypeDef has invalid contents or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/types/attribute-typedef/compatibility")

    public  BooleanResponse verifyAttributeTypeDef(@PathVariable String            userId,
                                                   @RequestParam AttributeTypeDef  attributeTypeDef)
    {
        final  String   methodName = "verifyAttributeTypeDef";

        BooleanResponse response = new BooleanResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setFlag(localMetadataCollection.verifyAttributeTypeDef(userId, attributeTypeDef));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotSupportedException error)
        {
            captureTypeDefNotSupportedException(response, error);
        }
        catch (TypeDefConflictException error)
        {
            captureTypeDefConflictException(response, error);
        }
        catch (InvalidTypeDefException error)
        {
            captureInvalidTypeDefException(response, error);
        }

        return response;
    }


    /**
     * Update one or more properties of the TypeDef.  The TypeDefPatch controls what types of updates
     * are safe to make to the TypeDef.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefPatch - TypeDef patch describing change to TypeDef.
     * @return TypeDefResponse:
     * updated TypeDef or
     * InvalidParameterException - the TypeDefPatch is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection or
     * PatchErrorException - the TypeDef can not be updated because the supplied patch is incompatible
     *                               with the stored TypeDef or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/types/typedef")

    public TypeDefResponse updateTypeDef(@PathVariable String       userId,
                                         @RequestParam TypeDefPatch typeDefPatch)
    {
        final  String   methodName = "updateTypeDef";

        TypeDefResponse response = new TypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDef(localMetadataCollection.updateTypeDef(userId, typeDefPatch));
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnownException(response, error);
        }
        catch (PatchErrorException error)
        {
            response.setRelatedHTTPCode(error.getReportedHTTPCode());
            response.setExceptionClassName(PatchErrorException.class.getName());
            response.setExceptionErrorMessage(error.getErrorMessage());
            response.setExceptionSystemAction(error.getReportedSystemAction());
            response.setExceptionUserAction(error.getReportedUserAction());
        }

        return response;
    }


    /**
     * Delete the TypeDef.  This is only possible if the TypeDef has never been used to create instances or any
     * instances of this TypeDef have been purged from the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the one of TypeDef identifiers is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeDefNotKnownException - the requested TypeDef is not found in the metadata collection or
     * TypeDefInUseException - the TypeDef can not be deleted because there are instances of this type in the
     *                                 the metadata collection.  These instances need to be purged before the
     *                                 TypeDef can be deleted or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/types/typedef/{obsoleteTypeDefGUID}")

    public VoidResponse deleteTypeDef(@PathVariable String    userId,
                                      @PathVariable String    obsoleteTypeDefGUID,
                                      @RequestParam String    obsoleteTypeDefName)
    {
        final  String   methodName = "deleteTypeDef";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.deleteTypeDef(userId, obsoleteTypeDefGUID, obsoleteTypeDefName);
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnownException(response, error);
        }
        catch (TypeDefInUseException error)
        {
            captureTypeDefInUseException(response, error);
        }

        return response;
    }


    /**
     * Delete an AttributeTypeDef.  This is only possible if the AttributeTypeDef has never been used to create
     * instances or any instances of this AttributeTypeDef have been purged from the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the one of AttributeTypeDef identifiers is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeDefNotKnownException - the requested AttributeTypeDef is not found in the metadata collection.
     * TypeDefInUseException - the AttributeTypeDef can not be deleted because there are instances of this type in the
     *                                 the metadata collection.  These instances need to be purged before the
     *                                 AttributeTypeDef can be deleted or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/types/attribute-typedef/{obsoleteTypeDefGUID}")

    public VoidResponse deleteAttributeTypeDef(@PathVariable String    userId,
                                               @PathVariable String    obsoleteTypeDefGUID,
                                               @RequestParam String    obsoleteTypeDefName)
    {
        final  String   methodName = "deleteAttributeTypeDef";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.deleteAttributeTypeDef(userId, obsoleteTypeDefGUID, obsoleteTypeDefName);
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnownException(response, error);
        }
        catch (TypeDefInUseException error)
        {
            captureTypeDefInUseException(response, error);
        }

        return response;
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
     * @return TypeDefResponse:
     * typeDef - new values for this TypeDef, including the new guid/name or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeDefNotKnownException - the TypeDef identified by the original guid/name is not found
     *                                    in the metadata collection or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/types/typedef/{originalTypeDefGUID}/identifier")

    public  TypeDefResponse reIdentifyTypeDef(@PathVariable String     userId,
                                              @PathVariable String     originalTypeDefGUID,
                                              @RequestParam String     originalTypeDefName,
                                              @RequestParam String     newTypeDefGUID,
                                              @RequestParam String     newTypeDefName)
    {
        final  String   methodName = "reIdentifyTypeDef";

        TypeDefResponse response = new TypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setTypeDef(localMetadataCollection.reIdentifyTypeDef(userId,
                                                                          originalTypeDefGUID,
                                                                          originalTypeDefName,
                                                                          newTypeDefGUID,
                                                                          newTypeDefName));
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnownException(response, error);
        }

        return response;
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
     * @return AttributeTypeDefResponse:
     * attributeTypeDef - new values for this AttributeTypeDef, including the new guid/name or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeDefNotKnownException - the AttributeTypeDef identified by the original guid/name is not
     *                                    found in the metadata collection or
     * FunctionNotSupportedException - the repository does not support this call or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/types/attribute-typedef/{originalAttributeTypeDefGUID}/identifier")

    public  AttributeTypeDefResponse reIdentifyAttributeTypeDef(@PathVariable String     userId,
                                                                @PathVariable String     originalAttributeTypeDefGUID,
                                                                @RequestParam String     originalAttributeTypeDefName,
                                                                @RequestParam String     newAttributeTypeDefGUID,
                                                                @RequestParam String     newAttributeTypeDefName)
    {
        final  String   methodName = "reIdentifyAttributeTypeDef";

        AttributeTypeDefResponse response = new AttributeTypeDefResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setAttributeTypeDef(localMetadataCollection.reIdentifyAttributeTypeDef(userId,
                                                                                            originalAttributeTypeDefGUID,
                                                                                            originalAttributeTypeDefName,
                                                                                            newAttributeTypeDefGUID,
                                                                                            newAttributeTypeDefName));
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeDefNotKnownException error)
        {
            captureTypeDefNotKnown(response, error);
        }

        return response;
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
     * InvalidParameterException - the guid is null.
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entity/{guid}/existence")

    public EntityDetailResponse isEntityKnown(@PathVariable String     userId,
                                              @PathVariable String     guid)
    {
        final  String   methodName = "isEntityKnown";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.isEntityKnown(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Return the header and classifications for a specific entity.  The returned entity summary may be from
     * a full entity object or an entity proxy.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity
     * @return EntitySummary structure or
     * InvalidParameterException - the guid is null.
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * EntityNotKnownException - the requested entity instance is not known in the metadata collection.
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entity/{guid}/summary")

    public EntitySummaryResponse getEntitySummary(@PathVariable String     userId,
                                                  @PathVariable String     guid)
    {
        final  String   methodName = "getEntitySummary";

        EntitySummaryResponse response = new EntitySummaryResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.getEntitySummary(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Return the header, classifications and properties of a specific entity.  This method supports anonymous
     * access to an instance.  The call may fail if the metadata is secured.
     *
     * @param guid - String unique identifier for the entity.
     * @return EntityDetailResponse:
     * EntityDetail structure or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored or
     * EntityNotKnownException - the requested entity instance is not known in the metadata collection or
     * EntityProxyOnlyException - the requested entity instance is only a proxy in the metadata collection or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/instances/entity/{guid}")

    public EntityDetailResponse getEntityDetail(@PathVariable String     guid)
    {
        return this.getEntityDetail(null, guid);
    }


    /**
     * Return the header, classifications and properties of a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @return EntityDetailResponse:
     * EntityDetail structure or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored or
     * EntityNotKnownException - the requested entity instance is not known in the metadata collection or
     * EntityProxyOnlyException - the requested entity instance is only a proxy in the metadata collection or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entity/{guid}")

    public EntityDetailResponse getEntityDetail(@PathVariable String     userId,
                                                @PathVariable String     guid)
    {
        final  String   methodName = "getEntityDetail";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.getEntityDetail(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (EntityProxyOnlyException error)
        {
            captureEntityProxyOnlyException(response, error);
        }

        return response;
    }


    /**
     * Return a historical version of an entity - includes the header, classifications and properties of the entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the entity.
     * @param asOfTime - the time used to determine which version of the entity that is desired.
     * @return EnityDetailResponse:
     * EntityDetail structure or
     * InvalidParameterException - the guid or date is null or the asOfTime property is for a future time or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored or
     * EntityNotKnownException - the requested entity instance is not known in the metadata collection
     *                                   at the time requested or
     * EntityProxyOnlyException - the requested entity instance is only a proxy in the metadata collection or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entity/{guid}/history")

    public  EntityDetailResponse getEntityDetail(@PathVariable String     userId,
                                                 @PathVariable String     guid,
                                                 @RequestParam Date       asOfTime)
    {
        final  String   methodName = "getEntityDetail";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.getEntityDetail(userId, guid, asOfTime));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (EntityProxyOnlyException error)
        {
            captureEntityProxyOnlyException(response, error);
        }

        return response;
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
     * @return RelationshipListResponse:
     * Relationships list.  Null means no relationships associated with the entity or
     * InvalidParameterException - a parameter is invalid or null or
     * TypeErrorException - the type guid passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the requested entity instance is not known in the metadata collection or
     * PropertyErrorException - the sequencing property is not valid for the attached classifications or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation or
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entity/{entityGUID}/relationships")

    public RelationshipListResponse getRelationshipsForEntity(@PathVariable                   String                     userId,
                                                              @PathVariable                   String                     entityGUID,
                                                              @RequestParam(required = false) String                     relationshipTypeGUID,
                                                              @RequestParam(required = false) int                        fromRelationshipElement,
                                                              @RequestParam(required = false) List<InstanceStatus>       limitResultsByStatus,
                                                              @RequestParam(required = false) Date                       asOfTime,
                                                              @RequestParam(required = false) String                     sequencingProperty,
                                                              @RequestParam(required = false) SequencingOrder            sequencingOrder,
                                                              @RequestParam(required = false) int                        pageSize)
    {
        final  String   methodName = "getRelationshipsForEntity";

        RelationshipListResponse response = new RelationshipListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<Relationship>  relationships = localMetadataCollection.getRelationshipsForEntity(userId,
                                                                                                  entityGUID,
                                                                                                  relationshipTypeGUID,
                                                                                                  fromRelationshipElement,
                                                                                                  limitResultsByStatus,
                                                                                                  asOfTime,
                                                                                                  sequencingProperty,
                                                                                                  sequencingOrder,
                                                                                                  pageSize);
            response.setRelationships(relationships);
            if (relationships != null)
            {
                response.setOffset(fromRelationshipElement);
                response.setPageSize(pageSize);
                if (response.getRelationships().size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/entity/{1}/relationships?relationshipTypeGUID={2}&fromRelationshipElement={3}&limitResultsByStatus={4}&asOfTime={5}&sequencingProperty={6}&sequencingOrder={7}&pageSize={8}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              entityGUID,
                                                              relationshipTypeGUID,
                                                              fromRelationshipElement + pageSize,
                                                              limitResultsByStatus,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }

        return response;
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
     * @return EntityListResponse:
     * a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection or
     * InvalidParameterException - a parameter is invalid or null or
     * TypeErrorException - the type guid passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * PropertyErrorException - the properties specified are not valid for any of the requested types of
     *                                  entity or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entities/by-property")

    public  EntityListResponse findEntitiesByProperty(@PathVariable                   String                    userId,
                                                      @RequestParam(required = false) String                    entityTypeGUID,
                                                      @RequestParam(required = false) InstanceProperties        matchProperties,
                                                      @RequestParam(required = false) MatchCriteria             matchCriteria,
                                                      @RequestParam(required = false) int                       fromEntityElement,
                                                      @RequestParam(required = false) List<InstanceStatus>      limitResultsByStatus,
                                                      @RequestParam(required = false) List<String>              limitResultsByClassification,
                                                      @RequestParam(required = false) Date                      asOfTime,
                                                      @RequestParam(required = false) String                    sequencingProperty,
                                                      @RequestParam(required = false) SequencingOrder           sequencingOrder,
                                                      @RequestParam(required = false) int                       pageSize)
    {
        final  String   methodName = "findEntitiesByProperty";

        EntityListResponse response = new EntityListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<EntityDetail>  entities = localMetadataCollection.findEntitiesByProperty(userId,
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
            response.setEntities(entities);
            if (entities != null)
            {
                response.setOffset(fromEntityElement);
                response.setPageSize(pageSize);
                if (entities.size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/entities/by-property?entityTypeGUID={1}&matchProperties={2}&matchCriteria={3}&fromEntityElement={4}&limitResultsByStatus={5}&limitResultsByClassification={6}&asOfTime={7}&sequencingProperty={8}&sequencingOrder={9}&pageSize={10}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              entityTypeGUID,
                                                              matchProperties,
                                                              matchCriteria,
                                                              fromEntityElement + pageSize,
                                                              limitResultsByStatus,
                                                              limitResultsByClassification,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }

        return response;
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
     * @return EntityListResponse:
     * a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection or
     * InvalidParameterException - a parameter is invalid or null or
     * TypeErrorException - the type guid passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * ClassificationErrorException - the classification request is not known to the metadata collection.
     * PropertyErrorException - the properties specified are not valid for the requested type of
     *                                  classification or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entities/by-classification/{classificationName}")

    public  EntityListResponse findEntitiesByClassification(@PathVariable                   String                    userId,
                                                            @RequestParam(required = false) String                    entityTypeGUID,
                                                            @PathVariable                   String                    classificationName,
                                                            @RequestParam(required = false) InstanceProperties        matchClassificationProperties,
                                                            @RequestParam(required = false) MatchCriteria             matchCriteria,
                                                            @RequestParam(required = false) int                       fromEntityElement,
                                                            @RequestParam(required = false) List<InstanceStatus>      limitResultsByStatus,
                                                            @RequestParam(required = false) Date                      asOfTime,
                                                            @RequestParam(required = false) String                    sequencingProperty,
                                                            @RequestParam(required = false) SequencingOrder           sequencingOrder,
                                                            @RequestParam(required = false) int                       pageSize)
    {
        final  String   methodName = "findEntitiesByClassification";

        EntityListResponse response = new EntityListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<EntityDetail>  entities = localMetadataCollection.findEntitiesByClassification(userId,
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
            response.setEntities(entities);
            if (entities != null)
            {
                response.setOffset(fromEntityElement);
                response.setPageSize(pageSize);
                if (entities.size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/entities/by-classification/{1}?entityTypeGUID={2}&matchClassificationProperties={3}&matchCriteria={4}&fromEntityElement={5}&limitResultsByStatus={6}&asOfTime={7}&sequencingProperty={8}&sequencingOrder={9}&pageSize={10}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              entityTypeGUID,
                                                              classificationName,
                                                              matchClassificationProperties,
                                                              matchCriteria,
                                                              fromEntityElement + pageSize,
                                                              limitResultsByStatus,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }
        catch (ClassificationErrorException error)
        {
            captureClassificationErrorException(response, error);
        }

        return response;
    }


    /**
     * Return a list of entities whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - GUID of the type of entity to search for. Null means all types will
     *                       be searched (could be slow so not recommended).
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
     * @return EntityListResponse:
     * a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection or
     * InvalidParameterException - a parameter is invalid or null or
     * TypeErrorException - the type guid passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entities/by-property-value")

    public  EntityListResponse findEntitiesByPropertyValue(@PathVariable                   String                  userId,
                                                           @RequestParam(required = false) String                  entityTypeGUID,
                                                           @RequestParam                   String                  searchCriteria,
                                                           @RequestParam(required = false) int                     fromEntityElement,
                                                           @RequestParam(required = false) List<InstanceStatus>    limitResultsByStatus,
                                                           @RequestParam(required = false) List<String>            limitResultsByClassification,
                                                           @RequestParam(required = false) Date                    asOfTime,
                                                           @RequestParam(required = false) String                  sequencingProperty,
                                                           @RequestParam(required = false) SequencingOrder         sequencingOrder,
                                                           @RequestParam(required = false) int                     pageSize)
    {
        final  String   methodName = "findEntitiesByPropertyValue";

        EntityListResponse response = new EntityListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<EntityDetail>  entities = localMetadataCollection.findEntitiesByPropertyValue(userId,
                                                                                               entityTypeGUID,
                                                                                               searchCriteria,
                                                                                               fromEntityElement,
                                                                                               limitResultsByStatus,
                                                                                               limitResultsByClassification,
                                                                                               asOfTime,
                                                                                               sequencingProperty,
                                                                                               sequencingOrder,
                                                                                               pageSize);
            response.setEntities(entities);
            if (entities != null)
            {
                response.setOffset(fromEntityElement);
                response.setPageSize(pageSize);
                if (entities.size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/entities/by-property-value?entityTypeGUID={1}&searchCriteria={2}&fromEntityElement={3}&limitResultsByStatus={4}&limitResultsByClassification={5}&asOfTime={6}&sequencingProperty={7}&sequencingOrder={8}&pageSize={9}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              searchCriteria,
                                                              fromEntityElement + pageSize,
                                                              limitResultsByStatus,
                                                              limitResultsByClassification,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }

        return response;
    }


    /**
     * Returns a boolean indicating if the relationship is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @return RelationshipResponse:
     * relationship details if the relationship is found in the metadata collection; otherwise return null or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/relationship/{guid}/existence")

    public RelationshipResponse  isRelationshipKnown(@PathVariable String     userId,
                                                     @PathVariable String     guid)
    {
        final  String   methodName = "isRelationshipKnown";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.isRelationshipKnown(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Return a requested relationship.  This is the anonymous form for repository.  The call may fail if security is
     * required.
     *
     * @param guid - String unique identifier for the relationship.
     * @return RelationshipResponse:
     * A relationship structure or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the metadata collection does not have a relationship with
     *                                         the requested GUID stored or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/instances/relationship/{guid}")

    public RelationshipResponse getRelationship(@PathVariable String    guid)
    {
        return this.getRelationship(null, guid);
    }


    /**
     * Return a requested relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @return RelationshipResponse:
     * a relationship structure or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the metadata collection does not have a relationship with
     *                                         the requested GUID stored or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/relationship/{guid}")

    public RelationshipResponse getRelationship(@PathVariable String    userId,
                                                @PathVariable String    guid)
    {
        final  String   methodName = "getRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.getRelationship(userId, guid));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Return a historical version of a relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique identifier for the relationship.
     * @param asOfTime - the time used to determine which version of the entity that is desired.
     * @return RelationshipResponse:
     * a relationship structure or
     * InvalidParameterException - the guid or date is null or the asOfTime property is for a future time or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored or
     * RelationshipNotKnownException - the requested entity instance is not known in the metadata collection
     *                                   at the time requested or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/relationship/{guid}/history")

    public  RelationshipResponse getRelationship(@PathVariable String    userId,
                                                 @PathVariable String    guid,
                                                 @RequestParam Date      asOfTime)
    {
        final  String   methodName = "getRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.getRelationship(userId, guid, asOfTime));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Return a list of relationships that match the requested properties by the matching criteria.   The results
     * can be broken into pages.
     *
     * @param userId - unique identifier for requesting user
     * @param relationshipTypeGUID - unique identifier (guid) for the new relationship's type.
     * @param matchProperties - list of  properties used to narrow the search.
     * @param matchCriteria - Enum defining how the properties should be matched to the relationships in the repository.
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
     * @param pageSize - the maximum number of result relationships that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return RelationshipListResponse:
     * a list of relationships.  Null means no matching relationships or
     * InvalidParameterException - one of the parameters is invalid or null or
     * TypeErrorException - the type guid passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * PropertyErrorException - the properties specified are not valid for any of the requested types of
     *                                  relationships or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/relationships/by-property")

    public  RelationshipListResponse findRelationshipsByProperty(@PathVariable                   String                    userId,
                                                                 @RequestParam(required = false) String                    relationshipTypeGUID,
                                                                 @RequestParam(required = false) InstanceProperties        matchProperties,
                                                                 @RequestParam(required = false) MatchCriteria             matchCriteria,
                                                                 @RequestParam(required = false) int                       fromRelationshipElement,
                                                                 @RequestParam(required = false) List<InstanceStatus>      limitResultsByStatus,
                                                                 @RequestParam(required = false) Date                      asOfTime,
                                                                 @RequestParam(required = false) String                    sequencingProperty,
                                                                 @RequestParam(required = false) SequencingOrder           sequencingOrder,
                                                                 @RequestParam(required = false) int                       pageSize)
    {
        final  String   methodName = "findRelationshipsByProperty";

        RelationshipListResponse response = new RelationshipListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<Relationship>  relationships = localMetadataCollection.findRelationshipsByProperty(userId,
                                                                                                    relationshipTypeGUID,
                                                                                                    matchProperties,
                                                                                                    matchCriteria,
                                                                                                    fromRelationshipElement,
                                                                                                    limitResultsByStatus,
                                                                                                    asOfTime,
                                                                                                    sequencingProperty,
                                                                                                    sequencingOrder,
                                                                                                    pageSize);
            response.setRelationships(relationships);
            if (relationships != null)
            {
                response.setOffset(fromRelationshipElement);
                response.setPageSize(pageSize);
                if (response.getRelationships().size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/relationships/by-property?relationshipTypeGUID={1}&matchProperties={2}&matchCriteria={3}&fromRelationshipElement={4}&limitResultsByStatus={5}&asOfTime={6}&sequencingProperty={7}&sequencingOrder={8}&pageSize={9}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              relationshipTypeGUID,
                                                              matchProperties,
                                                              matchCriteria,
                                                              fromRelationshipElement + pageSize,
                                                              limitResultsByStatus,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }

        return response;
    }


    /**
     * Return a list of relationships that match the search criteria.  The results can be paged.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipTypeGUID - unique identifier of a relationship type (or null for all types of relationship.
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
     * @return RelationshipListResponse:
     * a list of relationships.  Null means no matching relationships or
     * InvalidParameterException - one of the parameters is invalid or null or
     * TypeErrorException - the type guid passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * PropertyErrorException - there is a problem with one of the other parameters  or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/relationships/by-property-value")

    public  RelationshipListResponse findRelationshipsByPropertyValue(@PathVariable                   String                    userId,
                                                                      @RequestParam(required = false) String                    relationshipTypeGUID,
                                                                      @RequestParam                   String                    searchCriteria,
                                                                      @RequestParam(required = false) int                       fromRelationshipElement,
                                                                      @RequestParam(required = false) List<InstanceStatus>      limitResultsByStatus,
                                                                      @RequestParam(required = false) Date                      asOfTime,
                                                                      @RequestParam(required = false) String                    sequencingProperty,
                                                                      @RequestParam(required = false) SequencingOrder           sequencingOrder,
                                                                      @RequestParam(required = false) int                       pageSize)
    {
        final  String   methodName = "findRelationshipsByPropertyValue";

        RelationshipListResponse response = new RelationshipListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<Relationship>  relationships = localMetadataCollection.findRelationshipsByPropertyValue(userId,
                                                                                                         relationshipTypeGUID,
                                                                                                         searchCriteria,
                                                                                                         fromRelationshipElement,
                                                                                                         limitResultsByStatus,
                                                                                                         asOfTime,
                                                                                                         sequencingProperty,
                                                                                                         sequencingOrder,
                                                                                                         pageSize);
            response.setRelationships(relationships);
            if (relationships != null)
            {
                response.setOffset(fromRelationshipElement);
                response.setPageSize(pageSize);
                if (response.getRelationships().size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/relationships/by-property-value?relationshipTypeGUID={1}&searchCriteria={2}?fromRelationshipElement={3}&limitResultsByStatus={4}&asOfTime={5}&sequencingProperty={6}&sequencingOrder={7}&pageSize={8}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              searchCriteria,
                                                              fromRelationshipElement + pageSize,
                                                              limitResultsByStatus,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }

        return response;
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
     * @return InstanceGraphResponse:
     * the sub-graph that represents the returned linked entities and their relationships or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by either the startEntityGUID or the endEntityGUID
     *                                   is not found in the metadata collection or
     * PropertyErrorException - there is a problem with one of the other parameters or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entities/from-entity/{startEntityGUID}/by-linkage")

    public  InstanceGraphResponse getLinkingEntities(@PathVariable                   String                    userId,
                                                     @PathVariable                   String                    startEntityGUID,
                                                     @RequestParam                   String                    endEntityGUID,
                                                     @RequestParam(required = false) List<InstanceStatus>      limitResultsByStatus,
                                                     @RequestParam(required = false) Date                      asOfTime)
    {
        final  String   methodName = "getLinkingEntities";

        InstanceGraphResponse response = new InstanceGraphResponse();

        try
        {
            validateLocalRepository(methodName);

            InstanceGraph  instanceGraph = localMetadataCollection.getLinkingEntities(userId,
                                                                                      startEntityGUID,
                                                                                      endEntityGUID,
                                                                                      limitResultsByStatus,
                                                                                      asOfTime);
            if (instanceGraph != null)
            {
                response.setEntityElementList(instanceGraph.getEntities());
                response.setRelationshipElementList(instanceGraph.getRelationships());
            }
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
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
     * @return InstanceGraphResponse
     * the sub-graph that represents the returned linked entities and their relationships or
     * InvalidParameterException - one of the parameters is invalid or null or
     * TypeErrorException - one of the type guids passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the entityGUID is not found in the metadata collection or
     * PropertyErrorException - there is a problem with one of the other parameters or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entities/from-entity/{entityGUID}/by-neighborhood")

    public  InstanceGraphResponse getEntityNeighborhood(@PathVariable                   String               userId,
                                                        @PathVariable                   String               entityGUID,
                                                        @RequestParam(required = false) List<String>         entityTypeGUIDs,
                                                        @RequestParam(required = false) List<String>         relationshipTypeGUIDs,
                                                        @RequestParam(required = false) List<InstanceStatus> limitResultsByStatus,
                                                        @RequestParam(required = false) List<String>         limitResultsByClassification,
                                                        @RequestParam(required = false) Date                 asOfTime,
                                                        @RequestParam                   int                  level)
    {
        final  String   methodName = "getEntityNeighborhood";

        InstanceGraphResponse response = new InstanceGraphResponse();

        try
        {
            validateLocalRepository(methodName);

            InstanceGraph instanceGraph = localMetadataCollection.getEntityNeighborhood(userId,
                                                                                        entityGUID,
                                                                                        entityTypeGUIDs,
                                                                                        relationshipTypeGUIDs,
                                                                                        limitResultsByStatus,
                                                                                        limitResultsByClassification,
                                                                                        asOfTime,
                                                                                        level);
            if (instanceGraph != null)
            {
                response.setEntityElementList(instanceGraph.getEntities());
                response.setRelationshipElementList(instanceGraph.getRelationships());
            }
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }

        return response;
    }



    /**
     * Return the list of entities that are of the types listed in instanceTypes and are connected, either directly or
     * indirectly to the entity identified by startEntityGUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param startEntityGUID - unique identifier of the starting entity.
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
     * @return EntityListResponse:
     * list of entities either directly or indirectly connected to the start entity or
     * InvalidParameterException - one of the parameters is invalid or null or
     * TypeErrorException - one of the type guids passed on the request is not known by the metadata collection or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                              hosting the metadata collection or
     * EntityNotKnownException - the entity identified by the startEntityGUID
     *                                   is not found in the metadata collection or
     * PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity or
     * PagingErrorException - the paging/sequencing parameters are set up incorrectly or
     * FunctionNotSupportedException - the repository does not support satOfTime parameter or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/instances/entities/from-entity/{startEntityGUID}/by-relationship")

    public  EntityListResponse getRelatedEntities(@PathVariable                   String               userId,
                                                  @PathVariable                   String               startEntityGUID,
                                                  @RequestParam(required = false) List<String>         instanceTypes,
                                                  @RequestParam(required = false) int                  fromEntityElement,
                                                  @RequestParam(required = false) List<InstanceStatus> limitResultsByStatus,
                                                  @RequestParam(required = false) List<String>         limitResultsByClassification,
                                                  @RequestParam(required = false) Date                 asOfTime,
                                                  @RequestParam(required = false) String               sequencingProperty,
                                                  @RequestParam(required = false) SequencingOrder      sequencingOrder,
                                                  @RequestParam(required = false) int                  pageSize)
    {
        final  String   methodName = "getRelatedEntities";

        EntityListResponse response = new EntityListResponse();

        try
        {
            validateLocalRepository(methodName);

            List<EntityDetail>  entities = localMetadataCollection.getRelatedEntities(userId,
                                                                                      startEntityGUID,
                                                                                      instanceTypes,
                                                                                      fromEntityElement,
                                                                                      limitResultsByStatus,
                                                                                      limitResultsByClassification,
                                                                                      asOfTime,
                                                                                      sequencingProperty,
                                                                                      sequencingOrder,
                                                                                      pageSize);
            response.setEntities(entities);
            if (entities != null)
            {
                response.setOffset(fromEntityElement);
                response.setPageSize(pageSize);
                if (entities.size() == pageSize)
                {
                    final String urlTemplate = "{0}/instances/entities/from-entity/{1}/by-relationship?fromEntityElement={2}&limitResultsByStatus={3}&limitResultsByClassification={4}&asOfTime={5}&sequencingProperty={6}&sequencingOrder={7}&pageSize={8}";

                    response.setNextPageURL(formatNextPageURL(localServerURL + urlTemplate,
                                                              userId,
                                                              startEntityGUID,
                                                              instanceTypes,
                                                              fromEntityElement,
                                                              limitResultsByStatus,
                                                              limitResultsByClassification,
                                                              asOfTime,
                                                              sequencingProperty,
                                                              sequencingOrder,
                                                              pageSize));
                }
            }

        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (PagingErrorException error)
        {
            capturePagingErrorException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
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
     * @return EntityDetailResponse:
     * EntityDetail showing the new header plus the requested properties and classifications.  The entity will
     * not have any relationships at this stage or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                              hosting the metadata collection or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this entity's type or
     * ClassificationErrorException - one or more of the requested classifications are either not known or
     *                                           not defined for this entity type or
     * StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                       the requested status or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/instances/entity")

    public EntityDetailResponse addEntity(@PathVariable                   String                     userId,
                                          @RequestParam                   String                     entityTypeGUID,
                                          @RequestParam(required = false) InstanceProperties         initialProperties,
                                          @RequestParam(required = false) List<Classification>       initialClassifications,
                                          @RequestParam                   InstanceStatus             initialStatus)
    {
        final  String   methodName = "addEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.addEntity(userId,
                                                                 entityTypeGUID,
                                                                 initialProperties,
                                                                 initialClassifications,
                                                                 initialStatus));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (StatusNotSupportedException error)
        {
            captureStatusNotSupportedException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (ClassificationErrorException error)
        {
            captureClassificationErrorException(response, error);
        }

        return response;
    }



    /**
     * Create an entity proxy in the metadata collection.  This is used to store relationships that span metadata
     * repositories.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityProxy - details of entity to add.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the entity proxy is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this entity's type or
     * ClassificationErrorException - one or more of the requested classifications are either not known or
     *                                         not defined for this entity type or
     * StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status or
     * FunctionNotSupportedException - the repository does not support entity proxies as first class elements or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/instances/entity-proxy")

    public VoidResponse addEntityProxy(@PathVariable String       userId,
                                       @RequestParam EntityProxy  entityProxy)
    {
        final  String   methodName = "addEntityProxy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.addEntityProxy(userId, entityProxy);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }

        return response;
    }


    /**
     * Update the status for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - unique identifier (guid) for the requested entity.
     * @param newStatus - new InstanceStatus for the entity.
     * @return EntityDetailResponse:
     * EntityDetail showing the current entity header, properties and classifications or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection.
     * StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                      the requested status or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/status")

    public EntityDetailResponse updateEntityStatus(@PathVariable String           userId,
                                                   @PathVariable String           entityGUID,
                                                   @RequestParam InstanceStatus   newStatus)
    {
        final  String   methodName = "updateEntityStatus";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.updateEntityStatus(userId, entityGUID, newStatus));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (StatusNotSupportedException error)
        {
            captureStatusNotSupportedException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Update selected properties in an entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param properties - a list of properties to change.
     * @return EntityDetailResponse:
     * EntityDetail showing the resulting entity header, properties and classifications or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this entity's type or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation or
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/properties")

    public EntityDetailResponse updateEntityProperties(@PathVariable String               userId,
                                                       @PathVariable String               entityGUID,
                                                       @RequestParam InstanceProperties   properties)
    {
        final  String   methodName = "updateEntityProperties";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.updateEntityProperties(userId, entityGUID, properties));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }

        return response;
    }


    /**
     * Undo the last update to an entity and return the previous content.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetailResponse:
     * EntityDetail showing the resulting entity header, properties and classifications or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * FunctionNotSupportedException - the repository does not support undo or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/undo")

    public EntityDetailResponse undoEntityUpdate(@PathVariable String  userId,
                                                 @PathVariable String  entityGUID)
    {
        final  String   methodName = "undoEntityUpdate";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.undoEntityUpdate(userId, entityGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
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
     * @param obsoleteEntityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetailResponse
     * details of the deleted entity or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * FunctionNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                       soft-deletes - use purgeEntity() to remove the entity permanently or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{obsoleteEntityGUID}/delete")

    public EntityDetailResponse  deleteEntity(@PathVariable String    userId,
                                              @RequestParam String    typeDefGUID,
                                              @RequestParam String    typeDefName,
                                              @PathVariable String    obsoleteEntityGUID)
    {
        final  String   methodName = "deleteEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.deleteEntity(userId, typeDefGUID, typeDefName, obsoleteEntityGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (FunctionNotSupportedException error)
        {
            captureFunctionNotSupportedException(response, error);
        }

        return response;
    }


    /**
     * Permanently removes a deleted entity from the metadata collection.  This request can not be undone.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - unique identifier of the type of the entity to purge.
     * @param typeDefName - unique name of the type of the entity to purge.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * EntityNotDeletedException - the entity is not in DELETED status and so can not be purged or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{deletedEntityGUID}/purge")

    public VoidResponse purgeEntity(@PathVariable String    userId,
                                    @RequestParam String    typeDefGUID,
                                    @RequestParam String    typeDefName,
                                    @PathVariable String    deletedEntityGUID)
    {
        final  String   methodName = "purgeEntity";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.purgeEntity(userId, typeDefGUID, typeDefName, deletedEntityGUID);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotDeletedException error)
        {
            captureEntityNotDeletedException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Restore the requested entity to the state it was before it was deleted.
     *
     * @param userId - unique identifier for requesting user.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetailResponse:
     * EntityDetail showing the restored entity header, properties and classifications or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     * the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * EntityNotDeletedException - the entity is currently not in DELETED status and so it can not be restored or
     * FunctionNotSupportedException - the repository does not support soft-delete or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{deletedEntityGUID}/restore")

    public EntityDetailResponse restoreEntity(@PathVariable String    userId,
                                              @PathVariable String    deletedEntityGUID)
    {
        final  String   methodName = "restoreEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.restoreEntity(userId, deletedEntityGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (EntityNotDeletedException error)
        {
            captureEntityNotDeletedException(response, error);
        }

        return response;
    }


    /**
     * Add the requested classification to a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @param classificationProperties - list of properties to set in the classification.
     * @return EntityDetailResponse:
     * EntityDetail showing the resulting entity header, properties and classifications or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * ClassificationErrorException - the requested classification is either not known or not valid
     *                                         for the entity or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this classification type or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/classification/{classificationName}")

    public EntityDetailResponse classifyEntity(@PathVariable                   String               userId,
                                               @PathVariable                   String               entityGUID,
                                               @PathVariable                   String               classificationName,
                                               @RequestParam(required = false) InstanceProperties   classificationProperties)
    {
        final  String   methodName = "classifyEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.classifyEntity(userId,
                                                                      entityGUID,
                                                                      classificationName,
                                                                      classificationProperties));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (ClassificationErrorException error)
        {
            captureClassificationErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }

        return response;
    }


    /**
     * Remove a specific classification from an entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @return EntityDetailResponse:
     * EntityDetail showing the resulting entity header, properties and classifications or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection
     * ClassificationErrorException - the requested classification is not set on the entity or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/classification/{classificationName}/delete")

    public EntityDetailResponse declassifyEntity(@PathVariable String  userId,
                                                 @PathVariable String  entityGUID,
                                                 @PathVariable String  classificationName)
    {
        final  String   methodName = "declassifyEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.declassifyEntity(userId,
                                                                        entityGUID,
                                                                        classificationName));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (ClassificationErrorException error)
        {
            captureClassificationErrorException(response, error);
        }

        return response;
    }


    /**
     * Update one or more properties in one of an entity's classifications.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @param properties - list of properties for the classification.
     * @return EntityDetailResponse:
     * EntityDetail showing the resulting entity header, properties and classifications or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * ClassificationErrorException - the requested classification is not attached to the classification or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this classification type or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/classification/{classificationName}/properties")

    public EntityDetailResponse updateEntityClassification(@PathVariable String               userId,
                                                           @PathVariable String               entityGUID,
                                                           @PathVariable String               classificationName,
                                                           @RequestParam InstanceProperties   properties)
    {
        final  String   methodName = "updateEntityClassification";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.updateEntityClassification(userId,
                                                                                  entityGUID,
                                                                                  classificationName,
                                                                                  properties));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (ClassificationErrorException error)
        {
            captureClassificationErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }

        return response;
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
     * @return RelationshipResponse:
     * Relationship structure with the new header, requested entities and properties or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this relationship's type or
     * EntityNotKnownException - one of the requested entities is not known in the metadata collection or
     * StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/instances/relationship")

    public RelationshipResponse addRelationship(@PathVariable                   String               userId,
                                                @RequestParam                   String               relationshipTypeGUID,
                                                @RequestParam(required = false) InstanceProperties   initialProperties,
                                                @RequestParam                   String               entityOneGUID,
                                                @RequestParam                   String               entityTwoGUID,
                                                @RequestParam                   InstanceStatus       initialStatus)
    {
        final  String   methodName = "addRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.addRelationship(userId,
                                                                             relationshipTypeGUID,
                                                                             initialProperties,
                                                                             entityOneGUID,
                                                                             entityTwoGUID,
                                                                             initialStatus));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (StatusNotSupportedException error)
        {
            captureStatusNotSupportedException(response, error);
        }

        return response;
    }


    /**
     * Update the status of a specific relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param newStatus - new InstanceStatus for the relationship.
     * @return RelationshipResponse:
     * Resulting relationship structure with the new status set or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * RelationshipNotKnownException - the requested relationship is not known in the metadata collection or
     * StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation or
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{relationshipGUID}/status")

    public RelationshipResponse updateRelationshipStatus(@PathVariable String           userId,
                                                         @PathVariable String           relationshipGUID,
                                                         @RequestParam InstanceStatus   newStatus)
    {
        final  String   methodName = "updateRelationshipStatus";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.updateRelationshipStatus(userId,
                                                                                      relationshipGUID,
                                                                                      newStatus));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }
        catch (StatusNotSupportedException error)
        {
            captureStatusNotSupportedException(response, error);
        }

        return response;
    }


    /**
     * Update the properties of a specific relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param properties - list of the properties to update.
     * @return RelationshipResponse:
     * Resulting relationship structure with the new properties set or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * RelationshipNotKnownException - the requested relationship is not known in the metadata collection or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this relationship's type or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{relationshipGUID}/properties")

    public RelationshipResponse updateRelationshipProperties(@PathVariable String               userId,
                                                             @PathVariable String               relationshipGUID,
                                                             @RequestParam InstanceProperties   properties)
    {
        final  String   methodName = "updateRelationshipProperties";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.updateRelationshipProperties(userId,
                                                                                          relationshipGUID,
                                                                                          properties));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }

        return response;
    }


    /**
     * Undo the latest change to a relationship (either a change of properties or status).
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @return RelationshipResponse:
     * Relationship structure with the new current header, requested entities and properties or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored or
     * RelationshipNotKnownException - the requested relationship is not known in the metadata collection or
     * FunctionNotSupportedException - the repository does not support undo or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{relationshipGUID}/undo")

    public RelationshipResponse undoRelationshipUpdate(@PathVariable String  userId,
                                                       @PathVariable String  relationshipGUID)
    {
        final  String   methodName = "undoRelationshipUpdate";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.undoRelationshipUpdate(userId, relationshipGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
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
     * @return RelationshipResponse:
     * Updated relationship or
     * InvalidParameterException - one of the parameters is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     * the metadata collection is stored or
     * RelationshipNotKnownException - the requested relationship is not known in the metadata collection or
     * FunctionNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     soft-deletes or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{obsoleteRelationshipGUID}/delete")

    public RelationshipResponse deleteRelationship(@PathVariable String    userId,
                                                   @RequestParam String    typeDefGUID,
                                                   @RequestParam String    typeDefName,
                                                   @PathVariable String    obsoleteRelationshipGUID)
    {
        final  String   methodName = "deleteRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.deleteRelationship(userId,
                                                                                typeDefGUID,
                                                                                typeDefName,
                                                                                obsoleteRelationshipGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Permanently delete the relationship from the repository.  There is no means to undo this request.
     *
     * @param userId - unique identifier for requesting user.
     * @param typeDefGUID - unique identifier of the type of the relationship to purge.
     * @param typeDefName - unique name of the type of the relationship to purge.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - one of the parameters is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the requested relationship is not known in the metadata collection or
     * RelationshipNotDeletedException - the requested relationship is not in DELETED status or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{deletedRelationshipGUID}/purge")

    public VoidResponse purgeRelationship(@PathVariable String    userId,
                                          @RequestParam String    typeDefGUID,
                                          @RequestParam String    typeDefName,
                                          @PathVariable String    deletedRelationshipGUID)
    {
        final  String   methodName = "purgeRelationship";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.purgeRelationship(userId, typeDefGUID, typeDefName, deletedRelationshipGUID);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotDeletedException error)
        {
            captureRelationshipNotDeletedException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
    }


    /**
     * Restore a deleted relationship into the metadata collection.  The new status will be ACTIVE and the
     * restored details of the relationship are returned to the caller.
     *
     * @param userId - unique identifier for requesting user.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @return RelationshipResponse:
     * Relationship structure with the restored header, requested entities and properties or
     * InvalidParameterException - the guid is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     * the metadata collection is stored or
     * RelationshipNotKnownException - the requested relationship is not known in the metadata collection or
     * RelationshipNotDeletedException - the requested relationship is not in DELETED status or
     * FunctionNotSupportedException - the repository does not support soft-deletes
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{deletedRelationshipGUID}/restore")

    public RelationshipResponse restoreRelationship(@PathVariable String    userId,
                                                    @PathVariable String    deletedRelationshipGUID)
    {
        final  String   methodName = "restoreRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.restoreRelationship(userId, deletedRelationshipGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }
        catch (RelationshipNotDeletedException error)
        {
            captureRelationshipNotDeletedException(response, error);
        }

        return response;
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
     * @return EntityDetailResponse:
     * entity - new values for this entity, including the new guid or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * FunctionNotSupportedException - the repository does not support instance re-identification or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/identity")

    public EntityDetailResponse reIdentifyEntity(@PathVariable String     userId,
                                                 @RequestParam String     typeDefGUID,
                                                 @RequestParam String     typeDefName,
                                                 @PathVariable String     entityGUID,
                                                 @RequestParam String     newEntityGUID)
    {
        final  String   methodName = "reIdentifyEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.reIdentifyEntity(userId,
                                                                        typeDefGUID,
                                                                        typeDefName,
                                                                        entityGUID,
                                                                        newEntityGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
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
     * @return EntityDetailResponse:
     * entity - new values for this entity, including the new type information or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection or
     * PropertyErrorException - the instance's properties are not valid for the new type.
     * ClassificationErrorException - the instance's classifications are not valid for the new type.
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * FunctionNotSupportedException - the repository does not support instance re-typing or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/type")

    public EntityDetailResponse reTypeEntity(@PathVariable String         userId,
                                             @PathVariable String         entityGUID,
                                             @RequestParam TypeDefSummary currentTypeDefSummary,
                                             @RequestParam TypeDefSummary newTypeDefSummary)
    {
        final  String   methodName = "reTypeEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.reTypeEntity(userId,
                                                                    entityGUID,
                                                                    currentTypeDefSummary,
                                                                    newTypeDefSummary));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (ClassificationErrorException error)
        {
            captureClassificationErrorException(response, error);
        }

        return response;
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
     * @return EntityDetailResponse:
     * entity - new values for this entity, including the new home information or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * FunctionNotSupportedException - the repository does not support instance re-homing or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entity/{entityGUID}/home/{homeMetadataCollectionId}")

    public EntityDetailResponse reHomeEntity(@PathVariable String         userId,
                                             @PathVariable String         entityGUID,
                                             @RequestParam String         typeDefGUID,
                                             @RequestParam String         typeDefName,
                                             @PathVariable String         homeMetadataCollectionId,
                                             @RequestParam String         newHomeMetadataCollectionId)
    {
        final  String   methodName = "reHomeEntity";

        EntityDetailResponse response = new EntityDetailResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setEntity(localMetadataCollection.reHomeEntity(userId,
                                                                    entityGUID,
                                                                    typeDefGUID,
                                                                    typeDefName,
                                                                    homeMetadataCollectionId,
                                                                    newHomeMetadataCollectionId));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }

        return response;
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
     * @return RelationshipResponse:
     * relationship - new values for this relationship, including the new guid or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                         metadata collection or
     * FunctionNotSupportedException - the repository does not support instance re-identification or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{relationshipGUID}/identity")

    public RelationshipResponse reIdentifyRelationship(@PathVariable String     userId,
                                                       @RequestParam String     typeDefGUID,
                                                       @RequestParam String     typeDefName,
                                                       @PathVariable String     relationshipGUID,
                                                       @RequestParam String     newRelationshipGUID)
    {
        final  String   methodName = "reIdentifyRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.reIdentifyRelationship(userId,
                                                                                    typeDefGUID,
                                                                                    typeDefName,
                                                                                    relationshipGUID,
                                                                                    newRelationshipGUID));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
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
     * @return RelationshipResponse:
     * relationship - new values for this relationship, including the new type information or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection or
     * PropertyErrorException - the instance's properties are not valid for th new type.
     * RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                         metadata collection or
     * FunctionNotSupportedException - the repository does not support instance re-typing or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{relationshipGUID}/type")

    public RelationshipResponse reTypeRelationship(@PathVariable String         userId,
                                                   @PathVariable String         relationshipGUID,
                                                   @RequestParam TypeDefSummary currentTypeDefSummary,
                                                   @RequestParam TypeDefSummary newTypeDefSummary)
    {
        final  String   methodName = "reTypeRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.reTypeRelationship(userId,
                                                                                relationshipGUID,
                                                                                currentTypeDefSummary,
                                                                                newTypeDefSummary));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }

        return response;
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
     * @return RelationshipResponse:
     * relationship - new values for this relationship, including the new home information or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                         metadata collection or
     * FunctionNotSupportedException - the repository does not support instance re-homing or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationship/{relationshipGUID}/home")

    public RelationshipResponse reHomeRelationship(@PathVariable String   userId,
                                                   @PathVariable String   relationshipGUID,
                                                   @RequestParam String   typeDefGUID,
                                                   @RequestParam String   typeDefName,
                                                   @RequestParam String   homeMetadataCollectionId,
                                                   @RequestParam String   newHomeMetadataCollectionId)
    {
        final  String   methodName = "reHomeRelationship";

        RelationshipResponse response = new RelationshipResponse();

        try
        {
            validateLocalRepository(methodName);

            response.setRelationship(localMetadataCollection.reHomeRelationship(userId,
                                                                                relationshipGUID,
                                                                                typeDefGUID,
                                                                                typeDefName,
                                                                                homeMetadataCollectionId,
                                                                                newHomeMetadataCollectionId));
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }


        return response;
    }



    /* ======================================================================
     * Group 6: Local house-keeping of reference metadata instances
     */


    /**
     * Save the entity as a reference copy.  The id of the home metadata collection is already set up in the
     * entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entity - details of the entity to save.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the entity is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this entity's type or
     * HomeEntityException - the entity belongs to the local repository so creating a reference
     *                               copy would be invalid or
     * EntityConflictException - the new entity conflicts with an existing entity or
     * InvalidEntityException - the new entity has invalid contents or
     * FunctionNotSupportedException - the repository does not support instance reference copies or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entities/reference-copy")

    public VoidResponse saveEntityReferenceCopy(@PathVariable String         userId,
                                                @RequestParam EntityDetail   entity)
    {
        final  String   methodName = "saveEntityReferenceCopy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.saveEntityReferenceCopy(userId, entity);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeDefErrorException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (HomeEntityException error)
        {
            captureHomeEntityException(response, error);
        }
        catch (EntityConflictException error)
        {
            captureEntityConflictException(response, error);
        }
        catch (InvalidEntityException error)
        {
            captureInvalidEntityException(response, error);
        }

        return response;
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
     * @return VoidResponse:
     * void or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * HomeEntityException - the entity belongs to the local repository so creating a reference
     *                               copy would be invalid or
     * FunctionNotSupportedException - the repository does not support instance reference copies or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entities/reference-copy/{entityGUID}/purge")

    public VoidResponse purgeEntityReferenceCopy(@PathVariable String   userId,
                                                 @PathVariable String   entityGUID,
                                                 @RequestParam String   typeDefGUID,
                                                 @RequestParam String   typeDefName,
                                                 @RequestParam String   homeMetadataCollectionId)
    {
        final  String   methodName = "purgeEntityReferenceCopy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.purgeEntityReferenceCopy(userId,
                                                             entityGUID,
                                                             typeDefGUID,
                                                             typeDefName,
                                                             homeMetadataCollectionId);        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (HomeEntityException error)
        {
            captureHomeEntityException(response, error);
        }

        return response;
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified entity sends out the details of this entity so the local repository can create a reference copy.
     *
     * @param userId - unique identifier for requesting server.
     * @param entityGUID - unique identifier of requested entity.
     * @param typeDefGUID - unique identifier of requested entity's TypeDef.
     * @param typeDefName - unique name of requested entity's TypeDef.
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * EntityNotKnownException - the entity identified by the guid is not found in the metadata collection or
     * HomeEntityException - the entity belongs to the local repository so creating a reference
     *                               copy would be invalid or
     * FunctionNotSupportedException - the repository does not support instance reference copies or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/entities/reference-copy/{entityGUID}/refresh")

    public VoidResponse refreshEntityReferenceCopy(@PathVariable String   userId,
                                                   @PathVariable String   entityGUID,
                                                   @RequestParam String   typeDefGUID,
                                                   @RequestParam String   typeDefName,
                                                   @RequestParam String   homeMetadataCollectionId)
    {
        final  String   methodName = "refreshEntityReferenceCopy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.refreshEntityReferenceCopy(userId,
                                                               entityGUID,
                                                               typeDefGUID,
                                                               typeDefName,
                                                               homeMetadataCollectionId);        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (HomeEntityException error)
        {
            captureHomeEntityException(response, error);
        }

        return response;
    }


    /**
     * Save the relationship as a reference copy.  The id of the home metadata collection is already set up in the
     * relationship.
     *
     * @param userId - unique identifier for requesting userId.
     * @param relationship - relationship to save.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - the relationship is null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                            hosting the metadata collection or
     * EntityNotKnownException - one of the entities identified by the relationship is not found in the
     *                                   metadata collection or
     * PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this relationship's type or
     * HomeRelationshipException - the relationship belongs to the local repository so creating a reference
     *                                     copy would be invalid or
     * RelationshipConflictException - the new relationship conflicts with an existing relationship.
     * InvalidRelationshipException - the new relationship has invalid contents or
     * FunctionNotSupportedException - the repository does not support instance reference copies or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationships/reference-copy")

    public VoidResponse saveRelationshipReferenceCopy(@PathVariable String         userId,
                                                      @RequestParam Relationship   relationship)
    {
        final  String   methodName = "saveRelationshipReferenceCopy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.saveRelationshipReferenceCopy(userId, relationship);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (InvalidRelationshipException error)
        {
            captureInvalidRelationshipException(response, error);
        }
        catch (EntityNotKnownException error)
        {
            captureEntityNotKnownException(response, error);
        }
        catch (HomeRelationshipException error)
        {
            captureHomeRelationshipException(response, error);
        }
        catch (PropertyErrorException error)
        {
            capturePropertyErrorException(response, error);
        }
        catch (TypeErrorException error)
        {
            captureTypeDefErrorException(response, error);
        }
        catch (RelationshipConflictException error)
        {
            captureRelationshipConflictException(response, error);
        }

        return response;
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
     * @return VoidResponse:
     * void or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the relationship identifier is not recognized or
     * HomeRelationshipException - the relationship belongs to the local repository so creating a reference
     *                                     copy would be invalid or
     * FunctionNotSupportedException - the repository does not support instance reference copies or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationships/reference-copy/{relationshipGUID}/purge")

    public VoidResponse purgeRelationshipReferenceCopy(@PathVariable String   userId,
                                                       @PathVariable String   relationshipGUID,
                                                       @RequestParam String   typeDefGUID,
                                                       @RequestParam String   typeDefName,
                                                       @RequestParam String   homeMetadataCollectionId)
    {
        final  String   methodName = "purgeRelationshipReferenceCopy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.purgeRelationshipReferenceCopy(userId,
                                                                   relationshipGUID,
                                                                   typeDefGUID,
                                                                   typeDefName,
                                                                   homeMetadataCollectionId);        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }
        catch (HomeRelationshipException error)
        {
            captureHomeRelationshipException(response, error);
        }

        return response;
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified relationship sends out the details of this relationship so the local repository can create a
     * reference copy.
     *
     * @param userId - unique identifier for requesting server.
     * @param relationshipGUID - unique identifier of the relationship.
     * @param typeDefGUID - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @return VoidResponse:
     * void or
     * InvalidParameterException - one of the parameters is invalid or null or
     * RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored or
     * RelationshipNotKnownException - the relationship identifier is not recognized or
     * HomeRelationshipException - the relationship belongs to the local repository so creating a reference
     *                                     copy would be invalid or
     * FunctionNotSupportedException - the repository does not support instance reference copies or
     * UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/instances/relationships/reference-copy/{relationshipGUID}/refresh")

    public VoidResponse refreshRelationshipReferenceCopy(@PathVariable String userId,
                                                         @PathVariable String relationshipGUID,
                                                         @RequestParam String typeDefGUID,
                                                         @RequestParam String typeDefName,
                                                         @RequestParam String homeMetadataCollectionId)
    {
        final  String   methodName = "refreshRelationshipReferenceCopy";

        VoidResponse response = new VoidResponse();

        try
        {
            validateLocalRepository(methodName);

            localMetadataCollection.refreshRelationshipReferenceCopy(userId,
                                                                     relationshipGUID,
                                                                     typeDefGUID,
                                                                     typeDefName,
                                                                     homeMetadataCollectionId);
        }
        catch (RepositoryErrorException  error)
        {
            captureRepositoryErrorException(response, error);
        }
        catch (FunctionNotSupportedException  error)
        {
            captureFunctionNotSupportedException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }
        catch (InvalidParameterException error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (RelationshipNotKnownException error)
        {
            captureRelationshipNotKnownException(response, error);
        }
        catch (HomeRelationshipException error)
        {
            captureHomeRelationshipException(response, error);
        }

        return response;
    }




    /*
     * =============================================================
     * Private methods
     */


    /**
     * Validate that the local repository is available.
     *
     * @param methodName - method being called
     * @throws RepositoryErrorException - null local repository
     */
    private void validateLocalRepository(String methodName) throws RepositoryErrorException
    {
        /*
         * If the local repository is not set up then do not attempt to process the request.
         */
        if (localMetadataCollection == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NO_LOCAL_REPOSITORY;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(methodName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureUserNotAuthorizedException(OMRSRESTAPIResponse response, UserNotAuthorizedException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureFunctionNotSupportedException(OMRSRESTAPIResponse response, FunctionNotSupportedException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureRepositoryErrorException(OMRSRESTAPIResponse response, RepositoryErrorException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureInvalidParameterException(OMRSRESTAPIResponse response, InvalidParameterException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureInvalidTypeDefException(OMRSRESTAPIResponse response, InvalidTypeDefException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefConflictException(OMRSRESTAPIResponse response, TypeDefConflictException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefNotSupportedException(OMRSRESTAPIResponse response, TypeDefNotSupportedException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureHomeRelationshipException(OMRSRESTAPIResponse response, HomeRelationshipException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureRelationshipNotKnownException(OMRSRESTAPIResponse response, RelationshipNotKnownException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureInvalidRelationshipException(OMRSRESTAPIResponse response, InvalidRelationshipException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureRelationshipConflictException(OMRSRESTAPIResponse response, RelationshipConflictException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefErrorException(OMRSRESTAPIResponse response, TypeErrorException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void capturePropertyErrorException(OMRSRESTAPIResponse response, PropertyErrorException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureEntityNotKnownException(OMRSRESTAPIResponse response, EntityNotKnownException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureHomeEntityException(OMRSRESTAPIResponse response, HomeEntityException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefNotKnownException(TypeDefResponse response, TypeDefNotKnownException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefNotKnown(OMRSRESTAPIResponse response, TypeDefNotKnownException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefKnownException(OMRSRESTAPIResponse response, TypeDefKnownException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefInUseException(OMRSRESTAPIResponse response, TypeDefInUseException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeDefNotKnownException(OMRSRESTAPIResponse response, TypeDefNotKnownException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureEntityProxyOnlyException(OMRSRESTAPIResponse response, EntityProxyOnlyException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureClassificationErrorException(OMRSRESTAPIResponse response, ClassificationErrorException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void capturePagingErrorException(OMRSRESTAPIResponse response, PagingErrorException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureTypeErrorException(OMRSRESTAPIResponse response, TypeErrorException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureStatusNotSupportedException(OMRSRESTAPIResponse response, StatusNotSupportedException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureEntityNotDeletedException(OMRSRESTAPIResponse response, EntityNotDeletedException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }



    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureRelationshipNotDeletedException(OMRSRESTAPIResponse response, RelationshipNotDeletedException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }



    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureInvalidEntityException(OMRSRESTAPIResponse response, InvalidEntityException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureEntityConflictException(OMRSRESTAPIResponse response, EntityConflictException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     * @param exceptionClassName - class name of the exception to recreate
     */
    private void captureCheckedException(OMRSRESTAPIResponse      response,
                                         OMRSCheckedExceptionBase error,
                                         String                   exceptionClassName)
    {
        response.setRelatedHTTPCode(error.getReportedHTTPCode());
        response.setExceptionClassName(exceptionClassName);
        response.setExceptionErrorMessage(error.getErrorMessage());
        response.setExceptionSystemAction(error.getReportedSystemAction());
        response.setExceptionUserAction(error.getReportedUserAction());
    }


    /**
     * Format the url for the next page of a request that includes paging.
     *
     * @param urlTemplate - template of the request
     * @param parameters - parameters to include in the url
     * @return formatted string
     */
    private String  formatNextPageURL(String    urlTemplate,
                                      Object... parameters)
    {
        MessageFormat mf     = new MessageFormat(urlTemplate);

        return mf.format(parameters);
    }
}
