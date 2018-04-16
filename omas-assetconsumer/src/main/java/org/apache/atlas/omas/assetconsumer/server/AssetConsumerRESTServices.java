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
package org.apache.atlas.omas.assetconsumer.server;


import org.apache.atlas.ocf.properties.CommentType;
import org.apache.atlas.omag.admin.server.OMAGAccessServiceRegistration;
import org.apache.atlas.omag.configuration.registration.AccessServiceDescription;
import org.apache.atlas.omag.configuration.registration.AccessServiceOperationalStatus;
import org.apache.atlas.omag.configuration.registration.AccessServiceRegistration;
import org.apache.atlas.omas.assetconsumer.admin.AssetConsumerAdmin;
import org.apache.atlas.omas.assetconsumer.ffdc.AssetConsumerErrorCode;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.*;
import org.apache.atlas.omas.assetconsumer.server.properties.AssetConsumerOMASAPIResponse;
import org.apache.atlas.omas.assetconsumer.server.properties.ConnectionResponse;
import org.apache.atlas.omas.assetconsumer.server.properties.GUIDResponse;
import org.apache.atlas.omas.assetconsumer.server.properties.VoidResponse;
import org.apache.atlas.ocf.properties.StarRating;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;


/**
 * The AssetConsumerRESTServices provides the server-side implementation of the AssetConsumer Open Metadata
 * Assess Service (OMAS).  This interface provides connections to assets and APIs for adding feedback
 * on the asset.
 */
@RestController
@RequestMapping("/omag/omas/asset-consumer")
public class AssetConsumerRESTServices
{
    static private String                     accessServiceName = null;
    static private OMRSRepositoryConnector    repositoryConnector = null;

    private static final Logger log = LoggerFactory.getLogger(AssetConsumerRESTServices.class);

    /**
     * Provide a connector to the REST Services.
     *
     * @param accessServiceName - name of this access service
     * @param repositoryConnector - OMRS Repository Connector to the property server.
     */
    static public void setRepositoryConnector(String                   accessServiceName,
                                              OMRSRepositoryConnector  repositoryConnector)
    {
        AssetConsumerRESTServices.accessServiceName = accessServiceName;
        AssetConsumerRESTServices.repositoryConnector = repositoryConnector;
    }

    /**
     * Default constructor
     */
    public AssetConsumerRESTServices()
    {
        AccessServiceDescription   myDescription = AccessServiceDescription.ASSET_CONSUMER_OMAS;
        AccessServiceRegistration  myRegistration = new AccessServiceRegistration(myDescription.getAccessServiceCode(),
                                                                                  myDescription.getAccessServiceName(),
                                                                                  myDescription.getAccessServiceDescription(),
                                                                                  myDescription.getAccessServiceWiki(),
                                                                                  AccessServiceOperationalStatus.ENABLED,
                                                                                  AssetConsumerAdmin.class.getName());
        OMAGAccessServiceRegistration.registerAccessService(myRegistration);
    }



    /**
     * Returns the connection object corresponding to the supplied connection name.
     *
     * @param userId - String - userId of user making request.
     * @param name - this may be the qualifiedName or displayName of the connection.
     *
     * @return ConnectionResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * UnrecognizedConnectionNameException - there is no connection defined for this name.
     * AmbiguousConnectionNameException - there is more than one connection defined for this name.
     * PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/connections/by-name/{name}")

    ConnectionResponse getConnectionByName(@PathVariable String   userId,
                                           @PathVariable String   name)
    {
        final String        methodName = "getConnectionByName";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        ConnectionResponse  response = new ConnectionResponse();

        try
        {
            this.validateInitialization(methodName);

            ConnectionHandler   connectionHandler = new ConnectionHandler(accessServiceName,
                                                                          repositoryConnector);

            response.setConnection(connectionHandler.getConnectionByName(userId, name));
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UnrecognizedConnectionNameException  error)
        {
            captureUnrecognizedConnectionNameException(response, error);
        }
        catch (AmbiguousConnectionNameException  error)
        {
            captureAmbiguousConnectionNameException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Returns the connection object corresponding to the supplied connection GUID.
     *
     * @param userId - String - userId of user making request.
     * @param guid - the unique id for the connection within the property server.
     *
     * @return ConnectionResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the metadata repository.
     * PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/connections/{guid}")

    public ConnectionResponse getConnectionByGUID(@PathVariable String     userId,
                                                  @PathVariable String     guid)
    {
        final String        methodName = "getConnectionByGUID";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        ConnectionResponse  response = new ConnectionResponse();

        try
        {
            this.validateInitialization(methodName);

            ConnectionHandler   connectionHandler = new ConnectionHandler(accessServiceName,
                                                                          repositoryConnector);

            response.setConnection(connectionHandler.getConnectionByGUID(userId, guid));
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UnrecognizedConnectionGUIDException  error)
        {
            captureUnrecognizedConnectionGUIDException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Creates an Audit log record for the asset.  This log record is stored in the Asset's Audit Log.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the asset.
     * @param connectorInstanceId - String - (optional) id of connector in use (if any).
     * @param connectionName - String - (optional) name of the connection (extracted from the connector).
     * @param connectorType - String - (optional) type of connector in use (if any).
     * @param contextId - String - (optional) function name, or processId of the activity that the caller is performing.
     * @param message - log record content.
     *
     * @return VoidResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/assets/{guid}/log-record")

    public VoidResponse  addLogMessageToAsset(@PathVariable                   String      userId,
                                              @PathVariable                   String      guid,
                                              @RequestParam(required = false) String      connectorInstanceId,
                                              @RequestParam(required = false) String      connectionName,
                                              @RequestParam(required = false) String      connectorType,
                                              @RequestParam(required = false) String      contextId,
                                              @RequestParam(required = false) String      message)
    {
        final String        methodName = "addLogMessageToAsset";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        VoidResponse  response = new VoidResponse();

        try
        {
            this.validateInitialization(methodName);

            AuditLogHandler   auditLogHandler = new AuditLogHandler(accessServiceName,
                                                                    repositoryConnector);

            auditLogHandler.addLogMessageToAsset(userId,
                                                 guid,
                                                 connectorInstanceId,
                                                 connectionName,
                                                 connectorType,
                                                 contextId,
                                                 message);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Adds a new public tag to the asset's properties.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the asset.
     * @param tagName - String - name of the tag.
     * @param tagDescription - String - (optional) description of the tag.  Setting a description, particularly in
     *                       a public tag makes the tag more valuable to other users and can act as an embryonic
     *                       glossary term.
     *
     * @return GUIDResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/assets/{guid}/tags")

    public GUIDResponse addTagToAsset(@PathVariable                   String      userId,
                                      @PathVariable                   String      guid,
                                      @RequestParam                   String      tagName,
                                      @RequestParam(required = false) String      tagDescription)
    {
        final String        methodName = "addTagToAsset";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        GUIDResponse  response = new GUIDResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler   feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                    repositoryConnector);

            feedbackHandler.addTagToAsset(userId, guid, tagName, tagDescription);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Adds a new private tag to the asset's properties.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the asset.
     * @param tagName - String - name of the tag.
     * @param tagDescription - String - (optional) description of the tag.  Setting a description, particularly in
     *                       a public tag makes the tag more valuable to other users and can act as an embryonic
     *                       glossary term.
     *
     * @return GUIDResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/assets/{guid}/tags/private")

    public GUIDResponse addPrivateTagToAsset(@PathVariable                   String      userId,
                                             @PathVariable                   String      guid,
                                             @RequestParam                   String      tagName,
                                             @RequestParam(required = false) String      tagDescription)
    {
        final String        methodName = "addPrivateTagToAsset";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        GUIDResponse  response = new GUIDResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler   feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                    repositoryConnector);

            feedbackHandler.addPrivateTagToAsset(userId, guid, tagName, tagDescription);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Adds a rating to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the asset.
     * @param starRating - StarRating  - enumeration for none, one to five stars.
     * @param review - String - user review of asset.
     *
     * @return GUIDResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/assets/{guid}/ratings/")

    public GUIDResponse addRatingToAsset(@PathVariable String     userId,
                                         @PathVariable String     guid,
                                         @RequestParam StarRating starRating,
                                         @RequestParam String     review)
    {
        final String        methodName = "addRatingToAsset";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        GUIDResponse  response = new GUIDResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler   feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                    repositoryConnector);

            feedbackHandler.addRatingToAsset(userId, guid, starRating, review);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Adds a "Like" to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the asset
     *
     * @return GUIDResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/assets/{guid}/likes/")

    public GUIDResponse addLikeToAsset(@PathVariable String       userId,
                                       @PathVariable String       guid)
    {
        final String        methodName = "addLikeToAsset";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        GUIDResponse  response = new GUIDResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler   feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                    repositoryConnector);

            feedbackHandler.addLikeToAsset(userId, guid);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Adds a comment to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the asset.
     * @param commentType - type of comment enum.
     * @param commentText - String - the text of the comment.
     *
     * @return GUIDResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/assets/{guid}/comments/")

    public GUIDResponse addCommentToAsset(@PathVariable String      userId,
                                          @PathVariable String      guid,
                                          @RequestParam CommentType commentType,
                                          @RequestParam String      commentText)
    {
        final String        methodName = "addCommentToAsset";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        GUIDResponse  response = new GUIDResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler   feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                    repositoryConnector);

            feedbackHandler.addCommentToAsset(userId, guid, commentType, commentText);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Adds a comment to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param commentGUID - String - unique id for an existing comment.  Used to add a reply to a comment.
     * @param commentType - type of comment enum.
     * @param commentText - String - the text of the comment.
     *
     * @return GUIDResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem adding the asset properties to
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/{userId}/comments/{commentGUID}/reply")

    public GUIDResponse addCommentReply(@PathVariable String      userId,
                                        @PathVariable String      commentGUID,
                                        @RequestParam CommentType commentType,
                                        @RequestParam String      commentText)
    {
        final String        methodName = "addCommentReply";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        GUIDResponse  response = new GUIDResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler   feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                    repositoryConnector);

            feedbackHandler.addCommentReply(userId, commentGUID, commentType, commentText);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Removes a tag from the asset that was added by this user.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the tag.
     *
     * @return VoidResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem updating the asset properties in
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/tags/{guid}/delete")

    public VoidResponse   removeTag(@PathVariable String     userId,
                                    @PathVariable String     guid)
    {
        final String        methodName = "removeTag";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        VoidResponse  response = new VoidResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                  repositoryConnector);

            feedbackHandler.removeTagFromAsset(userId, guid);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Removes a tag from the asset that was added by this user.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the tag.
     *
     * @return VoidResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem updating the asset properties in
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/tags/private/{guid}/delete")

    public VoidResponse   removePrivateTag(@PathVariable String     userId,
                                           @PathVariable String     guid)
    {
        final String        methodName = "removePrivateTag";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        VoidResponse  response = new VoidResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                  repositoryConnector);

            feedbackHandler.removePrivateTagFromAsset(userId, guid);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Removes of a star rating that was added to the asset by this user.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the rating object
     *
     * @return VoidResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem updating the asset properties in
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/ratings/{guid}/delete")

    public VoidResponse   removeRating(@PathVariable String     userId,
                                       @PathVariable String     guid)
    {
        final String        methodName = "removeRating";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        VoidResponse  response = new VoidResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                  repositoryConnector);

            feedbackHandler.removeRatingFromAsset(userId, guid);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Removes a "Like" added to the asset by this user.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the like object
     * @return VoidResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem updating the asset properties in
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/likes/{guid}/delete")

    public VoidResponse   removeLike(@PathVariable String     userId,
                                              @PathVariable String     guid)
    {
        final String        methodName = "removeLike";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        VoidResponse  response = new VoidResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                  repositoryConnector);

            feedbackHandler.removeLikeFromAsset(userId, guid);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }


    /**
     * Removes a comment added to the asset by this user.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for the comment object
     * @return VoidResponse or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem updating the asset properties in
     *                                   the metadata repository.
     * UserNotAuthorizedException - the user does not have permission to perform this request.
     */
    @RequestMapping(method = RequestMethod.PATCH, path = "/{userId}/comments/{guid}/delete")

    public VoidResponse   removeComment(@PathVariable String     userId,
                                        @PathVariable String     guid)
    {
        final String        methodName = "removeComment";

        if (log.isDebugEnabled())
        {
            log.debug("Calling method: " + methodName);
        }

        VoidResponse  response = new VoidResponse();

        try
        {
            this.validateInitialization(methodName);

            FeedbackHandler feedbackHandler = new FeedbackHandler(accessServiceName,
                                                                  repositoryConnector);

            feedbackHandler.removeCommentFromAsset(userId, guid);
        }
        catch (InvalidParameterException  error)
        {
            captureInvalidParameterException(response, error);
        }
        catch (PropertyServerException  error)
        {
            capturePropertyServerException(response, error);
        }
        catch (UserNotAuthorizedException error)
        {
            captureUserNotAuthorizedException(response, error);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Returning from method: " + methodName + " with response: " + response.toString());
        }

        return response;
    }

    /* ==========================
     * Support methods
     * ==========================
     */


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     * @param exceptionClassName - class name of the exception to recreate
     */
    private void captureCheckedException(AssetConsumerOMASAPIResponse      response,
                                         AssetConsumerCheckedExceptionBase error,
                                         String                            exceptionClassName)
    {
        response.setRelatedHTTPCode(error.getReportedHTTPCode());
        response.setExceptionClassName(exceptionClassName);
        response.setExceptionErrorMessage(error.getErrorMessage());
        response.setExceptionSystemAction(error.getReportedSystemAction());
        response.setExceptionUserAction(error.getReportedUserAction());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureAmbiguousConnectionNameException(AssetConsumerOMASAPIResponse     response,
                                                         AmbiguousConnectionNameException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureInvalidParameterException(AssetConsumerOMASAPIResponse response,
                                                  InvalidParameterException    error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void capturePropertyServerException(AssetConsumerOMASAPIResponse     response,
                                                PropertyServerException          error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureUnrecognizedConnectionGUIDException(AssetConsumerOMASAPIResponse        response,
                                                            UnrecognizedConnectionGUIDException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureUnrecognizedConnectionNameException(AssetConsumerOMASAPIResponse        response,
                                                            UnrecognizedConnectionNameException error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Set the exception information into the response.
     *
     * @param response - REST Response
     * @param error returned response.
     */
    private void captureUserNotAuthorizedException(AssetConsumerOMASAPIResponse response,
                                                  UserNotAuthorizedException    error)
    {
        captureCheckedException(response, error, error.getClass().getName());
    }


    /**
     * Validate that this access service has been initialized before attempting to process a request.
     *
     * @param methodName - name of method called.
     * @throws PropertyServerException - not initialized
     */
    private void validateInitialization(String  methodName) throws PropertyServerException
    {
        if (repositoryConnector == null)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.SERVICE_NOT_INITIALIZED;
            String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(methodName);

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                                          this.getClass().getName(),
                                                          methodName,
                                                          errorMessage,
                                                          errorCode.getSystemAction(),
                                                          errorCode.getUserAction());
        }
    }
}
