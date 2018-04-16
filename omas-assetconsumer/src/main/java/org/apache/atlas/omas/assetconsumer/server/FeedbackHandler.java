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
import org.apache.atlas.ocf.properties.StarRating;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.InvalidParameterException;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.PropertyServerException;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.UserNotAuthorizedException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

import java.util.Date;

/**
 * FeedbackHandler manages the creation of asset feedback (likes, ratings, comments and tags) in the
 * property server.
 */
public class FeedbackHandler
{
    private static final String informalTagTypeName                  = "InformalTag";
    private static final String informalTagTypeGUID                  = "ba846a7b-2955-40bf-952b-2793ceca090a";
    private static final String privateTagTypeName                   = "PrivateTag";
    private static final String privateTagTypeGUID                   = "9b3f5443-2475-4522-bfda-8f1f17e9a6c3";
    private static final String tagNamePropertyName                  = "TagName";
    private static final String tagDescriptionPropertyName           = "TagDescription";
    private static final String attachedTagTypeGUID                  = "4b1641c4-3d1a-4213-86b2-d6968b6c65ab";

    private static final String likeTypeName                         = "Like";
    private static final String likeTypeGUID                         = "deaa5ca0-47a0-483d-b943-d91c76744e01";
    private static final String attachedLikeTypeGUID                 = "e2509715-a606-415d-a995-61d00503dad4";

    private static final String ratingTypeName                       = "Rating";
    private static final String ratingTypeGUID                       = "7299d721-d17f-4562-8286-bcd451814478";
    private static final String starsPropertyName                    = "stars";
    private static final String reviewPropertyName                   = "review";
    private static final String attachedRatingTypeGUID               = "0aaad9e9-9cc5-4ad8-bc2e-c1099bab6344";

    private static final String commentTypeName                      = "Comment";
    private static final String commentTypeGUID                      = "1a226073-9c84-40e4-a422-fbddb9b84278";
    private static final String qualifiedNamePropertyName            = "qualifiedName";
    private static final String commentPropertyName                  = "comment";
    private static final String commentTypePropertyName              = "commentType";
    private static final String attachedCommentTypeGUID              = "0d90501b-bf29-4621-a207-0c8c953bdac9";




    private String                  serviceName;

    private String                  serverName = null;
    private OMRSRepositoryHelper    repositoryHelper = null;
    private ErrorHandler            errorHandler     = null;


    /**
     * Construct the feedback handler with a link to the property server's connector and this access service's
     * official name.
     *
     * @param serviceName - name of this service
     * @param repositoryConnector - connector to the property server.
     */
    public FeedbackHandler(String                  serviceName,
                           OMRSRepositoryConnector repositoryConnector)
    {
        this.serviceName = serviceName;

        if (repositoryConnector != null)
        {
            this.repositoryHelper = repositoryConnector.getRepositoryHelper();
            this.serverName = repositoryConnector.getServerName();
            errorHandler = new ErrorHandler(repositoryConnector);
        }
    }


    /**
     * Adds a new public tag to the asset's properties.
     *
     * @param userId         - String - userId of user making request.
     * @param assetGUID      - String - unique id for the asset.
     * @param tagName        - String - name of the tag.
     * @param tagDescription - String - (optional) description of the tag.  Setting a description, particularly in
     *                       a public tag makes the tag more valuable to other users and can act as an embryonic
     *                       glossary term.
     * @return String - GUID for new tag.
     * @throws InvalidParameterException  - one of the parameters is null or invalid.
     * @throws PropertyServerException    - There is a problem adding the asset properties to
     *                                    the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public String addTagToAsset(String userId,
                                String assetGUID,
                                String tagName,
                                String tagDescription) throws InvalidParameterException,
                                                              PropertyServerException,
                                                              UserNotAuthorizedException
    {
        final String methodName = "addTagToAsset";

        return this.addTagToAsset(informalTagTypeGUID,
                                  userId,
                                  assetGUID,
                                  tagName,
                                  tagDescription,
                                  methodName);
    }


    /**
     * Adds a new private tag to the asset's properties.
     *
     * @param userId         - String - userId of user making request.
     * @param assetGUID      - String - unique id for the asset.
     * @param tagName        - String - name of the tag.
     * @param tagDescription - String - (optional) description of the tag.  Setting a description, particularly in
     *                       a public tag makes the tag more valuable to other users and can act as an embryonic
     *                       glossary term.
     * @return String - GUID for new tag.
     * @throws InvalidParameterException  - one of the parameters is null or invalid.
     * @throws PropertyServerException    - There is a problem adding the asset properties to
     *                                    the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public String addPrivateTagToAsset(String userId,
                                       String assetGUID,
                                       String tagName,
                                       String tagDescription) throws InvalidParameterException,
                                                                     PropertyServerException,
                                                                     UserNotAuthorizedException
    {
        final String methodName = "addPrivateTagToAsset";

        return this.addTagToAsset(privateTagTypeGUID,
                                  userId,
                                  assetGUID,
                                  tagName,
                                  tagDescription,
                                  methodName);
    }


    /**
     * Adds a new public tag to the asset's properties.
     *
     * @param userId         - String - userId of user making request.
     * @param assetGUID      - String - unique id for the asset.
     * @param tagName        - String - name of the tag.
     * @param tagDescription - String - (optional) description of the tag.  Setting a description, particularly in
     *                       a public tag makes the tag more valuable to other users and can act as an embryonic
     *                       glossary term.
     * @return String - GUID for new tag.
     * @throws InvalidParameterException  - one of the parameters is null or invalid.
     * @throws PropertyServerException    - There is a problem adding the asset properties to
     *                                    the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    private String addTagToAsset(String tagTypeGUID,
                                 String userId,
                                 String assetGUID,
                                 String tagName,
                                 String tagDescription,
                                 String methodName) throws InvalidParameterException,
                                                           PropertyServerException,
                                                           UserNotAuthorizedException
    {
        final String guidParameter = "assetGUID";
        final String nameParameter = "tagName";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(assetGUID, guidParameter, methodName);
        errorHandler.validateName(tagName, nameParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        this.validateEntity(userId, assetGUID, metadataCollection, methodName);

        try
        {
            /*
             * Create the Tag Entity
             */
            InstanceProperties properties;

            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      null,
                                                                      tagNamePropertyName,
                                                                      tagName,
                                                                      methodName);
            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      properties,
                                                                      tagDescriptionPropertyName,
                                                                      tagDescription,
                                                                      methodName);
            EntityDetail feedbackEntity = metadataCollection.addEntity(userId,
                                                                       tagTypeGUID,
                                                                       properties,
                                                                       null,
                                                                       InstanceStatus.ACTIVE);

            /*
             * Link the tag to the asset
             */
            metadataCollection.addRelationship(userId,
                                               attachedTagTypeGUID,
                                               null,
                                               assetGUID,
                                               feedbackEntity.getGUID(),
                                               InstanceStatus.ACTIVE);

            /*
             * Return the guid of the feedback entity
             */
            if (feedbackEntity != null)
            {
                return feedbackEntity.getGUID();
            }
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);
        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        return null;
    }

    /**
     * Adds a rating to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param assetGUID - String - unique id for the asset.
     * @param starRating - StarRating  - enumeration for none, one to five stars.
     * @param review - String - user review of asset.
     *
     * @return guid of new rating object.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public String addRatingToAsset(String     userId,
                                   String     assetGUID,
                                   StarRating starRating,
                                   String     review) throws InvalidParameterException,
                                                             PropertyServerException,
                                                             UserNotAuthorizedException
    {
        final String methodName = "addRatingToAsset";
        final String guidParameter = "assetGUID";
        final String ratingParameter = "starRating";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(assetGUID, guidParameter, methodName);
        errorHandler.validateEnum(starRating, ratingParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        this.validateEntity(userId, assetGUID, metadataCollection, methodName);

        try
        {
            /*
             * Create the Rating Entity
             */
            InstanceProperties properties  = null;

            properties = this.addStarRatingPropertyToInstance(properties,
                                                              starRating,
                                                              methodName);
            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      properties,
                                                                      reviewPropertyName,
                                                                      review,
                                                                      methodName);
            EntityDetail feedbackEntity = metadataCollection.addEntity(userId,
                                                                       ratingTypeGUID,
                                                                       properties,
                                                                       null,
                                                                       InstanceStatus.ACTIVE);

            /*
             * Link the Rating to the asset
             */
            metadataCollection.addRelationship(userId,
                                               attachedRatingTypeGUID,
                                               null,
                                               assetGUID,
                                               feedbackEntity.getGUID(),
                                               InstanceStatus.ACTIVE);

            /*
             * Return the guid of the feedback entity
             */
            if (feedbackEntity != null)
            {
                return feedbackEntity.getGUID();
            }
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);
        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        return null;
    }


    /**
     * Set up a property value for the StartRating enum property.
     *
     * @param properties - current properties
     * @param starRating - enum value
     * @param methodName - calling method
     * @return - InstanceProperties object with the enum value added
     */
    private InstanceProperties addStarRatingPropertyToInstance(InstanceProperties  properties,
                                                               StarRating          starRating,
                                                               String              methodName)
    {
        int                ordinal = 99;
        String             symbolicName = null;
        String             description = null;

        final int    element1Ordinal         = 0;
        final String element1Value           = "NotRecommended";
        final String element1Description     = "This content is not recommended.";

        final int    element2Ordinal         = 1;
        final String element2Value           = "OneStar";
        final String element2Description     = "One star rating.";

        final int    element3Ordinal         = 2;
        final String element3Value           = "TwoStar";
        final String element3Description     = "Two star rating.";

        final int    element4Ordinal         = 3;
        final String element4Value           = "ThreeStar";
        final String element4Description     = "Three star rating.";

        final int    element5Ordinal         = 4;
        final String element5Value           = "FourStar";
        final String element5Description     = "Four star rating.";

        final int    element6Ordinal         = 5;
        final String element6Value           = "FiveStar";
        final String element6Description     = "Five star rating.";

        switch (starRating)
        {
            case NOT_RECOMMENDED:
                ordinal = element1Ordinal;
                symbolicName = element1Value;
                description = element1Description;
                break;

            case ONE_STAR:
                ordinal = element2Ordinal;
                symbolicName = element2Value;
                description = element2Description;
                break;

            case TWO_STARS:
                ordinal = element3Ordinal;
                symbolicName = element3Value;
                description = element3Description;
                break;

            case THREE_STARS:
                ordinal = element4Ordinal;
                symbolicName = element4Value;
                description = element4Description;
                break;

            case FOUR_STARS:
                ordinal = element5Ordinal;
                symbolicName = element5Value;
                description = element5Description;
                break;

            case FIVE_STARS:
                ordinal = element6Ordinal;
                symbolicName = element6Value;
                description = element6Description;
                break;
        }

        return repositoryHelper.addEnumPropertyToInstance(serviceName,
                                                          properties,
                                                          starsPropertyName,
                                                          ordinal,
                                                          symbolicName,
                                                          description,
                                                          methodName);
    }


    /**
     * Adds a "Like" to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param assetGUID - String - unique id for the asset
     *
     * @return guid of new like object.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public String addLikeToAsset(String       userId,
                                 String       assetGUID) throws InvalidParameterException,
                                                                PropertyServerException,
                                                                UserNotAuthorizedException
    {
        final String methodName = "addLikeToAsset";
        final String guidParameter = "assetGUID";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(assetGUID, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        this.validateEntity(userId, assetGUID, metadataCollection, methodName);

        try
        {
            /*
             * Create the Like Entity
             */
            EntityDetail feedbackEntity = metadataCollection.addEntity(userId,
                                                                       likeTypeGUID,
                                                                       null,
                                                                       null,
                                                                       InstanceStatus.ACTIVE);

            /*
             * Link the Like to the asset
             */
            metadataCollection.addRelationship(userId,
                                               attachedLikeTypeGUID,
                                               null,
                                               assetGUID,
                                               feedbackEntity.getGUID(),
                                               InstanceStatus.ACTIVE);

            /*
             * Return the guid of the feedback entity
             */
            if (feedbackEntity != null)
            {
                return feedbackEntity.getGUID();
            }
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);
        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        return null;
    }


    /**
     * Adds a comment to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param assetGUID - String - unique id for the asset.
     * @param commentType - type of comment enum.
     * @param commentText - String - the text of the comment.
     *
     * @return guid of new comment.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public String addCommentToAsset(String      userId,
                                    String      assetGUID,
                                    CommentType commentType,
                                    String      commentText) throws InvalidParameterException,
                                                                    PropertyServerException,
                                                                    UserNotAuthorizedException
    {
        final String methodName = "addCommentToAsset";
        final String guidParameter = "assetGUID";

        return this.addCommentToEntity(userId, assetGUID, guidParameter, commentType, commentText, methodName);
    }

    /**
     * Adds a comment to the asset.
     *
     * @param userId - String - userId of user making request.
     * @param commentGUID - String - unique id for an existing comment.  Used to add a reply to a comment.
     * @param commentType - type of comment enum.
     * @param commentText - String - the text of the comment.
     *
     * @return guid of new comment.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public String addCommentReply(String      userId,
                                  String      commentGUID,
                                  CommentType commentType,
                                  String      commentText) throws InvalidParameterException,
                                                                  PropertyServerException,
                                                                  UserNotAuthorizedException
    {
        final String methodName = "addCommentReply";
        final String guidParameter = "assetGUID";


        return this.addCommentToEntity(userId, commentGUID, guidParameter, commentType, commentText, methodName);
    }


    /**
     * Set up a property value for the CommentType enum property.
     *
     * @param properties - current properties
     * @param commentType - enum value
     * @param methodName - calling method
     * @return - InstanceProperties object with the enum value added
     */
    private InstanceProperties addCommentTypePropertyToInstance(InstanceProperties  properties,
                                                                CommentType         commentType,
                                                                String              methodName)
    {
        int                ordinal = 99;
        String             symbolicName = null;
        String             description = null;

        final int    element1Ordinal         = 0;
        final String element1Value           = "GeneralComment";
        final String element1Description     = "General comment.";

        final int    element2Ordinal         = 1;
        final String element2Value           = "Question";
        final String element2Description     = "A question.";

        final int    element3Ordinal         = 2;
        final String element3Value           = "Answer";
        final String element3Description     = "An answer to a previously asked question.";

        final int    element4Ordinal         = 3;
        final String element4Value           = "Suggestion";
        final String element4Description     = "A suggestion for improvement.";

        final int    element5Ordinal         = 3;
        final String element5Value           = "Experience";
        final String element5Description     = "An account of an experience.";

        switch (commentType)
        {
            case STANDARD_COMMENT:
                ordinal = element1Ordinal;
                symbolicName = element1Value;
                description = element1Description;
                break;

            case QUESTION:
                ordinal = element2Ordinal;
                symbolicName = element2Value;
                description = element2Description;
                break;

            case ANSWER:
                ordinal = element3Ordinal;
                symbolicName = element3Value;
                description = element3Description;
                break;

            case SUGGESTION:
                ordinal = element4Ordinal;
                symbolicName = element4Value;
                description = element4Description;
                break;

            case USAGE_EXPERIENCE:
                ordinal = element5Ordinal;
                symbolicName = element5Value;
                description = element5Description;
                break;
        }

        return repositoryHelper.addEnumPropertyToInstance(serviceName,
                                                          properties,
                                                          commentTypePropertyName,
                                                          ordinal,
                                                          symbolicName,
                                                          description,
                                                          methodName);
    }


    /**
     * Adds a comment and links it to the supplied entity.
     *
     * @param userId - String - userId of user making request.
     * @param entityGUID - String - unique id for an existing comment.  Used to add a reply to a comment.
     * @param guidParameter - name of parameter that supplied the entity'ss unique identifier.
     * @param commentType - type of comment enum.
     * @param commentText - String - the text of the comment.
     *
     * @return guid of new comment.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    private String addCommentToEntity(String      userId,
                                      String      entityGUID,
                                      String      guidParameter,
                                      CommentType commentType,
                                      String      commentText,
                                      String      methodName) throws InvalidParameterException,
                                                                     PropertyServerException,
                                                                     UserNotAuthorizedException
    {
        final String typeParameter = "commentType";
        final String textParameter = "commentText";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(entityGUID, guidParameter, methodName);
        errorHandler.validateEnum(commentType, typeParameter, methodName);
        errorHandler.validateText(commentText, textParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        this.validateEntity(userId, entityGUID, metadataCollection, methodName);

        try
        {
            /*
             * Create the Comment Entity
             */
            InstanceProperties properties  = null;

            properties = this.addCommentTypePropertyToInstance(properties,
                                                               commentType,
                                                               methodName);
            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      properties,
                                                                      qualifiedNamePropertyName,
                                                                      "Comment:" + userId + ":" + new Date().toString(),
                                                                      methodName);
            properties = repositoryHelper.addStringPropertyToInstance(serviceName,
                                                                      properties,
                                                                      commentPropertyName,
                                                                      commentText,
                                                                      methodName);
            EntityDetail feedbackEntity = metadataCollection.addEntity(userId,
                                                                       commentTypeGUID,
                                                                       properties,
                                                                       null,
                                                                       InstanceStatus.ACTIVE);

            /*
             * Link the comment reply to the supplied entity
             */
            metadataCollection.addRelationship(userId,
                                               attachedCommentTypeGUID,
                                               null,
                                               entityGUID,
                                               feedbackEntity.getGUID(),
                                               InstanceStatus.ACTIVE);

            /*
             * Return the guid of the feedback entity
             */
            if (feedbackEntity != null)
            {
                return feedbackEntity.getGUID();
            }
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);
        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }

        return null;
    }


    /**
     * Removes a tag from the asset that was added by this user.
     *
     * @param userId - String - userId of user making request.
     * @param tagGUID - String - unique id for the tag.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem updating the asset properties in
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public void   removeTagFromAsset(String     userId,
                                     String     tagGUID) throws InvalidParameterException,
                                                                PropertyServerException,
                                                                UserNotAuthorizedException
    {
        final String methodName = "removeTagFromAsset";
        final String guidParameter = "tagGUID";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(tagGUID, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        try
        {
            metadataCollection.deleteEntity(userId, informalTagTypeGUID, informalTagTypeName, tagGUID);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }
    }


    /**
     * Removes a tag from the asset that was added by this user.
     *
     * @param userId - String - userId of user making request.
     * @param tagGUID - String - unique id for the tag.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem updating the asset properties in
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public void   removePrivateTagFromAsset(String     userId,
                                            String     tagGUID) throws InvalidParameterException,
                                                                       PropertyServerException,
                                                                       UserNotAuthorizedException
    {
        final String methodName = "removePrivateTagFromAsset";
        final String guidParameter = "tagGUID";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(tagGUID, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        try
        {
            metadataCollection.deleteEntity(userId, privateTagTypeGUID, privateTagTypeName, tagGUID);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }
    }


    /**
     * Removes of a star rating that was added to the asset by this user.
     *
     * @param userId - String - userId of user making request.
     * @param ratingGUID - String - unique id for the rating object
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem updating the asset properties in
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public void   removeRatingFromAsset(String     userId,
                                        String     ratingGUID) throws InvalidParameterException,
                                                                      PropertyServerException,
                                                                      UserNotAuthorizedException
    {
        final String methodName = "removeRatingFromAsset";
        final String guidParameter = "ratingGUID";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(ratingGUID, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        try
        {
            metadataCollection.deleteEntity(userId, ratingTypeGUID, ratingTypeName, ratingGUID);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }
    }


    /**
     * Removes a "Like" added to the asset by this user.
     *
     * @param userId - String - userId of user making request.
     * @param likeGUID - String - unique id for the like object
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem updating the asset properties in
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public void   removeLikeFromAsset(String     userId,
                                      String     likeGUID) throws InvalidParameterException,
                                                                  PropertyServerException,
                                                                  UserNotAuthorizedException
    {
        final String methodName = "removeLikeFromAsset";
        final String guidParameter = "likeGUID";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(likeGUID, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        try
        {
            metadataCollection.deleteEntity(userId, likeTypeGUID, likeTypeName, likeGUID);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }
    }


    /**
     * Removes a comment added to the asset by this user.
     *
     * @param userId - String - userId of user making request.
     * @param commentGUID - String - unique id for the comment object
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem updating the asset properties in
     *                                   the property server.
     * @throws UserNotAuthorizedException - the user does not have permission to perform this request.
     */
    public void   removeCommentFromAsset(String     userId,
                                         String     commentGUID) throws InvalidParameterException,
                                                                        PropertyServerException,
                                                                        UserNotAuthorizedException
    {
        final String methodName = "removeCommentFromAsset";
        final String guidParameter = "commentGUID";

        errorHandler.validateUserId(userId, methodName);
        errorHandler.validateGUID(commentGUID, guidParameter, methodName);

        OMRSMetadataCollection  metadataCollection = errorHandler.validateRepositoryConnector(methodName);

        try
        {
            metadataCollection.deleteEntity(userId, commentTypeGUID, commentTypeName, commentGUID);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }
    }


    /**
     * Validate that the supplied GUID is for a real entity.
     *
     * @param userId - user making the request.
     * @param assetGUID - unique identifier of the asset.
     * @param metadataCollection - repository's metadata collection
     * @param methodName - name of method called.
     * @throws InvalidParameterException - entity not known
     * @throws PropertyServerException - problem accessing property server
     * @throws UserNotAuthorizedException - security access problem
     */
    private void validateEntity(String                  userId,
                                String                  assetGUID,
                                OMRSMetadataCollection  metadataCollection,
                                String                  methodName) throws InvalidParameterException,
                                                                           PropertyServerException,
                                                                           UserNotAuthorizedException
    {
        try
        {
            metadataCollection.getEntitySummary(userId, assetGUID);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.EntityNotKnownException error)
        {
            errorHandler.handleUnknownAsset(error,
                                            assetGUID,
                                            methodName,
                                            serverName,
                                            serviceName);
        }
        catch (org.apache.atlas.omrs.ffdc.exception.UserNotAuthorizedException error)
        {
            errorHandler.handleUnauthorizedUser(userId,
                                                methodName,
                                                serverName,
                                                serviceName);
        }
        catch (Throwable   error)
        {
            errorHandler.handleRepositoryError(error,
                                               methodName,
                                               serverName,
                                               serviceName);
        }
    }
}
