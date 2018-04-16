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
package org.apache.atlas.omas.assetconsumer;


import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.CommentType;
import org.apache.atlas.omas.connectedasset.properties.AssetUniverse;
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.ocf.properties.StarRating;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.*;

/**
 * The AssetConsumer Open Metadata Access Service (OMAS) is used by applications and tools as a factory for Open
 * Connector Framework (OCF) connectors.  The configuration for the connectors is managed as open metadata in
 * a Connection definition.  The caller to the AssetConsumer OMAS passes either the name, GUID or URL for the
 * connection to the appropriate method to retrieve a connector.  The AssetConsumer OMAS retrieves the connection
 * from the metadata repository and creates an appropriate connector as described the connection and
 * returns it to the caller.
 *
 * Each connection has a unique guid and a name.  An application can request a connector instance
 * from the OCF's Connector Broker using the guid, name or URL of a connection, or by passing a fully
 * populated connection object.  If the connection guid, name or URL is used, AssetConsumer OMAS
 * looks up the connection properties in the metadata repository before calling the OCF ConnectorBroker to create the
 * connector
 *
 * In addition it is possible to maintain feedback for the asset through the AssetConsumer OMAS.
 * This is in terms of tags, star ratings, likes and comments.  There is also the ability to add audit log records
 * related to the use of the asset through the AssetConsumerInterface OMAS.
 */
public interface AssetConsumerInterface
{
    /**
     * Returns the connector corresponding to the supplied connection name.
     *
     * @param userId - String - userId of user making request.
     * @param connectionName - this may be the qualifiedName or displayName of the connection.
     *
     * @return Connector - connector instance.
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws UnrecognizedConnectionNameException - there is no connection defined for this name.
     * @throws AmbiguousConnectionNameException - there is more than one connection defined for this name.
     * @throws ConnectionCheckedException - there are errors in the configuration of the connection which is preventing
     *                                      the creation of a connector.
     * @throws ConnectorCheckedException - there are errors in the initialization of the connector.
     * @throws PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    Connector getConnectorByName(String   userId,
                                 String   connectionName) throws InvalidParameterException,
                                                                 UnrecognizedConnectionNameException,
                                                                 AmbiguousConnectionNameException,
                                                                 ConnectionCheckedException,
                                                                 ConnectorCheckedException,
                                                                 PropertyServerException,
                                                                 UserNotAuthorizedException;


    /**
     * Returns the connector corresponding to the supplied connection GUID.
     *
     * @param userId - String - userId of user making request.
     * @param connectionGUID - the unique id for the connection within the metadata repository.
     *
     * @return Connector - connector instance.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the property server.
     * @throws ConnectionCheckedException - there are errors in the configuration of the connection which is preventing
     *                                      the creation of a connector.
     * @throws ConnectorCheckedException - there are errors in the initialization of the connector.
     * @throws PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    Connector getConnectorByGUID(String     userId,
                                 String     connectionGUID) throws InvalidParameterException,
                                                                   UnrecognizedConnectionGUIDException,
                                                                   ConnectionCheckedException,
                                                                   ConnectorCheckedException,
                                                                   PropertyServerException,
                                                                   UserNotAuthorizedException;


    /**
     * Returns the connector corresponding to the supplied connection.
     *
     * @param userId - String - userId of user making request.
     * @param connection - the connection object that contains the properties needed to create the connection.
     *
     * @return Connector - connector instance
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws ConnectionCheckedException - there are errors in the configuration of the connection which is preventing
     *                                      the creation of a connector.
     * @throws ConnectorCheckedException - there are errors in the initialization of the connector.
     * @throws PropertyServerException - there is a problem retrieving information from the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    Connector  getConnectorByConnection(String        userId,
                                        Connection    connection) throws InvalidParameterException,
                                                                         ConnectionCheckedException,
                                                                         ConnectorCheckedException,
                                                                         PropertyServerException,
                                                                         UserNotAuthorizedException;


    /**
     * Returns a comprehensive collection of properties about the requested asset.
     *
     * @param userId - String - userId of user making request.
     * @param assetGUID - String - unique id for asset.
     *
     * @return AssetUniverse - a comprehensive collection of properties about the asset.

     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem retrieving the asset properties from
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    AssetUniverse getAssetProperties(String   userId,
                                     String   assetGUID) throws InvalidParameterException,
                                                                PropertyServerException,
                                                                UserNotAuthorizedException;


    /**
     * Creates an Audit log record for the asset.  This log record is stored in the Asset's Audit Log.
     *
     * @param userId - String - userId of user making request.
     * @param assetGUID - String - unique id for the asset.
     * @param connectorInstanceId - String - (optional) id of connector in use (if any).
     * @param connectionName - String - (optional) name of the connection (extracted from the connector).
     * @param connectorType - String - (optional) type of connector in use (if any).
     * @param contextId - String - (optional) function name, or processId of the activity that the caller is performing.
     * @param message - log record content.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem adding the asset properties to
     *                                   the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    void  addLogMessageToAsset(String      userId,
                               String      assetGUID,
                               String      connectorInstanceId,
                               String      connectionName,
                               String      connectorType,
                               String      contextId,
                               String      message) throws InvalidParameterException,
                                                           PropertyServerException,
                                                           UserNotAuthorizedException;



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
    String addTagToAsset(String userId,
                         String assetGUID,
                         String tagName,
                         String tagDescription) throws InvalidParameterException,
                                                       PropertyServerException,
                                                       UserNotAuthorizedException;


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
    String addPrivateTagToAsset(String userId,
                                String assetGUID,
                                String tagName,
                                String tagDescription) throws InvalidParameterException,
                                                              PropertyServerException,
                                                              UserNotAuthorizedException;



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
    String addRatingToAsset(String     userId,
                            String     assetGUID,
                            StarRating starRating,
                            String     review) throws InvalidParameterException,
                                                      PropertyServerException,
                                                      UserNotAuthorizedException;

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
    String addLikeToAsset(String       userId,
                          String       assetGUID) throws InvalidParameterException,
                                                         PropertyServerException,
                                                         UserNotAuthorizedException;


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
    String addCommentToAsset(String      userId,
                             String      assetGUID,
                             CommentType commentType,
                             String      commentText) throws InvalidParameterException,
                                                             PropertyServerException,
                                                             UserNotAuthorizedException;


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
    String addCommentReply(String      userId,
                           String      commentGUID,
                           CommentType commentType,
                           String      commentText) throws InvalidParameterException,
                                                           PropertyServerException,
                                                           UserNotAuthorizedException;


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
    void   removeTag(String     userId,
                     String     tagGUID) throws InvalidParameterException,
                                                PropertyServerException,
                                                UserNotAuthorizedException;


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
    void   removePrivateTag(String     userId,
                            String     tagGUID) throws InvalidParameterException,
                                                       PropertyServerException,
                                                       UserNotAuthorizedException;


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
    void   removeRating(String     userId,
                        String     ratingGUID) throws InvalidParameterException,
                                                      PropertyServerException,
                                                      UserNotAuthorizedException;


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
    void   removeLike(String     userId,
                      String     likeGUID) throws InvalidParameterException,
                                                  PropertyServerException,
                                                  UserNotAuthorizedException;


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
    void   removeComment(String     userId,
                         String     commentGUID) throws InvalidParameterException,
                                                        PropertyServerException,
                                                        UserNotAuthorizedException;
}
