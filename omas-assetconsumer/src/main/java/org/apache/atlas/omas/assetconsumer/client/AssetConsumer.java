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
package org.apache.atlas.omas.assetconsumer.client;

import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ConnectorBroker;
import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.CommentType;
import org.apache.atlas.omas.assetconsumer.server.properties.GUIDResponse;
import org.apache.atlas.omas.connectedasset.properties.AssetUniverse;
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.ocf.properties.StarRating;
import org.apache.atlas.omas.assetconsumer.AssetConsumerInterface;
import org.apache.atlas.omas.assetconsumer.ffdc.*;
import org.apache.atlas.omas.assetconsumer.ffdc.exceptions.*;
import org.apache.atlas.omas.assetconsumer.server.properties.AssetConsumerOMASAPIResponse;
import org.apache.atlas.omas.assetconsumer.server.properties.ConnectionResponse;
import org.apache.atlas.omas.assetconsumer.server.properties.VoidResponse;
import org.apache.atlas.omas.connectedasset.client.ConnectedAsset;
import org.apache.atlas.omas.connectedasset.client.ConnectedAssetProperties;
import org.springframework.web.client.RestTemplate;

/**
 * The AssetConsumer Open Metadata Access Service (OMAS) provides an interface to support an application or tool
 * that is using Open Connector Framework (OCF) connectors to work with assets such as data stores, data sets and
 * APIs.  The interface is divided into three parts:
 * <ul>
 *     <li>Client-side only services that work with the OCF to create requested connectors.</li>
 *     <li>OMAS Server calls to retrieve connection information on behalf of the OCF.</li>
 *     <li>OMAS Server calls to add feedback to the asset.</li>
 * </ul>
 */
public class AssetConsumer implements AssetConsumerInterface
{
    private String            omasServerURL;  /* Initialized in constructor */

    /**
     * Create a new AssetConsumer client.
     *
     * @param newServerURL - the network address of the server running the OMAS REST servers
     */
    public AssetConsumer(String     newServerURL)
    {
        omasServerURL = newServerURL;
    }


    /**
     * Use the Open Connector Framework (OCF) to create a connector using the supplied connection.
     *
     * @param requestedConnection - connection describing the required connector.
     * @param methodName - name of the calling method.
     * @return a new connector.
     */
    private Connector  getConnectorForConnection(String          userId,
                                                 Connection      requestedConnection,
                                                 String          methodName) throws ConnectionCheckedException,
                                                                                    ConnectorCheckedException
    {
        ConnectorBroker  connectorBroker = new ConnectorBroker();

        /*
         * Pass the connection to the ConnectorBroker to create the connector instance.
         * Again, exceptions from this process are returned directly to the caller.
         */
        Connector newConnector = connectorBroker.getConnector(requestedConnection);

        /*
         * If no exception is thrown by getConnector, we should have a connector instance.
         */
        if (newConnector == null)
        {
            /*
             * This is probably some sort of logic error since the connector should have been returned.
             * Whatever the cause, the process can not proceed without a connector.
             */
            AssetConsumerErrorCode  errorCode = AssetConsumerErrorCode.NULL_CONNECTOR_RETURNED;
            String                  errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new AssetConsumerRuntimeException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }

        /*
         * If the connector is successfully created, set up the Connected Asset Properties for the connector.
         * The properties should be retrieved from the open metadata repositories, so use an OMAS implementation
         * of the ConnectedAssetProperties object.
         */
        ConnectedAssetProperties connectedAssetProperties = new ConnectedAssetProperties(userId,
                                                                                         omasServerURL,
                                                                                         newConnector.getConnectorInstanceId(),
                                                                                         newConnector.getConnection());

        /*
         * Pass the new connected asset properties to the connector
         */
        newConnector.initializeConnectedAssetProperties(connectedAssetProperties);

        /*
         * At this stage, the asset properties are not retrieved from the server.  This does not happen until the caller
         * issues a connector.getConnectedAssetProperties.  This causes the connectedAssetProperties.refresh() call
         * to be made, which contacts the OMAS server and retrieves the asset properties.
         *
         * Delaying the population of the connected asset properties ensures the latest values are returned to the
         * caller (consider a long running connection).  Alternatively, these properties may not ever be used by the
         * caller so retrieving the properties at this point would be unnecessary.
         */

        return newConnector;
    }



    /**
     * Returns the connection object corresponding to the supplied connection name.
     *
     * @param userId - String - userId of user making request.
     * @param name - this may be the qualifiedName or displayName of the connection.
     *
     * @return Connection retrieved from property server
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws UnrecognizedConnectionNameException - there is no connection defined for this name.
     * @throws AmbiguousConnectionNameException - there is more than one connection defined for this name.
     * @throws PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    private Connection getConnectionByName(String   userId,
                                           String   name) throws InvalidParameterException,
                                                                 UnrecognizedConnectionNameException,
                                                                 AmbiguousConnectionNameException,
                                                                 PropertyServerException,
                                                                 UserNotAuthorizedException
    {
        final String   methodName = "getConnectionByName";
        final String   urlTemplate = "/{0}/connection/by-name/{1}";

        validateOMASServerURL(methodName);

        ConnectionResponse   restResult = callConnectionGetRESTCall(methodName,
                                                                    omasServerURL + urlTemplate,
                                                                    userId,
                                                                    name);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUnrecognizedConnectionNameException(methodName, restResult);
        this.detectAndThrowAmbiguousConnectionNameException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getConnection();
    }


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
    public Connector getConnectorByName(String   userId,
                                        String   connectionName) throws InvalidParameterException,
                                                                        UnrecognizedConnectionNameException,
                                                                        AmbiguousConnectionNameException,
                                                                        ConnectionCheckedException,
                                                                        ConnectorCheckedException,
                                                                        PropertyServerException,
                                                                        UserNotAuthorizedException
    {
        final String   methodName = "getConnectorByName";
        final  String  nameParameter = "connectionName";


        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateName(connectionName, nameParameter, methodName);

        return this.getConnectorForConnection(userId,
                                              this.getConnectionByName(userId, connectionName),
                                              methodName);
    }



    /**
     * Returns the connection object corresponding to the supplied connection GUID.
     *
     * @param userId - String - userId of user making request.
     * @param guid - the unique id for the connection within the property server.
     *
     * @return Connection retrieved from the property server
     * InvalidParameterException - one of the parameters is null or invalid.
     * UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the property server.
     * PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    private Connection getConnectionByGUID(String     userId,
                                           String     guid) throws InvalidParameterException,
                                                                   UnrecognizedConnectionGUIDException,
                                                                   PropertyServerException,
                                                                   UserNotAuthorizedException
    {
        final String   methodName  = "getConnectionByGUID";
        final String   urlTemplate = "/{0}/connection/{1}";

        validateOMASServerURL(methodName);

        ConnectionResponse   restResult = callConnectionGetRESTCall(methodName,
                                                                    omasServerURL + urlTemplate,
                                                                    userId,
                                                                    guid);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUnrecognizedConnectionGUIDException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getConnection();
    }



    /**
     * Returns the connector corresponding to the supplied connection GUID.
     *
     * @param userId - String - userId of user making request.
     * @param connectionGUID - the unique id for the connection within the property server.
     *
     * @return Connector - connector instance.
     *
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the property server.
     * @throws ConnectionCheckedException - there are errors in the configuration of the connection which is preventing
     *                                      the creation of a connector.
     * @throws ConnectorCheckedException - there are errors in the initialization of the connector.
     * @throws PropertyServerException - there is a problem retrieving information from the property server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public Connector getConnectorByGUID(String     userId,
                                        String     connectionGUID) throws InvalidParameterException,
                                                                          UnrecognizedConnectionGUIDException,
                                                                          ConnectionCheckedException,
                                                                          ConnectorCheckedException,
                                                                          PropertyServerException,
                                                                          UserNotAuthorizedException
    {
        final  String  methodName = "getConnectorByGUID";
        final  String  guidParameter = "connectionGUID";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(connectionGUID, guidParameter, methodName);

        return this.getConnectorForConnection(userId,
                                              this.getConnectionByGUID(userId, connectionGUID),
                                              methodName);
    }


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
     * @throws PropertyServerException - there is a problem retrieving information from the property (metadata) server.
     * @throws UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    public Connector  getConnectorByConnection(String        userId,
                                               Connection    connection) throws InvalidParameterException,
                                                                                ConnectionCheckedException,
                                                                                ConnectorCheckedException,
                                                                                PropertyServerException,
                                                                                UserNotAuthorizedException
    {
        final  String  methodName = "getConnectorByConnection";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);

        return this.getConnectorForConnection(userId, connection, methodName);
    }


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
    public AssetUniverse getAssetProperties(String   userId,
                                            String   assetGUID) throws InvalidParameterException,
                                                                       PropertyServerException,
                                                                       UserNotAuthorizedException
    {
        final String   methodName = "getAssetProperties";
        final String   guidParameter = "assetGUID";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);

        ConnectedAsset connectedAssetClient = new ConnectedAsset(omasServerURL);

        try
        {
            /*
             * Make use of the ConnectedAsset OMAS Service which provides the metadata services for the
             * Open Connector Framework (OCF).
             */
            return connectedAssetClient.getAssetProperties(userId, assetGUID);
        }
        catch (Throwable error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.NO_CONNECTED_ASSET;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(methodName);

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


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
    public void  addLogMessageToAsset(String      userId,
                                      String      assetGUID,
                                      String      connectorInstanceId,
                                      String      connectionName,
                                      String      connectorType,
                                      String      contextId,
                                      String      message) throws InvalidParameterException,
                                                                  PropertyServerException,
                                                                  UserNotAuthorizedException
    {
        final String   methodName = "addLogMessageToAsset";
        final String   guidParameter = "assetGUID";

        final String   urlTemplate = "/{0}/asset/{1}/log-record?connectorInstanceId={2}&connectionName={3}&connectorType={4}&contextId={5}&message={6}";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);

        VoidResponse restResult = callVoidPostRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       assetGUID,
                                                       connectorInstanceId,
                                                       connectionName,
                                                       connectorType,
                                                       contextId,
                                                       message);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);
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
        final String   methodName  = "addTagToAsset";
        final String   guidParameter = "assetGUID";
        final String   nameParameter = "tagName";

        final String   urlTemplate = "/{0}/asset/{1}/tags?tagName={2}&tagDescription={3}";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);
        validateName(tagName, nameParameter, methodName);

        GUIDResponse restResult = callGUIDPOSTRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       assetGUID,
                                                       tagName,
                                                       tagDescription);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getGUID();
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
        final String   methodName  = "addPrivateTagToAsset";
        final String   guidParameter = "assetGUID";
        final String   nameParameter = "tagName";

        final String   urlTemplate = "/{0}/asset/{1}/tags/private?tagName={2}&tagDescription={3}";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);
        validateName(tagName, nameParameter, methodName);

        GUIDResponse restResult = callGUIDPOSTRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       assetGUID,
                                                       tagName,
                                                       tagDescription);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getGUID();
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
        final String   methodName  = "addRatingToAsset";
        final String   guidParameter = "assetGUID";

        final String   urlTemplate = "/{0}/asset/{1}/ratings/?starRating={2}&review={3}";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);

        GUIDResponse restResult = callGUIDPOSTRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       assetGUID,
                                                       starRating,
                                                       review);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getGUID();
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
        final String   methodName  = "addRatingToAsset";
        final String   guidParameter = "assetGUID";

        final String   urlTemplate = "/{0}/asset/{1}/likes";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);

        GUIDResponse restResult = callGUIDPOSTRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       assetGUID);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getGUID();
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
        final String   methodName  = "addCommentToAsset";
        final String   guidParameter = "assetGUID";

        final String   urlTemplate = "/{0}/asset/{1}/comments?commentType{2}&commentText={3}";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(assetGUID, guidParameter, methodName);

        GUIDResponse restResult = callGUIDPOSTRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       assetGUID,
                                                       commentType,
                                                       commentText);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getGUID();
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
        final String   methodName  = "addCommentReply";
        final String   commentGUIDParameter = "commentGUID";
        final String   urlTemplate = "/{0}/comments/{1}/reply?commentType={2}&commentText={3}";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(commentGUID, commentGUIDParameter, methodName);

        GUIDResponse restResult = callGUIDPOSTRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       commentGUID,
                                                       commentType,
                                                       commentText);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);

        return restResult.getGUID();
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
    public void   removeTag(String     userId,
                            String     tagGUID) throws InvalidParameterException,
                                                       PropertyServerException,
                                                       UserNotAuthorizedException
    {
        final String   methodName = "removeTag";
        final String   guidParameter = "tagGUID";

        final String   urlTemplate = "/{0}/tags/{guid}/delete";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(tagGUID, guidParameter, methodName);

        VoidResponse restResult = callVoidPostRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       tagGUID);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);
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
    public void   removePrivateTag(String     userId,
                                   String     tagGUID) throws InvalidParameterException,
                                                              PropertyServerException,
                                                              UserNotAuthorizedException
    {
        final String   methodName = "removePrivateTag";
        final String   guidParameter = "tagGUID";

        final String   urlTemplate = "/{0}/tags/private/{guid}/delete";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(tagGUID, guidParameter, methodName);

        VoidResponse restResult = callVoidPostRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       tagGUID);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);
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
    public void   removeRating(String     userId,
                               String     ratingGUID) throws InvalidParameterException,
                                                             PropertyServerException,
                                                             UserNotAuthorizedException
    {
        final String   methodName = "removeRating";
        final String   guidParameter = "ratingGUID";

        final String   urlTemplate = "/{0}/ratings/{guid}/delete";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(ratingGUID, guidParameter, methodName);

        VoidResponse restResult = callVoidPostRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       ratingGUID);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);
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
    public void   removeLike(String     userId,
                             String     likeGUID) throws InvalidParameterException,
                                                         PropertyServerException,
                                                         UserNotAuthorizedException
    {
        final String   methodName = "removeLike";
        final String   guidParameter = "likeGUID";

        final String   urlTemplate = "/{0}/likes/{guid}/delete";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(likeGUID, guidParameter, methodName);

        VoidResponse restResult = callVoidPostRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       likeGUID);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);
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
    public void   removeComment(String     userId,
                                String     commentGUID) throws InvalidParameterException,
                                                               PropertyServerException,
                                                               UserNotAuthorizedException
    {
        final  String  methodName = "removeComment";
        final  String  guidParameter = "commentGUID";

        final String   urlTemplate = "/{0}/comments/{guid}/delete";

        validateOMASServerURL(methodName);
        validateUserId(userId, methodName);
        validateGUID(commentGUID, guidParameter, methodName);

        VoidResponse restResult = callVoidPostRESTCall(methodName,
                                                       omasServerURL + urlTemplate,
                                                       userId,
                                                       commentGUID);

        this.detectAndThrowInvalidParameterException(methodName, restResult);
        this.detectAndThrowUserNotAuthorizedException(methodName, restResult);
        this.detectAndThrowPropertyServerException(methodName, restResult);
    }


    /**
     * Throw an exception if a server URL has not been supplied on the constructor.
     *
     * @param methodName - name of the method making the call.
     * @throws PropertyServerException - the server URL is not set
     */
    private void validateOMASServerURL(String methodName) throws PropertyServerException
    {
        if (omasServerURL == null)
        {
            /*
             * It is not possible to retrieve a connection without knowledge of where the OMAS Server is located.
             */
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.SERVER_URL_NOT_SPECIFIED;
            String                 errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied userId is null
     *
     * @param userId - user name to validate
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the userId is null
     */
    private void validateUserId(String userId,
                                String methodName) throws InvalidParameterException
    {
        if (userId == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_USER_ID;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied userId is null
     *
     * @param guid - unique identifier to validate
     * @param guidParameter - name of the parameter that passed the guid.
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the guid is null
     */
    private void validateGUID(String guid,
                              String guidParameter,
                              String methodName) throws InvalidParameterException
    {
        if (guid == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_GUID;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(guidParameter,
                                                                                     methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }


    /**
     * Throw an exception if the supplied userId is null
     *
     * @param name - unique name to validate
     * @param nameParameter - name of the parameter that passed the name.
     * @param methodName - name of the method making the call.
     * @throws InvalidParameterException - the guid is null
     */
    private void validateName(String name,
                              String nameParameter,
                              String methodName) throws InvalidParameterException
    {
        if (name == null)
        {
            AssetConsumerErrorCode errorCode    = AssetConsumerErrorCode.NULL_NAME;
            String                 errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(nameParameter,
                                                                                     methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }
    }



    /**
     * Issue a GET REST call that returns a Connection object.
     *
     * @param methodName - name of the method being called
     * @param urlTemplate - template of the URL for the REST API call with place-holders for the parameters
     * @param params - a list of parameters that are slotted into the url template
     * @return ConnectionResponse
     * @throws PropertyServerException - something went wrong with the REST call stack.
     */
    private ConnectionResponse callConnectionGetRESTCall(String    methodName,
                                                         String    urlTemplate,
                                                         Object... params) throws PropertyServerException
    {
        ConnectionResponse restResult = new ConnectionResponse();

        /*
         * Issue the request
         */
        try
        {
            RestTemplate restTemplate = new RestTemplate();

            restResult = restTemplate.getForObject(urlTemplate, restResult.getClass(), params);
        }
        catch (Throwable error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.CLIENT_SIDE_REST_API_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     omasServerURL,
                                                                                                     error.getMessage());

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              error);
        }

        return restResult;
    }


    /**
     * Issue a PATCH REST call that returns a TypeDefResponse object.
     *
     * @param methodName - name of the method being called
     * @param urlTemplate - template of the URL for the REST API call with place-holders for the parameters
     * @param params - a list of parameters that are slotted into the url template
     * @return TypeDefResponse
     * @throws PropertyServerException - something went wrong with the REST call stack.
     */
    private GUIDResponse callGUIDPOSTRESTCall(String    methodName,
                                              String    urlTemplate,
                                              Object... params) throws PropertyServerException
    {
        GUIDResponse restResult = new GUIDResponse();

        /*
         * Issue the request
         */
        try
        {
            RestTemplate restTemplate = new RestTemplate();

            restResult = restTemplate.postForObject(urlTemplate, null, restResult.getClass(), params);
        }
        catch (Throwable error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.CLIENT_SIDE_REST_API_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     omasServerURL,
                                                                                                     error.getMessage());

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              error);
        }

        return restResult;
    }


    /**
     * Issue a POST REST call that returns a VoidResponse object.  This is typically a create
     *
     * @param methodName - name of the method being called
     * @param urlTemplate - template of the URL for the REST API call with place-holders for the parameters
     * @param params - a list of parameters that are slotted into the url template
     * @return VoidResponse
     * @throws PropertyServerException - something went wrong with the REST call stack.
     */
    private VoidResponse callVoidPostRESTCall(String    methodName,
                                              String    urlTemplate,
                                              Object... params) throws PropertyServerException
    {
        VoidResponse restResult = new VoidResponse();

        /*
         * Issue the request
         */
        try
        {
            RestTemplate restTemplate = new RestTemplate();

            restResult = restTemplate.postForObject(urlTemplate, null, restResult.getClass(), params);
        }
        catch (Throwable error)
        {
            AssetConsumerErrorCode errorCode = AssetConsumerErrorCode.CLIENT_SIDE_REST_API_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                                                                     omasServerURL,
                                                                                                     error.getMessage());

            throw new PropertyServerException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              error);
        }

        return restResult;
    }


    /**
     * Throw an AmbiguousConnectionNameException if it is encoded in the REST response.
     *
     * @param methodName - name of the method called
     * @param restResult - response from the rest call.  This generated in the remote server.
     * @throws AmbiguousConnectionNameException - encoded exception from the server
     */
    private void detectAndThrowAmbiguousConnectionNameException(String                       methodName,
                                                                AssetConsumerOMASAPIResponse restResult) throws AmbiguousConnectionNameException
    {
        final String   exceptionClassName = AmbiguousConnectionNameException.class.getName();

        if ((restResult != null) && (exceptionClassName.equals(restResult.getExceptionClassName())))
        {
            throw new AmbiguousConnectionNameException(restResult.getRelatedHTTPCode(),
                                                       this.getClass().getName(),
                                                       methodName,
                                                       restResult.getExceptionErrorMessage(),
                                                       restResult.getExceptionSystemAction(),
                                                       restResult.getExceptionUserAction());
        }
    }


    /**
     * Throw an InvalidParameterException if it is encoded in the REST response.
     *
     * @param methodName - name of the method called
     * @param restResult - response from the rest call.  This generated in the remote server.
     * @throws InvalidParameterException - encoded exception from the server
     */
    private void detectAndThrowInvalidParameterException(String                       methodName,
                                                         AssetConsumerOMASAPIResponse restResult) throws InvalidParameterException
    {
        final String   exceptionClassName = InvalidParameterException.class.getName();

        if ((restResult != null) && (exceptionClassName.equals(restResult.getExceptionClassName())))
        {
            throw new InvalidParameterException(restResult.getRelatedHTTPCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                restResult.getExceptionErrorMessage(),
                                                restResult.getExceptionSystemAction(),
                                                restResult.getExceptionUserAction());
        }
    }


    /**
     * Throw an PropertyServerException if it is encoded in the REST response.
     *
     * @param methodName - name of the method called
     * @param restResult - response from the rest call.  This generated in the remote server.
     * @throws PropertyServerException - encoded exception from the server
     */
    private void detectAndThrowPropertyServerException(String                       methodName,
                                                       AssetConsumerOMASAPIResponse restResult) throws PropertyServerException
    {
        final String   exceptionClassName = PropertyServerException.class.getName();

        if ((restResult != null) && (exceptionClassName.equals(restResult.getExceptionClassName())))
        {
            throw new PropertyServerException(restResult.getRelatedHTTPCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              restResult.getExceptionErrorMessage(),
                                              restResult.getExceptionSystemAction(),
                                              restResult.getExceptionUserAction());
        }
    }


    /**
     * Throw an UnrecognizedConnectionGUIDException if it is encoded in the REST response.
     *
     * @param methodName - name of the method called
     * @param restResult - response from the rest call.  This generated in the remote server.
     * @throws UnrecognizedConnectionGUIDException - encoded exception from the server
     */
    private void detectAndThrowUnrecognizedConnectionGUIDException(String                       methodName,
                                                                   AssetConsumerOMASAPIResponse restResult) throws UnrecognizedConnectionGUIDException
    {
        final String   exceptionClassName = UnrecognizedConnectionGUIDException.class.getName();

        if ((restResult != null) && (exceptionClassName.equals(restResult.getExceptionClassName())))
        {
            throw new UnrecognizedConnectionGUIDException(restResult.getRelatedHTTPCode(),
                                                          this.getClass().getName(),
                                                          methodName,
                                                          restResult.getExceptionErrorMessage(),
                                                          restResult.getExceptionSystemAction(),
                                                          restResult.getExceptionUserAction());
        }
    }


    /**
     * Throw an UnrecognizedConnectionNameException if it is encoded in the REST response.
     *
     * @param methodName - name of the method called
     * @param restResult - response from the rest call.  This generated in the remote server.
     * @throws UnrecognizedConnectionNameException - encoded exception from the server
     */
    private void detectAndThrowUnrecognizedConnectionNameException(String                       methodName,
                                                                   AssetConsumerOMASAPIResponse restResult) throws UnrecognizedConnectionNameException
    {
        final String   exceptionClassName = UnrecognizedConnectionNameException.class.getName();

        if ((restResult != null) && (exceptionClassName.equals(restResult.getExceptionClassName())))
        {
            throw new UnrecognizedConnectionNameException(restResult.getRelatedHTTPCode(),
                                                          this.getClass().getName(),
                                                          methodName,
                                                          restResult.getExceptionErrorMessage(),
                                                          restResult.getExceptionSystemAction(),
                                                          restResult.getExceptionUserAction());
        }
    }


    /**
     * Throw an UserNotAuthorizedException if it is encoded in the REST response.
     *
     * @param methodName - name of the method called
     * @param restResult - response from UserNotAuthorizedException - encoded exception from the server
     */
    private void detectAndThrowUserNotAuthorizedException(String                       methodName,
                                                          AssetConsumerOMASAPIResponse restResult) throws UserNotAuthorizedException
    {
        final String   exceptionClassName = UserNotAuthorizedException.class.getName();

        if ((restResult != null) && (exceptionClassName.equals(restResult.getExceptionClassName())))
        {
            throw new UserNotAuthorizedException(restResult.getRelatedHTTPCode(),
                                                 this.getClass().getName(),
                                                 methodName,
                                                 restResult.getExceptionErrorMessage(),
                                                 restResult.getExceptionSystemAction(),
                                                 restResult.getExceptionUserAction());
        }
    }
}
