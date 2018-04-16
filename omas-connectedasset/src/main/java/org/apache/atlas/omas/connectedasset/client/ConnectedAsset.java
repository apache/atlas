/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.omas.connectedasset.client;

import org.apache.atlas.omas.connectedasset.ffdc.exceptions.InvalidParameterException;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.PropertyServerException;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.UnrecognizedConnectionGUIDException;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.UserNotAuthorizedException;
import org.apache.atlas.omas.connectedasset.properties.AssetUniverse;
import org.apache.atlas.omas.connectedasset.ConnectedAssetInterface;


/**
 * ConnectedAsset is the OMAS client library implementation of the ConnectedAsset OMAS.
 * ConnectedAsset provides the metadata for the ConnectedAssetProperties API that is
 * supported by all Open Connector Framework (OCF)
 * connectors.   It provides access to the metadata about the Asset that the connector is linked to.
 */
public class ConnectedAsset implements ConnectedAssetInterface
{
    /*
     * The URL of the server where OMAS is active
     */
    private String                    omasServerURL = null;


    /**
     * Default Constructor used once a connector is created.
     *
     * @param omasServerURL - unique id for the connector instance
     */
    public ConnectedAsset(String   omasServerURL)
    {
        /*
         * Save OMAS Server URL
         */
        this.omasServerURL = omasServerURL;
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
        return null;
    }


    /**
     * Returns a comprehensive collection of properties about the asset linked to the supplied connection.
     *
     * @param connectionGUID - unique identifier for the connection
     * @return AssetUniverse - a comprehensive collection of properties about the connected asset
     * @throws PropertyServerException - There is a problem retrieving the connected asset properties from
     *                                   the metadata repository.
     */
    public AssetUniverse  getAssetPropertiesByConnection(String   connectionGUID) throws PropertyServerException
    {
        AssetUniverse   extractedAssetProperties = null;

        /*
         * Set up the OMAS URL in the asset universe
         */

        return extractedAssetProperties;
    }


    /**
     * Returns a comprehensive collection of properties about the asset linked to the supplied connection.
     *
     * @param connectionGUID - uniqueId for the connection.
     * @return AssetUniverse - a comprehensive collection of properties about the connected asset.
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem retrieving the connected asset properties from
     *                                   the property server.
     * @throws UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the property server.
     */
    public AssetUniverse  getAssetPropertiesByConnection(String   userId,
                                                         String   connectionGUID) throws InvalidParameterException,
                                                                                         UnrecognizedConnectionGUIDException,
                                                                                         PropertyServerException
    {
        return null;
    }
}
