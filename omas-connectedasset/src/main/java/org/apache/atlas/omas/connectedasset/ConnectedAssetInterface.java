/**
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

package org.apache.atlas.omas.connectedasset;

import org.apache.atlas.omas.connectedasset.ffdc.exceptions.InvalidParameterException;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.PropertyServerException;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.UnrecognizedConnectionGUIDException;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.UserNotAuthorizedException;
import org.apache.atlas.omas.connectedasset.properties.AssetUniverse;

/**
 * ConnectedAssetInterface is the OMAS client interface of the Connected Asset OMAS.
 *
 */
public interface ConnectedAssetInterface
{
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
     * Returns a comprehensive collection of properties about the asset linked to the supplied connection.
     *
     * @param userId - identifier for calling user
     * @param connectionGUID - uniqueId for the connection.
     * @return AssetUniverse - a comprehensive collection of properties about the connected asset.
     * @throws InvalidParameterException - one of the parameters is null or invalid.
     * @throws PropertyServerException - There is a problem retrieving the connected asset properties from
     *                                   the property server.
     * @throws UnrecognizedConnectionGUIDException - the supplied GUID is not recognized by the property server.
     */
    AssetUniverse  getAssetPropertiesByConnection(String   userId,
                                                  String   connectionGUID) throws InvalidParameterException,
                                                                                  UnrecognizedConnectionGUIDException,
                                                                                  PropertyServerException;
}
