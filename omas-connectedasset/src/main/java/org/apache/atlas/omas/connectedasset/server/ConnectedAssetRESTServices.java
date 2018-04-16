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

package org.apache.atlas.omas.connectedasset.server;

import org.apache.atlas.omas.connectedasset.ffdc.exceptions.PropertyServerException;
import org.apache.atlas.omas.connectedasset.properties.AssetUniverse;
import org.apache.atlas.omas.connectedasset.server.properties.AssetUniverseResponse;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * The ConnectedAssetRESTServices is the server-side implementation of the Connected Asset OMAS REST interface.
 */
@RestController
@RequestMapping("/omag/omas/connected-asset")
public class ConnectedAssetRESTServices
{

    public ConnectedAssetRESTServices()
    {
        /*
         *
         */
    }


    /**
     * Returns a comprehensive collection of properties about the requested asset.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for asset.
     *
     * @return AssetUniverseResponse - a comprehensive collection of properties about the asset or
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem retrieving the asset properties from
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/assets/{guid}")

    public AssetUniverseResponse getAssetProperties(@PathVariable String   userId,
                                                    @PathVariable String   guid)
    {
        return null;
    }



    /**
     * Returns a comprehensive collection of properties about the asset linked to the supplied connection.
     *
     * @param userId - String - userId of user making request.
     * @param guid - String - unique id for connection.
     * @return AssetUniverse - a comprehensive collection of properties about the connected asset
     * InvalidParameterException - one of the parameters is null or invalid.
     * PropertyServerException - There is a problem retrieving the asset properties from
     *                                   the metadata repository.
     * UserNotAuthorizedException - the requesting user is not authorized to issue this request.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/{userId}/assets/by-connection/{guid}")

    public AssetUniverseResponse  getAssetPropertiesByConnection(@PathVariable String   userId,
                                                                 @PathVariable String   guid)
    {
        AssetUniverse   extractedAssetProperties = null;

        return null;
    }
}
