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

package org.apache.atlas.ocf;

import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.ffdc.PropertyServerException;
import org.apache.atlas.ocf.properties.ConnectedAssetProperties;
import org.apache.atlas.ocf.properties.Connection;

/**
 * <p>
 * The Connector is the interface for all connector instances.   Connectors are client-side interfaces to assets
 * such as data stores, data sets, APIs, analytical functions.  They handle the communication with the server that
 * hosts the assets, along with the communication with the metadata server to serve up metadata (properties) about
 * the assets.
 * </p>
 * <p>
 * Each connector implementation is paired with a connector provider.  The connector provider is the factory for
 * connector instances.
 * </p>
 * <p>
 * The Connector interface defines that a connector instance should be able to return a unique
 * identifier, a connection object and a metadata object called ConnectedAssetProperties.
 * </p>
 * <p>
 * Each specific implementation of a connector then extends the Connector interface to add the methods to work with the
 * particular type of asset it supports.  For example, a JDBC connector would add the standard JDBC SQL interface, the
 * OMRS Connectors add the metadata repository management APIs...
 * </p>
 * <p>
 * The initialize() method is called by the Connector Provider to set up the connector instance Id and the
 * Connection properties for the connector as part of its construction process.
 * </p>
 * <p>
 * ConnectedAssetProperties provides descriptive properties about the asset that the connector is accessing.
 * It is supplied to the connector later during its initialization through the initializeConnectedAssetProperties() method.
 * See AssetConsumer OMAS for an example of this.
 * </p>
 * <p>
 * Both the connector and the connector provider have base classes (ConnectorBase and
 * ConnectorProviderBase respectively) that implement all of the standard methods.  The connector developer extends
 * these classes to add the specific methods to manage the asset and configure the base classes.
 * </p>
 */
public abstract class Connector
{
    /**
     * Call made by the ConnectorProvider to initialize the Connector with the base services.
     *
     * @param connectorInstanceId - unique id for the connector instance - useful for messages etc
     * @param connection - POJO for the configuration used to create the connector.
     */
    public abstract void initialize(String                    connectorInstanceId,
                                    Connection                connection);


    /**
     * Returns the unique connector instance id that assigned to the connector instance when it was created.
     * It is useful for error and audit messages.
     *
     * @return guid for the connector instance
     */
    public abstract String getConnectorInstanceId();


    /**
     * Returns the connection object that was used to create the connector instance.  Its contents are never refreshed
     * during the lifetime of a connector instance even if the connection information is updated or removed from
     * the originating metadata repository.
     *
     * @return connection object
     */
    public abstract Connection  getConnection();


    /**
     * Set up the connected asset properties object.  This provides the known metadata properties stored in one or more
     * metadata repositories.  The implementation of the connected asset properties object is free to determine when
     * the properties are populated.  It may be as lazy as whenever getConnectedAssetProperties() is called.
     *
     * @param connectedAssetProperties - properties of the connected asset
     */
    public abstract void initializeConnectedAssetProperties(ConnectedAssetProperties connectedAssetProperties);


    /**
     * Returns the properties that contain the metadata for the asset.  The asset metadata is retrieved from the
     * metadata repository and cached in the ConnectedAssetProperties object each time the getConnectedAssetProperties
     * method is requested by the caller.   Once the ConnectedAssetProperties object has the metadata cached, it can be
     * used to access the asset property values many times without a return to the metadata repository.
     * The cache of metadata can be refreshed simply by calling this getConnectedAssetProperties() method again.
     *
     * @return ConnectedAssetProperties - connected asset properties
     * @throws PropertyServerException - indicates a problem retrieving properties from a metadata repository
     */
    public abstract ConnectedAssetProperties getConnectedAssetProperties() throws PropertyServerException;


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public abstract void start() throws ConnectorCheckedException;



    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public abstract void disconnect() throws ConnectorCheckedException;
}