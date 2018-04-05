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
package org.apache.atlas.ocf;

import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.ffdc.PropertyServerException;
import org.apache.atlas.ocf.properties.AdditionalProperties;
import org.apache.atlas.ocf.properties.ConnectedAssetProperties;
import org.apache.atlas.ocf.properties.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * The ConnectorBase is an implementation of the Connector interface.
 *
 * Connectors are client-side interfaces to assets such as data stores, data sets, APIs, analytical functions.
 * They handle the communication with the server that hosts the assets, along with the communication with the
 * metadata server to serve up metadata about the assets, and support for an audit log for the caller to log its
 * activity.
 *
 * Each connector implementation is paired with a connector provider.  The connector provider is the factory for
 * connector instances.
 *
 * The Connector interface defines that a connector instance should be able to return a unique
 * identifier, a connection object and a metadata properties object for its connected asset.
 * These are supplied to the connector during its initialization.
 *
 * The ConnectorBase base class implements all of the methods required by the Connector interface.
 * Each specific implementation of a connector then extends this interface to add the methods to work with the
 * particular type of asset it supports.  For example, a JDBC connector would add the standard JDBC SQL interface, the
 * OMRS Connectors add the metadata repository management APIs...
 */
public abstract class ConnectorBase extends Connector
{
    protected String                   connectorInstanceId      = null;
    protected Connection               connection               = null;
    protected ConnectedAssetProperties connectedAssetProperties = null;
    protected boolean                  isActive                 = false;

    /*
     * Secured properties are protected properties from the connection.  They are retrieved as a protected
     * variable to allow subclasses of ConnectorBase to access them.
     */
    protected AdditionalProperties       securedProperties = null;

    private static final int      hashCode = UUID.randomUUID().hashCode();
    private static final Logger   log = LoggerFactory.getLogger(ConnectorBase.class);

    /**
     * Typical Constructor - Connectors should always have a constructor requiring no parameters and perform
     * initialization in the initialize method.
     */
    public  ConnectorBase()
    {
        /*
         * Nothing to do - real initialization happens in the initialize() method.
         * This pattern is used to make it possible for ConnectorBrokerBase to support the dynamic loading and
         * instantiation of arbitrary connector instances without needing to know the specifics of their constructor
         * methods
         */

        if (log.isDebugEnabled())
        {
            log.debug("New Connector Requested");
        }
    }


    /**
     * Call made by the ConnectorProvider to initialize the Connector with the base services.
     *
     * @param connectorInstanceId - unique id for the connector instance - useful for messages etc
     * @param connection - POJO for the configuration used to create the connector.
     */
    public void initialize(String                    connectorInstanceId,
                           Connection                connection)
    {
        this.connectorInstanceId = connectorInstanceId;
        this.connection = connection;

        /*
         * Set up the secured properties
         */
        ProtectedConnection  protectedConnection = new ProtectedConnection(connection);
        this.securedProperties = protectedConnection.getSecuredProperties();

        if (log.isDebugEnabled())
        {
            log.debug("New Connector initialized: " + connectorInstanceId + ", " + connection.getConnectionName());
        }
    }


    /**
     * Returns the unique connector instance id that assigned to the connector instance when it was created.
     * It is useful for error and audit messages.
     *
     * @return guid for the connector instance
     */
    public String getConnectorInstanceId()
    {
        return connectorInstanceId;
    }


    /**
     * Returns the connection object that was used to create the connector instance.  Its contents are never refreshed
     * during the lifetime of the connector instance, even if the connection information is updated or removed
     * from the originating metadata repository.
     *
     * @return connection object
     */
    public Connection  getConnection()
    {
        return connection;
    }


    /**
     * Set up the connected asset properties object.  This provides the known metadata properties stored in one or more
     * metadata repositories.  The properties are populated whenever getConnectedAssetProperties() is called.
     *
     * @param connectedAssetProperties - properties of the connected asset
     */
    public void initializeConnectedAssetProperties(ConnectedAssetProperties connectedAssetProperties)
    {
        this.connectedAssetProperties = connectedAssetProperties;
    }


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
    public ConnectedAssetProperties getConnectedAssetProperties() throws PropertyServerException
    {
        if (log.isDebugEnabled())
        {
            log.debug("ConnectedAssetProperties requested: " + connectorInstanceId + ", " + connection.getConnectionName());
        }

        if (connectedAssetProperties != null)
        {
            connectedAssetProperties.refresh();
        }

        return connectedAssetProperties;
    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        isActive = true;
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public  void disconnect() throws ConnectorCheckedException
    {
        isActive = false;
    }


    /**
     * Return a flag indicating whether the connector is active.  This means it has been started and not yet
     * disconnected.
     *
     * @return isActive flag
     */
    public boolean isActive()
    {
        return isActive;
    }


    /**
     * Provide a common implementation of hashCode for all OCF Connector objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to describe the hash code of the connector
     * object.
     *
     * @return random UUID as hashcode
     */
    public int hashCode()
    {
        return hashCode;
    }


    /**
     * Provide a common implementation of equals for all OCF Connector Provider objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to evaluate the equality of the connector
     * provider object.
     *
     * @param object - object to test
     * @return boolean flag
     */
    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        ConnectorBase that = (ConnectorBase) object;

        if (connectorInstanceId != null ? !connectorInstanceId.equals(that.connectorInstanceId) : that.connectorInstanceId != null)
        {
            return false;
        }
        if (connection != null ? !connection.equals(that.connection) : that.connection != null)
        {
            return false;
        }
        return connectedAssetProperties != null ? connectedAssetProperties.equals(that.connectedAssetProperties) : that.connectedAssetProperties == null;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ConnectorBase{" +
                "connectorInstanceId='" + connectorInstanceId + '\'' +
                ", connection=" + connection +
                ", connectedAssetProperties=" + connectedAssetProperties +
                '}';
    }

    private class ProtectedConnection extends Connection
    {
        private ProtectedConnection(Connection templateConnection)
        {
            super(templateConnection);
        }

        /**
         * Return a copy of the secured properties.  Null means no secured properties are available.
         * This method is protected so only OCF (or subclasses) can access them.  When Connector is passed to calling
         * OMAS, the secured properties are not available.
         *
         * @return secured properties - typically user credentials for the connection
         */
        protected AdditionalProperties getSecuredProperties()
        {
            if (super.securedProperties == null)
            {
                return securedProperties;
            }
            else
            {
                return new AdditionalProperties(super.getParentAsset(), securedProperties);
            }
        }
    }
}