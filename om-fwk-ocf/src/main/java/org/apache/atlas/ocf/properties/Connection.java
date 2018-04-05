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
package org.apache.atlas.ocf.properties;


/**
 * The connection is an object that contains the properties needed to create and initialise a connector to access a
 * specific data assets.
 *
 * The properties for a connection are defined in model 0201.  They include the following options for connector name:
 * <ul>
 *     <li>
 *         guid - Globally unique identifier for the connection.
 *     </li>
 *     <li>
 *         url - URL of the connection definition in the metadata repository.
 *         This URL can be stored as a property in another entity to create an explicit link to this connection.
 *     </li>
 *     <li>
 *         qualifiedName - The official (unique) name for the connection.
 *         This is often defined by the IT systems management organization and should be used (when available) on
 *         audit logs and error messages.  The qualifiedName is defined in the 0010 model as part of Referenceable.
 *     </li>
 *     <li>
 *         displayName - A consumable name for the connection.   Often a shortened form of the qualifiedName for use
 *         on user interfaces and messages.  The displayName should be only be used for audit logs and error messages
 *         if the qualifiedName is not set.
 *     </li>
 * </ul>
 *  Either the guid, qualifiedName or displayName can be used to specify the name for a connection.
 *
 *  Other properties for the connection include:
 *
 *  <ul>
 *      <li>
 *          type - information about the TypeDef for Connection
 *      </li>
 *      <li>
 *          description - A full description of the connection covering details of the assets it connects to
 *          along with usage and versioning information.
 *      </li>
 *      <li>
 *          additionalProperties - Any additional properties associated with the connection.
 *      </li>
 *      <li>
 *          securedProperties - Protected properties for secure log on by connector to back end server.  These
 *          are protected properties that can only be retrieved by privileged connector code.
 *      </li>
 *      <li>
 *          connectorType - Properties that describe the connector type for the connector.
 *      </li>
 *      <li>
 *          endpoint - Properties that describe the server endpoint where the connector will retrieve the assets.
 *      </li>
 *  </ul>

 * The connection class is simply used to cache the properties for an connection.
 * It is used by other classes to exchange this information between a metadata repository and a consumer.
 */
public class Connection extends Referenceable
{
    /*
     * Attributes of a connector
     */
    protected String                    displayName = null;
    protected String                    description = null;
    protected ConnectorType             connectorType = null;
    protected Endpoint                  endpoint = null;

    /*
     * Secured properties are protected so they can only be accessed by subclassing this object.
     */
    protected AdditionalProperties    securedProperties = null;

    /**
     * Typical Constructor - for constructing a new, independent populated Connection.
     *
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param displayName - consumable name
     * @param description - stored description property for the connection.
     * @param connectorType - connector type to copy
     * @param endpoint - endpoint properties
     * @param securedProperties - typically user credentials for the connection
     */
    public Connection(ElementType          type,
                      String               guid,
                      String               url,
                      Classifications      classifications,
                      String               qualifiedName,
                      AdditionalProperties additionalProperties,
                      Meanings             meanings,
                      String               displayName,
                      String               description,
                      ConnectorType        connectorType,
                      Endpoint             endpoint,
                      AdditionalProperties securedProperties)
    {
        super(null, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.connectorType = connectorType;
        this.endpoint = endpoint;
        this.securedProperties = securedProperties;
    }


    /**
     * Typical Constructor - for constructing a new, populated Connection as part of connected asset properties.
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object
     * @param meanings - list of glossary terms (summary)
     * @param displayName - consumable name
     * @param description - stored description property for the connection.
     * @param connectorType - connector type to copy
     * @param endpoint - endpoint properties
     * @param securedProperties - typically user credentials for the connection
     */
    public Connection(AssetDescriptor      parentAsset,
                      ElementType          type,
                      String               guid,
                      String               url,
                      Classifications      classifications,
                      String               qualifiedName,
                      AdditionalProperties additionalProperties,
                      Meanings             meanings,
                      String               displayName,
                      String               description,
                      ConnectorType        connectorType,
                      Endpoint             endpoint,
                      AdditionalProperties securedProperties)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.connectorType = connectorType;
        this.endpoint = endpoint;
        this.securedProperties = securedProperties;
    }


    /**
     * Copy/clone Constructor to return a copy of a connection object that is not connected to an asset.
     *
     * @param templateConnection - Connection to copy
     */
    public Connection(Connection   templateConnection)
    {
        /*
         * Set parentAsset to null
         */
        this(null, templateConnection);
    }

    /**
     * Copy/clone Constructor to return a copy of a connection object that is connected to an asset.
     *
     * @param parentAsset - description of the asset that this connection is attached to.
     * @param templateConnection - template object to copy.
     */
    public Connection(AssetDescriptor  parentAsset, Connection   templateConnection)
    {
        /*
         * Save parentAsset
         */
        super(parentAsset, templateConnection);

        /*
         * Copy over properties from the template.
         */
        if (templateConnection != null)
        {
            displayName = templateConnection.getDisplayName();
            description = templateConnection.getDescription();

            ConnectorType          templateConnectorType = templateConnection.getConnectorType();
            Endpoint               templateEndpoint = templateConnection.getEndpoint();
            AdditionalProperties   templateSecuredProperties = templateConnection.getSecuredProperties();

            if (templateConnectorType != null)
            {
                connectorType = new ConnectorType(parentAsset, templateConnectorType);
            }
            if (templateEndpoint != null)
            {
                endpoint = new Endpoint(parentAsset, templateEndpoint);
            }
            if (templateSecuredProperties != null)
            {
                securedProperties = new AdditionalProperties(parentAsset, templateSecuredProperties);
            }
        }
    }


    /**
     * Returns the stored display name property for the connection.
     * Null means no displayName is available.
     *
     * @return displayName
     */
    public String getDisplayName() { return displayName; }


    /**
     * Returns a formatted string with the connection name.  It is used in formatting error messages for the
     * exceptions thrown by consuming components.  It is extremely cautious because most of the exceptions
     * are reporting a malformed connection object so who knows what else is wrong with it.
     *
     * Within the connection are 2 possible properties that could
     * contain the connection name:
     *   ** qualifiedName - this is a uniqueName and should be there
     *   ** displayName - shorter simpler name but may not be unique - so may not identify the connection in error
     *
     * This method inspects these properties and builds up a string to represent the connection name
     *
     * @return connection name
     */
    public String  getConnectionName()
    {
        String   connectionName = "<Unknown>"; /* if all properties are blank */

        /*
         * The qualifiedName is preferred because it is unique.
         */
        if (qualifiedName != null && (!qualifiedName.equals("")))
        {
            /*
             * Use qualified name.
             */
            connectionName = qualifiedName;
        }
        else if (displayName != null && (!displayName.equals("")))
        {
            /*
             * The qualifiedName is not set but the displayName is available so use it.
             */
            connectionName = displayName;
        }

        return connectionName;
    }


    /**
     * Returns the stored description property for the connection.
     * If no description is provided then null is returned.
     *
     * @return description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Returns a copy of the properties for this connection's connector type.
     * A null means there is no connection type.
     *
     * @return connector type for the connection
     */
    public ConnectorType getConnectorType()
    {
        if (connectorType == null)
        {
            return connectorType;
        }
        else
        {
            return new ConnectorType(super.getParentAsset(), connectorType);
        }
    }


    /**
     * Returns a copy of the properties for this connection's endpoint.
     * Null means no endpoint information available.
     *
     * @return endpoint for the connection
     */
    public Endpoint getEndpoint()
    {
        if (endpoint == null)
        {
            return endpoint;
        }
        else
        {
            return new Endpoint(super.getParentAsset(), endpoint);
        }
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
        if (securedProperties == null)
        {
            return securedProperties;
        }
        else
        {
            return new AdditionalProperties(super.getParentAsset(), securedProperties);
        }
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Connection{" +
                "displayName='" + displayName + '\'' +
                ", description='" + description + '\'' +
                ", connectorType=" + connectorType +
                ", endpoint=" + endpoint +
                ", securedProperties=" + securedProperties +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}