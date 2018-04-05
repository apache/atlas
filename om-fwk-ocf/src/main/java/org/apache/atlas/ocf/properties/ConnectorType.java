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
 * The ConnectorType describe the implementation details of a particular type of OCF connector.
 * The properties for a connector type are defined in model 0201.
 * They include:
 *
 * <ul>
 *     <li>
 *         guid - Globally unique identifier for the connector type.
 *     </li>
 *     <li>
 *         url - External link address for the connector type properties in the metadata repository.  This URL can be
 *         stored as a property in another entity to create an explicit link to this connector type.
 *     </li>
 *     <li>
 *         qualifiedName - The official (unique) name for the connector type. This is often defined by the IT
 *         systems management organization and should be used (when available) on audit logs and error messages.
 *     </li>
 *     <li>
 *         displayName - A consumable name for the connector type.   Often a shortened form of the qualifiedName for use
 *         on user interfaces and messages.  The displayName should be only be used for audit logs and error messages
 *         if the qualifiedName is not set.
 *     </li>
 *     <li>
 *         description - A full description of the connector type covering details of the assets it connects to
 *         along with usage and versioning information.
 *     </li>
 *     <li>
 *         connectorProviderClassName - The connector provider is the factory for a particular type of connector.
 *         This property defines the class name for the connector provider that the Connector Broker should use to request
 *         new connector instances.
 *     </li>
 *     <li>
 *         additionalProperties - Any additional properties that the connector provider needs to know in order to
 *         create connector instances.
 *     </li>
 * </ul>
 *
 * The connectorTypeProperties class is simply used to cache the properties for an connector type.
 * It is used by other classes to exchange this information between a metadata repository and a consumer.
 */
public class ConnectorType extends Referenceable
{
    /*
     * Attributes of a connector type
     */
    protected   String                 displayName = null;
    protected   String                 description = null;
    protected   String                 connectorProviderClassName = null;

    /**
     * Typical Constructor - used when Connector Type is used inside a connection object which is itself
     * not yet connected to an asset.  In this case the ParentAsset is null.
     *
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param displayName - consumable name property stored for the connector type.
     * @param description - description property stored for the connector type.
     * @param connectorProviderClassName - class name (including package name)
     */
    public ConnectorType(ElementType          type,
                         String               guid,
                         String               url,
                         Classifications      classifications,
                         String               qualifiedName,
                         AdditionalProperties additionalProperties,
                         Meanings             meanings,
                         String               displayName,
                         String               description,
                         String               connectorProviderClassName)
    {
        super(null, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.connectorProviderClassName = connectorProviderClassName;
    }


    /**
     * Typical constructor for creating a connectorType linked to an asset.
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param displayName - consumable name property stored for the connector type.
     * @param description - description property stored for the connector type.
     * @param connectorProviderClassName - class name (including package name)
     */
    public ConnectorType(AssetDescriptor parentAsset, ElementType type, String guid, String url, Classifications classifications, String qualifiedName, AdditionalProperties additionalProperties, Meanings meanings, String displayName, String description, String connectorProviderClassName)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.connectorProviderClassName = connectorProviderClassName;
    }

    /**
     * Copy/clone constructor for a connectorType that is not connected to an asset (either directly or indirectly).
     *
     * @param templateConnectorType - template object to copy.
     */
    public ConnectorType(ConnectorType    templateConnectorType)
    {
        this(null, templateConnectorType);
    }


    /**
     * Copy/clone constructor for a connectorType that is connected to an asset (either directly or indirectly).
     *
     * @param parentAsset - description of the asset that this connector type is attached to.
     * @param templateConnectorType - template object to copy.
     */
    public ConnectorType(AssetDescriptor  parentAsset, ConnectorType templateConnectorType)
    {
        /*
         * Save parentAsset.
         */
        super(parentAsset, templateConnectorType);

        /*
         * All properties are initialised as null so only change their default setting if the template is
         * not null
         */
        if (templateConnectorType != null)
        {
            displayName = templateConnectorType.getDisplayName();
            description = templateConnectorType.getDescription();
            connectorProviderClassName = templateConnectorType.getConnectorProviderClassName();
        }
    }


    /**
     * Returns the stored display name property for the connector type.
     * If no display name is available then null is returned.
     *
     * @return displayName
     */
    public String getDisplayName()
    {
        return displayName;
    }


    /**
     * Returns the stored description property for the connector type.
     * If no description is available then null is returned.
     *
     * @return description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Returns the stored connectorProviderClassName property for the connector type.
     * If no connectorProviderClassName is available then null is returned.
     *
     * @return connectorProviderClassName - class name (including package name)
     */
    public String getConnectorProviderClassName()
    {
        return connectorProviderClassName;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ConnectorType{" +
                "displayName='" + displayName + '\'' +
                ", description='" + description + '\'' +
                ", connectorProviderClassName='" + connectorProviderClassName + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}