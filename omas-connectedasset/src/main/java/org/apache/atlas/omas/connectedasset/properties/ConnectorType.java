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
package org.apache.atlas.omas.connectedasset.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

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
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class ConnectorType extends Referenceable
{
    /*
     * Attributes of a connector type
     */
    private   String                 displayName = null;
    private   String                 description = null;
    private   String                 connectorProviderClassName = null;

    /**
     * Default Constructor - this is used when Connector Type is used inside a connection object which is itself
     * not yet connected to an asset.  In this case the ParentAsset is null.
     */
    public ConnectorType()
    {
        super();
    }


    /**
     * Copy/clone constructor for a connectorType that is connected to an asset (either directly or indirectly).
     *
     * @param templateConnectorType - template object to copy.
     */
    public ConnectorType(ConnectorType templateConnectorType)
    {
        super(templateConnectorType);

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
     * Updates the display name property stored for the connector type.
     * If a null is supplied it means no display name is available.
     *
     * @param  newDisplayName - consumable name
     */
    public void setDisplayName(String  newDisplayName) { displayName = newDisplayName; }


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
     * Updates the description property stored for the connector type.
     * If a null is supplied it means no description is available.
     *
     * @param  newDescription - description
     */
    public void setDescription(String  newDescription) { description = newDescription; }


    /**
     * Returns the stored connectorProviderClassName property for the connector type.
     * If no connectorProviderClassName is available then null is returned.
     *
     * @return connectorProviderClassName
     */
    public String getConnectorProviderClassName()
    {
        return connectorProviderClassName;
    }


    /**
     * Updates the connectorProviderClassName property stored for the connector type.
     * If a null is supplied it means no class name is available.
     *
     * @param  newClassName - class name (including package name)
     */
    public void setConnectorProviderClassName(String  newClassName) { connectorProviderClassName = newClassName; }
}