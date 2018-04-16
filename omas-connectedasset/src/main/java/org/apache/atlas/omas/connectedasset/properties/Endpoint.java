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
 * The Endpoint describes the network information necessary for a connector to connect to the server
 * where the Asset is accessible from.  The properties for an endpoint are defined in model 0040.
 * They include:
 * <ul>
 *     <li>
 *         type - definition of the specific metadata type for the endpoint.
 *     </li>
 *     <li>
 *         guid - Globally unique identifier for the endpoint.
 *     </li>
 *     <li>
 *         url - External link address for the endpoint properties in the metadata repository.
 *         This URL can be stored as a property in another entity to create an explicit link to this endpoint.
 *     </li>
 *     <li>
 *         qualifiedName - The official (unique) name for the endpoint. This is often defined by the IT systems management
 *         organization and should be used (when available) on audit logs and error messages.
 *     </li>
 *     <li>
 *         displayName - A consumable name for the endpoint.   Often a shortened form of the qualifiedName for use
 *         on user interfaces and messages.  The displayName should be only be used for audit logs and error messages
 *         if the qualifiedName is not set.
 *     </li>
 *     <li>
 *         description - A description for the endpoint.
 *     </li>
 *     <li>
 *         address - The location of the asset.  For network connected resources, this is typically the
 *         URL and port number (if needed) for the server where the asset is located
 *         (or at least accessible by the connector).  For file-based resources, this is typically the name of the file.
 *     </li>
 *     <li>
 *         protocol - The communication protocol that the connection should use to connect to the server.
 *     </li>
 *     <li>
 *         encryptionMethod - Describes the encryption method to use (if any).  This is an open value allowing
 *         information needed by the connector user to retrieve all of the information they need to work with
 *         the endpoint.
 *     </li>
 *     <li>
 *         additionalProperties - Any additional properties that the connector need to know in order to
 *         access the Asset.
 *     </li>
 * </ul>
 *
 * The Endpoint class is simply used to cache the properties for an endpoint.
 * It is used by other classes to exchange this information between a metadata repository and a consumer.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Endpoint extends Referenceable
{
    /*
     * Properties of an Endpoint
     */
    private   String                 displayName      = null;
    private   String                 description      = null;
    private   String                 address          = null;
    private   String                 protocol         = null;
    private   String                 encryptionMethod = null;

    /**
     * Default Constructor - used when Endpoint is inside a Connection that is not part of the connected asset
     * properties.  In this case there is no parent asset.
     */
    public Endpoint()
    {
        super();
    }


    /**
     * Copy/clone constructor for an Endpoint that is connected to an Asset (either directly or indirectly).
     *
     * @param templateEndpoint - template object to copy.
     */
    public Endpoint(Endpoint templateEndpoint)
    {
        super(templateEndpoint);

        /*
         * All properties are initialised as null so only change their default setting if the template is
         * not null
         */
        if (templateEndpoint != null)
        {
            displayName      = templateEndpoint.getDisplayName();
            address          = templateEndpoint.getAddress();
            protocol         = templateEndpoint.getProtocol();
            encryptionMethod = templateEndpoint.getEncryptionMethod();

            AdditionalProperties   templateAdditionalProperties = templateEndpoint.getAdditionalProperties();

            if (templateAdditionalProperties != null)
            {
                additionalProperties = new AdditionalProperties(templateAdditionalProperties);
            }
        }
    }


    /**
     * Returns the stored display name property for the endpoint.
     * If no display name is available then null is returned.
     *
     * @return displayName
     */
    public String getDisplayName()
    {
        return displayName;
    }


    /**
     * Updates the display name property stored for the endpoint.
     * If a null is supplied it means there is no display name for the endpoint.
     *
     * @param  newDisplayName - simple name for the endpoint
     */
    public void setDisplayName(String  newDisplayName) { displayName = newDisplayName; }


    /**
     * Return the description for the endpoint.
     *
     * @return String description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Set up the description for the endpoint.
     *
     * @param description - String description
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * Returns the stored address property for the endpoint.
     * If no network address is available then null is returned.
     *
     * @return address
     */
    public String getAddress()
    {
        return address;
    }


    /**
     * Updates the address property needed to get to the asset.
     * If a null is supplied it means there is no address for this endpoint.
     *
     * @param  newAddress - network url for the server
     */
    public void setAddress(String  newAddress) { address = newAddress; }


    /**
     * Returns the stored protocol property for the endpoint.
     * If no protocol is available then null is returned.
     *
     * @return protocol
     */
    public String getProtocol()
    {
        return protocol;
    }


    /**
     * Updates the protocol property stored for the endpoint.
     * If a null is supplied it is saved as an empty string.
     *
     * @param  newProtocol - endpoint protocol
     */
    public void setProtocol(String  newProtocol) { protocol = newProtocol; }


    /**
     * Returns the stored encryptionMethod property for the endpoint.  This is an open type allowing the information
     * needed to work with a specific encryption mechanism used by the endpoint to be defined.
     * If no encryptionMethod property is available (typically because this is an unencrypted endpoint)
     * then null is returned.
     *
     * @return encryption method information
     */
    public String getEncryptionMethod()
    {
        return encryptionMethod;
    }


    /**
     * Updates the encryptionMethod property stored for the endpoint.  If a null is supplied it means no
     * encryption is being used.
     *
     * @param  newEncryptionMethod - encryption mechanism in use
     */
    public void setEncryptionMethod(String  newEncryptionMethod) { encryptionMethod = newEncryptionMethod; }
}