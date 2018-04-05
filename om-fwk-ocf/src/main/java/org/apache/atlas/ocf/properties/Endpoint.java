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
public class Endpoint extends Referenceable
{
    /*
     * Properties of an Endpoint
     */
    protected   String                 displayName      = null;
    protected   String                 description      = null;
    protected   String                 address          = null;
    protected   String                 protocol         = null;
    protected   String                 encryptionMethod = null;

    /**
     * Admin Constructor - used when Endpoint is inside a Connection that is not part of the connected asset
     * properties.  In this case there is no parent asset.
     *
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param displayName - simple name for the endpoint
     * @param description - String description for the endpoint
     * @param address - network url for the server/resource
     * @param protocol - endpoint protocol
     * @param encryptionMethod - encryption mechanism in use by the endpoint
     */
    public Endpoint(ElementType          type,
                    String               guid,
                    String               url,
                    Classifications      classifications,
                    String               qualifiedName,
                    AdditionalProperties additionalProperties,
                    Meanings             meanings,
                    String               displayName,
                    String               description,
                    String               address,
                    String               protocol,
                    String               encryptionMethod)
    {
        this(null,
             type,
             guid,
             url,
             classifications,
             qualifiedName,
             additionalProperties,
             meanings,
             displayName,
             description,
             address,
             protocol,
             encryptionMethod);
    }


    /**
     * Typical Constructor for a new endpoint that is connected to an asset (either directly or indirectly.)
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param displayName - simple name for the endpoint
     * @param description - String description for the endpoint
     * @param address - network url for the server/resource
     * @param protocol - endpoint protocol
     * @param encryptionMethod - encryption mechanism in use by the endpoint
     */
    public Endpoint(AssetDescriptor      parentAsset,
                    ElementType          type,
                    String               guid,
                    String               url,
                    Classifications      classifications,
                    String               qualifiedName,
                    AdditionalProperties additionalProperties,
                    Meanings             meanings,
                    String               displayName,
                    String               description,
                    String               address,
                    String               protocol,
                    String               encryptionMethod)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.address = address;
        this.protocol = protocol;
        this.encryptionMethod = encryptionMethod;
    }


    /**
     * Copy/clone constructor for an Endpoint not connected to an asset.
     *
     * @param templateEndpoint - template object to copy.
     */
    public Endpoint(Endpoint  templateEndpoint)
    {
        this(null, templateEndpoint);
    }


    /**
     * Copy/clone constructor for an Endpoint that is connected to an Asset (either directly or indirectly).
     *
     * @param parentAsset - description of the asset that this endpoint is attached to.
     * @param templateEndpoint - template object to copy.
     */
    public Endpoint(AssetDescriptor  parentAsset, Endpoint templateEndpoint)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateEndpoint);

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
                additionalProperties = new AdditionalProperties(parentAsset, templateAdditionalProperties);
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
     * Return the description for the endpoint.
     *
     * @return String description
     */
    public String getDescription()
    {
        return description;
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
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Endpoint{" +
                "displayName='" + displayName + '\'' +
                ", description='" + description + '\'' +
                ", address='" + address + '\'' +
                ", protocol='" + protocol + '\'' +
                ", encryptionMethod='" + encryptionMethod + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}