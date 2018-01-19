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
 * A virtual connection is for an asset that provides data by delegating requests to one or more other connections.
 * it maintains a list of the connections that are used by its asset.  These are referred to as embedded connections.
 */
public class VirtualConnection extends Connection
{
    /*
     * Attributes of a virtual connection
     */
    protected EmbeddedConnections       embeddedConnections = null;


    /**
     * Typical Constructor
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
     * @param embeddedConnections - the embedded connections for this virtual connection.
     */
    public VirtualConnection(AssetDescriptor      parentAsset,
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
                             AdditionalProperties securedProperties,
                             EmbeddedConnections  embeddedConnections)
    {
        super(parentAsset,
              type,
              guid,
              url,
              classifications,
              qualifiedName,
              additionalProperties,
              meanings,
              displayName,
              description,
              connectorType,
              endpoint,
              securedProperties);

        this.embeddedConnections = embeddedConnections;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateVirtualConnection - element to copy
     */
    public VirtualConnection(AssetDescriptor parentAsset, VirtualConnection templateVirtualConnection)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateVirtualConnection);

        /*
         * Extract additional information from the template if available
         */
        if (templateVirtualConnection != null)
        {
            EmbeddedConnections  templateEmbeddedConnections = templateVirtualConnection.getEmbeddedConnections();

            if (templateEmbeddedConnections != null)
            {
                /*
                 * Ensure comment replies has this object's parent asset, not the template's.
                 */
                embeddedConnections = templateEmbeddedConnections.cloneIterator(parentAsset);
            }
        }
    }


    /**
     * Return the enumeration of embedded connections for this virtual connection.
     *
     * @return EmbeddedConnections
     */
    public EmbeddedConnections getEmbeddedConnections()
    {
        if (embeddedConnections == null)
        {
            return embeddedConnections;
        }
        else
        {
            return embeddedConnections.cloneIterator(super.getParentAsset());
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
        return "VirtualConnection{" +
                "embeddedConnections=" + embeddedConnections +
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