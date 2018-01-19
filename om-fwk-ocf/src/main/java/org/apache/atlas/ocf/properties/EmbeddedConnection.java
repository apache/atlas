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
 * The EmbeddedConnection is used within a VirtualConnection.  It contains a connection and additional properties
 * the VirtualConnection uses when working with the EmbeddedConnection.
 */
public class EmbeddedConnection extends AssetPropertyBase
{
    /*
     * Attributes of an embedded connection
     */
    private AdditionalProperties      embeddedConnectionProperties = null;
    private Connection                embeddedConnection = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param embeddedConnectionProperties - Additional properties
     * @param embeddedConnection - Connection
     */
    public EmbeddedConnection(AssetDescriptor      parentAsset,
                              AdditionalProperties embeddedConnectionProperties,
                              Connection           embeddedConnection)
    {
        super(parentAsset);

        this.embeddedConnectionProperties = embeddedConnectionProperties;
        this.embeddedConnection = embeddedConnection;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateEmbeddedConnection - element to copy
     */
    public EmbeddedConnection(AssetDescriptor parentAsset, EmbeddedConnection templateEmbeddedConnection)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateEmbeddedConnection);

        if (templateEmbeddedConnection != null)
        {
            AdditionalProperties  templateConnectionProperties = templateEmbeddedConnection.getEmbeddedConnectionProperties();
            Connection            templateConnection           = templateEmbeddedConnection.getEmbeddedConnection();

            if (templateConnectionProperties != null)
            {
                embeddedConnectionProperties = new AdditionalProperties(parentAsset, templateConnectionProperties);
            }
            if (templateConnection != null)
            {
                embeddedConnection = new Connection(parentAsset, templateConnection);
            }
        }
    }


    /**
     * Return the properties for the embedded connection.
     *
     * @return AdditionalProperties
     */
    public AdditionalProperties getEmbeddedConnectionProperties()
    {
        if (embeddedConnectionProperties == null)
        {
            return embeddedConnectionProperties;
        }
        else
        {
            return new AdditionalProperties(this.getParentAsset(), embeddedConnectionProperties);
        }
    }


    /**
     * Return the embedded connection.
     *
     * @return Connection object.
     */
    public Connection getEmbeddedConnection()
    {
        if (embeddedConnection == null)
        {
            return embeddedConnection;
        }
        else
        {
            return new Connection(this.getParentAsset(), embeddedConnection);
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
        return "EmbeddedConnection{" +
                "embeddedConnectionProperties=" + embeddedConnectionProperties +
                ", embeddedConnection=" + embeddedConnection +
                '}';
    }
}