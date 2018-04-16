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
 * The EmbeddedConnection is used within a VirtualConnection.  It contains a connection and additional properties
 * the VirtualConnection uses when working with the EmbeddedConnection.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class EmbeddedConnection extends PropertyBase
{
    /*
     * Attributes of an embedded connection
     */
    private AdditionalProperties      embeddedConnectionProperties = null;
    private Connection                embeddedConnection = null;


    /**
     * Default Constructor
     */
    public EmbeddedConnection()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateEmbeddedConnection - element to copy
     */
    public EmbeddedConnection(EmbeddedConnection templateEmbeddedConnection)
    {
        super(templateEmbeddedConnection);

        if (templateEmbeddedConnection != null)
        {
            AdditionalProperties  templateConnectionProperties = templateEmbeddedConnection.getEmbeddedConnectionProperties();
            Connection            templateConnection           = templateEmbeddedConnection.getEmbeddedConnection();

            if (templateConnectionProperties != null)
            {
                embeddedConnectionProperties = new AdditionalProperties(templateConnectionProperties);
            }
            if (templateConnection != null)
            {
                embeddedConnection = new Connection(templateConnection);
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
            return new AdditionalProperties(embeddedConnectionProperties);
        }
    }


    /**
     * Set up the embedded connection's properties.
     *
     * @param embeddedConnectionProperties - Additional properties
     */
    public void setEmbeddedConnectionProperties(AdditionalProperties embeddedConnectionProperties)
    {
        this.embeddedConnectionProperties = embeddedConnectionProperties;
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
            return new Connection(embeddedConnection);
        }
    }


    /**
     * Set up the embedded connection.
     *
     * @param embeddedConnection - Connection
     */
    public void setEmbeddedConnection(Connection embeddedConnection)
    {
        this.embeddedConnection = embeddedConnection;
    }
}