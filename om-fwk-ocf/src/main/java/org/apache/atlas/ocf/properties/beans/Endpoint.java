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
package org.apache.atlas.ocf.properties.beans;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The Endpoint bean extends the Endpoint from the properties package with a default constructor and
 * setter methods.  This means it can be used for REST calls and other JSON based functions.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Endpoint extends org.apache.atlas.ocf.properties.Endpoint
{
    /**
     * Default constructor
     */
    public Endpoint()
    {
        super(null);
    }


    /**
     * Copy/clone constructor for an Endpoint not connected to an asset.
     *
     * @param templateEndpoint - template object to copy.
     */
    public Endpoint(Endpoint  templateEndpoint)
    {
        super(templateEndpoint);
    }


    /**
     * Set up the type of this element.
     *
     * @param type - element type proprerties
     */
    public void setType(ElementType type)
    {
        super.type = type;
    }


    /**
     * Set up the guid for the element.
     *
     * @param guid - String unique identifier
     */
    public void setGUID(String guid)
    {
        super.guid = guid;
    }


    /**
     * Set up the URL of this element.
     *
     * @param url - String
     */
    public void setURL(String url)
    {
        super.url = url;
    }


    /**
     * Set up the fully qualified name.
     *
     * @param qualifiedName - String name
     */
    public void setQualifiedName(String qualifiedName)
    {
        super.qualifiedName = qualifiedName;
    }


    /**
     * Set up additional properties.
     *
     * @param additionalProperties - Additional properties object
     */
    public void setAdditionalProperties(AdditionalProperties additionalProperties)
    {
        super.additionalProperties = additionalProperties;
    }


    /**
     * Set up the display name for UIs and reports.
     *
     * @param displayName - String name
     */
    public void setDisplayName(String displayName)
    {
        super.displayName = displayName;
    }


    /**
     * Set up description of the element.
     *
     * @param description - String
     */
    public void setDescription(String description)
    {
        super.description = description;
    }


    /**
     * Set up the network address of the Endpoint.
     *
     * @param address - String resource name
     */
    public void setAddress(String address)
    {
        super.address = address;
    }


    /**
     * Set up the protocol to use for this Endpoint
     *
     * @param protocol - String protocol name
     */
    public void setProtocol(String protocol)
    {
        super.protocol = protocol;
    }


    /**
     * Set up the encryption method used on this Endpoint.
     *
     * @param encryptionMethod - String name
     */
    public void setEncryptionMethod(String encryptionMethod)
    {
        super.encryptionMethod = encryptionMethod;
    }
}
