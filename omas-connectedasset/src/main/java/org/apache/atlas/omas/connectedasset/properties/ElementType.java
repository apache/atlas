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
import org.apache.atlas.ocf.properties.ElementOrigin;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The ElementType provide details of the type information associated with the element.  Most consumers
 * of the properties do not need this information.  It is provided to asset consumers primarily as diagnostic
 * information.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class ElementType extends PropertyBase
{
    protected String        elementTypeId                   = null;
    protected String        elementTypeName                 = null;
    protected long          elementTypeVersion              = 0;
    protected String        elementTypeDescription          = null;
    protected String        elementAccessServiceURL         = null;
    protected ElementOrigin elementOrigin                   = null;
    protected String        elementHomeMetadataCollectionId = null;



    /**
     * Typical Constructor
     */
    public ElementType()
    {
        super();

        /*
         * Nothing to do - all local variables initialized in the declaration
         */
    }


    /**
     * Copy/clone constructor
     *
     * @param templateType - type to clone
     */
    public ElementType(ElementType templateType)
    {
        super(templateType);

        /*
         * Copy the properties from the supplied template
         */
        this.elementTypeId = templateType.getElementTypeId();
        this.elementTypeName = templateType.getElementTypeName();
        this.elementTypeVersion = templateType.getElementTypeVersion();
        this.elementTypeDescription = templateType.getElementTypeDescription();
        this.elementAccessServiceURL = templateType.getElementAccessServiceURL();
        this.elementHomeMetadataCollectionId = templateType.getElementHomeMetadataCollectionId();
    }

    /**
     * Return unique identifier for the element's type.
     *
     * @return element type id
     */
    public String getElementTypeId()
    {
        return elementTypeId;
    }


    /**
     * Set the unique identifier of the element's type.
     *
     * @param elementTypeId - new identifier for the element's type
     */
    public void setElementTypeId(String elementTypeId)
    {
        this.elementTypeId = elementTypeId;
    }


    /**
     * Return name of element's type.
     *
     * @return - elementTypeName
     */
    public String getElementTypeName()
    {
        return elementTypeName;
    }


    /**
     * Set name of element's type.
     *
     * @param elementTypeName - element type name
     */
    public void setElementTypeName(String elementTypeName)
    {
        this.elementTypeName = elementTypeName;
    }


    /**
     * Return the version number for the element type.
     *
     * @return elementTypeVersion - version number for the element type.
     */
    public long getElementTypeVersion()
    {
        return elementTypeVersion;
    }


    /**
     * Set up a new version number for the element type.
     *
     * @param elementTypeVersion version number for the element type
     */
    public void setElementTypeVersion(long elementTypeVersion)
    {
        this.elementTypeVersion = elementTypeVersion;
    }


    /**
     * Return the description for the element type.
     *
     * @return elementTypeDescription - description for the element type
     */
    public String getElementTypeDescription()
    {
        return elementTypeDescription;
    }


    /**
     * Set up a new description for the element type.
     *
     * @param elementTypeDescription - description of element type
     */
    public void setElementTypeDescription(String elementTypeDescription)
    {
        this.elementTypeDescription = elementTypeDescription;
    }


    /**
     * Return the URL of the server where the element was retrieved from.  Typically this is
     * a server where the OMAS interfaces are activated.  If no URL is known for the server then null is returned.
     *
     * @return elementSourceServerURL - the url of the server where the element came from
     */
    public String getElementAccessServiceURL()
    {
        return elementAccessServiceURL;
    }


    /**
     * Set up the name of the server where this metadata element was retrieved from. Typically this is
     * a server where the OMAS interfaces are activated.  If no URL is known for the server then null is passed in the
     * parameter.
     *
     * @param elementAccessServiceURL - url of the OMAS server
     */
    public void setElementAccessServiceURL(String    elementAccessServiceURL)
    {
        this.elementAccessServiceURL = elementAccessServiceURL;
    }



    /**
     * Return the origin of the metadata element.
     *
     * @return ElementOrigin enum
     */
    public ElementOrigin getElementOrigin() { return elementOrigin; }


    /**
     * Set up the origin of the metadata element.
     *
     * @param elementOrigin - enum
     */
    public void setElementOrigin(ElementOrigin elementOrigin)
    {
        this.elementOrigin = elementOrigin;
    }


    /**
     * Returns the unique identifier for the metadata collection that is managed by the repository
     * where the element originates (its home repository).
     *
     * @return String metadata collection id
     */
    public String getElementHomeMetadataCollectionId()
    {
        return elementHomeMetadataCollectionId;
    }


    /**
     * Set up the unique identifier for the metadata collection that is managed by the repository
     * where the element originates (its home repository).
     *
     * @param elementHomeMetadataCollectionId - String guid
     */
    public void setElementHomeMetadataCollectionId(String elementHomeMetadataCollectionId)
    {
        this.elementHomeMetadataCollectionId = elementHomeMetadataCollectionId;
    }
}