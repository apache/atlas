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
 * The ElementType provide details of the type information associated with the element.  Most consumers
 * of the properties do not need this information.  It is provided to asset consumers primarily as diagnostic
 * information.
 */
public class ElementType extends PropertyBase
{
    protected String        elementTypeId                   = null;
    protected String        elementTypeName                 = null;
    protected long          elementTypeVersion              = 0;
    protected String        elementTypeDescription          = null;
    protected String        elementSourceServer             = null;
    protected ElementOrigin elementOrigin                   = null;
    protected String        elementHomeMetadataCollectionId = null;


    /**
     * Typical Constructor
     *
     * @param elementTypeId - identifier for the element's type
     * @param elementTypeName - element type name
     * @param elementTypeVersion - version number for the element type
     * @param elementTypeDescription - description of element type
     * @param elementSourceServer - url of the OMAS server
     * @param elementOrigin - enum describing type of origin
     * @param elementHomeMetadataCollectionId - metadata collection id
     */
    public ElementType(String         elementTypeId,
                       String         elementTypeName,
                       long           elementTypeVersion,
                       String         elementTypeDescription,
                       String         elementSourceServer,
                       ElementOrigin  elementOrigin,
                       String         elementHomeMetadataCollectionId)
    {
        super();

        this.elementTypeId = elementTypeId;
        this.elementTypeName = elementTypeName;
        this.elementTypeVersion = elementTypeVersion;
        this.elementTypeDescription = elementTypeDescription;
        this.elementSourceServer = elementSourceServer;
        this.elementOrigin = elementOrigin;
        this.elementHomeMetadataCollectionId = elementHomeMetadataCollectionId;
    }


    /**
     * Copy/clone constructor
     *
     * @param templateType - type to clone
     */
    public ElementType(ElementType templateType)
    {
        super(templateType);

        if (templateType != null)
        {
            /*
             * Copy the properties from the supplied template
             */
            this.elementTypeId = templateType.getElementTypeId();
            this.elementTypeName = templateType.getElementTypeName();
            this.elementTypeVersion = templateType.getElementTypeVersion();
            this.elementTypeDescription = templateType.getElementTypeDescription();
            this.elementSourceServer = templateType.getElementSourceServer();
            this.elementOrigin = templateType.getElementOrigin();
            this.elementHomeMetadataCollectionId = templateType.getElementHomeMetadataCollectionId();
        }
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
     * Return name of element's type.
     *
     * @return - elementTypeName
     */
    public String getElementTypeName()
    {
        return elementTypeName;
    }


    /**
     * Return the version number for this element's type.
     *
     * @return elementTypeVersion - version number for the element type.
     */
    public long getElementTypeVersion()
    {
        return elementTypeVersion;
    }


    /**
     * Return the description for this element's type.
     *
     * @return elementTypeDescription - String description for the element type
     */
    public String getElementTypeDescription()
    {
        return elementTypeDescription;
    }


    /**
     * Return the URL of the server where the element was retrieved from.  Typically this is
     * a server where the OMAS interfaces are activated.  If no URL is known for the server then null is returned.
     *
     * @return elementSourceServerURL - the url of the server where the element came from
     */
    public String getElementSourceServer()
    {
        return elementSourceServer;
    }


    /**
     * Return the origin of the metadata element.
     *
     * @return ElementOrigin enum
     */
    public ElementOrigin getElementOrigin() { return elementOrigin; }


    /**
     * Returns the OMRS identifier for the metadata collection that is managed by the repository
     * where the element originates (its home repository).
     *
     * @return String metadata collection id
     */
    public String getElementHomeMetadataCollectionId()
    {
        return elementHomeMetadataCollectionId;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ElementType{" +
                "elementTypeId='" + elementTypeId + '\'' +
                ", elementTypeName='" + elementTypeName + '\'' +
                ", elementTypeVersion=" + elementTypeVersion +
                ", elementTypeDescription='" + elementTypeDescription + '\'' +
                ", elementSourceServer='" + elementSourceServer + '\'' +
                ", elementOrigin=" + elementOrigin +
                ", elementHomeMetadataCollectionId='" + elementHomeMetadataCollectionId + '\'' +
                '}';
    }
}
