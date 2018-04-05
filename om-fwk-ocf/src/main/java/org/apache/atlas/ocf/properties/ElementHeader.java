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
 * ElementHeader provides the common identifier and type information for all properties objects
 * that link off of the asset and have a guid associated with them.  This typically means it is
 * represented by an entity in the metadata repository.
 */
public abstract class ElementHeader extends AssetPropertyBase
{
    /*
     * Common header for first class elements from a metadata repository
     */
    protected ElementType               type = null;
    protected String                    guid = null;
    protected String                    url = null;

    /*
     * Attached classifications
     */
    protected Classifications classifications = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     */
    public ElementHeader(AssetDescriptor parentAsset,
                         ElementType     type,
                         String          guid,
                         String          url,
                         Classifications classifications)
    {
        super(parentAsset);

        this.type = type;
        this.guid = guid;
        this.url = url;
        this.classifications = classifications;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateHeader - element to copy
     */
    public ElementHeader(AssetDescriptor parentAsset, ElementHeader templateHeader)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateHeader);

        if (templateHeader != null)
        {
            /*
             * Copy the values from the supplied parameters.
             */
            type = templateHeader.getType();
            guid = templateHeader.getGUID();
            url  = templateHeader.getURL();

            Classifications      templateClassifications = templateHeader.getClassifications();
            if (templateClassifications != null)
            {
                classifications = templateClassifications.cloneIterator(parentAsset);
            }
        }
    }


    /**
     * Return the element type properties for this properties object.  These values are set up by the metadata repository
     * and define details to the metadata entity used to represent this element.
     *
     * @return ElementType - type information.
     */
    public ElementType getType() {
        return type;
    }


    /**
     * Return the unique id for the properties object.  Null means no guid is assigned.
     *
     * @return String - unique id
     */
    public String getGUID() {
        return guid;
    }


    /**
     * Returns the URL to access the properties object in the metadata repository.
     * If no url is available then null is returned.
     *
     * @return String - URL
     */
    public String getURL() {
        return url;
    }


    /**
     * Return the list of classifications associated with the asset.   This is an enumeration and the
     * pointers are set to the start of the list of classifications
     *
     * @return Classifications - enumeration of classifications
     */
    public Classifications getClassifications()
    {
        if (classifications == null)
        {
            return classifications;
        }
        else
        {
            return classifications.cloneIterator(super.getParentAsset());
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
        return "ElementHeader{" +
                "type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                ", classifications=" + classifications +
                '}';
    }
}