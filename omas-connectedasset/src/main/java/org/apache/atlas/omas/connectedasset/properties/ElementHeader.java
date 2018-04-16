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

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * ElementHeader provides the common identifier and type information for all properties objects
 * that link off of the asset and have a guid associated with them.  This typically means it is
 * represented by an entity in the metadata repository.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class ElementHeader extends PropertyBase
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
    private List<Classification> classifications = null;


    /**
     * Default Constructor
     */
    public ElementHeader()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateHeader - element to copy
     */
    public ElementHeader(ElementHeader templateHeader)
    {
        /*
         * Save the parent asset description.
         */
        super(templateHeader);

        if (templateHeader != null)
        {
            /*
             * Copy the values from the supplied like.
             */
            type = templateHeader.getType();
            guid = templateHeader.getGUID();
            url = templateHeader.getURL();
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
     * Set up the type for this properties object.  Null means the type information is unknown.
     *
     * @param type - details of the metadata type for this properties object
     */
    public void setType(ElementType type)
    {
        if (type == null)
        {
            this.type = null;
        }
        else
        {
            this.type = type;
        }
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
     * Set up the unique id for the properties object.  Null means no guid is assigned.
     *
     * @param guid - String - unique id
     */
    public void setGUID(String guid) { this.guid = guid; }


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
     * Set up the the URL for the properties object in the metadata repository.  Null means
     * no url is available.
     *
     * @param url - String - URL
     */
    public void setURL(String url) {
        this.url = url;
    }


    /**
     * Return the list of classifications associated with the asset.   This is an enumeration and the
     * pointers are set to the start of the list of classifications
     *
     * @return Classifications - list of classifications
     */
    public List<Classification> getClassifications()
    {
        if (classifications == null)
        {
            return classifications;
        }
        else
        {
            return new ArrayList<>(classifications);
        }
    }


    /**
     * Set up the list of classifications for this asset.
     *
     * @param classifications - list of classifications
     */
    public void setClassifications(List<Classification> classifications)
    {
        if (classifications == null)
        {
            this.classifications = classifications;
        }
        else
        {
            this.classifications = new ArrayList<>(classifications);
        }
    }

    /**
     * Provide a common implementation of hashCode for all OCF properties objects that have a guid.
     * The guid is unique and is randomly assigned and so its hashCode is as good as anything to
     * describe the hash code of the properties object.  If the guid is null then use the superclass implementation.
     */
    public int hashCode()
    {
        if (guid == null)
        {
            return super.hashCode();
        }
        else
        {
            return guid.hashCode();
        }
    }
}