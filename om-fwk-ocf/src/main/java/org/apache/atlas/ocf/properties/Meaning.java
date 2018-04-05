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
 * Meaning is a cut-down summary of a glossary term to aid the asset consumer in understanding the content
 * of an asset.
 */
public class Meaning extends ElementHeader
{
    /*
     * Attributes of a meaning object definition
     */
    protected String      name = null;
    protected String      description = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param name - name of the glossary term
     * @param description - description of the glossary term.
     */
    public Meaning(AssetDescriptor      parentAsset,
                   ElementType          type,
                   String               guid,
                   String               url,
                   Classifications      classifications,
                   String               name,
                   String               description)
    {
        super(parentAsset, type, guid, url, classifications);

        this.name = name;
        this.description = description;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateMeaning - element to copy
     */
    public Meaning(AssetDescriptor parentAsset, Meaning templateMeaning)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateMeaning);

        if (templateMeaning != null)
        {
            /*
             * Copy the values from the supplied meaning object.
             */
            name = templateMeaning.getName();
            description = templateMeaning.getDescription();
        }
    }


    /**
     * Return the glossary term name.
     *
     * @return String name
     */
    public String getName()
    {
        return name;
    }


    /**
     * Return the description of the glossary term.
     *
     * @return String description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Meaning{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                ", classifications=" + classifications +
                '}';
    }
}